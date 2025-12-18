from __future__ import annotations

import json
import logging
import os
from collections import defaultdict
from typing import Any

from django.core.exceptions import FieldDoesNotExist
from django.db import models as dj_models
from django.db import transaction
from django.utils import timezone
from ledger_api_client.ledger_models import EmailUserRO

from boranga.components.data_migration.adapters.occurrence import (  # shared canonical schema
    schema,
)
from boranga.components.data_migration.adapters.occurrence.tec import (
    OccurrenceTecAdapter,
)
from boranga.components.data_migration.adapters.occurrence.tpfl import (
    OccurrenceTpflAdapter,
)
from boranga.components.data_migration.adapters.sources import Source
from boranga.components.data_migration.handlers.helpers import (
    apply_value_to_instance,
    normalize_create_kwargs,
)
from boranga.components.data_migration.registry import (
    BaseSheetImporter,
    ImportContext,
    TransformContext,
    register,
    run_pipeline,
)
from boranga.components.occurrence.models import (
    AssociatedSpeciesTaxonomy,
    OCCAssociatedSpecies,
    OCCContactDetail,
    OCCFireHistory,
    OCCHabitatComposition,
    OCCLocation,
    OCCObservationDetail,
    Occurrence,
    OccurrenceDocument,
    OccurrenceSite,
)

logger = logging.getLogger(__name__)

SOURCE_ADAPTERS = {
    Source.TPFL.value: OccurrenceTpflAdapter(),
    Source.TEC.value: OccurrenceTecAdapter(),
}


@register
class OccurrenceImporter(BaseSheetImporter):
    slug = "occurrence_legacy"
    description = "Import occurrence data from legacy TEC / TFAUNA / TPFL sources"

    def clear_targets(
        self, ctx: ImportContext, include_children: bool = False, **options
    ):
        """Delete Occurrence target data and obvious children. Respect `ctx.dry_run`."""
        if ctx.dry_run:
            logger.info("OccurrenceImporter.clear_targets: dry-run, skipping delete")
            return

        logger.warning("OccurrenceImporter: deleting Occurrence and related data...")

        # Perform deletes in an autocommit block so they are committed
        # immediately. This avoids the case where clear_targets runs inside a
        # larger transaction that later rolls back leaving the wipe undone.
        from django.db import connections

        conn = connections["default"]
        was_autocommit = conn.get_autocommit()
        if not was_autocommit:
            conn.set_autocommit(True)
        try:
            try:
                OCCContactDetail.objects.all().delete()
                OCCLocation.objects.all().delete()
                OccurrenceSite.objects.all().delete()
                OCCObservationDetail.objects.all().delete()
                OCCHabitatComposition.objects.all().delete()
                OCCFireHistory.objects.all().delete()
                OCCAssociatedSpecies.objects.all().delete()
                AssociatedSpeciesTaxonomy.objects.all().delete()
                OccurrenceDocument.objects.all().delete()
            except Exception:
                logger.exception("Failed to delete related Occurrence data")
            try:
                Occurrence.objects.all().delete()
            except Exception:
                logger.exception("Failed to delete Occurrence")

            # Reset the primary key sequence for Occurrence when using PostgreSQL.
            try:
                if getattr(conn, "vendor", None) == "postgresql":
                    table = Occurrence._meta.db_table
                    with conn.cursor() as cur:
                        cur.execute(
                            "SELECT setval(pg_get_serial_sequence(%s, %s), %s, %s)",
                            [table, "id", 1, False],
                        )
                    logger.info("Reset primary key sequence for table %s", table)
            except Exception:
                logger.exception("Failed to reset Occurrence primary key sequence")
        finally:
            if not was_autocommit:
                conn.set_autocommit(False)

    def add_arguments(self, parser):
        parser.add_argument(
            "--sources",
            nargs="+",
            choices=list(SOURCE_ADAPTERS.keys()),
            help="Subset of sources (default: all implemented)",
        )
        parser.add_argument(
            "--path-map",
            nargs="+",
            metavar="SRC=PATH",
            help="Per-source path overrides (e.g. TPFL=/tmp/tpfl.xlsx). If omitted, --path is reused.",
        )

    def _parse_path_map(self, pairs):
        out = {}
        if not pairs:
            return out
        for p in pairs:
            if "=" not in p:
                raise ValueError(f"Invalid path-map entry: {p}")
            k, v = p.split("=", 1)
            out[k] = v
        return out

    def run(self, path: str, ctx: ImportContext, **options):
        start_time = timezone.now()
        logger.info(
            "OccurrenceImporter (%s) started at %s (dry_run=%s)",
            self.slug,
            start_time.isoformat(),
            ctx.dry_run,
        )
        sources = options.get("sources") or list(SOURCE_ADAPTERS.keys())
        path_map = self._parse_path_map(options.get("path_map"))

        stats = ctx.stats.setdefault(self.slug, self.new_stats())
        all_rows: list[dict] = []
        warnings = []
        errors_details = []
        warnings_details = []

        # 1. Extract
        for src in sources:
            adapter = SOURCE_ADAPTERS[src]
            src_path = path_map.get(src, path)
            result = adapter.extract(src_path, **options)
            for w in result.warnings:
                warnings.append(f"{src}: {w.message}")
            for r in result.rows:
                r["_source"] = src
            all_rows.extend(result.rows)

        # Apply optional global per-importer limit (ctx.limit) after extraction
        limit = getattr(ctx, "limit", None)
        if limit:
            try:
                all_rows = all_rows[: int(limit)]
            except Exception:
                pass

        # 2. Build pipelines per-source by merging base schema pipelines with
        # any adapter-provided `PIPELINES`. This allows adapters to own
        # source-specific transform bindings while the importer runs them
        # uniformly.
        from boranga.components.data_migration.registry import (
            registry as transform_registry,
        )

        base_column_names = schema.COLUMN_PIPELINES or {}
        pipelines_by_source: dict[str, dict] = {}
        for src_key, adapter in SOURCE_ADAPTERS.items():
            src_column_names = dict(base_column_names)
            adapter_pipes = getattr(adapter, "PIPELINES", None)
            if adapter_pipes:
                src_column_names.update(adapter_pipes)

            built: dict[str, list] = {}
            for col, names in src_column_names.items():
                built[col] = transform_registry.build_pipeline(names)
            pipelines_by_source[src_key] = built

        # Build a `pipelines` mapping (keys only) used by merge_group to know
        # which canonical columns to consider when merging entries. Use the
        # union of columns across all source-specific pipelines.
        all_columns = set()
        for built in pipelines_by_source.values():
            all_columns.update(built.keys())
        if not all_columns and schema.COLUMN_PIPELINES:
            all_columns.update(schema.COLUMN_PIPELINES.keys())
        pipelines = {col: None for col in sorted(all_columns)}

        processed = 0
        errors = 0
        created = 0
        updated = 0
        skipped = 0
        warn_count = 0

        # 3. Transform every row into canonical form, collect per-key groups
        groups: dict[str, list[tuple[dict, str, list[tuple[str, Any]]]]] = defaultdict(
            list
        )
        # groups[migrated_from_id] -> list of (transformed_dict, source, issues_list)

        for row in all_rows:
            processed += 1
            # progress output every 500 rows
            if processed % 500 == 0:
                logger.info(
                    "OccurrenceImporter %s: processed %d rows so far",
                    self.slug,
                    processed,
                )
            tcx = TransformContext(row=row, model=None, user_id=ctx.user_id)
            issues = []
            transformed = {}
            has_error = False
            # Choose pipeline map based on the row source (fallback to base)
            src = row.get("_source")
            pipeline_map = pipelines_by_source.get(
                src, pipelines_by_source.get(None, {})
            )
            for col, pipeline in pipeline_map.items():
                raw_val = row.get(col)
                res = run_pipeline(pipeline, raw_val, tcx)
                transformed[col] = res.value
                for issue in res.issues:
                    issues.append((col, issue))
                    level = getattr(issue, "level", "error")
                    record = {
                        "migrated_from_id": row.get("migrated_from_id"),
                        "column": col,
                        "level": level,
                        "message": getattr(issue, "message", str(issue)),
                        "raw_value": raw_val,
                    }
                    if level == "error":
                        has_error = True
                        errors += 1
                        errors_details.append(record)
                    else:
                        warn_count += 1
                        warnings_details.append(record)
            if has_error:
                skipped += 1
                continue

            # copy adapter-added keys (e.g. group_type_id) from the source row into
            # the transformed dict so they survive the merge. Skip internals.
            for k, v in row.items():
                if k.startswith("_"):
                    continue
                if k in transformed:
                    continue
                transformed[k] = v

            key = transformed.get("migrated_from_id")
            if not key:
                # missing key — cannot merge/persist
                skipped += 1
                errors += 1
                errors_details.append(
                    {"reason": "missing_migrated_from_id", "row": transformed}
                )
                continue
            groups[key].append((transformed, row.get("_source"), issues))

        # 4. Merge groups and persist one object per key
        def merge_group(entries, source_priority):
            """
            Merge canonical columns (from schema pipelines) and also preserve any
            adapter-added keys found in transformed dicts (first non-empty wins).
            """
            entries_sorted = sorted(
                entries,
                key=lambda e: (
                    source_priority.index(e[1])
                    if e[1] in source_priority
                    else len(source_priority)
                ),
            )
            merged = {}
            combined_issues = []
            # first merge the canonical columns defined by the schema/pipelines
            for col in pipelines.keys():
                val = None
                for trans, src, _ in entries_sorted:
                    v = trans.get(col)
                    if v not in (None, ""):
                        val = v
                        break
                merged[col] = val
            # also merge any adapter-added keys that are present in transformed dicts
            extra_keys = set().union(
                *(set(trans.keys()) for trans, _, _ in entries_sorted)
            )
            for extra in sorted(extra_keys):
                if extra in pipelines:
                    continue
                val = None
                for trans, src, _ in entries_sorted:
                    v = trans.get(extra)
                    if v not in (None, ""):
                        val = v
                        break
                merged[extra] = val
            for _, _, iss in entries_sorted:
                combined_issues.extend(iss)
            return merged, combined_issues

        # Persist merged rows in bulk where possible (prepare ops then create/update)

        # Pre-fetch TEC submitter if needed
        tec_submitter_id = None
        try:
            tec_user = EmailUserRO.objects.get(email="boranga.tec@dbca.wa.gov.au")
            tec_submitter_id = tec_user.id
        except Exception:
            logger.warning(
                "EmailUser 'boranga.tec@dbca.wa.gov.au' not found. TEC occurrences will have no submitter."
            )

        ops = []
        for migrated_from_id, entries in groups.items():
            merged, combined_issues = merge_group(entries, sources)
            # if any error in combined_issues => skip
            if any(i.level == "error" for _, i in combined_issues):
                skipped += 1
                continue

            involved_sources = sorted({src for _, src, _ in entries})
            occ_row = schema.OccurrenceRow.from_dict(merged)
            source_for_validation = (
                involved_sources[0] if len(involved_sources) == 1 else None
            )
            validation_issues = occ_row.validate(source=source_for_validation)
            if validation_issues:
                for level, msg in validation_issues:
                    rec = {
                        "migrated_from_id": merged.get("migrated_from_id"),
                        "reason": "validation",
                        "level": level,
                        "message": msg,
                        "row": merged,
                    }
                    if level == "error":
                        errors_details.append(rec)
                    else:
                        warnings_details.append(rec)
                if any(level == "error" for level, _ in validation_issues):
                    skipped += 1
                    errors += sum(
                        1 for level, _ in validation_issues if level == "error"
                    )
                    continue

            # QA check: occurrence_name must be unique for the same species
            if occ_row.occurrence_name and occ_row.species_id:
                dup_exists = (
                    Occurrence.objects.filter(
                        species_id=occ_row.species_id,
                        occurrence_name=occ_row.occurrence_name,
                    )
                    .exclude(migrated_from_id=migrated_from_id)
                    .exists()
                )
                if dup_exists:
                    rec = {
                        "migrated_from_id": merged.get("migrated_from_id"),
                        "reason": "duplicate_occurrence_name",
                        "level": "error",
                        "message": (
                            f"occurrence_name '{occ_row.occurrence_name}' "
                            f"already exists for species {occ_row.species_id}"
                        ),
                        "row": merged,
                    }
                    errors_details.append(rec)
                    skipped += 1
                    errors += 1
                    continue

            defaults = occ_row.to_model_defaults()

            # If submitter is missing and this is a TEC record, use the TEC submitter
            if not defaults.get("submitter") and Source.TEC.value in involved_sources:
                if tec_submitter_id:
                    defaults["submitter"] = tec_submitter_id

            defaults["lodgement_date"] = merged.get("datetime_created")
            if merged.get("locked") is not None:
                defaults["locked"] = merged.get("locked")

            # If transforms produced None for fields that have model defaults
            # (for example CharFields with default=''), prefer the model's
            # default value. This keeps transforms simple (they can return
            # None) while avoiding validation failures for non-nullable
            # fields that expect a non-None default like an empty string or
            # choice fields like review_status.
            for k, v in list(defaults.items()):
                if v is not None:
                    continue
                try:
                    field = Occurrence._meta.get_field(k)
                except FieldDoesNotExist:
                    continue
                # Prefer explicit field default (handles callables)
                field_default = field.get_default()
                if field_default is not None:
                    defaults[k] = field_default
                    continue
                # Fallback: for non-nullable text fields, prefer empty string
                if not getattr(field, "null", False) and isinstance(
                    field, (dj_models.CharField, dj_models.TextField)
                ):
                    defaults[k] = ""
                    continue

            # If dry-run, log planned defaults and skip adding to ops so no DB work
            if ctx.dry_run:
                pretty = json.dumps(defaults, default=str, indent=2, sort_keys=True)
                logger.debug(
                    "OccurrenceImporter %s dry-run: would persist migrated_from_id=%s defaults:\n%s",
                    self.slug,
                    migrated_from_id,
                    pretty,
                )
                continue

            ops.append(
                {
                    "migrated_from_id": migrated_from_id,
                    "defaults": defaults,
                    "merged": merged,
                }
            )

        # Note: do not exit early on dry-run here — continue so error CSV is generated

        # Determine existing occurrences and plan create vs update
        migrated_keys = [o["migrated_from_id"] for o in ops]
        existing_by_migrated = {
            s.migrated_from_id: s
            for s in Occurrence.objects.filter(migrated_from_id__in=migrated_keys)
        }

        to_create = []
        create_meta = []
        to_update = []
        BATCH = 1000

        for op in ops:
            migrated_from_id = op["migrated_from_id"]
            defaults = op["defaults"]
            merged = op.get("merged") or {}

            obj = existing_by_migrated.get(migrated_from_id)
            if obj:
                for k, v in defaults.items():
                    apply_value_to_instance(obj, k, v)
                to_update.append(obj)
                continue

            create_kwargs = dict(defaults)
            create_kwargs["migrated_from_id"] = migrated_from_id
            if getattr(ctx, "migration_run", None) is not None:
                create_kwargs["migration_run"] = ctx.migration_run
            inst = Occurrence(**normalize_create_kwargs(Occurrence, create_kwargs))
            to_create.append(inst)
            create_meta.append(
                (
                    migrated_from_id,
                    merged.get("OCCContactDetail__contact"),
                    merged.get("OCCContactDetail__contact_name"),
                    merged.get("OCCContactDetail__notes"),
                    merged.get("modified_by"),
                    merged.get("datetime_updated"),
                )
            )

        # Bulk create new Occurrences
        created_map = {}
        if to_create:
            logger.info(
                "OccurrenceImporter: bulk-creating %d new Occurrences",
                len(to_create),
            )
            for i in range(0, len(to_create), BATCH):
                chunk = to_create[i : i + BATCH]
                with transaction.atomic():
                    Occurrence.objects.bulk_create(chunk, batch_size=BATCH)

        # Refresh created objects to get PKs
        if create_meta:
            created_keys = [m[0] for m in create_meta]
            for s in Occurrence.objects.filter(migrated_from_id__in=created_keys):
                created_map[s.migrated_from_id] = s

        # If migration_run present, ensure it's attached to created objects
        if created_map and getattr(ctx, "migration_run", None) is not None:
            try:
                Occurrence.objects.filter(
                    migrated_from_id__in=list(created_map.keys())
                ).update(migration_run=ctx.migration_run)
            except Exception:
                logger.exception(
                    "Failed to attach migration_run to some created Occurrence(s)"
                )

        # Ensure occurrence_number is populated for newly-created Occurrence objects.
        # The model normally sets this in save() as 'OCC' + pk, but bulk_create
        # bypasses save(), so we must set it here using the assigned PKs.
        if created_map:
            occs_to_update = []
            for occ in created_map.values():
                try:
                    if not getattr(occ, "occurrence_number", None):
                        occ.occurrence_number = f"OCC{occ.pk}"
                        occs_to_update.append(occ)
                except Exception:
                    logger.exception(
                        "Error preparing occurrence_number for Occurrence %s",
                        getattr(occ, "pk", None),
                    )
            if occs_to_update:
                try:
                    Occurrence.objects.bulk_update(
                        occs_to_update, ["occurrence_number"], batch_size=BATCH
                    )
                except Exception:
                    logger.exception(
                        "Failed to bulk_update occurrence_number; falling back to individual saves"
                    )
                    for occ in occs_to_update:
                        try:
                            occ.save(update_fields=["occurrence_number"])
                        except Exception:
                            logger.exception(
                                "Failed to save occurrence_number for Occurrence %s",
                                getattr(occ, "pk", None),
                            )

        # Bulk update existing objects
        if to_update:
            logger.info(
                "OccurrenceImporter: bulk-updating %d existing Occurrences",
                len(to_update),
            )
            update_instances = to_update
            # determine fields to update: include only fields that are
            # non-None on every instance. Using the union (fields present on
            # some instances) can cause bulk_update to write NULL into rows
            # for instances where the attribute is None, which violates NOT
            # NULL constraints. Restricting to fields present on all instances
            # avoids that. Also exclude id and migrated_from_id which cannot be updated.
            fields = []
            if update_instances:
                all_fields = [f for f in update_instances[0]._meta.fields]
                for f in all_fields:
                    if f.name in ("id", "migrated_from_id"):
                        continue
                    # include field only if every instance has a non-None value
                    try:
                        if all(
                            getattr(inst, f.name, None) is not None
                            for inst in update_instances
                        ):
                            fields.append(f.name)
                    except Exception:
                        # Be conservative: skip fields that raise on getattr
                        continue
            # perform bulk_update only if we have safe fields to update
            try:
                if fields:
                    Occurrence.objects.bulk_update(
                        update_instances, fields, batch_size=BATCH
                    )
            except Exception:
                logger.exception(
                    "Failed to bulk_update Occurrence; falling back to individual saves"
                )
                for inst in update_instances:
                    try:
                        # Build a conservative per-instance update_fields list:
                        # include only model fields that currently have a non-None
                        # value on the instance. This avoids attempting to write
                        # NULL into non-nullable DB columns when the instance
                        # attribute is None. Also exclude id and migrated_from_id.
                        update_fields = [
                            f.name
                            for f in inst._meta.fields
                            if getattr(inst, f.name, None) is not None
                            and f.name not in ("id", "migrated_from_id")
                        ]
                        if update_fields:
                            inst.save(update_fields=update_fields)
                        else:
                            # Nothing to update (all values are None or only PK), skip
                            logger.debug(
                                "Skipping save for Occurrence %s: no updatable fields",
                                getattr(inst, "pk", None),
                            )
                    except Exception:
                        logger.exception(
                            "Failed to save Occurrence %s",
                            getattr(inst, "pk", None),
                        )

        # Now create contact details for created/updated occurrences when provided
        target_mig_ids = [o["migrated_from_id"] for o in ops]
        target_occs = list(
            Occurrence.objects.filter(migrated_from_id__in=target_mig_ids)
        )
        target_map = {o.migrated_from_id: o for o in target_occs}

        existing_contacts = set(
            OCCContactDetail.objects.filter(occurrence__in=target_occs).values_list(
                "occurrence_id", flat=True
            )
        )
        want_contact_create = []
        # created_meta contains tuples for created ones; for updates use merged from ops
        # create_meta may include extra fields (modified_by, datetime_updated) so ignore extras here
        for mig, contact, contact_name, notes, *rest in create_meta:
            occ = created_map.get(mig)
            if not occ:
                continue
            if occ.pk in existing_contacts:
                continue
            if contact or contact_name or notes:
                want_contact_create.append(
                    OCCContactDetail(
                        occurrence=occ,
                        contact=contact or "",
                        contact_name=contact_name or "",
                        notes=notes or "",
                        visible=True,
                    )
                )

        # also handle updates (ops where occurrence existed)
        for op in ops:
            mig = op["migrated_from_id"]
            merged = op.get("merged") or {}
            occ = target_map.get(mig)
            if not occ:
                continue
            if occ.pk in existing_contacts:
                continue
            if (
                merged.get("OCCContactDetail__contact")
                or merged.get("OCCContactDetail__contact_name")
                or merged.get("OCCContactDetail__notes")
            ):
                want_contact_create.append(
                    OCCContactDetail(
                        occurrence=occ,
                        contact=merged.get("OCCContactDetail__contact") or "",
                        contact_name=merged.get("OCCContactDetail__contact_name") or "",
                        notes=merged.get("OCCContactDetail__notes") or "",
                        visible=True,
                    )
                )

        if want_contact_create:
            try:
                OCCContactDetail.objects.bulk_create(
                    want_contact_create, batch_size=BATCH
                )
            except Exception:
                logger.exception(
                    "Failed to bulk_create OCCContactDetail; falling back to individual creates"
                )
                for obj in want_contact_create:
                    try:
                        obj.save()
                    except Exception:
                        logger.exception(
                            "Failed to create OCCContactDetail for occurrence %s",
                            getattr(obj.occurrence, "pk", None),
                        )

        # Create an OccurrenceUserAction for created/updated occurrences (one per occurrence)
        from boranga.components.occurrence.models import OccurrenceUserAction

        existing_actions = set(
            OccurrenceUserAction.objects.filter(occurrence__in=target_occs).values_list(
                "occurrence_id", flat=True
            )
        )

        want_action_create = []
        # created_meta contains tuples for created ones; tuples extended with modified_by and datetime_updated
        for tpl in create_meta:
            # tpl = (migrated_from_id, contact, contact_name, notes, modified_by, datetime_updated)
            mig = tpl[0]
            modified_by = tpl[4]
            datetime_updated = tpl[5]
            occ = created_map.get(mig)
            if not occ:
                continue
            if occ.pk in existing_actions:
                continue
            if modified_by and datetime_updated:
                want_action_create.append(
                    OccurrenceUserAction(
                        occurrence=occ,
                        what="Edited in TPFL",
                        when=datetime_updated,
                        who=modified_by,
                    )
                )

        # also create for updates (ops where occurrence existed)
        for op in ops:
            mig = op["migrated_from_id"]
            merged = op.get("merged") or {}
            occ = target_map.get(mig)
            if not occ:
                continue
            if occ.pk in existing_actions:
                continue
            modified_by = merged.get("modified_by")
            datetime_updated = merged.get("datetime_updated")
            if modified_by and datetime_updated:
                want_action_create.append(
                    OccurrenceUserAction(
                        occurrence=occ,
                        what="Edited in TPFL",
                        when=datetime_updated,
                        who=modified_by,
                    )
                )

        if want_action_create:
            try:
                OccurrenceUserAction.objects.bulk_create(
                    want_action_create, batch_size=BATCH
                )
            except Exception:
                logger.exception(
                    "Failed to bulk_create OccurrenceUserAction; falling back to individual creates"
                )
                for obj in want_action_create:
                    try:
                        obj.save()
                    except Exception:
                        logger.exception(
                            "Failed to create OccurrenceUserAction for occurrence %s",
                            getattr(obj.occurrence, "pk", None),
                        )

        # Create related objects for TEC data
        # We iterate through ops again. If the op has TEC-specific fields, we create/update the related models.
        # Note: This is a simplified approach. Ideally we would bulk create these too.
        # Given the complexity and number of related models, individual creation/update per
        # occurrence might be safer/easier to implement first.

        # Helper to get occurrence object
        def get_occ(mig_id):
            return created_map.get(mig_id) or target_map.get(mig_id)

        # Load DocumentCategory "ORF Document"
        from boranga.components.species_and_communities.models import DocumentCategory

        try:
            orf_document_category = DocumentCategory.objects.get(
                document_category_name="ORF Document"
            )
        except DocumentCategory.DoesNotExist:
            logger.warning(
                "DocumentCategory 'ORF Document' not found. OccurrenceDocuments will be created without category."
            )
            orf_document_category = None

        for op in ops:
            mig = op["migrated_from_id"]
            merged = op.get("merged") or {}
            occ = get_occ(mig)
            if not occ:
                continue

            # OCCLocation
            if any(k.startswith("OCCLocation__") for k in merged):
                OCCLocation.objects.update_or_create(
                    occurrence=occ,
                    defaults={
                        "coordinate_source_id": merged.get(
                            "OCCLocation__coordinate_source_id"
                        ),
                        "boundary_description": merged.get(
                            "OCCLocation__boundary_description"
                        ),
                        "locality": merged.get("OCCLocation__locality"),
                        "location_description": merged.get(
                            "OCCLocation__location_description"
                        ),
                    },
                )

            # OccurrenceSite
            if any(k.startswith("OccurrenceSite__") for k in merged):
                # We need to handle geometry creation here if not done in adapter
                # Adapter does it: OccurrenceSite__geometry
                site, created = OccurrenceSite.objects.update_or_create(
                    occurrence=occ,
                    defaults={
                        "site_name": merged.get("OccurrenceSite__site_name"),
                        "comments": merged.get("OccurrenceSite__comments"),
                        "geometry": merged.get("OccurrenceSite__geometry"),
                    },
                )
                # Manually update updated_date to bypass auto_now=True from GeometryBase
                if merged.get("OccurrenceSite__updated_date"):
                    OccurrenceSite.objects.filter(pk=site.pk).update(
                        updated_date=merged.get("OccurrenceSite__updated_date")
                    )

            # OCCObservationDetail
            if any(k.startswith("OCCObservationDetail__") for k in merged):
                OCCObservationDetail.objects.update_or_create(
                    occurrence=occ,
                    defaults={
                        "comments": merged.get("OCCObservationDetail__comments"),
                    },
                )

            # OCCHabitatComposition
            if any(k.startswith("OCCHabitatComposition__") for k in merged):
                OCCHabitatComposition.objects.update_or_create(
                    occurrence=occ,
                    defaults={
                        "water_quality": merged.get(
                            "OCCHabitatComposition__water_quality"
                        ),
                        "habitat_notes": merged.get(
                            "OCCHabitatComposition__habitat_notes"
                        ),
                    },
                )

            # OCCFireHistory
            if any(k.startswith("OCCFireHistory__") for k in merged):
                OCCFireHistory.objects.update_or_create(
                    occurrence=occ,
                    defaults={
                        "comment": merged.get("OCCFireHistory__comment"),
                    },
                )

            # OCCAssociatedSpecies
            if any(k.startswith("OCCAssociatedSpecies__") for k in merged):
                OCCAssociatedSpecies.objects.update_or_create(
                    occurrence=occ,
                    defaults={
                        "comment": merged.get("OCCAssociatedSpecies__comment"),
                    },
                )

            # OccurrenceDocument
            # Task 12278: Default Value = ORF Document (DocumentCategory)
            # Task 12279: document_sub_category (DocumentSubCategory)
            if any(k.startswith("OccurrenceDocument__") for k in merged):
                OccurrenceDocument.objects.update_or_create(
                    occurrence=occ,
                    defaults={
                        "document_sub_category_id": merged.get(
                            "OccurrenceDocument__document_sub_category_id"
                        ),
                        "description": merged.get("OccurrenceDocument__description"),
                        "document_category": orf_document_category,
                    },
                )

        # Update stats counts for created/updated based on performed ops
        created += len(created_map)
        updated += len(to_update)

        stats.update(
            processed=processed,
            created=created,
            updated=updated,
            skipped=skipped,
            errors=errors,
            warnings=warn_count,
        )
        # Attach lightweight error/warning details and write CSV if needed
        stats["error_count_details"] = len(errors_details)
        stats["warning_count_details"] = len(warnings_details)
        stats["warning_messages"] = warnings
        stats["error_details_csv"] = None

        elapsed = timezone.now() - start_time
        stats["time_taken"] = str(elapsed)

        if errors_details:
            csv_path = options.get("error_csv")
            if csv_path:
                csv_path = os.path.abspath(csv_path)
            else:
                ts = timezone.now().strftime("%Y%m%d_%H%M%S")
                csv_path = os.path.join(
                    os.getcwd(),
                    "private-media/handler_output",
                    f"{self.slug}_errors_{ts}.csv",
                )
            try:
                os.makedirs(os.path.dirname(csv_path), exist_ok=True)
                with open(csv_path, "w", newline="", encoding="utf-8") as fh:
                    fieldnames = [
                        "migrated_from_id",
                        "column",
                        "level",
                        "message",
                        "raw_value",
                        "reason",
                        "row_json",
                        "timestamp",
                    ]
                    import csv

                    writer = csv.DictWriter(fh, fieldnames=fieldnames)
                    writer.writeheader()
                    for rec in errors_details:
                        writer.writerow(
                            {
                                "migrated_from_id": rec.get("migrated_from_id"),
                                "column": rec.get("column"),
                                "level": rec.get("level"),
                                "message": rec.get("message"),
                                "raw_value": rec.get("raw_value"),
                                "reason": rec.get("reason"),
                                "row_json": json.dumps(rec.get("row", ""), default=str),
                                "timestamp": timezone.now().isoformat(),
                            }
                        )
                stats["error_details_csv"] = csv_path
                logger.info(
                    (
                        "OccurrenceImporter %s finished; processed=%d created=%d "
                        "updated=%d skipped=%d errors=%d warnings=%d time_taken=%s (details -> %s)"
                    ),
                    self.slug,
                    processed,
                    created,
                    updated,
                    skipped,
                    errors,
                    warn_count,
                    str(elapsed),
                    csv_path,
                )
            except Exception as e:
                logger.error(
                    "Failed to write error CSV for %s at %s: %s", self.slug, csv_path, e
                )
                logger.info(
                    (
                        "OccurrenceImporter %s finished; processed=%d created=%d "
                        "updated=%d skipped=%d errors=%d warnings=%d time_taken=%s"
                    ),
                    self.slug,
                    processed,
                    created,
                    updated,
                    skipped,
                    errors,
                    warn_count,
                    str(elapsed),
                )

        return stats
