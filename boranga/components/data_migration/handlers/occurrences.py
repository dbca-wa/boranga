from __future__ import annotations

import argparse
import json
import logging
import os
from collections import defaultdict
from typing import Any

from django.db import transaction
from django.utils import timezone

from boranga.components.data_migration.adapters.occurrence import (  # shared canonical schema
    schema,
)
from boranga.components.data_migration.adapters.occurrence.tec import (
    OccurrenceTecAdapter,
    tec_site_geometry_transform,
)
from boranga.components.data_migration.adapters.occurrence.tec_boundaries import (
    OccurrenceTecBoundariesAdapter,
)
from boranga.components.data_migration.adapters.occurrence.tpfl import (
    OccurrenceTpflAdapter,
)
from boranga.components.data_migration.adapters.sources import Source
from boranga.components.data_migration.handlers.helpers import (
    apply_model_defaults,
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
    OCCHabitatCondition,
    OCCIdentification,
    OCCLocation,
    OCCObservationDetail,
    Occurrence,
    OccurrenceDocument,
    OccurrenceGeometry,
    OccurrenceReport,
    OccurrenceSite,
    OccurrenceUserAction,
    OCCVegetationStructure,
    SpeciesRole,
)
from boranga.components.species_and_communities.models import Taxonomy

logger = logging.getLogger(__name__)

SOURCE_ADAPTERS = {
    Source.TPFL.value: OccurrenceTpflAdapter(),
    Source.TEC.value: OccurrenceTecAdapter(),
    Source.TEC_BOUNDARIES.value: OccurrenceTecBoundariesAdapter(),
}


@register
class OccurrenceImporter(BaseSheetImporter):
    """
    Example import commands for different data sources:
        ./manage.py migrate_data run occurrence_legacy \
            private-media/legacy_data/TPFL/DRF_POPULATION.csv --sources TPFL \
            --limit 10 --dry-run

        ./manage.py migrate_data run occurrence_legacy \
            private-media/legacy_data/TEC/ --sources TEC \
            --wipe-targets

    IMPLEMENTATION NOTES / TODOs:
    - Task 12177/12180 (OCCLocation.district/region): Uses DISTRICTS.csv + CALM_DISTRICTS.csv +
      CALM_REGIONS.csv chain. Requires LegacyValueMap pre-populated. Verify with S&C team.
    - Task 12225 (OCCIdentification.identification_certainty): Maps OCC_STATUS_CODE
      "Identified"→"High", "Believed"→"Medium". Confirm complete mapping with S&C team.
    - Task 12287 (OccurrenceDocument.uploaded_by): USERNAME column renamed to ADD_USERNAME
      in ADDITIONAL_DATA to avoid collision with SITES USERNAME. Review if better approach exists.
    """

    slug = "occurrence_legacy"
    description = "Import occurrence data from legacy TEC / TFAUNA / TPFL sources"

    def clear_targets(self, ctx: ImportContext, include_children: bool = False, **options):
        """Delete Occurrence target data and obvious children. Respect `ctx.dry_run`."""
        if ctx.dry_run:
            logger.info("OccurrenceImporter.clear_targets: dry-run, skipping delete")
            return

        from boranga.components.data_migration.adapters.sources import (
            SOURCE_GROUP_TYPE_MAP,
        )

        sources = options.get("sources")
        target_group_types = set()
        if sources:
            for s in sources:
                if s in SOURCE_GROUP_TYPE_MAP:
                    target_group_types.add(SOURCE_GROUP_TYPE_MAP[s])

        # If specific sources are requested, filter deletions by group_type.
        is_filtered = bool(sources)

        occ_filter = {}
        report_filter = {}
        rel_filter = {}

        if is_filtered:
            if not target_group_types:
                logger.warning(
                    "clear_targets: sources %s provided but no associated group_types found in map. Skipping delete.",
                    sources,
                )
                return

            if len(sources) == 1 and sources[0] == Source.TEC_BOUNDARIES.value:
                logger.warning("OccurrenceImporter: wiping targets for TEC_BOUNDARIES (OccurrenceGeometry only)...")
                OccurrenceGeometry.objects.filter(occurrence__group_type__name__in=target_group_types).delete()
                return

            logger.warning(
                "OccurrenceImporter: deleting Occurrence and related data for group_types: %s ...",
                target_group_types,
            )
            occ_filter = {"group_type__name__in": target_group_types}
            report_filter = {"group_type__name__in": target_group_types}
            rel_filter = {"occurrence__group_type__name__in": target_group_types}
        else:
            logger.warning("OccurrenceImporter: deleting ALL Occurrence and related data...")

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
                # Delete OccurrenceReport objects first as they depend on Occurrences
                OccurrenceReport.objects.filter(**report_filter).delete()

                # Nullify self-reference that blocks deletion of Occurrence
                Occurrence.objects.filter(**occ_filter).update(combined_occurrence=None)

                relations = [
                    OCCContactDetail,
                    OCCLocation,
                    OccurrenceSite,
                    OCCObservationDetail,
                    OCCHabitatComposition,
                    OCCFireHistory,
                    OCCAssociatedSpecies,
                    OccurrenceDocument,
                    OCCIdentification,
                    OCCHabitatCondition,
                    OCCVegetationStructure,
                    OccurrenceGeometry,
                ]
                for model in relations:
                    model.objects.filter(**rel_filter).delete()

                if not is_filtered:
                    AssociatedSpeciesTaxonomy.objects.all().delete()

            except Exception:
                logger.exception("Failed to delete related Occurrence data")
            try:
                Occurrence.objects.filter(**occ_filter).delete()
            except Exception:
                logger.exception("Failed to delete Occurrence")

        finally:
            if not was_autocommit:
                conn.set_autocommit(False)

        # Reset the primary key sequence for Occurrence and OccurrenceReport when using PostgreSQL.
        try:
            if getattr(conn, "vendor", None) == "postgresql":
                for model in [Occurrence, OccurrenceReport]:
                    table = model._meta.db_table
                    with conn.cursor() as cur:
                        cur.execute(f"SELECT MAX(id) FROM {table}")
                        row = cur.fetchone()
                        max_id = row[0] if row else None

                        if max_id is not None:
                            cur.execute(
                                "SELECT setval(pg_get_serial_sequence(%s, %s), %s, %s)",
                                [table, "id", max_id, True],
                            )
                        else:
                            cur.execute(
                                "SELECT setval(pg_get_serial_sequence(%s, %s), %s, %s)",
                                [table, "id", 1, False],
                            )
                    logger.info("Reset primary key sequence for table %s to %s", table, max_id)
        except Exception:
            logger.exception("Failed to reset primary key sequences")

    def add_arguments(self, parser):
        parser.add_argument(
            "--path-map",
            nargs="+",
            metavar="SRC=PATH",
            help="Per-source path overrides (e.g. TPFL=/tmp/tpfl.xlsx). If omitted, --path is reused.",
        )
        try:
            parser.add_argument(
                "--sources",
                nargs="+",
                choices=list(SOURCE_ADAPTERS.keys()),
                help="Subset of sources (default: all implemented)",
            )
        except argparse.ArgumentError:
            # Already added by management command
            pass

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
        groups: dict[str, list[tuple[dict, str, list[tuple[str, Any]]]]] = defaultdict(list)
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
            # Adapter provides canonical row (already mapped from raw CSV)
            canonical_row = row

            tcx = TransformContext(row=canonical_row, model=None, user_id=ctx.user_id)
            issues = []
            transformed = {}
            has_error = False
            # Choose pipeline map based on the row source (fallback to base)
            src = row.get("_source")
            pipeline_map = pipelines_by_source.get(src, pipelines_by_source.get(None, {}))
            for col, pipeline in pipeline_map.items():
                raw_val = canonical_row.get(col)
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
            # the transformed dict so they survive the merge. Also preserve _nested_*
            # keys which hold related model data, and _source for pipeline selection.
            for k, v in row.items():
                if k.startswith("_"):
                    # Preserve _nested_* keys and _source, skip other internals
                    if not (k.startswith("_nested_") or k == "_source"):
                        continue
                if k in transformed:
                    continue
                transformed[k] = v

            # Prepend source prefix to migrated_from_id if present
            _src = row.get("_source")
            _mid = transformed.get("migrated_from_id")
            if _src and _mid:
                if _src == "TEC_BOUNDARIES":
                    # Special case: TEC_BOUNDARIES links to TEC records, so use 'tec-' prefix
                    prefix = "tec"
                else:
                    prefix = _src.lower().replace("_", "-")

                if not str(_mid).startswith(f"{prefix}-"):
                    transformed["migrated_from_id"] = f"{prefix}-{_mid}"

            key = transformed.get("migrated_from_id")
            if not key:
                # missing key — cannot merge/persist
                skipped += 1
                errors += 1
                errors_details.append({"reason": "missing_migrated_from_id", "row": transformed})
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
                key=lambda e: (source_priority.index(e[1]) if e[1] in source_priority else len(source_priority)),
            )
            merged = {}
            combined_issues = []
            # first merge the canonical columns defined by the schema/pipelines
            for col in pipelines.keys():
                val = None
                for trans, src, _ in entries_sorted:
                    v = trans.get(col)
                    # Accept empty string as a valid value (e.g., for text fields with defaults)
                    if v not in (None,):
                        val = v
                        break
                merged[col] = val
            # also merge any adapter-added keys that are present in transformed dicts
            extra_keys = set().union(*(set(trans.keys()) for trans, _, _ in entries_sorted))
            for extra in sorted(extra_keys):
                if extra in pipelines:
                    continue
                val = None
                for trans, src, _ in entries_sorted:
                    v = trans.get(extra)
                    # Accept empty string as a valid value
                    if v not in (None,):
                        val = v
                        break
                merged[extra] = val
            for _, _, iss in entries_sorted:
                combined_issues.extend(iss)
            return merged, combined_issues

        # Persist merged rows in bulk where possible (prepare ops then create/update)

        ops = []
        for migrated_from_id, entries in groups.items():
            merged, combined_issues = merge_group(entries, sources)
            # if any error in combined_issues => skip
            if any(i.level == "error" for _, i in combined_issues):
                skipped += 1
                continue

            involved_sources = sorted({src for _, src, _ in entries})
            occ_row = schema.OccurrenceRow.from_dict(merged)
            source_for_validation = involved_sources[0] if len(involved_sources) == 1 else None
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
                    errors += sum(1 for level, _ in validation_issues if level == "error")
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

            # Apply model defaults (handles None -> "" for non-nullable text fields, etc.)
            apply_model_defaults(Occurrence, defaults)

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
                    "sources": list(involved_sources),
                }
            )

        # Note: do not exit early on dry-run here — continue so error CSV is generated

        # Determine existing occurrences and plan create vs update
        migrated_keys = [o["migrated_from_id"] for o in ops]
        existing_by_migrated = {
            s.migrated_from_id: s for s in Occurrence.objects.filter(migrated_from_id__in=migrated_keys)
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

            # Check if we should skip creation for boundary-only sources
            involved_sources = op.get("sources", [])
            if len(involved_sources) == 1 and Source.TEC_BOUNDARIES.value in involved_sources:
                logger.warning(
                    "Skipping creation of Occurrence %s (found in TEC_BOUNDARIES but not in primary TEC source)",
                    migrated_from_id,
                )
                errors_details.append(
                    {
                        "migrated_from_id": migrated_from_id,
                        "reason": "missing_primary_record",
                        "level": "error",
                        "message": "Occurrence found in TEC_BOUNDARIES but not in primary TEC source",
                        "row": merged,
                    }
                )
                skipped += 1
                errors += 1
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
                Occurrence.objects.filter(migrated_from_id__in=list(created_map.keys())).update(
                    migration_run=ctx.migration_run
                )
            except Exception:
                logger.exception("Failed to attach migration_run to some created Occurrence(s)")

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
                    Occurrence.objects.bulk_update(occs_to_update, ["occurrence_number"], batch_size=BATCH)
                except Exception:
                    logger.exception("Failed to bulk_update occurrence_number; falling back to individual saves")
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
                        if all(getattr(inst, f.name, None) is not None for inst in update_instances):
                            fields.append(f.name)
                    except Exception:
                        # Be conservative: skip fields that raise on getattr
                        continue
            # perform bulk_update only if we have safe fields to update
            try:
                if fields:
                    Occurrence.objects.bulk_update(update_instances, fields, batch_size=BATCH)
            except Exception:
                logger.exception("Failed to bulk_update Occurrence; falling back to individual saves")
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
                            if getattr(inst, f.name, None) is not None and f.name not in ("id", "migrated_from_id")
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

        # Process related objects in chunks to avoid massive SQL queries
        RELATED_BATCH_SIZE = 1000
        total_ops = len(ops)

        # Load DocumentCategory "ORF Document" once
        from boranga.components.species_and_communities.models import DocumentCategory

        try:
            orf_document_category = DocumentCategory.objects.get(document_category_name="ORF Document")
        except DocumentCategory.DoesNotExist:
            logger.warning(
                "DocumentCategory 'ORF Document' not found. OccurrenceDocuments will be created without category."
            )
            orf_document_category = None

        # Load ContentType for Occurrence (used by OccurrenceGeometry.content_type)
        from django.contrib.contenttypes.models import ContentType

        try:
            occ_content_type = ContentType.objects.get_for_model(Occurrence)
        except Exception:
            logger.warning("Could not resolve ContentType for Occurrence model")
            occ_content_type = None

        # community_group_type_id = get_group_type_id(GroupType.GROUP_TYPE_COMMUNITY)

        logger.info(
            "OccurrenceImporter: processing related objects in chunks (total %d ops)...",
            total_ops,
        )

        for i in range(0, total_ops, RELATED_BATCH_SIZE):
            chunk_ops = ops[i : i + RELATED_BATCH_SIZE]

            logger.info("Processing related objects: %d / %d ...", i + len(chunk_ops), total_ops)

            chunk_mig_ids = [o["migrated_from_id"] for o in chunk_ops]

            # Fetch occurrences for this chunk
            chunk_occs = list(Occurrence.objects.filter(migrated_from_id__in=chunk_mig_ids))
            chunk_occ_map = {o.migrated_from_id: o for o in chunk_occs}
            chunk_occ_ids = [o.pk for o in chunk_occs]

            # Helper
            def get_chunk_occ(mig_id):
                return chunk_occ_map.get(mig_id)

            # --- OCCContactDetail ---
            existing_contacts = set()
            if not getattr(ctx, "wipe_targets", False):
                existing_contacts = set(
                    OCCContactDetail.objects.filter(occurrence_id__in=chunk_occ_ids).values_list(
                        "occurrence_id", flat=True
                    )
                )

            want_contact_create = []

            for op in chunk_ops:
                mig = op["migrated_from_id"]
                merged = op.get("merged") or {}
                occ = get_chunk_occ(mig)
                if not occ:
                    continue

                if (
                    merged.get("OCCContactDetail__contact")
                    or merged.get("OCCContactDetail__contact_name")
                    or merged.get("OCCContactDetail__notes")
                ):
                    if occ.pk not in existing_contacts:
                        want_contact_create.append(
                            OCCContactDetail(
                                occurrence=occ,
                                contact=merged.get("OCCContactDetail__contact") or "",
                                contact_name=merged.get("OCCContactDetail__contact_name") or "",
                                notes=merged.get("OCCContactDetail__notes") or "",
                                visible=True,
                            )
                        )
                        existing_contacts.add(occ.pk)

            if want_contact_create:
                try:
                    OCCContactDetail.objects.bulk_create(want_contact_create, batch_size=BATCH)
                except Exception:
                    logger.exception("Failed to bulk_create OCCContactDetail; falling back to individual creates")
                    for obj in want_contact_create:
                        try:
                            obj.save()
                        except Exception:
                            logger.exception("Failed to create OCCContactDetail")

            # --- OccurrenceUserAction ---
            existing_actions = set()
            if not getattr(ctx, "wipe_targets", False):
                existing_actions = set(
                    OccurrenceUserAction.objects.filter(occurrence_id__in=chunk_occ_ids).values_list(
                        "occurrence_id", flat=True
                    )
                )

            want_action_create = []

            for op in chunk_ops:
                mig = op["migrated_from_id"]
                merged = op.get("merged") or {}
                occ = get_chunk_occ(mig)
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
                    OccurrenceUserAction.objects.bulk_create(want_action_create, batch_size=BATCH)
                except Exception:
                    logger.exception("Failed to bulk_create OccurrenceUserAction; falling back to individual creates")
                    for obj in want_action_create:
                        try:
                            obj.save()
                        except Exception:
                            logger.exception("Failed to create OccurrenceUserAction")

            # --- Related Models (OneToOne) ---
            loc_create, loc_update = [], []
            obs_create, obs_update = [], []
            hab_create, hab_update = [], []
            fire_create, fire_update = [], []
            assoc_create, assoc_update = [], []
            doc_create, doc_update = [], []
            geo_create, geo_update = [], []
            ident_create, ident_update = [], []
            veg_create, veg_update = [], []

            existing_locs = {}
            existing_obs = {}
            existing_hab = {}
            existing_fire = {}
            existing_assoc = {}
            existing_docs = {}
            existing_geo = {}
            existing_ident = {}
            existing_veg = {}

            if not getattr(ctx, "wipe_targets", False):
                existing_locs = {
                    loc.occurrence_id: loc for loc in OCCLocation.objects.filter(occurrence_id__in=chunk_occ_ids)
                }
                existing_obs = {
                    o.occurrence_id: o for o in OCCObservationDetail.objects.filter(occurrence_id__in=chunk_occ_ids)
                }
                existing_hab = {
                    h.occurrence_id: h for h in OCCHabitatComposition.objects.filter(occurrence_id__in=chunk_occ_ids)
                }
                existing_fire = {
                    f.occurrence_id: f for f in OCCFireHistory.objects.filter(occurrence_id__in=chunk_occ_ids)
                }
                existing_assoc = {
                    a.occurrence_id: a for a in OCCAssociatedSpecies.objects.filter(occurrence_id__in=chunk_occ_ids)
                }
                existing_docs = {
                    d.occurrence_id: d for d in OccurrenceDocument.objects.filter(occurrence_id__in=chunk_occ_ids)
                }
                existing_geo = {
                    g.occurrence_id: g for g in OccurrenceGeometry.objects.filter(occurrence_id__in=chunk_occ_ids)
                }
                existing_ident = {
                    i.occurrence_id: i for i in OCCIdentification.objects.filter(occurrence_id__in=chunk_occ_ids)
                }
                existing_veg = {
                    v.occurrence_id: v for v in OCCVegetationStructure.objects.filter(occurrence_id__in=chunk_occ_ids)
                }

            for op in chunk_ops:
                mig = op["migrated_from_id"]
                merged = op.get("merged") or {}
                occ = get_chunk_occ(mig)
                if not occ:
                    continue

                # OCCLocation
                if any(k.startswith("OCCLocation__") for k in merged):
                    defaults = {
                        "coordinate_source_id": merged.get("OCCLocation__coordinate_source_id"),
                        "boundary_description": merged.get("OCCLocation__boundary_description"),
                        "locality": merged.get("OCCLocation__locality"),
                        "location_description": merged.get("OCCLocation__location_description"),
                        "district_id": merged.get("OCCLocation__district_id"),
                        "region_id": merged.get("OCCLocation__region_id"),
                    }
                    apply_model_defaults(OCCLocation, defaults)
                    if occ.pk in existing_locs:
                        obj = existing_locs[occ.pk]
                        for k, v in defaults.items():
                            setattr(obj, k, v)
                        loc_update.append(obj)
                    else:
                        loc_create.append(OCCLocation(occurrence=occ, **defaults))

                # OCCObservationDetail
                if any(k.startswith("OCCObservationDetail__") for k in merged):
                    defaults = {"comments": merged.get("OCCObservationDetail__comments")}
                    apply_model_defaults(OCCObservationDetail, defaults)
                    if occ.pk in existing_obs:
                        obj = existing_obs[occ.pk]
                        for k, v in defaults.items():
                            setattr(obj, k, v)
                        obs_update.append(obj)
                    else:
                        obs_create.append(OCCObservationDetail(occurrence=occ, **defaults))

                # OCCHabitatComposition
                if any(k.startswith("OCCHabitatComposition__") for k in merged):
                    defaults = {
                        "water_quality": merged.get("OCCHabitatComposition__water_quality"),
                        "habitat_notes": merged.get("OCCHabitatComposition__habitat_notes"),
                    }
                    apply_model_defaults(OCCHabitatComposition, defaults)
                    if occ.pk in existing_hab:
                        obj = existing_hab[occ.pk]
                        for k, v in defaults.items():
                            setattr(obj, k, v)
                        hab_update.append(obj)
                    else:
                        hab_create.append(OCCHabitatComposition(occurrence=occ, **defaults))

                # OCCFireHistory
                if any(k.startswith("OCCFireHistory__") for k in merged):
                    defaults = {"comment": merged.get("OCCFireHistory__comment")}
                    apply_model_defaults(OCCFireHistory, defaults)
                    if occ.pk in existing_fire:
                        obj = existing_fire[occ.pk]
                        for k, v in defaults.items():
                            setattr(obj, k, v)
                        fire_update.append(obj)
                    else:
                        fire_create.append(OCCFireHistory(occurrence=occ, **defaults))

                # OCCAssociatedSpecies
                if any(k.startswith("OCCAssociatedSpecies__") for k in merged) or merged.get("_nested_species"):
                    defaults = {"comment": merged.get("OCCAssociatedSpecies__comment") or ""}
                    if occ.pk in existing_assoc:
                        obj = existing_assoc[occ.pk]
                        for k, v in defaults.items():
                            setattr(obj, k, v)
                        assoc_update.append(obj)
                    else:
                        new_obj = OCCAssociatedSpecies(occurrence=occ, **defaults)
                        assoc_create.append(new_obj)

                # OccurrenceGeometry
                geo_src = merged.get("_source")
                if geo_src != Source.TPFL.value and any(k.startswith("OccurrenceGeometry__") for k in merged):
                    defaults = {
                        "geometry": merged.get("OccurrenceGeometry__geometry"),
                        "locked": merged.get("OccurrenceGeometry__locked"),
                    }
                    if occ_content_type:
                        defaults["content_type"] = occ_content_type
                    apply_model_defaults(OccurrenceGeometry, defaults)
                    if occ.pk in existing_geo:
                        obj = existing_geo[occ.pk]
                        for k, v in defaults.items():
                            setattr(obj, k, v)
                        geo_update.append(obj)
                    else:
                        geo_create.append(OccurrenceGeometry(occurrence=occ, **defaults))

                # OccurrenceDocument
                # Check if we have actual data before creating/updating a document on the main row
                doc_sub = merged.get("OccurrenceDocument__document_sub_category_id")
                doc_desc = merged.get("OccurrenceDocument__description")

                if doc_sub is not None or doc_desc:
                    doc_uploaded_by = merged.get("OccurrenceDocument__uploaded_by")
                    defaults = {
                        "document_sub_category_id": doc_sub,
                        "description": doc_desc or "",
                        "document_category": orf_document_category,
                        "uploaded_by": doc_uploaded_by,
                    }
                    if occ.pk in existing_docs:
                        obj = existing_docs[occ.pk]
                        for k, v in defaults.items():
                            setattr(obj, k, v)
                        doc_update.append(obj)
                    else:
                        doc_create.append(OccurrenceDocument(occurrence=occ, **defaults))

                # OCCIdentification
                id_certainty = merged.get("OCCIdentification__identification_certainty_id")
                if id_certainty is not None:
                    defaults = {"identification_certainty_id": id_certainty}
                    apply_model_defaults(OCCIdentification, defaults)
                    if occ.pk in existing_ident:
                        obj = existing_ident[occ.pk]
                        for k, v in defaults.items():
                            setattr(obj, k, v)
                        ident_update.append(obj)
                    else:
                        ident_create.append(OCCIdentification(occurrence=occ, **defaults))

                # OCCVegetationStructure
                veg_layer_one = merged.get("OCCVegetationStructure__vegetation_structure_layer_one")
                if veg_layer_one:
                    defaults = {"vegetation_structure_layer_one": veg_layer_one}
                    apply_model_defaults(OCCVegetationStructure, defaults)
                    if occ.pk in existing_veg:
                        obj = existing_veg[occ.pk]
                        for k, v in defaults.items():
                            setattr(obj, k, v)
                        veg_update.append(obj)
                    else:
                        veg_create.append(OCCVegetationStructure(occurrence=occ, **defaults))

            # Execution
            if loc_create:
                OCCLocation.objects.bulk_create(loc_create, batch_size=BATCH)
            if loc_update:
                OCCLocation.objects.bulk_update(
                    loc_update,
                    [
                        "coordinate_source_id",
                        "boundary_description",
                        "locality",
                        "location_description",
                        "district_id",
                        "region_id",
                    ],
                    batch_size=BATCH,
                )

            if obs_create:
                OCCObservationDetail.objects.bulk_create(obs_create, batch_size=BATCH)
            if obs_update:
                OCCObservationDetail.objects.bulk_update(obs_update, ["comments"], batch_size=BATCH)

            if hab_create:
                OCCHabitatComposition.objects.bulk_create(hab_create, batch_size=BATCH)
            if hab_update:
                OCCHabitatComposition.objects.bulk_update(
                    hab_update, ["water_quality", "habitat_notes"], batch_size=BATCH
                )

            if fire_create:
                OCCFireHistory.objects.bulk_create(fire_create, batch_size=BATCH)
            if fire_update:
                OCCFireHistory.objects.bulk_update(fire_update, ["comment"], batch_size=BATCH)

            if assoc_create:
                OCCAssociatedSpecies.objects.bulk_create(assoc_create, batch_size=BATCH)
            if assoc_update:
                OCCAssociatedSpecies.objects.bulk_update(assoc_update, ["comment"], batch_size=BATCH)

            if doc_create:
                OccurrenceDocument.objects.bulk_create(doc_create, batch_size=BATCH)
            if doc_update:
                OccurrenceDocument.objects.bulk_update(
                    doc_update,
                    [
                        "document_sub_category_id",
                        "description",
                        "document_category",
                        "uploaded_by",
                    ],
                    batch_size=BATCH,
                )

            if geo_create:
                OccurrenceGeometry.objects.bulk_create(geo_create, batch_size=BATCH)
            if geo_update:
                OccurrenceGeometry.objects.bulk_update(
                    geo_update, ["geometry", "locked", "content_type"], batch_size=BATCH
                )

            if ident_create:
                OCCIdentification.objects.bulk_create(ident_create, batch_size=BATCH)
            if ident_update:
                OCCIdentification.objects.bulk_update(ident_update, ["identification_certainty_id"], batch_size=BATCH)

            if veg_create:
                OCCVegetationStructure.objects.bulk_create(veg_create, batch_size=BATCH)
            if veg_update:
                OCCVegetationStructure.objects.bulk_update(
                    veg_update, ["vegetation_structure_layer_one"], batch_size=BATCH
                )

            # Re-fetch OCCAssociatedSpecies for current chunk to get PKs
            if assoc_create:
                existing_assoc = {
                    a.occurrence_id: a for a in OCCAssociatedSpecies.objects.filter(occurrence_id__in=chunk_occ_ids)
                }

            # --- OccurrenceSite ---
            site_create = []
            site_update = []
            existing_sites = defaultdict(dict)
            if not getattr(ctx, "wipe_targets", False):
                for s in OccurrenceSite.objects.filter(occurrence_id__in=chunk_occ_ids):
                    existing_sites[s.occurrence_id][s.site_name] = s

            for op in chunk_ops:
                mig = op["migrated_from_id"]
                merged = op.get("merged") or {}
                occ = get_chunk_occ(mig)
                if not occ:
                    continue

                src = merged.get("_source")
                pipeline_map = pipelines_by_source.get(src, pipelines_by_source.get(None, {}))

                sites_to_process = []
                if src != Source.TPFL.value:
                    if merged.get("_nested_sites"):
                        for raw_site in merged.get("_nested_sites"):
                            mapped_site = schema.SCHEMA.map_raw_row(raw_site)
                            tcx = TransformContext(row=mapped_site, model=None, user_id=ctx.user_id)
                            transformed_site = dict(mapped_site)
                            for col, pipeline in pipeline_map.items():
                                if col.startswith("OccurrenceSite__"):
                                    raw_val = mapped_site.get(col)
                                    res = run_pipeline(pipeline, raw_val, tcx)
                                    transformed_site[col] = res.value
                            sites_to_process.append(transformed_site)
                    elif any(k.startswith("OccurrenceSite__") for k in merged):
                        sites_to_process.append(merged)

                for mapped_site in sites_to_process:
                    site_name = mapped_site.get("OccurrenceSite__site_name")
                    defaults = {
                        "comments": mapped_site.get("OccurrenceSite__comments"),
                        "geometry": mapped_site.get("OccurrenceSite__geometry")
                        or tec_site_geometry_transform(mapped_site, None),
                        "updated_date": mapped_site.get("OccurrenceSite__updated_date"),
                        "drawn_by": mapped_site.get("OccurrenceSite__drawn_by"),
                        "last_updated_by": mapped_site.get("OccurrenceSite__drawn_by"),
                    }

                    if site_name in existing_sites[occ.pk]:
                        s = existing_sites[occ.pk][site_name]
                        for k, v in defaults.items():
                            if k != "updated_date":
                                setattr(s, k, v)
                        if defaults["updated_date"]:
                            s.updated_date = defaults["updated_date"]
                        site_update.append(s)
                    else:
                        s = OccurrenceSite(
                            occurrence=occ,
                            site_name=site_name,
                            comments=defaults["comments"],
                            geometry=defaults["geometry"],
                            drawn_by=defaults["drawn_by"],
                            last_updated_by=defaults["last_updated_by"],
                        )
                        if defaults["updated_date"]:
                            s.updated_date = defaults["updated_date"]
                        site_create.append(s)

            if site_create:
                OccurrenceSite.objects.bulk_create(site_create, batch_size=BATCH)
            if site_update:
                OccurrenceSite.objects.bulk_update(
                    site_update,
                    [
                        "comments",
                        "geometry",
                        "updated_date",
                        "drawn_by",
                        "last_updated_by",
                    ],
                    batch_size=BATCH,
                )

            # --- Nested Documents (OccurrenceDocument) ---
            nested_doc_create = []

            # Pre-load existing documents to check for duplicates (avoid creating identical copies on re-runs)
            existing_docs_check = defaultdict(list)
            if not getattr(ctx, "wipe_targets", False):
                for d in OccurrenceDocument.objects.filter(occurrence_id__in=chunk_occ_ids):
                    existing_docs_check[d.occurrence_id].append(d)

            for op in chunk_ops:
                mig = op["migrated_from_id"]
                merged = op.get("merged") or {}
                occ = get_chunk_occ(mig)
                if not occ:
                    continue

                nested = merged.get("_nested_additional_data")
                # Only process if we have nested data and it's from TEC (explicit check)
                if nested and merged.get("_source") == Source.TEC.value:
                    src = merged.get("_source")
                    pipeline_map = pipelines_by_source.get(src, pipelines_by_source.get(None, {}))

                    for raw_doc in nested:
                        mapped_doc = schema.SCHEMA.map_raw_row(raw_doc)
                        tcx = TransformContext(row=mapped_doc, model=None, user_id=ctx.user_id)
                        transformed_doc = dict(mapped_doc)
                        doc_has_error = False
                        doc_issues = []

                        # Apply pipelines for relevant columns
                        for col, pipeline in pipeline_map.items():
                            if col.startswith("OccurrenceDocument__"):
                                raw_val = mapped_doc.get(col)
                                res = run_pipeline(pipeline, raw_val, tcx)
                                transformed_doc[col] = res.value
                                for issue in res.issues:
                                    doc_issues.append((col, issue))
                                    level = getattr(issue, "level", "error")
                                    record = {
                                        "migrated_from_id": op["migrated_from_id"],
                                        "column": col,
                                        "level": level,
                                        "message": getattr(issue, "message", str(issue)),
                                        "raw_value": raw_val,
                                    }
                                    if level == "error":
                                        doc_has_error = True
                                        errors += 1
                                        errors_details.append(record)
                                    else:
                                        warn_count += 1
                                        warnings_details.append(record)

                        if doc_has_error:
                            continue  # Skip this document row if any error

                        sub_cat_id = transformed_doc.get("OccurrenceDocument__document_sub_category_id")
                        desc = transformed_doc.get("OccurrenceDocument__description") or ""

                        # Create if we have data and it's not a duplicate
                        if sub_cat_id is not None or desc:
                            # Check for duplicates
                            is_duplicate = False
                            if existing_docs_check.get(occ.pk):
                                for ex in existing_docs_check[occ.pk]:
                                    # Compare fields to determine if this exact document exists
                                    if (
                                        ex.document_sub_category_id == sub_cat_id
                                        and ex.description == desc
                                        and (
                                            orf_document_category
                                            and ex.document_category_id == orf_document_category.id
                                        )
                                    ):
                                        is_duplicate = True
                                        break

                            if not is_duplicate:
                                doc_uploaded = transformed_doc.get("OccurrenceDocument__uploaded_by")
                                nested_doc_create.append(
                                    OccurrenceDocument(
                                        occurrence=occ,
                                        document_category=orf_document_category,
                                        document_sub_category_id=sub_cat_id,
                                        description=desc,
                                        uploaded_by=doc_uploaded,
                                    )
                                )

            if nested_doc_create:
                try:
                    OccurrenceDocument.objects.bulk_create(nested_doc_create, batch_size=BATCH)
                except Exception:
                    logger.exception("Failed to bulk_create nested OccurrenceDocument")

            # --- Nested Species ---
            needed_taxa = set()
            needed_roles = set()
            species_ops = []

            for op in chunk_ops:
                mig = op["migrated_from_id"]
                merged = op.get("merged") or {}
                occ = get_chunk_occ(mig)
                if not occ:
                    continue

                nested_species = merged.get("_nested_species")
                if nested_species:
                    for sp in nested_species:
                        raw_taxon_id = sp.get("SPEC_TAXON_ID")
                        role_name = sp.get("_resolved_role")
                        voucher = sp.get("SPEC_VOUCHER_NO")
                        if raw_taxon_id:
                            try:
                                taxon_id = int(raw_taxon_id)
                                needed_taxa.add(taxon_id)
                                if role_name:
                                    needed_roles.add(role_name)
                                species_ops.append((occ.pk, taxon_id, role_name, voucher, mig))
                            except (ValueError, TypeError):
                                logger.warning(f"Skipping invalid SPEC_TAXON_ID: {raw_taxon_id}")

            if species_ops:
                tax_map = {t.taxon_name_id: t for t in Taxonomy.objects.filter(taxon_name_id__in=needed_taxa)}
                role_map = {r.name: r for r in SpeciesRole.objects.filter(name__in=needed_roles)}

                relevant_tax_ids = [t.id for t in tax_map.values()]

                existing_asts = defaultdict(list)
                for ast in AssociatedSpeciesTaxonomy.objects.filter(taxonomy_id__in=relevant_tax_ids):
                    existing_asts[(ast.taxonomy_id, ast.species_role_id)].append(ast)

                missing_keys = set()
                voucher_by_key = {}  # (taxonomy_id, role_id) -> first non-empty voucher comment
                for occ_pk, taxon_id, role_name, voucher, mig in species_ops:
                    tax = tax_map.get(taxon_id)
                    if not tax:
                        errors_details.append(
                            {
                                "migrated_from_id": mig,
                                "column": "AssociatedSpecies",
                                "level": "error",
                                "message": f"Taxonomy not found for taxon_name_id {taxon_id}",
                                "raw_value": str(taxon_id),
                                "reason": "missing_taxonomy",
                            }
                        )
                        errors += 1
                        continue
                    role = role_map.get(role_name)
                    role_id = role.id if role else None
                    key = (tax.id, role_id)
                    if not existing_asts.get(key):
                        missing_keys.add(key)
                    # Track voucher comments (first non-empty wins)
                    if voucher and key not in voucher_by_key:
                        voucher_by_key[key] = str(voucher).strip()

                if missing_keys:
                    new_asts = [
                        AssociatedSpeciesTaxonomy(
                            taxonomy_id=tid,
                            species_role_id=rid,
                            comments=voucher_by_key.get((tid, rid), ""),
                        )
                        for tid, rid in missing_keys
                    ]
                    AssociatedSpeciesTaxonomy.objects.bulk_create(new_asts, batch_size=BATCH)

                    existing_asts = defaultdict(list)
                    for ast in AssociatedSpeciesTaxonomy.objects.filter(taxonomy_id__in=relevant_tax_ids):
                        existing_asts[(ast.taxonomy_id, ast.species_role_id)].append(ast)

                occ_assoc_ids = [a.id for a in existing_assoc.values()]
                if occ_assoc_ids:
                    through_model = OCCAssociatedSpecies.related_species.through
                    existing_links = set()
                    if not getattr(ctx, "wipe_targets", False):
                        existing_links = set(
                            through_model.objects.filter(occassociatedspecies_id__in=occ_assoc_ids).values_list(
                                "occassociatedspecies_id",
                                "associatedspeciestaxonomy_id",
                            )
                        )

                    through_objs = []
                    for occ_pk, taxon_id, role_name, voucher, mig in species_ops:
                        tax = tax_map.get(taxon_id)
                        if not tax:
                            continue
                        role = role_map.get(role_name)
                        role_id = role.id if role else None

                        asts = existing_asts.get((tax.id, role_id))
                        if not asts:
                            continue
                        ast = asts[0]

                        occ_assoc = existing_assoc.get(occ_pk)
                        if occ_assoc:
                            if (occ_assoc.id, ast.id) not in existing_links:
                                through_objs.append(
                                    through_model(
                                        occassociatedspecies_id=occ_assoc.id,
                                        associatedspeciestaxonomy_id=ast.id,
                                    )
                                )
                                existing_links.add((occ_assoc.id, ast.id))

                    if through_objs:
                        through_model.objects.bulk_create(through_objs, batch_size=BATCH)

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
                logger.error("Failed to write error CSV for %s at %s: %s", self.slug, csv_path, e)
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
