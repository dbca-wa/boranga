from __future__ import annotations

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
from boranga.components.occurrence.models import OCCContactDetail, Occurrence

logger = logging.getLogger(__name__)

SOURCE_ADAPTERS = {
    Source.TPFL.value: OccurrenceTpflAdapter(),
    # Add new adapters here as they are implemented:
    # Source.TEC.value: OccurrenceTECAdapter(),
    # Source.TFAUNA.value: OccurrenceTFAUNAAdapter(),
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
        from django.db import transaction

        with transaction.atomic():
            try:
                OCCContactDetail.objects.all().delete()
            except Exception:
                logger.exception("Failed to delete OCCContactDetail")
            try:
                Occurrence.objects.all().delete()
            except Exception:
                logger.exception("Failed to delete Occurrence")

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
            defaults["lodgement_date"] = merged.get("datetime_created")
            if merged.get("locked") is not None:
                defaults["locked"] = merged.get("locked")

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
                    merged.get("contact"),
                    merged.get("contact_name"),
                    merged.get("notes"),
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

        # Bulk update existing objects
        if to_update:
            logger.info(
                "OccurrenceImporter: bulk-updating %d existing Occurrences",
                len(to_update),
            )
            update_instances = to_update
            fields = set()
            for inst in update_instances:
                fields.update(
                    [
                        f.name
                        for f in inst._meta.fields
                        if getattr(inst, f.name, None) is not None
                    ]
                )
            try:
                Occurrence.objects.bulk_update(
                    update_instances, list(fields), batch_size=BATCH
                )
            except Exception:
                logger.exception(
                    "Failed to bulk_update Occurrence; falling back to individual saves"
                )
                for inst in update_instances:
                    try:
                        inst.save()
                    except Exception:
                        logger.exception(
                            "Failed to save Occurrence %s", getattr(inst, "pk", None)
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
        for mig, contact, contact_name, notes in create_meta:
            occ = created_map.get(mig)
            if not occ:
                continue
            if occ.pk in existing_contacts:
                continue
            if contact or contact_name or notes:
                want_contact_create.append(
                    OCCContactDetail(
                        occurrence=occ,
                        contact=contact,
                        contact_name=contact_name,
                        notes=notes,
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
                merged.get("contact")
                or merged.get("contact_name")
                or merged.get("notes")
            ):
                want_contact_create.append(
                    OCCContactDetail(
                        occurrence=occ,
                        contact=merged.get("contact"),
                        contact_name=merged.get("contact_name"),
                        notes=merged.get("notes"),
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
                    "boranga/components/data_migration/handlers/handler_output",
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
