from __future__ import annotations

from typing import Any

from django.apps import apps
from django.db import transaction

from boranga.components.data_migration.adapters.occurrence_documents import schema
from boranga.components.data_migration.adapters.occurrence_documents.tpfl import (
    OccurrenceDocumentTpflAdapter,
)
from boranga.components.data_migration.adapters.sources import Source
from boranga.components.data_migration.registry import (
    BaseSheetImporter,
    ImportContext,
    TransformContext,
    register,
    run_pipeline,
)

SOURCE_ADAPTERS = {
    Source.TPFL.value: OccurrenceDocumentTpflAdapter(),
    # Add new adapters here as implemented
}


@register
class OccurrenceDocumentImporter(BaseSheetImporter):
    slug = "occurrence_documents_legacy"
    description = (
        "Import occurrence child documents from legacy TPFL/TEC/TFAUNA sources"
    )

    def clear_targets(
        self, ctx: ImportContext, include_children: bool = False, **options
    ):
        """Delete OccurrenceDocument target data. Respect `ctx.dry_run`."""
        if ctx.dry_run:
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

        is_filtered = bool(sources)

        logger = __import__("logging").getLogger(__name__)

        if is_filtered:
            if not target_group_types:
                return
            logger.warning(
                "OccurrenceDocumentImporter: deleting OccurrenceDocument data for group_types: %s ...",
                target_group_types,
            )
            doc_filter = {"occurrence__group_type__name__in": target_group_types}
        else:
            logger.warning(
                "OccurrenceDocumentImporter: deleting OccurrenceDocument data..."
            )
            doc_filter = {}

        from django.apps import apps
        from django.db import connections

        conn = connections["default"]
        was_autocommit = conn.get_autocommit()
        if not was_autocommit:
            conn.set_autocommit(True)

        try:
            OccurrenceDocument = apps.get_model("boranga", "OccurrenceDocument")
            try:
                if is_filtered:
                    OccurrenceDocument.objects.filter(**doc_filter).delete()
                else:
                    OccurrenceDocument.objects.all().delete()
            except Exception:
                logger.exception("Failed to delete OccurrenceDocument")

            # Reset the primary key sequence for OccurrenceDocument when using PostgreSQL.
            try:
                if getattr(conn, "vendor", None) == "postgresql":
                    table = OccurrenceDocument._meta.db_table
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
                    logger.info(
                        "Reset primary key sequence for table %s to %s", table, max_id
                    )
            except Exception:
                logger.exception(
                    "Failed to reset OccurrenceDocument primary key sequence"
                )
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
        sources = options.get("sources") or list(SOURCE_ADAPTERS.keys())
        path_map = self._parse_path_map(options.get("path_map"))

        stats = ctx.stats.setdefault(self.slug, self.new_stats())
        all_rows: list[dict] = []
        warnings = []

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
        # any adapter-provided `PIPELINES` so source-specific transforms live
        # with the adapter while remaining runnable by the importer.
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

        processed = 0
        errors = 0
        created = 0
        updated = 0
        skipped = 0
        warn_count = 0

        errors_details = []

        # 3. Transform each row (documents are 1:many children; no merging)
        transformed_rows: list[tuple[dict, str, list[tuple[str, Any]], dict]] = []
        for row in all_rows:
            processed += 1
            tcx = TransformContext(row=row, model=None, user_id=ctx.user_id)
            issues = []
            transformed = {}
            has_error = False
            # Choose pipeline map based on row source (fallback to base)
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
                    if issue.level == "error":
                        has_error = True
                        errors += 1
                        errors_details.append(
                            {
                                "migrated_from_id": row.get("migrated_from_id"),
                                "column": col,
                                "level": issue.level,
                                "message": issue.message,
                                "raw_value": raw_val,
                                "reason": "transform_error",
                                "row": row,
                            }
                        )
                    else:
                        warn_count += 1
            if has_error:
                skipped += 1
                continue
            transformed_rows.append((transformed, row.get("_source"), issues, row))

        # 4. Persist each transformed document row, linking to parent occurrence
        # Resolve models
        try:
            Occurrence = apps.get_model("boranga", "Occurrence")
        except LookupError:
            raise RuntimeError("Model occurrence.Occurrence not found")

        try:
            OccurrenceDocument = apps.get_model("boranga", "OccurrenceDocument")
        except LookupError:
            # If OccurrenceDocument model is not present, we cannot persist documents
            raise RuntimeError("Model occurrence.OccurrenceDocument not found")

        # Pre-fetch valid occurrence IDs to avoid in-loop queries
        valid_occurrence_ids = set(Occurrence.objects.values_list("pk", flat=True))

        to_create = []
        logger = __import__("logging").getLogger(__name__)

        for transformed, src, issues, row in transformed_rows:
            # normalized key: pipelines for occurrence_id may return occurrence instance or migrated id
            occurrence_id = transformed.get("occurrence_id")
            if occurrence_id is None:
                warnings.append(f"{src}: missing occurrence_id after transform")
                skipped += 1
                errors += 1
                errors_details.append(
                    {
                        "migrated_from_id": row.get("migrated_from_id"),
                        "column": "occurrence_id",
                        "level": "error",
                        "message": "missing occurrence_id after transform",
                        "raw_value": None,
                        "reason": "validation_error",
                        "row": row,
                    }
                )
                continue

            # Validate occurrence_id
            try:
                # Assuming PK is int or compatible
                occ_id_val = int(occurrence_id)
            except (ValueError, TypeError):
                warnings.append(f"{src}: invalid occurrence_id format {occurrence_id}")
                skipped += 1
                errors += 1
                errors_details.append(
                    {
                        "migrated_from_id": row.get("migrated_from_id"),
                        "column": "occurrence_id",
                        "level": "error",
                        "message": f"invalid occurrence_id format {occurrence_id}",
                        "raw_value": occurrence_id,
                        "reason": "validation_error",
                        "row": row,
                    }
                )
                continue

            if occ_id_val not in valid_occurrence_ids:
                warnings.append(f"{src}: occurrence_id {occurrence_id} not found")
                skipped += 1
                errors += 1
                errors_details.append(
                    {
                        "migrated_from_id": row.get("migrated_from_id"),
                        "column": "occurrence_id",
                        "level": "error",
                        "message": f"occurrence_id {occurrence_id} not found",
                        "raw_value": occurrence_id,
                        "reason": "validation_error",
                        "row": row,
                    }
                )
                continue

            # build payload mapping to model fields
            payload = {
                "occurrence_id": occ_id_val,
                "document_category_id": transformed.get("document_category_id"),
                "document_sub_category_id": transformed.get("document_sub_category_id"),
                "uploaded_by": transformed.get("uploaded_by"),
                "uploaded_date": transformed.get("uploaded_date"),
                "description": transformed.get("description") or "",
                "name": transformed.get("name") or "",
                "_file": "",  # Placeholder
                "active": True,
            }

            to_create.append((OccurrenceDocument(**payload), row))

        if ctx.dry_run:
            stats.update(
                processed=processed,
                created=len(to_create),
                updated=0,
                skipped=skipped,
                errors=errors,
                warnings=warn_count,
            )
            return stats

        # Bulk create
        if to_create:
            logger.info(f"Creating {len(to_create)} documents...")
            # Unzip instances for bulk operation
            instances = [x[0] for x in to_create]
            # Capture original uploaded_date values because bulk_create mutates instances
            # and might overwrite them with DB defaults (auto_now_add)
            original_dates = [inst.uploaded_date for inst in instances]

            try:
                with transaction.atomic():
                    # Use batch_size to avoid huge SQL queries
                    created_objs = OccurrenceDocument.objects.bulk_create(
                        instances, batch_size=1000
                    )
                    created = len(created_objs)

                    # Post-process to set document_number and ensure uploaded_date is preserved
                    for i, obj in enumerate(created_objs):
                        obj.document_number = f"D{obj.pk}"
                        # Restore the original date if it was set
                        if original_dates[i]:
                            obj.uploaded_date = original_dates[i]

                    OccurrenceDocument.objects.bulk_update(
                        created_objs,
                        ["document_number", "uploaded_date"],
                        batch_size=1000,
                    )

            except Exception as e:
                logger.error(
                    f"Bulk create failed ({e}), falling back to individual saves..."
                )
                # Fallback: Try to save one by one to isolate errors
                for instance, row in to_create:
                    try:
                        target_date = instance.uploaded_date
                        instance.save()
                        created += 1

                        # Patch the date if needed
                        if target_date:
                            OccurrenceDocument.objects.filter(pk=instance.pk).update(
                                uploaded_date=target_date
                            )

                    except Exception as inner_e:
                        skipped += 1
                        errors += 1
                        errors_details.append(
                            {
                                "migrated_from_id": row.get("migrated_from_id"),
                                "column": None,
                                "level": "error",
                                "message": str(inner_e),
                                "raw_value": None,
                                "reason": "create_error",
                                "row": row,
                            }
                        )
                        continue

        # Write error CSV if needed
        if errors_details:
            import csv
            import json
            import os

            from django.utils import timezone

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
                logger.warning(f"Wrote {len(errors_details)} errors to {csv_path}")
            except Exception:
                logger.exception("Failed to write error CSV")

        stats.update(
            processed=processed,
            created=created,
            updated=updated,
            skipped=skipped,
            errors=errors,
            warnings=warn_count,
        )
        return stats
