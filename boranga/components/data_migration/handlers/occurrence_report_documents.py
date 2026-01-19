from __future__ import annotations

import logging

from django.utils import timezone

from boranga.components.data_migration.adapters.occurrence_report.document import (
    OccurrenceReportDocumentAdapter,
)
from boranga.components.data_migration.registry import (
    BaseSheetImporter,
    ImportContext,
    TransformContext,
    register,
    run_pipeline,
)
from boranga.components.occurrence.models import OccurrenceReportDocument

logger = logging.getLogger(__name__)


@register
class OccurrenceReportDocumentImporter(BaseSheetImporter):
    slug = "occurrence_report_documents_legacy"
    description = "Import occurrence report documents from DRF_RFR_FORMS"

    def run(self, path: str, ctx: ImportContext, **options):
        start_time = timezone.now()
        logger.info(
            "OccurrenceReportDocumentImporter started at %s (dry_run=%s)",
            start_time.isoformat(),
            ctx.dry_run,
        )

        adapter = OccurrenceReportDocumentAdapter()
        result = adapter.extract(path, **options)

        logger.info(
            "Extracted %d rows from %s",
            len(result.rows),
            path,
        )

        # Build pipelines
        from boranga.components.data_migration.registry import (
            registry as transform_registry,
        )

        pipelines = {}
        if hasattr(adapter, "PIPELINES"):
            for col, names in adapter.PIPELINES.items():
                pipelines[col] = transform_registry.build_pipeline(names)

        processed = 0
        created = 0
        skipped = 0
        errors = 0

        errors_details = []

        to_create = []

        for row in result.rows:
            processed += 1
            tcx = TransformContext(row=row, model=None, user_id=ctx.user_id)

            transformed = {}
            has_error = False

            for col, pipeline in pipelines.items():
                # Some fields might not be in the row if they are purely synthetic (like document_category_id)
                # But run_pipeline handles None input if the first transform handles it.
                # However, usually we iterate over the PIPELINES keys.

                # If the column is in the row, use it. If not, pass None.
                raw_val = row.get(col)
                res = run_pipeline(pipeline, raw_val, tcx)

                if any(i.level == "error" for i in res.issues):
                    has_error = True
                    # Log error
                    msg = (
                        f"Error transforming {col} for row {row.get('SHEETNO')}: "
                        f"{res.issues}"
                    )
                    logger.error(msg)
                    for issue in res.issues:
                        if issue.level == "error":
                            errors_details.append(
                                {
                                    "migrated_from_id": row.get("SHEETNO"),
                                    "column": col,
                                    "level": issue.level,
                                    "message": issue.message,
                                    "raw_value": raw_val,
                                    "reason": "transform_error",
                                    "row": row,
                                }
                            )
                    break

                transformed[col] = res.value

            if has_error:
                skipped += 1
                errors += 1
                continue

            # Validate using dataclass
            try:
                # We don't have a validate method on OccurrenceReportDocumentRow yet, but we can add one or just use it
                # for type checking/defaults
                # For now, just use the dict
                pass
            except Exception as e:
                logger.error(f"Validation error: {e}")
                skipped += 1
                errors += 1
                errors_details.append(
                    {
                        "migrated_from_id": row.get("SHEETNO"),
                        "column": None,
                        "level": "error",
                        "message": str(e),
                        "raw_value": None,
                        "reason": "validation_error",
                        "row": row,
                    }
                )
                continue

            # Check required fields
            if not transformed.get("occurrence_report_id"):
                logger.warning(
                    f"Skipping row {row.get('SHEETNO')}: occurrence_report_id not resolved"
                )
                skipped += 1
                # This is a warning/skip, not necessarily an error we want to dump to CSV unless requested.
                # But let's log it as an error detail if we want to track skips.
                # The other importer tracks errors only in CSV usually.
                # But let's add it if it's critical.
                # Actually, if ID is missing, we can't link it.
                errors_details.append(
                    {
                        "migrated_from_id": row.get("SHEETNO"),
                        "column": "occurrence_report_id",
                        "level": "warning",
                        "message": "occurrence_report_id not resolved",
                        "raw_value": row.get("SHEETNO"),
                        "reason": "missing_id",
                        "row": row,
                    }
                )
                continue

            if not transformed.get("description"):
                # If description is empty, it means no attachments were found in the row.
                # We should skip this row.
                skipped += 1
                continue

            # Prepare creation
            # We need to handle the _file field.
            # As discussed, we will set it to empty string or a placeholder if allowed.
            # We verified in shell that _file='' works.

            defaults = {
                "occurrence_report_id": transformed.get("occurrence_report_id"),
                "document_category_id": transformed.get("document_category_id"),
                "document_sub_category_id": transformed.get("document_sub_category_id"),
                "description": transformed.get("description"),
                "active": transformed.get("active", True),
                "uploaded_by": transformed.get("uploaded_by"),
                "uploaded_date": transformed.get("uploaded_date"),
                "name": transformed.get("name"),  # Should be None
                "_file": "",  # Placeholder
            }

            # Store tuple of (instance, original_row) to allow fallback error logging
            to_create.append((OccurrenceReportDocument(**defaults), row))

        if ctx.dry_run:
            logger.info(f"Dry run: would create {len(to_create)} documents")
            return

        # Bulk create
        logger.info(f"Creating {len(to_create)} documents...")

        if to_create:
            # Unzip instances for bulk operation
            instances = [x[0] for x in to_create]
            # Capture original uploaded_date values because bulk_create mutates instances
            # and might overwrite them with DB defaults (auto_now_add)
            original_dates = [inst.uploaded_date for inst in instances]

            try:
                from django.db import transaction

                with transaction.atomic():
                    # Use batch_size to avoid huge SQL queries
                    created_objs = OccurrenceReportDocument.objects.bulk_create(
                        instances, batch_size=1000
                    )
                    created = len(created_objs)

                    # Post-process to set document_number and ensure uploaded_date is preserved
                    for i, obj in enumerate(created_objs):
                        obj.document_number = f"D{obj.pk}"
                        # Restore the original date if it was set
                        if original_dates[i]:
                            obj.uploaded_date = original_dates[i]

                    OccurrenceReportDocument.objects.bulk_update(
                        created_objs,
                        ["document_number", "uploaded_date"],
                        batch_size=1000,
                    )

            except Exception as e:
                logger.error(
                    f"Bulk create failed ({e}), falling back to individual saves..."
                )

                # Fallback: Try to save one by one to isolate errors
                for instance, row_data in to_create:
                    try:
                        # We must manually handle uploaded_date because auto_now_add=True
                        # ignores the value on the instance during creation.
                        target_date = instance.uploaded_date

                        instance.save()
                        created += 1

                        # Patch the date if needed
                        if target_date:
                            OccurrenceReportDocument.objects.filter(
                                pk=instance.pk
                            ).update(uploaded_date=target_date)

                    except Exception as inner_e:
                        errors += 1
                        errors_details.append(
                            {
                                "migrated_from_id": row_data.get("SHEETNO"),
                                "column": None,
                                "level": "error",
                                "message": str(inner_e),
                                "raw_value": None,
                                "reason": "create_error",
                                "row": row_data,
                            }
                        )

        end_time = timezone.now()
        duration = end_time - start_time

        # Write error CSV if needed
        if errors_details:
            import csv
            import json
            import os

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
                logger.info(f"Error details written to {csv_path}")
            except Exception as e:
                logger.error(f"Failed to write error CSV: {e}")

        logger.info(
            "OccurrenceReportDocumentImporter finished. Created: %d, Skipped: %d, Errors: %d. Duration: %s",
            created,
            skipped,
            errors,
            duration,
        )
