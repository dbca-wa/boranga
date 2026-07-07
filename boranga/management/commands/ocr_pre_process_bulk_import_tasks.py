import logging

from django.core.management.base import BaseCommand

from boranga.components.occurrence.models import OccurrenceReportBulkImportTask

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Pre process the OCR bulk import tasks"

    def handle(self, *args, **options):
        logger.info(f"Running command {__name__}")

        # Find the next task that needs pre-processing (rows not yet counted).
        # Intentionally does NOT claim the task to STARTED — row-counting is
        # idempotent, so two concurrent runs computing the same value and saving
        # it is harmless.  Claiming to STARTED would cause ocr_process_bulk_import_queue
        # to see "a task already running" and bail for up to 5 minutes.
        task = (
            OccurrenceReportBulkImportTask.objects.filter(
                processing_status=OccurrenceReportBulkImportTask.PROCESSING_STATUS_QUEUED,
                _file__isnull=False,
                rows__isnull=True,
            )
            .order_by("datetime_queued")
            .first()
        )

        if task is None:
            logger.info("No tasks to pre-process, returning")
            return

        # Count rows without changing status — process queue can still pick up
        # the task at any time (process() re-counts rows if needed).
        task.count_rows()
        logger.info(f"OCR Bulk Import Task {task.id} has {task.rows} rows.")

        column_count = task.schema.columns.count() if task.schema else None
        logger.info(f"OCR Bulk Import Task {task.id} has {column_count} columns.")

        OccurrenceReportBulkImportTask.objects.filter(id=task.id).update(
            column_count=column_count,
        )
        return
