import logging

from django.core.management.base import BaseCommand
from django.utils import timezone

from boranga.components.occurrence.models import OccurrenceReportBulkImportTask

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Pre process the OCR bulk import tasks"

    def handle(self, *args, **options):
        logger.info(f"Running command {__name__}")

        # Find a candidate (non-blocking)
        candidate = (
            OccurrenceReportBulkImportTask.objects.filter(
                processing_status=OccurrenceReportBulkImportTask.PROCESSING_STATUS_QUEUED,
                _file__isnull=False,
                rows__isnull=True,
            )
            .order_by("datetime_queued")
            .first()
        )

        if candidate is None:
            logger.info("No tasks to process, returning")
            return

        # Try to claim it atomically — only one process will succeed
        updated = OccurrenceReportBulkImportTask.objects.filter(
            id=candidate.id,
            processing_status=OccurrenceReportBulkImportTask.PROCESSING_STATUS_QUEUED,
        ).update(
            processing_status=OccurrenceReportBulkImportTask.PROCESSING_STATUS_STARTED,
            datetime_started=timezone.now(),
        )

        if updated != 1:
            logger.info("Task already claimed by another worker, returning")
            return

        # We own the task — reload instance and do pre-processing (long work) outside the claim step
        task = OccurrenceReportBulkImportTask.objects.get(id=candidate.id)
        task.count_rows()
        logger.info(f"OCR Bulk Import Task {task.id} has {task.rows} rows.")
        return
