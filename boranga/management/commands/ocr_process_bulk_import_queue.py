import logging
import traceback

from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils import timezone

from boranga.components.occurrence.models import OccurrenceReportBulkImportTask

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Process the OCR bulk import queue"

    def handle(self, *args, **options):
        logger.info(f"Running command {__name__}")

        # Check if there are any tasks that have been processing for too long
        qs = OccurrenceReportBulkImportTask.objects.filter(
            processing_status=OccurrenceReportBulkImportTask.PROCESSING_STATUS_STARTED,
            datetime_started__lt=timezone.now()
            - timezone.timedelta(seconds=settings.OCR_BULK_IMPORT_TASK_TIMEOUT_SECONDS),
        )
        if qs.exists():
            for task in qs:
                logger.info(
                    f"Task {task.id} has been processing for too long. Adding back to the queue"
                )
                task.processing_status = (
                    OccurrenceReportBulkImportTask.PROCESSING_STATUS_QUEUED
                )
                task.rows_processed = 0
                task.save()

        # Check if there are already any tasks running and return if so
        if OccurrenceReportBulkImportTask.objects.filter(
            processing_status=OccurrenceReportBulkImportTask.PROCESSING_STATUS_STARTED,
        ).exists():
            logger.info("There is already a task running, returning")
            return

        # Get the next task to process (non-blocking) and try to claim it atomically
        candidate = (
            OccurrenceReportBulkImportTask.objects.filter(
                processing_status=OccurrenceReportBulkImportTask.PROCESSING_STATUS_QUEUED,
                _file__isnull=False,
            )
            .order_by("datetime_queued")
            .first()
        )

        if candidate is None:
            logger.info("No tasks to process, returning")
            return

        # Attempt to claim the candidate so only one worker processes it
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

        # We own the task — reload instance and process
        task = OccurrenceReportBulkImportTask.objects.get(id=candidate.id)

        try:
            # Process the task
            errors = task.process()
            if errors:
                task.processing_status = (
                    OccurrenceReportBulkImportTask.PROCESSING_STATUS_FAILED
                )
                task.datetime_error = timezone.now()
                task.error_message = "Errors occurred during processing:\n"
                for error in errors:
                    task.error_message += f"Row: {error['row_index'] + 1}. Error: {error['error_message']}\n"
            else:
                # Set the task to completed
                task.processing_status = (
                    OccurrenceReportBulkImportTask.PROCESSING_STATUS_COMPLETED
                )
                task.datetime_completed = timezone.now()
            task.save()

        except KeyboardInterrupt:
            logger.info(f"OCR Bulk Import Task {task.id} was interrupted")
            task.processing_status = (
                OccurrenceReportBulkImportTask.PROCESSING_STATUS_FAILED
            )
            task.error_message = "KeyboardInterrupt"
            task.save()
            return
        except Exception as e:
            logger.error(f"Error processing OCR Bulk Import Task {task.id}: {e}")
            logger.error(traceback.format_exc())
            task.processing_status = (
                OccurrenceReportBulkImportTask.PROCESSING_STATUS_FAILED
            )
            task.error_message = str(e)
            task.save()
            return

        logger.info(f"OCR Bulk Import Task {task.id} completed")

        return
