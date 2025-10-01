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
        stuck_qs = OccurrenceReportBulkImportTask.objects.filter(
            processing_status=OccurrenceReportBulkImportTask.PROCESSING_STATUS_STARTED,
            datetime_started__lt=timezone.now()
            - timezone.timedelta(seconds=settings.OCR_BULK_IMPORT_TASK_TIMEOUT_SECONDS),
        )
        stuck_qs.update(
            processing_status=OccurrenceReportBulkImportTask.PROCESSING_STATUS_QUEUED,
            rows_processed=0,
            datetime_queued=timezone.now(),
        )

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

        # Let OccurrenceReportBulkImportTask.process() handle atomic claiming;
        # reload instance and call process()
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
