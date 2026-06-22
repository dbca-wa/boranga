import logging

from django.core.management.base import BaseCommand

from boranga.components.occurrence.models import OccurrenceReportBulkImportTask

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Seed initial django-reversion history for completed OCR bulk import tasks that have not yet been seeded"

    def handle(self, *args, **options):
        logger.info(f"Running command {__name__}")

        pending = OccurrenceReportBulkImportTask.objects.filter(
            processing_status=OccurrenceReportBulkImportTask.PROCESSING_STATUS_COMPLETED,
            history_seeded=False,
        ).order_by("datetime_completed")

        count = pending.count()
        if count == 0:
            logger.info("No completed bulk import tasks require history seeding, returning")
            return

        logger.info(f"Found {count} task(s) requiring history seeding")

        from boranga.components.data_migration.history_seeding.reversion_seeder import (
            MigratedHistorySeeder,
        )

        seeder = MigratedHistorySeeder(batch_size=500)

        for task in pending:
            logger.info(f"Seeding history for bulk import task {task.id}")
            try:
                seeded = seeder.seed_bulk_import_occurrence_reports(task.pk)
                OccurrenceReportBulkImportTask.objects.filter(pk=task.pk).update(history_seeded=True)
                logger.info(f"Bulk import task {task.id}: seeded {seeded} reversion version(s)")
            except Exception:
                logger.exception(f"Bulk import task {task.id}: history seeding failed, will retry next run")
