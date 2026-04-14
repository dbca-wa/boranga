import logging

from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils import timezone

from boranga.components.occurrence.models import OccurrenceReport

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Auto lock unlocked occurrence report records that have not been updated within a specified time window."

    def handle(self, *args, **options):
        # Calculate the threshold time for locking
        threshold_time = timezone.now() - timezone.timedelta(
            minutes=settings.UNLOCKED_OCCURRENCE_REPORT_EDITING_WINDOW_MINUTES
        )

        # Lock all unlocked occurrence report records that were updated before the threshold time
        occurrence_reports_to_lock = OccurrenceReport.objects.filter(
            processing_status=OccurrenceReport.PROCESSING_STATUS_APPROVED,
            locked=False,
            datetime_updated__lt=threshold_time,
        )

        if occurrence_reports_to_lock.exists():
            logger.info(
                "The following occurrence report records will be locked: "
                f"{list(occurrence_reports_to_lock.values_list('occurrence_report_number', flat=True))}"
            )
            occurrence_reports_to_lock.update(locked=True)
