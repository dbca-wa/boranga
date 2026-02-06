import logging

from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils import timezone

from boranga.components.occurrence.models import Occurrence

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Auto lock unlocked occurrence records that have " "not been updated within a specified time window."

    def handle(self, *args, **options):
        # Calculate the threshold time for locking
        threshold_time = timezone.now() - timezone.timedelta(
            minutes=settings.UNLOCKED_OCCURRENCE_EDITING_WINDOW_MINUTES
        )

        # Lock all unlocked occurrence records that were updated before the threshold time
        occurrences_to_lock = Occurrence.objects.filter(
            processing_status__in=[
                Occurrence.PROCESSING_STATUS_ACTIVE,
            ],
            locked=False,
            datetime_updated__lt=threshold_time,
        )

        if occurrences_to_lock.exists():
            logger.info(
                "The following occurrence records will be locked: "
                f"{list(occurrences_to_lock.values_list('occurrence_number', flat=True))}"
            )
            occurrences_to_lock.update(locked=True)
