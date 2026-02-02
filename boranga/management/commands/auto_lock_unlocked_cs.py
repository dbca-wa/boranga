import logging

from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils import timezone

from boranga.components.conservation_status.models import ConservationStatus

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = (
        "Auto lock unlocked conservation status records that have " "not been updated within a specified time window."
    )

    def handle(self, *args, **options):
        # Calculate the threshold time for locking
        threshold_time = timezone.now() - timezone.timedelta(
            minutes=settings.UNLOCKED_CONSERVATION_STATUS_EDITING_WINDOW_MINUTES
        )

        # Lock all unlocked conservation status records that were updated before the threshold time
        conservation_statuses_to_lock = ConservationStatus.objects.filter(
            processing_status__in=[
                ConservationStatus.PROCESSING_STATUS_CLOSED,
                ConservationStatus.PROCESSING_STATUS_DELISTED,
                ConservationStatus.PROCESSING_STATUS_APPROVED,
                ConservationStatus.PROCESSING_STATUS_DECLINED,
            ],
            locked=False,
            datetime_updated__lt=threshold_time,
        )

        if conservation_statuses_to_lock.exists():
            logger.info(
                "The following conservation status records will be locked: "
                f"{list(conservation_statuses_to_lock.values_list('conservation_status_number', flat=True))}"
            )
            conservation_statuses_to_lock.update(locked=True)
