import logging

from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils import timezone
from django_cron.models import CronJobLog

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Delete CronJobLog entries older than CLEAR_CRON_JOB_LOGS_DAYS_TO_KEEP days (default: 30)."

    def handle(self, *args, **options):
        logger.info(f"Running command {__name__}")

        days_to_keep = settings.CLEAR_CRON_JOB_LOGS_DAYS_TO_KEEP
        cutoff = timezone.now() - timezone.timedelta(days=days_to_keep)
        deleted_count, _ = CronJobLog.objects.filter(start_time__lt=cutoff).delete()

        logger.info(f"Deleted {deleted_count} cron job log entries older than {cutoff}.")
        self.stdout.write(self.style.SUCCESS(f"Deleted {deleted_count} cron job log entries older than {cutoff}."))
