"""
Management command that processes pending items in the JobQueue.

Picks up to N pending jobs, marks them as running, executes the referenced
management command, and sets the status to completed or failed.

Invoked by ``CronJobProcessReportQueue`` (every 2 minutes via django-cron).
"""

import json
import logging
from time import time

from django.conf import settings
from django.core import management
from django.core.management.base import BaseCommand
from django.db import connection
from django.db.models import F
from django.utils import timezone

from boranga.components.main.models import JobQueue

logger = logging.getLogger(__name__)

NUMBER_OF_QUEUE_JOBS = settings.NUMBER_OF_QUEUE_JOBS


class Command(BaseCommand):
    help = "Process pending jobs in the JobQueue."

    def add_arguments(self, parser):
        parser.add_argument(
            "--limit",
            type=int,
            default=NUMBER_OF_QUEUE_JOBS,
            help=f"Maximum number of jobs to process (default: {NUMBER_OF_QUEUE_JOBS}).",
        )

    def handle(self, *args, **options):
        # Find any jobs that have been running too long and mark them as pending again (in case they got stuck)
        stale_jobs = JobQueue.objects.filter(
            status=JobQueue.STATUS_RUNNING,
            started_dt__lte=timezone.now() - timezone.timedelta(seconds=settings.QUEUE_JOB_MAX_RUN_TIME),
        )
        stale_count = stale_jobs.count()
        if stale_count > 0:
            logger.warning(
                "run_queue_job: found %s stale jobs that have been running for too long. Resetting to pending.",
                stale_count,
            )
            stale_jobs.update(
                status=JobQueue.STATUS_PENDING, started_dt=None, processed_dt=None, retry_count=F("retry_count") + 1
            )

            JobQueue.objects.filter(
                status=JobQueue.STATUS_PENDING, retry_count__gte=settings.QUEUE_JOB_MAX_RETRIES
            ).update(
                status=JobQueue.STATUS_FAILED,
                error_message=f"Aborted: Job exceeded the maximum allowed retries ({settings.QUEUE_JOB_MAX_RETRIES}).",
            )

        job_queue = list(
            JobQueue.objects.filter(
                status=JobQueue.STATUS_PENDING, retry_count__lt=settings.QUEUE_JOB_MAX_RETRIES
            ).order_by("created")[: options["limit"]]
        )

        start_time = time()

        for jq in job_queue:
            # --- TOTAL QUEUE CHECK ---
            # Check if the overall cron run has been running for too long
            if time() - start_time > settings.CRON_MAX_RUN_TIME:
                self.stdout.write(
                    f"run_queue_job cron reached max run time of {settings.CRON_MAX_RUN_TIME} seconds. Stopping queue safely."
                )
                break

            jq.status = JobQueue.STATUS_RUNNING
            jq.started_dt = timezone.now()
            jq.save(update_fields=["status", "started_dt"])

            try:
                parameters = json.dumps(jq.parameters_json)
            except (TypeError, ValueError) as e:
                logger.error("run_queue_job: failed to serialise params for job %s: %s", jq.id, e)
                jq.status = JobQueue.STATUS_FAILED
                jq.error_message = str(e)
                jq.save(update_fields=["status", "error_message"])
                continue

            with connection.cursor() as cursor:
                # Set a strict DB timeout for this specific job
                cursor.execute(f"SET statement_timeout = {settings.QUEUE_JOB_ITEM_MAX_POSTGRES_RUN_TIME};")

                try:
                    management.call_command(jq.job_cmd, parameters, jq.user)
                    jq.processed_dt = timezone.now()
                    jq.status = JobQueue.STATUS_COMPLETED
                    jq.save(update_fields=["status", "processed_dt"])
                    logger.info("run_queue_job: completed job %s (%s)", jq.id, jq.job_cmd)
                except Exception as e:
                    logger.exception("run_queue_job: error processing job %s: %s", jq.id, e)
                    jq.status = JobQueue.STATUS_FAILED
                    jq.error_message = str(e)
                    jq.save(update_fields=["status", "error_message"])

                finally:
                    # Reset the DB timeout to the default for the rest of the cron run
                    cursor.execute("RESET statement_timeout;")
