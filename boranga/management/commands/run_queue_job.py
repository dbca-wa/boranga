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
        job_queue = JobQueue.objects.filter(status=JobQueue.STATUS_PENDING).order_by("created")[: options["limit"]]

        start_time = time.time()

        for jq in job_queue:
            # --- TOTAL QUEUE CHECK ---
            # Check if the overall cron run has been running for too long
            if time.time() - start_time > settings.QUEUE_JOB_MAX_RUN_TIME:
                self.stdout.write(
                    f"run_queue_job cron reached max run time of {settings.QUEUE_JOB_MAX_RUN_TIME} seconds. Stopping queue safely."
                )
                break

            jq.status = JobQueue.STATUS_RUNNING
            jq.save(update_fields=["status"])

            try:
                parameters = json.dumps(jq.parameters_json)
            except (TypeError, ValueError) as e:
                logger.error("run_queue_job: failed to serialise params for job %s: %s", jq.id, e)
                jq.status = JobQueue.STATUS_FAILED
                jq.error_message = str(e)
                jq.save(update_fields=["status", "error_message"])
                continue

            with connection.cursor() as cursor:
                # Set a strict 2-minute DB timeout for THIS specific item
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
