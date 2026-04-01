"""
Management command that processes pending items in the JobQueue.

Picks up to N pending jobs, marks them as running, executes the referenced
management command, and sets the status to completed or failed.

Invoked by ``CronJobProcessReportQueue`` (every 2 minutes via django-cron).
"""

import json
import logging

from django.core import management
from django.core.management.base import BaseCommand
from django.utils import timezone

from boranga.components.main.models import JobQueue

logger = logging.getLogger(__name__)

NUMBER_OF_QUEUE_JOBS = 3


class Command(BaseCommand):
    help = "Process pending jobs in the JobQueue."

    def handle(self, *args, **options):
        job_queue = JobQueue.objects.filter(status=JobQueue.STATUS_PENDING).order_by("created")[:NUMBER_OF_QUEUE_JOBS]

        for jq in job_queue:
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
