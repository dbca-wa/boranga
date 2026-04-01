"""
Management command invoked by the job queue to generate and email a dashboard
report to the requesting internal user.

Usage (called programmatically by ``run_queue_job``):
    manage.py email_exports '<json_params>' <user_id>
"""

import json
import logging

from django.core.management.base import BaseCommand
from ledger_api_client.ledger_models import EmailUserRO

from boranga.components.emails.emails import TemplateEmailBase
from boranga.components.main.export_utils import (
    EXPORT_MODELS,
    MAX_NUM_ROWS_MODEL_EXPORT,
    export_model_data,
    format_export_data,
)
from boranga.helpers import is_internal_by_user_id

logger = logging.getLogger(__name__)


class ReportAttachedEmail(TemplateEmailBase):
    subject = "Boranga - Report Attached"
    html_template = "boranga/emails/report_attached.html"
    txt_template = "boranga/emails/report_attached.txt"


class Command(BaseCommand):
    help = "Generate a dashboard export and email it to the requesting user."

    def add_arguments(self, parser):
        parser.add_argument("parameters", type=str)
        parser.add_argument("user_id", type=int)

    def handle(self, *args, **options):
        user_id = options["user_id"]
        params = json.loads(options["parameters"])

        try:
            user = EmailUserRO.objects.get(id=user_id)
        except EmailUserRO.DoesNotExist:
            logger.error("email_exports: user id %s not found", user_id)
            return

        if not is_internal_by_user_id(user_id):
            logger.error("email_exports: user %s is not an internal user", user_id)
            return

        model = params.get("model")
        if not model:
            logger.error("email_exports: no model specified in parameters")
            return

        fmt = params.get("format", "csv")
        num_records = params.get("num_records", MAX_NUM_ROWS_MODEL_EXPORT)
        try:
            num_records = min(int(num_records), MAX_NUM_ROWS_MODEL_EXPORT)
        except (TypeError, ValueError):
            num_records = MAX_NUM_ROWS_MODEL_EXPORT

        filters = params.get("filters") or {}
        if isinstance(filters, str):
            filters = json.loads(filters)

        data = export_model_data(model, filters, num_records)
        if data is None:
            logger.error("email_exports: unknown model '%s'", model)
            return

        # Resolve group type label for filename
        group_type = params.get("group_type", "")
        gt_label_map = {"flora": "Flora", "fauna": "Fauna", "community": "Communities"}
        group_type_label = gt_label_map.get(group_type, "")
        include_group_type = not group_type or group_type == "all"

        attachment = format_export_data(model, data, fmt, group_type_label, include_group_type=include_group_type)
        if attachment is None:
            logger.error("email_exports: failed to format data for model '%s'", model)
            return

        label = EXPORT_MODELS[model]["label"] if model in EXPORT_MODELS else model.replace("_", " ").title()
        if group_type_label:
            label = f"{label} - {group_type_label}"
        email = ReportAttachedEmail()
        email.subject = f"Boranga - {label} - Report Export"
        context = {"recipient": user, "model": label}
        email.send(user.email, context=context, attachments=[attachment])
        logger.info("email_exports: sent %s report to %s", model, user.email)
