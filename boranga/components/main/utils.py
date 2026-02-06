import json
import logging
from datetime import datetime

import pytz
from django.conf import settings
from django.db import connection
from rest_framework import serializers

from boranga.helpers import member_ids
from boranga.settings import (
    GROUP_NAME_OCCURRENCE_APPROVER,
    GROUP_NAME_OCCURRENCE_ASSESSOR,
)

logger = logging.getLogger(__name__)


def to_local_tz(_date):
    local_tz = pytz.timezone(settings.TIME_ZONE)
    return _date.astimezone(local_tz)


def check_db_connection():
    """check connection to DB exists, connect if no connection exists"""
    try:
        if not connection.is_usable():
            connection.connect()
    except Exception as e:
        logger.error(f"Error connecting to DB: {e}")
        connection.connect()


def handle_validation_error(e):
    if hasattr(e, "error_dict"):
        raise serializers.ValidationError(repr(e.error_dict))
    else:
        if hasattr(e, "message"):
            raise serializers.ValidationError(e.message)
        else:
            raise


def validate_threat_request(request):
    data = json.loads(request.data.get("data"))

    required_fields = {
        "threat_category_id": "Threat Category",
        "current_impact": "Current Impact",
        "date_observed": "Date Observed",
    }
    missing_required = []

    for i in required_fields:
        if i not in data or not data[i]:
            missing_required.append(required_fields[i])

    if missing_required:
        raise serializers.ValidationError("Missing required values for Threat - " + ", ".join(missing_required))

    # data observed must not be in the future
    if (
        "date_observed" in data
        and data["date_observed"]
        and datetime.strptime(data["date_observed"], "%Y-%m-%d") > datetime.now()
    ):
        raise serializers.ValidationError("Date observed value invalid - must be on or before the current date")

    return True


def get_geometry_source(geometry_obj):
    from boranga.components.occurrence.models import OccurrenceReportGeometry

    source = ""

    if not hasattr(geometry_obj, "drawn_by"):
        source = None
    elif not geometry_obj.drawn_by:
        source = "Unknown"

    elif isinstance(geometry_obj, OccurrenceReportGeometry) and geometry_obj.drawn_by in [
        geometry_obj.occurrence_report.submitter,
    ]:
        source = "Proponent"
    else:
        assessor_ids = member_ids(GROUP_NAME_OCCURRENCE_ASSESSOR)
        approver_ids = member_ids(GROUP_NAME_OCCURRENCE_APPROVER)

        if geometry_obj.drawn_by in assessor_ids:
            source = "Assessor"
        elif geometry_obj.drawn_by in approver_ids:
            source = "Approver"

    return source
