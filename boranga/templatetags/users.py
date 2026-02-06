import logging
from datetime import timedelta

import pytz
from django.conf import settings
from django.template import Library
from django.utils import timezone

from boranga import helpers as boranga_helpers
from boranga.components.main.models import SystemMaintenance

register = Library()


logger = logging.getLogger(__name__)


@register.simple_tag(takes_context=True)
def is_django_admin(context):
    request = context["request"]
    return boranga_helpers.is_django_admin(request)


@register.simple_tag(takes_context=True)
def is_internal(context):
    # checks if user is a departmentuser and logged in via single sign-on
    request = context["request"]
    return boranga_helpers.is_internal(request)


@register.simple_tag(takes_context=True)
def is_conservation_status_assessor(context):
    request = context["request"]
    return boranga_helpers.is_conservation_status_assessor(request)


@register.simple_tag(takes_context=True)
def is_conservation_status_approver(context):
    request = context["request"]
    return boranga_helpers.is_conservation_status_approver(request)


@register.simple_tag(takes_context=True)
def is_occurrence_assessor(context):
    request = context["request"]
    return boranga_helpers.is_occurrence_assessor(request)


@register.simple_tag(takes_context=True)
def is_occurrence_approver(context):
    request = context["request"]
    return boranga_helpers.is_occurrence_approver(request)


@register.simple_tag(takes_context=True)
def is_readonly_user(context):
    request = context["request"]
    return boranga_helpers.is_readonly_user(request)


@register.simple_tag(takes_context=True)
def is_species_communities_approver(context):
    request = context["request"]
    return boranga_helpers.is_species_communities_approver(request)


@register.simple_tag(takes_context=True)
def is_contributor(context):
    request = context["request"]
    return boranga_helpers.is_contributor(request)


@register.simple_tag(takes_context=True)
def is_external_contributor(context):
    request = context["request"]
    return boranga_helpers.is_external_contributor(request)


@register.simple_tag(takes_context=True)
def is_internal_contributor(context):
    request = context["request"]
    return boranga_helpers.is_internal_contributor(request)


@register.simple_tag(takes_context=True)
def show_internal_menu_items(context):
    request = context["request"]
    if not request.user.is_authenticated:
        return False
    return (
        request.user.is_superuser
        or boranga_helpers.is_conservation_status_approver(request)
        or boranga_helpers.is_conservation_status_assessor(request)
        or boranga_helpers.is_occurrence_approver(request)
        or boranga_helpers.is_occurrence_assessor(request)
        or boranga_helpers.is_readonly_user(request)
        or boranga_helpers.is_species_communities_approver(request)
    )


@register.simple_tag(takes_context=True)
def show_meetings_menu_item(context):
    request = context["request"]
    if not request.user.is_authenticated:
        return False
    return (
        request.user.is_superuser
        or boranga_helpers.is_conservation_status_approver(request)
        or boranga_helpers.is_conservation_status_assessor(request)
        or boranga_helpers.is_occurrence_approver(request)
        or boranga_helpers.is_occurrence_assessor(request)
        or boranga_helpers.is_readonly_user(request)
        or boranga_helpers.is_species_communities_approver(request)
    )


@register.simple_tag()
def system_maintenance_due():
    """Returns True (actually a time str), if within <timedelta hours> of system maintenance due datetime"""
    tz = pytz.timezone(settings.TIME_ZONE)
    now = timezone.now()  # returns UTC time
    qs = SystemMaintenance.objects.filter(start_date__gte=now - timedelta(minutes=1))
    if qs:
        obj = qs.earliest("start_date")
        if now >= obj.start_date - timedelta(
            hours=settings.SYSTEM_MAINTENANCE_WARNING
        ) and now <= obj.start_date + timedelta(minutes=1):
            # display time in local timezone
            return f"{obj.start_date.astimezone(tz=tz).ctime()} - {obj.end_date.astimezone(tz=tz).ctime()} (Duration: {obj.duration()} mins)"
    return False


@register.simple_tag()
def system_maintenance_can_start():
    """Returns True if current datetime is within 1 minute past scheduled start_date"""
    now = timezone.now()  # returns UTC time
    qs = SystemMaintenance.objects.filter(start_date__gte=now - timedelta(minutes=1))
    if qs:
        obj = qs.earliest("start_date")
        if now >= obj.start_date and now <= obj.start_date + timedelta(minutes=1):
            return True
    return False


@register.simple_tag()
def dept_support_phone2():
    return settings.DEPT_NAME
