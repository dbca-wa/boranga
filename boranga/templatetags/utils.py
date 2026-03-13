import base64
import logging
import os

from django.conf import settings
from django.contrib.staticfiles import finders
from django.template import Library
from django.utils.safestring import mark_safe

logger = logging.getLogger(__name__)

register = Library()


@register.simple_tag()
def application_version():
    return settings.APPLICATION_VERSION


@register.simple_tag()
def system_name():
    return settings.SYSTEM_NAME


@register.simple_tag()
def system_name_short():
    return settings.SYSTEM_NAME_SHORT


@register.simple_tag()
def support_email():
    return settings.SUPPORT_EMAIL


@register.simple_tag()
def dept_name():
    return settings.DEP_NAME


@register.simple_tag()
def dept_support_phone():
    return settings.DEP_PHONE_SUPPORT


@register.simple_tag()
def can_show_tests():
    return settings.SHOW_TESTS_URL


@register.filter
def total_line_price(price, qty):
    return f"{round(price * qty, 2):.2f}"


@register.filter
def basket_total_price(lines):
    total = 0.00
    for line in lines:
        total += line["price_incl_tax"] * line["quantity"]
    return f"{round(total, 2):.2f}"


@register.filter
def dict_get(d, key):
    try:
        return d.get(key, "")
    except AttributeError:
        return ""


@register.simple_tag()
def static_image_base64(relative_path):
    """Return a base64 data URI for a static image file.

    Usage: {% static_image_base64 'boranga/img/CBS.png' %}
    """
    result = finders.find(relative_path)
    if result is None:
        # Fall back to STATIC_ROOT for collected static files
        result = os.path.join(settings.STATIC_ROOT, relative_path)

    try:
        with open(result, "rb") as f:
            encoded = base64.b64encode(f.read()).decode("ascii")
        return mark_safe(f"data:image/png;base64,{encoded}")
    except Exception:
        logger.exception("Failed to read static image %s for base64 encoding", relative_path)
        return f"{settings.PUBLIC_URL}/static/{relative_path}"
