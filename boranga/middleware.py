import re
from urllib.parse import quote_plus

from django.shortcuts import redirect
from django.urls import reverse
from reversion.middleware import RevisionMiddleware
from reversion.views import _request_creates_revision

from boranga.helpers import is_internal

CHECKOUT_PATH = re.compile("^/ledger/checkout/checkout")


class FirstTimeNagScreenMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        if (
            not request.user.is_authenticated
            or not request.method == "GET"
            or "api" in request.path
            or "admin" in request.path
            or "static" in request.path
        ):
            return self.get_response(request)

        if (
            request.user.first_name
            and request.user.last_name
            and request.user.residential_address_id
            # Don't require internal users to fill in phone numbers
            and is_internal(request)
            or (request.user.phone_number or request.user.mobile_number)
        ):
            return self.get_response(request)

        path_ft = reverse("account-firstime")
        if request.path in ("/sso/setting", path_ft, reverse("logout")):
            return self.get_response(request)

        return redirect(path_ft + "?next=" + quote_plus(request.get_full_path()))


class ReadOnlyMiddleware:
    """Blocks all write requests (POST/PUT/PATCH/DELETE) to the API when
    settings.DATA_VERIFICATION_READ_ONLY is truthy.  Django admin, authentication,
    and other non-API paths are exempt so that admin and ORM-level operations still
    work.  POST requests to *_paginated endpoints are also allowed through because
    DataTables uses POST when the querystring is too long — these are read-only
    list queries, not writes."""

    EXEMPT_PATH_PREFIXES = (
        "/admin/",
        "/ledger/",
        "/sso/",
        "/logout",
        "/ssologin",
    )
    WRITE_METHODS = frozenset({"POST", "PUT", "PATCH", "DELETE"})

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        from django.conf import settings as django_settings

        if (
            getattr(django_settings, "DATA_VERIFICATION_READ_ONLY", False)
            and request.method in self.WRITE_METHODS
            and request.path.startswith("/api/")
            and not any(request.path.startswith(p) for p in self.EXEMPT_PATH_PREFIXES)
            # Allow DataTables POST queries on paginated endpoints (read-only list queries)
            and "_paginated" not in request.path
            # Allow document list POST requests (filefield_immediate uses POST with action=list)
            and not (request.method == "POST" and request.POST.get("action") == "list")
            # Allow shapefile conversion (functionally read-only: converts GeoJSON for geometry download)
            and request.path != "/api/geojson_to_shapefile"
            # Allow report queueing (needed to run reports while in verification mode)
            and request.path != "/api/queue_report/"
        ):
            from django.http import JsonResponse

            return JsonResponse(
                {
                    "detail": "The system is currently in read-only mode for data verification. "
                    "No changes can be made at this time."
                },
                status=503,
            )

        return self.get_response(request)


class RevisionOverrideMiddleware(RevisionMiddleware):
    """
    Wraps the entire request in a revision.

    override venv/lib/python2.7/site-packages/reversion/middleware.py
    """

    # exclude ledger payments/checkout from revision - hack to overcome basket (lagging status)
    # issue/conflict with reversion
    def request_creates_revision(self, request):
        return _request_creates_revision(request) and "checkout" not in request.get_full_path()
