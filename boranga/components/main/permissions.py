import logging

from rest_framework import permissions
from rest_framework.permissions import BasePermission

from boranga.helpers import (
    is_conservation_status_approver,
    is_conservation_status_assessor,
    is_conservation_status_referee,
    is_internal,
    is_occurrence_approver,
    is_occurrence_assessor,
    is_readonly_user,
    is_species_communities_approver,
)

logger = logging.getLogger(__name__)


class CommsLogPermission(BasePermission):
    def has_permission(self, request, view):
        if not request.user.is_authenticated:
            return False

        if request.user.is_superuser:
            return True

        if hasattr(view, "action") and view.action == "add_comms_log":
            return (
                is_conservation_status_assessor(request)
                or is_conservation_status_approver(request)
                or is_species_communities_approver(request)
                or is_occurrence_assessor(request)
                or is_occurrence_approver(request)
            )

        return (
            is_readonly_user(request)
            or is_conservation_status_assessor(request)
            or is_conservation_status_approver(request)
            or is_species_communities_approver(request)
            or is_occurrence_assessor(request)
            or is_occurrence_approver(request)
            or is_conservation_status_referee(request)
        )

    def has_object_permission(self, request, view, obj):
        if request.method in permissions.SAFE_METHODS:
            return True

        return (
            is_conservation_status_assessor(request)
            or is_conservation_status_approver(request)
            or is_species_communities_approver(request)
            or is_occurrence_assessor(request)
            or is_occurrence_approver(request)
        )


class HelpTextEntryPermission(BasePermission):
    def has_object_permission(self, request, view, obj):
        if obj.authenticated_users_only:
            return request.user.is_authenticated

        if obj.internal_users_only:
            return is_internal(request)

        return True
