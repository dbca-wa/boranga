import logging

from rest_framework import permissions
from rest_framework.permissions import BasePermission

from boranga.helpers import (
    is_conservation_status_approver,
    is_conservation_status_assessor,
    is_occurrence_approver,
    is_occurrence_assessor,
    is_readonly_user,
    is_species_communities_approver,
)

logger = logging.getLogger(__name__)


class SpeciesCommunitiesPermission(BasePermission):
    def has_permission(self, request, view):
        if hasattr(view, "action") and view.action == "create":
            return is_species_communities_approver(request)

        return (
            is_readonly_user(request)
            or is_conservation_status_assessor(request)
            or is_conservation_status_approver(request)
            or is_species_communities_approver(request)
            or is_occurrence_assessor(request)
            or is_occurrence_approver(request)
        )

    def has_object_permission(self, request, view, obj):
        if request.method in permissions.SAFE_METHODS:
            return True

        return is_species_communities_approver(request)


class ConservationThreatPermission(BasePermission):
    def has_permission(self, request, view):
        if hasattr(view, "action") and view.action == "create":
            return is_species_communities_approver(request)

        return request.user.is_authenticated

    def has_object_permission(self, request, view, obj):
        if request.method in permissions.SAFE_METHODS:
            return True

        return is_species_communities_approver(request)
