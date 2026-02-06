import logging

from rest_framework.permissions import BasePermission

from boranga.helpers import (
    is_conservation_status_approver,
    is_conservation_status_assessor,
    is_internal,
    is_occurrence_approver,
    is_occurrence_assessor,
    is_species_communities_approver,
)

logger = logging.getLogger(__name__)


class IsSuperuser(BasePermission):
    def has_permission(self, request, view):
        return request.user.is_superuser


class IsInternal(BasePermission):
    def has_permission(self, request, view):
        if not request.user.is_authenticated:
            return False

        if request.user.is_superuser:
            return True

        return is_internal(request)


class IsAssessor(BasePermission):
    def has_permission(self, request, view):
        if not request.user.is_authenticated:
            return False

        if request.user.is_superuser:
            return True

        return is_conservation_status_assessor(request) or is_occurrence_assessor(request)


class IsApprover(BasePermission):
    def has_permission(self, request, view):
        if not request.user.is_authenticated:
            return False

        if request.user.is_superuser:
            return True

        return (
            is_species_communities_approver(request)
            or is_conservation_status_approver(request)
            or is_occurrence_approver(request)
        )
