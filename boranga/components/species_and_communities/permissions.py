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
            if is_internal(request):
                return True
            if is_conservation_status_referee(request):
                # Check if the user is a referee for a conservation status related to this species/community
                from boranga.components.conservation_status.models import (
                    ConservationStatusReferral,
                )

                # Check if the object is a Species or Community
                if hasattr(obj, "conservation_status"):
                    # This covers Species and Community models as they have reverse relation 'conservation_status'
                    # So obj.conservation_status.all() gives all CS for this species.

                    # We need to check if ANY of these CS has a referral for this user.
                    return ConservationStatusReferral.objects.filter(
                        referral=request.user.id,
                        conservation_status__in=obj.conservation_status.all(),
                    ).exists()

            return False

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
