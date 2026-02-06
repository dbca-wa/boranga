from rest_framework.permissions import BasePermission

from boranga.helpers import (
    is_conservation_status_approver,
    is_conservation_status_assessor,
    is_external_contributor,
    is_internal_contributor,
    is_occurrence_approver,
    is_occurrence_assessor,
    is_occurrence_report_referee,
    is_readonly_user,
    is_species_communities_approver,
)


class TileLayerPermission(BasePermission):
    def has_permission(self, request, view):
        if not request.user.is_authenticated:
            return False

        return (
            is_readonly_user(request)
            or is_conservation_status_assessor(request)
            or is_conservation_status_approver(request)
            or is_species_communities_approver(request)
            or is_occurrence_assessor(request)
            or is_occurrence_approver(request)
            or is_internal_contributor(request)
            or is_occurrence_report_referee(request)
            or is_external_contributor(request)
        )
