import json
import logging

from rest_framework import permissions
from rest_framework.permissions import BasePermission

from boranga.components.conservation_status.models import (
    ConservationStatus,
    ConservationStatusReferral,
)
from boranga.helpers import (
    is_conservation_status_approver,
    is_conservation_status_assessor,
    is_conservation_status_referee,
    is_contributor,
    is_occurrence_approver,
    is_occurrence_assessor,
    is_readonly_user,
    is_species_communities_approver,
)

logger = logging.getLogger(__name__)


class IsConservationStatusAssessor(BasePermission):
    def has_permission(self, request, view):
        if not request.user.is_authenticated:
            return False

        if request.user.is_superuser:
            return True

        return is_conservation_status_assessor(request)


class IsConservationStatusApprover(BasePermission):
    def has_permission(self, request, view):
        if not request.user.is_authenticated:
            return False

        if request.user.is_superuser:
            return True

        return is_conservation_status_approver(request)


class IsConservationStatusReferee(BasePermission):
    def has_permission(self, request, view):
        return is_conservation_status_referee(request)

    def has_object_permission(self, request, view, obj):
        return (
            obj.referrals.filter(referral=request.user.id)
            .exclude(processing_status=ConservationStatusReferral.PROCESSING_STATUS_RECALLED)
            .exists()
        )


class ConservationStatusPermission(BasePermission):
    def has_permission(self, request, view):
        if not request.user.is_authenticated:
            return False

        if request.user.is_superuser:
            return True

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
            or request.user.is_superuser
        )


class ExternalConservationStatusPermission(BasePermission):
    def has_permission(self, request, view):
        if not request.user.is_authenticated:
            return False

        if request.user.is_superuser:
            return True

        return (
            is_contributor(request)
            or is_readonly_user(request)
            or is_conservation_status_assessor(request)
            or is_conservation_status_approver(request)
            or is_species_communities_approver(request)
            or is_occurrence_assessor(request)
            or is_occurrence_approver(request)
        )

    def has_object_permission(self, request, view, obj):
        if request.method in permissions.SAFE_METHODS:
            return True

        if (
            hasattr(view, "action")
            and view.action == "reinstate"
            and obj.submitter == request.user.id
            and obj.processing_status == ConservationStatus.PROCESSING_STATUS_DISCARDED
        ):
            return (
                is_contributor(request)
                or is_readonly_user(request)
                or is_conservation_status_assessor(request)
                or is_conservation_status_approver(request)
                or is_species_communities_approver(request)
                or is_occurrence_assessor(request)
                or is_occurrence_approver(request)
            )

        if obj.submitter == request.user.id and obj.processing_status == ConservationStatus.PROCESSING_STATUS_DRAFT:
            return (
                is_contributor(request)
                or is_readonly_user(request)
                or is_conservation_status_assessor(request)
                or is_conservation_status_approver(request)
                or is_species_communities_approver(request)
                or is_occurrence_assessor(request)
                or is_occurrence_approver(request)
            )


class ConservationStatusReferralPermission(BasePermission):
    def has_permission(self, request, view):
        if not request.user.is_authenticated:
            return False

        if request.user.is_superuser:
            return True

        return (
            is_readonly_user(request)
            or is_conservation_status_assessor(request)
            or is_conservation_status_approver(request)
            or is_species_communities_approver(request)
            or is_occurrence_assessor(request)
            or is_occurrence_approver(request)
            # The following group has access to the conservation status referral
            # however the queryset is filtered to only include the conservation status referrals
            # they are assigned to
            or is_conservation_status_referee(request)
        )

    def has_object_permission(self, request, view, obj):
        if request.method in permissions.SAFE_METHODS:
            return True

        if obj.referral == request.user.id and obj.processing_status not in [
            ConservationStatusReferral.PROCESSING_STATUS_RECALLED
        ]:
            return is_conservation_status_referee(request)

        return is_conservation_status_assessor(request)


class ConservationStatusAmendmentRequestPermission(BasePermission):
    def has_permission(self, request, view):
        if not request.user.is_authenticated:
            return False

        if request.user.is_superuser:
            return True

        if hasattr(view, "action") and view.action == "create":
            return is_conservation_status_assessor(request)

        return (
            is_readonly_user(request)
            or is_conservation_status_assessor(request)
            or is_conservation_status_approver(request)
            or is_species_communities_approver(request)
            or is_occurrence_assessor(request)
            or is_occurrence_approver(request)
            or is_contributor(request)
        )

    def has_object_permission(self, request, view, obj):
        if request.method in permissions.SAFE_METHODS:
            return True

        return is_conservation_status_assessor(request)


class ConservationStatusDocumentPermission(BasePermission):
    def has_permission(self, request, view):
        if not request.user.is_authenticated:
            return False

        if request.user.is_superuser:
            return True

        if hasattr(view, "action") and view.action in ["create"] and request.method == "POST":
            if is_conservation_status_assessor(request) or is_conservation_status_approver(request):
                return True

            data_str = request.data.get("data", None)

            if not data_str:
                return False

            try:
                data = json.loads(data_str)
            except json.JSONDecodeError:
                return False

            conservation_status_id = data.get("conservation_status", None)

            if not conservation_status_id:
                return False
            try:
                conservation_status = ConservationStatus.objects.get(id=conservation_status_id)
            except ConservationStatus.DoesNotExist:
                return False

            return (
                conservation_status.submitter == request.user.id
                and (
                    is_readonly_user(request)
                    or is_conservation_status_assessor(request)
                    or is_conservation_status_approver(request)
                    or is_species_communities_approver(request)
                    or is_occurrence_assessor(request)
                    or is_occurrence_approver(request)
                    or is_contributor(request)
                    or is_conservation_status_referee(request)
                )
                and conservation_status.processing_status == ConservationStatus.PROCESSING_STATUS_DRAFT
            ) or (  # Check if the user is a referee for the document
                view.basename == "conservationstatusdocument"
                and conservation_status.referrals.filter(referral=request.user.id).exists()
                and is_conservation_status_referee(request)
                and conservation_status.processing_status == ConservationStatus.PROCESSING_STATUS_WITH_REFERRAL
            )

        return (
            is_readonly_user(request)
            or is_conservation_status_assessor(request)
            or is_conservation_status_approver(request)
            or is_species_communities_approver(request)
            or is_occurrence_assessor(request)
            or is_occurrence_approver(request)
            or is_contributor(request)
            or is_conservation_status_referee(request)
        )

    def has_object_permission(self, request, view, obj):
        if request.method in permissions.SAFE_METHODS:
            return True

        if (
            obj.conservation_status.submitter == request.user.id
            and obj.conservation_status.processing_status == ConservationStatus.PROCESSING_STATUS_DRAFT
        ):
            if view.action in ["update", "discard", "reinstate"]:
                return (
                    is_readonly_user(request)
                    or is_conservation_status_assessor(request)
                    or is_conservation_status_approver(request)
                    or is_species_communities_approver(request)
                    or is_occurrence_assessor(request)
                    or is_occurrence_approver(request)
                    or is_contributor(request)
                )
        # Check if the user is a referee for the document
        if (
            obj.conservation_status.referrals.filter(referral=request.user.id).exists()
            and obj.conservation_status.processing_status == ConservationStatus.PROCESSING_STATUS_WITH_REFERRAL
        ):
            return is_conservation_status_referee(request)

        return is_conservation_status_assessor(request)


class ConservationStatusExternalRefereeInvitePermission(BasePermission):
    def has_permission(self, request, view):
        if not request.user.is_authenticated:
            return False

        if request.user.is_superuser:
            return True

        if hasattr(view, "action") and view.action == "create":
            return is_conservation_status_assessor(request)

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

        return is_conservation_status_assessor(request)
