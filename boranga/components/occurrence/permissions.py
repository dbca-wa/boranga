import json
import logging

from ledger_api_client.ledger_models import EmailUserRO as EmailUser
from rest_framework import permissions, serializers
from rest_framework.permissions import BasePermission

from boranga.components.occurrence.models import (
    Occurrence,
    OccurrenceReport,
    OccurrenceReportReferral,
)
from boranga.helpers import (
    is_conservation_status_approver,
    is_conservation_status_assessor,
    is_contributor,
    is_django_admin,
    is_occurrence_approver,
    is_occurrence_assessor,
    is_occurrence_report_referee,
    is_readonly_user,
    is_species_communities_approver,
)

logger = logging.getLogger(__name__)


class IsOccurrenceReportReferee(BasePermission):
    def has_permission(self, request, view):
        return is_occurrence_report_referee(request)

    def has_object_permission(self, request, view, obj):
        if obj._meta.model_name == "occurrencereport":
            if (
                obj.referrals.filter(referral=request.user.id)
                .exclude(processing_status=OccurrenceReportReferral.PROCESSING_STATUS_RECALLED)
                .exists()
            ):
                return True
            # NOTE replace/remove when process_shapefile_document is reworked
            elif hasattr(view, "action") and view.action == "process_shapefile_document":
                return obj.referrals.filter(referral=request.user.id).exists()

        else:
            return obj.referral == request.user.id

        return False


class OccurrenceReportPermission(BasePermission):
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
            or is_occurrence_report_referee(request)
        )

    def is_authorised_to_update(self, request, obj):
        return (
            (obj.can_user_edit(request) and (request.user.id == obj.submitter or request.user.is_superuser))
            or (obj.has_assessor_mode(request))
            or (obj.has_unlocked_mode(request))
        )

    def is_authorised_to_approve(self, request, obj):
        return obj.has_approver_mode(request)

    def is_authorised_to_assess(self, request, obj):
        return obj.has_assessor_mode(request)

    def is_authorised_to_assign(self, request, obj, assigner, assignee=None):
        # To assign a report:
        # - the report must be under assessment, the assigner must be in the assessment group,
        # and the assignee must be in the assessment group or
        # - the report must be under approval, the assigner must be in the approver group,
        # and the assignee must be in the approval group
        # AND the Assignee must be the proposed assignee, or already assigned
        in_assessor_group = assignee and (is_occurrence_assessor(request) or request.user.is_superuser)
        in_approver_group = assignee and (is_occurrence_approver(request) or request.user.is_superuser)

        self_assigning = assigner == assignee

        assigner_assigned = obj.assigned_officer == assigner.id
        assigner_approver = obj.assigned_approver == assigner.id

        return (
            (
                obj.processing_status
                in [
                    OccurrenceReport.PROCESSING_STATUS_WITH_REFERRAL,
                    OccurrenceReport.PROCESSING_STATUS_WITH_ASSESSOR,
                    OccurrenceReport.PROCESSING_STATUS_UNLOCKED,
                ]
            )
            and (
                (self_assigning and (in_assessor_group or in_approver_group))
                or (not (assignee) and assigner_assigned and obj.has_assessor_mode(request))
                or ((in_assessor_group or in_approver_group) and assigner_assigned and obj.has_assessor_mode(request))
            )
        ) or (
            (
                obj.processing_status
                in [
                    OccurrenceReport.PROCESSING_STATUS_WITH_APPROVER,
                ]
            )
            and (
                (self_assigning and in_approver_group)
                or (not (assignee) and assigner_approver and obj.has_approver_mode(request))
                or ((in_approver_group) and assigner_assigned and obj.has_assessor_mode(request))
            )
        )

    def is_authorised_to_change_lock(self, request, obj):
        return obj.can_change_lock(request)

    def is_authorised_to_update_show_on_map(self, request, obj):
        if not obj.occurrence:
            return False

        return is_occurrence_approver(request) or is_occurrence_assessor(request)

    def has_object_permission(self, request, view, obj):
        if request.method in permissions.SAFE_METHODS or view.action == "process_shapefile_document":
            return True

        if view.action in ["propose_decline", "propose_approve", "send_referral"]:
            return self.is_authorised_to_assess(request, obj)

        if (
            view.action == "back_to_assessor"
            and obj.processing_status == OccurrenceReport.PROCESSING_STATUS_WITH_APPROVER
        ) or view.action in ["decline", "approve"]:
            return self.is_authorised_to_approve(request, obj)

        if view.action == "assign_request_user":
            self.is_authorised_to_assign(request, obj, request.user, request.user)

        if view.action == "assign_to":
            try:
                user_id = request.data.get("assessor_id", None)
                user = EmailUser.objects.get(id=user_id)
            except EmailUser.DoesNotExist:
                raise serializers.ValidationError("A user with the id passed in does not exist")
            self.is_authorised_to_assign(request, obj, request.user, user)

        if view.action == "unassign":
            self.is_authorised_to_assign(request, obj, request.user)

        if view.action in ["lock_occurrence_report", "unlock_occurrence_report"]:
            return self.is_authorised_to_change_lock(request, obj)

        if hasattr(view, "action") and view.action == "update_show_on_map":
            return self.is_authorised_to_update_show_on_map(request, obj)

        return self.is_authorised_to_update(request, obj)


class ExternalOccurrenceReportPermission(BasePermission):
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
        if request.method in permissions.SAFE_METHODS or view.action == "process_shapefile_document":
            return True

        if obj.submitter == request.user.id and (
            obj.can_user_edit(request) or (hasattr(view, "action") and view.action == "process_shapefile_document")
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
        return False


# accounts for objects that belong to an occurrence report
# only works if the object has an assigned occurrence report
class OccurrenceReportObjectPermission(BasePermission):
    def has_permission(self, request, view):
        if not request.user.is_authenticated:
            return False

        if hasattr(view, "action") and view.action == "create":
            request_data = request.data.get("data")
            if not request_data:
                raise serializers.ValidationError("No parameter named 'data' found in request")

            try:
                data = json.loads(request_data)
            except (TypeError, json.JSONDecodeError):
                raise serializers.ValidationError("Data parameter is not a valid JSON string")

            try:
                occurrence_report_id = int(data["occurrence_report"])
                occurrence_report = OccurrenceReport.objects.get(id=occurrence_report_id)
            except OccurrenceReport.DoesNotExist:
                raise serializers.ValidationError(f"No occurrence report found with id: {occurrence_report_id}")

            if view.basename == "ocr_amendment_request":
                if not occurrence_report.has_assessor_mode(request):
                    return False
            elif view.basename == "occurrencereportdocument":
                # Allow for referees to upload documents
                if not is_occurrence_report_referee(request, occurrence_report=occurrence_report):
                    return False
            elif not self.is_authorised_to_update(request, occurrence_report):
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
            or is_occurrence_report_referee(request)
        )

    def is_authorised_to_update(self, request, occurrence_report):
        user = request.user
        return (
            (
                occurrence_report.can_user_edit(request)
                and (user.id == occurrence_report.submitter or request.user.is_superuser)
            )
            or (occurrence_report.has_assessor_mode(request))
            or (occurrence_report.has_unlocked_mode(request))
            or (occurrence_report.has_referral_mode(request))
        )

    def has_object_permission(self, request, view, obj):
        if request.method in permissions.SAFE_METHODS:
            return True

        occurrence_report = obj.occurrence_report

        if obj._meta.model_name == "occurrencereportamendmentrequest" and occurrence_report:
            occurrence_report = obj.occurrence_report
            return occurrence_report.has_assessor_mode(request)

        if occurrence_report:
            return self.is_authorised_to_update(request, occurrence_report)

        return False


# accounts for objects that belong to an occurrence report and can be managed externally
# only works if the object has an assigned occurrence report
class ExternalOccurrenceReportObjectPermission(BasePermission):
    def has_permission(self, request, view):
        if not request.user.is_authenticated:
            return False

        if hasattr(view, "action") and view.action == "create":
            request_data = request.data.get("data")
            if not request_data:
                raise serializers.ValidationError("No parameter named 'data' found in request")

            try:
                data = json.loads(request_data)
            except (TypeError, json.JSONDecodeError):
                raise serializers.ValidationError("Data parameter is not a valid JSON string")

            try:
                occurrence_report_id = int(data["occurrence_report"])
                occurrence_report = OccurrenceReport.objects.get(id=occurrence_report_id)
            except OccurrenceReport.DoesNotExist:
                raise serializers.ValidationError(f"No occurrence report found with id: {occurrence_report_id}")
            if not (
                occurrence_report
                and (occurrence_report.submitter == request.user.id or request.user.is_superuser)
                and occurrence_report.can_user_edit(request)
            ):
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

        occurrence_report = obj.occurrence_report

        if (
            occurrence_report
            and occurrence_report.submitter == request.user.id
            and occurrence_report.can_user_edit(request)
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

        return False


class OccurrenceReportCopyPermission(BasePermission):
    def has_object_permission(self, request, view, obj):
        if not request.user.is_authenticated:
            return False

        if request.user.is_superuser:
            return True

        return obj.submitter == request.user.id or is_occurrence_assessor(request)


class OccurrenceReportReassignDraftPermission(BasePermission):
    def has_object_permission(self, request, view, obj):
        if not request.user.is_authenticated:
            return False

        if request.user.is_superuser:
            return True

        return obj.submitter == request.user.id or is_occurrence_approver(request)


class OccurrenceReportBulkImportPermission(BasePermission):
    def has_permission(self, request, view):
        if not request.user.is_authenticated:
            return False

        if request.user.is_superuser:
            return True

        return is_django_admin(request) or is_occurrence_approver(request)

    def has_object_permission(self, request, view, obj):
        if not request.user.is_authenticated:
            return False

        if request.user.is_superuser:
            return True

        if view.action != "copy" and request.method not in permissions.SAFE_METHODS and obj.is_master:
            return is_django_admin(request)

        return is_django_admin(request) or is_occurrence_approver(request)


class OccurrencePermission(BasePermission):
    def has_permission(self, request, view):
        if not request.user.is_authenticated:
            return False

        if request.user.is_superuser:
            return True

        if hasattr(view, "action") and view.action == "create":
            return is_occurrence_approver(request)

        return (
            is_readonly_user(request)
            or is_conservation_status_assessor(request)
            or is_conservation_status_approver(request)
            or is_species_communities_approver(request)
            or is_occurrence_assessor(request)
            or is_occurrence_approver(request)
        )

    def is_authorised_to_update(self, request, obj):
        return (is_occurrence_approver(request) or request.user.is_superuser) and (
            obj.processing_status == Occurrence.PROCESSING_STATUS_ACTIVE
            or obj.processing_status == Occurrence.PROCESSING_STATUS_DRAFT
            or obj.processing_status == Occurrence.PROCESSING_STATUS_DISCARDED
        )

    def is_authorised_to_reopen(self, request, obj):
        return (is_occurrence_approver(request) or request.user.is_superuser) and (
            obj.processing_status == Occurrence.PROCESSING_STATUS_HISTORICAL
        )

    def is_authorised_to_unlock(self, request, obj):
        return is_occurrence_approver(request)

    def is_authorised_to_update_show_on_map(self, request):
        return is_occurrence_approver(request) or is_occurrence_assessor(request)

    def has_object_permission(self, request, view, obj):
        if request.method in permissions.SAFE_METHODS:
            return True

        if hasattr(view, "action") and view.action == "unlock_occurrence":
            return self.is_authorised_to_unlock(request, obj)
        if hasattr(view, "action") and view.action == "update_show_on_map":
            return self.is_authorised_to_update_show_on_map(request)
        if hasattr(view, "action") and view.action == "reopen_occurrence":
            return self.is_authorised_to_reopen(request, obj)

        return self.is_authorised_to_update(request, obj)


class OccurrenceObjectPermission(BasePermission):
    def has_permission(self, request, view):
        if not request.user.is_authenticated:
            return False

        if request.user.is_superuser:
            return True

        if hasattr(view, "action") and view.action == "create":
            request_data = request.data.get("data")
            if not request_data:
                raise serializers.ValidationError("No parameter named 'data' found in request")

            try:
                data = json.loads(request_data)
            except (TypeError, json.JSONDecodeError):
                raise serializers.ValidationError("Data parameter is not a valid JSON string")

            try:
                occurrence_id = int(data["occurrence"])
                occurrence = Occurrence.objects.get(id=occurrence_id)
            except Occurrence.DoesNotExist:
                raise serializers.ValidationError(f"No occurrence found with id: {occurrence_id}")

            return self.is_authorised_to_update(request, occurrence)

        return (
            is_readonly_user(request)
            or is_conservation_status_assessor(request)
            or is_conservation_status_approver(request)
            or is_species_communities_approver(request)
            or is_occurrence_assessor(request)
            or is_occurrence_approver(request)
        )

    def is_authorised_to_update(self, request, occurrence):
        return (is_occurrence_approver(request) or request.user.is_superuser) and (
            occurrence.processing_status == Occurrence.PROCESSING_STATUS_ACTIVE
            or occurrence.processing_status == Occurrence.PROCESSING_STATUS_DRAFT
        )

    def has_object_permission(self, request, view, obj):
        if request.method in permissions.SAFE_METHODS:
            return True

        occurrence = obj.occurrence

        if occurrence:
            return self.is_authorised_to_update(request, occurrence)

        return False


class AssociatedSpeciesTaxonomyPermission(BasePermission):
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

        if obj.ocrassociatedspecies_set.exists():
            root_parent_record = obj.ocrassociatedspecies_set.first().occurrence_report

        elif obj.occassociatedspecies_set.exists():
            root_parent_record = obj.occassociatedspecies_set.first().occurrence
        else:
            raise serializers.ValidationError("No associated species found for this object.")

        if not root_parent_record:
            return False

        return (
            is_occurrence_assessor(request)
            or is_occurrence_approver(request)
            or root_parent_record.submitter == request.user.id
        )
