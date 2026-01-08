from ledger_api_client.ledger_models import EmailUserRO

from boranga.components.data_migration.mappings import get_group_type_id
from boranga.components.data_migration.registry import (
    _result,
    dependent_from_column_factory,
    emailuser_by_legacy_username_factory,
    registry,
    static_value_factory,
)

from ..base import ExtractionResult, SourceAdapter
from ..sources import Source
from . import schema

DUMMY_TEC_USER = "boranga.tec@dbca.wa.gov.au"


def _fallback_dummy():
    try:
        u = EmailUserRO.objects.get(email__iexact=DUMMY_TEC_USER)
        return _result(u.id)
    except EmailUserRO.DoesNotExist:
        # Provide a meaningful error if dummy user is missing - this is critical setup data
        return _result(None)


# Use the standard factory to get the user from the username
EMAILUSER_BY_LEGACY_USERNAME_TRANSFORM = emailuser_by_legacy_username_factory("TEC")


def tec_user_lookup(value, ctx):
    """
    Resolve TEC user.
    - If mapped from Survey USERNAME or CREATED_BY (and not empty): try lookup using factory.
    - If lookup returns None (or value was empty, like in Site Visit), fallback to Dummy user.
    """
    # 1. Try standard lookup if value is present
    if value:
        # Call the factory-registered transform logic directly via registry
        fn = registry._fns.get(EMAILUSER_BY_LEGACY_USERNAME_TRANSFORM)
        if fn:
            res = fn(value, ctx)
            # Only return if successful (no errors)
            if res.value and not res.errors:
                return res

    # 2. Fallback to Dummy User
    return _fallback_dummy()


TEC_USER_LOOKUP = tec_user_lookup


PIPELINES = {
    "internal_application": [static_value_factory(True)],
    "submitter": [TEC_USER_LOOKUP],
    # Copy submitter to other user fields
    "assigned_approver_id": [
        dependent_from_column_factory("submitter", mapping=TEC_USER_LOOKUP)
    ],
    "assigned_officer_id": [
        dependent_from_column_factory("submitter", mapping=TEC_USER_LOOKUP)
    ],
    "approved_by": [
        dependent_from_column_factory("submitter", mapping=TEC_USER_LOOKUP)
    ],
    # Also populate SubmitterInformation with the same user
    "SubmitterInformation__email_user": [
        dependent_from_column_factory("submitter", mapping=TEC_USER_LOOKUP)
    ],
    "processing_status": [
        lambda val, ctx: _result("Approved") if not val else _result(val)
    ],
    "customer_status": [
        dependent_from_column_factory(
            "processing_status",
            mapper=lambda val, ctx: "Approved" if val == "Approved" else None,
        )
    ],
}


class OccurrenceReportTecSitesAdapter(SourceAdapter):
    source_key = Source.TEC_SITES.value
    domain = "occurrence_report"

    PIPELINES = PIPELINES

    def extract(self, path: str, **options) -> ExtractionResult:
        # Get community group id safely
        community_group_id = get_group_type_id("community")

        raw_rows, warnings = self.read_table(path)
        rows = []

        for raw in raw_rows:
            canonical = schema.map_raw_row(raw)
            canonical["group_type_id"] = community_group_id

            # Sites rely on SITE_VISIT_ID mapping in schema to populate migrated_from_id
            if not canonical.get("migrated_from_id"):
                # warning?
                pass

            rows.append(canonical)

        return ExtractionResult(rows=rows, warnings=warnings)


class OccurrenceReportTecSurveysAdapter(SourceAdapter):
    source_key = Source.TEC_SURVEYS.value
    domain = "occurrence_report"

    PIPELINES = PIPELINES

    def extract(self, path: str, **options) -> ExtractionResult:
        # Get community group id safely
        community_group_id = get_group_type_id("community")

        raw_rows, warnings = self.read_table(path)
        rows = []

        for raw in raw_rows:
            canonical = schema.map_raw_row(raw)
            canonical["group_type_id"] = community_group_id

            # Structural fix for Survey ID
            sur_no = raw.get("SUR_NO")
            occ_id = raw.get("OCC_UNIQUE_ID")

            # Construct migrated_from_id for Surveys
            if sur_no and occ_id:
                canonical["migrated_from_id"] = f"Survey {sur_no} of OCC {occ_id}"

            # Ensure SUR_NO is in canonical row so tec_user_lookup can find it in context
            if sur_no:
                canonical["SUR_NO"] = sur_no

            rows.append(canonical)

        return ExtractionResult(rows=rows, warnings=warnings)
