from boranga.components.data_migration.mappings import get_group_type_id
from boranga.components.data_migration.registry import (
    _result,
    dependent_from_column_factory,
    static_value_factory,
)

from ..base import ExtractionResult, SourceAdapter
from ..sources import Source
from . import schema
from .tec_shared import TEC_USER_LOOKUP


class OccurrenceReportTecSurveysAdapter(SourceAdapter):
    source_key = Source.TEC_SURVEYS.value
    domain = "occurrence_report"

    PIPELINES = {
        "internal_application": [static_value_factory(True)],
        "submitter": [TEC_USER_LOOKUP, "required"],
        # Copy submitter to other user fields
        "assigned_approver_id": [dependent_from_column_factory("submitter", mapping=TEC_USER_LOOKUP)],
        "assigned_officer_id": [dependent_from_column_factory("submitter", mapping=TEC_USER_LOOKUP)],
        "approved_by": [dependent_from_column_factory("submitter", mapping=TEC_USER_LOOKUP)],
        # Also populate SubmitterInformation with the same user
        "SubmitterInformation__email_user": [dependent_from_column_factory("submitter", mapping=TEC_USER_LOOKUP)],
        "processing_status": [lambda val, ctx: _result("Approved") if not val else _result(val)],
        "customer_status": [
            dependent_from_column_factory(
                "processing_status",
                mapper=lambda val, ctx: "Approved" if val == "Approved" else None,
            )
        ],
    }

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
                canonical["migrated_from_id"] = f"tec-survey-{sur_no}-occ-{occ_id}"

            # Ensure SUR_NO is in canonical row so tec_user_lookup can find it in context
            if sur_no:
                canonical["SUR_NO"] = sur_no

            rows.append(canonical)

        return ExtractionResult(rows=rows, warnings=warnings)
