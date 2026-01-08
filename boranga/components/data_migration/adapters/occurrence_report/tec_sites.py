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


class OccurrenceReportTecSitesAdapter(SourceAdapter):
    source_key = Source.TEC_SITES.value
    domain = "occurrence_report"

    PIPELINES = {
        "internal_application": [static_value_factory(True)],
        "submitter": [TEC_USER_LOOKUP, "required"],
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

    def extract(self, path: str, **options) -> ExtractionResult:
        # Get community group id safely
        community_group_id = get_group_type_id("community")

        raw_rows, warnings = self.read_table(path)
        rows = []

        for raw in raw_rows:
            canonical = schema.map_raw_row(raw)
            canonical["group_type_id"] = community_group_id

            # Map site (S_ID) to Occurrence link so handler can copy details/relate records
            if canonical.get("site"):
                canonical["Occurrence__migrated_from_id"] = canonical["site"]

            # Sites rely on SITE_VISIT_ID mapping in schema to populate migrated_from_id
            if not canonical.get("migrated_from_id"):
                # warning?
                pass

            rows.append(canonical)

        return ExtractionResult(rows=rows, warnings=warnings)
