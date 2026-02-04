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


class OccurrenceReportTecSiteVisitsAdapter(SourceAdapter):
    source_key = Source.TEC_SITE_VISITS.value
    domain = "occurrence_report"

    PIPELINES = {
        "internal_application": [static_value_factory(True)],
        "submitter": [TEC_USER_LOOKUP],  # TEC_USER_LOOKUP has built-in fallback
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
        import os

        import pandas as pd

        # Get community group id safely
        community_group_id = get_group_type_id("community")

        # We are now driving from SITE_VISITS.csv (path), so we need to side-load SITES.csv
        # to get OCC_UNIQUE_ID and Spatial data.
        sites_path = path.replace("SITE_VISITS.csv", "SITES.csv")
        if not os.path.exists(sites_path):
            sites_path = os.path.join(os.path.dirname(path), "SITES.csv")

        # Cache SITES info: S_ID -> {OCC_UNIQUE_ID, ...}
        sites_lookup = {}
        if os.path.exists(sites_path):
            try:
                # Read as string to avoid type inference issues
                df = pd.read_csv(sites_path, dtype=str).fillna("")
                for _, row in df.iterrows():
                    s_id = row.get("S_ID", "").strip()
                    if s_id:
                        sites_lookup[s_id] = row.to_dict()
            except Exception:
                # In strict environment we might want to log this
                pass

        raw_rows, warnings = self.read_table(path)
        rows = []

        for raw in raw_rows:
            # Map columns from SITE_VISITS (SITE_VISIT_ID, SV_VISIT_DATE, etc)
            canonical = schema.map_raw_row(raw)
            canonical["group_type_id"] = community_group_id

            # Ensure ID is prefixed
            visit_id = raw.get("SITE_VISIT_ID")
            if visit_id:
                canonical["migrated_from_id"] = f"tec-site-{visit_id}"

            # Linking to Parent Occurrence via SITES lookup
            s_id = raw.get("S_ID")
            if s_id and s_id in sites_lookup:
                site_row = sites_lookup[s_id]

                # Get OCC_UNIQUE_ID from Site
                occ_id = site_row.get("OCC_UNIQUE_ID")
                if occ_id:
                    canonical["Occurrence__migrated_from_id"] = f"tec-{occ_id}"

                # NOTE: Geometry fields (S_LATITUDE, S_LONGITUDE) exist in site_row.
                # If Occurrence Report schema supports geometry, we should map them here.
                # Currently schema for result doesn't seem to have explicit Lat/Long fields
                # except via 'location_description' or similar, but let's keep it safe.

            else:
                # If we have a visit but no matching site, we can't link it to an Occurrence
                pass

            rows.append(canonical)

        return ExtractionResult(rows=rows, warnings=warnings)
