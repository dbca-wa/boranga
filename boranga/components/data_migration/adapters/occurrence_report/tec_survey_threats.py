import logging
import os

import pandas as pd

from boranga.components.data_migration.adapters.base import (
    ExtractionResult,
    SourceAdapter,
)
from boranga.components.data_migration.adapters.sources import Source
from boranga.components.data_migration.registry import (
    build_legacy_map_transform,
    static_value_factory,
)

from .threats import occurrence_report_lookup_transform

logger = logging.getLogger(__name__)


class OccurrenceReportTecSurveyThreatsAdapter(SourceAdapter):
    source_key = Source.TEC_SURVEY_THREATS.value
    domain = "occurrence_report_threats"

    PIPELINES = {
        "visible": [static_value_factory(True)],
        "threat_category": [build_legacy_map_transform("TEC", "THREATS", required=False)],
        "current_impact": [
            # If value is empty, treat as "Unknown". Relies on "Unknown" being mapped in LegacyValueMapping.
            lambda val, ctx: "Unknown" if not val else val,
            build_legacy_map_transform("TEC", "THREAT_IMPACTS", required=True),
        ],
        "potential_impact": [build_legacy_map_transform("TEC", "THREAT_IMPACTS", required=False)],
        "occurrence_report": [occurrence_report_lookup_transform],
    }

    def extract(self, path: str, **options) -> ExtractionResult:
        # Load SURVEYS.csv for date mapping
        surveys_path = path.replace("SURVEY_THREATS.csv", "SURVEYS.csv")
        if not os.path.exists(surveys_path):
            surveys_path = os.path.join(os.path.dirname(path), "SURVEYS.csv")

        survey_dates = {}  # (occ_id, sur_no) -> sur_date
        if os.path.exists(surveys_path):
            try:
                df = pd.read_csv(surveys_path, dtype=str).fillna("")
                for _, row in df.iterrows():
                    key = (row.get("OCC_UNIQUE_ID"), row.get("SUR_NO"))
                    if key[0] and key[1]:
                        survey_dates[key] = row.get("SUR_DATE")
            except Exception as e:
                logger.error(f"Failed to load SURVEYS.csv: {e}")
        else:
            logger.warning(f"SURVEYS.csv not found at {surveys_path}. Dates will be missing.")

        raw_rows, warnings = self.read_table(path)
        rows = []
        for raw in raw_rows:
            # Construct migrated_from_id for Occurrence Report identification
            occ_id = raw.get("OCC_UNIQUE_ID")
            sur_no = raw.get("SUR_NO")

            if not occ_id or not sur_no:
                logger.debug("Skipping row missing OCC_UNIQUE_ID/SUR_NO")
                continue

            # migrated_from_id MUST match what tec_surveys.py generates:
            # f"Survey {sur_no} of OCC {occ_id}"
            migrated_from_id_val = f"Survey {sur_no} of OCC {occ_id}"

            # Comments
            comments_parts = []
            if raw.get("THR_COMMENTS"):
                comments_parts.append(raw["THR_COMMENTS"])
            if raw.get("THR_PERCENT"):
                comments_parts.append(f"Current % Affected: {raw['THR_PERCENT']}")
            if raw.get("THR_MODIFICATION"):
                comments_parts.append(raw["THR_MODIFICATION"])

            comment = "; ".join(comments_parts)

            # Date Observed from JOIN
            date_observed = survey_dates.get((occ_id, sur_no))

            row = {
                # Canonical fields
                # handler expects 'occurrence_report' to be transformed into FK
                "occurrence_report": migrated_from_id_val,
                "current_impact": raw.get("THR_HIST_IMP_CODE"),
                "potential_impact": raw.get("THR_POT_IMP_CODE"),
                "threat_category": raw.get("THR_THREAT_CODE"),
                "comment": comment,
                "date_observed": date_observed,
                "visible": True,
                # For debugging
                "_source_row": raw,
            }
            rows.append(row)

        return ExtractionResult(rows=rows, warnings=warnings)
