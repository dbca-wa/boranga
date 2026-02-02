import csv
import os

from boranga.components.data_migration.adapters.base import (
    ExtractionResult,
    ExtractionWarning,
    SourceAdapter,
)
from boranga.components.data_migration.registry import (
    TransformIssue,
    _result,
    build_legacy_map_transform,
    static_value_factory,
)
from boranga.components.occurrence.models import OccurrenceReport, OCRConservationThreat

from ..sources import Source


def preload_observation_dates(path: str) -> dict[str, str]:
    """
    Load DRF_RFR_FORMS.csv into a dict:
    SHEETNO -> OBSERVATION_DATE
    """
    if not os.path.exists(path):
        return {}

    mapping = {}
    with open(path, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            sheetno = row.get("SHEETNO", "").strip()
            obs_date = row.get("OBSERVATION_DATE", "").strip()
            if sheetno and obs_date:
                mapping[sheetno] = obs_date
    return mapping


def occurrence_report_lookup_transform(value, ctx):
    # Cache on function attribute
    if not hasattr(occurrence_report_lookup_transform, "_cache"):
        mapping = dict(
            OccurrenceReport.objects.filter(migrated_from_id__isnull=False).values_list("migrated_from_id", "pk")
        )
        occurrence_report_lookup_transform._cache = mapping

    if value in (None, ""):
        return _result(None)

    val_str = str(value)
    unique_id = occurrence_report_lookup_transform._cache.get(val_str)

    if unique_id:
        return _result(unique_id)

    return _result(
        value,
        TransformIssue("error", f"OccurrenceReport with migrated_from_id='{value}' not found"),
    )


class OCRConservationThreatAdapter(SourceAdapter):
    source_key = Source.TPFL.value
    domain = "ocr_conservation_threat"
    model = OCRConservationThreat

    PIPELINES = {
        "occurrence_report_id": [occurrence_report_lookup_transform],
        "threat_category": [
            build_legacy_map_transform(
                "TPFL",
                "THREAT_CODE (DRF_LOV_THREATS_VWS)",
                required=False,
                return_type="id",
            ),
        ],
        "threat_agent": [
            build_legacy_map_transform(
                "TPFL",
                "AGENT_CODE (DRF_LOV_THREAT_AGENT_VWS)",
                required=False,
                return_type="id",
            ),
        ],
        "current_impact": [
            build_legacy_map_transform(
                "TPFL",
                "CUR_IMPACT (DRF_LOV_THREAT_IMPACT_VWS)",
                required=False,
                return_type="id",
            ),
        ],
        "potential_impact": [
            build_legacy_map_transform(
                "TPFL",
                "POT_IMPACT (DRF_LOV_THREAT_IMPACT_VWS)",
                required=False,
                return_type="id",
            ),
        ],
        "potential_threat_onset": [
            build_legacy_map_transform("TPFL", "ONSET (DRF_LOV_ONSET_VWS)", required=False, return_type="id"),
        ],
        "comment": [],
        "date_observed": ["date_from_datetime_iso"],
        "visible": [static_value_factory(True)],
    }

    def extract(self, path: str, **options) -> ExtractionResult:
        rows = []
        warnings: list[ExtractionWarning] = []

        # Preload observation dates
        # Assuming DRF_RFR_FORMS.csv is in the same directory as the source file (legacy_data/TPFL)
        # But path passed to extract is the path to DRF_SHEET_THREATS.csv
        # So I can deduce the path to DRF_RFR_FORMS.csv

        base_dir = os.path.dirname(path)
        rfr_forms_path = os.path.join(base_dir, "DRF_RFR_FORMS.csv")

        obs_dates_map = preload_observation_dates(rfr_forms_path)

        raw_rows, read_warnings = self.read_table(path)
        warnings.extend(read_warnings)

        for raw in raw_rows:
            canonical = {}

            # Map fields
            sheetno = raw.get("SHEETNO")
            if sheetno and not str(sheetno).startswith(f"{Source.TPFL.value.lower()}-"):
                sheetno = f"{Source.TPFL.value.lower()}-{sheetno}"

            canonical["occurrence_report_id"] = sheetno
            canonical["threat_category"] = raw.get("THREAT_CODE")
            canonical["threat_agent"] = raw.get("AGENT_CODE")
            canonical["current_impact"] = raw.get("CUR_IMPACT")
            canonical["potential_impact"] = raw.get("POT_IMPACT")
            canonical["potential_threat_onset"] = raw.get("ONSET")
            canonical["comment"] = raw.get("COMMENTS")

            # Map date_observed from preloaded map
            sheetno = raw.get("SHEETNO", "").strip()
            if sheetno in obs_dates_map:
                canonical["date_observed"] = obs_dates_map[sheetno]

            canonical["visible"] = True

            rows.append(canonical)

        return ExtractionResult(rows=rows, warnings=warnings)
