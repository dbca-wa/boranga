from boranga.components.data_migration.registry import (
    TransformResult,
    build_legacy_map_transform,
    fk_lookup,
    registry,
    static_value_factory,
)
from boranga.components.occurrence.models import Occurrence
from boranga.components.species_and_communities.models import CurrentImpact

from ..base import ExtractionResult, ExtractionWarning, SourceAdapter
from ..sources import Source
from . import schema


@registry.register("current_impact_fallback")
def current_impact_fallback(value, ctx):
    if value:
        return TransformResult(value)
    # If value is None/Empty, try to find "unknown"
    try:
        unknown = CurrentImpact.objects.get(name__iexact="unknown")
        return TransformResult(unknown.id)
    except CurrentImpact.DoesNotExist:
        return TransformResult(None)


PIPELINES = {
    "occurrence_id": [fk_lookup(Occurrence, "migrated_from_id")],
    "occurrence_report_threat_id": [],  # Should be None as per condition
    "threat_category_id": [
        build_legacy_map_transform(
            "TPFL",
            "THREAT_CODE (DRF_LOV_THREATS_VWS)",
            required=False,
            return_type="id",
        ),
    ],
    "threat_agent_id": [
        build_legacy_map_transform(
            "TPFL",
            "AGENT_CODE (DRF_LOV_THREAT_AGENT_VWS)",
            required=False,
            return_type="id",
        ),
    ],
    "current_impact_id": [
        build_legacy_map_transform(
            "TPFL",
            "CUR_IMPACT (DRF_LOV_THREAT_IMPACT_VWS)",
            required=False,
            return_type="id",
        ),
        "current_impact_fallback",
    ],
    "potential_impact_id": [
        build_legacy_map_transform(
            "TPFL",
            "POT_IMPACT (DRF_LOV_THREAT_IMPACT_VWS)",
            required=False,
            return_type="id",
        ),
    ],
    "potential_threat_onset_id": [
        build_legacy_map_transform(
            "TPFL", "ONSET (DRF_LOV_ONSET_VWS)", required=False, return_type="id"
        ),
    ],
    "comment": ["strip"],
    "date_observed": ["date_from_datetime_iso"],
    "visible": [static_value_factory(True)],
}


class OccurrenceThreatTpflAdapter(SourceAdapter):
    source_key = Source.TPFL.value
    domain = "occurrence_threats"

    def extract(self, path: str, **options) -> ExtractionResult:
        rows = []
        warnings: list[ExtractionWarning] = []

        raw_rows, read_warnings = self.read_table(path)
        warnings.extend(read_warnings)

        for raw in raw_rows:
            # Condition: only migrate records from DRF_POP_THREATS if SHEETNO = Null
            sheetno = raw.get("SHEETNO")
            if sheetno and str(sheetno).strip():
                continue

            canonical = schema.map_raw_row(raw)

            # Prepend source prefix to occurrence_id (migrated_from_id)
            mid = canonical.get("occurrence_id")
            if mid and not str(mid).startswith(f"{Source.TPFL.value.lower()}-"):
                canonical["occurrence_id"] = f"{Source.TPFL.value.lower()}-{mid}"

            rows.append(canonical)

        return ExtractionResult(rows=rows, warnings=warnings)


# Attach pipelines to adapter
OccurrenceThreatTpflAdapter.PIPELINES = PIPELINES
