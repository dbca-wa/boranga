from __future__ import annotations

from dataclasses import dataclass

from boranga.components.data_migration.adapters.schema_base import Schema


@dataclass
class OccurrenceThreatRow:
    occurrence_id: int | None = None
    occurrence_report_threat_id: int | None = None
    threat_category_id: int | None = None
    threat_agent_id: int | None = None
    current_impact_id: int | None = None
    potential_impact_id: int | None = None
    potential_threat_onset_id: int | None = None
    comment: str | None = None
    date_observed: str | None = None
    visible: bool = True


COLUMN_MAP = {
    "POP_ID": "occurrence_id",
    "SHEETNO": "occurrence_report_threat_id",
    "THREAT_CODE": "threat_category_id",
    "AGENT_CODE": "threat_agent_id",
    "CUR_IMPACT": "current_impact_id",
    "POT_IMPACT": "potential_impact_id",
    "ONSET": "potential_threat_onset_id",
    "COMMENTS": "comment",
    "OBSERVATION_DATE": "date_observed",
}

SCHEMA = Schema(
    column_map=COLUMN_MAP,
    required=[],
    pipelines={},
)

# Convenience exports
map_raw_row = SCHEMA.map_raw_row
COLUMN_PIPELINES = SCHEMA.effective_pipelines()
