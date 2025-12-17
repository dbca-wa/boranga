from __future__ import annotations

from dataclasses import dataclass

from boranga.components.data_migration.adapters.schema_base import Schema

COLUMN_MAP = {
    "COM_FORMER_RANGE": "former_range",
    "COM_RANGE_DECLINE": "range_decline",
    "COM_OCC_DECLINE": "occ_decline",
    "COM_ID": "community_common_id",
    "COM_DESC": "community_description",
    "COM_NO": "migrated_from_id",
    "COM_NAME": "community_name",
    "COM_ORIG_AREA": "community_original_area",
    "COM_AREA_ACC": "community_original_area_accuracy",
    "Distribution": "distribution",
}

REQUIRED_COLUMNS = [
    "migrated_from_id",
]

PIPELINES: dict[str, list[str]] = {}

SCHEMA = Schema(
    column_map=COLUMN_MAP,
    required=REQUIRED_COLUMNS,
    pipelines=PIPELINES,
    source_choices=None,
)

# Re-export convenience functions
normalise_header = SCHEMA.normalise_header
canonical_key = SCHEMA.canonical_key
required_missing = SCHEMA.required_missing
validate_headers = SCHEMA.validate_headers
map_raw_row = SCHEMA.map_raw_row
COLUMN_PIPELINES = SCHEMA.effective_pipelines()


@dataclass
class CommunityRow:
    """
    Canonical (post-transform) community row used for persistence.
    """

    migrated_from_id: str
    former_range: str | None = None
    range_decline: str | None = None
    occ_decline: str | None = None
    community_common_id: str | None = None
    community_description: str | None = None
    community_name: str | None = None
    community_original_area: float | None = None
    community_original_area_accuracy: float | None = None
    distribution: str | None = None
