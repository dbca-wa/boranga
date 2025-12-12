from __future__ import annotations

from dataclasses import dataclass

from boranga.components.data_migration.adapters.schema_base import Schema

# Column header → canonical key map (adjust headers to match CSV)
COLUMN_MAP = {
    "SHEETNO": "migrated_from_id",
    "SPECIES_NAME": "species_name",
    "WA_LEGISLATIVE_CATEGORY": "wa_legislative_category",
    "WA_PRIORITY_LIST": "wa_priority_list",
    "PROCESSING_STATUS": "processing_status",
    "EFFECTIVE_FROM_DATE": "effective_from_date",
    "SUBMITTER": "submitter",
    "ASSIGNED_APPROVER": "assigned_approver",
    "COMMENT": "comment",
}

# Minimal required canonical fields for migration
REQUIRED_COLUMNS = [
    "migrated_from_id",
    "processing_status",
    "species_name",
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
class ConservationStatusRow:
    """
    Canonical (post-transform) conservation status row used for persistence.
    """

    migrated_from_id: str
    processing_status: str
    species_name: str
    wa_legislative_category: str | None = None
    wa_priority_list: str | None = None
    effective_from_date: object | None = None
    submitter: str | None = None
    assigned_approver: str | None = None
    comment: str | None = None
