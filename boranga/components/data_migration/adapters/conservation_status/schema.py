from __future__ import annotations

from dataclasses import dataclass

from boranga.components.data_migration.adapters.schema_base import Schema

# Column header → canonical key map (adjust headers to match CSV)
COLUMN_MAP = {
    "migrated_from_id": "migrated_from_id",
    "species": "species_name",
    "community": "community_migrated_from_id",
    "wa_legislative_category": "wa_legislative_category",
    "wa_legislative_list": "wa_legislative_list",
    "wa_priority_category": "wa_priority_category",
    "wa_priority_list": "wa_priority_list",
    "approved_by": "approved_by",
    "processing_status": "processing_status",
    "effective_from": "effective_from_date",
    "submitter": "submitter",
    "comment": "comment",
    "customer_status": "customer_status",
    "internal_application": "internal_application",
    "locked": "locked",
}

# Minimal required canonical fields for migration
REQUIRED_COLUMNS = [
    "migrated_from_id",
    "processing_status",
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
    wa_legislative_list: str | None = None
    wa_priority_list: str | None = None
    wa_priority_category: str | None = None
    effective_from_date: object | None = None
    submitter: str | None = None
    assigned_approver: str | None = None
    approved_by: str | None = None
    comment: str | None = None
    group_type_id: int | None = None
    customer_status: str | None = None
    locked: bool = False
    internal_application: bool = False
    approval_level: str | None = None
    review_due_date: object | None = None
