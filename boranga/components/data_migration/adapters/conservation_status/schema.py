from __future__ import annotations

from dataclasses import dataclass

from boranga.components.data_migration.adapters.schema_base import Schema

# Column header → canonical key map (adjust headers to match CSV)
# This is a generic map that will be source-specific
COLUMN_MAP = {
    # TPFL format (species conservation status)
    "migrated_from_id": "migrated_from_id",
    "species": "species_id",
    "community": "community_migrated_from_id",
    "wa_legislative_category": "wa_legislative_category",
    "wa_legislative_list": "wa_legislative_list",
    "wa_priority_category": "wa_priority_category",
    "wa_priority_list": "wa_priority_list",
    "approved_by": "approved_by",
    "processing_status": "processing_status",
    "effective_from": "effective_from_date",
    "effective_to": "effective_to_date",
    "listing_date": "listing_date",
    "lodgement_date": "lodgement_date",
    "submitter": "submitter",
    "comment": "comment",
    "customer_status": "customer_status",
    "internal_application": "internal_application",
    "locked": "locked",
    # TEC format (community conservation status)
    "COM_NO": "migrated_from_id",  # TEC uses COM_NO as the unique identifier
    "CAT_CT_TYPE": "processing_status",
    "CAT_EFFECT_DATE": "effective_from_date",
    "CAT_COMMENT": "comment",
    "CAT_REVIEW_DATE": "review_due_date",
    "CAT_ENDORSED_CODE": "customer_status",
    "CAT_ENDORSED_DATE": "approved_date",
    "CAT_ENDORSED_BY_MINISTER": "approved_by_minister",
}

# Minimal required canonical fields for migration
REQUIRED_COLUMNS = [
    "community_migrated_from_id",
    "processing_status",
]

# Pipelines are defined in the source adapters (tpfl.py, tec.py, etc.)
# Each adapter defines PIPELINES specific to its format
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
