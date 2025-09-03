from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

from boranga.components.data_migration.adapters.schema_base import Schema
from boranga.components.data_migration.registry import (
    choices_transform,
    emailuser_by_legacy_username_factory,
    fk_lookup,
)
from boranga.components.species_and_communities.models import Species, Taxonomy

# Legacy → target FK / lookup transforms (require LegacyValueMap data)
TAXONOMY_TRANSFORM = fk_lookup(
    model=Taxonomy,
    lookup_field="scientific_name",
)

SUBMITTER_TRANSFORM = emailuser_by_legacy_username_factory("TPFL")

PROCESSING_STATUS = choices_transform([c[0] for c in Species.PROCESSING_STATUS_CHOICES])

COLUMN_MAP = {
    "TXN_LIST_ID": "migrated_from_id",
    "NAME": "taxonomy_id",
    "FILE_COMMENTS": "comment",
    "R_PLAN": "conservation_plan_exists",
    "FILE_NO": "department_file_numbers",
    "FILE_LAST_UPDATED": "last_data_curation_date",
    "CREATED_DATE": "lodgement_date",
    "ACTIVE_IND": "processing_status",
    "CREATED_BY": "submitter",
    "DISTRIBUTION": "distribution",
}

# Minimal required canonical fields for migration
REQUIRED_COLUMNS = [
    "migrated_from_id",  # legacy identifier used to relate back to source
    "taxonomy_id",
    "processing_status",
]

PIPELINES = {
    "migrated_from_id": ["strip", "required"],
    "taxonomy_id": ["strip", "blank_to_none", "required", TAXONOMY_TRANSFORM],
    "comment": ["strip", "blank_to_none"],
    "conservation_plan_exists": ["strip", "blank_to_none", "is_present"],
    "department_file_numbers": ["strip", "blank_to_none"],
    "last_data_curation_date": ["strip", "blank_to_none", "date_iso"],
    "lodgement_date": ["strip", "blank_to_none", "datetime_iso"],
    "processing_status": [
        "strip",
        "blank_to_none",
        "required",
        "Y_to_active_else_historical",
        PROCESSING_STATUS,
    ],
    "submitter": ["strip", "blank_to_none", SUBMITTER_TRANSFORM],
    "distribution": ["strip", "blank_to_none"],
}

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
class SpeciesRow:
    """
    Canonical (post-transform) species row used for persistence.
    """

    migrated_from_id: str
    processing_status: str
    # adapter is expected to populate group_type_id (may be resolved id); keep optional
    group_type_id: int | None = None
    taxonomy_id: int | None = None
    comment: str | None = None
    conservation_plan_exists: bool | None = None
    conservation_plan_reference: str | None = None
    department_file_numbers: str | None = None
    submitter: int | None = None
    lodgement_date: datetime | None = None
    last_data_curation_date: date | None = None
    distribution: str | None = None


def validate_species_row(row) -> list[tuple[str, str]]:
    """
    Validate cross-field rules for a canonical species row.

    Returns a list of (field_name, message). Empty list = valid.
    Rule implemented: if processing_status == 'active', submitter must be present.
    Accepts either a dict (canonical row from map_raw_row) or a SpeciesRow instance.
    """

    # support both dict and dataclass-like access
    def _get(k):
        if row is None:
            return None
        if isinstance(row, dict):
            return row.get(k)
        return getattr(row, k, None)

    errors: list[tuple[str, str]] = []

    proc = _get("processing_status")
    submitter = _get("submitter")
    lodgement_date = _get("lodgement_date")

    # Normalise comparison to canonical processing status value
    if (
        isinstance(proc, str)
        and proc.strip().lower() == Species.PROCESSING_STATUS_ACTIVE
    ):
        if submitter in (None, "", []):
            errors.append(
                (
                    "submitter",
                    f"Submitter is required when processing_status is '{Species.PROCESSING_STATUS_ACTIVE}'",
                )
            )
        if lodgement_date is None:
            errors.append(
                (
                    "lodgement_date",
                    f"Lodgement date is required when processing_status is '{Species.PROCESSING_STATUS_ACTIVE}'",
                )
            )

    return errors
