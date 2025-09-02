from __future__ import annotations

from dataclasses import dataclass

from boranga.components.data_migration.adapters.schema_base import Schema
from boranga.components.data_migration.registry import choices_transform
from boranga.components.occurrence.models import Occurrence

PROCESSING_STATUS = choices_transform(
    [c[0] for c in Occurrence.PROCESSING_STATUS_CHOICES]
)

COLUMN_MAP = {
    "Legacy Community ID": "migrated_from_id",
    "Community Number": "community_number",
    "Group Type": "group_type",
    "Community Name": "taxonomy",  # maps to CommunityTaxonomy via transform
    "Community Taxonomy ID": "taxonomy_id",
    "Species List": "species",  # multi-select / delimiter separated list of legacy species ids or codes
    "Submitter": "submitter",
    "Processing Status": "processing_status",
    "Lodgement Date": "lodgement_date",
    "Last Data Curation Date": "last_data_curation_date",
    "Conservation Plan Exists": "conservation_plan_exists",
    "Conservation Plan Reference": "conservation_plan_reference",
    "Comment": "comment",
    "Department File Numbers": "department_file_numbers",
    "Migrated From ID": "migrated_from_id",
}

REQUIRED_COLUMNS = [
    "migrated_from_id",
    "group_type",
    "processing_status",
]

PIPELINES = {
    "migrated_from_id": ["strip", "required"],
    "community_number": ["strip", "blank_to_none"],
    "group_type": ["strip", "blank_to_none", "group_type_by_name", "required"],
    # taxonomy/community name -> create/lookup CommunityTaxonomy
    # species list: split multiselect and resolve to species ids (implement split/validate in registry)
    "species": [
        "strip",
        "blank_to_none",
        "split_multiselect_species",  # placeholder: split on ',' or ';'
        "validate_species_list",  # placeholder: resolve/validate each entry with SPECIES_TRANSFORM
    ],
    "submitter": ["strip", "blank_to_none"],
    "processing_status": ["strip", "required", PROCESSING_STATUS],
    "lodgement_date": ["strip", "blank_to_none", "date_iso"],
    "last_data_curation_date": ["strip", "blank_to_none", "date_iso"],
    "conservation_plan_exists": ["strip", "blank_to_none", "bool"],
    "conservation_plan_reference": ["strip", "blank_to_none"],
    "comment": ["strip", "blank_to_none"],
    "department_file_numbers": ["strip", "blank_to_none"],
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
class CommunityRow:
    """
    Canonical (post-transform) community row used for persistence.
    Types are examples; adjust to match pipeline outputs (ids resolved, booleans/dates normalized).
    """

    migrated_from_id: str
    group_type: int | str
    processing_status: str
    community_number: str | None = None
    taxonomy: int | None = None
    taxonomy_id: int | None = None
    species: list[int] | None = None
    submitter: int | None = None
    lodgement_date: object | None = None
    last_data_curation_date: object | None = None
    conservation_plan_exists: bool | None = None
    conservation_plan_reference: str | None = None
    comment: str | None = None
    department_file_numbers: str | None = None
