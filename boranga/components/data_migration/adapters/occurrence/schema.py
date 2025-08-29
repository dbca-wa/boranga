from __future__ import annotations

from dataclasses import dataclass, field

from boranga.components.data_migration import utils
from boranga.components.data_migration.adapters.schema_base import Schema
from boranga.components.data_migration.registry import (
    choices_transform,
    fk_lookup,
    normalize_delimited_list_factory,
)
from boranga.components.occurrence.models import Occurrence
from boranga.components.species_and_communities.models import GroupType

from ..sources import Source

# NOTE: All values in this file are examples only at this point
# TODO: Replace with real data

# Legacy → target FK / lookup transforms (require LegacyValueMap data)
WILD_STATUS_TRANSFORM = fk_lookup(
    model="components.occurrence.WildStatus", lookup_field="name", return_field="id"
)
SPECIES_NAME_TRANSFORM = fk_lookup(
    model="components.species_and_communities.Species",
    lookup_field="taxonomy__scientific_name",
    return_field="id",
)
COMMUNITY_NAME_TRANSFORM = fk_lookup(
    model="components.species_and_communities.Community",
    lookup_field="taxonomy__community_name",
    return_field="id",
)

REVIEW_STATUS = choices_transform([c[0] for c in Occurrence.REVIEW_STATUS_CHOICES])
PROCESSING_STATUS = choices_transform(
    [c[0] for c in Occurrence.PROCESSING_STATUS_CHOICES]
)

# You still need a multi-select splitter + per-item validator in the registry:
# - split_multiselect_occ_source (splits on ';' or ',' into list)
# - validate_occurrence_source_choices (iterates items, applying OCCURRENCE_SOURCE_CHOICE_TRANSFORM or equivalent)
# (Placeholders referenced below.)

COLUMN_MAP = {
    "POP_ID": "migrated_from_id",
    "Occurrence Name": "occurrence_name",
    "Group Type": "group_type_id",
    "SPNAME": "species_id",
    "Community Code": "community_id",
    "Wild Status": "wild_status_id",
    "Occurrence Source": "occurrence_source",
    "POP_COMMENTS": "comment",
    "Review Status": "review_status",
    "Processing Status": "processing_status",
    "Review Due Date": "review_due_date",
    "Combined Into Occurrence ID": "combined_occurrence_legacy_id",
    "OCRVegetationStructure vegetation_structure_layer_one": "vegetation_structure_layer_one",
    "OCRVegetationStructure vegetation_structure_layer_two": "vegetation_structure_layer_two",
    "OCRVegetationStructure vegetation_structure_layer_three": "vegetation_structure_layer_three",
    "OCRVegetationStructure vegetation_structure_layer_four": "vegetation_structure_layer_four",
}

REQUIRED_COLUMNS = [
    "occurrence_name",
    "processing_status",
]

PIPELINES = {
    # Identifiers / required basics
    "migrated_from_id": ["strip", "required"],
    "occurrence_name": ["strip", "blank_to_none", "cap_length_50"],
    "combined_occurrence_legacy_id": ["strip", "blank_to_none"],
    # FK / lookup mappings
    "group_type_id": ["strip", "blank_to_none", "group_type_by_name", "required"],
    "species_id": [
        "strip",
        "blank_to_none",
        SPECIES_NAME_TRANSFORM,
    ],  # conditional required later
    "community_id": ["strip", "blank_to_none", COMMUNITY_NAME_TRANSFORM],
    "wild_status_id": ["strip", "blank_to_none", WILD_STATUS_TRANSFORM],
    # Simple text
    "comment": ["strip", "blank_to_none"],
    # Choices (enums)
    "review_status": ["strip", "blank_to_none", REVIEW_STATUS],
    "processing_status": ["strip", "required", PROCESSING_STATUS],
    # Dates
    "review_due_date": ["strip", "blank_to_none", "date_iso"],
    # Example of normalising semi colon delimited string
    "vegetation_structure_layer_one": [
        "strip",
        "blank_to_none",
        normalize_delimited_list_factory(),
    ],
    "vegetation_structure_layer_two": [
        "strip",
        "blank_to_none",
        normalize_delimited_list_factory(),
    ],
    "vegetation_structure_layer_three": [
        "strip",
        "blank_to_none",
        normalize_delimited_list_factory(),
    ],
    "vegetation_structure_layer_four": [
        "strip",
        "blank_to_none",
        normalize_delimited_list_factory(),
    ],
}

SCHEMA = Schema(
    column_map=COLUMN_MAP,
    required=REQUIRED_COLUMNS,
    pipelines=PIPELINES,
    # Not using source_choices placeholder here; multiple legacy systems may share this schema
    source_choices=None,
)

# Re-export convenience functions (optional)
normalise_header = SCHEMA.normalise_header
canonical_key = SCHEMA.canonical_key
required_missing = SCHEMA.required_missing
validate_headers = SCHEMA.validate_headers
map_raw_row = SCHEMA.map_raw_row
COLUMN_PIPELINES = SCHEMA.effective_pipelines()


@dataclass
class OccurrenceRow:
    """
    Canonical (post-transform) occurrence data used for persistence.
    Field names match pipeline output and types here are the expected Python types.
    """

    migrated_from_id: str
    occurrence_name: str | None
    group_type_id: int | None
    species_id: int | None = None
    community_id: int | None = None
    wild_status_id: int | None = None
    occurrence_source: list[str] = field(default_factory=list)
    comment: str | None = None
    review_status: str | None = None
    processing_status: str | None = None
    review_due_date: object | None = None  # datetime preferred after parsing
    combined_occurrence_legacy_id: str | None = None

    @classmethod
    def from_dict(cls, d: dict) -> OccurrenceRow:
        """
        Build OccurrenceRow from pipeline output. Coerce simple types and accept
        either species_id or species_name (for backward compatibility).
        """
        species_id = utils.to_int_maybe(d.get("species_id"))
        # backward-compat: pipelines that placed FK into species_name
        if species_id is None:
            species_id = utils.to_int_maybe(d.get("species_name"))

        return cls(
            migrated_from_id=str(d["migrated_from_id"]),
            occurrence_name=utils.safe_strip(d.get("occurrence_name")),
            group_type_id=utils.to_int_maybe(
                d.get("group_type_id") or d.get("group_type")
            ),
            species_id=species_id,
            community_id=utils.to_int_maybe(
                d.get("community_id") or d.get("community_code")
            ),
            wild_status_id=utils.to_int_maybe(
                d.get("wild_status_id") or d.get("wild_status")
            ),
            occurrence_source=d.get("occurrence_source") or [],
            comment=utils.safe_strip(d.get("comment")),
            review_status=utils.safe_strip(d.get("review_status")),
            processing_status=utils.safe_strip(d.get("processing_status")),
            review_due_date=d.get("review_due_date"),
            combined_occurrence_legacy_id=utils.safe_strip(
                d.get("combined_occurrence_legacy_id")
            ),
        )

    def validate(self, source: str | None = None) -> list[tuple[str, str]]:
        """
        Return list of (level, message). Business rules that depend on source
        or group_type_id should be enforced here.
        """
        issues: list[tuple[str, str]] = []

        if self.group_type_id is not None:
            # if group_type_id refers to a known flora id, require species
            if str(self.group_type_id).lower() in [
                GroupType.GROUP_TYPE_FLORA,
                GroupType.GROUP_TYPE_FAUNA,
            ]:
                if not self.species_id:
                    issues.append(("error", "species_id is required for flora/fauna"))
            elif (
                str(self.group_type_id).lower()
                == str(GroupType.GROUP_TYPE_COMMUNITY).lower()
            ):
                if not self.community_id:
                    issues.append(("error", "community_id is required for community"))
        # source-specific rule example
        if source == Source.TPFL.value:
            if not self.species_id:
                issues.append(("error", "TPFL rows must include species"))
        if source == Source.TFAUNA.value:
            if not self.species_id:
                issues.append(("error", "TFAUNA rows must include species"))
        if source == Source.TEC.value:
            if not self.community_id:
                issues.append(("error", "TEC rows must include community"))
        # other checks (dates, enums) can be added here
        return issues

    def to_model_defaults(self) -> dict:
        """
        Return dict ready for ORM update/create defaults.
        Convert occurrence_source list -> storage string if needed.
        """
        occ_source = self.occurrence_source
        if isinstance(occ_source, list):
            occ_source = ",".join(occ_source)
        return {
            "occurrence_name": self.occurrence_name,
            "group_type_id": self.group_type_id,
            "species_id": self.species_id,
            "community_id": self.community_id,
            "wild_status_id": self.wild_status_id,
            "occurrence_source": occ_source,
            "comment": self.comment,
            "review_status": self.review_status,
            "processing_status": self.processing_status,
            "review_due_date": self.review_due_date,
            "combined_occurrence_legacy_id": self.combined_occurrence_legacy_id,
        }
