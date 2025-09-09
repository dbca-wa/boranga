from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

from boranga.components.data_migration import utils
from boranga.components.data_migration.adapters.schema_base import Schema
from boranga.components.data_migration.registry import (
    choices_transform,
    emailuser_by_legacy_username_factory,
    fk_lookup,
    normalize_delimited_list_factory,
    taxonomy_lookup,
)
from boranga.components.occurrence.models import (
    Occurrence,
    OccurrenceTenurePurpose,
    OccurrenceTenureVesting,
    WildStatus,
)
from boranga.components.species_and_communities.models import (
    Community,
    GroupType,
    Species,
)

from ..sources import Source

TAXONOMY_TRANSFORM = taxonomy_lookup(
    lookup_field="scientific_name",
)

SPECIES_TRANSFORM = fk_lookup(model=Species, lookup_field="taxonomy_id")

COMMUNITY_TRANSFORM = fk_lookup(
    model=Community,
    lookup_field="taxonomy__community_name",
)

WILD_STATUS_TRANSFORM = fk_lookup(model=WildStatus, lookup_field="name")

PROCESSING_STATUS = choices_transform(
    [c[0] for c in Occurrence.PROCESSING_STATUS_CHOICES]
)

SUBMITTER_TRANSFORM = emailuser_by_legacy_username_factory("TPFL")

PURPOSE_TRANSFORM = fk_lookup(
    model=OccurrenceTenurePurpose,
    lookup_field="code",  # TODO confirm field
)

VESTING_TRANSFORM = fk_lookup(
    model=OccurrenceTenureVesting,
    lookup_field="code",  # TODO confirm field
)

COLUMN_MAP = {
    "POP_ID": "migrated_from_id",
    "SPNAME": "species_id",
    "Community Code": "community_id",  # TODO Add real column name later when working on community import
    "STATUS": "wild_status_id",
    "CREATED_DATE": "datetime_created",
    "MODIFIED_DATE": "datetime_updated",
    "POP_COMMENTS": "comment",
    "ACTIVE_IND": "processing_status",
    "CREATED_BY": "submitter",
    "LAND_MANAGER": "OCCContactDetail__contact_name",
    "LAND_MGR_NOTES": "OCCContactDetail__notes",
    "PURPOSE1": "OccurrenceTenure__purpose",
    "VESTING": "OccurrenceTenure__vesting",
    "OCRVegetationStructure vegetation_structure_layer_one": "vegetation_structure_layer_one",
    "OCRVegetationStructure vegetation_structure_layer_two": "vegetation_structure_layer_two",
    "OCRVegetationStructure vegetation_structure_layer_three": "vegetation_structure_layer_three",
    "OCRVegetationStructure vegetation_structure_layer_four": "vegetation_structure_layer_four",
}

REQUIRED_COLUMNS = [
    "migrated_from_id",
    "processing_status",
]

PIPELINES = {
    "migrated_from_id": ["strip", "required"],
    "species_id": [
        "strip",
        "blank_to_none",
        TAXONOMY_TRANSFORM,  # Get the taxonomy id from the scientific name
        SPECIES_TRANSFORM,  # Then get the species id from the taxonomy id
    ],  # conditional required later (cross field check that either species_id or community_id is present)
    "community_id": ["strip", "blank_to_none", COMMUNITY_TRANSFORM],
    "wild_status_id": ["strip", "blank_to_none", WILD_STATUS_TRANSFORM],
    "comment": ["strip", "blank_to_none"],
    "datetime_created": ["strip", "blank_to_none", "datetime_iso"],
    "datetime_updated": ["strip", "blank_to_none", "datetime_iso"],
    "processing_status": [
        "strip",
        "required",
        "Y_to_active_else_historical",
        PROCESSING_STATUS,
    ],
    "submitter": ["strip", "blank_to_none", SUBMITTER_TRANSFORM],
    "OCCContactDetail__contact_name": ["strip", "blank_to_none"],
    "OCCContactDetail__notes": ["strip", "blank_to_none"],
    "OccurrenceTenure__purpose": ["strip", "blank_to_none", PURPOSE_TRANSFORM],
    "OccurrenceTenure__vesting": ["strip", "blank_to_none", VESTING_TRANSFORM],
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
    occurrence_source: str | None = None
    comment: str | None = None
    review_status: str | None = None
    processing_status: str | None = None
    review_due_date: date | None = None
    datetime_created: datetime | None = None
    datetime_updated: datetime | None = None
    locked: bool = False
    OCCContactDetail__contact: str | None = None
    OCCContactDetail__contact_name: str | None = None
    OCCContactDetail__notes: str | None = None

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
        }
