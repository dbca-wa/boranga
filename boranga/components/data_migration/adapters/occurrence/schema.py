from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from boranga.components.data_migration import utils
from boranga.components.data_migration.adapters.schema_base import Schema
from boranga.components.species_and_communities.models import GroupType

from ..sources import Source

COLUMN_MAP = {
    "POP_ID": "migrated_from_id",
    "SPNAME": "species_id",
    "Community Code": "community_id",  # TODO Add real column name later when working on community import
    "STATUS": "wild_status_id",
    "CREATED_DATE": "datetime_created",
    "MODIFIED_DATE": "datetime_updated",
    "MODIFIED_BY": "modified_by",
    "POP_COMMENTS": "comment",
    "ACTIVE_IND": "processing_status",
    "CREATED_BY": "submitter",
    "LAND_MANAGER": "OCCContactDetail__contact_name",
    "LAND_MGR_NOTES": "OCCContactDetail__notes",
    "PURPOSE1": "OccurrenceTenure__purpose_id",
    "VESTING": "OccurrenceTenure__vesting_id",
    "POP_NUMBER": "pop_number",
    "SUBPOP_CODE": "subpop_code",
    # TEC Mappings
    "OCC_UNIQUE_ID": "migrated_from_id",
    "COM_NO": "community_id",
    "OCC_DATE_ENTERED": "datetime_created",
    "OCC_DATE_EDITED": "datetime_updated",
    # Comment components
    "OCC_OTHER": "_temp_occ_other",
    "OCC_DATA": "_temp_occ_data",
    "OCC_ORIGINAL_AREA": "_temp_occ_original_area",
    "OCC_AREA_ACCURACY": "_temp_occ_area_accuracy",
    "OCC_BEARD_MAP_CODE": "_temp_occ_beard_map_code",
    "OCC_BEARD_DESC": "_temp_occ_beard_desc",
    "OCC_BUSH_FOREVER_SITE_NO": "_temp_occ_bush_forever_site_no",
    # OCCLocation
    "OCC_SOURCE_CODE": "OCCLocation__coordinate_source_id",
    "OCC_BOUNDARY_DESC": "OCCLocation__boundary_description",
    "OCC_DOLA_REF": "OCCLocation__locality",
    "OCC_DESC": "OCCLocation__location_description",
    # OccurrenceSite
    "S_COMMENTS": "OccurrenceSite__comments",
    "S_LATITUDE_PREF": "OccurrenceSite__latitude",
    "S_LONGITUDE_PREF": "OccurrenceSite__longitude",
    "S_ID": "OccurrenceSite__site_name",
    "S_DATE_EDITED": "OccurrenceSite__updated_date",
    # OCCObservationDetail
    "OCC_BR_CODE": "OCCObservationDetail__comments",
    # OCCHabitatComposition
    "OCC_OTHER_ATTR": "_temp_occ_other_attr",
    "OCC_LAND_ELEMENT": "_temp_occ_land_element",
    "OCC_DRAINAGE": "_temp_occ_drainage",
    "OCC_SOIL": "_temp_occ_soil",
    "OCC_SURF_GEOLOGY": "_temp_occ_surf_geology",
    "OCC_CLASSIFICATION": "_temp_occ_classification",
    "OCC_WATER": "OCCHabitatComposition__water_quality",
    # OCCFireHistory
    "FIRE_DATE": "_temp_fire_date",
    "FIRE_COMMENT": "_temp_fire_comment",
    # OCCAssociatedSpecies
    "OCC_SPECIES_DESC": "OCCAssociatedSpecies__comment",
    # AssociatedSpeciesTaxonomy
    "SPEC_SP_ROLE_CODE": "AssociatedSpeciesTaxonomy__species_role_id",
    # OccurrenceDocument
    "ADD_ITEM_CODE": "OccurrenceDocument__document_sub_category_id",
    "ADD_DESC": "OccurrenceDocument__description",
}

REQUIRED_COLUMNS = [
    "migrated_from_id",
    "processing_status",
]

PIPELINES: dict[str, list[str]] = {}

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
    submitter: int | None = None
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

    OccurrenceTenure__purpose_id: int | None = None
    OccurrenceTenure__vesting_id: int | None = None

    # TEC Fields
    OCCLocation__coordinate_source_id: int | None = None
    OCCLocation__boundary_description: str | None = None
    OCCLocation__locality: str | None = None
    OCCLocation__location_description: str | None = None

    OccurrenceSite__comments: str | None = None
    OccurrenceSite__latitude: float | None = None
    OccurrenceSite__longitude: float | None = None
    OccurrenceSite__site_name: str | None = None
    OccurrenceSite__updated_date: datetime | None = None
    OccurrenceSite__geometry: Any | None = None

    OCCObservationDetail__comments: str | None = None

    OCCHabitatComposition__water_quality: str | None = None
    OCCHabitatComposition__habitat_notes: str | None = None

    OCCFireHistory__comment: str | None = None

    OCCAssociatedSpecies__comment: str | None = None

    AssociatedSpeciesTaxonomy__species_role_id: int | None = None

    OccurrenceDocument__document_sub_category_id: int | None = None
    OccurrenceDocument__description: str | None = None

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
            submitter=utils.to_int_maybe(d.get("submitter")),
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
            datetime_created=d.get("datetime_created"),
            datetime_updated=d.get("datetime_updated"),
            locked=d.get("locked", False),
            OCCContactDetail__contact=utils.safe_strip(
                d.get("OCCContactDetail__contact")
            ),
            OCCContactDetail__contact_name=utils.safe_strip(
                d.get("OCCContactDetail__contact_name")
            ),
            OCCContactDetail__notes=utils.safe_strip(d.get("OCCContactDetail__notes")),
            OccurrenceTenure__purpose_id=utils.to_int_maybe(
                d.get("OccurrenceTenure__purpose_id")
            ),
            OccurrenceTenure__vesting_id=utils.to_int_maybe(
                d.get("OccurrenceTenure__vesting_id")
            ),
            OCCLocation__coordinate_source_id=utils.to_int_maybe(
                d.get("OCCLocation__coordinate_source_id")
            ),
            OCCLocation__boundary_description=utils.safe_strip(
                d.get("OCCLocation__boundary_description")
            ),
            OCCLocation__locality=utils.safe_strip(d.get("OCCLocation__locality")),
            OCCLocation__location_description=utils.safe_strip(
                d.get("OCCLocation__location_description")
            ),
            OccurrenceSite__comments=utils.safe_strip(
                d.get("OccurrenceSite__comments")
            ),
            OccurrenceSite__latitude=utils.to_float_maybe(
                d.get("OccurrenceSite__latitude")
            ),
            OccurrenceSite__longitude=utils.to_float_maybe(
                d.get("OccurrenceSite__longitude")
            ),
            OccurrenceSite__site_name=utils.safe_strip(
                d.get("OccurrenceSite__site_name")
            ),
            OccurrenceSite__updated_date=d.get("OccurrenceSite__updated_date"),
            OccurrenceSite__geometry=d.get("OccurrenceSite__geometry"),
            OCCObservationDetail__comments=utils.safe_strip(
                d.get("OCCObservationDetail__comments")
            ),
            OCCHabitatComposition__water_quality=utils.safe_strip(
                d.get("OCCHabitatComposition__water_quality")
            ),
            OCCHabitatComposition__habitat_notes=utils.safe_strip(
                d.get("OCCHabitatComposition__habitat_notes")
            ),
            OCCFireHistory__comment=utils.safe_strip(d.get("OCCFireHistory__comment")),
            OCCAssociatedSpecies__comment=utils.safe_strip(
                d.get("OCCAssociatedSpecies__comment")
            ),
            AssociatedSpeciesTaxonomy__species_role_id=utils.to_int_maybe(
                d.get("AssociatedSpeciesTaxonomy__species_role_id")
            ),
            OccurrenceDocument__document_sub_category_id=utils.to_int_maybe(
                d.get("OccurrenceDocument__document_sub_category_id")
            ),
            OccurrenceDocument__description=utils.safe_strip(
                d.get("OccurrenceDocument__description")
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
            "submitter": self.submitter,
            "lodgement_date": self.datetime_created,
            "locked": self.locked,
        }
