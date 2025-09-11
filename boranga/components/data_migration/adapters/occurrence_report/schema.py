from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from boranga.components.data_migration.adapters.schema_base import Schema
from boranga.components.data_migration.registry import (
    build_legacy_map_transform,
    csv_lookup_factory,
    dependent_from_column_factory,
    emailuser_by_legacy_username_factory,
    fk_lookup,
    pluck_attribute_factory,
    taxonomy_lookup,
)
from boranga.components.occurrence.models import OccurrenceReport
from boranga.components.species_and_communities.models import Species

TAXONOMY_TRANSFORM = taxonomy_lookup(
    lookup_field="scientific_name",
)

SPECIES_TRANSFORM = fk_lookup(model=Species, lookup_field="taxonomy_id")

COMMUNITY_TRANSFORM = build_legacy_map_transform("TPFL", "community", return_type="id")


def map_form_status_code_to_processing_status(value: str) -> str | None:
    mapping = {
        "NEW": OccurrenceReport.PROCESSING_STATUS_DRAFT,
        "READY": OccurrenceReport.PROCESSING_STATUS_WITH_ASSESSOR,
        "ACCEPTED": OccurrenceReport.PROCESSING_STATUS_APPROVED,
        "REJECTED": OccurrenceReport.PROCESSING_STATUS_DECLINED,
    }
    return mapping.get(value.strip().upper()) if value else None


def map_form_status_code_to_customer_status(value: str) -> str | None:
    mapping = {
        "NEW": OccurrenceReport.CUSTOMER_STATUS_DRAFT,
        "READY": OccurrenceReport.CUSTOMER_STATUS_WITH_ASSESSOR,
        "ACCEPTED": OccurrenceReport.CUSTOMER_STATUS_APPROVED,
        "REJECTED": OccurrenceReport.CUSTOMER_STATUS_DECLINED,
    }
    return mapping.get(value.strip().upper()) if value else None


MAP_FORM_STATUS_CODE_TO_PROCESSING_STATUS = map_form_status_code_to_processing_status
MAP_FORM_STATUS_CODE_TO_CUSTOMER_STATUS = map_form_status_code_to_customer_status

# derive customer_status from the raw FORM_STATUS_CODE column
CUSTOMER_STATUS_FROM_FORM_STATUS_CODE = dependent_from_column_factory(
    "FORM_STATUS_CODE",
    mapper=lambda dep_val, ctx: MAP_FORM_STATUS_CODE_TO_CUSTOMER_STATUS(dep_val),
    default=None,
)

FK_OCCURRENCE = fk_lookup(OccurrenceReport, "migrated_from_id")

OCCURRENCE_FROM_POP_ID = dependent_from_column_factory("POP_ID", FK_OCCURRENCE)

# map raw RECORD_SRC_CODE -> LABEL using the provided CSV (filename required;
# default search location is handled by load_csv_mapping)
RECORD_SOURCE_FROM_CSV = csv_lookup_factory(
    key_column="TERM_CODE",
    value_column="LABEL",
    csv_filename="DRF_LOV_RECORD_SOURCE_VWS.csv",
    required=False,
)

SUBMITTER_TRANSFORM = emailuser_by_legacy_username_factory("TPFL")

ROLE_TRANSFORM = build_legacy_map_transform(
    "TPFL",
    "ROLE (DRF_LOV_ROLE_VWS)",
    required=False,
)

# Header → canonical key
COLUMN_MAP = {
    "SHEETNO": "migrated_from_id",
    "POP_ID": "Occurrence__migrated_from_id",
    "SPNAME": "species_id",
    "COMM_NAME": "community_id",  # TODO replace with real column name later
    # approved_by - has no column as is derived from two other columns: See synthetic fields in handler
    # assessor_data - is pre-populated by concatenating two other columns: see tpfl adapter
    # comments - synthetic field, see pipelines
    "CREATED_DATE": "lodgement_date",
    "OBSERVATION_DATE": "observation_date",
    # ocr_for_occ_name - is pre-populated by concatenating two other columns: see tpfl adapter
    # ocr_for_occ_number - synthetic field, see pipelines
    "FORM_STATUS_CODE": "processing_status",
    "RECORD_SRC_CODE": "record_source",
    # reported_date: just a copy of lodgement_date, so will be applied in handler
    "CREATED_BY": "submitter",
    # OCRObserverDetail fields
    "OBS_ROLE_CODE": "OCRObserverDetail__role",
    # OCRObserverDetail__main_observer - is pre-populated in tpfl adapter
    "OBS_NAME": "OCRObserverDetail__observer_name",
}

REQUIRED_COLUMNS = [
    "migrated_from_id",
    "Occurrence__migrated_from_id",
    "processing_status",
]

PIPELINES = {
    "migrated_from_id": ["strip", "required"],
    "Occurrence__migrated_from_id": ["strip", "required"],
    "species_id": ["strip", "blank_to_none", TAXONOMY_TRANSFORM, SPECIES_TRANSFORM],
    "community_id": ["strip", "blank_to_none", COMMUNITY_TRANSFORM],
    "lodgement_date": ["strip", "blank_to_none", "datetime_iso"],
    "observation_date": ["strip", "blank_to_none", "date_from_datetime_iso"],
    "record_source": ["strip", "blank_to_none", RECORD_SOURCE_FROM_CSV],
    "customer_status": [
        CUSTOMER_STATUS_FROM_FORM_STATUS_CODE
    ],  # synthetic field - derived from FORM_STATUS_CODE
    "comments": ["ocr_comments_transform"],  # synthetic field
    "ocr_for_occ_name": [
        OCCURRENCE_FROM_POP_ID,
        pluck_attribute_factory("occurrence_name"),
    ],  # synthetic field
    "processing_status": [
        "strip",
        "required",
        MAP_FORM_STATUS_CODE_TO_PROCESSING_STATUS,
    ],
    "submitter": ["strip", "blank_to_none", SUBMITTER_TRANSFORM],
    # OCRObserverDetail fields
    "OCRObserverDetail__role": ["strip", "blank_to_none", ROLE_TRANSFORM],
    "OCRObserverDetail__observer_name": ["strip", "blank_to_none"],
}

SCHEMA = Schema(
    column_map=COLUMN_MAP,
    required=REQUIRED_COLUMNS,
    pipelines=PIPELINES,
    source_choices=None,
)

# Convenience exports
normalise_header = SCHEMA.normalise_header
canonical_key = SCHEMA.canonical_key
required_missing = SCHEMA.required_missing
validate_headers = SCHEMA.validate_headers
map_raw_row = SCHEMA.map_raw_row
COLUMN_PIPELINES = SCHEMA.effective_pipelines()


@dataclass
class OccurrenceReportRow:
    """
    Canonical (post-transform) occurrence report data for persistence.
    """

    migrated_from_id: str
    Occurrence__migrated_from_id: str
    group_type_id: int
    species_id: int | None = None  # FK id (Species) after transform
    community_id: int | None = None  # FK id (Community) after transform
    processing_status: str | None = None
    customer_status: str | None = None
    observation_date: date | None = None
    record_source: str | None = None
    comments: str | None = None
    ocr_for_occ_number: str | None = None
    ocr_for_occ_name: str | None = None
    approver_comment: str | None = None
    assessor_data: str | None = None
    reported_date: date | None = None  # copy of lodgement_date
    lodgement_date: date | None = None
    approved_by: int | None = None  # FK id (EmailUser) after transform
    submitter: int | None = None  # FK id (EmailUser) after transform

    # OCRObserverDetail fields
    OCRObserverDetail__role: str | None = None
