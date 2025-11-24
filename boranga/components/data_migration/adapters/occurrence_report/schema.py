from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from boranga.components.data_migration.adapters.schema_base import Schema

# Header → canonical key
COLUMN_MAP = {
    "SHEETNO": "migrated_from_id",
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
    # OCRHabitatComposition fields
    "GRAVEL": "OCRHabitatComposition__loose_rock_percent",
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

    # OCRHabitatComposition fields
    OCRHabitatComposition__loose_rock_percent: int | None = None
