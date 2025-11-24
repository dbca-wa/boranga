from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from boranga.components.data_migration import utils
from boranga.components.data_migration.adapters.schema_base import Schema
from boranga.components.species_and_communities.models import GroupType

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

    @classmethod
    def from_dict(cls, d: dict) -> OccurrenceReportRow:
        """
        Build OccurrenceReportRow from pipeline output. Coerce simple types.
        """
        # lodgement_date and reported_date are datetimes; observation_date is date
        lodgement_dt = utils.parse_date_iso(d.get("lodgement_date"))
        reported_dt = utils.parse_date_iso(d.get("reported_date"))
        obs_dt = utils.parse_date_iso(d.get("observation_date"))
        obs_date = obs_dt.date() if obs_dt is not None else None

        return cls(
            migrated_from_id=str(d["migrated_from_id"]),
            Occurrence__migrated_from_id=d.get("Occurrence__migrated_from_id"),
            group_type_id=utils.to_int_maybe(d.get("group_type_id")),
            species_id=utils.to_int_maybe(d.get("species_id")),
            community_id=utils.to_int_maybe(d.get("community_id")),
            processing_status=utils.safe_strip(d.get("processing_status")),
            customer_status=utils.safe_strip(d.get("customer_status")),
            observation_date=obs_date,
            record_source=utils.safe_strip(d.get("record_source")),
            comments=utils.safe_strip(d.get("comments")),
            ocr_for_occ_number=utils.safe_strip(d.get("ocr_for_occ_number")),
            ocr_for_occ_name=utils.safe_strip(d.get("ocr_for_occ_name")),
            approver_comment=utils.safe_strip(d.get("approver_comment")),
            assessor_data=utils.safe_strip(d.get("assessor_data")),
            reported_date=reported_dt,
            lodgement_date=lodgement_dt,
            approved_by=utils.to_int_maybe(d.get("approved_by")),
            submitter=utils.to_int_maybe(d.get("submitter")),
            OCRObserverDetail__role=utils.safe_strip(d.get("OCRObserverDetail__role")),
            OCRHabitatComposition__loose_rock_percent=utils.to_int_maybe(
                d.get("OCRHabitatComposition__loose_rock_percent")
            ),
        )

    def validate(self, source: str | None = None) -> list[tuple[str, str]]:
        """
        Return list of (level, message). Basic business rules enforced here.
        """
        issues: list[tuple[str, str]] = []

        if not self.migrated_from_id:
            issues.append(("error", "migrated_from_id is required"))

        if not self.processing_status:
            issues.append(("error", "processing_status is required"))

        if self.group_type_id is not None:
            # for flora/fauna require species; for community require community
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

        # source-specific examples could be added here using `source`
        return issues

    def to_model_defaults(self) -> dict:
        """
        Return dict ready for ORM update/create defaults.
        """
        return {
            "group_type_id": self.group_type_id,
            "species_id": self.species_id,
            "community_id": self.community_id,
            "processing_status": self.processing_status,
            "customer_status": self.customer_status,
            "observation_date": self.observation_date,
            "record_source": self.record_source,
            "comments": self.comments or "",
            "ocr_for_occ_number": self.ocr_for_occ_number or "",
            "ocr_for_occ_name": self.ocr_for_occ_name or "",
            "approver_comment": self.approver_comment or "",
            "assessor_data": self.assessor_data or "",
            "reported_date": self.reported_date,
            "lodgement_date": self.lodgement_date,
            "approved_by": self.approved_by,
            "submitter": self.submitter,
        }
