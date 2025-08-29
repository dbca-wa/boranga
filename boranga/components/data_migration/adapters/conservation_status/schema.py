from __future__ import annotations

from dataclasses import dataclass

from boranga.components.conservation_status.models import ConservationStatus
from boranga.components.data_migration.adapters.schema_base import Schema
from boranga.components.data_migration.mappings import build_legacy_map_transform
from boranga.components.data_migration.registry import choices_transform

# NOTE: Example schema for migrating legacy ConservationStatus data.
# Adjust legacy source key, column names and transforms to match the real legacy CSVs
# and the registry transforms available in your project.

LEGACY_SOURCE_KEY = "LEGACY_CS"  # replace with actual legacy system key if known

# Legacy → target FK / lookup transforms (require LegacyValueMap data)
SPECIES_TAXONOMY_TRANSFORM = build_legacy_map_transform(
    LEGACY_SOURCE_KEY, "species_taxonomy", return_type="id"
)
SPECIES_TRANSFORM = build_legacy_map_transform(
    LEGACY_SOURCE_KEY, "species", return_type="id"
)
COMMUNITY_TRANSFORM = build_legacy_map_transform(
    LEGACY_SOURCE_KEY, "community", return_type="id"
)
CHANGE_CODE_TRANSFORM = build_legacy_map_transform(
    LEGACY_SOURCE_KEY, "conservation_change_code", return_type="id"
)
WA_PRIORITY_LIST_TRANSFORM = build_legacy_map_transform(
    LEGACY_SOURCE_KEY, "wa_priority_list", return_type="id"
)
WA_PRIORITY_CATEGORY_TRANSFORM = build_legacy_map_transform(
    LEGACY_SOURCE_KEY, "wa_priority_category", return_type="id"
)
WA_LEGISLATIVE_LIST_TRANSFORM = build_legacy_map_transform(
    LEGACY_SOURCE_KEY, "wa_legislative_list", return_type="id"
)
WA_LEGISLATIVE_CATEGORY_TRANSFORM = build_legacy_map_transform(
    LEGACY_SOURCE_KEY, "wa_legislative_category", return_type="id"
)
IUCN_VERSION_TRANSFORM = build_legacy_map_transform(
    LEGACY_SOURCE_KEY, "iucn_version", return_type="id"
)
COMMONWEALTH_CATEGORY_TRANSFORM = build_legacy_map_transform(
    LEGACY_SOURCE_KEY, "commonwealth_conservation_category", return_type="id"
)
OTHER_ASSESSMENT_TRANSFORM = build_legacy_map_transform(
    LEGACY_SOURCE_KEY, "other_conservation_assessment", return_type="id"
)

# Choice enums based on model choices (examples mirror model constants)
CUSTOMER_STATUS_CHOICES = [
    "draft",
    "with_assessor",
    "ready_for_agenda",
    "approved",
    "declined",
    "discarded",
    "closed",
]
PROCESSING_STATUS_CHOICES = [
    "draft",
    "discarded",
    "with_assessor",
    "with_referral",
    "deferred",
    "proposed_for_agenda",
    "ready_for_agenda",
    "on_agenda",
    "with_approver",
    "approved",
    "declined",
    "delisted",
    "closed",
]

CUSTOMER_STATUS = choices_transform(
    [c[0] for c in ConservationStatus.CUSTOMER_STATUS_CHOICES]
)
PROCESSING_STATUS = choices_transform(
    [c[0] for c in ConservationStatus.PROCESSING_STATUS_CHOICES]
)

# Column header → canonical key map (adjust headers to match CSV)
COLUMN_MAP = {
    "Legacy CS ID": "migrated_from_id",
    "Conservation Status Number": "conservation_status_number",
    "Customer Status": "customer_status",
    "Processing Status": "processing_status",
    "Change Code": "change_code",
    "Species Taxonomy": "species_taxonomy",
    "Species": "species",
    "Community": "community",
    "WA Priority List": "wa_priority_list",
    "WA Priority Category": "wa_priority_category",
    "WA Legislative List": "wa_legislative_list",
    "WA Legislative Category": "wa_legislative_category",
    "IUCN Version": "iucn_version",
    "Commonwealth Category": "commonwealth_conservation_category",
    "Other Assessment": "other_conservation_assessment",
    "Conservation Criteria": "conservation_criteria",
    "CAM MOU": "cam_mou",
    "CAM MOU Date Sent": "cam_mou_date_sent",
    "Public Consultation": "public_consultation",
    "Public Consultation Start": "public_consultation_start_date",
    "Public Consultation End": "public_consultation_end_date",
    "Approval Level": "approval_level",
    "Comment": "comment",
    "Review Due Date": "review_due_date",
    "Effective From": "effective_from",
    "Effective To": "effective_to",
    "Submitter": "submitter",
    "Assigned Officer": "assigned_officer",
    "Assigned Approver": "assigned_approver",
    "Approved By": "approved_by",
    "Listing Date": "listing_date",
    "Lodgement Date": "lodgement_date",
    "Internal Application": "internal_application",
    "Locked": "locked",
}

# Minimal required canonical fields for migration; add more business rules as needed
REQUIRED_COLUMNS = [
    "migrated_from_id",  # legacy identifier used to relate back to source
    "processing_status",
]

PIPELINES = {
    # Identifiers / basics
    "migrated_from_id": ["strip", "required"],
    "conservation_status_number": ["strip", "blank_to_none"],
    # Statuses / choices
    "customer_status": ["strip", "blank_to_none", CUSTOMER_STATUS],
    "processing_status": ["strip", "required", PROCESSING_STATUS],
    "change_code": ["strip", "blank_to_none", CHANGE_CODE_TRANSFORM],
    # FK / lookup mappings
    "species_taxonomy": ["strip", "blank_to_none", SPECIES_TAXONOMY_TRANSFORM],
    "species": ["strip", "blank_to_none", SPECIES_TRANSFORM],
    "community": ["strip", "blank_to_none", COMMUNITY_TRANSFORM],
    "wa_priority_list": ["strip", "blank_to_none", WA_PRIORITY_LIST_TRANSFORM],
    "wa_priority_category": ["strip", "blank_to_none", WA_PRIORITY_CATEGORY_TRANSFORM],
    "wa_legislative_list": ["strip", "blank_to_none", WA_LEGISLATIVE_LIST_TRANSFORM],
    "wa_legislative_category": [
        "strip",
        "blank_to_none",
        WA_LEGISLATIVE_CATEGORY_TRANSFORM,
    ],
    "iucn_version": ["strip", "blank_to_none", IUCN_VERSION_TRANSFORM],
    "commonwealth_conservation_category": [
        "strip",
        "blank_to_none",
        COMMONWEALTH_CATEGORY_TRANSFORM,
    ],
    "other_conservation_assessment": [
        "strip",
        "blank_to_none",
        OTHER_ASSESSMENT_TRANSFORM,
    ],
    # Simple fields
    "conservation_criteria": ["strip", "blank_to_none"],
    "approval_level": ["strip", "blank_to_none"],
    "comment": ["strip", "blank_to_none"],
    # Booleans
    "cam_mou": ["strip", "blank_to_none", "bool"],
    "public_consultation": ["strip", "blank_to_none", "bool"],
    "internal_application": ["strip", "blank_to_none", "bool"],
    "locked": ["strip", "blank_to_none", "bool"],
    # Dates
    "cam_mou_date_sent": ["strip", "blank_to_none", "date_iso"],
    "public_consultation_start_date": ["strip", "blank_to_none", "date_iso"],
    "public_consultation_end_date": ["strip", "blank_to_none", "date_iso"],
    "review_due_date": ["strip", "blank_to_none", "date_iso"],
    "effective_from": ["strip", "blank_to_none", "date_iso"],
    "effective_to": ["strip", "blank_to_none", "date_iso"],
    "listing_date": ["strip", "blank_to_none", "date_iso"],
    "lodgement_date": ["strip", "blank_to_none", "date_iso"],
    # User references (may be migrated to ledger ids separately)
    "submitter": ["strip", "blank_to_none"],
    "assigned_officer": ["strip", "blank_to_none"],
    "assigned_approver": ["strip", "blank_to_none"],
    "approved_by": ["strip", "blank_to_none"],
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
class ConservationStatusRow:
    """
    Canonical (post-transform) conservation status row used for persistence.
    Types are examples; adjust to match pipeline outputs (IDs resolved, booleans/dates normalized).
    """

    migrated_from_id: str
    processing_status: str
    conservation_status_number: str | None = None
    customer_status: str | None = None
    change_code: int | None = None
    species_taxonomy: int | None = None
    species: int | None = None
    community: int | None = None
    wa_priority_list: int | None = None
    wa_priority_category: int | None = None
    wa_legislative_list: int | None = None
    wa_legislative_category: int | None = None
    iucn_version: int | None = None
    commonwealth_conservation_category: int | None = None
    other_conservation_assessment: int | None = None
    conservation_criteria: str | None = None
    cam_mou: bool | None = None
    cam_mou_date_sent: object | None = None
    public_consultation: bool | None = None
    public_consultation_start_date: object | None = None
    public_consultation_end_date: object | None = None
    approval_level: str | None = None
    comment: str | None = None
    review_due_date: object | None = None
    effective_from: object | None = None
    effective_to: object | None = None
    submitter: str | None = None
    assigned_officer: str | None = None
    assigned_approver: str | None = None
    approved_by: str | None = None
    listing_date: object | None = None
    lodgement_date: object | None = None
    internal_application: bool | None = None
    locked: bool | None = None
