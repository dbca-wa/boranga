from boranga.components.conservation_status.models import ConservationStatus
from boranga.components.data_migration.mappings import get_group_type_id
from boranga.components.data_migration.registry import choices_transform
from boranga.components.species_and_communities.models import GroupType

from ..base import ExtractionResult, ExtractionWarning, SourceAdapter
from ..sources import Source
from . import schema

# TPFL-specific transforms and pipelines
CUSTOMER_STATUS = choices_transform(
    [c[0] for c in ConservationStatus.CUSTOMER_STATUS_CHOICES]
)
PROCESSING_STATUS = choices_transform(
    [c[0] for c in ConservationStatus.PROCESSING_STATUS_CHOICES]
)

PIPELINES = {
    # Identifiers / basics
    "migrated_from_id": ["strip", "required"],
    "conservation_status_number": ["strip", "blank_to_none"],
    # Statuses / choices
    "customer_status": ["strip", "blank_to_none", CUSTOMER_STATUS],
    "processing_status": ["strip", "required", PROCESSING_STATUS],
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


class ConservationStatusTpflAdapter(SourceAdapter):
    source_key = Source.TPFL.value
    domain = "conservation_status"

    def extract(self, path: str, **options) -> ExtractionResult:
        rows = []
        warnings: list[ExtractionWarning] = []

        raw_rows, read_warnings = self.read_table(path)
        warnings.extend(read_warnings)

        for raw in raw_rows:
            canonical = schema.map_raw_row(raw)
            canonical["group_type_id"] = get_group_type_id(GroupType.GROUP_TYPE_FLORA)
            rows.append(canonical)
        return ExtractionResult(rows=rows, warnings=warnings)


# Attach pipelines to adapter
ConservationStatusTpflAdapter.PIPELINES = PIPELINES
