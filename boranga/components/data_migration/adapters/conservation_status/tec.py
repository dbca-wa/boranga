"""
TEC (Threatened Ecological Communities) Conservation Status adapter.

Source CSV: community-cs-migration-template.csv
Columns: migrated_from_id, iucn_version, effective_from, effective_to,
         listing_date, processing_status, commonwealth_conservation_category,
         comment, wa_legislative_category, wa_legislative_list,
         wa_priority_category, wa_priority_list, conservation_criteria

Key task rules (from work items 12060-12093):
  - migrated_from_id    : duplicates -> unique suffix "01", "02" prepended with "tec-" by handler
  - processing_status   : lowercase CSV values already match Boranga choices
  - lodgement_date      : N/A - leave null (Task 12091)
  - change_code         : N/A - leave null (Task 12060)
  - listing_date        : smart_date_parse
  - effective_from/to   : smart_date_parse
  - wa_legislative_list : FK lookup by code
  - wa_legislative_cat  : FK lookup by code
  - wa_priority_list    : FK lookup by code
  - wa_priority_cat     : FK lookup by code
  - iucn_version        : FK lookup by code
  - commonwealth_cat    : FK lookup by code
  - conservation_criteria / comment : str pass-through
"""

import logging

from boranga.components.conservation_status.models import (
    CommonwealthConservationList,
    ConservationStatus,
    IUCNVersion,
)
from boranga.components.data_migration.mappings import get_group_type_id
from boranga.components.data_migration.registry import fk_lookup
from boranga.components.species_and_communities.models import GroupType

from ..base import ExtractionResult, SourceAdapter
from ..sources import Source
from . import schema

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# FK lookups (return model PK on hit, raw value + error on miss)
# ---------------------------------------------------------------------------
IUCN_LOOKUP = fk_lookup(IUCNVersion, "code")
COMMONWEALTH_LOOKUP = fk_lookup(CommonwealthConservationList, "code")

# Valid processing_status values derived directly from the model's choices tuple —
# no need to duplicate the string literals here.
_VALID_PROCESSING_STATUSES: frozenset[str] = frozenset(v for v, _ in ConservationStatus.PROCESSING_STATUS_CHOICES)

# Map processing_status -> customer_status using model constants only.
# Only statuses that appear in TEC data need an explicit entry; everything
# else falls back to CUSTOMER_STATUS_DRAFT.
_CUSTOMER_STATUS_MAP: dict[str, str] = {
    ConservationStatus.PROCESSING_STATUS_APPROVED: ConservationStatus.CUSTOMER_STATUS_APPROVED,
    ConservationStatus.PROCESSING_STATUS_CLOSED: ConservationStatus.CUSTOMER_STATUS_CLOSED,
    ConservationStatus.PROCESSING_STATUS_DECLINED: ConservationStatus.CUSTOMER_STATUS_DECLINED,
    ConservationStatus.PROCESSING_STATUS_DELISTED: ConservationStatus.CUSTOMER_STATUS_CLOSED,
    ConservationStatus.PROCESSING_STATUS_DISCARDED: ConservationStatus.CUSTOMER_STATUS_DISCARDED,
}

# ---------------------------------------------------------------------------
# Pipelines - applied by the handler after extract().
# Keys use canonical field names (as produced by schema.map_raw_row).
# ---------------------------------------------------------------------------
PIPELINES: dict[str, list] = {
    # Core identity
    "migrated_from_id": ["strip", "required"],
    "community_migrated_from_id": ["strip", "blank_to_none", "community_id_from_legacy"],
    # FK lookups - Tasks 12063, 12061
    "iucn_version": ["strip", "blank_to_none", IUCN_LOOKUP],
    "commonwealth_conservation_category": ["strip", "blank_to_none", COMMONWEALTH_LOOKUP],
    # WA lists - Tasks 12069, 12068, 12070, 12071 (all code-based lookups)
    "wa_legislative_list": ["strip", "blank_to_none", "wa_legislative_list_from_code"],
    "wa_legislative_category": ["strip", "blank_to_none", "wa_legislative_category_from_code"],
    "wa_priority_list": ["strip", "blank_to_none", "wa_priority_list_from_code"],
    "wa_priority_category": ["strip", "blank_to_none", "wa_priority_category_from_code"],
    # Dates - Tasks 12086, 12087, 12089
    "effective_from_date": ["strip", "smart_date_parse"],
    "effective_to_date": ["strip", "smart_date_parse"],
    "listing_date": ["strip", "smart_date_parse"],
    # Task 12091: lodgement_date - no data, leave null
    "lodgement_date": ["strip", "blank_to_none"],
    # Task 12060: change_code - no data, leave null
    "change_code": ["strip", "blank_to_none"],
    # Text fields - Tasks 12081, 12080
    "conservation_criteria": ["strip", "blank_to_none"],
    "comment": ["strip", "blank_to_none"],
    # Static booleans - always True for migrated TEC records
    "locked": ["static_value_True"],
    "internal_application": ["static_value_True"],
}


class ConservationStatusTecAdapter(SourceAdapter):
    source_key = Source.TEC.value
    domain = "conservation_status"

    def extract(self, path: str, **options) -> ExtractionResult:
        raw_rows, read_warnings = self.read_table(path, encoding="utf-8-sig")
        rows: list[dict] = []

        # ----------------------------------------------------------------
        # Track occurrence count per raw migrated_from_id so that each
        # record gets a unique 2-digit suffix.
        # Task 12092: duplicate IDs -> "1-01", "1-02", ...
        # Handler then prepends "tec-" -> "tec-1-01", "tec-1-02", ...
        # ----------------------------------------------------------------
        migrated_id_counts: dict[str, int] = {}

        for raw in raw_rows:
            canonical = schema.map_raw_row(raw)

            # ----------------------------------------------------------
            # Task 12092: unique migrated_from_id
            # ----------------------------------------------------------
            raw_mig_id = str(canonical.get("migrated_from_id") or "").strip()
            if raw_mig_id:
                count = migrated_id_counts.get(raw_mig_id, 0) + 1
                migrated_id_counts[raw_mig_id] = count
                # Unique record ID; handler prepends "tec-"
                canonical["migrated_from_id"] = f"{raw_mig_id}-{count:02d}"

                # community_migrated_from_id uses the RAW id (no suffix)
                # so community_id_from_legacy can look up the Community.
                if not canonical.get("community_migrated_from_id"):
                    canonical["community_migrated_from_id"] = raw_mig_id
            else:
                # Will fail the "required" pipeline check downstream
                canonical["migrated_from_id"] = None

            # ----------------------------------------------------------
            # Task 12093: processing_status normalisation
            # CSV values are lowercase and already match Boranga constants.
            # Validate against the model's own PROCESSING_STATUS_CHOICES set;
            # casefold to absorb any accidental capitalisation variants.
            # ----------------------------------------------------------
            p_status_raw = str(canonical.get("processing_status") or "").strip()
            p_status_casefolded = p_status_raw.casefold()
            p_status = p_status_casefolded if p_status_casefolded in _VALID_PROCESSING_STATUSES else None
            canonical["processing_status"] = p_status

            # Derive customer_status from processing_status
            canonical["customer_status"] = _CUSTOMER_STATUS_MAP.get(p_status, ConservationStatus.CUSTOMER_STATUS_DRAFT)

            # ----------------------------------------------------------
            # Approval level: ministerial when a WA legislative category
            # is present (BCA listing), immediate otherwise.
            # ----------------------------------------------------------
            wa_leg_cat = str(canonical.get("wa_legislative_category") or "").strip()
            canonical["approval_level"] = (
                ConservationStatus.APPROVAL_LEVEL_MINISTER
                if wa_leg_cat
                else ConservationStatus.APPROVAL_LEVEL_IMMEDIATE
            )

            # Group type = Community for all TEC records
            canonical["group_type_id"] = get_group_type_id(GroupType.GROUP_TYPE_COMMUNITY)

            # Static fields - always True for migrated records
            canonical["internal_application"] = True
            canonical["locked"] = True

            # Null-safe defaults for fields with no TEC source data
            for field in (
                "submitter",
                "approved_by",
                "assigned_approver",
                "review_due_date",
                "lodgement_date",
                "change_code",
            ):
                canonical.setdefault(field, None)

            rows.append(canonical)

        return ExtractionResult(rows=rows, warnings=read_warnings)


# Attach pipelines so the handler can find them
ConservationStatusTecAdapter.PIPELINES = PIPELINES
