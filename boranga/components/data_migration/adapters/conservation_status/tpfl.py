import logging
from datetime import datetime, timedelta

from boranga.components.conservation_status.models import ConservationStatus
from boranga.components.data_migration.mappings import get_group_type_id
from boranga.components.data_migration.registry import (
    emailuser_by_legacy_username_factory,
    taxonomy_lookup_legacy_mapping_species,
)
from boranga.components.species_and_communities.models import GroupType

from ..base import ExtractionResult, ExtractionWarning, SourceAdapter
from ..sources import Source
from . import schema

logger = logging.getLogger(__name__)


# TPFL-specific transforms and pipelines
SPECIES_LOOKUP = taxonomy_lookup_legacy_mapping_species("TPFL")
EMAIL_USER_TPFL = emailuser_by_legacy_username_factory("TPFL")

PROCESSING_STATUS_MAP = {
    "Approved": ConservationStatus.PROCESSING_STATUS_APPROVED,
    "Closed": ConservationStatus.PROCESSING_STATUS_CLOSED,
    "Delisted": ConservationStatus.PROCESSING_STATUS_DELISTED,
}

PIPELINES = {
    "migrated_from_id": ["strip", "required"],
    "species_id": ["strip", "blank_to_none", "required", SPECIES_LOOKUP],
    "review_due_date": ["strip", "smart_date_parse"],
    "community_migrated_from_id": ["strip", "blank_to_none"],
    "wa_legislative_category": ["strip", "blank_to_none"],
    "wa_legislative_list": ["strip", "blank_to_none"],
    "wa_priority_category": ["strip", "blank_to_none"],
    "wa_priority_list": ["strip", "blank_to_none"],
    "approved_by": ["strip", "blank_to_none", EMAIL_USER_TPFL],
    "processing_status": ["strip", "blank_to_none"],
    "effective_from_date": ["strip", "smart_date_parse"],
    "effective_to_date": ["strip", "smart_date_parse"],
    "listing_date": ["strip", "smart_date_parse"],
    "lodgement_date": ["strip", "smart_date_parse"],
    "submitter": ["strip", "blank_to_none", EMAIL_USER_TPFL],
    "comment": ["strip", "blank_to_none"],
    "customer_status": ["strip", "blank_to_none"],
    "internal_application": ["strip", "blank_to_none"],
    "locked": ["strip", "blank_to_none"],
}


class ConservationStatusTpflAdapter(SourceAdapter):
    source_key = Source.TPFL.value
    domain = "conservation_status"

    def extract(self, path: str, **options) -> ExtractionResult:
        rows = []
        warnings: list[ExtractionWarning] = []

        # Use utf-8-sig to handle potential BOM
        raw_rows, read_warnings = self.read_table(path, encoding="utf-8-sig")
        warnings.extend(read_warnings)

        migrated_id_counts = {}

        for raw in raw_rows:
            canonical = schema.map_raw_row(raw)

            # Handle duplicate migrated_from_id - append sequence suffix
            m_id = canonical.get("migrated_from_id")
            if m_id:
                m_id = str(m_id).strip()
                count = migrated_id_counts.get(m_id, 0) + 1
                migrated_id_counts[m_id] = count
                canonical["migrated_from_id"] = f"{m_id}-{count:02d}"

            # 1. Group Type
            canonical["group_type_id"] = get_group_type_id(GroupType.GROUP_TYPE_FLORA)

            # 2. Processing Status
            p_status = canonical.get("processing_status")
            if p_status:
                p_status = p_status.strip()
                canonical["processing_status"] = PROCESSING_STATUS_MAP.get(p_status, p_status.lower())

            # 3. Dates
            eff_date = canonical.get("effective_from_date")
            if eff_date:
                try:
                    # Try parsing with time
                    dt = datetime.strptime(eff_date, "%d/%m/%Y %H:%M")
                    canonical["effective_from_date"] = dt.date()
                except ValueError:
                    try:
                        # Try parsing without time just in case
                        dt = datetime.strptime(eff_date, "%d/%m/%Y")
                        canonical["effective_from_date"] = dt.date()
                    except ValueError:
                        canonical["effective_from_date"] = None

            # 4. Users (moved to pipeline)

            # 5. Calculated fields
            raw_leg_list = canonical.get("wa_legislative_list")
            raw_leg_cat = canonical.get("wa_legislative_category")
            raw_prio_cat = canonical.get("wa_priority_category")

            # Clean up
            if raw_leg_list:
                raw_leg_list = raw_leg_list.strip()
            if raw_leg_cat:
                raw_leg_cat = raw_leg_cat.strip()
            if raw_prio_cat:
                raw_prio_cat = raw_prio_cat.strip()

            # Priority List Logic
            if raw_prio_cat:
                canonical["wa_priority_list"] = "FLORA"
                canonical["wa_priority_category"] = raw_prio_cat
            else:
                canonical["wa_priority_list"] = None
                canonical["wa_priority_category"] = None

            # Legislative List Logic
            if raw_leg_list:
                canonical["wa_legislative_list"] = raw_leg_list
                canonical["wa_legislative_category"] = raw_leg_cat
            else:
                canonical["wa_legislative_list"] = None
                canonical["wa_legislative_category"] = None

            # review_due_date
            wa_leg_cat = canonical.get("wa_legislative_category")
            if (
                canonical.get("processing_status") == ConservationStatus.PROCESSING_STATUS_APPROVED
                and wa_leg_cat in ["CR", "EN", "VU"]
                and canonical.get("effective_from_date")
            ):
                dt = canonical["effective_from_date"]
                try:
                    new_date = dt.replace(year=dt.year + 10)
                except ValueError:  # Feb 29
                    new_date = dt + timedelta(days=365 * 10 + 2)
                canonical["review_due_date"] = new_date

            # approval_level
            if not wa_leg_cat:
                canonical["approval_level"] = "immediate"
            else:
                canonical["approval_level"] = "minister"

            # 6. Static values
            canonical["locked"] = True
            canonical["internal_application"] = True
            canonical["customer_status"] = canonical.get("processing_status")

            rows.append(canonical)

        return ExtractionResult(rows=rows, warnings=warnings)


# Attach to adapter class so handlers/registry can detect adapter-specific pipelines
ConservationStatusTpflAdapter.PIPELINES = PIPELINES
