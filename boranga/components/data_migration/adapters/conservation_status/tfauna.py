"""TFAUNA Conservation Status adapter.

Maps rows from the FAUNA CS Migration Template CSV to canonical conservation
status records for bulk-import by the ConservationStatusImporter handler.

Task coverage (DevOps work items):
  11988 change_code             – FK lookup by code
  11989 commonwealth_conserv…   – FK lookup by code
  11991 iucn_version            – FK lookup by code
  11992 other_conserv_assess…   – FK lookup by code
  11993 species                 – resolved via species_taxonomy (taxon_name_id)
  11994 species_taxonomy        – taxon_name_id → Taxonomy PK via LegacyTaxonomyMapping
  11995 submitter_information   – created by handler (SubmitterInformation)
  11996 wa_legislative_category – FK lookup by code
  11997 wa_legislative_list     – FK lookup by code
  11998 wa_priority_category    – FK lookup by code
  11999 wa_priority_list        – static "FAUNA"
  12000 approval_level          – IF wa_leg_cat null → immediate ELSE minister
  12001 approved_by             – static default user boranga.tfauna@dbca.wa.gov.au
  12004 assigned_approver       – static default user
  12008 comment                 – string → text (passthrough)
  12009 conservation_criteria   – passthrough
  12011 customer_status         – aligned with processing_status value
  12014 effective_from          – date parse
  12015 effective_to            – date parse
  12016 internal_application    – static True
  12017 listing_date            – date parse
  12018 locked                  – static True
  12019 lodgement_date          – datetime parse
  12020 migrated_from_id        – passthrough with dedup suffix
  12021 processing_status       – Y→approved, N→delisted, C→closed
  12027 review_due_date         – calculated: approved + leg_cat CR/EN/VU → +10yr
  12028 submitter               – static default user
  12044 submitter_category      – handled by handler (default DBCA)
  12046 email_user (SI)         – set via submitter field → default user
  12048 organisation (SI)       – handled by handler (static DBCA)
"""

import logging
from datetime import datetime, timedelta

from boranga.components.conservation_status.models import (
    CommonwealthConservationList,
    ConservationChangeCode,
    ConservationStatus,
    IUCNVersion,
    OtherConservationAssessmentList,
)
from boranga.components.data_migration.mappings import get_group_type_id
from boranga.components.data_migration.registry import (
    datetime_iso_factory,
    fk_lookup,
)
from boranga.components.species_and_communities.models import GroupType

from ..base import ExtractionResult, ExtractionWarning, SourceAdapter
from ..sources import Source
from . import schema

logger = logging.getLogger(__name__)

# ── Constants ────────────────────────────────────────────────────────────────
DEFAULT_EMAIL = "boranga.tfauna@dbca.wa.gov.au"

# Processing-status code → Boranga value  (Task 12021)
PROCESSING_STATUS_MAP = {
    "Y": ConservationStatus.PROCESSING_STATUS_APPROVED,
    "N": ConservationStatus.PROCESSING_STATUS_DELISTED,
    "C": ConservationStatus.PROCESSING_STATUS_CLOSED,
}

# WA Legislative Category codes that trigger review_due_date (Task 12027)
REVIEW_DUE_WA_LEG_CATS = {"CR", "EN", "VU"}

# ── Pipeline look-ups (resolved during transform phase in handler) ───────────
COMMONWEALTH_LOOKUP = fk_lookup(CommonwealthConservationList, "code")
IUCN_LOOKUP = fk_lookup(IUCNVersion, "code")
CHANGE_CODE_LOOKUP = fk_lookup(ConservationChangeCode, "code")
OTHER_ASSESSMENT_LOOKUP = fk_lookup(OtherConservationAssessmentList, "code")
DATETIME_ISO_PERTH = datetime_iso_factory("Australia/Perth")

PIPELINES = {
    "migrated_from_id": ["strip", "required"],
    # species_id is resolved to Taxonomy PK in extract(); pipeline just validates
    "species_id": ["strip", "blank_to_none", "required"],
    "review_due_date": ["strip", "smart_date_parse"],
    "wa_legislative_category": ["strip", "blank_to_none", "wa_legislative_category_from_code"],
    "wa_legislative_list": ["strip", "blank_to_none", "wa_legislative_list_from_code"],
    "wa_priority_category": ["strip", "blank_to_none", "wa_priority_category_from_code"],
    "wa_priority_list": ["strip", "blank_to_none", "wa_priority_list_from_code"],
    "commonwealth_conservation_category": ["strip", "blank_to_none", COMMONWEALTH_LOOKUP],
    "iucn_version": ["strip", "blank_to_none", IUCN_LOOKUP],
    "change_code": ["strip", "blank_to_none", CHANGE_CODE_LOOKUP],
    "other_conservation_assessment": ["strip", "blank_to_none", OTHER_ASSESSMENT_LOOKUP],
    "conservation_criteria": ["strip", "blank_to_none"],
    "processing_status": ["strip", "blank_to_none"],
    "effective_from_date": ["strip", "smart_date_parse"],
    "effective_to_date": ["strip", "smart_date_parse"],
    "listing_date": ["strip", "smart_date_parse"],
    "lodgement_date": ["strip", DATETIME_ISO_PERTH],
    "comment": ["strip", "blank_to_none"],
}


class ConservationStatusTfaunaAdapter(SourceAdapter):
    source_key = Source.TFAUNA.value
    domain = "conservation_status"

    # ------------------------------------------------------------------ #
    #  Pre-load helpers (called once per run)
    # ------------------------------------------------------------------ #
    @staticmethod
    def _load_taxon_name_id_map() -> dict[int, int]:
        """Return {taxon_name_id: taxonomy_pk} from LegacyTaxonomyMapping for TFAUNA."""
        from boranga.components.main.models import LegacyTaxonomyMapping

        return dict(
            LegacyTaxonomyMapping.objects.filter(list_name="TFAUNA")
            .exclude(taxonomy_id__isnull=True)
            .values_list("taxon_name_id", "taxonomy_id")
        )

    @staticmethod
    def _resolve_default_user() -> int | None:
        """Return the emailuser_id for the TFAUNA default user."""
        from ledger_api_client.ledger_models import EmailUserRO

        try:
            return EmailUserRO.objects.get(email=DEFAULT_EMAIL).id
        except EmailUserRO.DoesNotExist:
            logger.error("Default TFAUNA email user '%s' not found!", DEFAULT_EMAIL)
            return None

    # ------------------------------------------------------------------ #
    #  Main extraction
    # ------------------------------------------------------------------ #
    def extract(self, path: str, **options) -> ExtractionResult:
        rows: list[dict] = []
        warnings: list[ExtractionWarning] = []

        # Pre-load lookups
        tni_to_tid = self._load_taxon_name_id_map()
        default_user_id = self._resolve_default_user()
        group_type_id = get_group_type_id(GroupType.GROUP_TYPE_FAUNA)

        if default_user_id is None:
            warnings.append(
                ExtractionWarning(
                    row_index=0,
                    column="submitter",
                    message=f"Default email user '{DEFAULT_EMAIL}' not found – "
                    "approved_by / assigned_approver / submitter will be None",
                )
            )

        # Read CSV
        raw_rows, read_warnings = self.read_table(path, encoding="utf-8-sig")
        warnings.extend(read_warnings)

        migrated_id_counts: dict[str, int] = {}

        for row_idx, raw in enumerate(raw_rows, start=1):
            canonical = schema.map_raw_row(raw)

            # ── migrated_from_id dedup  (Task 12020) ──────────────────
            m_id = canonical.get("migrated_from_id")
            if m_id:
                m_id = str(m_id).strip()
                count = migrated_id_counts.get(m_id, 0) + 1
                migrated_id_counts[m_id] = count
                canonical["migrated_from_id"] = f"{m_id}-{count:02d}"

            # ── Group Type ────────────────────────────────────────────
            canonical["group_type_id"] = group_type_id

            # ── Species resolution  (Tasks 11993, 11994) ─────────────
            # species_taxonomy column → taxon_name_id → Taxonomy PK
            raw_tni = canonical.pop("species_taxonomy_taxon_name_id", None)
            if raw_tni is not None:
                try:
                    tni = int(str(raw_tni).strip())
                except (ValueError, TypeError):
                    tni = None
            else:
                tni = None

            if tni and tni in tni_to_tid:
                canonical["species_id"] = tni_to_tid[tni]
            elif tni:
                # taxon_name_id not in LegacyTaxonomyMapping; try Taxonomy directly
                from boranga.components.species_and_communities.models import Taxonomy

                tid = Taxonomy.objects.filter(taxon_name_id=tni).values_list("id", flat=True).first()
                if tid:
                    canonical["species_id"] = tid
                else:
                    warnings.append(
                        ExtractionWarning(
                            row_index=row_idx,
                            column="species_taxonomy",
                            message=f"taxon_name_id={tni} not found in Taxonomy",
                        )
                    )
                    canonical["species_id"] = None  # will fail required check
            else:
                # No taxon_name_id; fall back to species name via LegacyTaxonomyMapping
                # (species column mapped to species_id by schema)
                # The pipeline "required" step will catch missing values.
                pass

            # ── Processing Status  (Task 12021) ──────────────────────
            p_status = canonical.get("processing_status")
            if p_status:
                p_status = str(p_status).strip().upper()
                canonical["processing_status"] = PROCESSING_STATUS_MAP.get(p_status, p_status.lower())

            # ── WA Priority List  (Task 11999) ───────────────────────
            raw_prio_cat = canonical.get("wa_priority_category")
            if raw_prio_cat and str(raw_prio_cat).strip():
                canonical["wa_priority_list"] = "FAUNA"
                canonical["wa_priority_category"] = str(raw_prio_cat).strip()
            else:
                canonical["wa_priority_list"] = None
                canonical["wa_priority_category"] = None

            # ── WA Legislative List  (Tasks 11996, 11997) ────────────
            raw_leg_list = canonical.get("wa_legislative_list")
            raw_leg_cat = canonical.get("wa_legislative_category")
            if raw_leg_list and str(raw_leg_list).strip():
                canonical["wa_legislative_list"] = str(raw_leg_list).strip()
                canonical["wa_legislative_category"] = str(raw_leg_cat).strip() if raw_leg_cat else None
            else:
                canonical["wa_legislative_list"] = None
                canonical["wa_legislative_category"] = None

            # ── review_due_date  (Task 12027) ────────────────────────
            wa_leg_cat = canonical.get("wa_legislative_category")
            if (
                canonical.get("processing_status") == ConservationStatus.PROCESSING_STATUS_APPROVED
                and wa_leg_cat
                and str(wa_leg_cat).strip().upper() in REVIEW_DUE_WA_LEG_CATS
                and canonical.get("effective_from_date")
            ):
                dt_str = canonical["effective_from_date"]
                dt = None
                if isinstance(dt_str, str):
                    for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
                        try:
                            dt = datetime.strptime(dt_str.strip(), fmt).date()
                            break
                        except ValueError:
                            pass

                if dt:
                    try:
                        new_date = dt.replace(year=dt.year + 10)
                    except ValueError:  # Feb 29
                        new_date = dt + timedelta(days=365 * 10 + 2)
                    canonical["review_due_date"] = new_date

            # ── approval_level  (Task 12000) ─────────────────────────
            if not wa_leg_cat or not str(wa_leg_cat).strip():
                canonical["approval_level"] = "immediate"
            else:
                canonical["approval_level"] = "minister"

            # ── Static fields ────────────────────────────────────────
            canonical["locked"] = True  # Task 12018
            canonical["internal_application"] = True  # Task 12016
            canonical["customer_status"] = canonical.get("processing_status")  # Task 12011

            # Default user fields  (Tasks 12001, 12004, 12028)
            canonical["approved_by"] = default_user_id
            canonical["assigned_approver"] = default_user_id
            canonical["submitter"] = default_user_id

            rows.append(canonical)

        return ExtractionResult(rows=rows, warnings=warnings)


# Expose pipelines so the handler can merge them
ConservationStatusTfaunaAdapter.PIPELINES = PIPELINES
