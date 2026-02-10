from django.contrib.auth import get_user_model

from boranga.components.data_migration.registry import _result, registry

from ..base import ExtractionResult, SourceAdapter
from ..sources import Source
from . import schema


@registry.register("cs_processing_status_transform")
def t_cs_processing_status(value, ctx):
    """Transform TEC processing_status codes to system values."""
    if not value:
        return _result(None)
    val = str(value).strip().upper()
    # Handle TEC priority codes (P1, P2, P3, P4, P5)
    if val.startswith("P"):
        return _result("approved")  # Priority listings are approved
    # Handle explicit status codes
    if val == "Y":
        return _result("approved")
    if val == "N":
        return _result("delisted")
    if val == "C":
        return _result("closed")
    return _result(value.lower())


@registry.register("cs_customer_status_transform")
def t_cs_customer_status(value, ctx):
    """Map customer_status from processing_status."""
    raw_status = ctx.row.get("processing_status")
    if not raw_status:
        return _result(None)
    val = str(raw_status).strip().upper()
    if val == "APPROVED":
        return _result("approved")
    if val == "DELISTED":
        return _result("delisted")
    if val == "CLOSED":
        return _result("closed")
    return _result("draft")


@registry.register("cs_approval_level_transform")
def t_cs_approval_level(value, ctx):
    """Determine approval_level based on wa_legislative_category."""
    row = ctx.row
    wa_leg_cat = row.get("wa_legislative_category")
    if not wa_leg_cat or not str(wa_leg_cat).strip():
        return _result("immediate")
    return _result("ministerial")


PIPELINES = {
    "processing_status": ["strip", "cs_processing_status_transform"],
    "customer_status": ["cs_customer_status_transform"],
    "approval_level": ["cs_approval_level_transform"],
    "internal_application": ["static_value_True"],
    "locked": ["static_value_True"],
    "community_migrated_from_id": ["strip", "required", "community_id_from_legacy"],
    "effective_from_date": ["strip", "date_iso"],
    "wa_priority_list": ["wa_priority_list_from_code"],
    "wa_priority_category": ["strip", "blank_to_none", "wa_priority_category_from_code"],
    "wa_legislative_list": ["strip", "blank_to_none", "wa_legislative_list_from_code"],
    "wa_legislative_category": ["strip", "blank_to_none", "wa_legislative_category_from_code"],
}


class ConservationStatusTecAdapter(SourceAdapter):
    source_key = Source.TEC.value
    domain = "conservation_status"

    def extract(self, path: str, **options) -> ExtractionResult:
        raw_rows, read_warnings = self.read_table(path, encoding="utf-8-sig")
        rows = []

        # Load TEC user for submitter/approver defaults
        User = get_user_model()
        tec_user = User.objects.filter(email="boranga.tec@dbca.wa.gov.au").first()

        for raw in raw_rows:
            # Map raw CSV headers to canonical field names
            canonical = schema.map_raw_row(raw)

            # Ensure all required optional fields exist with defaults
            for field in [
                "customer_status",
                "approval_level",
                "review_due_date",
                "internal_application",
                "locked",
                "submitter",
                "approved_by",
                "assigned_approver",
                "wa_priority_list",
                "species_name",
                "wa_legislative_category",
                "wa_legislative_list",
                "wa_priority_category",
            ]:
                if field not in canonical:
                    canonical[field] = None

            # Set TEC-specific defaults directly
            canonical["internal_application"] = True
            canonical["locked"] = True
            canonical["wa_priority_list"] = "COMMUNITY"

            # TEC uses COM_NO as both the unique migrated_from_id AND as the community identifier
            # So community_migrated_from_id should also be COM_NO
            com_no = canonical.get("migrated_from_id")
            if com_no:
                canonical["community_migrated_from_id"] = com_no

            # Set user defaults
            if tec_user:
                canonical["submitter"] = tec_user.id
                canonical["approved_by"] = tec_user.id
                canonical["assigned_approver"] = tec_user.id

            rows.append(canonical)

        return ExtractionResult(rows=rows, warnings=read_warnings)


# Attach to adapter class so handlers can detect adapter-specific pipelines
ConservationStatusTecAdapter.PIPELINES = PIPELINES
