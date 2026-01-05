from datetime import datetime

from dateutil.relativedelta import relativedelta

from boranga.components.data_migration.registry import _result, registry

from ..base import ExtractionResult, SourceAdapter
from ..sources import Source
from . import schema


@registry.register("cs_processing_status_transform")
def t_cs_processing_status(value, ctx):
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
    # Align with processing_status
    # If value is present, use it? Task says "no data" in legacy table.
    # So we derive from processing_status (which is in ctx.row["processing_status"])
    # Note: ctx.row is the RAW row.
    raw_status = ctx.row.get("processing_status")
    if not raw_status:
        return _result(None)
    val = str(raw_status).strip().upper()
    if val == "Y":
        return _result("approved")
    if val == "N":
        return _result("delisted")
    if val == "C":
        return _result("closed")
    return _result("draft")


@registry.register("cs_approval_level_transform")
def t_cs_approval_level(value, ctx):
    # IF wa_legislative_category = Null, then "Immediate"; ELSE, "Ministerial"
    row = ctx.row
    wa_leg_cat = row.get("wa_legislative_category")
    if not wa_leg_cat or not str(wa_leg_cat).strip():
        return _result("immediate")
    return _result("ministerial")


@registry.register("cs_review_due_date_transform")
def t_cs_review_due_date(value, ctx):
    # IF processing_status = "Approved" AND wa_legislative_category IN ["CR", "EN", "VU"]
    # THEN effective_from_date + 10 years
    row = ctx.row
    raw_p_status = row.get("processing_status")
    raw_wa_leg_cat = row.get("wa_legislative_category")
    raw_eff_date = row.get("effective_from")

    if not raw_eff_date:
        return _result(None)

    # Parse date
    dt = None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d/%m/%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.strptime(str(raw_eff_date).strip(), fmt).date()
            break
        except ValueError:
            pass

    if not dt:
        return _result(None)

    is_approved = str(raw_p_status).strip().upper() == "Y"
    cat = str(raw_wa_leg_cat).strip().upper() if raw_wa_leg_cat else ""
    is_threatened = cat in ["CR", "EN", "VU"]

    if is_approved and is_threatened:
        return _result(dt + relativedelta(years=10))

    return _result(None)


PIPELINES = {
    "processing_status": ["strip", "cs_processing_status_transform"],
    "customer_status": ["cs_customer_status_transform"],
    "approval_level": ["cs_approval_level_transform"],
    "review_due_date": ["cs_review_due_date_transform"],
    "internal_application": ["static_value_True"],
    "locked": ["static_value_True"],
    "submitter": ["static_value_boranga.tec@dbca.wa.gov.au", "emailuser_by_email"],
    "approved_by": ["static_value_boranga.tec@dbca.wa.gov.au", "emailuser_by_email"],
    "assigned_approver": [
        "static_value_boranga.tec@dbca.wa.gov.au",
        "emailuser_by_email",
    ],
    # wa_priority_list will be handled in the handler (set to COMMUNITY for TEC)
    "community_migrated_from_id": ["strip", "required"],
    "effective_from_date": [
        "strip",
        "date_iso",
    ],  # Ensure this maps from 'effective_from' column
}


class ConservationStatusTecAdapter(SourceAdapter):
    source_key = Source.TEC.value
    domain = "conservation_status"
    PIPELINES = PIPELINES  # Make pipelines available to handler

    def extract(self, path: str, **options) -> ExtractionResult:
        raw_rows, read_warnings = self.read_table(path, encoding="utf-8-sig")
        rows = []

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

            rows.append(canonical)

        return ExtractionResult(rows=rows, warnings=read_warnings)
