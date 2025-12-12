import csv
import logging
from datetime import date, timedelta
from pathlib import Path

from django.conf import settings

from boranga.components.conservation_status.models import ConservationStatus
from boranga.components.data_migration.mappings import get_group_type_id
from boranga.components.species_and_communities.models import GroupType

from ..base import ExtractionResult, ExtractionWarning, SourceAdapter
from ..sources import Source
from . import schema

logger = logging.getLogger(__name__)

# Load user map
USER_MAP = {}
try:
    # Path to the user map CSV
    csv_path = (
        Path(settings.BASE_DIR)
        / "private-media"
        / "legacy_data"
        / "legacy-username-emailuser-map-TPFL.csv"
    )
    if csv_path.exists():
        with open(csv_path) as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Assuming columns: legacy_username, email_user_id
                if row.get("legacy_username") and row.get("email_user_id"):
                    USER_MAP[row["legacy_username"].strip()] = int(row["email_user_id"])
    else:
        logger.warning(f"User map CSV not found at {csv_path}")
except Exception as e:
    logger.warning(f"Could not load user map: {e}")

# TPFL-specific transforms and pipelines
PROCESSING_STATUS_MAP = {
    "Approved": ConservationStatus.PROCESSING_STATUS_APPROVED,
    "Closed": ConservationStatus.PROCESSING_STATUS_CLOSED,
    "Delisted": ConservationStatus.PROCESSING_STATUS_DELISTED,
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

            # 1. Group Type
            canonical["group_type_id"] = get_group_type_id(GroupType.GROUP_TYPE_FLORA)

            # 2. Processing Status
            p_status = raw.get("PROCESSING_STATUS")
            if p_status:
                p_status = p_status.strip()
                canonical["processing_status"] = PROCESSING_STATUS_MAP.get(
                    p_status, p_status.lower()
                )

            # 3. Dates
            eff_date = raw.get("EFFECTIVE_FROM_DATE")
            if eff_date:
                try:
                    canonical["effective_from_date"] = date.fromisoformat(eff_date)
                except ValueError:
                    canonical["effective_from_date"] = None

            # 4. Users
            sub = raw.get("SUBMITTER")
            if sub:
                canonical["submitter"] = USER_MAP.get(sub.strip())

            app = raw.get("ASSIGNED_APPROVER")
            if app:
                canonical["assigned_approver"] = USER_MAP.get(app.strip())
                canonical["approved_by"] = USER_MAP.get(app.strip())

            # 5. Calculated fields
            wa_leg_cat = raw.get("WA_LEGISLATIVE_CATEGORY")
            if wa_leg_cat:
                wa_leg_cat = wa_leg_cat.strip()
                canonical["wa_legislative_category"] = wa_leg_cat

            # review_due_date
            if (
                canonical.get("processing_status")
                == ConservationStatus.PROCESSING_STATUS_APPROVED
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
            canonical["wa_priority_list"] = "FLORA"

            rows.append(canonical)

        return ExtractionResult(rows=rows, warnings=warnings)
