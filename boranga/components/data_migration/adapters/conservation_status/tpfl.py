import csv
import logging
from datetime import datetime, timedelta
from pathlib import Path

from django.conf import settings
from ledger_api_client.ledger_models import EmailUserRO

from boranga.components.conservation_status.models import ConservationStatus
from boranga.components.data_migration.mappings import get_group_type_id
from boranga.components.species_and_communities.models import GroupType

from ..base import ExtractionResult, ExtractionWarning, SourceAdapter
from ..sources import Source
from . import schema

logger = logging.getLogger(__name__)


def _load_user_map():
    user_map = {}
    try:
        # Path to the user map CSV
        csv_path = (
            Path(settings.BASE_DIR)
            / "private-media"
            / "legacy_data"
            / "TPFL"
            / "legacy-username-emailuser-map-TPFL.csv"
        )
        if csv_path.exists():
            with open(csv_path, encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    legacy_username = row.get("legacy_username")
                    email = row.get("email")

                    if legacy_username and email:
                        try:
                            user = EmailUserRO.objects.get(email__iexact=email.strip())
                            user_map[legacy_username.strip()] = user.id
                        except EmailUserRO.DoesNotExist:
                            # logger.warning(f"User with email {email} not found in DB.")
                            pass
        else:
            logger.warning(f"User map CSV not found at {csv_path}")
    except Exception as e:
        logger.warning(f"Could not load user map: {e}")
    return user_map


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
        user_map = _load_user_map()
        rows = []
        warnings: list[ExtractionWarning] = []

        # Use utf-8-sig to handle potential BOM
        raw_rows, read_warnings = self.read_table(path, encoding="utf-8-sig")
        warnings.extend(read_warnings)

        for raw in raw_rows:
            canonical = schema.map_raw_row(raw)

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

            # 4. Users
            sub = canonical.get("submitter")
            appr = canonical.get("approved_by")
            if sub:
                # Get user id for submitter and approver from user_map
                user_id = user_map.get(sub.strip())
                canonical["submitter"] = user_id

            if appr:
                appr_user_id = user_map.get(appr.strip())
                canonical["approved_by"] = appr_user_id

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
