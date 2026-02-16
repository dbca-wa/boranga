from boranga.components.data_migration.mappings import get_group_type_id
from boranga.components.data_migration.registry import (
    _result,
    dependent_from_column_factory,
    emailuser_object_by_legacy_username_factory,
    fk_lookup_static,
    static_value_factory,
)
from boranga.components.users.models import SubmitterCategory

from ..base import ExtractionResult, SourceAdapter
from ..sources import Source
from . import schema
from .tec_shared import TEC_USER_LOOKUP

# Lookup submitter category by name (not hardcoded ID)
SUBMITTER_CATEGORY_DBCA = fk_lookup_static(
    model=SubmitterCategory,
    lookup_field="name",
    static_value="DBCA",
)

# Task 12623: Cache for SURVEY_CONDITIONS.csv data
_SURVEY_CONDITIONS_CACHE = None


def load_survey_conditions_map(path: str) -> dict[tuple[str, str], list[str]]:
    """
    Load SURVEY_CONDITIONS.csv and map (OCC_UNIQUE_ID, SUR_NO) -> [SCON_COMMENTS, ...]
    Returns dict mapping (occ_id, sur_no) tuples to list of comment strings.
    """
    global _SURVEY_CONDITIONS_CACHE

    if _SURVEY_CONDITIONS_CACHE is not None:
        return _SURVEY_CONDITIONS_CACHE

    import logging
    import os
    from collections import defaultdict

    import pandas as pd

    logger = logging.getLogger(__name__)

    base_dir = os.path.dirname(path)
    conditions_path = os.path.join(base_dir, "SURVEY_CONDITIONS.csv")

    if not os.path.exists(conditions_path):
        logger.warning(f"SURVEY_CONDITIONS.csv not found at {conditions_path}")
        _SURVEY_CONDITIONS_CACHE = {}
        return _SURVEY_CONDITIONS_CACHE

    mapping = defaultdict(list)
    try:
        df = pd.read_csv(conditions_path, dtype=str).fillna("")
        for _, row in df.iterrows():
            occ_id = row.get("OCC_UNIQUE_ID", "").strip()
            sur_no = row.get("SUR_NO", "").strip()
            comment = row.get("SCON_COMMENTS", "").strip()

            if occ_id and sur_no and comment:
                key = (occ_id, sur_no)
                mapping[key].append(comment)

        logger.info(f"Loaded {len(mapping)} OCC+SUR combinations from {conditions_path}")
        _SURVEY_CONDITIONS_CACHE = dict(mapping)
    except Exception as e:
        logger.warning(f"Failed to load SURVEY_CONDITIONS.csv: {e}")
        _SURVEY_CONDITIONS_CACHE = {}

    return _SURVEY_CONDITIONS_CACHE


def concatenate_survey_conditions(val, ctx):
    """
    Task 12623: Concatenate SCON_COMMENTS from SURVEY_CONDITIONS.csv
    Uses OCC_UNIQUE_ID + SUR_NO to find related subrecords.

    Note: After column mapping, OCC_UNIQUE_ID is renamed to "Occurrence__migrated_from_id",
    but we need the raw value for looking up in SURVEY_CONDITIONS mapping.
    """
    if not ctx or not hasattr(ctx, "row"):
        return _result(None)

    row = ctx.row

    # Get the mapped conditions from the row (attached during extract)
    conditions_map = row.get("_survey_conditions_map", {})
    # Also get raw OCC_UNIQUE_ID and SUR_NO for lookup key
    occ_id = row.get("_raw_OCC_UNIQUE_ID", "").strip()
    sur_no = row.get("SUR_NO", "").strip()

    if not occ_id or not sur_no:
        return _result(None)

    key = (occ_id, sur_no)
    comments = conditions_map.get(key, [])

    if not comments:
        return _result(None)

    # Concatenate with "; " separator
    concatenated = "; ".join(comments)
    return _result(concatenated)


class OccurrenceReportTecSurveysAdapter(SourceAdapter):
    source_key = Source.TEC_SURVEYS.value
    domain = "occurrence_report"

    # Reusable transform: USERNAME -> EmailUser object -> full name
    _EMAILUSER_OBJ = emailuser_object_by_legacy_username_factory("TEC")

    @staticmethod
    def _get_full_name_or_default(value, ctx):
        """Call get_full_name() on EmailUserRO object, return 'DBCA' if None/error."""
        if value is None:
            return _result("DBCA")
        try:
            if hasattr(value, "get_full_name"):
                full_name = value.get_full_name()
                if full_name and full_name.strip():
                    return _result(full_name.strip())
        except Exception:
            pass
        return _result("DBCA")

    PIPELINES = {
        "internal_application": [static_value_factory(True)],
        "submitter": [TEC_USER_LOOKUP, "required"],
        # Copy submitter to other user fields
        "assigned_approver_id": [dependent_from_column_factory("submitter", mapping=TEC_USER_LOOKUP)],
        "assigned_officer_id": [dependent_from_column_factory("submitter", mapping=TEC_USER_LOOKUP)],
        "approved_by": [dependent_from_column_factory("submitter", mapping=TEC_USER_LOOKUP)],
        # Also populate SubmitterInformation with the same user
        "SubmitterInformation__email_user": [dependent_from_column_factory("submitter", mapping=TEC_USER_LOOKUP)],
        # Task 12570: SubmitterInformation name field - map USERNAME to EmailUser full name
        "SubmitterInformation__name": [
            dependent_from_column_factory("submitter", mapping=_EMAILUSER_OBJ),
            _get_full_name_or_default,
        ],
        # SubmitterInformation defaults
        "SubmitterInformation__submitter_category": [SUBMITTER_CATEGORY_DBCA],
        "SubmitterInformation__organisation": [static_value_factory("DBCA")],
        "processing_status": [lambda val, ctx: _result("Approved") if not val else _result(val)],
        "customer_status": [
            dependent_from_column_factory(
                "processing_status",
                mapper=lambda val, ctx: "Approved" if val == "Approved" else None,
            )
        ],
        # OCRObserverDetail defaults (Tasks 12563, 12566)
        "OCRObserverDetail__main_observer": [static_value_factory(True)],
        "OCRObserverDetail__visible": [static_value_factory(True)],
        # Task 12599: SURVEYS geometry show_on_map = False (different from SITE_VISITS)
        "OccurrenceReportGeometry__show_on_map": [static_value_factory(False)],
        "OccurrenceReportGeometry__locked": [static_value_factory(True)],
        # Task 12623: habitat_notes from SURVEY_CONDITIONS concatenation
        "OCRHabitatComposition__habitat_notes": [concatenate_survey_conditions],
    }

    def extract(self, path: str, **options) -> ExtractionResult:
        # Get community group id safely
        community_group_id = get_group_type_id("community")

        # Task 12623: Pre-load SURVEY_CONDITIONS for habitat_notes concatenation
        survey_conditions_map = load_survey_conditions_map(path)

        raw_rows, warnings = self.read_table(path)
        rows = []

        for raw in raw_rows:
            canonical = schema.map_raw_row(raw)
            canonical["group_type_id"] = community_group_id

            # Structural fix for Survey ID
            sur_no = raw.get("SUR_NO")
            occ_id = raw.get("OCC_UNIQUE_ID")

            # Construct migrated_from_id for Surveys
            if sur_no and occ_id:
                canonical["migrated_from_id"] = f"tec-survey-{sur_no}-occ-{occ_id}"

            # Map OCC_UNIQUE_ID to Occurrence__migrated_from_id with prefix so Handler can find it
            if occ_id:
                canonical["Occurrence__migrated_from_id"] = f"tec-{occ_id}"

            # Ensure SUR_NO is in canonical row so tec_user_lookup can find it in context
            if sur_no:
                canonical["SUR_NO"] = sur_no

            # Task 12623: Preserve raw OCC_UNIQUE_ID for SURVEY_CONDITIONS lookup in transform
            canonical["_raw_OCC_UNIQUE_ID"] = occ_id if occ_id else ""

            # Task 12623: Attach survey_conditions_map to each row for the pipeline to access
            canonical["_survey_conditions_map"] = survey_conditions_map

            rows.append(canonical)

        return ExtractionResult(rows=rows, warnings=warnings)
