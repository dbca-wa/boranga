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


# Shared cache for parent Occurrence objects to avoid redundant queries
_PARENT_OCC_CACHE = {}


def get_parent_occurrence(occ_mig_id):
    """
    Get parent Occurrence by migrated_from_id with caching.
    Shared by all parent OCC helper functions to minimize DB queries.
    """
    if not occ_mig_id:
        return None

    global _PARENT_OCC_CACHE
    if occ_mig_id not in _PARENT_OCC_CACHE:
        try:
            from boranga.components.occurrence.models import Occurrence

            occ = Occurrence.objects.filter(migrated_from_id=occ_mig_id).first()
            _PARENT_OCC_CACHE[occ_mig_id] = occ
        except Exception:
            _PARENT_OCC_CACHE[occ_mig_id] = None

    return _PARENT_OCC_CACHE.get(occ_mig_id)


_OCC_LOCATION_CACHE = {}


def get_parent_occ_location_value(occ_mig_id, field_name):
    """
    Copy location field from parent Occurrence's OCCLocation model.
    Used by Tasks 12573, 12575, 12578, 12583 to copy coordinate_source, district, region, location_description.
    """
    if not occ_mig_id:
        return None

    global _OCC_LOCATION_CACHE
    if occ_mig_id not in _OCC_LOCATION_CACHE:
        occ = get_parent_occurrence(occ_mig_id)
        if occ and hasattr(occ, "location"):
            _OCC_LOCATION_CACHE[occ_mig_id] = occ.location
        else:
            _OCC_LOCATION_CACHE[occ_mig_id] = None

    loc = _OCC_LOCATION_CACHE.get(occ_mig_id)
    if not loc:
        return None

    return getattr(loc, field_name, None)


_OCC_GEOMETRY_CACHE = {}


def get_parent_occ_geometry_value(occ_mig_id, field_name):
    """
    Copy geometry field from parent Occurrence's OccurrenceGeometry model.
    Task 12594: Copy geometry from parent OCC (not from SITES table).
    """
    if not occ_mig_id:
        return None

    global _OCC_GEOMETRY_CACHE
    if occ_mig_id not in _OCC_GEOMETRY_CACHE:
        try:
            from boranga.components.occurrence.models import OccurrenceGeometry

            occ = get_parent_occurrence(occ_mig_id)
            if occ:
                # Get the first geometry record for this occurrence
                geom = OccurrenceGeometry.objects.filter(occurrence=occ).first()
                _OCC_GEOMETRY_CACHE[occ_mig_id] = geom
            else:
                _OCC_GEOMETRY_CACHE[occ_mig_id] = None
        except Exception:
            _OCC_GEOMETRY_CACHE[occ_mig_id] = None

    geom = _OCC_GEOMETRY_CACHE.get(occ_mig_id)
    if not geom:
        return None

    return getattr(geom, field_name, None)


_OCC_IDENTIFICATION_CACHE = {}


def get_parent_occ_identification_value(occ_mig_id, field_name):
    """
    Copy identification field from parent Occurrence's OCCIdentification model.
    Used to copy identification_certainty from parent OCC to OCR.
    """
    if not occ_mig_id:
        return None

    global _OCC_IDENTIFICATION_CACHE
    if occ_mig_id not in _OCC_IDENTIFICATION_CACHE:
        occ = get_parent_occurrence(occ_mig_id)
        if occ and hasattr(occ, "identification"):
            _OCC_IDENTIFICATION_CACHE[occ_mig_id] = occ.identification
        else:
            _OCC_IDENTIFICATION_CACHE[occ_mig_id] = None

    identification = _OCC_IDENTIFICATION_CACHE.get(occ_mig_id)
    if not identification:
        return None

    return getattr(identification, field_name, None)


_OCR_CONTENT_TYPE_ID = None


def get_occurrence_report_content_type_id():
    """
    Get ContentType ID for OccurrenceReport model.
    Task 12589: OccurrenceReportGeometry__content_type needs static value for OccurrenceReport.
    """
    global _OCR_CONTENT_TYPE_ID
    if _OCR_CONTENT_TYPE_ID is None:
        try:
            from django.contrib.contenttypes.models import ContentType

            from boranga.components.occurrence.models import OccurrenceReport

            _OCR_CONTENT_TYPE_ID = ContentType.objects.get_for_model(OccurrenceReport).id
        except Exception:
            pass
    return _OCR_CONTENT_TYPE_ID


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
        "processing_status": [lambda val, ctx: _result("approved") if not val else _result(val)],
        "customer_status": [static_value_factory("approved")],
        # OCRObserverDetail defaults (Tasks 12563, 12564, 12566)
        "OCRObserverDetail__main_observer": [static_value_factory(True)],
        "OCRObserverDetail__visible": [static_value_factory(True)],
        # Task 12564: observer_name from SUR_SURVEYOR (mapped by schema, pass through)
        # Task 12573: Copy coordinate_source from parent OCC
        "OCRLocation__coordinate_source": [
            lambda val, ctx: _result(
                get_parent_occ_location_value(ctx.row.get("Occurrence__migrated_from_id"), "coordinate_source_id")
            )
        ],
        # Task 12575: Copy district from parent OCC
        "OCRLocation__district": [
            lambda val, ctx: _result(
                get_parent_occ_location_value(ctx.row.get("Occurrence__migrated_from_id"), "district_id")
            )
        ],
        # Task 12578: Copy region from parent OCC
        "OCRLocation__region": [
            lambda val, ctx: _result(
                get_parent_occ_location_value(ctx.row.get("Occurrence__migrated_from_id"), "region_id")
            )
        ],
        # Task 12583: Copy location_description from parent OCC
        "OCRLocation__location_description": [
            lambda val, ctx: _result(
                get_parent_occ_location_value(ctx.row.get("Occurrence__migrated_from_id"), "location_description")
            )
        ],
        # Task 12589: OccurrenceReportGeometry content_type = OccurrenceReport
        "OccurrenceReportGeometry__content_type": [lambda val, ctx: _result(get_occurrence_report_content_type_id())],
        # Task 12594: Copy geometry from parent OCC's OccurrenceGeometry (not from SITES)
        "OccurrenceReportGeometry__geometry": [
            lambda val, ctx: _result(
                get_parent_occ_geometry_value(ctx.row.get("Occurrence__migrated_from_id"), "geometry")
            )
        ],
        # Task 12599: SURVEYS geometry show_on_map = False (different from SITE_VISITS)
        "OccurrenceReportGeometry__show_on_map": [static_value_factory(False)],
        "OccurrenceReportGeometry__locked": [static_value_factory(True)],
        # Task 12623: habitat_notes from SURVEY_CONDITIONS concatenation
        "OCRHabitatComposition__habitat_notes": [concatenate_survey_conditions],
        # Copy identification_certainty from parent Occurrence's OCC Identification
        "OCRIdentification__identification_certainty": [
            lambda val, ctx: _result(
                get_parent_occ_identification_value(
                    ctx.row.get("Occurrence__migrated_from_id"), "identification_certainty_id"
                )
            )
        ],
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
