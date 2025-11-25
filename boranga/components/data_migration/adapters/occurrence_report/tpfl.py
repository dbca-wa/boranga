import boranga.components.data_migration.mappings as dm_mappings
from boranga.components.data_migration.mappings import get_group_type_id
from boranga.components.data_migration.registry import (
    _result,
    build_legacy_map_transform,
    csv_lookup_factory,
    dependent_from_column_factory,
    emailuser_by_legacy_username_factory,
    fk_lookup,
    pluck_attribute_factory,
    taxonomy_lookup_legacy_mapping_species,
    to_int_trailing_factory,
)
from boranga.components.occurrence.models import OccurrenceReport
from boranga.components.species_and_communities.models import Community, GroupType

from ..base import ExtractionResult, ExtractionWarning, SourceAdapter
from ..sources import Source
from . import schema

# TPFL-specific transforms and pipelines
# build mapper that reads SHEETNO column and returns POP_ID (uses cached map)
POP_ID_FROM_SHEETNO = dependent_from_column_factory(
    "SHEETNO",
    mapper=lambda dep, ctx: dm_mappings.get_pop_id_for_sheetno(
        dep, legacy_system="TPFL"
    ),
    default=None,
)

SPECIES_TRANSFORM = taxonomy_lookup_legacy_mapping_species("TPFL")

COMMUNITY_TRANSFORM = fk_lookup(model=Community, lookup_field="community_name")


def map_form_status_code_to_processing_status(value: str, ctx=None) -> str | None:
    mapping = {
        "NEW": OccurrenceReport.PROCESSING_STATUS_DRAFT,
        "READY": OccurrenceReport.PROCESSING_STATUS_WITH_ASSESSOR,
        "ACCEPTED": OccurrenceReport.PROCESSING_STATUS_APPROVED,
        "REJECTED": OccurrenceReport.PROCESSING_STATUS_DECLINED,
    }
    return mapping.get(value.strip().upper()) if value else None


def map_form_status_code_to_customer_status(value: str, ctx=None) -> str | None:
    mapping = {
        "NEW": OccurrenceReport.CUSTOMER_STATUS_DRAFT,
        "READY": OccurrenceReport.CUSTOMER_STATUS_WITH_ASSESSOR,
        "ACCEPTED": OccurrenceReport.CUSTOMER_STATUS_APPROVED,
        "REJECTED": OccurrenceReport.CUSTOMER_STATUS_DECLINED,
    }
    return mapping.get(value.strip().upper()) if value else None


def MAP_FORM_STATUS_CODE_TO_PROCESSING_STATUS(value, ctx):
    return _result(map_form_status_code_to_processing_status(value, ctx))


MAP_FORM_STATUS_CODE_TO_CUSTOMER_STATUS = map_form_status_code_to_customer_status


CUSTOMER_STATUS_FROM_FORM_STATUS_CODE = dependent_from_column_factory(
    "processing_status",
    mapper=lambda dep_val, ctx: MAP_FORM_STATUS_CODE_TO_CUSTOMER_STATUS(dep_val, ctx),
    default=None,
)

FK_OCCURRENCE = fk_lookup(OccurrenceReport, "migrated_from_id")

OCCURRENCE_FROM_POP_ID = dependent_from_column_factory("POP_ID", FK_OCCURRENCE)

RECORD_SOURCE_FROM_CSV = csv_lookup_factory(
    key_column="TERM_CODE",
    value_column="LABEL",
    csv_filename="DRF_LOV_RECORD_SOURCE_VWS.csv",
    legacy_system="TPFL",
    required=False,
)

SUBMITTER_TRANSFORM = emailuser_by_legacy_username_factory("TPFL")

ROLE_TRANSFORM = build_legacy_map_transform(
    "TPFL",
    "OBS_ROLE_CODE (DRF_LOV_ROLE_VWS)",
    required=False,
)

GRAVEL_TRAILING_INT = to_int_trailing_factory(prefix="GRVL_", required=True)

DRAINAGE_TRANSFORM = build_legacy_map_transform(
    "TPFL",
    "DRAINAGE (DRF_LOV_DRAINAGE_VWS)",
    required=False,
)

ROCK_TYPE_TRANSFORM = build_legacy_map_transform(
    "TPFL",
    "ROCK_TYPE (DRF_LOV_ROCK_TYPE_VWS)",
    required=False,
)

# List mappings for habitat closed-lists
SOIL_COLOR_TRANSFORM = build_legacy_map_transform(
    "TPFL", "SOIL_COLOR (DRF_LOV_SOIL_COLOR_VWS)", required=False
)

SOIL_CONDITION_TRANSFORM = build_legacy_map_transform(
    "TPFL", "SOIL_CONDITION (DRF_LOV_SOIL_COND_VWS)", required=False
)

LANDFORM_TRANSFORM = build_legacy_map_transform(
    "TPFL", "LANDFORM (DRF_LOV_LAND_FORM_VWS)", required=False
)

SOIL_TYPE_TRANSFORM = build_legacy_map_transform(
    "TPFL", "SOIL_TYPE (DRF_LOV_SOIL_TYPE_VWS)", required=False
)


def veg_condition_prefix_transform(value: str, ctx=None) -> str | None:
    """Prefix vegetation condition values for habitat notes."""
    if not value:
        return None
    v = value.strip()
    return f"Vegetation Condition: {v}" if v else None


PIPELINES = {
    "migrated_from_id": ["strip", "required"],
    "Occurrence__migrated_from_id": [POP_ID_FROM_SHEETNO],
    "species_id": ["strip", "blank_to_none", SPECIES_TRANSFORM],
    "community_id": ["strip", "blank_to_none", COMMUNITY_TRANSFORM],
    "lodgement_date": ["strip", "blank_to_none", "datetime_iso"],
    "observation_date": ["strip", "blank_to_none", "date_from_datetime_iso"],
    "record_source": ["strip", "blank_to_none", RECORD_SOURCE_FROM_CSV],
    "customer_status": [CUSTOMER_STATUS_FROM_FORM_STATUS_CODE],
    "comments": ["ocr_comments_transform"],
    "ocr_for_occ_name": [
        OCCURRENCE_FROM_POP_ID,
        pluck_attribute_factory("occurrence_name"),
    ],
    "processing_status": [
        "strip",
        "required",
        MAP_FORM_STATUS_CODE_TO_PROCESSING_STATUS,
    ],
    "submitter": ["strip", "blank_to_none", SUBMITTER_TRANSFORM],
    "OCRObserverDetail__role": ["strip", "blank_to_none", ROLE_TRANSFORM],
    "OCRObserverDetail__observer_name": ["strip", "blank_to_none"],
    "OCRHabitatComposition__loose_rock_percent": [
        "strip",
        "blank_to_none",
        GRAVEL_TRAILING_INT,
    ],
    "OCRHabitatComposition__drainage": ["strip", "blank_to_none", DRAINAGE_TRANSFORM],
    "OCRHabitatComposition__rock_type": ["strip", "blank_to_none", ROCK_TYPE_TRANSFORM],
    # Habitat composition extras (apply TPFL closed-list mappings)
    "OCRHabitatComposition__habitat_notes": ["strip", "blank_to_none"],
    "OCRHabitatComposition__vegetation_condition": [
        "strip",
        "blank_to_none",
        veg_condition_prefix_transform,
    ],
    "OCRHabitatComposition__soil_colour": [
        "strip",
        "blank_to_none",
        SOIL_COLOR_TRANSFORM,
    ],
    "OCRHabitatComposition__soil_condition": [
        "strip",
        "blank_to_none",
        SOIL_CONDITION_TRANSFORM,
    ],
    "OCRHabitatComposition__land_form": ["strip", "blank_to_none", LANDFORM_TRANSFORM],
    "OCRHabitatComposition__soil_type": ["strip", "blank_to_none", SOIL_TYPE_TRANSFORM],
}


class OccurrenceReportTpflAdapter(SourceAdapter):
    source_key = Source.TPFL.value
    domain = "occurrence_report"

    def extract(self, path: str, **options) -> ExtractionResult:
        rows = []
        warnings: list[ExtractionWarning] = []

        raw_rows, read_warnings = self.read_table(path)
        warnings.extend(read_warnings)

        for raw in raw_rows:
            canonical = schema.map_raw_row(raw)
            canonical["occurrence_report_name"] = (
                f"{canonical.get('POP_NUMBER', '').strip()} {canonical.get('SUBPOP_CODE', '').strip()}".strip()
            )
            canonical["group_type_id"] = get_group_type_id(GroupType.GROUP_TYPE_FLORA)
            assessor_data = None
            REASON_DEACTIVATED = canonical.get("REASON_DEACTIVATED", "").strip()
            if REASON_DEACTIVATED:
                DEACTIVATED_DATE = canonical.get("DEACTIVATED_DATE", "").strip()
                assessor_data = (
                    f"Reason Deactivated: {REASON_DEACTIVATED}, {DEACTIVATED_DATE}"
                )
            canonical["assessor_data"] = assessor_data
            ocr_for_occ_number = None
            SHEET_POP_NUMBER = canonical.get("SHEET_POP_NUMBER", "").strip()
            if SHEET_POP_NUMBER:
                SHEET_SUBPOP_CODE = canonical.get("SHEET_SUBPOP_CODE", "").strip()
                ocr_for_occ_number = f"{SHEET_POP_NUMBER}{SHEET_SUBPOP_CODE}"
            canonical["ocr_for_occ_name"] = ocr_for_occ_number
            canonical["OCRObserverDetail__main_observer"] = True
            canonical["internal_application"] = True
            # Build habitat_notes by combining habitat notes, aspect and vegetation condition
            hab_notes_parts: list[str] = []
            HAB_NOTES = canonical.get("HABITAT_NOTES", "")
            if HAB_NOTES:
                hab_notes_parts.append(HAB_NOTES.strip())
            ASPECT = canonical.get("ASPECT", "")
            if ASPECT:
                asp = ASPECT.strip()
                if asp:
                    hab_notes_parts.append(f"ASPECT: {asp}")
            # SV_VEGETATION_CONDITION should be prefixed
            SV_VEG = canonical.get("SV_VEGETATION_CONDITION", "")
            if SV_VEG:
                sv = SV_VEG.strip()
                if sv:
                    hab_notes_parts.append(f"Vegetation Condition: {sv}")

            # also consider SCON_COMMENTS (survey conditions) if present
            SCON = canonical.get("SCON_COMMENTS", "")
            if SCON:
                scon = SCON.strip()
                if scon:
                    hab_notes_parts.append(scon)

            if hab_notes_parts:
                canonical["OCRHabitatComposition__habitat_notes"] = "; ".join(
                    hab_notes_parts
                )

                # HABITAT_CONDITION => populate OCRHabitatCondition flags per task rules
                hc_raw = canonical.get("HABITAT_CONDITION", "")
                hc = hc_raw.strip().upper() if hc_raw else ""
                # default all zeros
                canonical["OCRHabitatCondition__pristine"] = (
                    100 if hc in ("PRISTINE",) else 0
                )
                canonical["OCRHabitatCondition__excellent"] = (
                    100
                    if hc
                    in (
                        "EXCELENT",
                        "EXCELLENT",
                    )
                    else 0
                )
                canonical["OCRHabitatCondition__very_good"] = (
                    100
                    if hc
                    in (
                        "VRY_GOOD",
                        "VRYGOOD",
                        "VERY_GOOD",
                        "VERYGOOD",
                    )
                    else 0
                )
                canonical["OCRHabitatCondition__good"] = 100 if hc in ("GOOD",) else 0
                canonical["OCRHabitatCondition__degraded"] = (
                    100 if hc in ("DEGRADED",) else 0
                )
                canonical["OCRHabitatCondition__completely_degraded"] = (
                    100
                    if hc
                    in (
                        "COM_DEGR",
                        "COMPLETELY_DEGRADED",
                        "COM_DEG",
                    )
                    else 0
                )

            # copy through simple habitat fields if present
            if canonical.get("SOIL_COLOR"):
                canonical["OCRHabitatComposition__soil_colour"] = canonical.get(
                    "SOIL_COLOR"
                )
            if canonical.get("SOIL_CONDITION"):
                canonical["OCRHabitatComposition__soil_condition"] = canonical.get(
                    "SOIL_CONDITION"
                )
            if canonical.get("LANDFORM"):
                canonical["OCRHabitatComposition__land_form"] = canonical.get(
                    "LANDFORM"
                )
            if canonical.get("SOIL_TYPE"):
                canonical["OCRHabitatComposition__soil_type"] = canonical.get(
                    "SOIL_TYPE"
                )

            # If an explicit occurrence id is present in the source we do not assign
            # it here; the importer will link habitat to the parent OccurrenceReport
            rows.append(canonical)
        return ExtractionResult(rows=rows, warnings=warnings)


# Attach pipelines to adapter
OccurrenceReportTpflAdapter.PIPELINES = PIPELINES
