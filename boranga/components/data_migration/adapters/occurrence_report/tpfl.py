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
    "FORM_STATUS_CODE",
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
            rows.append(canonical)
        return ExtractionResult(rows=rows, warnings=warnings)


# Attach pipelines to adapter
OccurrenceReportTpflAdapter.PIPELINES = PIPELINES
