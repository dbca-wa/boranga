from boranga.components.data_migration.mappings import get_group_type_id
from boranga.components.occurrence.models import OccurrenceReport
from boranga.components.species_and_communities.models import GroupType

from ..base import ExtractionResult, ExtractionWarning, SourceAdapter
from ..sources import Source
from . import schema


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
                f"{canonical.get('POP_NUMBER','').strip()} {canonical.get('SUBPOP_CODE','').strip()}".strip()
            )
            canonical["group_type_id"] = get_group_type_id(GroupType.GROUP_TYPE_FLORA)
            canonical["occurrence_source"] = OccurrenceReport.OCCURRENCE_CHOICE_OCR
            canonical["processing_status"] = OccurrenceReport.PROCESSING_STATUS_ACTIVE
            canonical["locked"] = True
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
            rows.append(canonical)
        return ExtractionResult(rows=rows, warnings=warnings)
