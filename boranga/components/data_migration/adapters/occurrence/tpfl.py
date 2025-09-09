from boranga.components.occurrence.models import Occurrence

from ..base import ExtractionResult, ExtractionWarning, SourceAdapter
from ..sources import Source
from . import schema


class OccurrenceTpflAdapter(SourceAdapter):
    source_key = Source.TPFL.value
    domain = "occurrence"

    def extract(self, path: str, **options) -> ExtractionResult:
        rows = []
        warnings: list[ExtractionWarning] = []

        raw_rows, read_warnings = self.read_table(path)
        warnings.extend(read_warnings)

        for raw in raw_rows:
            canonical = schema.map_raw_row(raw)
            canonical["occurrence_name"] = (
                f"{canonical.get('POP_NUMBER','').strip()} {canonical.get('SUBPOP_CODE','').strip()}".strip()
            )
            canonical["group_type"] = "flora"
            canonical["occurrence_source"] = Occurrence.OCCURRENCE_CHOICE_OCR
            canonical["processing_status"] = Occurrence.PROCESSING_STATUS_ACTIVE
            canonical["locked"] = True
            POP_COMMENTS = canonical.get("POP_COMMENTS", "")
            REASON_DEACTIVATED = canonical.get("REASON_DEACTIVATED", "")
            DEACTIVATED_DATE = canonical.get("DEACTIVATED_DATE", "")
            comment = POP_COMMENTS
            if REASON_DEACTIVATED:
                if comment:
                    comment += "\n\n"
                comment += f"Reason Deactivated: {REASON_DEACTIVATED}"
            if DEACTIVATED_DATE:
                if comment:
                    comment += "\n\n"
                comment += f"Date Deactivated: {DEACTIVATED_DATE}"
            canonical["comment"] = comment if comment else None
            LAND_MGR_ADDRESS = canonical.get("LAND_MGR_ADDRESS", "")
            LAND_MGR_PHONE = canonical.get("LAND_MGR_PHONE", "")
            contact = LAND_MGR_ADDRESS
            if LAND_MGR_PHONE:
                if contact:
                    contact += ", "
                contact += LAND_MGR_PHONE
            canonical["OCCContactDetail__contact"] = contact if contact else None
            rows.append(canonical)
        return ExtractionResult(rows=rows, warnings=warnings)
