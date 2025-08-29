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
            # TODO: Add any other source dependent constants here
            rows.append(canonical)
        return ExtractionResult(rows=rows, warnings=warnings)
