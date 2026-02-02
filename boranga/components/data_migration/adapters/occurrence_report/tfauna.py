from ..base import ExtractionResult, ExtractionWarning, SourceAdapter
from ..sources import Source
from . import schema


class OccurrenceReportTfaunaAdapter(SourceAdapter):
    source_key = Source.TFAUNA.value
    domain = "occurrence_report"

    def extract(self, path: str, **options) -> ExtractionResult:
        rows = []
        warnings: list[ExtractionWarning] = []

        raw_rows, read_warnings = self.read_table(path)
        warnings.extend(read_warnings)

        for raw in raw_rows:
            canonical = schema.map_raw_row(raw)
            canonical["group_type"] = "fauna"  # TODO: Add any other source dependent constants here
            rows.append(canonical)
        return ExtractionResult(rows=rows, warnings=warnings)
