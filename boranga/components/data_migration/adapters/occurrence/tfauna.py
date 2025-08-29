from boranga.components.occurrence.models import Occurrence
from boranga.components.species_and_communities.models import GroupType

from ..base import ExtractionResult, ExtractionWarning, SourceAdapter
from ..sources import Source
from . import schema


class OccurrenceTfaunaAdapter(SourceAdapter):
    source_key = Source.TFAUNA.value
    domain = "occurrence"

    def extract(self, path: str, **options) -> ExtractionResult:
        rows = []
        warnings: list[ExtractionWarning] = []

        raw_rows, read_warnings = self.read_table(path)
        warnings.extend(read_warnings)

        for raw in raw_rows:
            canonical = schema.map_raw_row(raw)
            canonical["group_type"] = GroupType.GROUP_TYPE_FAUNA
            canonical["occurrence_source"] = Occurrence.OCCURRENCE_CHOICE_OCR
            canonical["processing_status"] = Occurrence.PROCESSING_STATUS_ACTIVE
            # TODO: Add any other source dependent constants here
            rows.append(canonical)
        return ExtractionResult(rows=rows, warnings=warnings)
