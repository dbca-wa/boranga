from boranga.components.data_migration.mappings import get_group_type_id
from boranga.components.species_and_communities.models import GroupType

from ..base import ExtractionResult, ExtractionWarning, SourceAdapter
from ..sources import Source
from . import schema


class ConservationStatusTpflAdapter(SourceAdapter):
    source_key = Source.TPFL.value
    domain = "conservation_status"

    def extract(self, path: str, **options) -> ExtractionResult:
        rows = []
        warnings: list[ExtractionWarning] = []

        raw_rows, read_warnings = self.read_table(path)
        warnings.extend(read_warnings)

        for raw in raw_rows:
            canonical = schema.map_raw_row(raw)
            canonical["group_type_id"] = get_group_type_id(GroupType.GROUP_TYPE_FLORA)
            rows.append(canonical)
        return ExtractionResult(rows=rows, warnings=warnings)
