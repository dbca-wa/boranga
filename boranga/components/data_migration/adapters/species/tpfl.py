from boranga.components.species_and_communities.models import GroupType

from ..base import ExtractionResult, ExtractionWarning, SourceAdapter
from ..sources import Source
from . import schema


class SpeciesTpflAdapter(SourceAdapter):
    source_key = Source.TPFL.value
    domain = "species"

    def extract(self, path: str, **options) -> ExtractionResult:
        rows = []
        warnings: list[ExtractionWarning] = []

        raw_rows, read_warnings = self.read_table(path)
        warnings.extend(read_warnings)

        for raw in raw_rows:
            canonical = schema.map_raw_row(raw)
            RP_COMMENTS = canonical.get("RP_COMMENTS", "").strip()
            RP_EXP_DATE = canonical.get("RP_EXP_DATE", "").strip()
            canonical["conservation_plan_reference"] = f"{RP_COMMENTS}{RP_EXP_DATE}"
            canonical["group_type_id"] = GroupType.objects.get(name="flora").id
            rows.append(canonical)
        return ExtractionResult(rows=rows, warnings=warnings)
