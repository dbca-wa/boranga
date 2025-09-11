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
            # RP_EXP_DATE is already formatted by the pipeline to "dd/mm/YYYY"
            rp_comments = (canonical.get("RP_COMMENTS") or "").strip()
            rp_exp_date = canonical.get("RP_EXP_DATE")  # already formatted or None
            parts = []
            if rp_comments:
                parts.append(rp_comments)
            if rp_exp_date:
                parts.append(f"RP Expiry Date: {rp_exp_date}")
            canonical["conservation_plan_reference"] = (
                " ".join(parts) if parts else None
            )
            canonical["group_type_id"] = GroupType.objects.get(name="flora").id
            rows.append(canonical)
        return ExtractionResult(rows=rows, warnings=warnings)
