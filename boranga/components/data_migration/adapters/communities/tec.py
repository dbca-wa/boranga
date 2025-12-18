from ..base import ExtractionResult, ExtractionWarning, SourceAdapter
from ..sources import Source
from . import schema

# TEC-specific transforms and pipelines
PIPELINES = {
    "migrated_from_id": ["strip", "required"],
    "former_range": ["strip", "blank_to_none"],
    "range_decline": ["strip", "blank_to_none"],
    "occ_decline": ["strip", "blank_to_none"],
    "community_common_id": ["strip", "blank_to_none"],
    "community_description": ["strip", "blank_to_none"],
    "community_name": ["strip", "blank_to_none"],
    "community_original_area": ["strip", "blank_to_none", "to_decimal"],
    "community_original_area_accuracy": ["strip", "blank_to_none", "to_decimal"],
    "distribution": ["strip", "blank_to_none"],
    "regions": ["strip", "blank_to_none"],
    "districts": ["strip", "blank_to_none"],
    "active_cs": ["strip", "y_to_true_else_false"],
    "pub_title": ["strip", "blank_to_none"],
    "pub_author": ["strip", "blank_to_none"],
    "pub_date": ["strip", "blank_to_none"],
    "pub_place": ["strip", "blank_to_none"],
    "threat_category": ["strip", "blank_to_none"],
    "threat_comment": ["strip", "blank_to_none"],
    "date_observed": ["strip", "blank_to_none", "date_from_datetime_iso"],
}


class CommunityTecAdapter(SourceAdapter):
    source_key = Source.TEC.value
    domain = "communities"

    def extract(self, path: str, **options) -> ExtractionResult:
        rows = []
        warnings: list[ExtractionWarning] = []

        raw_rows, read_warnings = self.read_table(path)
        warnings.extend(read_warnings)

        for raw in raw_rows:
            canonical = schema.map_raw_row(raw)

            # Construct comment from 3 legacy fields
            comment_parts = []
            former_range = (canonical.get("former_range") or "").strip()
            range_decline = (canonical.get("range_decline") or "").strip()
            occ_decline = (canonical.get("occ_decline") or "").strip()

            if former_range:
                comment_parts.append(f"Former Range: {former_range}")
            if range_decline:
                comment_parts.append(f"Range Decline: {range_decline}")
            if occ_decline:
                comment_parts.append(f"Occurrence Decline: {occ_decline}")

            if comment_parts:
                canonical["comment"] = "; ".join(comment_parts)

            rows.append(canonical)
        return ExtractionResult(rows=rows, warnings=warnings)


# Attach pipelines to adapter
CommunityTecAdapter.PIPELINES = PIPELINES
