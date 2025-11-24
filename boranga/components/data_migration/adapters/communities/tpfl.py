from boranga.components.data_migration.registry import choices_transform
from boranga.components.occurrence.models import Occurrence

from ..base import ExtractionResult, ExtractionWarning, SourceAdapter
from ..sources import Source
from . import schema

# TPFL-specific transforms and pipelines
PROCESSING_STATUS = choices_transform(
    [c[0] for c in Occurrence.PROCESSING_STATUS_CHOICES]
)

PIPELINES = {
    "migrated_from_id": ["strip", "required"],
    "community_number": ["strip", "blank_to_none"],
    "group_type": ["strip", "blank_to_none", "group_type_by_name", "required"],
    # taxonomy/community name -> create/lookup CommunityTaxonomy
    # species list: split multiselect and resolve to species ids (implement split/validate in registry)
    "species": [
        "strip",
        "blank_to_none",
        "split_multiselect_species",
        "validate_species_list",
    ],
    "submitter": ["strip", "blank_to_none"],
    "processing_status": ["strip", "required", PROCESSING_STATUS],
    "lodgement_date": ["strip", "blank_to_none", "date_iso"],
    "last_data_curation_date": ["strip", "blank_to_none", "date_iso"],
    "conservation_plan_exists": ["strip", "blank_to_none", "bool"],
    "conservation_plan_reference": ["strip", "blank_to_none"],
    "comment": ["strip", "blank_to_none"],
    "department_file_numbers": ["strip", "blank_to_none"],
}


class CommunityTpflAdapter(SourceAdapter):
    source_key = Source.TPFL.value
    domain = "communities"

    def extract(self, path: str, **options) -> ExtractionResult:
        rows = []
        warnings: list[ExtractionWarning] = []

        raw_rows, read_warnings = self.read_table(path)
        warnings.extend(read_warnings)

        for raw in raw_rows:
            canonical = schema.map_raw_row(raw)
            rows.append(canonical)
        return ExtractionResult(rows=rows, warnings=warnings)


# Attach pipelines to adapter
CommunityTpflAdapter.PIPELINES = PIPELINES
