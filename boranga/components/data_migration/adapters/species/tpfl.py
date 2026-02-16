from boranga.components.data_migration.mappings import get_group_type_id
from boranga.components.data_migration.registry import (
    choices_transform,
    date_from_datetime_iso_local_factory,
    datetime_iso_factory,
    emailuser_by_legacy_username_factory,
    registry,
    static_value_factory,
    taxonomy_lookup_legacy_id_mapping,
)
from boranga.components.species_and_communities.models import GroupType, Species

from ..base import ExtractionResult, ExtractionWarning, SourceAdapter
from ..sources import Source
from . import schema

# TPFL-specific transform bindings and pipeline mapping. Keep the same
# syntax as previously used in `schema.py` but expose it on the adapter
# so pipelines remain source-local.


TAXONOMY_TRANSFORM = taxonomy_lookup_legacy_id_mapping("TPFL")

EMAILUSER_BY_LEGACY_USERNAME_TRANSFORM = emailuser_by_legacy_username_factory("TPFL")

DATE_FROM_DATETIME_ISO_PERTH = date_from_datetime_iso_local_factory("Australia/Perth")

DATETIME_ISO_PERTH = datetime_iso_factory("Australia/Perth")

PROCESSING_STATUS = choices_transform([c[0] for c in Species.PROCESSING_STATUS_CHOICES])

NOO_AUTO_TRUE = static_value_factory(True)

PIPELINES = {
    "migrated_from_id": ["strip", "required"],
    "taxonomy_id": ["strip", "blank_to_none", "required", TAXONOMY_TRANSFORM],
    "comment": ["strip", "blank_to_none"],
    # normalize & format RP_EXP_DATE to "dd/mm/YYYY"
    "RP_EXP_DATE": [
        "strip",
        "blank_to_none",
        DATE_FROM_DATETIME_ISO_PERTH,
        "format_date_dmy",
    ],
    "RP_COMMENTS": ["strip", "blank_to_none"],
    "conservation_plan_exists": ["strip", "blank_to_none", "is_present"],
    "department_file_numbers": ["strip", "blank_to_none"],
    "last_data_curation_date": ["strip", "blank_to_none", DATE_FROM_DATETIME_ISO_PERTH],
    "lodgement_date": ["strip", "blank_to_none", DATETIME_ISO_PERTH],
    "processing_status": [
        "strip",
        "blank_to_none",
        "required",
        "Y_to_active_else_historical",
        PROCESSING_STATUS,
    ],
    "submitter": ["strip", "blank_to_none", EMAILUSER_BY_LEGACY_USERNAME_TRANSFORM],
    "distribution": ["strip", "blank_to_none"],
    "modified_by": ["strip", "blank_to_none", EMAILUSER_BY_LEGACY_USERNAME_TRANSFORM],
    "datetime_updated": ["strip", "blank_to_none", DATETIME_ISO_PERTH],
    "noo_auto": [NOO_AUTO_TRUE],
}


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
            # Extract raw values from the raw row (not canonical, which hasn't had pipelines applied yet)
            # The pipelines will be applied by the importer after extraction.
            rp_comments = (raw.get("RP_COMMENTS") or "").strip()
            rp_exp_date_raw = (raw.get("RP_EXP_DATE") or "").strip()

            parts = []
            if rp_comments:
                parts.append(rp_comments)
            if rp_exp_date_raw:
                # Parse and format the date using the same transforms as the pipeline
                date_from_iso = registry._fns.get(DATE_FROM_DATETIME_ISO_PERTH)
                format_dmy = registry._fns.get("format_date_dmy")
                if date_from_iso and format_dmy:
                    # Apply date_from_datetime_iso to parse the raw date string
                    parsed_date_result = date_from_iso(rp_exp_date_raw, None)
                    if parsed_date_result and parsed_date_result.value:
                        # Apply format_date_dmy to format as dd/mm/YYYY
                        formatted_result = format_dmy(parsed_date_result.value, None)
                        if formatted_result and formatted_result.value:
                            parts.append(f"RP Expiry Date: {formatted_result.value}")
            canonical["conservation_plan_reference"] = " ".join(parts) if parts else None
            canonical["group_type_id"] = get_group_type_id(GroupType.GROUP_TYPE_FLORA)
            rows.append(canonical)
        return ExtractionResult(rows=rows, warnings=warnings)


# Attach to adapter class so handlers/registry can detect adapter-specific pipelines
SpeciesTpflAdapter.PIPELINES = PIPELINES
