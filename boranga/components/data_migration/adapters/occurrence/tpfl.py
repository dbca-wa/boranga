from boranga.components.data_migration.mappings import get_group_type_id
from boranga.components.data_migration.registry import (
    build_legacy_map_transform,
    choices_transform,
    emailuser_by_legacy_username_factory,
    fk_lookup,
    taxonomy_lookup,
)
from boranga.components.occurrence.models import Occurrence, WildStatus
from boranga.components.species_and_communities.models import (
    Community,
    GroupType,
    Species,
)

from ..base import ExtractionResult, ExtractionWarning, SourceAdapter
from ..sources import Source
from . import schema

# TPFL-specific transform bindings
TAXONOMY_TRANSFORM = taxonomy_lookup(
    lookup_field="scientific_name",
)

SPECIES_TRANSFORM = fk_lookup(model=Species, lookup_field="taxonomy_id")

COMMUNITY_TRANSFORM = fk_lookup(
    model=Community,
    lookup_field="taxonomy__community_name",
)

WILD_STATUS_TRANSFORM = fk_lookup(model=WildStatus, lookup_field="name")

PROCESSING_STATUS = choices_transform(
    [c[0] for c in Occurrence.PROCESSING_STATUS_CHOICES]
)

SUBMITTER_TRANSFORM = emailuser_by_legacy_username_factory("TPFL")

PURPOSE_TRANSFORM = build_legacy_map_transform(
    "TPFL",
    "PURPOSE (DRF_LOV_PURPOSE_VWS)",
    required=False,
)

VESTING_TRANSFORM = build_legacy_map_transform(
    "TPFL",
    "VESTING (DRF_LOV_PURPOSE_VWS)",
    required=False,
)

PIPELINES = {
    "migrated_from_id": ["strip", "required"],
    "species_id": [
        "strip",
        "blank_to_none",
        TAXONOMY_TRANSFORM,
        SPECIES_TRANSFORM,
    ],
    "community_id": ["strip", "blank_to_none", COMMUNITY_TRANSFORM],
    "wild_status_id": ["strip", "blank_to_none", WILD_STATUS_TRANSFORM],
    "comment": ["strip", "blank_to_none"],
    "datetime_created": ["strip", "blank_to_none", "datetime_iso"],
    "datetime_updated": ["strip", "blank_to_none", "datetime_iso"],
    "processing_status": [
        "strip",
        "required",
        "Y_to_active_else_historical",
        PROCESSING_STATUS,
    ],
    "submitter": ["strip", "blank_to_none", SUBMITTER_TRANSFORM],
    "OCCContactDetail__contact_name": ["strip", "blank_to_none"],
    "OCCContactDetail__notes": ["strip", "blank_to_none"],
    "OccurrenceTenure__purpose_id": ["strip", "blank_to_none", PURPOSE_TRANSFORM],
    "OccurrenceTenure__vesting_id": ["strip", "blank_to_none", VESTING_TRANSFORM],
}


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
                f"{canonical.get('POP_NUMBER', '').strip()} {canonical.get('SUBPOP_CODE', '').strip()}".strip()
            )
            canonical["group_type_id"] = get_group_type_id(GroupType.GROUP_TYPE_FLORA)
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


# Attach pipelines to adapter class
OccurrenceTpflAdapter.PIPELINES = PIPELINES
