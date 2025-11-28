import re
from datetime import datetime

from boranga.components.data_migration.mappings import get_group_type_id
from boranga.components.data_migration.registry import (
    build_legacy_map_transform,
    choices_transform,
    emailuser_by_legacy_username_factory,
    fk_lookup,
    taxonomy_lookup_legacy_mapping,
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
TAXONOMY_TRANSFORM = taxonomy_lookup_legacy_mapping("TPFL")

SPECIES_TRANSFORM = fk_lookup(model=Species, lookup_field="taxonomy_id")

COMMUNITY_TRANSFORM = fk_lookup(
    model=Community,
    lookup_field="taxonomy__community_name",
)

WILD_STATUS_TRANSFORM = fk_lookup(model=WildStatus, lookup_field="id")

# Legacy mapping for STATUS closed list -> WildStatus name
LEGACY_WILD_STATUS_TRANSFORM = build_legacy_map_transform(
    "TPFL",
    "STATUS (DRF_LOV_POP_STATUS_VWS)",
    required=False,
)

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
    "VESTING (DRF_LOV_VESTING_VWS)",
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
    "wild_status_id": [
        "strip",
        "blank_to_none",
        LEGACY_WILD_STATUS_TRANSFORM,
        "to_int",
        WILD_STATUS_TRANSFORM,
    ],
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

        def _format_date_ddmmyyyy(val: str) -> str:
            if not val:
                return ""
            v = str(val).strip()
            # already in d/m/yyyy or dd/mm/yyyy -> normalise to dd/mm/YYYY
            m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})$", v)
            if m:
                day, mon, year = m.groups()
                return f"{int(day):02d}/{int(mon):02d}/{year}"
            # handle ISO-ish formats, adjust timezone format if needed
            try:
                vv = v
                if vv.endswith("Z"):
                    vv = vv.replace("Z", "+00:00")
                # convert +HHMM to +HH:MM for fromisoformat
                tz_match = re.search(r"([+-]\d{4})$", vv)
                if tz_match:
                    tz = tz_match.group(1)
                    vv = vv[:-5] + tz[:3] + ":" + tz[3:]
                dt = datetime.fromisoformat(vv)
                return dt.strftime("%d/%m/%Y")
            except Exception:
                pass
            # try common fallbacks
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d-%m-%Y"):
                try:
                    dt = datetime.strptime(v, fmt)
                    return dt.strftime("%d/%m/%Y")
                except Exception:
                    continue
            # fallback: return raw value
            return v

        for raw in raw_rows:
            canonical = schema.map_raw_row(raw)
            # Build occurrence_name: concat POP_NUMBER + SUBPOP_CODE (no space)
            pop = str(canonical.get("POP_NUMBER", "") or "").strip()
            sub = str(canonical.get("SUBPOP_CODE", "") or "").strip()
            occ_name = (pop + sub).strip()
            # If only a single digit (e.g. "1"), pad with leading zero -> "01"
            if occ_name and len(occ_name) == 1 and occ_name.isdigit():
                occ_name = occ_name.zfill(2)
            canonical["occurrence_name"] = occ_name if occ_name else None
            canonical["group_type_id"] = get_group_type_id(GroupType.GROUP_TYPE_FLORA)
            canonical["occurrence_source"] = Occurrence.OCCURRENCE_CHOICE_OCR
            canonical["processing_status"] = Occurrence.PROCESSING_STATUS_ACTIVE
            canonical["locked"] = True
            canonical["lodgment_date"] = canonical.get("datetime_created")
            POP_COMMENTS = canonical.get("POP_COMMENTS", "")
            REASON_DEACTIVATED = canonical.get("REASON_DEACTIVATED", "")
            DEACTIVATED_DATE = canonical.get("DEACTIVATED_DATE", "")
            parts = []
            if POP_COMMENTS:
                parts.append(str(POP_COMMENTS).strip())
            if REASON_DEACTIVATED:
                parts.append(f"Reason Deactivated: {str(REASON_DEACTIVATED).strip()}")
            if DEACTIVATED_DATE:
                dd = _format_date_ddmmyyyy(DEACTIVATED_DATE)
                parts.append(f"Date Deactivated: {dd}")
            canonical["comment"] = "; ".join(parts) if parts else None
            LAND_MGR_ADDRESS = canonical.get("LAND_MGR_ADDRESS", "")
            LAND_MGR_PHONE = canonical.get("LAND_MGR_PHONE", "")
            contact = LAND_MGR_ADDRESS
            if LAND_MGR_PHONE:
                if contact:
                    contact += "; "
                contact += LAND_MGR_PHONE
            canonical["OCCContactDetail__contact"] = contact if contact else None
            rows.append(canonical)
        return ExtractionResult(rows=rows, warnings=warnings)


# Attach pipelines to adapter class
OccurrenceTpflAdapter.PIPELINES = PIPELINES
