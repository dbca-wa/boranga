import re
from datetime import datetime

from boranga.components.data_migration.adapters.occurrence.schema import SCHEMA
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

# TPFL-specific transform bindings
TAXONOMY_TRANSFORM = taxonomy_lookup_legacy_mapping("TPFL")
COORD_SOURCE_TRANSFORM = build_legacy_map_transform(
    legacy_system="TPFL",
    list_name="CO_ORD_SOURCE_CODE (DRF_LOV_CORDINATE_SOURCE_VWS)",
    required=False,
    return_type="id",
)

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

PROCESSING_STATUS = choices_transform([c[0] for c in Occurrence.PROCESSING_STATUS_CHOICES])

EMAILUSER_BY_LEGACY_USERNAME_TRANSFORM = emailuser_by_legacy_username_factory("TPFL")

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
    "submitter": ["strip", "blank_to_none", EMAILUSER_BY_LEGACY_USERNAME_TRANSFORM],
    "modified_by": ["strip", "blank_to_none", EMAILUSER_BY_LEGACY_USERNAME_TRANSFORM],
    "pop_number": ["strip", "blank_to_none"],
    "sub_pop_code": ["strip", "blank_to_none"],
    "OCCContactDetail__contact_name": ["strip", "blank_to_none"],
    "OCCContactDetail__notes": ["strip", "blank_to_none"],
    "OccurrenceTenure__purpose_id": ["strip", "blank_to_none", PURPOSE_TRANSFORM],
    "OccurrenceTenure__vesting_id": ["strip", "blank_to_none", VESTING_TRANSFORM],
    "OCCLocation__coordinate_source_id": [
        "strip",
        "blank_to_none",
        COORD_SOURCE_TRANSFORM,
        "to_int",
    ],
    "OCCLocation__location_description": ["strip", "blank_to_none"],
    "OCCLocation__boundary_description": [
        "strip",
        "blank_to_none",
    ],
    "OCCLocation__locality": ["strip", "blank_to_none"],
    "OCCObservationDetail__comments": [
        "strip",
        "blank_to_none",
    ],
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
            # Map raw row to canonical keys
            canonical_row = SCHEMA.map_raw_row(raw)

            # Prepend source prefix to migrated_from_id
            mid = canonical_row.get("migrated_from_id")
            if mid and not str(mid).startswith(f"{Source.TPFL.value.lower()}-"):
                canonical_row["migrated_from_id"] = f"{Source.TPFL.value.lower()}-{mid}"

            # Preserve internal keys (starting with _)
            for k, v in raw.items():
                if k.startswith("_"):
                    canonical_row[k] = v

            # Set TPFL-specific location fields (map from TPFL columns to OCCLocation canonical fields)
            canonical_row["OCCLocation__location_description"] = raw.get("LOCATION")
            # CO_ORD_SOURCE_CODE will be mapped via COORD_SOURCE_TRANSFORM in PIPELINES
            canonical_row["OCCLocation__coordinate_source_id"] = raw.get("CO_ORD_SOURCE_CODE")
            # TPFL doesn't have boundary_description in standard columns - use empty string for NOT NULL constraint
            canonical_row["OCCLocation__boundary_description"] = ""
            canonical_row["OCCLocation__locality"] = None

            # Compute occurrence_name from raw row (raw column names)
            pop = str(raw.get("POP_NUMBER", "") or "").strip()
            sub = str(raw.get("SUBPOP_CODE", "") or "").strip()
            occ_name = (pop + sub).strip()
            # If only a single digit (e.g. "1"), pad with leading zero -> "01"
            if occ_name and len(occ_name) == 1 and occ_name.isdigit():
                occ_name = occ_name.zfill(2)
            canonical_row["occurrence_name"] = occ_name if occ_name else None

            # Set TPFL-specific defaults on canonical row
            canonical_row["group_type_id"] = get_group_type_id(GroupType.GROUP_TYPE_FLORA)
            canonical_row["occurrence_source"] = Occurrence.OCCURRENCE_CHOICE_OCR
            canonical_row["locked"] = True
            canonical_row["lodgment_date"] = canonical_row.get("datetime_created")

            # Handle comment fields using raw row for column access
            POP_COMMENTS = raw.get("POP_COMMENTS", "")
            REASON_DEACTIVATED = raw.get("REASON_DEACTIVATED", "")
            DEACTIVATED_DATE = raw.get("DEACTIVATED_DATE", "")
            parts = []
            if POP_COMMENTS:
                parts.append(str(POP_COMMENTS).strip())
            if REASON_DEACTIVATED:
                parts.append(f"Reason Deactivated: {str(REASON_DEACTIVATED).strip()}")
            if DEACTIVATED_DATE:
                dd = _format_date_ddmmyyyy(DEACTIVATED_DATE)
                parts.append(f"Date Deactivated: {dd}")
            # Set using canonical field name 'comment' (POP_COMMENTS maps to 'comment' in COLUMN_MAP)
            canonical_row["comment"] = "; ".join(parts) if parts else canonical_row.get("comment")
            LAND_MGR_ADDRESS = raw.get("LAND_MGR_ADDRESS", "")
            LAND_MGR_PHONE = raw.get("LAND_MGR_PHONE", "")
            contact = LAND_MGR_ADDRESS
            if LAND_MGR_PHONE:
                if contact:
                    contact += "; "
                contact += LAND_MGR_PHONE
            # Set using canonical field name for contact (address + phone combined)
            canonical_row["OCCContactDetail__contact"] = contact if contact else None
            rows.append(canonical_row)
        return ExtractionResult(rows=rows, warnings=warnings)


# Attach pipelines to adapter class
OccurrenceTpflAdapter.PIPELINES = PIPELINES
