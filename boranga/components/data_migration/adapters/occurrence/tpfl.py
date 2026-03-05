import re
from datetime import datetime

from boranga.components.data_migration.adapters.occurrence.schema import SCHEMA
from boranga.components.data_migration.mappings import get_group_type_id
from boranga.components.data_migration.registry import (
    TransformIssue,
    _result,
    build_legacy_map_transform,
    choices_transform,
    date_from_datetime_iso_local_factory,
    datetime_iso_factory,
    emailuser_by_legacy_username_factory,
    fk_lookup,
    registry,
    static_value_factory,
    taxonomy_lookup_legacy_mapping,
    to_decimal_factory,
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
DATETIME_ISO_PERTH = datetime_iso_factory("Australia/Perth")
DATE_FROM_DATETIME_ISO_PERTH = date_from_datetime_iso_local_factory("Australia/Perth")

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

# --- OCCHabitatComposition transforms ---
DRAINAGE_TRANSFORM = build_legacy_map_transform(
    "TPFL",
    "DRAINAGE (DRF_LOV_DRAINAGE_VWS)",
    required=False,
    return_type="id",
)
LAND_FORM_TRANSFORM = build_legacy_map_transform(
    "TPFL",
    "LANDFORM (DRF_LOV_LAND_FORM_VWS)",
    required=False,
    return_type="id",
)
ROCK_TYPE_TRANSFORM = build_legacy_map_transform(
    "TPFL",
    "ROCK_TYPE (DRF_LOV_ROCK_TYPE_VWS)",
    required=False,
    return_type="id",
)
SOIL_COLOUR_TRANSFORM = build_legacy_map_transform(
    "TPFL",
    "SOIL_COLOR (DRF_LOV_SOIL_COLOR_VWS)",
    required=False,
    return_type="id",
)
SOIL_CONDITION_TRANSFORM = build_legacy_map_transform(
    "TPFL",
    "SOIL_CONDITION (DRF_LOV_SOIL_COND_VWS)",
    required=False,
    return_type="id",
)
SOIL_TYPE_TRANSFORM = build_legacy_map_transform(
    "TPFL",
    "SOIL_TYPE (DRF_LOV_SOIL_TYPE_VWS)",
    required=False,
    return_type="id",
)

# --- OCCFireHistory transforms ---
FIRE_SEASON_TRANSFORM = build_legacy_map_transform(
    "TPFL",
    "FIRE_SEASON (DRF_LOV_SEASONS_VWS)",
    required=False,
    return_type="canonical",
)
FIRE_INTENSITY_TRANSFORM = build_legacy_map_transform(
    "TPFL",
    "FIRE_INTENSITY (DRF_LOV_LW_MD_HI_VWS)",
    required=False,
    return_type="id",
)

# --- OCCIdentification transforms ---
SAMPLE_DESTINATION_TRANSFORM = build_legacy_map_transform(
    "TPFL",
    "VOUCHER_LOCATION (DRF_LOV_VOUCHER_LOC_VWS)",
    required=False,
    return_type="id",
)
VCHR_STATUS_CODE_TRANSFORM = build_legacy_map_transform(
    "TPFL",
    "VCHR_STATUS_CODE (DRF_LOV_VOUCHER_STAT_VWS)",
    required=False,
    return_type="canonical",
)
DUPVOUCH_LOCATION_TRANSFORM = build_legacy_map_transform(
    "TPFL",
    "DUPVOUCH_LOCATION (DRF_LOV_VOUCHER_LOC_VWS)",
    required=False,
    return_type="canonical",
)

# --- OCCLocation transforms ---
DISTRICT_TRANSFORM = build_legacy_map_transform(
    "TPFL",
    "DISTRICT (DRF_LOV_DEC_DISTRICT_VWS)",
    required=False,
    return_type="id",
)
REGION_TRANSFORM = build_legacy_map_transform(
    "TPFL",
    "DISTRICT (DRF_LOV_DEC_DISTRICT_VWS)",
    required=False,
    return_type="id",
)
LOCATION_ACCURACY_TRANSFORM = build_legacy_map_transform(
    "TPFL",
    "RESOLUTION (DRF_LOV_RESOLUTION_VWS)",
    required=False,
    return_type="id",
)
LOCALITY_TRANSFORM = build_legacy_map_transform(
    "TPFL",
    "LANDDISTRICT (DRF_LOV_LAND_DISTRICT_VWS)",
    required=False,
    return_type="canonical",
)
LGA_CODE_TRANSFORM = build_legacy_map_transform(
    "TPFL",
    "LGA_CODE (DRF_LOV_LGA_VWS)",
    required=False,
    return_type="canonical",
)

# --- OCCObservationDetail transforms ---
AREA_ASSESSMENT_TRANSFORM = build_legacy_map_transform(
    "TPFL",
    "SVY_EXTENT (DRF_LOV_SVY_EXTENT_VWS)",
    required=False,
    return_type="id",
)

# --- OCCPlantCount transforms ---
COUNTED_SUBJECT_TRANSFORM = build_legacy_map_transform(
    "TPFL",
    "CNT_PLANT_TYPE_CODE (DRF_LOV_CNT_PLANT_TYPE_VWS)",
    required=False,
    return_type="id",
)
PLANT_CONDITION_TRANSFORM = build_legacy_map_transform(
    "TPFL",
    "POPULATION_CONDITION (DRF_LOV_PLNT_COND_VWS)",
    required=False,
    return_type="id",
)
PLANT_COUNT_METHOD_TRANSFORM = build_legacy_map_transform(
    "TPFL",
    "COUNT_MTHD_CODE (DRF_LOV_COUNT_METHOD_VWS)",
    required=False,
    return_type="id",
)
AREA_OCCUPIED_METHOD_TRANSFORM = build_legacy_map_transform(
    "TPFL",
    "AREA_OCCUPIED_METHOD (DRF_LOV_AREA_CALC_VWS)",
    required=False,
    return_type="canonical",
)

# Decimal factories for specific field constraints
DECIMAL_12_2 = to_decimal_factory(max_digits=12, decimal_places=2)
DECIMAL_14_4 = to_decimal_factory(max_digits=14, decimal_places=4)
DECIMAL_5_2 = to_decimal_factory(max_digits=5, decimal_places=2)

# Static value for OCCIdentification.identification_certainty default
STATIC_HIGH_CERTAINTY = static_value_factory("High Certainty")

# Static value for OCCLocation.boundary_description default
STATIC_BOUNDARY_DESC = static_value_factory(
    "Boundary not mapped, migrated point coordinate has had a 1 metre buffer applied"
)


# --- Custom inline transforms ---


# GRAVEL code → integer
@registry.register("gravel_to_int")
def t_gravel_to_int(value, ctx):
    """Convert GRAVEL codes (GRVL_10, GRVL_30, etc.) to integer values."""
    if value in (None, ""):
        return _result(None)
    v = str(value).strip().upper()
    mapping = {"GRVL_10": 10, "GRVL_30": 30, "GRVL_50": 50, "GRVL_100": 100}
    if v in mapping:
        return _result(mapping[v])
    return _result(None, TransformIssue("warning", f"Unknown GRAVEL code: {value}"))


# HABITAT_CONDITION → individual percentage fields
@registry.register("habitat_condition_to_fields")
def t_habitat_condition_to_fields(value, ctx):
    """Convert HABITAT_CONDITION code to a dict with 100/0 values for each condition field."""
    if value in (None, ""):
        return _result(
            {
                "pristine": 0,
                "excellent": 0,
                "very_good": 0,
                "good": 0,
                "degraded": 0,
                "completely_degraded": 0,
            }
        )
    v = str(value).strip().upper()
    fields = {
        "pristine": 100 if v == "PRISTINE" else 0,
        "excellent": 100 if v == "EXCELENT" else 0,  # Note: legacy spelling
        "very_good": 100 if v == "VRY_GOOD" else 0,
        "good": 100 if v == "GOOD" else 0,
        "degraded": 100 if v == "DEGRADED" else 0,
        "completely_degraded": 100 if v == "COM_DEGR" else 0,
    }
    return _result(fields)


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
    "datetime_created": ["strip", "blank_to_none", DATETIME_ISO_PERTH],
    "datetime_updated": ["strip", "blank_to_none", DATETIME_ISO_PERTH],
    "lodgment_date": ["strip", "blank_to_none", DATE_FROM_DATETIME_ISO_PERTH],
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
    # --- OCCLocation ---
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
    "OCCLocation__locality": ["strip", "blank_to_none", LOCALITY_TRANSFORM],
    "OCCLocation__district_id": ["strip", "blank_to_none", DISTRICT_TRANSFORM, "to_int"],
    "OCCLocation__region_id": ["strip", "blank_to_none", REGION_TRANSFORM, "to_int"],
    "OCCLocation__location_accuracy_id": ["strip", "blank_to_none", LOCATION_ACCURACY_TRANSFORM, "to_int"],
    "OCCLocation__lga_code": ["strip", "blank_to_none", LGA_CODE_TRANSFORM],
    # --- OCCObservationDetail ---
    "OCCObservationDetail__comments": [
        "strip",
        "blank_to_none",
    ],
    "OCCObservationDetail__area_assessment_id": ["strip", "blank_to_none", AREA_ASSESSMENT_TRANSFORM, "to_int"],
    "OCCObservationDetail__area_surveyed": ["strip", "blank_to_none", DECIMAL_14_4],
    "OCCObservationDetail__survey_duration": ["strip", "blank_to_none", "to_int"],
    # --- OCCHabitatComposition ---
    "OCCHabitatComposition__drainage_id": ["strip", "blank_to_none", DRAINAGE_TRANSFORM, "to_int"],
    "OCCHabitatComposition__land_form": ["strip", "blank_to_none", LAND_FORM_TRANSFORM],
    "OCCHabitatComposition__loose_rock_percent": ["strip", "blank_to_none", "gravel_to_int"],
    "OCCHabitatComposition__rock_type_id": ["strip", "blank_to_none", ROCK_TYPE_TRANSFORM, "to_int"],
    "OCCHabitatComposition__soil_colour_id": ["strip", "blank_to_none", SOIL_COLOUR_TRANSFORM, "to_int"],
    "OCCHabitatComposition__soil_condition_id": ["strip", "blank_to_none", SOIL_CONDITION_TRANSFORM, "to_int"],
    "OCCHabitatComposition__soil_type": ["strip", "blank_to_none", SOIL_TYPE_TRANSFORM],
    # --- OCCFireHistory ---
    "OCCFireHistory__fire_season": ["strip", "blank_to_none", FIRE_SEASON_TRANSFORM],
    "OCCFireHistory__fire_year": ["strip", "blank_to_none"],
    "OCCFireHistory__intensity_id": ["strip", "blank_to_none", FIRE_INTENSITY_TRANSFORM, "to_int"],
    # --- OCCIdentification ---
    "OCCIdentification__barcode_number": ["strip", "blank_to_none"],
    "OCCIdentification__collector_number": ["strip", "blank_to_none"],
    "OCCIdentification__permit_id": ["strip", "blank_to_none"],
    "OCCIdentification__sample_destination_id": ["strip", "blank_to_none", SAMPLE_DESTINATION_TRANSFORM, "to_int"],
    "OCCIdentification__vchr_status_code": ["strip", "blank_to_none", VCHR_STATUS_CODE_TRANSFORM],
    "OCCIdentification__dupvouch_location": ["strip", "blank_to_none", DUPVOUCH_LOCATION_TRANSFORM],
    # --- OCCAssociatedSpecies ---
    "OCCAssociatedSpecies__comment": ["strip", "blank_to_none"],
    # --- OCCPlantCount ---
    "OCCPlantCount__counted_subject_id": ["strip", "blank_to_none", COUNTED_SUBJECT_TRANSFORM, "to_int"],
    "OCCPlantCount__plant_condition_id": ["strip", "blank_to_none", PLANT_CONDITION_TRANSFORM, "to_int"],
    "OCCPlantCount__plant_count_method_id": ["strip", "blank_to_none", PLANT_COUNT_METHOD_TRANSFORM, "to_int"],
    "OCCPlantCount__clonal_reproduction_present": ["strip", "blank_to_none", "y_to_true_n_to_none"],
    "OCCPlantCount__vegetative_state_present": ["strip", "blank_to_none", "y_to_true_n_to_none"],
    "OCCPlantCount__flower_bud_present": ["strip", "blank_to_none", "y_to_true_n_to_none"],
    "OCCPlantCount__flower_present": ["strip", "blank_to_none", "y_to_true_n_to_none"],
    "OCCPlantCount__immature_fruit_present": ["strip", "blank_to_none", "y_to_true_n_to_none"],
    "OCCPlantCount__ripe_fruit_present": ["strip", "blank_to_none", "y_to_true_n_to_none"],
    "OCCPlantCount__dehisced_fruit_present": ["strip", "blank_to_none", "y_to_true_n_to_none"],
    "OCCPlantCount__detailed_alive_mature": ["strip", "blank_to_none", "to_int"],
    "OCCPlantCount__detailed_dead_mature": ["strip", "blank_to_none", "to_int"],
    "OCCPlantCount__detailed_alive_juvenile": ["strip", "blank_to_none", "to_int"],
    "OCCPlantCount__detailed_dead_juvenile": ["strip", "blank_to_none", "to_int"],
    "OCCPlantCount__detailed_alive_seedling": ["strip", "blank_to_none", "to_int"],
    "OCCPlantCount__detailed_dead_seedling": ["strip", "blank_to_none", "to_int"],
    "OCCPlantCount__simple_alive": ["strip", "blank_to_none", "to_int"],
    "OCCPlantCount__simple_dead": ["strip", "blank_to_none", "to_int"],
    "OCCPlantCount__quadrats_surveyed": ["strip", "blank_to_none", "to_int"],
    "OCCPlantCount__estimated_population_area": ["strip", "blank_to_none", DECIMAL_12_2],
    "OCCPlantCount__flowering_plants_per": ["strip", "blank_to_none", DECIMAL_5_2],
    "OCCPlantCount__total_quadrat_area": ["strip", "blank_to_none", DECIMAL_12_2],
    "OCCPlantCount__pollinator_observation": ["strip", "blank_to_none"],
    "OCCPlantCount__area_occupied_method": ["strip", "blank_to_none", AREA_OCCUPIED_METHOD_TRANSFORM],
    "OCCPlantCount__quad_size": ["strip", "blank_to_none"],
    "OCCPlantCount__quad_num_total": ["strip", "blank_to_none"],
    "OCCPlantCount__quad_num_mature": ["strip", "blank_to_none"],
    "OCCPlantCount__quad_num_juvenile": ["strip", "blank_to_none"],
    "OCCPlantCount__quad_num_seedlings": ["strip", "blank_to_none"],
    "OCCPlantCount__population_notes": ["strip", "blank_to_none"],
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

            # Set TPFL-specific location fields (static default, not from raw column)
            canonical_row["OCCLocation__boundary_description"] = (
                "Boundary not mapped, migrated point coordinate has had a 1 metre buffer applied"
            )

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

            # --- OCCHabitatComposition ---
            # habitat_notes: composed from HABITAT_NOTES + "ASPECT: " + ASPECT
            _hab_notes = str(raw.get("HABITAT_NOTES", "") or "").strip()
            _aspect = str(raw.get("ASPECT", "") or "").strip()
            _hn_parts: list[str] = []
            if _hab_notes:
                _hn_parts.append(_hab_notes)
            if _aspect:
                _hn_parts.append(f"ASPECT: {_aspect}")
            canonical_row["OCCHabitatComposition__habitat_notes"] = "; ".join(_hn_parts) if _hn_parts else None

            # --- OCCHabitatCondition ---
            # Dispatch HABITAT_CONDITION code to 6 individual percentage fields
            _hc_code = str(raw.get("HABITAT_CONDITION", "") or "").strip().upper()
            _HC_MAP = {
                "PRISTINE": (100, 0, 0, 0, 0, 0),
                "EXCELENT": (0, 100, 0, 0, 0, 0),
                "VRY_GOOD": (0, 0, 100, 0, 0, 0),
                "GOOD": (0, 0, 0, 100, 0, 0),
                "DEGRADED": (0, 0, 0, 0, 100, 0),
                "COM_DEGR": (0, 0, 0, 0, 0, 100),
            }
            _hc_fields = (
                "pristine",
                "excellent",
                "very_good",
                "good",
                "degraded",
                "completely_degraded",
            )
            _hc_vals = _HC_MAP.get(_hc_code)
            if _hc_vals:
                for _hc_f, _hc_v in zip(_hc_fields, _hc_vals):
                    canonical_row[f"OCCHabitatCondition__{_hc_f}"] = _hc_v
            else:
                for _hc_f in _hc_fields:
                    canonical_row[f"OCCHabitatCondition__{_hc_f}"] = None

            rows.append(canonical_row)
        return ExtractionResult(rows=rows, warnings=warnings)


# Attach pipelines to adapter class
OccurrenceTpflAdapter.PIPELINES = PIPELINES
