from boranga.components.data_migration.mappings import get_group_type_id
from boranga.components.data_migration.registry import (
    _result,
    build_legacy_map_transform,
    conditional_transform_factory,
    csv_lookup_factory,
    date_from_datetime_iso_factory,
    dependent_from_column_factory,
    emailuser_by_legacy_username_factory,
    emailuser_object_by_legacy_username_factory,
    fk_lookup,
    fk_lookup_static,
    geometry_from_coords_factory,
    occurrence_from_pop_id_factory,
    occurrence_number_from_pop_id_factory,
    pop_id_from_sheetno_factory,
    region_from_district_factory,
    static_value_factory,
    taxonomy_lookup_legacy_mapping_species,
    to_decimal_factory,
    to_int_trailing_factory,
)
from boranga.components.occurrence.models import (
    IdentificationCertainty,
    OccurrenceReport,
)
from boranga.components.species_and_communities.models import Community, GroupType
from boranga.components.users.models import SubmitterCategory

from ..base import ExtractionResult, ExtractionWarning, SourceAdapter
from ..sources import Source
from . import schema

# TPFL-specific transforms and pipelines
# Create factory transform that maps POP_ID to Occurrence instance with persistent caching
OCCURRENCE_FROM_POP_ID_TRANSFORM = occurrence_from_pop_id_factory("TPFL")

# Create factory transform that maps POP_ID to occurrence_number with persistent caching
OCCURRENCE_NUMBER_FROM_POP_ID = occurrence_number_from_pop_id_factory("TPFL")

# Create factory transform that maps SHEETNO to POP_ID directly
POP_ID_FROM_SHEETNO = pop_id_from_sheetno_factory("TPFL")

DATE_FROM_DATETIME_ISO_PERTH = date_from_datetime_iso_factory("Australia/Perth")

# Create factory transform for geometry from coordinates
GEOMETRY_FROM_COORDS = geometry_from_coords_factory(
    latitude_field="GDA94LAT",
    longitude_field="GDA94LONG",
    datum_field="DATUM",
    radius_m=1.0,
)

# Create static value transform for locked=True
GEOMETRY_LOCKED_DEFAULT = static_value_factory(True)

SPECIES_TRANSFORM = taxonomy_lookup_legacy_mapping_species("TPFL")

COMMUNITY_TRANSFORM = fk_lookup(model=Community, lookup_field="community_name")


def map_form_status_code_to_processing_status(value: str, ctx=None) -> str | None:
    mapping = {
        "NEW": OccurrenceReport.PROCESSING_STATUS_DRAFT,
        "READY": OccurrenceReport.PROCESSING_STATUS_WITH_ASSESSOR,
        "ACCEPTED": OccurrenceReport.PROCESSING_STATUS_APPROVED,
        "REJECTED": OccurrenceReport.PROCESSING_STATUS_DECLINED,
    }
    return mapping.get(value.strip().upper()) if value else None


def map_form_status_code_to_customer_status(value: str, ctx=None) -> str | None:
    mapping = {
        "NEW": OccurrenceReport.CUSTOMER_STATUS_DRAFT,
        "READY": OccurrenceReport.CUSTOMER_STATUS_WITH_ASSESSOR,
        "ACCEPTED": OccurrenceReport.CUSTOMER_STATUS_APPROVED,
        "REJECTED": OccurrenceReport.CUSTOMER_STATUS_DECLINED,
    }
    return mapping.get(value.strip().upper()) if value else None


def MAP_FORM_STATUS_CODE_TO_PROCESSING_STATUS(value, ctx):
    return _result(map_form_status_code_to_processing_status(value, ctx))


MAP_FORM_STATUS_CODE_TO_CUSTOMER_STATUS = map_form_status_code_to_customer_status


CUSTOMER_STATUS_FROM_FORM_STATUS_CODE = dependent_from_column_factory(
    "processing_status",
    mapper=lambda dep_val, ctx: MAP_FORM_STATUS_CODE_TO_CUSTOMER_STATUS(dep_val, ctx),
    default=None,
)

FK_OCCURRENCE = fk_lookup(OccurrenceReport, "migrated_from_id")

OCCURRENCE_FROM_POP_ID = dependent_from_column_factory("POP_ID", FK_OCCURRENCE)

RECORD_SOURCE_FROM_CSV = csv_lookup_factory(
    key_column="TERM_CODE",
    value_column="LABEL",
    csv_filename="DRF_LOV_RECORD_SOURCE_VWS.csv",
    legacy_system="TPFL",
    required=False,
)

EMAILUSER_BY_LEGACY_USERNAME_TRANSFORM = emailuser_by_legacy_username_factory("TPFL")

ROLE_TRANSFORM = build_legacy_map_transform(
    "TPFL",
    "OBS_ROLE_CODE (DRF_LOV_ROLE_VWS)",
    required=False,
)

GRAVEL_TRAILING_INT = to_int_trailing_factory(prefix="GRVL_", required=True)

DRAINAGE_TRANSFORM = build_legacy_map_transform(
    "TPFL",
    "DRAINAGE (DRF_LOV_DRAINAGE_VWS)",
    required=False,
)

ROCK_TYPE_TRANSFORM = build_legacy_map_transform(
    "TPFL",
    "ROCK_TYPE (DRF_LOV_ROCK_TYPE_VWS)",
    required=False,
)

# List mappings for habitat closed-lists
SOIL_COLOR_TRANSFORM = build_legacy_map_transform("TPFL", "SOIL_COLOR (DRF_LOV_SOIL_COLOR_VWS)", required=False)

SOIL_CONDITION_TRANSFORM = build_legacy_map_transform("TPFL", "SOIL_CONDITION (DRF_LOV_SOIL_COND_VWS)", required=False)

LANDFORM_TRANSFORM = build_legacy_map_transform("TPFL", "LANDFORM (DRF_LOV_LAND_FORM_VWS)", required=False)

SOIL_TYPE_TRANSFORM = build_legacy_map_transform("TPFL", "SOIL_TYPE (DRF_LOV_SOIL_TYPE_VWS)", required=False)

# Identification closed-list / default transforms
SAMPLE_DEST_TRANSFORM = build_legacy_map_transform("TPFL", "VOUCHER_LOCATION (DRF_LOV_VOUCHER_LOC_VWS)", required=False)


def observer_name_fallback_transform(value: str, ctx=None):
    """If observer name is missing, try to use OBSERVER_CODE from the raw row.

    Behaviour:
    - If `value` is present -> return it unchanged.
    - Otherwise try to read `OBSERVER_CODE` from the transform context row and
      return that (stripped) when present.
    - Leave as `None` when no fallback found (pipeline should include `required`
      to fail in that case).
    """
    # prefer provided value when present
    if value not in (None, ""):
        return _result(value)

    # ctx may be a TransformContext or a plain dict; obtain the row dict
    row = getattr(ctx, "row", None) if ctx is not None else None
    if row is None and isinstance(ctx, dict):
        row = ctx.get("row") or ctx

    if isinstance(row, dict):
        obs_code = row.get("OBSERVER_CODE")
        if obs_code not in (None, ""):
            try:
                return _result(str(obs_code).strip())
            except Exception:
                return _result(obs_code)

    # no fallback found
    return _result(None)


def identification_certainty_default_transform(value: str, ctx=None):
    """If incoming value blank, default to IdentificationCertainty 'High',
    otherwise try to lookup by name (case-insensitive) and return its id."""
    # prefer provided value when present
    try:
        if value not in (None, ""):
            v = str(value).strip()
            if not v:
                raise ValueError("blank")
            obj = IdentificationCertainty.objects.filter(name__iexact=v).first()
            return _result(obj.pk if obj else None)
    except Exception:
        pass

    # fallback default -> 'High'
    try:
        default = IdentificationCertainty.objects.filter(name__iexact="High").first()
        return _result(default.pk if default else None)
    except Exception:
        return _result(None)


def veg_condition_prefix_transform(value: str, ctx=None) -> str | None:
    """Prefix vegetation condition values for habitat notes."""
    if not value:
        return None
    v = value.strip()
    return f"Vegetation Condition: {v}" if v else None


SUBMITTER_CATEGORY_DEFAULT_TRANSFORM = fk_lookup_static(
    model=SubmitterCategory,
    lookup_field="name",
    static_value="DBCA",
)

GET_SUBMITTER_VALUE = dependent_from_column_factory(
    "submitter",
    mapper=lambda dep, ctx: dep.strip() if dep else None,
    default=None,
)

STATIC_DBCA = static_value_factory("DBCA")

# Transform to fetch EmailUser object by legacy username, then pluck full_name
EMAILUSER_OBJ_BY_LEGACY_USERNAME_TRANSFORM = emailuser_object_by_legacy_username_factory("TPFL")


def submitter_name_from_emailuser(value, ctx=None):
    """Extract full name from EmailUser object by calling get_full_name() method."""
    if value is None:
        return _result(None)
    try:
        if hasattr(value, "get_full_name"):
            return _result(value.get_full_name())
        return _result(None)
    except Exception:
        return _result(None)


# Transform for approved_by: if processing_status is 'ACCEPTED', use modified_by and resolve to EmailUser id
# The condiion_value is the raw CSV value before transformation
APPROVED_BY_TRANSFORM = conditional_transform_factory(
    condition_column="processing_status",
    condition_value="ACCEPTED",  # Raw CSV value before transformation
    true_column="modified_by",
    true_transform=EMAILUSER_BY_LEGACY_USERNAME_TRANSFORM,
    false_value=None,
)

# OCRLocation transforms
COORDINATE_SOURCE_TRANSFORM = build_legacy_map_transform(
    "TPFL",
    "CO_ORD_SOURCE_CODE (DRF_LOV_CORDINATE_SOURCE_VWS)",
    required=False,
)

LOCATION_ACCURACY_TRANSFORM = build_legacy_map_transform(
    "TPFL",
    "RESOLUTION (DRF_LOV_RESOLUTION_VWS)",
    required=False,
)

DISTRICT_TRANSFORM = build_legacy_map_transform(
    "TPFL",
    "DISTRICT (DRF_LOV_DEC_DISTRICT_VWS)",
    required=False,
)

# Use the factory-based region_from_district transform from registry
REGION_FROM_DISTRICT = region_from_district_factory()

LANDDISTRICT_TRANSFORM = build_legacy_map_transform(
    "TPFL",
    "LANDDISTRICT (DRF_LOV_LAND_DISTRICT_VWS)",
    required=False,
    return_type="canonical",
)


def lga_code_transform_canonical(value: str, ctx=None) -> str | None:
    """Transform LGA_CODE to canonical_name, then reverse format from "Town, Type" to "Type Town".

    Example: "Busselton, Shire Of" -> "Shire Of Busselton"
    """
    if not value or value.strip() == "":
        return None

    from boranga.components.main.models import LegacyValueMap

    try:
        lvm = LegacyValueMap.objects.filter(
            legacy_system="TPFL",
            list_name="LGA_CODE (DRF_LOV_LGA_VWS)",
            legacy_value=str(value).strip(),
        ).first()

        if not lvm or not lvm.canonical_name:
            return None

        canonical = lvm.canonical_name.strip()
        # Reverse format: "Town, Type" -> "Type Town"
        if "," in canonical:
            parts = canonical.split(",", 1)
            town = parts[0].strip()
            lga_type = parts[1].strip()
            return f"{lga_type} {town}"
        else:
            # If no comma, return as-is
            return canonical
    except Exception:
        return None


def location_description_concatenate(value: str, ctx=None) -> str | None:
    """Concatenate LOCATION with transformed LGA_CODE.

    Formula: LOCATION + " LGA: " + transformed_LGA_CODE

    This transform is designed to run after LGA_CODE has been transformed to the
    canonical reversed format. It reads both the original LOCATION value and the
    transformed LGA_CODE from the transform context.
    """
    if ctx is None:
        return None

    # Extract row from TransformContext or dict
    row = getattr(ctx, "row", None) if ctx is not None else None
    if row is None and isinstance(ctx, dict):
        row = ctx.get("row") or ctx

    if not isinstance(row, dict):
        return None

    location = row.get("LOCATION", "")
    lga_code = row.get("LGA_CODE", "")

    # Strip values
    location = location.strip() if location else ""
    lga_code = lga_code.strip() if lga_code else ""

    # Transform LGA_CODE to canonical reversed format
    lga_canonical = None
    if lga_code:
        lga_canonical = lga_code_transform_canonical(lga_code, ctx)

    # Build result based on what we have
    if location and lga_canonical:
        # Full format: location + " LGA: " + transformed_lga_code
        return _result(f"{location} LGA: {lga_canonical}")
    elif location:
        # Only location available
        return _result(location)
    elif lga_canonical:
        # Only LGA available (unlikely)
        return _result(f"LGA: {lga_canonical}")
    else:
        # Nothing available
        return None


def location_description_wrap_result(value: str, ctx=None) -> str | None:
    """Wrapper that returns TransformResult for location_description_concatenate."""
    result = location_description_concatenate(value, ctx)
    if isinstance(result, str):
        return _result(result)
    return result


# OCRObservationDetail transforms (Tasks 11380, 11382, 11383, 11385)
AREA_ASSESSMENT_TRANSFORM = build_legacy_map_transform(
    "TPFL",
    "SVY_EXTENT (DRF_LOV_SRV_EXT_VWS)",
    required=False,
)

BOUNDARY_DESCRIPTION_DEFAULT = static_value_factory(
    "Boundary not mapped, migrated point coordinate has had a 1 metre buffer applied"
)

EPSG_CODE_DEFAULT = static_value_factory(4326)


# OCRPlantCount transforms
COUNTED_SUBJECT_TRANSFORM = build_legacy_map_transform(
    "TPFL",
    "CNT_PLANT_TYPE_CODE (DRF_LOV_CNT_PLANT_TYPE_VWS)",
    required=False,
)

PLANT_CONDITION_TRANSFORM = build_legacy_map_transform(
    "TPFL",
    "POPULATION_CONDITION (DRF_LOV_PLNT_COND_VWS)",
    required=False,
)

PLANT_COUNT_METHOD_TRANSFORM = build_legacy_map_transform(
    "TPFL",
    "COUNT_MTHD_CODE (DRF_LOV_COUNT_METHOD_VWS)",
    required=False,
)


def ocr_plant_count_comment_transform(value, ctx):
    """Concatenate fields for OCRPlantCount comment."""
    parts = []

    # 1. POPULATION_NOTES
    if value and str(value).strip():
        parts.append(str(value).strip())

    row = getattr(ctx, "row", None) if ctx is not None else None
    if row is None and isinstance(ctx, dict):
        row = ctx.get("row") or ctx

    if not isinstance(row, dict):
        return _result("; ".join(parts) if parts else "")

    from boranga.components.main.models import LegacyValueMap

    # 2. AREA_OCCUPIED_METHOD
    area_method = row.get("AREA_OCCUPIED_METHOD")
    if area_method and str(area_method).strip():
        mapped = LegacyValueMap.get_target(
            legacy_system="TPFL",
            list_name="AREA_OCCUPIED_METHOD (DRF_LOV_AREA_CALC_VWS)",
            legacy_value=str(area_method).strip(),
        )
        if mapped:
            parts.append(f"Area Occupied Method: {mapped}")

    # 3. QUAD_SIZE
    quad_size = row.get("QUAD_SIZE")
    if quad_size and str(quad_size).strip():
        parts.append(f"Quadrat Size: {str(quad_size).strip()}")

    # 4. QUAD_NUM_TOTAL
    quad_num_total = row.get("QUAD_NUM_TOTAL")
    if quad_num_total and str(quad_num_total).strip():
        parts.append(f"Quadrat Simple Count: {str(quad_num_total).strip()}")

    # 5. QUAD_NUM_MATURE
    quad_num_mature = row.get("QUAD_NUM_MATURE")
    if quad_num_mature and str(quad_num_mature).strip():
        parts.append(f"Quadrat Mature Count: {str(quad_num_mature).strip()}")

    # 6. QUAD_NUM_JUVENILE
    quad_num_juvenile = row.get("QUAD_NUM_JUVENILE")
    if quad_num_juvenile and str(quad_num_juvenile).strip():
        parts.append(f"Quadrat Juvenile Count: {str(quad_num_juvenile).strip()}")

    # 7. QUAD_NUM_SEEDLINGS
    quad_num_seedlings = row.get("QUAD_NUM_SEEDLINGS")
    if quad_num_seedlings and str(quad_num_seedlings).strip():
        parts.append(f"Quadrat Seedlings Count: {str(quad_num_seedlings).strip()}")

    return _result("; ".join(parts))


def ocr_plant_count_status_transform(value, ctx):
    """Derive count_status from presence of count data."""
    from boranga.settings import (
        COUNT_STATUS_COUNTED,
        COUNT_STATUS_NOT_COUNTED,
        COUNT_STATUS_SIMPLE_COUNT,
    )

    row = getattr(ctx, "row", None) if ctx is not None else None
    if row is None and isinstance(ctx, dict):
        row = ctx.get("row") or ctx

    if not isinstance(row, dict):
        return _result(COUNT_STATUS_NOT_COUNTED)

    # Check detailed count fields
    # Note: Use canonical field names as `row` has been mapped by schema
    detailed_fields = [
        "OCRPlantCount__detailed_alive_juvenile",
        "OCRPlantCount__detailed_alive_mature",
        "OCRPlantCount__detailed_alive_seedling",
        "OCRPlantCount__detailed_dead_juvenile",
        "OCRPlantCount__detailed_dead_mature",
        "OCRPlantCount__detailed_dead_seedling",
    ]
    has_detailed = any(row.get(f) is not None and str(row.get(f)).strip() != "" for f in detailed_fields)

    if has_detailed:
        return _result(COUNT_STATUS_COUNTED)

    # Check simple count fields
    simple_fields = ["OCRPlantCount__simple_alive", "OCRPlantCount__simple_dead"]
    has_simple = any(row.get(f) is not None and str(row.get(f)).strip() != "" for f in simple_fields)

    if has_simple:
        return _result(COUNT_STATUS_SIMPLE_COUNT)

    return _result(COUNT_STATUS_NOT_COUNTED)


def ocr_fire_history_comment_transform(value, ctx):
    """Concatenate FIRE_SEASON and FIRE_YEAR for OCRFireHistory comment."""
    row = getattr(ctx, "row", None) if ctx is not None else None
    if row is None and isinstance(ctx, dict):
        row = ctx.get("row") or ctx

    if not isinstance(row, dict):
        return _result("")

    parts = []
    # value is FIRE_SEASON (mapped to OCRFireHistory__comment)
    if value and str(value).strip():
        parts.append(str(value).strip())

    # FIRE_YEAR is mapped to itself in schema, so it should be in row
    year = row.get("FIRE_YEAR")
    if year and str(year).strip():
        parts.append(str(year).strip())

    return _result(" ".join(parts))


FIRE_INTENSITY_TRANSFORM = build_legacy_map_transform(
    "TPFL",
    "FIRE_INTENSITY (DRF_LOV_LW_MD_HI_VWS)",
    required=False,
)


PIPELINES = {
    "migrated_from_id": ["strip", "required"],
    "Occurrence__migrated_from_id": [OCCURRENCE_FROM_POP_ID_TRANSFORM],
    "species_id": ["strip", "blank_to_none", SPECIES_TRANSFORM],
    "community_id": ["strip", "blank_to_none", COMMUNITY_TRANSFORM],
    "lodgement_date": ["strip", "blank_to_none", "datetime_iso"],
    "observation_date": ["strip", "blank_to_none", DATE_FROM_DATETIME_ISO_PERTH],
    "record_source": ["strip", "blank_to_none", RECORD_SOURCE_FROM_CSV],
    "customer_status": [CUSTOMER_STATUS_FROM_FORM_STATUS_CODE],
    "comments": ["ocr_comments_transform"],
    "ocr_for_occ_name": ["strip", "blank_to_none"],
    "ocr_for_occ_number": [POP_ID_FROM_SHEETNO],
    "processing_status": [
        "strip",
        "required",
        MAP_FORM_STATUS_CODE_TO_PROCESSING_STATUS,
    ],
    "submitter": ["strip", "blank_to_none", EMAILUSER_BY_LEGACY_USERNAME_TRANSFORM],
    "approved_by": [APPROVED_BY_TRANSFORM],
    "OCRObserverDetail__role": ["strip", "blank_to_none", ROLE_TRANSFORM],
    "OCRObserverDetail__observer_name": [
        "strip",
        "blank_to_none",
        observer_name_fallback_transform,
    ],
    "OCRObserverDetail__organisation": ["strip", "blank_to_none"],
    "OCRHabitatComposition__loose_rock_percent": [
        "strip",
        "blank_to_none",
        GRAVEL_TRAILING_INT,
    ],
    "OCRHabitatComposition__drainage": ["strip", "blank_to_none", DRAINAGE_TRANSFORM],
    "OCRHabitatComposition__rock_type": ["strip", "blank_to_none", ROCK_TYPE_TRANSFORM],
    # Habitat composition extras (apply TPFL closed-list mappings)
    "OCRHabitatComposition__habitat_notes": ["strip", "blank_to_none"],
    "OCRHabitatComposition__vegetation_condition": [
        "strip",
        "blank_to_none",
        veg_condition_prefix_transform,
    ],
    "OCRHabitatComposition__soil_colour": [
        "strip",
        "blank_to_none",
        SOIL_COLOR_TRANSFORM,
    ],
    "OCRHabitatComposition__soil_condition": [
        "strip",
        "blank_to_none",
        SOIL_CONDITION_TRANSFORM,
    ],
    "OCRHabitatComposition__land_form": ["strip", "blank_to_none", LANDFORM_TRANSFORM],
    "OCRHabitatComposition__soil_type": ["strip", "blank_to_none", SOIL_TYPE_TRANSFORM],
    # Identification pipelines
    "OCRIdentification__barcode_number": ["strip", "blank_to_none"],
    "OCRIdentification__collector_number": ["strip", "blank_to_none"],
    "OCRIdentification__permit_id": ["strip", "blank_to_none"],
    "OCRIdentification__sample_destination": [
        "strip",
        "blank_to_none",
        SAMPLE_DEST_TRANSFORM,
    ],
    "OCRIdentification__identification_certainty": [
        "strip",
        "blank_to_none",
        identification_certainty_default_transform,
    ],
    "OCRIdentification__identification_comment": ["strip", "blank_to_none"],
    # SubmitterInformation pipelines (Task 11302-11306)
    "SubmitterInformation__submitter_category": [SUBMITTER_CATEGORY_DEFAULT_TRANSFORM],
    "SubmitterInformation__email_user": [
        GET_SUBMITTER_VALUE,
        "strip",
        "blank_to_none",
        EMAILUSER_BY_LEGACY_USERNAME_TRANSFORM,
    ],
    "SubmitterInformation__name": [
        GET_SUBMITTER_VALUE,
        "strip",
        "blank_to_none",
        EMAILUSER_OBJ_BY_LEGACY_USERNAME_TRANSFORM,
        submitter_name_from_emailuser,
    ],
    "SubmitterInformation__organisation": [STATIC_DBCA],
    # OCRLocation pipelines (Tasks 11347-11355)
    "OCRLocation__coordinate_source": [
        "strip",
        "blank_to_none",
        COORDINATE_SOURCE_TRANSFORM,
    ],
    "OCRLocation__location_accuracy": [
        "strip",
        "blank_to_none",
        LOCATION_ACCURACY_TRANSFORM,
    ],
    "OCRLocation__district": ["strip", "blank_to_none", DISTRICT_TRANSFORM],
    "OCRLocation__region": [REGION_FROM_DISTRICT],
    "OCRLocation__locality": ["strip", "blank_to_none", LANDDISTRICT_TRANSFORM],
    "OCRLocation__location_description": [
        location_description_wrap_result,
    ],
    "OCRLocation__boundary_description": [BOUNDARY_DESCRIPTION_DEFAULT],
    "OCRLocation__epsg_code": [EPSG_CODE_DEFAULT],
    # OCRObservationDetail pipelines (Tasks 11380, 11382, 11383, 11385)
    "OCRObservationDetail__area_assessment": [
        "strip",
        "blank_to_none",
        AREA_ASSESSMENT_TRANSFORM,
    ],
    "OCRObservationDetail__area_surveyed": [
        "strip",
        "blank_to_none",
        to_decimal_factory(decimal_places=4),
    ],
    "OCRObservationDetail__survey_duration": [
        "strip",
        "blank_to_none",
        "to_int",
    ],
    # OccurrenceReportGeometry pipelines (Tasks 11359, 11364, 11366)
    "OccurrenceReportGeometry__geometry": [GEOMETRY_FROM_COORDS],
    "OccurrenceReportGeometry__locked": [GEOMETRY_LOCKED_DEFAULT],
    # OCRPlantCount pipelines
    "OCRPlantCount__counted_subject": [
        "strip",
        "blank_to_none",
        COUNTED_SUBJECT_TRANSFORM,
    ],
    "OCRPlantCount__plant_condition": [
        "strip",
        "blank_to_none",
        PLANT_CONDITION_TRANSFORM,
    ],
    "OCRPlantCount__plant_count_method": [
        "strip",
        "blank_to_none",
        PLANT_COUNT_METHOD_TRANSFORM,
    ],
    "OCRPlantCount__clonal_reproduction_present": [
        "strip",
        "blank_to_none",
        "y_to_true_n_to_none",
    ],
    "OCRPlantCount__comment": [ocr_plant_count_comment_transform],
    "OCRPlantCount__count_status": [ocr_plant_count_status_transform],
    "OCRPlantCount__dehisced_fruit_present": [
        "strip",
        "blank_to_none",
        "y_to_true_n_to_none",
    ],
    "OCRPlantCount__detailed_alive_juvenile": ["strip", "blank_to_none", "to_int"],
    "OCRPlantCount__detailed_alive_mature": ["strip", "blank_to_none", "to_int"],
    "OCRPlantCount__detailed_alive_seedling": ["strip", "blank_to_none", "to_int"],
    "OCRPlantCount__detailed_dead_juvenile": ["strip", "blank_to_none", "to_int"],
    "OCRPlantCount__detailed_dead_mature": ["strip", "blank_to_none", "to_int"],
    "OCRPlantCount__detailed_dead_seedling": ["strip", "blank_to_none", "to_int"],
    "OCRPlantCount__estimated_population_area": [
        "strip",
        "blank_to_none",
        "to_decimal",
    ],
    "OCRPlantCount__flower_bud_present": [
        "strip",
        "blank_to_none",
        "y_to_true_n_to_none",
    ],
    "OCRPlantCount__flower_present": [
        "strip",
        "blank_to_none",
        "y_to_true_n_to_none",
    ],
    "OCRPlantCount__flowering_plants_per": [
        "strip",
        "blank_to_none",
        "to_decimal",
    ],
    "OCRPlantCount__immature_fruit_present": [
        "strip",
        "blank_to_none",
        "y_to_true_n_to_none",
    ],
    "OCRPlantCount__pollinator_observation": ["strip", "blank_to_none"],
    "OCRPlantCount__quadrats_surveyed": ["strip", "blank_to_none", "to_int"],
    "OCRPlantCount__ripe_fruit_present": [
        "strip",
        "blank_to_none",
        "y_to_true_n_to_none",
    ],
    "OCRPlantCount__simple_alive": ["strip", "blank_to_none", "to_int"],
    "OCRPlantCount__simple_dead": ["strip", "blank_to_none", "to_int"],
    "OCRPlantCount__total_quadrat_area": [
        "strip",
        "blank_to_none",
        to_decimal_factory(max_digits=12, decimal_places=2),
    ],
    "OCRPlantCount__vegetative_state_present": [
        "strip",
        "blank_to_none",
        "y_to_true_n_to_none",
    ],
    "OCRPlantCount__obs_date": ["strip", "blank_to_none", DATE_FROM_DATETIME_ISO_PERTH],
    # OCRFireHistory pipelines
    "OCRFireHistory__comment": [ocr_fire_history_comment_transform],
    "OCRFireHistory__intensity": [FIRE_INTENSITY_TRANSFORM],
}


def preload_veg_classes_map(path: str) -> dict[str, list[str]]:
    """
    Load DRF_SHEET_VEG_CLASSES_Sanitised.csv into a dict:
    SHEETNO -> [VEG_CLASS_CODE, ...]
    Preserves order of records for each SHEETNO.
    """
    import csv
    import os

    if not os.path.exists(path):
        return {}

    mapping = {}
    with open(path, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            sheetno = row.get("SHEETNO", "").strip()
            code = row.get("VEG_CLASS_CODE", "").strip()
            if sheetno and code:
                if sheetno not in mapping:
                    mapping[sheetno] = []
                mapping[sheetno].append(code)
    return mapping


class OccurrenceReportTpflAdapter(SourceAdapter):
    source_key = Source.TPFL.value
    domain = "occurrence_report"

    def extract(self, path: str, **options) -> ExtractionResult:
        rows = []
        warnings: list[ExtractionWarning] = []

        # Preload veg classes
        import os

        # Always use the standard location
        veg_classes_path = "private-media/legacy_data/TPFL/DRF_SHEET_VEG_CLASSES_Sanitised.csv"

        if not os.path.exists(veg_classes_path):
            raise FileNotFoundError(f"Vegetation classes file not found at {veg_classes_path}")

        veg_classes_map = preload_veg_classes_map(veg_classes_path)

        raw_rows, read_warnings = self.read_table(path)
        warnings.extend(read_warnings)

        for raw in raw_rows:
            canonical = schema.map_raw_row(raw)
            # Map observation_date to OCRPlantCount__obs_date
            if canonical.get("observation_date"):
                canonical["OCRPlantCount__obs_date"] = canonical.get("observation_date")

            # Map OCRVegetationStructure fields from preloaded map
            sheetno = canonical.get("migrated_from_id")
            if sheetno and sheetno in veg_classes_map:
                veg_codes = veg_classes_map[sheetno]
                # 1st record -> layer 4
                if len(veg_codes) >= 1:
                    canonical["OCRVegetationStructure__vegetation_structure_layer_four"] = veg_codes[0]
                # 2nd record -> layer 3
                if len(veg_codes) >= 2:
                    canonical["OCRVegetationStructure__vegetation_structure_layer_three"] = veg_codes[1]
                # 3rd record -> layer 2
                if len(veg_codes) >= 3:
                    canonical["OCRVegetationStructure__vegetation_structure_layer_two"] = veg_codes[2]
                # 4th record -> layer 1
                if len(veg_codes) >= 4:
                    canonical["OCRVegetationStructure__vegetation_structure_layer_one"] = veg_codes[3]

            # Use SHEET_POP_NUMBER/SHEET_SUBPOP_CODE as POP_NUMBER/SUBPOP_CODE are not in RFR forms
            # and not in schema column map
            pop_num = canonical.get("SHEET_POP_NUMBER") or raw.get("SHEET_POP_NUMBER") or raw.get("POP_NUMBER") or ""
            sub_pop = canonical.get("SHEET_SUBPOP_CODE") or raw.get("SHEET_SUBPOP_CODE") or raw.get("SUBPOP_CODE") or ""
            canonical["occurrence_report_name"] = f"{str(pop_num).strip()} {str(sub_pop).strip()}".strip()

            canonical["group_type_id"] = get_group_type_id(GroupType.GROUP_TYPE_FLORA)
            assessor_data = None
            # REASON_DEACTIVATED and DEACTIVATED_DATE are not in schema column map, so read from raw
            REASON_DEACTIVATED = raw.get("REASON_DEACTIVATED", "").strip()
            if REASON_DEACTIVATED:
                DEACTIVATED_DATE = raw.get("DEACTIVATED_DATE", "").strip()
                assessor_data = f"Reason Deactivated: {REASON_DEACTIVATED}, {DEACTIVATED_DATE}"
            canonical["assessor_data"] = assessor_data
            # Build occurrence_name: concat POP_NUMBER + SUBPOP_CODE (no space)
            pop = str(canonical.get("SHEET_POP_NUMBER", "") or "").strip()
            sub = str(canonical.get("SHEET_SUBPOP_CODE", "") or "").strip()
            ocr_for_occ_name = (pop + sub).strip()
            # If only a single digit (e.g. "1"), pad with leading zero -> "01"
            if ocr_for_occ_name and len(ocr_for_occ_name) == 1 and ocr_for_occ_name.isdigit():
                ocr_for_occ_name = ocr_for_occ_name.zfill(2)
            canonical["ocr_for_occ_name"] = ocr_for_occ_name
            canonical["OCRObserverDetail__main_observer"] = True
            canonical["internal_application"] = True
            # Build habitat_notes by combining habitat notes, aspect and vegetation condition
            hab_notes_parts: list[str] = []
            HAB_NOTES = canonical.get("HABITAT_NOTES", "")
            if HAB_NOTES:
                hab_notes_parts.append(HAB_NOTES.strip())
            ASPECT = canonical.get("ASPECT", "")
            if ASPECT:
                asp = ASPECT.strip()
                if asp:
                    hab_notes_parts.append(f"ASPECT: {asp}")
            # SV_VEGETATION_CONDITION should be prefixed
            SV_VEG = canonical.get("SV_VEGETATION_CONDITION", "")
            if SV_VEG:
                sv = SV_VEG.strip()
                if sv:
                    hab_notes_parts.append(f"Vegetation Condition: {sv}")

            # also consider SCON_COMMENTS (survey conditions) if present
            SCON = canonical.get("SCON_COMMENTS", "")
            if SCON:
                scon = SCON.strip()
                if scon:
                    hab_notes_parts.append(scon)

            # always populate habitat_notes (None when empty) so downstream
            # merge/creation logic sees the key consistently
            canonical["OCRHabitatComposition__habitat_notes"] = "; ".join(hab_notes_parts) if hab_notes_parts else None

            # HABITAT_CONDITION => populate OCRHabitatCondition flags per task
            # rules regardless of whether habitat notes exist. Previously these
            # flags were only set when `hab_notes_parts` existed which caused
            # missing flags (and default 0s) to be used during merge/creation.
            hc_raw = canonical.get("HABITAT_CONDITION", "")
            hc = hc_raw.strip().upper() if hc_raw else ""
            # default all zeros (will be overridden by merge logic which
            # prefers the maximum percentage across source rows)
            canonical["OCRHabitatCondition__pristine"] = 100 if hc in ("PRISTINE",) else 0
            canonical["OCRHabitatCondition__excellent"] = (
                100
                if hc
                in (
                    "EXCELENT",
                    "EXCELLENT",
                )
                else 0
            )
            canonical["OCRHabitatCondition__very_good"] = (
                100
                if hc
                in (
                    "VRY_GOOD",
                    "VRYGOOD",
                    "VERY_GOOD",
                    "VERYGOOD",
                )
                else 0
            )
            canonical["OCRHabitatCondition__good"] = 100 if hc in ("GOOD",) else 0
            canonical["OCRHabitatCondition__degraded"] = 100 if hc in ("DEGRADED",) else 0
            canonical["OCRHabitatCondition__completely_degraded"] = (
                100
                if hc
                in (
                    "COM_DEGR",
                    "COMPLETELY_DEGRADED",
                    "COM_DEG",
                )
                else 0
            )

            # copy through simple habitat fields if present
            if canonical.get("SOIL_COLOR"):
                canonical["OCRHabitatComposition__soil_colour"] = canonical.get("SOIL_COLOR")
            if canonical.get("SOIL_CONDITION"):
                canonical["OCRHabitatComposition__soil_condition"] = canonical.get("SOIL_CONDITION")
            if canonical.get("LANDFORM"):
                canonical["OCRHabitatComposition__land_form"] = canonical.get("LANDFORM")
            if canonical.get("SOIL_TYPE"):
                canonical["OCRHabitatComposition__soil_type"] = canonical.get("SOIL_TYPE")

            # Identification: copy simple fields through so pipelines can map them
            if canonical.get("BARCODE"):
                canonical["OCRIdentification__barcode_number"] = canonical.get("BARCODE")
            if canonical.get("COLLECTOR_NO"):
                canonical["OCRIdentification__collector_number"] = canonical.get("COLLECTOR_NO")
            if canonical.get("LICENCE"):
                canonical["OCRIdentification__permit_id"] = canonical.get("LICENCE")

            # Identification comment composition: map and prefix VCHR_STATUS_CODE & DUPVOUCH_LOCATION
            try:
                from boranga.components.main.models import LegacyValueMap

                idc_parts = []
                VCHR = canonical.get("VCHR_STATUS_CODE", "")
                if VCHR:
                    mapped = LegacyValueMap.get_target(
                        legacy_system="TPFL",
                        list_name="VCHR_STATUS_CODE (DRF_LOV_VOUCHER_STAT_VWS)",
                        legacy_value=VCHR,
                    )
                    if mapped:
                        idc_parts.append(f"Specimen Status: {mapped}")

                DUPV = canonical.get("DUPVOUCH_LOCATION", "")
                if DUPV:
                    mapped2 = LegacyValueMap.get_target(
                        legacy_system="TPFL",
                        list_name="DUPVOUCH_LOCATION (DRF_LOV_VOUCHER_LOC_VWS)",
                        legacy_value=DUPV,
                    )
                    if mapped2:
                        idc_parts.append(f"Duplicate Voucher Location: {mapped2}")

                if idc_parts:
                    canonical["OCRIdentification__identification_comment"] = "; ".join(idc_parts)
            except Exception:
                # non-fatal: continue without identification comment
                pass

            # OCRLocation: copy simple fields through so pipelines can map them
            if canonical.get("CO_ORD_SOURCE_CODE"):
                canonical["OCRLocation__coordinate_source"] = canonical.get("CO_ORD_SOURCE_CODE")
            if canonical.get("RESOLUTION"):
                canonical["OCRLocation__location_accuracy"] = canonical.get("RESOLUTION")
            if canonical.get("DISTRICT"):
                canonical["OCRLocation__district"] = canonical.get("DISTRICT")
            if canonical.get("LANDDISTRICT"):
                canonical["OCRLocation__locality"] = canonical.get("LANDDISTRICT")
            # location_description is composed from LOCATION + LGA_CODE
            # The pipeline will handle the concatenation; we just preserve raw values
            if canonical.get("LOCATION"):
                canonical["LOCATION"] = canonical.get("LOCATION")
            if canonical.get("LGA_CODE"):
                canonical["LGA_CODE"] = canonical.get("LGA_CODE")

            # OCRObservationDetail: copy simple fields through so pipelines can map them
            if canonical.get("SVY_EXTENT"):
                canonical["OCRObservationDetail__area_assessment"] = canonical.get("SVY_EXTENT")
            if canonical.get("SVY_EFFORT_AREA"):
                canonical["OCRObservationDetail__area_surveyed"] = canonical.get("SVY_EFFORT_AREA")
            if canonical.get("SVY_EFFORT_TIME"):
                canonical["OCRObservationDetail__survey_duration"] = canonical.get("SVY_EFFORT_TIME")

            # OccurrenceReportGeometry: trigger geometry creation with lat/long
            # Store lat/long as separate keys; the pipeline will convert them to geometry
            if canonical.get("GDA94LAT") or canonical.get("GDA94LONG"):
                canonical["OccurrenceReportGeometry__geometry"] = None  # Will be populated by pipeline
                canonical["OccurrenceReportGeometry__locked"] = True  # Default locked=True

            # Prepend source prefix to migrated_from_id
            mid = canonical.get("migrated_from_id")
            if mid and not str(mid).startswith(f"{Source.TPFL.value.lower()}-"):
                canonical["migrated_from_id"] = f"{Source.TPFL.value.lower()}-{mid}"

            # If an explicit occurrence id is present in the source we do not assign
            # it here; the importer will link habitat to the parent OccurrenceReport
            rows.append(canonical)
        return ExtractionResult(rows=rows, warnings=warnings)


# Attach pipelines to adapter
OccurrenceReportTpflAdapter.PIPELINES = PIPELINES
