"""TFAUNA OccurrenceReport adapter — migrates Fauna Records.csv into
OccurrenceReport + OCRAnimalObservation + related child models.

Source CSV: private-media/legacy_data/TFAUNA/Fauna Records.csv
"""

from __future__ import annotations

import logging
from collections import defaultdict

from boranga.components.data_migration.mappings import get_group_type_id
from boranga.components.data_migration.registry import (
    TransformIssue,
    _result,
    build_legacy_map_transform,
    date_from_datetime_iso_local_factory,
    datetime_iso_factory,
    emailuser_by_legacy_username_factory,
    emailuser_object_by_legacy_username_factory,
    fk_lookup_static,
    geometry_from_coords_factory,
    region_from_district_factory,
    static_value_factory,
)
from boranga.components.occurrence.models import OccurrenceReport
from boranga.components.species_and_communities.models import GroupType
from boranga.components.users.models import SubmitterCategory

from ..base import ExtractionResult, ExtractionWarning, SourceAdapter
from ..sources import Source
from . import schema

logger = logging.getLogger(__name__)


# ── Cached species lookup: NameId (taxon_name_id) → Species PK ─────

_name_id_to_species_cache: dict[str, int] | None = None


def _get_name_id_to_species_map() -> dict[str, int]:
    """Build and cache a map from taxon_name_id → Species primary key
    using TFAUNA LegacyTaxonomyMapping entries."""
    global _name_id_to_species_cache
    if _name_id_to_species_cache is not None:
        return _name_id_to_species_cache

    from boranga.components.main.models import LegacyTaxonomyMapping
    from boranga.components.species_and_communities.models import Species

    # taxonomy_id → species pk
    tax_to_species: dict[int, int] = {}
    for sp in Species.objects.values("taxonomy_id", "id"):
        if sp["taxonomy_id"]:
            tax_to_species[sp["taxonomy_id"]] = sp["id"]

    # taxon_name_id → species pk
    result: dict[str, int] = {}
    for m in LegacyTaxonomyMapping.objects.filter(list_name="TFAUNA").only("taxon_name_id", "taxonomy_id"):
        if m.taxon_name_id and m.taxonomy_id:
            sp_id = tax_to_species.get(m.taxonomy_id)
            if sp_id:
                result[str(m.taxon_name_id)] = sp_id

    logger.info(
        "TFAUNA species cache: %d NameId→Species mappings from %d TFAUNA taxonomy entries",
        len(result),
        LegacyTaxonomyMapping.objects.filter(list_name="TFAUNA").count(),
    )
    _name_id_to_species_cache = result
    return result


def _species_from_name_id(value, ctx=None):
    """Pipeline transform: resolve NameId (taxon_name_id) → Species PK."""
    if value in (None, ""):
        return _result(None)
    key = str(value).strip()
    if not key:
        return _result(None)
    lookup = _get_name_id_to_species_map()
    sp_id = lookup.get(key)
    if sp_id:
        return _result(sp_id)
    return _result(
        value,
        TransformIssue("error", f"No Species resolved for TFAUNA NameId={key}"),
    )


# ── Cached district lookup: DistrictNo → District PK ───────────────

_district_no_cache: dict[str, int] | None = None


def _get_district_no_map() -> dict[str, int]:
    """Build and cache a map from DistrictNo → District PK,
    using the TFAUNA 'District' entries in LegacyValueMap that map
    district names → District PKs, combined with a static DistrictNo→name table."""
    global _district_no_cache
    if _district_no_cache is not None:
        return _district_no_cache

    from boranga.components.main.models import LegacyValueMap

    # Load TFAUNA District (name → target_object_id) from LegacyValueMap
    name_to_pk: dict[str, int] = {}
    for m in LegacyValueMap.objects.filter(legacy_system="TFAUNA", list_name="District"):
        if m.legacy_value and m.target_object_id:
            name_to_pk[m.legacy_value.strip().casefold()] = m.target_object_id

    # Standard WA DBCA DistrictNo code → district name mapping
    # Source: WA DEC/DBCA district numbering convention
    _DISTRICT_NO_TO_NAME: dict[str, str] = {
        "11": "Goldfields Region",
        "12": "Esperance",
        "13": "Albany",
        "14": "Esperance",  # fallback
        "21": "Swan Coastal",
        "23": "Perth Hills",
        "24": "Wellington",
        "31": "Moora",
        "32": "Central Wheatbelt",
        "33": "Wheatbelt Region",
        "41": "Geraldton",
        "42": "Murchison",
        "44": "Geraldton",  # fallback
        "51": "Exmouth",
        "53": "Pilbara Region",
        "61": "East Kimberley",
        "62": "West Kimberley",
        "70": "Donnelly",  # SW region fallback
        "71": "Albany",
        "81": "Blackwood",
        "82": "Donnelly",
        "90": "Swan Coastal",
        "91": "Frankland",
        "92": "Frankland",  # fallback
    }

    result: dict[str, int] = {}
    for code, name in _DISTRICT_NO_TO_NAME.items():
        pk = name_to_pk.get(name.casefold())
        if pk:
            result[code] = pk
        else:
            logger.warning("TFAUNA DistrictNo %s → '%s' not found in District LegacyValueMap", code, name)

    logger.info("TFAUNA district cache: %d DistrictNo→District PK mappings", len(result))
    _district_no_cache = result
    return result


# ── Cached district → region map ───────────────────────────────────

_district_pk_to_region_cache: dict[int, int] | None = None


def _get_district_to_region_map() -> dict[int, int]:
    """Build and cache a map from District PK → Region PK."""
    global _district_pk_to_region_cache
    if _district_pk_to_region_cache is not None:
        return _district_pk_to_region_cache

    from boranga.components.species_and_communities.models import District

    result: dict[int, int] = {}
    for d in District.objects.select_related("region").all():
        if d.region_id:
            result[d.pk] = d.region_id

    _district_pk_to_region_cache = result
    return result


# ── Default TFAUNA user fallback ────────────────────────────────────
# The default TFAUNA email user (boranga.tfauna@dbca.wa.gov.au) is used
# when legacy username lookups fail due to absent LegacyUsernameEmailuserMapping.
TFAUNA_DEFAULT_USER_ID = 474408


def _emailuser_with_fallback(value, ctx=None):
    """Pipeline transform: try legacy username lookup, fall back to default user."""
    if value in (None, ""):
        return _result(TFAUNA_DEFAULT_USER_ID)
    # Try the standard lookup first

    pipeline_fn = EMAILUSER_BY_LEGACY_USERNAME
    # Call the registered transform function directly
    from boranga.components.data_migration.registry import registry as _reg

    fn = _reg._fns.get(pipeline_fn)
    if fn:
        res = fn(value, ctx)
        # If the lookup succeeded (no error issues), return its result
        if not any(getattr(i, "level", "") == "error" for i in res.issues):
            return res
    # Fallback to default user
    return _result(TFAUNA_DEFAULT_USER_ID)


def _emailuser_obj_with_fallback(value, ctx=None):
    """Pipeline transform: try legacy username→EmailUser object lookup, fall back to default."""
    if value in (None, ""):
        return _result(None)
    from boranga.components.data_migration.registry import registry as _reg

    fn = _reg._fns.get(EMAILUSER_OBJ_BY_LEGACY_USERNAME)
    if fn:
        res = fn(value, ctx)
        if not any(getattr(i, "level", "") == "error" for i in res.issues):
            return res
    # Fallback: return None (name extraction will fail gracefully)
    return _result(None)


# ── Factory transforms ──────────────────────────────────────────────

EMAILUSER_BY_LEGACY_USERNAME = emailuser_by_legacy_username_factory("TFAUNA")

EMAILUSER_OBJ_BY_LEGACY_USERNAME = emailuser_object_by_legacy_username_factory("TFAUNA")

DATETIME_ISO_PERTH = datetime_iso_factory("Australia/Perth")

# For DateField columns: return the local (Perth) date to avoid day-shift from UTC conversion
DATE_LOCAL_PERTH = date_from_datetime_iso_local_factory("Australia/Perth")

GEOMETRY_FROM_COORDS = geometry_from_coords_factory(
    latitude_field="Lat",
    longitude_field="Long",
    datum_field="DATUM",
    radius_m=1.0,
    point_only=True,  # Fauna OCRs only accept Point geometry, not Polygon
)

GEOMETRY_LOCKED_DEFAULT = static_value_factory(True)

SUBMITTER_CATEGORY_DBCA = fk_lookup_static(
    model=SubmitterCategory,
    lookup_field="name",
    static_value="DBCA",
)

STATIC_DBCA = static_value_factory("DBCA")

REGION_FROM_DISTRICT = region_from_district_factory()

EPSG_CODE_DEFAULT = static_value_factory(4326)

# Task 12762 (S&C-blocked): Resolution → LocationAccuracy mapping.
# `required=False` so that blank values → None silently, and unmapped codes
# → None with an error issue rather than raising a FK violation.
LOCATION_ACCURACY_TRANSFORM = build_legacy_map_transform(
    "TFAUNA",
    "Resolution",
    required=False,
)

# ── Dead/alive determination helpers ────────────────────────────────

DEAD_OBSERV_TYPES = frozenset({"Dead", "Dead ", "Fossil", "Subfossil material"})


def _is_dead(observ_type: str | None) -> bool:
    """Return True when ObservType indicates the observed animal was dead."""
    if not observ_type:
        return False
    return observ_type.strip() in DEAD_OBSERV_TYPES or observ_type in DEAD_OBSERV_TYPES


# ── Submitted-by helpers ────────────────────────────────────────────


def submitter_name_from_emailuser(value, ctx=None):
    """Extract full name from EmailUser object."""
    if value is None:
        return _result(None)
    try:
        if hasattr(value, "get_full_name"):
            return _result(value.get_full_name())
        return _result(None)
    except Exception:
        return _result(None)


# ── count_status derivation ─────────────────────────────────────────


def _derive_count_status(canonical: dict) -> str:
    """Derive OCRAnimalObservation count_status from populated count fields."""
    from boranga.settings import (
        COUNT_STATUS_COUNTED,
        COUNT_STATUS_NOT_COUNTED,
        COUNT_STATUS_SIMPLE_COUNT,
    )

    detailed_fields = [
        "OCRAnimalObservation__alive_adult_male",
        "OCRAnimalObservation__dead_adult_male",
        "OCRAnimalObservation__alive_adult_female",
        "OCRAnimalObservation__dead_adult_female",
        "OCRAnimalObservation__alive_adult_unknown",
        "OCRAnimalObservation__dead_adult_unknown",
        "OCRAnimalObservation__alive_juvenile_male",
        "OCRAnimalObservation__dead_juvenile_male",
        "OCRAnimalObservation__alive_juvenile_female",
        "OCRAnimalObservation__dead_juvenile_female",
        "OCRAnimalObservation__alive_juvenile_unknown",
        "OCRAnimalObservation__dead_juvenile_unknown",
    ]
    has_detailed = any(canonical.get(f) is not None and str(canonical.get(f)).strip() != "" for f in detailed_fields)
    if has_detailed:
        return COUNT_STATUS_COUNTED

    simple_fields = [
        "OCRAnimalObservation__simple_alive",
        "OCRAnimalObservation__simple_dead",
    ]
    has_simple = any(canonical.get(f) is not None and str(canonical.get(f)).strip() != "" for f in simple_fields)
    if has_simple:
        return COUNT_STATUS_SIMPLE_COUNT

    return COUNT_STATUS_NOT_COUNTED


# ── Processing / customer status (all TFAUNA = approved) ────────────


def _processing_status(_value, _ctx=None):
    return _result(OccurrenceReport.PROCESSING_STATUS_APPROVED)


def _customer_status(_value, _ctx=None):
    return _result(OccurrenceReport.CUSTOMER_STATUS_APPROVED)


# ── PIPELINES ───────────────────────────────────────────────────────

PIPELINES = {
    "migrated_from_id": ["strip", "required"],
    "species_id": ["strip", "blank_to_none", _species_from_name_id],
    "processing_status": ["strip", "required", _processing_status],
    "customer_status": [_customer_status],
    "lodgement_date": ["strip", "blank_to_none", DATETIME_ISO_PERTH],
    "observation_date": ["strip", "blank_to_none", DATE_LOCAL_PERTH],
    "comments": ["strip", "blank_to_none"],
    "record_source": ["strip", "blank_to_none"],
    "ocr_for_occ_name": ["strip", "blank_to_none"],
    "submitter": ["strip", "blank_to_none", _emailuser_with_fallback],
    "approved_by": ["strip", "blank_to_none", _emailuser_with_fallback],
    # SubmitterInformation
    "SubmitterInformation__submitter_category": [SUBMITTER_CATEGORY_DBCA],
    "SubmitterInformation__email_user": [
        "strip",
        "blank_to_none",
        _emailuser_with_fallback,
    ],
    "SubmitterInformation__name": [
        "strip",
        "blank_to_none",
        _emailuser_obj_with_fallback,
        submitter_name_from_emailuser,
    ],
    "SubmitterInformation__organisation": [STATIC_DBCA],
    # OCRObserverDetail
    "OCRObserverDetail__observer_name": ["strip", "blank_to_none"],
    "OCRObserverDetail__organisation": ["strip", "blank_to_none"],
    "OCRObserverDetail__contact": ["strip", "blank_to_none"],
    # OCRLocation
    "OCRLocation__locality": ["strip", "blank_to_none"],
    "OCRLocation__location_description": ["strip", "blank_to_none"],
    "OCRLocation__location_accuracy": ["strip", "blank_to_none", LOCATION_ACCURACY_TRANSFORM],
    "OCRLocation__district": ["strip", "blank_to_none"],
    "OCRLocation__region": ["strip", "blank_to_none"],  # region resolved in extract()
    "OCRLocation__epsg_code": [EPSG_CODE_DEFAULT],
    # OCRObservationDetail
    "OCRObservationDetail__comments": ["strip", "blank_to_none"],
    # OCRIdentification
    "OCRIdentification__barcode_number": ["strip", "blank_to_none"],
    "OCRIdentification__id_confirmed_by": ["strip", "blank_to_none"],
    "OCRIdentification__identification_comment": ["strip", "blank_to_none"],
    # OCRAnimalObservation fields — integers
    "OCRAnimalObservation__distinctive_feature": ["strip", "blank_to_none"],
    "OCRAnimalObservation__animal_observation_detail_comment": ["strip", "blank_to_none"],
    "OCRAnimalObservation__count_status": ["strip", "blank_to_none"],
    "OCRAnimalObservation__alive_adult_male": ["strip", "blank_to_none", "to_int"],
    "OCRAnimalObservation__dead_adult_male": ["strip", "blank_to_none", "to_int"],
    "OCRAnimalObservation__alive_adult_female": ["strip", "blank_to_none", "to_int"],
    "OCRAnimalObservation__dead_adult_female": ["strip", "blank_to_none", "to_int"],
    "OCRAnimalObservation__alive_adult_unknown": ["strip", "blank_to_none", "to_int"],
    "OCRAnimalObservation__dead_adult_unknown": ["strip", "blank_to_none", "to_int"],
    "OCRAnimalObservation__alive_juvenile_male": ["strip", "blank_to_none", "to_int"],
    "OCRAnimalObservation__dead_juvenile_male": ["strip", "blank_to_none", "to_int"],
    "OCRAnimalObservation__alive_juvenile_female": ["strip", "blank_to_none", "to_int"],
    "OCRAnimalObservation__dead_juvenile_female": ["strip", "blank_to_none", "to_int"],
    "OCRAnimalObservation__alive_juvenile_unknown": ["strip", "blank_to_none", "to_int"],
    "OCRAnimalObservation__dead_juvenile_unknown": ["strip", "blank_to_none", "to_int"],
    "OCRAnimalObservation__simple_alive": ["strip", "blank_to_none", "to_int"],
    "OCRAnimalObservation__simple_dead": ["strip", "blank_to_none", "to_int"],
    # OCRFireHistory
    "OCRFireHistory__comment": ["strip", "blank_to_none"],
    # OCRAssociatedSpecies
    "OCRAssociatedSpecies__comment": ["strip", "blank_to_none"],
    # Geometry
    "OccurrenceReportGeometry__geometry": [GEOMETRY_FROM_COORDS],
    "OccurrenceReportGeometry__locked": [GEOMETRY_LOCKED_DEFAULT],
    "OccurrenceReportGeometry__show_on_map": [static_value_factory(True)],  # Task 12781
}


class OccurrenceReportTfaunaAdapter(SourceAdapter):
    source_key = Source.TFAUNA.value
    domain = "occurrence_report"

    def extract(self, path: str, **options) -> ExtractionResult:
        rows: list[dict] = []
        warnings: list[ExtractionWarning] = []

        raw_rows, read_warnings = self.read_table(path)
        warnings.extend(read_warnings)

        # Counter for sequential ocr_for_occ_name per SpCode
        sp_code_counters: dict[str, int] = defaultdict(int)

        for raw in raw_rows:
            canonical = schema.map_raw_row(raw)

            # ── Core fields ─────────────────────────────────────
            canonical["group_type_id"] = get_group_type_id(GroupType.GROUP_TYPE_FAUNA)
            canonical["processing_status"] = "ACCEPTED"  # pipeline will map
            canonical["internal_application"] = True

            # ── species_id: use NameId (taxon_name_id) not SpCode ──
            name_id = (raw.get("NameId") or "").strip()
            if name_id:
                canonical["species_id"] = name_id

            # ── district: resolve DistrictNo → District FK ─────
            district_no = (raw.get("DistrictNo") or "").strip()
            if district_no:
                district_map = _get_district_no_map()
                district_pk = district_map.get(district_no)
                if district_pk:
                    canonical["OCRLocation__district"] = district_pk
                    # Also resolve region from district
                    region_map = _get_district_to_region_map()
                    region_pk = region_map.get(district_pk)
                    if region_pk:
                        canonical["OCRLocation__region"] = region_pk

            # ── migrated_from_id prefix ─────────────────────────
            mid = canonical.get("migrated_from_id")
            if mid and not str(mid).startswith("tfauna-"):
                canonical["migrated_from_id"] = f"tfauna-{mid}"

            # ── ocr_for_occ_name: "SpCode ###" sequential ──────
            sp_code = raw.get("SpCode", "").strip()
            if sp_code:
                sp_code_counters[sp_code] += 1
                canonical["ocr_for_occ_name"] = f"{sp_code} {sp_code_counters[sp_code]:03d}"

            # ── comments = Comments + "Tenure: " + TenCode ─────
            parts: list[str] = []
            comments_val = (raw.get("Comments") or "").strip()
            if comments_val:
                parts.append(comments_val)
            ten_code = (raw.get("TenCode") or "").strip()
            if ten_code:
                parts.append(f"Tenure: {ten_code}")
            canonical["comments"] = "; ".join(parts) if parts else ""

            # ── record_source = ReportTitle + "Author: " + Author
            src_parts: list[str] = []
            report_title = (raw.get("ReportTitle") or "").strip()
            if report_title:
                src_parts.append(report_title)
            author = (raw.get("Author") or "").strip()
            if author:
                src_parts.append(f"Author: {author}")
            canonical["record_source"] = "; ".join(src_parts) if src_parts else None

            # ── approved_by = ChName (if present), else EnName ──
            ch_name = (raw.get("ChName") or "").strip()
            en_name = (raw.get("EnName") or "").strip()
            canonical["approved_by"] = ch_name if ch_name else en_name

            # ── submitter information (EnName) ──────────────────
            canonical["SubmitterInformation__email_user"] = en_name if en_name else None
            canonical["SubmitterInformation__name"] = en_name if en_name else None

            # ── reported_date = lodgement_date (EnDate) ─────────
            canonical["reported_date"] = canonical.get("lodgement_date")

            # ── Observer contact: Address + Phone ───────────────
            addr = (raw.get("Address") or "").strip()
            phone = (raw.get("Phone") or "").strip()
            contact_parts: list[str] = []
            if addr:
                contact_parts.append(f"Address: {addr}")
            if phone:
                contact_parts.append(f"Phone: {phone}")
            canonical["OCRObserverDetail__contact"] = ". ".join(contact_parts) if contact_parts else None
            canonical["OCRObserverDetail__main_observer"] = True

            # ── OCRObservationDetail comments = ObservMethod ────
            observ_method = (raw.get("ObservMethod") or "").strip()
            canonical["OCRObservationDetail__comments"] = observ_method or None

            # ── Dead / alive count split ────────────────────────
            observ_type = (raw.get("ObservType") or "").strip()
            is_dead = _is_dead(observ_type)
            prefix = "dead" if is_dead else "alive"

            # Detailed counts
            for csv_col, age_sex in (
                ("AdultM", "adult_male"),
                ("AdultF", "adult_female"),
                ("AdultU", "adult_unknown"),
                ("JuvM", "juvenile_male"),
                ("JuvF", "juvenile_female"),
                ("JuvU", "juvenile_unknown"),
            ):
                val = (raw.get(csv_col) or "").strip()
                if val:
                    canonical[f"OCRAnimalObservation__{prefix}_{age_sex}"] = val

            # Simple count: NumSeen
            num_seen = (raw.get("NumSeen") or "").strip()
            if num_seen:
                canonical[f"OCRAnimalObservation__simple_{prefix}"] = num_seen

            # obs_date for animal observation = observation_date (date-only)
            raw_obs_date = canonical.get("observation_date") or ""
            date_only = raw_obs_date.split(" ")[0].strip() if raw_obs_date else None
            canonical["OCRAnimalObservation__obs_date"] = date_only if date_only else None

            # count_status derivation
            canonical["OCRAnimalObservation__count_status"] = _derive_count_status(canonical)

            # ── Associated species: Sp1–Sp6 concatenation ──────
            assoc_parts: list[str] = []
            for i in range(1, 7):
                sp = (raw.get(f"Sp{i}") or "").strip()
                if sp:
                    assoc_parts.append(sp)
            canonical["OCRAssociatedSpecies__comment"] = "; ".join(assoc_parts) if assoc_parts else None

            # ── Document description flags ──────────────────────
            doc_types: list[str] = []
            for col, label in (
                ("Map", "Map"),
                ("MudMap", "Mud Map"),
                ("Photo", "Photo"),
                ("Notes", "Notes"),
            ):
                val = (raw.get(col) or "").strip()
                if val.upper() == "Y":
                    doc_types.append(label)
            if doc_types:
                canonical["temp_document_description"] = "File is in S&C SharePoint Library - Fauna: " + ", ".join(
                    doc_types
                )

            # ── UserAction raw fields (for handler) ─────────────
            # Preserve ChName/ChDate for OccurrenceReportUserAction
            canonical["ChName"] = ch_name if ch_name else None
            ch_date = (raw.get("ChDate") or "").strip()
            canonical["ChDate"] = ch_date if ch_date else None

            # ── Habitat notes: Landform + VegType ───────────────
            hab_parts: list[str] = []
            landform = (raw.get("Landform") or "").strip()
            if landform:
                hab_parts.append(f"Landform: {landform}")
            veg_type = (raw.get("VegType") or "").strip()
            if veg_type:
                hab_parts.append(f"Vegetation Type: {veg_type}")
            if hab_parts:
                canonical["OCRHabitatComposition__habitat_notes"] = "; ".join(hab_parts)

            # ── Geometry: ensure keys present for pipeline ──────
            if canonical.get("Lat") or canonical.get("Long"):
                canonical["OccurrenceReportGeometry__geometry"] = None
                canonical["OccurrenceReportGeometry__locked"] = True
                canonical["OccurrenceReportGeometry__show_on_map"] = True

            rows.append(canonical)

        return ExtractionResult(rows=rows, warnings=warnings)


# Attach pipelines
OccurrenceReportTfaunaAdapter.PIPELINES = PIPELINES
