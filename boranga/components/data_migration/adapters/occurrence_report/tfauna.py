"""TFAUNA OccurrenceReport adapter — migrates Fauna Records.csv into
OccurrenceReport + OCRAnimalObservation + related child models.

Source CSV: private-media/legacy_data/TFAUNA/Fauna Records.csv
"""

from __future__ import annotations

import logging
from collections import defaultdict

from django.conf import settings
from django.contrib.gis.geos import Point
from pyproj import Transformer

from boranga.components.data_migration.mappings import get_group_type_id
from boranga.components.data_migration.registry import (
    TransformContext,
    _result,
    build_legacy_map_multi_transform,
    build_legacy_map_transform,
    date_from_datetime_iso_local_factory,
    datetime_iso_factory,
    emailuser_by_legacy_username_with_fallback_factory,
    emailuser_object_by_legacy_username_with_fallback_factory,
    fk_lookup,
    fk_lookup_static,
    geometry_from_coords_factory,
    registry,
    static_value_factory,
    taxonomy_lookup_legacy_id_mapping_species,
)
from boranga.components.occurrence.models import ObservationTime, OccurrenceReport
from boranga.components.species_and_communities.models import GroupType
from boranga.components.users.models import SubmitterCategory
from boranga.settings import (
    COUNT_STATUS_COUNTED,
    COUNT_STATUS_NOT_COUNTED,
    COUNT_STATUS_SIMPLE_COUNT,
)

from ..base import ExtractionResult, ExtractionWarning, SourceAdapter
from ..sources import Source
from . import schema

logger = logging.getLogger(__name__)


# ── Species lookup: NameId (taxon_name_id) → Species PK ───────────
# Resolved via LegacyTaxonomyMapping (list_name="TFAUNA"), fauna-only.

SPECIES_FROM_NAME_ID = taxonomy_lookup_legacy_id_mapping_species("TFAUNA", group_type="fauna")

# ── Factory transforms ──────────────────────────────────────────────
# EmailUser by legacy username; unknown usernames fall back to the shared
# TFAUNA service account (boranga.tfauna@dbca.wa.gov.au) with a warning.
TFAUNA_FALLBACK_EMAIL = "boranga.tfauna@dbca.wa.gov.au"

EMAILUSER_BY_LEGACY_USERNAME = emailuser_by_legacy_username_with_fallback_factory("TFAUNA", TFAUNA_FALLBACK_EMAIL)
# EmailUser object lookup; silently returns None when the username is not a registered
# legacy mapping (e.g. when the value is a display name rather than a system username).
EMAILUSER_OBJ_BY_LEGACY_USERNAME = emailuser_object_by_legacy_username_with_fallback_factory("TFAUNA")

DATETIME_ISO_PERTH = datetime_iso_factory("Australia/Perth")

# For DateField columns: return the local (Perth) date to avoid day-shift from UTC conversion
DATE_LOCAL_PERTH = date_from_datetime_iso_local_factory("Australia/Perth")

OBSERVATION_TIME_FK_LOOKUP = fk_lookup(
    model=ObservationTime,
    lookup_field="name",
)

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

EPSG_CODE_DEFAULT = static_value_factory(settings.DEFAULT_SRID)

# Task 12762 (S&C-blocked): Resolution → LocationAccuracy mapping.
# `required=False` so that blank values → None silently, and unmapped codes
# → None with an error issue rather than raising a FK violation.
LOCATION_ACCURACY_TRANSFORM = build_legacy_map_transform(
    "TFAUNA",
    "Resolution",
    required=False,
)

# TenCode → canonical display name (e.g. "NP" → "National Park").
TEN_CODE_CANONICAL = build_legacy_map_transform(
    "TFAUNA",
    "TenCode",
    required=False,
    return_type="canonical",
)

# TenCode → OccurrenceTenurePurpose PK via TFAUNA "Purpose" legacy value map.
TEN_CODE_PURPOSE_TRANSFORM = build_legacy_map_transform(
    "TFAUNA",
    "Purpose",
    required=False,
)

LANDFORM_TRANSFORM = build_legacy_map_transform(
    "TFAUNA",
    "Landform",
    required=False,
)

VEGETATION_TYPE_TRANSFORM = build_legacy_map_transform(
    "TFAUNA",
    "VegType",
    required=False,
    return_type="canonical",
)

OBSERVATION_METHOD_TRANSFORM = build_legacy_map_transform(
    "TFAUNA",
    "ObservMethod",
    required=False,
)

SECONDARY_SIGN_TRANSFORM = build_legacy_map_multi_transform(
    "TFAUNA",
    "SecSign",
    required=False,
)

PRIMARY_DETECTION_METHOD_TRANSFORM = build_legacy_map_multi_transform(
    "TFAUNA",
    "ObservType",
    required=False,
)

REPRODUCTIVE_STATE_TRANSFORM = build_legacy_map_transform(
    "TFAUNA",
    "Breeding",
    required=False,
)

IDENTIFICATION_CERTAINTY_TRANSFORM = build_legacy_map_transform(
    "TFAUNA",
    "Certainty",
    required=False,
)

SAMPLE_TYPE_TRANSFORM = build_legacy_map_transform(
    "TFAUNA",
    "Specimen",
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

# Lazily cached fallback user name (resolved on first use).
_fallback_name_cache: list = [None, False]  # [name, loaded]


def _get_fallback_user_name() -> str | None:
    if not _fallback_name_cache[1]:
        try:
            from ledger_api_client.ledger_models import EmailUserRO

            user = EmailUserRO.objects.get(email__iexact=TFAUNA_FALLBACK_EMAIL)
            _fallback_name_cache[0] = user.get_full_name() or None
        except Exception:
            _fallback_name_cache[0] = None
        _fallback_name_cache[1] = True
    return _fallback_name_cache[0]


def submitter_name_from_emailuser(value, ctx=None):
    """Extract full name from EmailUser object.

    Falls back to:
    - The raw EnName string if the EmailUser lookup missed (display name, not a
      mapped username).
    - The fallback service-account's full name when EnName is also empty (i.e.
      the row has no submitter and EMAILUSER_BY_LEGACY_USERNAME resolved to the
      TFAUNA fallback account).
    """
    if value is None:
        if ctx and getattr(ctx, "row", None):
            en_name = (ctx.row.get("EnName") or "").strip()
            if en_name:
                return _result(en_name)
        # EnName is empty → fallback-user scenario; use that account's full name.
        return _result(_get_fallback_user_name())
    try:
        if hasattr(value, "get_full_name"):
            return _result(value.get_full_name())
        return _result(None)
    except Exception:
        return _result(None)


# ── count_status derivation ─────────────────────────────────────────


def _derive_count_status(canonical: dict) -> str:
    """Derive OCRAnimalObservation count_status from populated count fields."""
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
    detailed_count = sum(int(canonical.get(f) or 0) for f in detailed_fields if canonical.get(f) is not None)

    simple_fields = [
        "OCRAnimalObservation__simple_alive",
        "OCRAnimalObservation__simple_dead",
    ]
    has_simple = any(canonical.get(f) is not None and str(canonical.get(f)).strip() != "" for f in simple_fields)
    simple_count = sum(int(canonical.get(f) or 0) for f in simple_fields if canonical.get(f) is not None)

    if has_detailed and has_simple:
        if detailed_count > simple_count:
            return COUNT_STATUS_COUNTED
        elif simple_count > detailed_count:
            return COUNT_STATUS_SIMPLE_COUNT

    if has_detailed:
        return COUNT_STATUS_COUNTED

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
    "species_id": ["strip", "blank_to_none", SPECIES_FROM_NAME_ID],
    "processing_status": ["strip", "required", _processing_status],
    "customer_status": [_customer_status],
    "lodgement_date": ["strip", "blank_to_none", DATETIME_ISO_PERTH],
    "observation_date": ["strip", "blank_to_none", DATE_LOCAL_PERTH],
    "observation_time_id": ["strip", "blank_to_none", OBSERVATION_TIME_FK_LOOKUP],
    "datetime_updated": ["strip", "blank_to_none", DATE_LOCAL_PERTH],
    "comments": ["strip", "blank_to_none"],
    "record_source": ["strip", "blank_to_none"],
    "ocr_for_occ_name": ["strip", "blank_to_none"],
    "submitter": ["strip", "blank_to_none", EMAILUSER_BY_LEGACY_USERNAME],
    "approved_by": ["strip", "blank_to_none", EMAILUSER_BY_LEGACY_USERNAME],
    "last_modified_by": ["strip", "blank_to_none", EMAILUSER_BY_LEGACY_USERNAME],
    # SubmitterInformation
    "SubmitterInformation__submitter_category": [SUBMITTER_CATEGORY_DBCA],
    "SubmitterInformation__email_user": [
        "strip",
        "blank_to_none",
        EMAILUSER_BY_LEGACY_USERNAME,
    ],
    "SubmitterInformation__name": [
        "strip",
        "blank_to_none",
        EMAILUSER_OBJ_BY_LEGACY_USERNAME,
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
    # OCRObservationDetail
    "OCRObservationDetail__observation_method": ["strip", "blank_to_none", OBSERVATION_METHOD_TRANSFORM],
    "OCRObservationDetail__comments": ["strip", "blank_to_none"],
    # OCRIdentification
    "OCRIdentification__barcode_number": ["strip", "blank_to_none"],
    "OCRIdentification__id_confirmed_by": ["strip", "blank_to_none"],
    "OCRIdentification__identification_comment": ["strip", "blank_to_none"],
    "OCRIdentification__identification_certainty": ["strip", "blank_to_none", IDENTIFICATION_CERTAINTY_TRANSFORM],
    "OCRIdentification__sample_type": ["strip", "blank_to_none", SAMPLE_TYPE_TRANSFORM],
    # OCRHabitatComposition
    "OCRHabitatComposition__land_form": [
        "strip",
        "blank_to_none",
        LANDFORM_TRANSFORM,
    ],
    # OCRVegetationStructure
    "OCRVegetationStructure__vegetation_structure_layer_one": ["strip", "blank_to_none", VEGETATION_TYPE_TRANSFORM],
    # OCRAnimalObservation fields — integers
    "OCRAnimalObservation__secondary_sign": ["strip", "blank_to_none", SECONDARY_SIGN_TRANSFORM],
    "OCRAnimalObservation__primary_detection_method": ["strip", "blank_to_none", PRIMARY_DETECTION_METHOD_TRANSFORM],
    "OCRAnimalObservation__reproductive_state": ["strip", "blank_to_none", REPRODUCTIVE_STATE_TRANSFORM],
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
    "OCRAnimalObservation__obs_date": ["strip", "blank_to_none", "smart_date_parse"],
    # OCRFireHistory
    "OCRFireHistory__comment": ["strip", "blank_to_none"],
    # OCRAssociatedSpecies
    "OCRAssociatedSpecies__comment": ["strip", "blank_to_none"],
    # Geometry
    "OccurrenceReportGeometry__geometry": [GEOMETRY_FROM_COORDS],
    "OccurrenceReportGeometry__locked": [GEOMETRY_LOCKED_DEFAULT],
    "OccurrenceReportGeometry__show_on_map": [static_value_factory(True)],  # Task 12781
    "ChDate": ["strip", "blank_to_none"],
    "ChName": ["strip", "blank_to_none"],
    "EnDate": ["strip", "blank_to_none"],
    "EnName": ["strip", "blank_to_none"],
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

        # Build the coordinate transformer once — creating it inside the loop
        # for every row is expensive (~250k pyproj initialisation calls).
        _default_srid = settings.DEFAULT_SRID
        _source_epsg = "EPSG:4283"
        _target_epsg = f"EPSG:{_default_srid}"
        _needs_transform = _source_epsg != _target_epsg
        _transformer = None
        if _needs_transform:
            try:
                _transformer = Transformer.from_crs(_source_epsg, _target_epsg, always_xy=True)
            except Exception:
                _transformer = None

        _fauna_group_type_id = get_group_type_id(GroupType.GROUP_TYPE_FAUNA)

        for raw in raw_rows:
            canonical = schema.map_raw_row(raw)

            # ── Core fields ─────────────────────────────────────
            canonical["group_type_id"] = _fauna_group_type_id
            canonical["processing_status"] = "ACCEPTED"  # pipeline will map
            canonical["internal_application"] = True

            # ── migrated_from_id prefix ─────────────────────────
            mid = canonical.get("migrated_from_id")
            if mid and not str(mid).startswith("tfauna-"):
                canonical["migrated_from_id"] = f"tfauna-{mid}"

            # ── identification_comment: SpHeld + ". Features: " + Features ─
            sp_held = (raw.get("SpHeld") or "").strip()
            features = (raw.get("Features") or "").strip()
            if sp_held and features:
                canonical["OCRIdentification__identification_comment"] = f"{sp_held}. Features: {features}"
            elif sp_held:
                canonical["OCRIdentification__identification_comment"] = sp_held
            elif features:
                canonical["OCRIdentification__identification_comment"] = f"Features: {features}"
            else:
                canonical["OCRIdentification__identification_comment"] = None

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
                _ten_fn = registry._fns.get(TEN_CODE_CANONICAL)
                if _ten_fn is not None:
                    _ten_res = _ten_fn(ten_code, TransformContext(row=raw))
                    ten_code = _ten_res.value if _ten_res.value is not None else ten_code
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

            # ── approved_by / last_modified_by = ChName (if present), else EnName ──
            ch_name = (raw.get("ChName") or "").strip()
            en_name = (raw.get("EnName") or "").strip()
            canonical["approved_by"] = ch_name if ch_name else en_name
            canonical["last_modified_by"] = ch_name if ch_name else en_name

            # ── submitter information (EnName) ──────────────────
            canonical["SubmitterInformation__email_user"] = en_name if en_name else None
            canonical["SubmitterInformation__name"] = en_name if en_name else None

            # ── datetime_created = lodgement_date (EnDate) ─────────────────────
            canonical["datetime_created"] = canonical.get("lodgement_date")

            # ── datetime_updated = ChDate (if present), else lodgement_date (EnDate) ─────
            canonical["datetime_updated"] = raw.get("ChDate") or canonical.get("lodgement_date")

            # ── Observer contact: Address + Phone ───────────────
            addr = (raw.get("Address") or "").strip()
            phone = (raw.get("Phone") or "").strip()
            contact_parts: list[str] = []
            if addr:
                contact_parts.append(f"Address: {addr}")
            if phone:
                contact_parts.append(f"Phone: {phone}")
            canonical["OCRObserverDetail__contact"] = "\n".join(contact_parts) if contact_parts else None
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

            # ── Geometry: build Point from Lat/Long (GDA94) ──────
            if canonical.get("Lat") or canonical.get("Long"):
                try:
                    lat_val = canonical.get("Lat")
                    lon_val = canonical.get("Long")
                    if lat_val is None or lon_val is None:
                        raise ValueError("missing coordinate")
                    lat = float(str(lat_val).strip())
                    lon = float(str(lon_val).strip())

                    if _transformer is not None:
                        try:
                            lon, lat = _transformer.transform(lon, lat)
                        except Exception:
                            # Fallback to using raw coords if transform fails
                            pass

                    canonical["OccurrenceReportGeometry__geometry"] = Point(lon, lat, srid=_default_srid)
                except Exception:
                    # If anything goes wrong, fall back to leaving geometry unset
                    canonical["OccurrenceReportGeometry__geometry"] = None

                canonical["OccurrenceReportGeometry__locked"] = True
                canonical["OccurrenceReportGeometry__show_on_map"] = True

            rows.append(canonical)
            if len(rows) % 1000 == 0:
                logger.info("TFAUNA extract: %d rows processed", len(rows))

        logger.info("TFAUNA extract complete: %d rows total", len(rows))
        return ExtractionResult(rows=rows, warnings=warnings)


# Attach pipelines
OccurrenceReportTfaunaAdapter.PIPELINES = PIPELINES
