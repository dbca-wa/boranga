from boranga.components.data_migration.mappings import get_group_type_id
from boranga.components.data_migration.registry import (
    _result,
    build_legacy_map_transform,
    dependent_from_column_factory,
    fk_lookup_static,
    static_value_factory,
)
from boranga.components.users.models import SubmitterCategory

from ..base import ExtractionResult, SourceAdapter
from ..sources import Source
from . import schema
from .tec_shared import TEC_USER_LOOKUP

# Lookup submitter category by name (not hardcoded ID)
SUBMITTER_CATEGORY_DBCA = fk_lookup_static(
    model=SubmitterCategory,
    lookup_field="name",
    static_value="DBCA",
)

# Task 12499: Map SV_OBSERVATION_TYPE codes (Q, R, T) to SpeciesListRelatesTo via legacy map
SPECIES_LIST_RELATES_TO_TRANSFORM = build_legacy_map_transform(
    legacy_system="TEC",
    list_name="SV_OBSERVATION_TYPE (SITE_VISITS)",
    required=False,
    return_type="id",
)


# Shared cache for parent Occurrence objects to avoid redundant queries
_PARENT_OCC_CACHE = {}


def get_parent_occurrence(occ_mig_id):
    """
    Get parent Occurrence by migrated_from_id with caching.
    Shared by all parent OCC helper functions to minimize DB queries.
    """
    if not occ_mig_id:
        return None

    global _PARENT_OCC_CACHE
    if occ_mig_id not in _PARENT_OCC_CACHE:
        try:
            from boranga.components.occurrence.models import Occurrence

            occ = Occurrence.objects.filter(migrated_from_id=occ_mig_id).first()
            _PARENT_OCC_CACHE[occ_mig_id] = occ
        except Exception:
            _PARENT_OCC_CACHE[occ_mig_id] = None

    return _PARENT_OCC_CACHE.get(occ_mig_id)


_OCC_LOCATION_CACHE = {}


def get_parent_occ_location_value(occ_mig_id, field_name):
    if not occ_mig_id:
        return None

    global _OCC_LOCATION_CACHE
    if occ_mig_id not in _OCC_LOCATION_CACHE:
        occ = get_parent_occurrence(occ_mig_id)
        if occ and hasattr(occ, "location"):
            _OCC_LOCATION_CACHE[occ_mig_id] = occ.location
        else:
            _OCC_LOCATION_CACHE[occ_mig_id] = None

    loc = _OCC_LOCATION_CACHE.get(occ_mig_id)
    if not loc:
        return None

    return getattr(loc, field_name, None)


_OCC_IDENTIFICATION_CACHE = {}


def get_parent_occ_identification_value(occ_mig_id, field_name):
    if not occ_mig_id:
        return None

    global _OCC_IDENTIFICATION_CACHE
    if occ_mig_id not in _OCC_IDENTIFICATION_CACHE:
        occ = get_parent_occurrence(occ_mig_id)
        if occ and hasattr(occ, "identification"):
            _OCC_IDENTIFICATION_CACHE[occ_mig_id] = occ.identification
        else:
            _OCC_IDENTIFICATION_CACHE[occ_mig_id] = None

    identification = _OCC_IDENTIFICATION_CACHE.get(occ_mig_id)
    if not identification:
        return None

    return getattr(identification, field_name, None)


def make_geometry(lat, lon):
    if not lat or not lon:
        return None
    try:
        from django.contrib.gis.geos import Point

        p = Point(float(lon), float(lat), srid=4283)  # GDA94
        p.transform(4326)  # Convert to WGS84
        return p
    except Exception:
        return None


_OCR_CONTENT_TYPE_ID = None


def get_occurrence_report_content_type_id():
    global _OCR_CONTENT_TYPE_ID
    if _OCR_CONTENT_TYPE_ID is None:
        try:
            from django.contrib.contenttypes.models import ContentType

            from boranga.components.occurrence.models import OccurrenceReport

            _OCR_CONTENT_TYPE_ID = ContentType.objects.get_for_model(OccurrenceReport).id
        except Exception:
            pass
    return _OCR_CONTENT_TYPE_ID


class OccurrenceReportTecSiteVisitsAdapter(SourceAdapter):
    source_key = Source.TEC_SITE_VISITS.value
    domain = "occurrence_report"

    PIPELINES = {
        "internal_application": [static_value_factory(True)],
        "submitter": [TEC_USER_LOOKUP],  # TEC_USER_LOOKUP has built-in fallback
        # Copy submitter to other user fields
        "assigned_approver_id": [dependent_from_column_factory("submitter", mapping=TEC_USER_LOOKUP)],
        "assigned_officer_id": [dependent_from_column_factory("submitter", mapping=TEC_USER_LOOKUP)],
        "approved_by": [dependent_from_column_factory("submitter", mapping=TEC_USER_LOOKUP)],
        # Also populate SubmitterInformation with the same user
        "SubmitterInformation__email_user": [dependent_from_column_factory("submitter", mapping=TEC_USER_LOOKUP)],
        "processing_status": [lambda val, ctx: _result("approved") if not val else _result(val)],
        "customer_status": [
            dependent_from_column_factory(
                "processing_status",
                mapper=lambda val, ctx: "approved" if val == "approved" else None,
            )
        ],
        # OCRObserverDetail defaults (Tasks 12333, 12334, 12336)
        "OCRObserverDetail__main_observer": [static_value_factory(True)],
        "OCRObserverDetail__visible": [static_value_factory(True)],
        # Task 12334: observer_name from SV_DESCRIBED_BY (mapped by schema, pass through)
        # SubmitterInformation defaults (Task 12570: name default "DBCA" since SITE_VISITS has no USERNAME column)
        "SubmitterInformation__name": [static_value_factory("DBCA")],
        "SubmitterInformation__submitter_category": [SUBMITTER_CATEGORY_DBCA],
        "SubmitterInformation__organisation": [static_value_factory("DBCA")],
        # OCRLocation defaults from Parent Occurrence
        "OCRLocation__coordinate_source": [
            lambda val, ctx: _result(
                get_parent_occ_location_value(ctx.row.get("Occurrence__migrated_from_id"), "coordinate_source_id")
            )
        ],
        "OCRLocation__district": [
            lambda val, ctx: _result(
                get_parent_occ_location_value(ctx.row.get("Occurrence__migrated_from_id"), "district_id")
            )
        ],
        "OCRLocation__region": [
            lambda val, ctx: _result(
                get_parent_occ_location_value(ctx.row.get("Occurrence__migrated_from_id"), "region_id")
            )
        ],
        "OCRLocation__location_description": [
            lambda val, ctx: _result(
                get_parent_occ_location_value(ctx.row.get("Occurrence__migrated_from_id"), "location_description")
            )
        ],
        # Geometry defaults
        "OccurrenceReportGeometry__locked": [static_value_factory(True)],
        "OccurrenceReportGeometry__show_on_map": [static_value_factory(True)],
        "OccurrenceReportGeometry__geometry": [
            lambda val, ctx: _result(make_geometry(ctx.row.get("GDA94LAT"), ctx.row.get("GDA94LONG")))
        ],
        "OccurrenceReportGeometry__content_type": [lambda val, ctx: _result(get_occurrence_report_content_type_id())],
        # OCRHabitatComposition transformation (Task 12472)
        "OCRHabitatComposition__habitat_notes": [
            lambda val, ctx: (
                _result(f"Vegetation Condition: {ctx.row.get('SV_VEGETATION_CONDITION')}")
                if ctx.row.get("SV_VEGETATION_CONDITION")
                else _result(None)
            )
        ],
        # OCRFireHistory transformation (Task 12495)
        "OCRFireHistory__comment": [
            lambda val, ctx: _result(
                "; ".join(
                    filter(
                        None,
                        [
                            ctx.row.get("SV_FIRE_NOTES"),
                            f"Fire Age: {ctx.row.get('SV_FIRE_AGE')}" if ctx.row.get("SV_FIRE_AGE") else None,
                        ],
                    )
                )
            )
        ],
        # Task 12499: species_list_relates_to from SV_OBSERVATION_TYPE
        "OCRAssociatedSpecies__species_list_relates_to": [SPECIES_LIST_RELATES_TO_TRANSFORM],
        # Copy identification_certainty from parent Occurrence's OCC Identification
        "OCRIdentification__identification_certainty": [
            lambda val, ctx: _result(
                get_parent_occ_identification_value(
                    ctx.row.get("Occurrence__migrated_from_id"), "identification_certainty_id"
                )
            )
        ],
    }

    def extract(self, path: str, **options) -> ExtractionResult:
        import os

        import pandas as pd

        # Get community group id safely
        community_group_id = get_group_type_id("community")

        # We are now driving from SITE_VISITS.csv (path), so we need to side-load SITES.csv
        # to get OCC_UNIQUE_ID and Spatial data.
        sites_path = path.replace("SITE_VISITS.csv", "SITES.csv")
        if not os.path.exists(sites_path):
            sites_path = os.path.join(os.path.dirname(path), "SITES.csv")

        # Cache SITES info: S_ID -> {OCC_UNIQUE_ID, ...}
        sites_lookup = {}
        if os.path.exists(sites_path):
            try:
                # Read as string to avoid type inference issues
                df = pd.read_csv(sites_path, dtype=str).fillna("")
                for _, row in df.iterrows():
                    s_id = row.get("S_ID", "").strip()
                    if s_id:
                        sites_lookup[s_id] = row.to_dict()
            except Exception:
                # In strict environment we might want to log this
                pass

        raw_rows, warnings = self.read_table(path)
        rows = []

        for raw in raw_rows:
            # Map columns from SITE_VISITS (SITE_VISIT_ID, SV_VISIT_DATE, etc)
            canonical = schema.map_raw_row(raw)
            canonical["group_type_id"] = community_group_id

            # Ensure ID is prefixed
            visit_id = raw.get("SITE_VISIT_ID")
            if visit_id:
                canonical["migrated_from_id"] = f"tec-site-{visit_id}"

            # Linking to Parent Occurrence via SITES lookup
            s_id = raw.get("S_ID")
            if s_id and s_id in sites_lookup:
                site_row = sites_lookup[s_id]

                # Get OCC_UNIQUE_ID from Site
                occ_id = site_row.get("OCC_UNIQUE_ID")
                if occ_id:
                    canonical["Occurrence__migrated_from_id"] = f"tec-{occ_id}"

                # NOTE: Geometry fields (S_LATITUDE, S_LONGITUDE) exist in site_row.
                # Map them to canonical keys expected by schema (GDA94LAT, GDA94LONG)
                if site_row.get("S_LATITUDE") and site_row.get("S_LONGITUDE"):
                    canonical["GDA94LAT"] = site_row.get("S_LATITUDE")
                    canonical["GDA94LONG"] = site_row.get("S_LONGITUDE")
                    canonical["DATUM"] = site_row.get("S_DATUM") or "GDA94"

            else:
                # If we have a visit but no matching site, we can't link it to an Occurrence
                pass

            rows.append(canonical)

        return ExtractionResult(rows=rows, warnings=warnings)
