from boranga.components.data_migration.mappings import get_group_type_id
from boranga.components.data_migration.registry import (
    _result,
    dependent_from_column_factory,
    static_value_factory,
)

from ..base import ExtractionResult, SourceAdapter
from ..sources import Source
from . import schema
from .tec_shared import TEC_USER_LOOKUP

_SPECIES_LIST_RELATES_TO_CACHE = {}


def lookup_species_list_relates_to(val):
    if not val:
        return None
    val = str(val).strip()
    if not val or val.lower() == "nan":
        return None

    global _SPECIES_LIST_RELATES_TO_CACHE
    if not _SPECIES_LIST_RELATES_TO_CACHE:
        try:
            from boranga.components.occurrence.models import SpeciesListRelatesTo

            for obj in SpeciesListRelatesTo.objects.all():
                _SPECIES_LIST_RELATES_TO_CACHE[str(obj.name).lower()] = obj.id
        except Exception:
            pass

    return _SPECIES_LIST_RELATES_TO_CACHE.get(val.lower())


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
        "processing_status": [lambda val, ctx: _result("Approved") if not val else _result(val)],
        "customer_status": [
            dependent_from_column_factory(
                "processing_status",
                mapper=lambda val, ctx: "Approved" if val == "Approved" else None,
            )
        ],
        # OCRObserverDetail defaults
        "OCRObserverDetail__main_observer": [static_value_factory(True)],
        "OCRObserverDetail__visible": [static_value_factory(True)],
        # SubmitterInformation defaults
        "SubmitterInformation__submitter_category": [static_value_factory(15)],  # DBCA
        "SubmitterInformation__organisation": [static_value_factory("DBCA")],
        # Geometry defaults
        "OccurrenceReportGeometry__locked": [static_value_factory(True)],
        "OccurrenceReportGeometry__show_on_map": [static_value_factory(True)],
        "OccurrenceReportGeometry__geometry": [
            lambda val, ctx: _result(make_geometry(ctx.row.get("GDA94LAT"), ctx.row.get("GDA94LONG")))
        ],
        "OccurrenceReportGeometry__content_type": [lambda val, ctx: _result(get_occurrence_report_content_type_id())],
        # OCRHabitatComposition transformation (Task 12472)
        "OCRHabitatComposition__habitat_notes": [
            lambda val, ctx: _result(f"Vegetation Condition: {ctx.row.get('SV_VEGETATION_CONDITION')}")
            if ctx.row.get("SV_VEGETATION_CONDITION")
            else _result(None)
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
        # Task 12499
        "OCRAssociatedSpecies__species_list_relates_to": [
            lambda val, ctx: _result(lookup_species_list_relates_to(val))
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
