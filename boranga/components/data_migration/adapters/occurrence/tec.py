from __future__ import annotations

import os
from collections import defaultdict

from django.conf import settings

from boranga.components.data_migration.adapters.base import (
    ExtractionResult,
    ExtractionWarning,
    SourceAdapter,
)
from boranga.components.data_migration.adapters.occurrence.schema import SCHEMA
from boranga.components.data_migration.mappings import get_group_type_id
from boranga.components.data_migration.registry import (
    _parse_datetime_iso,
    build_legacy_map_transform,
    datetime_iso_factory,
    emailuser_by_legacy_username_factory,
    static_value_factory,
    t_smart_date_parse,
)
from boranga.components.species_and_communities.models import (
    GroupType,
)


def tec_comment_transform(val, ctx):
    row = ctx.row
    parts = []
    if row.get("_temp_occ_other"):
        parts.append(row["_temp_occ_other"])
    if row.get("_temp_occ_data"):
        parts.append(row["_temp_occ_data"])
    if row.get("_temp_occ_original_area"):
        parts.append(f"Occurrence Original Area: {row['_temp_occ_original_area']}")
    if row.get("_temp_occ_area_accuracy"):
        parts.append(f"Occurrence Original Area Accuracy: {row['_temp_occ_area_accuracy']}")
    if row.get("_temp_occ_beard_map_code"):
        parts.append(f"Beard Map: {row['_temp_occ_beard_map_code']}")
    if row.get("_temp_occ_beard_desc"):
        parts.append(f"Beard Description: {row['_temp_occ_beard_desc']}")
    if row.get("_temp_occ_bush_forever_site_no"):
        parts.append(f"Bush Forever Site Number: {row['_temp_occ_bush_forever_site_no']}")

    # Additional Data
    additional_data = row.get("_nested_additional_data", [])
    for item in additional_data:
        desc = item.get("ADD_DESC")
        if desc:
            parts.append(f"Additional Data: {desc}")

    return "; ".join(parts)


def tec_habitat_notes_transform(val, ctx):
    row = ctx.row
    parts = []
    if row.get("_temp_occ_other_attr"):
        parts.append(row["_temp_occ_other_attr"])
    if row.get("_temp_occ_land_element"):
        parts.append(row["_temp_occ_land_element"])
    if row.get("_temp_occ_drainage"):
        parts.append(f"Drainage: {row['_temp_occ_drainage']}")
    if row.get("_temp_occ_soil"):
        parts.append(f"Soil Type: {row['_temp_occ_soil']}")
    if row.get("_temp_occ_surf_geology"):
        parts.append(f"Surface Geology: {row['_temp_occ_surf_geology']}")
    if row.get("_temp_occ_classification"):
        parts.append(f"Classification System: {row['_temp_occ_classification']}")

    return "; ".join(parts)


def parse_fire_history_sort_date(value):
    """Parse a fire-history date value into a comparable date for sorting.

    Uses the registry's `t_smart_date_parse` to leverage existing parsing
    heuristics. Returns a `datetime.date` on success or `date.min` on failure
    so that entries without a date sort last when sorting descending.
    """
    from datetime import date as _date

    try:
        res = t_smart_date_parse(value, None)
        d = getattr(res, "value", None)
        if isinstance(d, _date):
            return d
    except Exception:
        pass
    return _date.min


def tec_fire_history_comment_transform(val, ctx):
    row = ctx.row
    nested = row.get("_nested_fire_history", [])

    # Build list of formatted fire history entries
    entries = []
    for item in nested:
        date = item.get("FIRE_DATE", "").strip()
        comment = item.get("FIRE_COMMENT", "").strip()
        if date:
            entry = f"Fire Date: {date}"
            if comment:
                entry += f", {comment}"
        elif comment:
            entry = f"{comment}"
        else:
            continue  # skip blank
        entries.append((parse_fire_history_sort_date(date), entry))

    # Sort by date descending (most recent first)
    entries.sort(key=lambda x: x[0], reverse=True)
    formatted = [e[1] for e in entries]
    return "; ".join(formatted)


def tec_observation_detail_comments_transform(val, ctx):
    row = ctx.row
    if row.get("_resolved_reliability"):
        return f"Boundary Reliability: {row['_resolved_reliability']}"
    if val:
        return f"Boundary Reliability: {val}"
    return val


def tec_location_locality_transform(val, ctx):
    row = ctx.row
    if row.get("_resolved_dola"):
        return row["_resolved_dola"]
    return val


def tec_site_geometry_transform(val, ctx):
    """
    Construct a Point geometry from OccurrenceSite__latitude and OccurrenceSite__longitude.
    Coordinates are in GDA94 (EPSG:4283) and are reprojected to the project default SRID
    (GDA2020/EPSG:7844) using pyproj before being stored.
    Supports being called as a pipeline transform (ctx.row) or manually (val=row).
    """
    if ctx:
        row = ctx.row
    else:
        row = val

    lat = row.get("OccurrenceSite__latitude")
    lon = row.get("OccurrenceSite__longitude")

    if lat and lon:
        try:
            lat = float(lat)
            lon = float(lon)
            from django.contrib.gis.geos import Point
            from pyproj import Transformer

            transformer = Transformer.from_crs("EPSG:4283", f"EPSG:{settings.DEFAULT_SRID}", always_xy=True)
            x, y = transformer.transform(lon, lat)
            return Point(x, y, srid=settings.DEFAULT_SRID)
        except (ValueError, TypeError):
            pass
    return None


DOCUMENT_SUB_CATEGORY_TRANSFORM = build_legacy_map_transform(
    legacy_system="TEC",
    list_name="ADD_ITEM_CODE (ITEMS)",
    required=False,
    return_type="id",
)

# Use static_value_factory for explicit None assignments
STATIC_NONE = static_value_factory(None)

# ISO datetime parser — TEC dates carry +0000 but are Perth local time
DATETIME_ISO_PERTH = datetime_iso_factory("Australia/Perth")


def tec_datetime_updated_transform(val, ctx):
    """Return OCC_DATE_EDITED parsed as datetime, falling back to OCC_DATE_ENTERED."""
    # val is already the OCC_DATE_EDITED value (may be None after blank_to_none)
    if val is not None:
        parsed = _parse_datetime_iso(val, default_tz="Australia/Perth")
        if parsed.value is not None:
            return parsed
    # Fallback: use OCC_DATE_ENTERED (mapped to datetime_created in the raw row)
    fallback = ctx.row.get("datetime_created")
    if fallback is not None:
        return _parse_datetime_iso(fallback, default_tz="Australia/Perth")
    return None


_tec_user_id_cache = None


def default_to_tec_user(val, ctx):
    global _tec_user_id_cache
    if val is not None:
        return val

    if _tec_user_id_cache is None:
        try:
            from ledger_api_client.ledger_models import EmailUserRO

            tec_user = EmailUserRO.objects.get(email="boranga.tec@dbca.wa.gov.au")
            _tec_user_id_cache = tec_user.id
        except Exception:
            pass
    return _tec_user_id_cache


# TODO Task 12225: Uses LegacyValueMap with legacy_system="TEC", list_name="OCC_STATUS_CODE (STATUS)"
# to map legacy status codes (e.g., "Identified", "Believed") to IdentificationCertainty IDs.
# Verify list_name format and that LegacyValueMap is populated with correct mappings.
IDENTIFICATION_CERTAINTY_TRANSFORM = build_legacy_map_transform(
    legacy_system="TEC",
    list_name="OCC_STATUS_CODE (STATUS)",
    required=False,
    return_type="id",
)

# Task: Map OCC_SOURCE_CODE from OCCURRENCES table to CoordinateSource via SOURCES lookup
COORDINATE_SOURCE_TRANSFORM = build_legacy_map_transform(
    legacy_system="TEC",
    list_name="OCC_SOURCE_CODE (SOURCES)",
    required=False,
    return_type="id",
)

PIPELINES = {
    "occurrence_name": ["strip", "blank_to_none", "required"],
    "community_id": ["community_id_from_legacy"],
    "comment": [tec_comment_transform],
    "OCCHabitatComposition__habitat_notes": [tec_habitat_notes_transform],
    "OCCFireHistory__comment": [tec_fire_history_comment_transform],
    "OCCObservationDetail__comments": [
        tec_observation_detail_comments_transform,
        lambda v, ctx: v if v else "",
    ],
    "OCCLocation__coordinate_source_id": [
        "strip",
        "blank_to_none",
        COORDINATE_SOURCE_TRANSFORM,
        "to_int",
    ],
    "OCCLocation__locality": [
        tec_location_locality_transform,
        # lambda v, ctx: v if v else "(Not specified)",
    ],
    "OCCLocation__location_description": ["strip"],
    "OCCLocation__boundary_description": ["strip"],
    "AssociatedSpeciesTaxonomy__species_role_id": [STATIC_NONE],
    "OccurrenceDocument__document_sub_category_id": [
        "strip",
        "blank_to_none",
        DOCUMENT_SUB_CATEGORY_TRANSFORM,
    ],
    "OccurrenceDocument__uploaded_by": [
        "strip",
        "blank_to_none",
        emailuser_by_legacy_username_factory("TEC"),
        default_to_tec_user,
    ],
    # OCCIdentification - transform OCC_STATUS_CODE to IdentificationCertainty FK via LegacyValueMap
    "OCCIdentification__identification_certainty_id": [
        "strip",
        "blank_to_none",
        IDENTIFICATION_CERTAINTY_TRANSFORM,
    ],
    # OCCVegetationStructure
    "OCCVegetationStructure__vegetation_structure_layer_one": ["strip", "blank_to_none"],
    # OCCLocation district/region - resolved via DISTRICTS.csv lookup
    "OCCLocation__district_id": [],
    "OCCLocation__region_id": [],
    # Geometry transform for OccurrenceSite
    "OccurrenceSite__geometry": [tec_site_geometry_transform],
    # Pass-through fields for OccurrenceSite
    "OccurrenceSite__comments": [],
    "OccurrenceSite__latitude": [],
    "OccurrenceSite__longitude": [],
    "OccurrenceSite__site_name": [],
    "OccurrenceSite__updated_date": ["blank_to_none", DATETIME_ISO_PERTH],
    "OccurrenceSite__drawn_by": [
        "strip",
        "blank_to_none",
        emailuser_by_legacy_username_factory("TEC"),
        default_to_tec_user,
    ],
    # Pass-through fields
    "migrated_from_id": [],
    "processing_status": [],
    "species_id": [STATIC_NONE],  # TEC is community-based, not species
    "wild_status_id": [],
    "datetime_created": ["strip", "blank_to_none", DATETIME_ISO_PERTH, "required"],
    "datetime_updated": ["strip", "blank_to_none", tec_datetime_updated_transform],
    "modified_by": [],
    "submitter": [],
    "pop_number": [],
    "subpop_code": [],
    "OCCAssociatedSpecies__comment": [],
    "OCCHabitatComposition__water_quality": [],
    "_nested_species": [],
    "OccurrenceGeometry__buffer_radius": ["strip", "blank_to_none", "to_float"],
}


class OccurrenceTecAdapter(SourceAdapter):
    schema = SCHEMA
    source = "TEC"
    PIPELINES = PIPELINES

    def extract(self, path: str, **options) -> ExtractionResult:
        tec_submitter_id = None
        try:
            from ledger_api_client.ledger_models import EmailUserRO

            tec_user = EmailUserRO.objects.get(email="boranga.tec@dbca.wa.gov.au")
            tec_submitter_id = tec_user.id
        except Exception:
            pass

        occ_path = path
        site_path = None
        fire_path = None
        additional_path = None
        species_path = None
        reliability_path = None
        dola_path = None
        species_role_path = None

        warnings = []

        if os.path.isdir(path):
            # Try to find occurrences file
            for name in ["OCCURRENCES.csv", "occurrences.csv"]:
                p = os.path.join(path, name)
                if os.path.exists(p):
                    occ_path = p
                    break
            else:
                return ExtractionResult(
                    rows=[],
                    warnings=[ExtractionWarning(f"Missing OCCURRENCES.csv in {path}")],
                )

            # Try to find sites file
            for name in ["SITES.csv", "sites.csv"]:
                p = os.path.join(path, name)
                if os.path.exists(p):
                    site_path = p
                    break

            # Find FIRE_HISTORY.csv
            for name in ["FIRE_HISTORY.csv", "fire_history.csv"]:
                p = os.path.join(path, name)
                if os.path.exists(p):
                    fire_path = p
                    break

            # Find ADDITIONAL_DATA.csv
            for name in ["ADDITIONAL_DATA.csv", "additional_data.csv"]:
                p = os.path.join(path, name)
                if os.path.exists(p):
                    additional_path = p
                    break

            # Find OCCURRENCE_SPECIES_COMBINE.csv (consolidates flora and fauna)
            for name in ["OCCURRENCE_SPECIES_COMBINE.csv", "occurrence_species_combine.csv"]:
                p = os.path.join(path, name)
                if os.path.exists(p):
                    species_path = p
                    break

            # Find RELIABILITY.csv
            for name in ["RELIABILITY.csv", "reliability.csv"]:
                p = os.path.join(path, name)
                if os.path.exists(p):
                    reliability_path = p
                    break

            # Find DOLA_LOCATIONS.csv
            for name in ["DOLA_LOCATIONS.csv", "dola_locations.csv"]:
                p = os.path.join(path, name)
                if os.path.exists(p):
                    dola_path = p
                    break

            # Find SPECIES_ROLES.csv
            for name in ["SPECIES_ROLES.csv", "species_roles.csv"]:
                p = os.path.join(path, name)
                if os.path.exists(p):
                    species_role_path = p
                    break
        else:
            # Path is the occurrences file
            dirname = os.path.dirname(path)
            for name in ["SITES.csv", "sites.csv"]:
                p = os.path.join(dirname, name)
                if os.path.exists(p):
                    site_path = p
                    break

            for name in ["FIRE_HISTORY.csv", "fire_history.csv"]:
                p = os.path.join(dirname, name)
                if os.path.exists(p):
                    fire_path = p
                    break

            for name in ["ADDITIONAL_DATA.csv", "additional_data.csv"]:
                p = os.path.join(dirname, name)
                if os.path.exists(p):
                    additional_path = p
                    break

            for name in ["OCCURRENCE_SPECIES_COMBINE.csv", "occurrence_species_combine.csv"]:
                p = os.path.join(dirname, name)
                if os.path.exists(p):
                    species_path = p
                    break

            for name in ["RELIABILITY.csv", "reliability.csv"]:
                p = os.path.join(dirname, name)
                if os.path.exists(p):
                    reliability_path = p
                    break

            for name in ["DOLA_LOCATIONS.csv", "dola_locations.csv"]:
                p = os.path.join(dirname, name)
                if os.path.exists(p):
                    dola_path = p
                    break

            for name in ["SPECIES_ROLES.csv", "species_roles.csv"]:
                p = os.path.join(dirname, name)
                if os.path.exists(p):
                    species_role_path = p
                    break

        # Read occurrences
        occ_rows, occ_warns = self.read_table(occ_path, **options)
        warnings.extend(occ_warns)

        # Read sites if found - don't apply limit to auxiliary tables
        site_rows = []
        if site_path:
            # Set limit=0 to override environment variable and read all rows
            site_options = {k: v for k, v in options.items() if k != "limit"}
            site_options["limit"] = 0  # 0 means no limit - read all rows
            site_rows, site_warns = self.read_table(site_path, **site_options)
            warnings.extend(site_warns)
        else:
            warnings.append(ExtractionWarning(f"Missing SITES.csv near {occ_path}, proceeding without sites"))

        # Read Fire History
        fire_rows = []
        if fire_path:
            fire_options = {k: v for k, v in options.items() if k != "limit"}
            fire_options["limit"] = 0  # 0 means no limit - read all rows
            fire_rows, fire_warns = self.read_table(fire_path, **fire_options)
            warnings.extend(fire_warns)

        # Read Additional Data
        additional_rows = []
        if additional_path:
            add_options = {k: v for k, v in options.items() if k != "limit"}
            add_options["limit"] = 0  # 0 means no limit - read all rows
            additional_rows, add_warns = self.read_table(additional_path, **add_options)
            warnings.extend(add_warns)

        # Read Species (OCCURRENCE_SPECIES_COMBINE contains both flora and fauna)
        species_rows = []
        if species_path:
            sp_options = {k: v for k, v in options.items() if k != "limit"}
            sp_options["limit"] = 0  # 0 means no limit - read all rows
            species_rows, sp_warns = self.read_table(species_path, **sp_options)
            warnings.extend(sp_warns)
        else:
            warnings.append(
                ExtractionWarning(f"Missing OCCURRENCE_SPECIES_COMBINE.csv near {occ_path}, proceeding without species")
            )

        # Note: OCCURRENCE_SPECIES_COMBINE.csv replaces both OCCURRENCE_SPECIES.csv and OCCURRENCE_FAUNA.csv

        # Read Lookups
        reliability_map = {}
        if reliability_path:
            rel_rows, rel_warns = self.read_table(reliability_path, **options)
            warnings.extend(rel_warns)
            for r in rel_rows:
                reliability_map[r["BR_CODE"]] = r["BR_DESC"]

        dola_map = {}
        if dola_path:
            dola_rows, dola_warns = self.read_table(dola_path, **options)
            warnings.extend(dola_warns)
            for r in dola_rows:
                dola_map[r["DOLA_REF"]] = r["DOLA_REF_DESC"]

        role_map = {}
        if species_role_path:
            role_rows, role_warns = self.read_table(species_role_path, **options)
            warnings.extend(role_warns)
            for r in role_rows:
                role_map[r["SP_ROLE_CODE"]] = r["SP_ROLE_DESC"]

        # Index auxiliary data by OCC_UNIQUE_ID
        sites_by_occ = defaultdict(list)
        for s in site_rows:
            fk = s.get("OCC_UNIQUE_ID")
            if fk:
                sites_by_occ[fk].append(s)

        fire_by_occ = defaultdict(list)
        for row in fire_rows:
            fire_by_occ[row["OCC_UNIQUE_ID"]].append(row)

        additional_by_occ = defaultdict(list)
        for row in additional_rows:
            # TODO Task 12287: Review USERNAME column disambiguation approach
            # ADDITIONAL_DATA.csv and SITES.csv both have USERNAME columns but for different purposes:
            #   - SITES.csv USERNAME -> OccurrenceSite.drawn_by
            #   - ADDITIONAL_DATA.csv USERNAME -> OccurrenceDocument.uploaded_by
            # Currently renaming ADDITIONAL_DATA USERNAME to ADD_USERNAME to avoid collision.
            # Verify this is the cleanest approach or if schema should handle this differently.
            if "USERNAME" in row and "ADD_USERNAME" not in row:
                row["ADD_USERNAME"] = row["USERNAME"]
            additional_by_occ[row["OCC_UNIQUE_ID"]].append(row)

        species_by_occ = defaultdict(list)
        seen_species_keys = set()
        for row in species_rows:
            # Map taxon_name_id (Nomos ID) to SPEC_TAXON_ID for handler compatibility
            # OCCURRENCE_SPECIES_COMBINE.csv has taxon_name_id which is the Nomos ID
            if "taxon_name_id" in row and row["taxon_name_id"]:
                row["SPEC_TAXON_ID"] = row["taxon_name_id"]
            occ_id = row.get("OCC_UNIQUE_ID")
            taxon_name_id = row.get("taxon_name_id")
            if occ_id and taxon_name_id:
                # Deduplicate by (OCC_UNIQUE_ID, taxon_name_id): keep the first record in
                # file order (data has been sorted so that the most complete record comes first).
                key = (str(occ_id), str(taxon_name_id))
                if key in seen_species_keys:
                    warnings.append(
                        ExtractionWarning(
                            f"Duplicate associated species skipped (kept first): "
                            f"OCC_UNIQUE_ID={occ_id}, taxon_name_id={taxon_name_id}"
                        )
                    )
                    continue
                seen_species_keys.add(key)
            species_by_occ[occ_id].append(row)

        # Join
        joined_rows = []
        for row in occ_rows:
            occ_id = row.get("OCC_UNIQUE_ID")

            # Sites
            sites = sites_by_occ.get(occ_id, [])
            if sites:
                row["_nested_sites"] = sites

            # Fire History
            row["_nested_fire_history"] = fire_by_occ.get(occ_id, [])

            # Additional Data
            row["_nested_additional_data"] = additional_by_occ.get(occ_id, [])

            # Species
            species_list = species_by_occ.get(occ_id, [])
            for sp in species_list:
                role_code = sp.get("SPEC_SP_ROLE_CODE") or sp.get("OF_SP_ROLE_CODE")
                if role_code and role_code in role_map:
                    sp["_resolved_role"] = role_map[role_code]
            row["_nested_species"] = species_list

            # Lookups
            br_code = row.get("OCC_BR_CODE")
            if br_code and br_code in reliability_map:
                row["_resolved_reliability"] = reliability_map[br_code]

            dola_ref = row.get("OCC_DOLA_REF")
            if dola_ref and dola_ref in dola_map:
                row["_resolved_dola"] = dola_map[dola_ref]

            # Map raw row to canonical keys
            canonical_row = SCHEMA.map_raw_row(row)
            # Preserve internal keys (starting with _)
            for k, v in row.items():
                if k.startswith("_"):
                    canonical_row[k] = v

            # Set TEC-specific defaults on canonical row
            canonical_row["group_type_id"] = get_group_type_id(GroupType.GROUP_TYPE_COMMUNITY)
            canonical_row["locked"] = True
            if not canonical_row.get("submitter") and tec_submitter_id:
                canonical_row["submitter"] = tec_submitter_id

            joined_rows.append(canonical_row)

        return ExtractionResult(rows=joined_rows, warnings=warnings)
