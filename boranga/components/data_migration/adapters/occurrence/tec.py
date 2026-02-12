from __future__ import annotations

import os
from collections import defaultdict

from boranga.components.data_migration.adapters.base import (
    ExtractionResult,
    ExtractionWarning,
    SourceAdapter,
)
from boranga.components.data_migration.adapters.occurrence.schema import SCHEMA
from boranga.components.data_migration.mappings import get_group_type_id
from boranga.components.data_migration.registry import emailuser_by_legacy_username_factory
from boranga.components.species_and_communities.models import GroupType


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


def tec_fire_history_comment_transform(val, ctx):
    row = ctx.row
    parts = []

    # Handle nested fire history
    nested = row.get("_nested_fire_history", [])
    for item in nested:
        date = item.get("FIRE_DATE")
        comment = item.get("FIRE_COMMENT")
        p = []
        if date:
            p.append(f"Date: {date}")
        if comment:
            p.append(comment)
        if p:
            parts.append(" - ".join(p))

    return "; ".join(parts)


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
            # Assuming WGS84 (SRID 4326) for lat/lon
            from django.contrib.gis.geos import Point

            return Point(lon, lat, srid=4326)
        except (ValueError, TypeError):
            pass
    return None


_sub_category_cache = {}


def tec_document_sub_category_transform(val, ctx):
    if not val:
        return None
    val = str(val).strip()
    if not val:
        return None

    if not _sub_category_cache:
        from boranga.components.species_and_communities.models import (
            DocumentSubCategory,
        )

        for obj in DocumentSubCategory.objects.all():
            _sub_category_cache[obj.document_sub_category_name.lower()] = obj.id

    return _sub_category_cache.get(val.lower())


def val_to_none(val, ctx):
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
    # TODO: Implement lookups for these fields
    "OCCLocation__coordinate_source_id": [val_to_none],
    "OCCLocation__locality": [
        tec_location_locality_transform,
        # lambda v, ctx: v if v else "(Not specified)",
    ],
    "OCCLocation__location_description": ["strip"],
    "OCCLocation__boundary_description": ["strip"],
    "AssociatedSpeciesTaxonomy__species_role_id": [val_to_none],
    "OccurrenceDocument__document_sub_category_id": [tec_document_sub_category_transform],
    "OccurrenceDocument__uploaded_by": [
        "strip",
        "blank_to_none",
        emailuser_by_legacy_username_factory("TEC"),
        default_to_tec_user,
    ],
    # OCCIdentification - identification_certainty is pre-resolved by S&C in the CSV
    "OCCIdentification__identification_certainty_id": ["strip", "blank_to_none"],
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
    "OccurrenceSite__updated_date": ["blank_to_none"],
    "OccurrenceSite__drawn_by": [
        "strip",
        "blank_to_none",
        emailuser_by_legacy_username_factory("TEC"),
        default_to_tec_user,
    ],
    # Pass-through fields
    "migrated_from_id": [],
    "processing_status": [],
    "species_id": [val_to_none],
    "wild_status_id": [],
    "datetime_created": ["blank_to_none"],
    "datetime_updated": ["blank_to_none"],
    "modified_by": [],
    "submitter": [],
    "pop_number": [],
    "subpop_code": [],
    "OCCAssociatedSpecies__comment": [],
    "OCCHabitatComposition__water_quality": [],
    "_nested_species": [],
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
        fauna_path = None
        reliability_path = None
        dola_path = None
        species_role_path = None
        districts_path = None
        calm_districts_path = None
        calm_regions_path = None

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

            # Find OCCURRENCE_SPECIES.csv
            for name in ["OCCURRENCE_SPECIES.csv", "occurrence_species.csv"]:
                p = os.path.join(path, name)
                if os.path.exists(p):
                    species_path = p
                    break

            # Find OCCURRENCE_FAUNA.csv
            for name in ["OCCURRENCE_FAUNA.csv", "occurrence_fauna.csv"]:
                p = os.path.join(path, name)
                if os.path.exists(p):
                    fauna_path = p
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

            # Find DISTRICTS.csv (OCC_UNIQUE_ID -> DIST_CALM_DIST_CODE)
            for name in ["DISTRICTS.csv", "districts.csv"]:
                p = os.path.join(path, name)
                if os.path.exists(p):
                    districts_path = p
                    break

            # Find CALM_DISTRICTS.csv (CALM_DIST_CODE -> CALM_DIST_NAME, CALM_REG_CODE)
            for name in ["CALM_DISTRICTS.csv", "calm_districts.csv"]:
                p = os.path.join(path, name)
                if os.path.exists(p):
                    calm_districts_path = p
                    break

            # Find CALM_REGIONS.csv (CALM_REG_CODE -> CALM_REG_NAME)
            for name in ["CALM_REGIONS.csv", "calm_regions.csv"]:
                p = os.path.join(path, name)
                if os.path.exists(p):
                    calm_regions_path = p
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

            for name in ["OCCURRENCE_SPECIES.csv", "occurrence_species.csv"]:
                p = os.path.join(dirname, name)
                if os.path.exists(p):
                    species_path = p
                    break

            for name in ["OCCURRENCE_FAUNA.csv", "occurrence_fauna.csv"]:
                p = os.path.join(dirname, name)
                if os.path.exists(p):
                    fauna_path = p
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

            for name in ["DISTRICTS.csv", "districts.csv"]:
                p = os.path.join(dirname, name)
                if os.path.exists(p):
                    districts_path = p
                    break

            for name in ["CALM_DISTRICTS.csv", "calm_districts.csv"]:
                p = os.path.join(dirname, name)
                if os.path.exists(p):
                    calm_districts_path = p
                    break

            for name in ["CALM_REGIONS.csv", "calm_regions.csv"]:
                p = os.path.join(dirname, name)
                if os.path.exists(p):
                    calm_regions_path = p
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

        # Read Species
        species_rows = []
        if species_path:
            sp_options = {k: v for k, v in options.items() if k != "limit"}
            sp_options["limit"] = 0  # 0 means no limit - read all rows
            species_rows, sp_warns = self.read_table(species_path, **sp_options)
            warnings.extend(sp_warns)

        # Read Fauna
        fauna_rows = []
        if fauna_path:
            fauna_options = {k: v for k, v in options.items() if k != "limit"}
            fauna_options["limit"] = 0  # 0 means no limit - read all rows
            fauna_rows, fauna_warns = self.read_table(fauna_path, **fauna_options)
            warnings.extend(fauna_warns)

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

        # TODO Task 12177/12180: Verify district/region resolution approach
        # Original task mentions "TEC_DISTRICT_REGION table" but that doesn't exist as a single CSV.
        # Currently using DISTRICTS.csv + CALM_DISTRICTS.csv + CALM_REGIONS.csv chain.
        # This requires LegacyValueMap to be pre-populated with mappings from district/region names to PKs.
        # Data source structure:
        #   DISTRICTS.csv: OCC_UNIQUE_ID -> DIST_CALM_DIST_CODE
        #   CALM_DISTRICTS.csv: CALM_DIST_CODE -> CALM_DIST_NAME, CALM_REG_CODE
        #   CALM_REGIONS.csv: CALM_REG_CODE -> CALM_REG_NAME
        # Confirm with S&C team if this is the correct approach.
        district_by_occ = {}  # occ_id -> district_name
        region_by_occ = {}  # occ_id -> region_name
        if districts_path:
            # Build CALM lookup tables
            calm_dist_to_name = {}  # code -> name
            calm_dist_to_reg = {}  # dist_code -> reg_code
            if calm_districts_path:
                cd_rows, cd_warns = self.read_table(calm_districts_path, **options)
                warnings.extend(cd_warns)
                for r in cd_rows:
                    code = r.get("CALM_DIST_CODE", "").strip()
                    if code:
                        calm_dist_to_name[code] = r.get("CALM_DIST_NAME", "").strip()
                        calm_dist_to_reg[code] = r.get("CALM_REG_CODE", "").strip()

            calm_reg_to_name = {}
            if calm_regions_path:
                cr_rows, cr_warns = self.read_table(calm_regions_path, **options)
                warnings.extend(cr_warns)
                for r in cr_rows:
                    code = r.get("CALM_REG_CODE", "").strip()
                    if code:
                        calm_reg_to_name[code] = r.get("CALM_REG_NAME", "").strip()

            dist_options = {k: v for k, v in options.items() if k != "limit"}
            dist_options["limit"] = 0
            dist_rows, dist_warns = self.read_table(districts_path, **dist_options)
            warnings.extend(dist_warns)
            for r in dist_rows:
                occ_uid = r.get("OCC_UNIQUE_ID")
                dist_code = r.get("DIST_CALM_DIST_CODE", "").strip()
                if occ_uid and dist_code:
                    dist_name = calm_dist_to_name.get(dist_code, dist_code)
                    district_by_occ[occ_uid] = dist_name
                    reg_code = calm_dist_to_reg.get(dist_code)
                    if reg_code:
                        reg_name = calm_reg_to_name.get(reg_code, reg_code)
                        region_by_occ[occ_uid] = reg_name

        # Load legacy value maps for district/region FK resolution
        district_pk_map = {}  # district_name -> pk
        region_pk_map = {}  # region_name -> pk
        if district_by_occ or region_by_occ:
            from boranga.components.data_migration.mappings import load_legacy_to_pk_map

            district_pk_map = load_legacy_to_pk_map(legacy_system="TEC", model_name="District")
            region_pk_map = load_legacy_to_pk_map(legacy_system="TEC", model_name="Region")

        # Load identification certainty lookup for OCC_STATUS_CODE resolution
        id_certainty_map = {}  # certainty name (e.g. "High") -> pk
        try:
            from boranga.components.occurrence.models import IdentificationCertainty

            for ic in IdentificationCertainty.objects.all():
                id_certainty_map[ic.name.lower()] = ic.id
        except Exception:
            pass

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
        for row in species_rows:
            species_by_occ[row["OCC_UNIQUE_ID"]].append(row)

        for row in fauna_rows:
            # Normalize fauna fields to match SPEC_ naming expected by handler
            if "OF_TAXON_ID" in row:
                row["SPEC_TAXON_ID"] = row["OF_TAXON_ID"]
            if "OF_VOUCHER_NO" in row:
                row["SPEC_VOUCHER_NO"] = row["OF_VOUCHER_NO"]
            species_by_occ[row["OCC_UNIQUE_ID"]].append(row)

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

            # District/Region resolution
            if occ_id and occ_id in district_by_occ:
                dist_name = district_by_occ[occ_id]
                dist_pk = district_pk_map.get(dist_name)
                if dist_pk:
                    row["_resolved_district_id"] = dist_pk
            if occ_id and occ_id in region_by_occ:
                reg_name = region_by_occ[occ_id]
                reg_pk = region_pk_map.get(reg_name)
                if reg_pk:
                    row["_resolved_region_id"] = reg_pk

            # TODO Task 12225: Verify identification certainty mapping
            # Currently mapping OCC_STATUS_CODE values:
            #   "Identified" -> IdentificationCertainty "High"
            #   "Believed" -> IdentificationCertainty "Medium"
            # This mapping is assumed based on field names but needs confirmation from S&C team.
            # Confirm the complete list of OCC_STATUS_CODE values and their correct mappings.
            status_code = row.get("OCC_STATUS_CODE", "").strip()
            if status_code and id_certainty_map:
                certainty_name = None
                status_lower = status_code.lower()
                if status_lower == "identified":
                    certainty_name = "high"
                elif status_lower == "believed":
                    certainty_name = "medium"
                else:
                    # Try direct match (S&C may have already set it to High/Medium/Low)
                    certainty_name = status_lower

                if certainty_name and certainty_name in id_certainty_map:
                    row["_resolved_identification_certainty_id"] = id_certainty_map[certainty_name]

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

            # Apply resolved district/region IDs
            if row.get("_resolved_district_id"):
                canonical_row["OCCLocation__district_id"] = row["_resolved_district_id"]
            if row.get("_resolved_region_id"):
                canonical_row["OCCLocation__region_id"] = row["_resolved_region_id"]

            # Apply resolved identification certainty ID
            if row.get("_resolved_identification_certainty_id"):
                canonical_row["OCCIdentification__identification_certainty_id"] = row[
                    "_resolved_identification_certainty_id"
                ]

            joined_rows.append(canonical_row)

        return ExtractionResult(rows=joined_rows, warnings=warnings)
