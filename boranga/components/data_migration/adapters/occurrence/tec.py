from __future__ import annotations

from boranga.components.data_migration.adapters.base import BaseAdapter
from boranga.components.data_migration.adapters.occurrence.schema import SCHEMA
from boranga.components.data_migration.registry import register_transform


@register_transform
def tec_comment_transform(row, val):
    parts = []
    if row.get("_temp_occ_other"):
        parts.append(row["_temp_occ_other"])
    if row.get("_temp_occ_data"):
        parts.append(row["_temp_occ_data"])
    if row.get("_temp_occ_original_area"):
        parts.append(f"Occurrence Original Area: {row['_temp_occ_original_area']}")
    if row.get("_temp_occ_area_accuracy"):
        parts.append(
            f"Occurrence Original Area Accuracy: {row['_temp_occ_area_accuracy']}"
        )
    if row.get("_temp_occ_beard_map_code"):
        parts.append(f"Beard Map: {row['_temp_occ_beard_map_code']}")
    if row.get("_temp_occ_beard_desc"):
        parts.append(f"Beard Description: {row['_temp_occ_beard_desc']}")
    if row.get("_temp_occ_bush_forever_site_no"):
        parts.append(
            f"Bush Forever Site Number: {row['_temp_occ_bush_forever_site_no']}"
        )

    return "; ".join(parts)


@register_transform
def tec_habitat_notes_transform(row, val):
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


@register_transform
def tec_fire_history_comment_transform(row, val):
    parts = []
    date = row.get("_temp_fire_date")
    comment = row.get("_temp_fire_comment")

    if date:
        parts.append(f"Fire Date: {date}")
    if comment:
        parts.append(comment)

    return ", ".join(parts)


@register_transform
def tec_observation_detail_comments_transform(row, val):
    if val:
        return f"Boundary Reliability: {val}"
    return val


@register_transform
def tec_site_geometry_transform(row, val):
    """
    Construct a Point geometry from S_LATITUDE_PREF and S_LONGITUDE_PREF.
    """
    lat = row.get("OccurrenceSite__latitude")
    lon = row.get("OccurrenceSite__longitude")

    if lat is not None and lon is not None:
        # Assuming WGS84 (SRID 4326) for lat/lon
        from django.contrib.gis.geos import Point

        return Point(lon, lat, srid=4326)
    return None


class OccurrenceTecAdapter(BaseAdapter):
    schema = SCHEMA
    source = "TEC"

    PIPELINES = {
        "comment": [tec_comment_transform],
        "OCCHabitatComposition__habitat_notes": [tec_habitat_notes_transform],
        "OCCFireHistory__comment": [tec_fire_history_comment_transform],
        "OCCObservationDetail__comments": [
            "{SOURCE_CHOICE}",
            tec_observation_detail_comments_transform,
        ],
        "OCCLocation__coordinate_source_id": ["{SOURCE_CHOICE}"],
        "OCCLocation__locality": ["{SOURCE_CHOICE}"],
        "AssociatedSpeciesTaxonomy__species_role_id": ["{SOURCE_CHOICE}"],
        "OccurrenceDocument__document_sub_category_id": ["{SOURCE_CHOICE}"],
        # Geometry transform for OccurrenceSite
        "OccurrenceSite__geometry": [tec_site_geometry_transform],
    }
