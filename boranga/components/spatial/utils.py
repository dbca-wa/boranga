import base64
import json
import logging
import re
import sys
import urllib.parse
import xml.etree.ElementTree as ET
from itertools import combinations

import geojson
import numpy as np
import requests
import shapely.geometry as shp
from django.apps import apps
from django.contrib.contenttypes import models as ct_models
from django.contrib.gis.geos import GEOSGeometry, Polygon
from django.core.cache import cache
from django.core.exceptions import ValidationError
from django.db import IntegrityError, connection
from django.db.models import Q
from django.http import Http404, HttpResponse
from django.views.decorators.csrf import csrf_exempt
from rest_framework import serializers
from shapely import wkt
from shapely.ops import transform, unary_union, voronoi_diagram
from wagov_utils.components.proxy.views import proxy_view

from boranga import settings
from boranga.components.main.models import CadastreLayer
from boranga.components.occurrence.models import (
    BufferGeometry,
    OccurrenceGeometry,
    OccurrenceTenure,
)
from boranga.components.spatial.models import PlausibilityGeometry, Proxy, TileLayer
from boranga.helpers import is_internal

logger = logging.getLogger(__name__)

# Albers Equal Area projection string for Western Australia
aea_wa_string = (
    "+proj=aea +lat_1=-17.5 +lat_2=-31.5 +lat_0=0 +lon_0=121 +x_0=0 +y_0=0 "
    "+ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
)


def invert_xy_coordinates(geometries):
    geometries = [transform(lambda x, y: (y, x), wkt.loads(p.wkt)) for p in geometries]
    geometries = [GEOSGeometry.from_ewkt(p.wkt) for p in geometries]

    return geometries


def intersect_geometry_with_layer(geometry, intersect_layer, geometry_name="SHAPE", result_type="results"):
    """Query a geoserver WFS layer with a geometry and return the intersecting features as JSON."""

    if result_type not in ["hits", "results"]:
        raise serializers.ValidationError(f"Invalid result type {result_type}. Must be 'hits' or 'results'")

    geoserver_url = intersect_layer.geoserver_url
    intersect_layer_name = intersect_layer.layer_name
    invert_xy = intersect_layer.invert_xy

    # Use a separate geometry for WFS queries (which some layers expect inverted XY)
    # and keep the original geometry for local DB/PostGIS queries. Previously we
    # inverted the geometry unconditionally which caused local PostGIS intersects
    # to receive lat/lon swapped coordinates and return 0 results.
    db_geom = geometry
    wfs_geom = geometry
    if invert_xy:
        wfs_geom = invert_xy_coordinates([geometry])[0]

    if wfs_geom.geom_type in ["MultiPoint"]:
        # For some unknown silly reason, geoserver's jts cannot handle valid single-bracketed multipoint geometries,
        # e.g. MULTIPOINT (3 1, 4 1, 5 2), and rather throws unintelligible java class exceptions at me, so we
        # have to convert them to a double-bracket notation in the form of MULTIPOINT ((3 1), (4 1), (5 2)). Even
        # though both forms are (topologically) valid by OGC definition, the jts (java topology suite) library only
        # seems to except singleton lists
        # (https://www.tsusiatsoftware.net/jts/javadoc/com/vividsolutions/jts/io/WKTReader.html)
        logger.warning(f"Converting MultiPoint geometry {wfs_geom} to double-bracket notation")
        test_geom_wkt = f"MULTIPOINT ({', '.join([f'({c[0]} {c[1]})' for c in wfs_geom.coords])})"
    else:
        test_geom_wkt = wfs_geom.wkt

    wkt.loads(wfs_geom.wkt)
    params = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeName": f"{intersect_layer_name}",
        "maxFeatures": "5000",
        "srsName": "EPSG:4326",  # using the default projection for open layers and geodjango
        "outputFormat": "application/json",
        "propertyName": f"{geometry_name},CAD_OWNER_NAME,CAD_OWNER_COUNT",
        "resultType": result_type,
        "CQL_FILTER": f"INTERSECTS({geometry_name}, {test_geom_wkt})",
    }

    m = re.match(r"^\/geoproxy\/(?P<request_path>[\w-]+)/.*$", geoserver_url.url or "")
    request_path = m.groupdict().get("request_path") if m else None

    # If this layer represents the cadastre and we have a local CadastreLayer model,
    # perform the intersection locally using PostGIS instead of calling the remote WFS.
    try:
        layer_name_lower = (intersect_layer.layer_name or "").lower()
    except Exception:
        layer_name_lower = ""

    if "cadastre" in layer_name_lower:
        # Prefer the unmanaged CadastreLayer model only when the underlying table exists
        # and contains at least one row. This avoids returning empty/local stubs when an
        # ogr2ogr import hasn't been performed yet.
        try:
            # Derive schema and table name from the model's db_table (handles quoted names)
            db_table = getattr(CadastreLayer._meta, "db_table", None) or "kb_cadastre"
            parts = [p.strip('"') for p in db_table.split(".")]
            if len(parts) == 2:
                sch, tbl = parts
            else:
                sch = "public"
                tbl = parts[-1]

            logger.info(
                "intersect_geometry_with_layer: attempting local cadastre lookup for table %s",
                db_table,
            )
            # Check table exists
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema=%s AND table_name=%s)",
                    [sch, tbl],
                )
                exists = bool(cursor.fetchone()[0])

            if not exists:
                logger.error(
                    "Local cadastre table %s.%s not found; falling back to remote WFS",
                    sch,
                    tbl,
                )
                raise Exception("local-cadastre-missing")

            # Check table has at least one row. Validate identifiers before using them
            if not re.match(r"^[A-Za-z0-9_]+$", sch) or not re.match(r"^[A-Za-z0-9_]+$", tbl):
                # Unexpected/complex table name; don't attempt raw SQL, fall back
                logger.error(
                    "Local cadastre table name appears complex (%s.%s); skipping local check and falling back.",
                    sch,
                    tbl,
                )
                raise Exception("local-cadastre-complex-name")

            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema=%s AND table_name=%s)",
                    [sch, tbl],
                )
                exists = bool(cursor.fetchone()[0])

            if not exists:
                logger.error(
                    "Local cadastre table %s.%s not found; falling back to remote WFS",
                    sch,
                    tbl,
                )
                raise Exception("local-cadastre-missing")

            with connection.cursor() as cursor:
                cursor.execute(f"SELECT EXISTS (SELECT 1 FROM {sch}.{tbl} LIMIT 1)")
                # Note: The above uses simple interpolation only after validation;
                # it is safe for common schema/table names
                has_row = bool(cursor.fetchone()[0])

            if not has_row:
                logger.error(
                    "Local cadastre table %s.%s exists but is empty; falling back to remote WFS",
                    sch,
                    tbl,
                )
                raise Exception("local-cadastre-empty")

            # Ensure db_geom is in the correct SRID (CadastreLayer uses 4326)
            geom_for_query = db_geom
            if getattr(db_geom, "srid", None) and db_geom.srid != 4326:
                geom_for_query = GEOSGeometry(db_geom.wkt, srid=db_geom.srid)
                geom_for_query.transform(4326)

            qs = CadastreLayer.objects.filter(geom__intersects=geom_for_query)
            count = qs.count()
            logger.debug(
                "intersect_geometry_with_layer: local cadastre count=%s for geometry extent=%s",
                count,
                getattr(geom_for_query, "extent", None),
            )

            if result_type == "hits":
                # Return a small XML snippet compatible with existing parser that reads numberMatched
                return f'<features numberMatched="{count}" />'

            # Build a minimal GeoJSON-like structure expected by downstream code
            features = []
            for row in qs:
                try:
                    geom_json = json.loads(row.geom.json)
                except Exception:
                    geom_json = None

                gid = getattr(row, "gid", None)

                # Extract the layer name (after the colon) to use as the feature ID prefix
                # e.g., "kaartdijin-boodja-private:LGATE-001" -> "LGATE-001"
                # or "CPT_CADASTRE_SCDB:cadastre_scdb" -> "cadastre_scdb"
                layer_name_parts = intersect_layer_name.split(":")
                layer_suffix = layer_name_parts[-1] if len(layer_name_parts) > 1 else intersect_layer_name

                # Build feature ID with layer name to match WFS format
                # e.g., "LGATE-001.12345" or "cadastre_scdb.12345"
                feature_id = f"{layer_suffix}.{gid}" if gid else None

                features.append(
                    {
                        "id": feature_id,
                        "type": "Feature",
                        "properties": {
                            "CAD_OWNER_NAME": getattr(row, "cad_owner_name", None),
                            "CAD_OWNER_COUNT": getattr(row, "cad_owner_count", None),
                        },
                        "geometry": geom_json,
                    }
                )

            return {
                "type": "FeatureCollection",
                "totalFeatures": count,
                "features": features,
            }
        except Exception as e:
            logger.exception("Local cadastre intersection skipped/fallback: %s", e)

    if request_path:
        try:
            proxy = Proxy.objects.get(active=True, request_path=request_path)
        except Proxy.DoesNotExist:
            intersect = "intersect " if intersect_layer.is_tenure_intersects_query_layer else ""
            raise Http404(
                f"No active Proxy entry found for {request_path} for {intersect}layer {intersect_layer.display_title}"
            )
        else:
            url = proxy.proxy_url
            auth_name = proxy.username
            auth_password = proxy.password
            res = requests.post(url + "wfs", params=params, auth=(auth_name, auth_password))
    else:
        res = requests.post(geoserver_url.url, params=params)

    if res.reason != "OK":
        basic_error_message = f"Failed to intersect geometry with layer {intersect_layer_name}."

        if res.status_code == 401:
            raise serializers.ValidationError(
                f"Failed to intersect geometry with layer {intersect_layer_name}. "
                f"{res.status_code} {res.reason} response from {geoserver_url.url}. "
            )

        root = ET.fromstring(res._content, ET.XMLParser(encoding="utf-8"))
        namespace = root.tag.split("}")[0] + "}"
        exception = root.find(f".//{namespace}Exception")
        if exception is None:
            raise serializers.ValidationError(f"{basic_error_message}. {res.reason}: {res.text}")
        exception_code = exception.attrib.get("exceptionCode")
        exception_text = root.find(f".//{namespace}ExceptionText")

        raise serializers.ValidationError(f"{basic_error_message}. {exception_code}: {exception_text.text}")

    if result_type == "hits":
        # resultType hits returns text/xml
        return res.text

    return res.json()


def populate_occurrence_tenure_data(geometry_instance, features, request):
    # Get existing occurrence tenures for this geometry
    occurrence_tenures_before = OccurrenceTenure.objects.filter(occurrence_geometry=geometry_instance)
    # Keep a track of the occurrence tenure IDs that are created or updated
    occurrence_tenure_ids = []
    # Process each feature in the geometry
    for feature in features:
        feature_id = feature.get("id", None)
        owner_name = feature.get("properties", {}).get("CAD_OWNER_NAME", None)
        owner_count = feature.get("properties", {}).get("CAD_OWNER_COUNT", None)
        tenure_area_ewkb = feature_json_to_geosgeometry(feature).ewkb

        if not feature_id:
            logger.warning(f"Feature does not have an ID: {feature}")
            continue
        # Check if an occurrence tenure entry already exists for this feature ID
        occurrence_tenure_before = OccurrenceTenure.objects.filter(tenure_area_id=feature_id)
        created = False
        if not occurrence_tenure_before.exists():
            # No tenure entry exists yet for this occurrence geometry
            try:
                occurrence_tenure = OccurrenceTenure(
                    occurrence_geometry=geometry_instance,
                    tenure_area_id=feature_id,
                    owner_name=owner_name,
                    owner_count=owner_count,
                    tenure_area_ewkb=tenure_area_ewkb,
                )
                occurrence_tenure.save(version_user=request.user)
            except IntegrityError as e:
                logger.error(f"Error creating OccurrenceTenure: {e}")
                continue
        else:
            occurrence_tenures = OccurrenceTenure.objects.filter(tenure_area_id=feature_id).exclude(
                occurrence_geometry=None, tenure_area_id=None
            )

            occurrence_tenures_current = occurrence_tenures.filter(
                occurrence_geometry=geometry_instance,
                status=OccurrenceTenure.STATUS_CURRENT,
            )

            occurrence_tenures_historical = occurrence_tenures.filter(
                historical_occurrence=geometry_instance.occurrence.id,
                status=OccurrenceTenure.STATUS_HISTORICAL,
            )

            if occurrence_tenures_current.exists():
                occurrence_tenure = occurrence_tenures_current.first()
                occurrence_tenure.owner_name = owner_name
                occurrence_tenure.owner_count = owner_count
                occurrence_tenure.tenure_area_ewkb = tenure_area_ewkb
                occurrence_tenure.save(version_user=request.user)
            else:
                created = True
                # Restore historical tenure details to current one if applicable
                historical = occurrence_tenures_historical.order_by("-datetime_updated").first()
                occurrence_tenure = OccurrenceTenure(
                    occurrence_geometry=geometry_instance,
                    tenure_area_id=feature_id,
                    owner_name=owner_name,
                    owner_count=owner_count,
                    tenure_area_ewkb=tenure_area_ewkb,
                    purpose=historical.purpose if historical else None,
                    vesting=historical.vesting if historical else None,
                    significant_to_occurrence=(historical.significant_to_occurrence if historical else None),
                    comments=historical.comments if historical else None,
                )
                occurrence_tenure.save(version_user=request.user)

        if created:
            logger.info(f"Created OccurrenceTenure: {occurrence_tenure}")
        else:
            logger.info(f"Updated OccurrenceTenure: {occurrence_tenure}")
        # Add the occurrence tenure ID to the list
        occurrence_tenure_ids.append(occurrence_tenure.id)

    # Remaining tenures that where not handled up to this point
    remaining_tenures = occurrence_tenures_before.filter(~Q(id__in=occurrence_tenure_ids))
    for tenure_area in remaining_tenures:
        logger.info(f"Setting OccurrenceTenure {tenure_area} to historical")
        # Set the status of occurrence tenures that existed before, but were not created or updated to historical
        tenure_area.status = tenure_area.STATUS_HISTORICAL
        # Also populate the historical_ fields for back reference
        tenure_area.historical_occurrence = tenure_area.occurrence_geometry.occurrence.id
        tenure_area.historical_occurrence_geometry_ewkb = tenure_area.occurrence_geometry.geometry.ewkb
        # Remove the reference to the occurrence geometry
        tenure_area.occurrence_geometry = None
        tenure_area.save()


def save_geometry(
    request,
    instance,
    geometry_data,
    instance_fk_field_name=None,
    component="occurrence",
    app_label="boranga",
):
    """
    Save geometry data to the database for an instance of a model.
    Args:
        request: The request object
        instance: The instance of the model to save the geometry for
        geometry_data: The geometry data to save
        instance_fk_field_name: The name of the foreign key field on the geometry model
        component: The component name
        app_label: The app label
    """

    instance_model_name = instance._meta.model.__name__

    if not geometry_data:
        logger.warning(f"No {instance_model_name} geometry to save")
        return {}

    InstanceGeometry = apps.get_model(app_label=app_label, model_name=f"{instance_model_name}Geometry")
    InstanceGeometrySaveSerializer = getattr(
        sys.modules[f"boranga.components.{component}.serializers"],
        f"{instance_model_name}GeometrySaveSerializer",
    )
    if instance_fk_field_name is None:
        instance_fk_field_name = instance_model_name.lower()
    if isinstance(geometry_data, dict):
        geometry = geometry_data
    else:
        geometry = json.loads(geometry_data)

    if (
        0 == len(geometry["features"])
        and 0 == InstanceGeometry.objects.filter(**{instance_fk_field_name: instance}).count()
    ):
        return

    action = request.data.get("action", None)
    geometry_id_intersect_data = {}
    for feature in geometry.get("features"):
        supported_geometry_types = ["MultiPolygon", "Polygon", "MultiPoint", "Point"]
        geometry_type = feature.get("geometry").get("type")
        # Check if feature is of a supported type, continue if not
        if geometry_type not in supported_geometry_types:
            logger.warning(
                f"{instance_model_name}: {instance} contains a feature that is not a "
                f"{' or '.join(supported_geometry_types)}: {feature}"
            )
            continue

        logger.info(f"Processing {instance_model_name} {instance} geometry feature type: {geometry_type}")

        # Check if the feature has a buffer radius to later update or create a buffer geometry
        buffer_radius = feature.get("properties", {}).get("buffer_radius", None)
        created_from = feature.get("properties", {}).get("created_from_object", {})
        object_id = feature.get("properties", {}).get("object_id", None)
        content_type = feature.get("properties", {}).get("content_type", None)

        InstanceCopiedFrom = None
        created_from_model = None
        content_type_object = None
        if created_from:
            try:
                InstanceCopiedFrom = apps.get_model("boranga", created_from.get("model_class", None))
            except LookupError:
                pass
            except ValueError:
                pass
            else:
                created_from_model = InstanceCopiedFrom.objects.filter(id=created_from.get("model_id")).last()
                content_type_object = ct_models.ContentType.objects.get_for_model(InstanceCopiedFrom)

        opacity = feature.get("properties", {}).get("opacity", 0.5)

        geom_4326 = feature_json_to_geosgeometry(feature)

        original_geometry = feature.get("properties", {}).get("original_geometry")
        srid_original = original_geometry.get("properties", {}).get("srid", 4326)
        if not srid_original:
            raise ValidationError(f"Geometry must have an SRID set: {original_geometry.get('coordinates', [])}")

        if not original_geometry.get("type", None):
            original_geometry["type"] = geometry_type
        feature_json = {"type": "Feature", "geometry": original_geometry}
        geom_original = feature_json_to_geosgeometry(feature_json, srid_original)

        geoms = [(geom_4326, geom_original)]

        for geom in geoms:
            content_type_id = getattr(content_type_object, "id", content_type)
            object_id = getattr(created_from_model, "id", object_id)

            if not InstanceCopiedFrom:
                try:
                    content_type_model = ct_models.ContentType.objects.get(pk=content_type_id)
                except ct_models.ContentType.DoesNotExist:
                    pass
                else:
                    created_from_geometry = content_type_model.model_class().objects.get(pk=object_id)
                    logger.info(f"Created from geometry: {created_from_geometry}")

            geometry_data = {
                f"{instance_fk_field_name}_id": instance.id,
                "geometry": geom[0],
                "original_geometry_ewkb": geom[1].ewkb,
                "buffer_radius": buffer_radius,
                "object_id": object_id,
                "content_type": content_type_id,
                "opacity": opacity,
            }

            intersect_data = {}
            # Note: Hardcoded. Possibly pass in via fn parameter whether to intersect and with what
            if instance_fk_field_name == "occurrence":
                try:
                    intersect_layer = TileLayer.objects.get(is_tenure_intersects_query_layer=True)
                except TileLayer.DoesNotExist:
                    logger.info("No tenure intersects query layer specified")
                    intersect_layer = None
                except TileLayer.MultipleObjectsReturned:
                    logger.warning("Multiple tenure intersects query layers found")
                    intersect_layer = None
                else:
                    plausibility_geometries = PlausibilityGeometry.objects.filter(
                        active=True,
                        check_for_geometry="OccurrenceGeometry",
                        error_value__isnull=False,
                    ).filter(geometry__intersects=geom[0])

                    # Number of cadastre layer features matched against geom[0]
                    number_matched = None
                    # Value of matched features at which to error out
                    error_value = None
                    if plausibility_geometries.exists():
                        error_value = (
                            plausibility_geometries.order_by("-error_value")
                            .values("error_value")[0]
                            .get("error_value", None)
                        )

                        # Query the intersect layer to get the number of features that intersect with geom[0]
                        result_hits_xml = intersect_geometry_with_layer(geom[0], intersect_layer, result_type="hits")
                        # Query returns xml, so have to parse
                        root = ET.fromstring(result_hits_xml)
                        number_matched = root.attrib.get("numberMatched", None)

                    if number_matched and isinstance(number_matched, str) and number_matched.isnumeric():
                        number_matched = int(number_matched)

                    if number_matched:
                        if error_value and number_matched >= error_value:
                            logger.info(
                                f"Rejecting geometry {geom[0]}, it intersects with {number_matched} "
                                f"features from {intersect_layer.layer_name}. "
                                f"Error value: {error_value}"
                            )
                            raise serializers.ValidationError(
                                f"Geometry intersects with too many features from "
                                f"{intersect_layer.layer_name}: {number_matched}. Error value: {error_value}"
                            )

                    intersect_data = intersect_geometry_with_layer(geom[0], intersect_layer)
                    totalFeatures = intersect_data.get("totalFeatures")
                    logger.info(
                        f"Geometry {geom[0]} intersects with {totalFeatures} features from {intersect_layer.layer_name}"
                    )

            else:
                logger.info("No intersect layer specified")

            if feature.get("id"):
                logger.info(f"Updating existing {instance_model_name} geometry: {feature.get('id')} for: {instance}")
                try:
                    geometry = InstanceGeometry.objects.get(id=feature.get("id"))
                except InstanceGeometry.DoesNotExist:
                    logger.warning(f"{instance_model_name} geometry does not exist: {feature.get('id')}")
                    continue
                geometry_data["drawn_by"] = geometry.drawn_by
                geometry_data["last_updated_by"] = request.user.id
                geometry_data["locked"] = (
                    action in ["submit"] and geometry.drawn_by == request.user.id or geometry.locked
                )
                serializer = InstanceGeometrySaveSerializer(geometry, data=geometry_data)
            else:
                logger.info(f"Creating new geometry for {instance_model_name}: {instance}")

                # For fauna occurrences, limit to one geometry per occurrence
                if instance_fk_field_name == "occurrence":
                    from boranga.components.species_and_communities.models import GroupType

                    if hasattr(instance, "group_type") and instance.group_type.name == GroupType.GROUP_TYPE_FAUNA:
                        existing_count = InstanceGeometry.objects.filter(**{instance_fk_field_name: instance}).count()
                        if existing_count >= 1:
                            raise serializers.ValidationError(
                                "Fauna occurrences are limited to one geometry. "
                                "Please edit the existing geometry instead of creating a new one."
                            )

                geometry_data["drawn_by"] = request.user.id
                geometry_data["last_updated_by"] = request.user.id
                geometry_data["locked"] = action in ["submit"]
                serializer = InstanceGeometrySaveSerializer(data=geometry_data)

            serializer.is_valid(raise_exception=True)
            geometry_instance = serializer.save()
            logger.info(f"Saved {instance_model_name} geometry: {geometry_instance}")

            geometry_id_intersect_data[geometry_instance.id] = intersect_data

            if not isinstance(geometry_instance, OccurrenceGeometry):
                # Only occurrence geometries can have buffer geometries
                continue

            opacity = feature.get("properties", {}).get("buffer_opacity", 0.5)

            try:
                buffer_geometry = BufferGeometry.objects.get(buffered_from_geometry=geometry_instance)
            except BufferGeometry.DoesNotExist:
                if buffer_radius:
                    content_type_object = ct_models.ContentType.objects.get_for_model(geometry_instance)
                    # There is a buffer radius, but no buffer geometry, so create a buffer geometry
                    BufferGeometry.objects.create(
                        buffered_from_geometry=geometry_instance,
                        geometry=buffer_geos_geometry(geometry_instance.geometry, buffer_radius),
                        object_id=geometry_instance.id,
                        content_type=content_type_object,
                        opacity=opacity,
                    )
                    logger.info(f"Created buffer geometry for {instance_model_name} geometry: {geometry_instance}")
            else:
                if buffer_radius:
                    buffer_geometry.geometry = buffer_geos_geometry(geometry_instance.geometry, buffer_radius)
                    buffer_geometry.opacity = opacity
                    buffer_geometry.save()
                    logger.info(f"Updated buffer geometry for {instance_model_name} geometry: {geometry_instance}")
                else:
                    buffer_geometry.delete()
                    logger.info(f"Deleted buffer geometry for {instance_model_name} geometry: {geometry_instance}")

    # Remove any ocr geometries from the db that are no longer in the ocr_geometry that was submitted
    # Prevent deletion of polygons that are locked after status change (e.g. after submit)
    geometry_ids = list(geometry_id_intersect_data.keys())

    # Build the base queryset for geometries to potentially delete
    to_delete_qs = InstanceGeometry.objects.filter(**{instance_fk_field_name: instance}).exclude(
        Q(id__in=geometry_ids) | Q(locked=True)
    )

    # For non-occurrence geometries, respect the drawn_by constraint
    # For occurrence geometries, only users who can edit the occurrence can delete geometries
    if instance_fk_field_name != "occurrence":
        to_delete_qs = to_delete_qs.exclude(~Q(drawn_by=request.user.id))
    elif not instance.can_user_edit:
        # User cannot edit this occurrence, so restrict to only their own geometries
        to_delete_qs = to_delete_qs.exclude(~Q(drawn_by=request.user.id))

    if instance_fk_field_name == "occurrence":
        affected_tenure_ids = list(
            OccurrenceTenure.objects.filter(occurrence_geometry__in=to_delete_qs).values_list("id", flat=True)
        )

        # Delete buffer geometries for parents being deleted
        buffers_deleted = BufferGeometry.objects.filter(buffered_from_geometry__in=to_delete_qs).delete()
        if buffers_deleted[0] > 0:
            logger.info(
                f"Deleted {buffers_deleted[0]} buffer geometries for {instance_model_name} {instance} (parent removed)"
            )

    deleted_geometries = to_delete_qs.delete()
    if deleted_geometries[0] > 0:
        logger.info(f"Deleted {instance_model_name} geometries: {deleted_geometries} for {instance}")

    if instance_fk_field_name == "occurrence":
        # we save affected tenures to record the historical change
        affected_tenures = OccurrenceTenure.objects.filter(id__in=affected_tenure_ids)
        for i in affected_tenures:
            i.save(version_user=request.user)

    return geometry_id_intersect_data


def wkb_to_geojson(wkb):
    from shapely.wkt import loads

    geos_geometry = GEOSGeometry(wkb)
    shapely_geometry = loads(geos_geometry.wkt)
    geo_json = shp.mapping(shapely_geometry)
    geo_json["properties"] = {
        "srid": geos_geometry.srid,
        "crs_projected": geos_geometry.crs.projected,
    }

    return geo_json


def features_json_to_geosgeometry(features, srid=4326):
    return [feature_json_to_geosgeometry(feature, srid) for feature in features]


def feature_json_to_geosgeometry(feature, srid=4326):
    if isinstance(srid, str) and srid.isnumeric():
        srid = int(srid)
    if "geometry" in feature:
        geo_json = feature
    else:
        # Convert feature to geojson
        geo_json = shp.mapping(geojson.loads(json.dumps(feature)))

    shape = geo_json.get("geometry") if "geometry" in geo_json else geo_json
    geom_shape = shp.shape(shape)

    try:
        geos = GEOSGeometry(geom_shape.wkt, srid=srid)
        # Debug information to help trace unexpected intersection results
        try:
            logger.debug(
                "feature_json_to_geosgeometry: srid=%s type=%s bounds=%s",
                getattr(geos, "srid", None),
                getattr(geos, "geom_type", None),
                getattr(geos, "extent", None),
            )
        except Exception:
            # Ignore logging errors
            pass
        return geos
    except Exception as e:
        logger.exception("Failed to convert feature to GEOSGeometry: %s -- feature: %s", e, feature)
        raise


def transform_json_geometry(json_geom, from_srid, to_srid):
    feature_json = {"type": "Feature", "geometry": json_geom}
    geom = feature_json_to_geosgeometry(feature_json, from_srid)
    geom.transform(to_srid)

    return geom.json


def spatially_process_geometry(json_geom, operation, parameters=[], unit=None):
    geoms = features_json_to_geosgeometry(json_geom["features"])

    try:
        # Function from string
        spatial_operation = getattr(sys.modules[__name__], operation)
    except AttributeError:
        raise serializers.ValidationError(f"Spatial operation {operation} not supported")
    else:
        res_json = spatial_operation(geoms, *parameters, unit)

    return res_json


def feature_collection(geoms):
    return {
        "type": "FeatureCollection",
        "features": [{"type": "Feature", "geometry": json.loads(geom.json), "properties": {}} for geom in geoms],
    }


def projection(crs_from, crs_to):
    from pyproj import Transformer

    transformer = Transformer.from_crs(
        crs_from,
        crs_to,
        always_xy=True,
    )
    return transformer.transform


def projection_4326_to_aea_wa():
    return projection(4326, aea_wa_string)


def projection_aea_wa_to_4326():
    return projection(aea_wa_string, 4326)


def transform_geosgeometry_3857_to_4326(geometry):
    """Transforms a gis.geos GEOSGeometry from Web-Mercator to WGS-84
    This function is mainly intended to be used in an admin panel map that uses OSMWidget.
    For some reason, OSMWidget wants to save a geometry that has been edited in the admin panel
    with SRID 3857, so it needs to be transformed to SRID 4326 first.
    """

    from pyproj import Transformer

    if geometry.srid != 3857:
        # Potentially have to make this function more generic and allow for other projections as well
        return geometry

    geom_type = "points" if geometry.geom_type in ["Point", "MultiPoint"] else "polygons"
    projection_3857_to_4326 = Transformer.from_crs("EPSG:3857", "EPSG:4326")

    if geom_type == "points":
        linear_ring = [geometry.coords]
    else:
        linear_ring = geometry.exterior_ring.coords
    pnts = [shp.Point(p) for p in linear_ring]
    pnts_transformed = [projection_3857_to_4326.transform(p.x, p.y) for p in pnts]

    pnts_xy = [shp.Point(p[1], p[0]) for p in pnts_transformed]

    if geom_type == "points":
        if len(pnts_xy) == 1:
            instance_geometry = shp.Point(pnts_xy[0])
        else:
            instance_geometry = shp.MultiPoint(pnts_xy)
    else:
        # Maybe need to also check for multi-polygon here
        instance_geometry = shp.Polygon(pnts_xy)

    return instance_geometry


def polygon_points(polygon):
    return [shp.Point(p) for p in polygon.exterior.coords]


def buffer_point_m(point, distance):
    pnt_buffered = transform(projection_4326_to_aea_wa(), point).buffer(distance)
    return transform(projection_aea_wa_to_4326(), pnt_buffered)


def buffer_polygon_m(polygon, distance):
    # Transform the polygon exterior points to AEA WA
    linear_ring = polygon.exterior_ring.coords
    pnts = [shp.Point(p) for p in linear_ring]
    pnts_transformed = [transform(projection_4326_to_aea_wa(), p) for p in pnts]

    # Create a polygon from the the transformed points and buffer it
    plg_buffered = shp.Polygon(pnts_transformed).buffer(distance)

    # Transform the buffered polygon's exterior points back to 4326
    xy = plg_buffered.exterior.coords.xy
    plg_buffered_pnts = [shp.Point(p) for p in list(zip(xy[0], xy[1]))]

    return shp.Polygon([transform(projection_aea_wa_to_4326(), p) for p in plg_buffered_pnts])


def buffer_geometries(geoms, distance, unit):
    if unit == "m":
        buffered_geoms = []
        for geom in geoms:
            if geom.dims == 0:
                # A point
                pnt = shp.Point(geom)
                buffer_geom = buffer_point_m(pnt, distance)
            elif geom.dims == 1:
                raise serializers.ValidationError("Buffer operation not supported for LineString (dim-1)")
            else:
                # A polygon (dim 2), can there possibly be a dim-3 geometry?
                if geom.geom_type == "MultiPolygon":
                    buffer_geoms = []
                    for polygon in geom:
                        buffer_geoms.append(buffer_polygon_m(polygon, distance))
                    buffer_geom_mp = shp.MultiPolygon(buffer_geoms)
                    buffer_geom = unary_union(buffer_geom_mp)
                else:
                    buffer_geom = buffer_polygon_m(geom, distance)

            buffered_geoms.append(GEOSGeometry(buffer_geom.wkt))

    elif unit == "deg":
        buffered_geoms = [geom.buffer(distance) for geom in geoms]
    else:
        raise serializers.ValidationError(f"Buffer operation requires unit parameter, got {unit}")

    return json.dumps(feature_collection(buffered_geoms))


def buffer_geos_geometry(geometry, buffer_radius, unit="m"):
    # Convert Decimal to float for numpy/shapely operations
    if buffer_radius is not None:
        buffer_radius = float(buffer_radius)

    buffer_geometry_json = buffer_geometries([geometry], buffer_radius, unit)
    geosgeometries_list = features_json_to_geosgeometry(json.loads(buffer_geometry_json).get("features"))
    if len(geosgeometries_list) == 1:
        geometry_object = geosgeometries_list[0]
    else:
        geometry_object = GEOSGeometry(shp.MultiPolygon(geosgeometries_list).wkt)

    return geometry_object


def convex_hull(geoms, *args, **kwargs):
    shapely_geoms = [wkt.loads(g.wkt) for g in geoms]
    convex_hull = shp.GeometryCollection(shapely_geoms).convex_hull
    geom = GEOSGeometry(convex_hull.wkt)

    return json.dumps(feature_collection([geom]))


def intersect_geometries(geoms, *args, **kwargs):
    """Calculates the intersection of the input geometries."""

    if len(geoms) < 2:
        raise serializers.ValidationError("Intersection operation requires more than one geometry")
    if not geoms[0].intersects(geoms[1]):
        raise serializers.ValidationError("Intersection operation requires intersecting geometries")

    shapely_geoms = [wkt.loads(g.wkt) for g in geoms]
    intersection = unary_union(shp.MultiPolygon([a.intersection(b) for a, b in combinations(shapely_geoms, 2)]))

    geom = GEOSGeometry(intersection.wkt)

    return json.dumps(feature_collection([geom]))


def union_geometries(geoms, *args, **kwargs):
    """Calculates the union of the input geometries."""

    shapely_geoms = [wkt.loads(g.wkt) for g in geoms]
    mp = shp.MultiPolygon(shapely_geoms)
    unary_union_geoms = unary_union(mp)
    union_geom = GEOSGeometry(unary_union_geoms.wkt)

    return json.dumps(feature_collection([union_geom]))


def voronoi(geoms, *args, **kwargs):
    """Calculates the Voronoi diagram of the input geometries."""

    shapely_geoms = [wkt.loads(g.wkt) for g in geoms]
    mp = shp.MultiPoint(shapely_geoms)
    voronoi = voronoi_diagram(mp)
    voronoi_geom = GEOSGeometry(shp.MultiPolygon(voronoi).wkt)

    return json.dumps(feature_collection([voronoi_geom]))


def centroid(geoms, *args, **kwargs):
    """Calculates the centroid of the input geometries."""

    polygons = []
    for geom in geoms:
        shapely_geom = wkt.loads(geom.wkt)
        if geom.geom_type == "MultiPolygon":
            polygons += list(shapely_geom.geoms)
        elif geom.geom_type == "Polygon":
            polygons.append(shapely_geom)
        else:
            raise serializers.ValidationError("Centroid operation requires Polygon or MultiPolygon geometries")

    centroid = shp.MultiPolygon(polygons).centroid
    geom = GEOSGeometry(centroid.wkt)

    return json.dumps(feature_collection([geom]))


def mean_center_point(geoms):
    # the mean of the input coordinates (see: https://shapely.readthedocs.io/en/stable/reference/shapely.centroid.html)
    shapely_geoms = [wkt.loads(g.wkt) for g in geoms]
    return shp.MultiPoint(shapely_geoms).centroid


def mean_center(geoms, *args, **kwargs):
    """Calculates the mean center point of the input geometries."""

    geom = GEOSGeometry(mean_center_point(geoms).wkt)

    return json.dumps(feature_collection([geom]))


def standard_distance(geoms, *args, **kwargs):
    """Calculates the standard distance deviation, the average distance all features
    vary from the mean center point of the input geometries.
    Returns a circle with the mean center point as center and the standard distance as radius,
    indicating the compactness of the input geometries.
    """

    shapely_geoms = [wkt.loads(g.wkt) for g in geoms]
    mean = mean_center_point(geoms)
    n = len(geoms)

    X = [np.power(p.x - mean.x, 2) for p in shapely_geoms]
    Y = [np.power(p.y - mean.y, 2) for p in shapely_geoms]

    std = np.sqrt(np.sum(X) / n + np.sum(Y) / n)

    return buffer_geometries([GEOSGeometry(mean.wkt)], std, "deg")


def proxy_object(request_path):
    cache_key = settings.CACHE_KEY_PROXY_NODE_DATA.format(request_path=request_path)
    proxy_dict = cache.get(cache_key)

    if not proxy_dict:
        try:
            proxy = Proxy.objects.get(
                active=True,
                request_path=request_path,
            )
        except Proxy.DoesNotExist:
            raise
        else:
            proxy_dict = {
                "proxy_url": proxy.proxy_url,
                "basic_auth_enabled": proxy.basic_auth_enabled,
                "username": proxy.username,
                "password": proxy.password,
            }
            cache.set(cache_key, proxy_dict, settings.CACHE_TIMEOUT_24_HOURS)

    return proxy_dict


def get_proxy_cache(app_label, model_name):
    cache_key = settings.CACHE_KEY_PROXY_LAYER_DATA.format(app_label=app_label, model_name=model_name)
    proxy_cache_dumped_data = cache.get(cache_key)
    proxy_cache_array = []

    if proxy_cache_dumped_data is None:
        proxy_cache_query = apps.get_model(app_label, model_name).objects.all()

        for pr in proxy_cache_query:
            proxy_cache_array.append(
                {
                    "layer_name": pr.layer_name,
                    "cache_expiry": 300,
                    "is_external": pr.is_external,
                    "is_internal": pr.is_internal,
                }
            )

        cache.set(cache_key, proxy_cache_array, settings.CACHE_TIMEOUT_24_HOURS)
    else:
        proxy_cache_array = proxy_cache_dumped_data

    return proxy_cache_array


@csrf_exempt
def process_proxy(request, remoteurl, queryString, auth_user, auth_password):
    if not request.user.is_authenticated:
        return

    proxy_cache = None
    proxy_response = None
    proxy_response_content = None
    base64_json = {}
    query_string_remote_url = remoteurl + "?" + queryString

    cache_times_strings = get_proxy_cache("boranga", "tilelayer")
    if is_internal(request):
        cache_times_strings = [cts for cts in cache_times_strings if cts["is_internal"] is True]
    else:
        cache_times_strings = [cts for cts in cache_times_strings if cts["is_external"] is True]

    CACHE_EXPIRY = 300
    layer_allowed = False

    proxy_cache = cache.get(query_string_remote_url)

    # A dictionary of query string parameters with the keys in lowercase
    params = urllib.parse.parse_qs(queryString)
    params = {k.lower(): v for k, v in params.items()}

    if "GetMap" in params.get("request", []):
        # GetMap request for tile rendering
        layers = params.get("layers", [])
    elif "GetFeature" in params.get("request", []):
        # GetFeature request for feature querying
        layers = params.get("typename", [])
    elif "GetCapabilities" in params.get("request", []):
        # GetCapabilities request for layer information
        layers = []
        if params.get("service", [""])[0] in ["WMS", "WMTS"]:
            layer_allowed = True
    else:
        # Note: possibly add support for other request types if needed, like GetFeatureInfo
        raise Http404(f"Request {params.get('request')} not supported")

    # Layer name should include namespace
    if any(cts["layer_name"] in layers for cts in cache_times_strings):
        layer_allowed = True

    if layer_allowed is True:
        if proxy_cache is None:
            auth_details = None
            if auth_user is None and auth_password is None:
                auth_details = None
            else:
                auth_details = {"user": auth_user, "password": auth_password}
            proxy_response = proxy_view(request, remoteurl, basic_auth=auth_details)

            proxy_response_content_encoded = base64.b64encode(proxy_response.content)
            base64_json = {
                "status_code": proxy_response.status_code,
                "content_type": proxy_response.headers["content-type"],
                "content": proxy_response_content_encoded.decode("utf-8"),
                "cache_expiry": CACHE_EXPIRY,
            }
            if proxy_response.status_code == 200:
                cache.set(query_string_remote_url, json.dumps(base64_json), CACHE_EXPIRY)
            else:
                cache.set(query_string_remote_url, json.dumps(base64_json), 15)
        else:
            base64_json = json.loads(proxy_cache)
        proxy_response_content = base64.b64decode(base64_json["content"].encode())
        http_response = HttpResponse(
            proxy_response_content,
            content_type=base64_json["content_type"],
            status=base64_json["status_code"],
        )
        http_response["Django-Cache-Expiry"] = str(base64_json["cache_expiry"]) + " seconds"
        return http_response
    else:
        http_response = HttpResponse(
            "Access Denied: Layer Name not found in boranga proxy cache",
            content_type="text/html",
            status=401,
        )
        return http_response


def get_geometry_array_from_geojson(
    geojson: dict,
    cell_value: any,
    index: int,
    column_name: str,
    errors: list,
    errors_added: int,
) -> list:
    """
    Extracts the geometry array from a GeoJSON object.
    """
    if not geojson:
        return None

    features = geojson.get("features")

    geoms = []
    bbox = Polygon.from_bbox(settings.GIS_EXTENT)

    for feature in features:
        geom = feature.get("geometry")

        if not geom:
            error_message = f"Geometry not defined in {cell_value} for column {column_name}"
            errors.append(
                {
                    "row_index": index,
                    "error_type": "column",
                    "data": cell_value,
                    "error_message": error_message,
                }
            )
            errors_added += 1
            continue

        geom = GEOSGeometry(json.dumps(geom))

        if not geom.within(bbox):
            error_message = (
                f"Geomtry defined in {cell_value} for column {column_name} "
                "is not within the extent defined for the Boranga application ({})".format(settings.GIS_EXTENT)
            )
            errors.append(
                {
                    "row_index": index,
                    "error_type": "column",
                    "data": cell_value,
                    "error_message": error_message,
                }
            )
            errors_added += 1
            continue

        geoms.append(geom)

    return geoms


def validate_geometry_within_gis_extent(geometry, verbose_field_name="geometry"):
    """
    Ensure the provided geometry lies entirely within the application GIS extent.
    Expects settings.GIS_EXTENT to be (minx, miny, maxx, maxy). If GIS_EXTENT is
    not configured the check is skipped.

    Raises rest_framework.serializers.ValidationError when geometry is outside.
    """
    gis_extent = getattr(settings, "GIS_EXTENT", None)
    if not gis_extent:
        return True

    try:
        geom_extent = geometry.extent  # (xmin, ymin, xmax, ymax)
    except Exception:
        # If we cannot determine the extent, skip validation to avoid blocking saves.
        return True

    minx, miny, maxx, maxy = gis_extent
    gxmin, gymin, gxmax, gymax = geom_extent

    # Require the geometry to be completely within the configured extent.
    if gxmin < minx or gymin < miny or gxmax > maxx or gymax > maxy:
        raise serializers.ValidationError(
            f"{verbose_field_name.capitalize()}  is outside the WA boundaries. "
            f"Please select a Latitude between {miny} and {maxy} and Longitude between {minx} and {maxx}. "
            "(Remember to include the negative symbol for Latitude.)"
        )

    return True
