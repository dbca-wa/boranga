import io
import logging
import os
import tempfile
import zipfile

import fiona
from django.http import HttpResponse
from django.shortcuts import get_object_or_404
from fiona.crs import CRS
from rest_framework import status, viewsets
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView
from shapely.geometry import mapping, shape

from boranga.components.spatial.models import TileLayer
from boranga.components.spatial.permissions import TileLayerPermission
from boranga.components.spatial.serializers import TileLayerSerializer
from boranga.helpers import is_contributor, is_customer, is_internal, is_referee

logger = logging.getLogger(__name__)


class GeoJsonToShapefileView(APIView):
    """Accepts a GeoJSON FeatureCollection via POST and returns a zipped shapefile."""

    permission_classes = [IsAuthenticated]

    def post(self, request, *args, **kwargs):
        geojson_data = request.data
        if not isinstance(geojson_data, dict):
            return Response(
                {"error": "Invalid JSON body"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        features = geojson_data.get("features", [])
        if not features:
            return Response(
                {"error": "No features provided"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        try:
            zip_bytes = self._geojson_to_shapefile_zip(geojson_data)
        except Exception:
            logger.exception("Failed to convert GeoJSON to shapefile")
            return Response(
                {"error": "Failed to convert GeoJSON to shapefile"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        response = HttpResponse(zip_bytes, content_type="application/zip")
        response["Content-Disposition"] = 'attachment; filename="boranga_layers.zip"'
        return response

    @staticmethod
    def _geojson_to_shapefile_zip(geojson_data):
        """Convert a GeoJSON FeatureCollection to a zipped ESRI Shapefile."""
        features = geojson_data.get("features", [])

        # Group features by geometry type (shapefiles only support one type per file)
        groups = {}
        for feat in features:
            geom = shape(feat["geometry"])
            geom_type = geom.geom_type
            # Normalise to multi-types for consistency
            if geom_type == "Polygon":
                geom_type = "Polygon"
            elif geom_type == "MultiPolygon":
                geom_type = "Polygon"
            elif geom_type == "LineString":
                geom_type = "LineString"
            elif geom_type == "MultiLineString":
                geom_type = "LineString"
            elif geom_type == "Point":
                geom_type = "Point"
            elif geom_type == "MultiPoint":
                geom_type = "Point"
            groups.setdefault(geom_type, []).append(feat)

        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            for geom_type, type_features in groups.items():
                # Determine fiona schema from features
                properties = {}
                for feat in type_features:
                    for key, value in (feat.get("properties") or {}).items():
                        if key not in properties:
                            if isinstance(value, int):
                                properties[key] = "int"
                            elif isinstance(value, float):
                                properties[key] = "float"
                            else:
                                properties[key] = "str"

                schema = {
                    "geometry": geom_type,
                    "properties": properties,
                }

                suffix = f"_{geom_type.lower()}" if len(groups) > 1 else ""
                layer_name = f"boranga_layers{suffix}"

                with tempfile.TemporaryDirectory() as tmpdir:
                    shapefile_path = f"{tmpdir}/{layer_name}.shp"
                    with fiona.open(
                        shapefile_path,
                        "w",
                        driver="ESRI Shapefile",
                        crs=CRS.from_epsg(4326),
                        schema=schema,
                    ) as shp_file:
                        for feat in type_features:
                            geom = shape(feat["geometry"])
                            props = {}
                            for key in properties:
                                props[key] = (feat.get("properties") or {}).get(key)
                            shp_file.write({"geometry": mapping(geom), "properties": props})

                    # Add all shapefile component files to the zip
                    for fname in os.listdir(tmpdir):
                        filepath = os.path.join(tmpdir, fname)
                        zf.write(filepath, fname)

        zip_buffer.seek(0)
        return zip_buffer.getvalue()


class TileLayerViewSet(viewsets.ReadOnlyModelViewSet):
    """
    A simple ModelViewSet for listing or retrieving tile layers.
    """

    queryset = TileLayer.objects.none()
    serializer_class = TileLayerSerializer
    permission_classes = [TileLayerPermission]

    def list(self, request):
        queryset = self.get_queryset()
        serializer = TileLayerSerializer(queryset, many=True)
        return Response(serializer.data)

    def retrieve(self, request, pk=None):
        queryset = TileLayer.objects.all()
        tile_layer = get_object_or_404(queryset, pk=pk)
        serializer = TileLayerSerializer(tile_layer)
        return Response(serializer.data)

    def get_queryset(self):
        if is_internal(self.request):
            return TileLayer.objects.filter(active=True, is_internal=True).order_by("id")
        elif is_contributor(self.request):
            # Internal/External Contributors are not in the INTERNAL_GROUPS so
            # is_internal() returns False for them. They still need access to
            # map base layers (satellite + streets).
            return TileLayer.objects.filter(active=True, is_external=True).order_by("id")
        elif is_referee(self.request):
            return TileLayer.objects.filter(active=True, is_external=True).order_by("id")
        elif is_customer(self.request):
            return TileLayer.objects.filter(active=True, is_external=True).order_by("id")
        else:
            return TileLayer.objects.none()
            # raise ValueError("User is not a customer, internal user, or referee.")
