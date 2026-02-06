import html

from rest_framework import serializers
from rest_framework_gis.serializers import GeoFeatureModelSerializer

from boranga.components.main.serializers import BaseModelSerializer
from boranga.components.spatial.models import (
    GeoserverUrl,
    PlausibilityGeometry,
    Proxy,
    TileLayer,
)


class GeoserverUrlSerializer(BaseModelSerializer):
    class Meta:
        model = GeoserverUrl
        fields = "__all__"


class TileLayerSerializer(BaseModelSerializer):
    geoserver_url = serializers.SerializerMethodField(read_only=True)

    class Meta:
        model = TileLayer
        fields = "__all__"

    def get_geoserver_url(self, obj):
        geoserver_url = GeoserverUrlSerializer(obj.geoserver_url).data.get("url", None)

        return html.unescape(geoserver_url or "")


class ProxySerializer(BaseModelSerializer):
    class Meta:
        model = Proxy
        fields = "__all__"


class PlausibilityGeometrySerializer(GeoFeatureModelSerializer):
    class Meta:
        model = PlausibilityGeometry
        geo_field = "geometry"
        fields = [
            "id",
            "check_for_geometry",
            "geometry",
            "average_area",
            "ratio_effective_area",
            "warning_value",
            "error_value",
        ]
        read_only_fields = ("id",)

    def get_srid(self, obj):
        if obj.geometry:
            return obj.geometry.srid
        else:
            return None
