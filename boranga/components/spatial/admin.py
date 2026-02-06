import json
import re

import geojson
from django.contrib.gis import admin, forms
from django.contrib.gis.geos import GEOSGeometry
from django.core.cache import cache
from shapely import from_geojson
from shapely.geometry import MultiPolygon, Polygon

from boranga import settings

from .models import GeoserverUrl, PlausibilityGeometry, Proxy, TileLayer


class TileLayerModelForm(forms.ModelForm):
    class Meta:
        model = TileLayer
        fields = "__all__"
        help_texts = {
            "display_title": "The title to display in the layer switcher",
            "layer_name": "The name of the layer in Geoserver",
            "layer_title": "The title of the layer",
            "geoserver_url": "The Geoserver URL to which the layer belongs",
            "is_capabilities_url": "Whether the layer is a capabilities URL",
            "is_satellite_background": "Whether the layer is the satellite background layer (mutually exclusive with is_streets_background)",
            "is_streets_background": "Whether the layer is the streets background layer (mutually exclusive with is_satellite_background)",
            "is_external": "Whether the layer is available for external use",
            "is_internal": "Whether the layer is available for internal use",
            "visible": "Whether the layer is visible by default",
            "min_zoom": "The minimum zoom level at which the layer is visible",
            "max_zoom": "The maximum zoom level at which the layer is visible",
            "active": "Whether the layer is disabled and won't be used by the map component",
            "invert_xy": "Whether to invert the X and Y coordinates on when querying the layer",
            "is_tenure_intersects_query_layer": "Whether the layer is used for querying tenure intersects",
            "matrix_set": "The matrix set for the layer (for WMTS layers)",
            "tile_pixel_size": "The tile pixel size for the layer (for WMTS layers)",
            "service": "The service type of the layer: WMS or WMTS",
        }


class GeoserverUrlForm(forms.ModelForm):
    class Meta:
        model = GeoserverUrl
        fields = "__all__"
        help_texts = {
            "url": "The URL of the Geoserver",
            "wms_version": "The WMS version of the Geoserver",
        }


class TileLayerInline(admin.TabularInline):
    model = TileLayer
    extra = 0
    fields = (
        "display_title",
        "layer_name",
        "layer_title",
        "geoserver_url",
        "is_satellite_background",
        "is_streets_background",
        "is_external",
        "is_internal",
        "visible",
    )
    ordering = ("display_title", "layer_name")
    list_filter = (
        "is_satellite_background",
        "is_streets_background",
        "is_external",
        "is_internal",
        "visible",
    )
    list_editable = ("is_external", "is_internal", "visible")
    list_display_links = ("display_title", "layer_name")
    verbose_name = "Tile Layer for this Geoserver"
    verbose_name_plural = "Tile Layers for this Geoserver"

    def get_queryset(self, request):
        return super().get_queryset(request).select_related("geoserver_url")

    def formfield_for_foreignkey(self, db_field, request, **kwargs):
        if db_field.name == "geoserver_url":
            kwargs["queryset"] = GeoserverUrl.objects.order_by("url")
        return super().formfield_for_foreignkey(db_field, request, **kwargs)

    def formfield_for_dbfield(self, db_field, **kwargs):
        formfield = super().formfield_for_dbfield(db_field, **kwargs)
        if db_field.name == "layer_name":
            formfield.widget.attrs["size"] = 50
        return formfield

    def get_formset(self, request, obj=None, **kwargs):
        formset = super().get_formset(request, obj, **kwargs)
        formset.form.base_fields["geoserver_url"].widget.can_add_related = False
        formset.form.base_fields["geoserver_url"].widget.can_change_related = False
        formset.form.base_fields["geoserver_url"].widget.can_delete_related = False
        return formset

    def has_add_permission(self, request, ob=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return False

    def has_change_permission(self, request, obj=None):
        return False


@admin.register(GeoserverUrl)
class GeoserverUrlAdmin(admin.ModelAdmin):
    form = GeoserverUrlForm

    list_display = ("url", "wms_version")
    search_fields = ("url",)
    ordering = ("url",)

    inlines = [TileLayerInline]


@admin.register(TileLayer)
class TileLayerAdmin(admin.ModelAdmin):
    form = TileLayerModelForm

    list_display = (
        "display_title",
        "layer_name",
        "layer_title",
        "geoserver_url",
        "is_satellite_background",
        "is_streets_background",
        "is_external",
        "is_internal",
        "visible",
        "active",
        "invert_xy",
        "is_tenure_intersects_query_layer",
    )
    search_fields = ("display_title", "layer_name", "layer_title", "geoserver_url__url")
    ordering = ("display_title", "layer_name")
    list_filter = (
        "is_satellite_background",
        "is_streets_background",
        "is_external",
        "is_internal",
        "visible",
    )
    list_editable = ("is_external", "is_internal", "visible")
    list_display_links = ("display_title", "layer_name")
    fieldsets = (
        (
            None,
            {
                "fields": (
                    (
                        "display_title",
                        "layer_name",
                        "layer_title",
                    ),
                    (
                        "geoserver_url",
                        "service",
                        "is_capabilities_url",
                    ),
                    (
                        "matrix_set",
                        "tile_pixel_size",
                    ),
                    (
                        "active",
                        "visible",
                    ),
                    (
                        "is_satellite_background",
                        "is_streets_background",
                    ),
                    (
                        "is_external",
                        "is_internal",
                    ),
                    (
                        "min_zoom",
                        "max_zoom",
                    ),
                    "invert_xy",
                    "is_tenure_intersects_query_layer",
                )
            },
        ),
    )


@admin.register(Proxy)
class ProxyAdmin(admin.ModelAdmin):
    list_display = ("request_path", "username", "basic_auth_enabled", "active")
    search_fields = ("request_path",)
    ordering = ("request_path",)
    fieldsets = (
        (
            None,
            {
                "fields": (
                    "request_path",
                    "proxy_url",
                    "basic_auth_enabled",
                    "username",
                    "password",
                    "active",
                )
            },
        ),
    )


class GeometryField(forms.GeometryField):
    widget = forms.OSMWidget(
        attrs={
            "display_raw": True,
            "map_width": 800,
            "map_srid": 4326,
            "map_height": 600,
            "default_lat": -31.9502682,
            "default_lon": 115.8590241,
        }
    )


class PlausibilityGeometryForm(forms.ModelForm):
    geometry = GeometryField()

    class Meta:
        model = PlausibilityGeometry
        fields = "__all__"
        help_texts = {
            "active": "Whether this plausibility check geometry is active",
            "average_area": "Average area [m²] of a tenure area in this geometry",
            "ratio_effective_area": "Ratio of effective tenure area to total area of drawn polygon, i.e. total area minus roads, ...",
            "warning_value": "Number of potential tenure areas at which to issue a warning before finishing a geometry",
            "error_value": "Number of potential tenure areas at which to reject the geometry and don't finish it",
            "check_for_geometry": "The geometry model this plausibility check applies to",
            "geometry": "The geometry for this plausibility check",
        }

    def clean(self):
        if "geometry" in self.changed_data:
            geometry = self.data.get("geometry")

            geo_json = geojson.loads(geometry)
            srid = 3857
            crs_name = geo_json.get("crs", {}).get("properties", {}).get("name", f"EPSG:{srid}")
            res = re.search(r"EPSG::(\d+)", crs_name)
            if res:
                srid = res.groups(1)[0]
                srid = int(srid)

            geom_shape = from_geojson(geometry)

            geosgeom = GEOSGeometry(geom_shape.wkt, srid=srid)
            geosgeom.transform(3857)

            self.data = self.data.copy()

            if geosgeom.geom_type in ["GeometryCollection"]:
                # Handling GeometryCollections is painful, so we just extract the polygons into a MultiPolygon
                geo_json = json.loads(geosgeom.geojson)
                polygons = [Polygon(p["coordinates"][0]) for p in geo_json["geometries"] if p["type"] in ["Polygon"]]
                multi_polygon = MultiPolygon(polygons)
                geosgeom = GEOSGeometry(multi_polygon.wkt)

            geo_json = geojson.loads(geosgeom.json)
            self.data["geometry"] = json.dumps(geo_json)

        cleaned_data = super().clean()
        geometry = cleaned_data.get("geometry")

        return cleaned_data

    def save(self, commit=True):
        instance = super().save(commit=False)
        instance.geometry = self.cleaned_data["geometry"]
        if commit:
            instance.save()

        cache_key_old = None
        check_for_geometry_old = self.initial.get("check_for_geometry", None)
        if check_for_geometry_old:
            cache_key_old = settings.CACHE_KEY_PLAUSIBILITY_GEOMETRY.format(
                **{"geometry_model": self.initial.get("check_for_geometry", None)}
            )
        cache_key = settings.CACHE_KEY_PLAUSIBILITY_GEOMETRY.format(**{"geometry_model": instance.check_for_geometry})
        cache.delete(cache_key)
        if cache_key_old and cache_key_old != cache_key:
            cache.delete(cache_key_old)

        return instance


@admin.register(PlausibilityGeometry)
class PlausibilityGeometryAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "check_for_geometry",
        "average_area",
        "warning_value",
        "error_value",
        "active",
    )
    form = PlausibilityGeometryForm
    fieldsets = (
        (
            None,
            {
                "fields": (
                    "active",
                    (
                        "average_area",
                        "ratio_effective_area",
                    ),
                    (
                        "warning_value",
                        "error_value",
                    ),
                    "check_for_geometry",
                    "geometry",
                )
            },
        ),
    )
