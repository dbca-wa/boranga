from django.contrib.gis.db import models as gis_models
from django.core.cache import cache
from django.core.validators import MaxValueValidator, MinValueValidator
from django.db import models

from boranga import settings
from boranga.components.main.models import BaseModel


class GeoserverUrl(BaseModel):
    url = models.CharField(max_length=255, unique=True)
    wms_version = models.CharField(max_length=10, default="1.3.0")

    class Meta:
        app_label = "boranga"
        ordering = ["url"]
        verbose_name = "Geoserver URL"
        verbose_name_plural = "Geoserver URLs"

    def __str__(self):
        return self.url

    @property
    def get_capabilities_url(self):
        return f"{self.url}/?SERVICE=WMS&VERSION={self.wms_version}&REQUEST=GetCapabilities"


class TileLayer(BaseModel):
    SERVICE_WMS = "wms"
    SERVICE_WMTS = "wmts"
    SERVICE_CHOICES = (
        (SERVICE_WMS, "WMS"),
        (SERVICE_WMTS, "WMTS"),
    )
    service = models.CharField(max_length=10, choices=SERVICE_CHOICES, default=SERVICE_WMS)
    geoserver_url = models.ForeignKey(
        GeoserverUrl,
        on_delete=models.CASCADE,
        null=False,
        blank=False,
        related_name="tile_layers",
    )
    is_capabilities_url = models.BooleanField(default=False)  # Whether the layer is a capabilities URL
    layer_name = models.CharField(
        max_length=255, unique=True, null=False, blank=False
    )  # Name of the layer in Geoserver
    layer_title = models.CharField(max_length=255)  # Title of the layer
    display_title = models.CharField(max_length=255)  # Title to display in the layer switcher
    is_satellite_background = models.BooleanField(
        default=False
    )  # Whether the layer is the satellite background layer (mutually exclusive with is_streets_background)
    is_streets_background = models.BooleanField(
        default=False
    )  # Whether the layer is the streets background layer (mutually exclusive with is_satellite_background)
    is_external = models.BooleanField(default=True)  # Whether the layer is available for external use
    is_internal = models.BooleanField(default=True)  # Whether the layer is available for internal use
    visible = models.BooleanField(default=False)  # Whether the layer is visible by default
    active = models.BooleanField(default=True)  # Whether the layer is disabled and won't be used by the map component
    min_zoom = models.PositiveIntegerField(
        default=0,
        validators=[MinValueValidator(0), MaxValueValidator(21)],
        null=False,
        blank=False,
    )  # Minimum zoom level at which the layer is visible
    max_zoom = models.PositiveIntegerField(
        default=21,
        validators=[MinValueValidator(0), MaxValueValidator(21)],
        null=False,
        blank=False,
    )  # Maximum zoom level at which the layer is visible
    is_tenure_intersects_query_layer = models.BooleanField(
        default=False
    )  # Whether the layer is used for querying tenure intersects
    invert_xy = models.BooleanField(default=False)  # Whether the x and y coordinates should be inverted
    matrix_set = models.CharField(max_length=255, blank=True)  # Matrix set for WMTS layers
    tile_pixel_size = models.PositiveIntegerField(
        default=256, null=False, blank=False
    )  # Tile pixel size for WMTS layers

    class Meta:
        app_label = "boranga"
        ordering = ["display_title", "layer_name"]
        verbose_name = "Geoserver Tile Layer"
        verbose_name_plural = "Geoserver Tile Layers"
        constraints = [
            models.CheckConstraint(
                check=models.Q(
                    ~models.Q(
                        ("is_satellite_background", True),
                        ("is_streets_background", True),
                    ),
                ),
                name="tilelayer_is_either_satellite_or_streets_background_or_neither",
            ),
        ]

    def __str__(self):
        return self.layer_name

    def save(self, *args, **kwargs):
        # Clear the cache for the proxy layer data
        cache_key = settings.CACHE_KEY_PROXY_LAYER_DATA.format(app_label="boranga", model_name="tilelayer")
        cache.delete(cache_key)

        super().save(*args, **kwargs)


class Proxy(BaseModel):
    request_path = models.CharField(max_length=255)
    proxy_url = models.CharField(max_length=255)
    basic_auth_enabled = models.BooleanField(default=False)
    username = models.CharField(max_length=255, blank=False)
    password = models.CharField(max_length=255, blank=False)
    active = models.BooleanField(default=True)

    class Meta:
        app_label = "boranga"
        ordering = ["request_path"]
        verbose_name = "Proxy"
        verbose_name_plural = "Proxies"

    def save(self, *args, **kwargs):
        if self.basic_auth_enabled:
            if self.username == "" or self.password == "":
                raise ValueError("Username and password are required for basic auth")

        # Clear the cache for the proxy data
        cache_key = settings.CACHE_KEY_PROXY_NODE_DATA.format(request_path=self.request_path)
        cache.delete(cache_key)
        super().save(*args, **kwargs)


class PlausibilityGeometry(BaseModel):
    OCCURRENCE_GEOMETRY = "OccurrenceGeometry"
    OCCURRENCE_REPORT_GEOMETRY = "OccurrenceReportGeometry"
    CHECK_FOR_GEOMETRY_CHOICES = (
        (OCCURRENCE_GEOMETRY, "Occurrence-Geometry"),
        (OCCURRENCE_REPORT_GEOMETRY, "Occurrence Report-Geometry"),
    )

    check_for_geometry = models.CharField(
        "Check for Geometry",
        max_length=50,
        choices=CHECK_FOR_GEOMETRY_CHOICES,
        default=OCCURRENCE_GEOMETRY,
    )
    geometry = gis_models.GeometryField(blank=True, null=True)
    warning_value = models.FloatField(blank=True, null=True)
    error_value = models.FloatField(blank=True, null=True)
    average_area = models.FloatField(blank=True, null=True)
    ratio_effective_area = models.FloatField(
        blank=False,
        null=False,
        validators=[MinValueValidator(0.0), MaxValueValidator(1.0)],
        default=1.0,
    )
    active = models.BooleanField(default=True)

    class Meta:
        app_label = "boranga"
        ordering = ["average_area"]
        verbose_name = "Plausibility Geometry"
        verbose_name_plural = "Plausibility Geometries"

    def __str__(self):
        return f"{self.check_for_geometry} {self.__class__.__name__} {self.id} - {self.average_area}m²"
