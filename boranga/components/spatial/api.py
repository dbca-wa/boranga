from django.shortcuts import get_object_or_404
from rest_framework import viewsets
from rest_framework.response import Response

from boranga.components.spatial.models import TileLayer
from boranga.components.spatial.permissions import TileLayerPermission
from boranga.components.spatial.serializers import TileLayerSerializer
from boranga.helpers import is_customer, is_internal, is_referee


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
        elif is_referee(self.request):
            return TileLayer.objects.filter(active=True, is_external=True).order_by("id")
        elif is_customer(self.request):
            return TileLayer.objects.filter(active=True, is_external=True).order_by("id")
        else:
            return TileLayer.objects.none()
            # raise ValueError("User is not a customer, internal user, or referee.")
