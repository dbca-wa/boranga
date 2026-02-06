from rest_framework.decorators import action
from rest_framework.response import Response

from boranga.components.main.api import search_datums


class DatumSearchMixin:
    @action(detail=False, methods=["get"], url_path="epsg-code-datums")
    def get_epsg_code_datums(self, request):
        search = request.GET.get("search", None)

        return Response(search_datums(search))
