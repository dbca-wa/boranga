from django.forms import ValidationError
from django.http import Http404
from django.views.decorators.csrf import csrf_exempt

from boranga.components.spatial.models import Proxy
from boranga.components.spatial.utils import process_proxy, proxy_object


@csrf_exempt
def mapProxyView(request, request_path, path):
    if not request.user.is_authenticated:
        raise ValidationError("User is not authenticated")

    queryString = request.META["QUERY_STRING"]
    username = request.user.username
    auth_user = None
    auth_password = None

    try:
        proxy = proxy_object(request_path)
    except Proxy.DoesNotExist:
        raise Http404(f"No active Proxy entry found for {username} and {request_path}")
    else:
        if proxy.get("basic_auth_enabled"):
            auth_user = proxy.get("username")
            auth_password = proxy.get("password")
        remoteurl = proxy.get("proxy_url") + path

    response = process_proxy(request, remoteurl, queryString, auth_user, auth_password)

    return response
