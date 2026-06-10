from django.conf import settings


def config(request):
    return {
        "template_group": "bioscience",
        "template_title": settings.TEMPLATE_TITLE,
        "GIT_COMMIT_HASH": settings.GIT_COMMIT_HASH,
        "GIS_SERVER_URL": settings.GIS_SERVER_URL,
        "vue3_entry_script": settings.VUE3_ENTRY_SCRIPT,
        "enable_external_proposals": getattr(settings, "ENABLE_EXTERNAL_PROPOSALS", True),
    }
