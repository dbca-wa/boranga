from django.apps import AppConfig
from django.conf import settings
from django.contrib.admin.apps import AdminConfig


class BorangaConfig(AppConfig):
    name = "boranga"
    verbose_name = settings.SYSTEM_NAME

    def ready(self):
        from boranga.components.users import signals  # noqa


class BorangaAdminConfig(AdminConfig):
    default_site = "boranga.admin.BorangaAdminSite"
