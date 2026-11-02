import django.contrib.admin.apps as admin_apps
from django.apps import AppConfig
from django.conf import settings


class BorangaConfig(AppConfig):
    default = True
    name = "boranga"
    verbose_name = settings.SYSTEM_NAME

    def ready(self):
        from boranga.components.users import signals  # noqa


class BorangaAdminConfig(admin_apps.AdminConfig):
    default = False
    default_site = "boranga.admin_site.BorangaAdminSite"
