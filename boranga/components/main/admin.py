from typing import Any

from django import forms
from django.apps import apps
from django.contrib.gis import admin
from django.http import HttpRequest

from boranga.admin import (
    ArchivableModelAdminMixin,
    CsvExportMixin,
    DeleteProtectedModelAdmin,
)
from boranga.components.main.models import (
    Document,
    FileExtensionWhitelist,
    HelpTextEntry,
    LegacyTaxonomyMapping,
    LegacyUsernameEmailuserMapping,
    LegacyValueMap,
    OccToOcrSectionMapping,
    UserSystemSettings,
)


class UserSystemSettingsAdmin(admin.ModelAdmin):
    list_display = ["user", "area_of_interest"]


class ModelForm(forms.ModelForm):
    choices = (
        (
            "all",
            "all",
        ),
    ) + tuple(
        map(
            lambda m: (m, m),
            filter(
                lambda m: Document
                in apps.get_app_config("boranga").models[m].__bases__,
                apps.get_app_config("boranga").models,
            ),
        )
    )

    model = forms.ChoiceField(choices=choices)


class FileExtensionWhitelistAdmin(DeleteProtectedModelAdmin):
    fields = (
        "name",
        "model",
        "compressed",
    )
    list_display = (
        "name",
        "model",
        "compressed",
    )
    form = ModelForm
    search_fields = ["name", "model"]
    list_filter = ["compressed", "model"]


class HelpTextEntryAdmin(
    CsvExportMixin, ArchivableModelAdminMixin, DeleteProtectedModelAdmin
):
    list_display = [
        "section_id",
        "text",
        "icon_with_popover",
        "authenticated_users_only",
        "internal_users_only",
    ]

    def get_readonly_fields(
        self, request: HttpRequest, obj: Any | None = ...
    ) -> list[str] | tuple[Any, ...]:
        fields = super().get_readonly_fields(request, obj)
        if not request.user.is_superuser:
            return list(fields) + ["section_id"]
        return fields


class LegacyValueMapAdmin(admin.ModelAdmin):
    list_display = [
        "legacy_system",
        "list_name",
        "legacy_value",
        "canonical_name",
        "target_object",
        "target_object_id",
    ]
    list_filter = ["legacy_system", "list_name"]
    search_fields = ["list_name", "legacy_value", "canonical_name"]


class OccToOcrSectionMappingAdmin(admin.ModelAdmin):
    list_display = [
        "legacy_system",
        "occ_migrated_from_id",
        "ocr_migrated_from_id",
    ]
    list_filter = ["legacy_system", "processed"]
    search_fields = ["legacy_system", "section"]


class LegacyUsernameEmailuserMappingAdmin(admin.ModelAdmin):
    list_display = [
        "legacy_system",
        "legacy_username",
        "email",
        "first_name",
        "last_name",
        "emailuser_id",
    ]
    search_fields = ["legacy_username", "email_user__email"]


class LegacyTaxonomyMappingAdmin(admin.ModelAdmin):
    list_display = [
        "list_name",
        "legacy_taxon_name_id",
        "legacy_canonical_name",
        "taxon_name_id",
        "taxonomy",
    ]
    list_filter = ["list_name"]
    search_fields = [
        "legacy_taxon_name_id",
        "legacy_canonical_name",
        "taxon_name_id",
        "taxonomy__scientific_name",
    ]
    raw_id_fields = ["taxonomy"]


admin.site.register(FileExtensionWhitelist, FileExtensionWhitelistAdmin)
admin.site.register(UserSystemSettings, UserSystemSettingsAdmin)
admin.site.register(HelpTextEntry, HelpTextEntryAdmin)
admin.site.register(LegacyValueMap, LegacyValueMapAdmin)
admin.site.register(OccToOcrSectionMapping, OccToOcrSectionMappingAdmin)
admin.site.register(LegacyUsernameEmailuserMapping, LegacyUsernameEmailuserMappingAdmin)
admin.site.register(LegacyTaxonomyMapping, LegacyTaxonomyMappingAdmin)
