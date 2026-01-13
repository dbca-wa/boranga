import csv
from copy import deepcopy

from csvexport.actions import csvexport
from django.conf import settings
from django.contrib.auth.models import Group
from django.contrib.gis import admin
from django.forms import ValidationError
from django.http import HttpResponse
from django.urls import path
from ledger_api_client.admin import SystemGroupAdmin, SystemGroupPermissionInline
from ledger_api_client.ledger_models import EmailUserRO as EmailUser
from ledger_api_client.managed_models import SystemGroup, SystemGroupPermission

from boranga import helpers as boranga_helpers
from boranga.components.main.models import ArchivableModel
from boranga.components.users.models import ExternalContributorBlacklist


class DeleteProtectedModelAdmin(admin.ModelAdmin):
    """Stops regular users from deleting objects in the admin but still allow superusers to do so."""

    def get_actions(self, request):
        actions = super().get_actions(request)
        if "delete_selected" in actions and not request.user.is_superuser:
            del actions["delete_selected"]
        return actions

    def has_delete_permission(self, request, obj=None):
        return request.user.is_superuser


@admin.action(description="Mark as archived")
def archive(modeladmin, request, queryset):
    queryset.update(archived=True)


@admin.action(description="Mark as unarchived")
def unarchive(modeladmin, request, queryset):
    queryset.update(archived=False)


class ArchivableModelAdminMixin:
    actions = [archive, unarchive]
    list_filter = ("archived",)
    search_fields = ("id",)
    ordering = ("id",)

    def __init__(self, model, admin_site) -> None:
        super().__init__(model, admin_site)

        if not hasattr(self, "model"):
            raise AttributeError("Please set the model attribute on the admin class.")

        if not issubclass(self.model, ArchivableModel):
            raise AttributeError("The model must be a sub class of ArchivableModel.")

    def get_list_display(self, request):
        list_display = super().get_list_display(request)
        if type(list_display) is tuple:
            list_display = list(list_display)
        return list_display + ["achived_list_view_display"]

    def achived_list_view_display(self, obj):
        return "Yes" if obj.archived else "No"

    achived_list_view_display.short_description = "Archived"


class CsvExportMixin:
    def get_actions(self, request):
        actions = super().get_actions(request)
        actions["csvexport"] = (csvexport, "csvexport", "Export to CSV")
        return actions


admin.site.index_template = "admin-index.html"
admin.autodiscover()


@admin.register(EmailUser)
class EmailUserAdmin(admin.ModelAdmin):
    list_display = (
        "ledger_id",
        "email",
        "first_name",
        "last_name",
        "groups",
        "is_staff",
        "is_active",
        "is_superuser",
    )
    fields = (
        "ledger_id",
        "email",
        "first_name",
        "last_name",
        "groups",
        "is_staff",
        "is_active",
        "is_superuser",
    )
    ordering = ("email",)
    search_fields = ("id", "email", "first_name", "last_name")

    def __init__(self, model, admin_site):
        super().__init__(model, admin_site)
        self.opts.verbose_name_plural = "Email Users (Read-Only)"

    def has_change_permission(self, request, obj=None):
        if obj is None:  # and obj.status > 1:
            return True
        return None

    def has_delete_permission(self, request, obj=None):
        return None

    def groups(self, obj):
        groups = SystemGroupPermission.objects.filter(emailuser=obj)
        return ", ".join([group.system_group.name for group in groups])

    def ledger_id(self, obj):
        return obj.id

    ledger_id.short_description = "Ledger ID"


class CustomSystemGroupPermissionInlineForm(SystemGroupPermissionInline.form):
    def clean(self):
        cleaned_data = super().clean()
        system_group = cleaned_data.get("system_group")
        emailuser = cleaned_data.get("emailuser")
        if not system_group or not emailuser:
            return cleaned_data

        if system_group.name in settings.GROUPS_THAT_ALLOW_INTERNAL_MEMBERS_ONLY:
            if not emailuser.is_staff:
                raise ValidationError("Only internal users can be added to this group.")
        else:
            if emailuser.is_staff:
                raise ValidationError("Only external users can be added to this group.")

        if system_group.name != settings.GROUP_NAME_EXTERNAL_CONTRIBUTOR:
            return cleaned_data

        if not ExternalContributorBlacklist.objects.filter(
            email=emailuser.email
        ).exists():
            return cleaned_data

        raise ValidationError(
            f"The email address {emailuser.email} is blacklisted. "
            "Please remove the email from the blacklist and the user will be automatically "
            "added back into the external contributor group."
        )


class CustomSystemGroupPermissionInline(SystemGroupPermissionInline):
    form = CustomSystemGroupPermissionInlineForm
    fields = (
        "emailuser",
        "first_name",
        "last_name",
        "active",
    )
    readonly_fields = ("first_name", "last_name")

    def first_name(self, obj):
        return EmailUser.objects.get(id=obj.emailuser_id).first_name

    def last_name(self, obj):
        return EmailUser.objects.get(id=obj.emailuser_id).last_name


def export_system_group_permissions(modeladmin, request, queryset):
    """
    Export the system group permissions to a CSV file.
    """
    if queryset.count() == 0:
        modeladmin.message_user(
            request,
            "No system groups selected. Please select at least one system group.",
            level="error",
        )
        return None

    filename = "system_group_permissions.csv"

    if queryset.count() == 1:
        filename = f"system_group_permissions_{queryset.first().name}.csv"

    # Get the system group permissions
    system_group_permissions = SystemGroupPermission.objects.filter(
        system_group__in=queryset
    )

    response = HttpResponse(content_type="text/csv")
    response["Content-Disposition"] = f'attachment; filename="{filename}"'

    writer = csv.writer(response)

    system_group_permissions = system_group_permissions.values(
        "emailuser_id",
        "system_group__name",
    )

    # Write the header row
    writer.writerow(
        [
            "System Group Name",
            "Email",
            "First Name",
            "Last Name",
        ]
    )
    # Write the data rows
    for system_group_permission in system_group_permissions:
        emailuser = EmailUser.objects.get(id=system_group_permission["emailuser_id"])
        writer.writerow(
            [
                system_group_permission["system_group__name"],
                emailuser.email,
                emailuser.first_name,
                emailuser.last_name,
            ]
        )

    return response


class CustomSystemGroupAdmin(SystemGroupAdmin):
    """
    Overriding the SystemGroupAdmin from ledger.accounts.admin,
    to remove ledger_permissions selection field for DjangoAdmin SystemGroup on Admin page
    """

    inlines = [CustomSystemGroupPermissionInline]
    filter_horizontal = ("permissions",)

    def get_urls(self):
        urls = super().get_urls()
        custom_urls = [
            # Add custom URL patterns here if needed
            path(
                "export_system_group_permissions/<int:object_id>/",
                self.admin_site.admin_view(export_system_group_permissions),
                name="export_system_group_permissions",
            ),
        ]
        return custom_urls + urls

    def export_system_group_permissions_detail(self, request, object_id):
        """
        Export the system group permissions to a CSV file.
        """
        system_group = SystemGroup.objects.filter(id=object_id)

        return export_system_group_permissions(self, request, system_group)

    def response_change(self, request, obj):
        """
        Override the response_change method to handle the export action.
        """
        if "_export_system_group_permissions_detail" in request.POST:
            return self.export_system_group_permissions_detail(request, obj.id)

        return super().response_change(request, obj)

    def get_actions(self, request):
        """
        Remove the default delete action from the admin page.
        """
        actions = super().get_actions(request)
        actions["export_system_group_users"] = (
            export_system_group_permissions,
            "export_system_group_users",
            "Export System Group Users",
        )
        return actions

    def get_fieldsets(self, request, obj=None):
        """Remove the ledger_permissions checkbox from the Admin page, if user is DjangoAdmin and NOT superuser"""
        fieldsets = super(SystemGroupAdmin, self).get_fieldsets(request, obj)
        if not obj:
            return fieldsets
        if request.user.is_superuser:
            return fieldsets
        elif boranga_helpers.is_django_admin(request):
            fieldsets = deepcopy(fieldsets)
            for fieldset in fieldsets:
                if "permissions" in fieldset[1]["fields"]:
                    if type(fieldset[1]["fields"]) is tuple:
                        fieldset[1]["fields"] = list(fieldset[1]["fields"])
                    fieldset[1]["fields"].remove("permissions")

        return fieldsets

    def get_readonly_fields(self, request, obj=None):
        if request.user.is_superuser:
            return []
        elif boranga_helpers.is_django_admin(request):
            return ["name"]  # make fields readonly when editing an existing object


admin.site.unregister(Group)
admin.site.unregister(SystemGroup)
admin.site.register(SystemGroup, CustomSystemGroupAdmin)
