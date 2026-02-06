from django import forms
from django.contrib import admin
from django.forms import ValidationError
from ledger_api_client.ledger_models import EmailUserRO as EmailUser

from boranga.admin import (
    ArchivableModelAdminMixin,
    CsvExportMixin,
    DeleteProtectedModelAdmin,
)
from boranga.components.users import models


class SubmitterCategoryAdmin(CsvExportMixin, ArchivableModelAdminMixin, DeleteProtectedModelAdmin):
    list_display = ("name", "visible_to")


class SubmitterInformationAdmin(admin.ModelAdmin):
    list_display = ("name", "organisation", "position", "submitter_category")


class ExternalContributorBlacklistForm(forms.ModelForm):
    class Meta:
        model = models.ExternalContributorBlacklist
        fields = "__all__"

    def clean(self):
        email = self.cleaned_data["email"]

        try:
            EmailUser.objects.get(email=email)
        except EmailUser.DoesNotExist:
            raise ValidationError(f"EmailUser with email {email} does not exist in ledger. Cannot blacklist.")

        if models.ExternalContributorBlacklist.objects.filter(email=email).exists():
            raise forms.ValidationError("This email is already blacklisted.")

        return super().clean()


class ExternalContributorBlacklistAdmin(admin.ModelAdmin):
    list_display = ("email", "reason", "datetime_created", "datetime_updated")
    form = ExternalContributorBlacklistForm

    def get_actions(self, request):
        actions = super().get_actions(request)
        if "delete_selected" in actions:
            del actions["delete_selected"]
        return actions


admin.site.register(models.SubmitterCategory, SubmitterCategoryAdmin)
admin.site.register(models.SubmitterInformation, SubmitterInformationAdmin)
admin.site.register(models.ExternalContributorBlacklist, ExternalContributorBlacklistAdmin)
