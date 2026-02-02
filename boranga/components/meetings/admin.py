from django.contrib.gis import admin
from import_export.admin import ImportMixin

from boranga.admin import (
    ArchivableModelAdminMixin,
    CsvExportMixin,
    DeleteProtectedModelAdmin,
)
from boranga.components.meetings import models
from boranga.components.meetings.models import CommitteeMembers


class MeetingRoomAdmin(CsvExportMixin, ArchivableModelAdminMixin, DeleteProtectedModelAdmin):
    list_display = ["id", "room_name"]


class CommitteeMembersInline(admin.TabularInline):
    model = CommitteeMembers
    extra = 0
    # raw_id_fields = ('email',)


class CommitteeMembersAdmin(CsvExportMixin, ArchivableModelAdminMixin, DeleteProtectedModelAdmin):
    list_display = ["email", "id", "first_name", "last_name", "committee"]


class CommitteeAdmin(ImportMixin, CsvExportMixin, ArchivableModelAdminMixin, DeleteProtectedModelAdmin):
    list_display = ["name", "id"]
    inlines = [CommitteeMembersInline]


admin.site.register(models.MeetingRoom, MeetingRoomAdmin)
admin.site.register(models.Committee, CommitteeAdmin)
admin.site.register(models.CommitteeMembers, CommitteeMembersAdmin)
