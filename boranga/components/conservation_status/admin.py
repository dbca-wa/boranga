from django.contrib.gis import admin

from boranga.admin import (
    ArchivableModelAdminMixin,
    CsvExportMixin,
    DeleteProtectedModelAdmin,
)
from boranga.components.conservation_status import models


@admin.register(models.ProposalAmendmentReason)
class ProposalAmendmentReasonAdmin(CsvExportMixin, ArchivableModelAdminMixin, DeleteProtectedModelAdmin):
    list_display = ["reason"]


class AbstractListAdmin(DeleteProtectedModelAdmin):
    list_display = [
        "code",
        "label",
        "applies_to_flora",
        "applies_to_fauna",
        "applies_to_communities",
    ]


class AbstractCategoryAdmin(DeleteProtectedModelAdmin):
    list_display = ["code", "label"]


class WAPriorityListAdmin(CsvExportMixin, ArchivableModelAdminMixin, AbstractListAdmin):
    pass


class WAPriorityCategoryAdmin(CsvExportMixin, ArchivableModelAdminMixin, AbstractCategoryAdmin):
    filter_horizontal = ("wa_priority_lists",)


class IUCNVersionAdmin(CsvExportMixin, ArchivableModelAdminMixin, AbstractListAdmin):
    pass


class WALegislativeListAdmin(CsvExportMixin, ArchivableModelAdminMixin, AbstractListAdmin):
    pass


class WALegislativeCategoryAdmin(CsvExportMixin, ArchivableModelAdminMixin, AbstractCategoryAdmin):
    filter_horizontal = ("wa_legislative_lists",)


class CommonwealthConservationListAdmin(CsvExportMixin, ArchivableModelAdminMixin, AbstractListAdmin):
    pass


class OtherConservationAssessmentListAdmin(CsvExportMixin, ArchivableModelAdminMixin, AbstractListAdmin):
    pass


class ConservationChangeCodeAdmin(CsvExportMixin, ArchivableModelAdminMixin, DeleteProtectedModelAdmin):
    list_display = ["code", "label"]


admin.site.register(models.WAPriorityList, WAPriorityListAdmin)
admin.site.register(models.WAPriorityCategory, WAPriorityCategoryAdmin)
admin.site.register(models.WALegislativeList, WALegislativeListAdmin)
admin.site.register(models.WALegislativeCategory, WALegislativeCategoryAdmin)
admin.site.register(models.IUCNVersion, IUCNVersionAdmin)
admin.site.register(models.CommonwealthConservationList, CommonwealthConservationListAdmin)
admin.site.register(models.OtherConservationAssessmentList, OtherConservationAssessmentListAdmin)
admin.site.register(models.ConservationChangeCode, ConservationChangeCodeAdmin)
