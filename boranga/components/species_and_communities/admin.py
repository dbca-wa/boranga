from django.contrib.gis import admin
from import_export.admin import ImportMixin
from ordered_model.admin import OrderedModelAdmin

from boranga.admin import (
    ArchivableModelAdminMixin,
    CsvExportMixin,
    DeleteProtectedModelAdmin,
)
from boranga.components.species_and_communities.models import (
    ClassificationSystem,
    CurrentImpact,
    District,
    DocumentCategory,
    DocumentSubCategory,
    FaunaGroup,
    FaunaSubGroup,
    GroupType,
    InformalGroup,
    Kingdom,
    PotentialImpact,
    PotentialThreatOnset,
    Region,
    SystemEmail,
    SystemEmailGroup,
    Taxonomy,
    TaxonomyRank,
    TaxonPreviousName,
    TaxonVernacular,
    ThreatAgent,
    ThreatCategory,
)


class DocumentCategoryAdmin(
    CsvExportMixin,
    ArchivableModelAdminMixin,
    OrderedModelAdmin,
    DeleteProtectedModelAdmin,
):
    list_display = [
        "document_category_name",
        "move_up_down_links",
    ]
    search_fields = ["document_category_name"]
    ordering = ("order",)


@admin.register(DocumentSubCategory)
class DocumentSubCategoryAdmin(
    CsvExportMixin,
    OrderedModelAdmin,
    ArchivableModelAdminMixin,
    DeleteProtectedModelAdmin,
):
    list_display = [
        "document_sub_category_name",
        "document_category",
        "move_up_down_links",
    ]
    list_filter = ["document_category"]
    search_fields = ["document_sub_category_name"]
    ordering = ("document_category", "order")


@admin.register(FaunaGroup)
class FaunaGroupAdmin(
    CsvExportMixin,
    ImportMixin,
    OrderedModelAdmin,
    ArchivableModelAdminMixin,
    DeleteProtectedModelAdmin,
):
    list_display = [
        "name",
        "move_up_down_links",
    ]
    search_fields = ["name"]
    ordering = ("order",)


@admin.register(FaunaSubGroup)
class FaunaSubGroupAdmin(
    CsvExportMixin,
    ImportMixin,
    OrderedModelAdmin,
    ArchivableModelAdminMixin,
    DeleteProtectedModelAdmin,
):
    list_display = [
        "name",
        "fauna_group",
        "move_up_down_links",
    ]
    list_filter = ["fauna_group"]
    search_fields = ["name"]
    ordering = ("fauna_group", "order")


class ThreatCategoryAdmin(
    CsvExportMixin,
    ImportMixin,
    OrderedModelAdmin,
    ArchivableModelAdminMixin,
    DeleteProtectedModelAdmin,
):
    list_display = [
        "name",
        "move_up_down_links",
    ]
    search_fields = ["name"]
    ordering = ("order",)


class ThreatAgentAdmin(
    CsvExportMixin,
    ImportMixin,
    OrderedModelAdmin,
    ArchivableModelAdminMixin,
    DeleteProtectedModelAdmin,
):
    list_display = [
        "name",
        "move_up_down_links",
    ]
    search_fields = ["name"]
    ordering = ("order",)


class PotentialThreatOnsetAdmin(
    CsvExportMixin,
    ImportMixin,
    OrderedModelAdmin,
    ArchivableModelAdminMixin,
    DeleteProtectedModelAdmin,
):
    list_display = [
        "name",
        "move_up_down_links",
    ]
    search_fields = ["name"]
    ordering = ("order",)


class PotentialImpactAdmin(
    CsvExportMixin,
    ImportMixin,
    OrderedModelAdmin,
    ArchivableModelAdminMixin,
    DeleteProtectedModelAdmin,
):
    list_display = [
        "name",
        "move_up_down_links",
    ]
    search_fields = ["name"]
    ordering = ("order",)


class CurrentImpactAdmin(
    CsvExportMixin,
    ImportMixin,
    OrderedModelAdmin,
    ArchivableModelAdminMixin,
    DeleteProtectedModelAdmin,
):
    list_display = [
        "name",
        "move_up_down_links",
    ]
    search_fields = ["name"]
    ordering = ("order",)


class DistrictAdmin(
    CsvExportMixin,
    ImportMixin,
    DeleteProtectedModelAdmin,
):
    list_display = [
        "name",
        "region",
        "archive_date",
    ]
    list_filter = ["region"]
    search_fields = ["name"]


# Each of the following models will be available to Django Admin.
admin.site.register(GroupType)
admin.site.register(Region, DeleteProtectedModelAdmin)
admin.site.register(District, DistrictAdmin)
admin.site.register(DocumentCategory, DocumentCategoryAdmin)
admin.site.register(ThreatCategory, ThreatCategoryAdmin)

# NOTE: The following 3 admins are hidden for now as conservation attributes are not shown on the front end
# admin.site.register(FloraRecruitmentType, DeleteProtectedModelAdmin)
# admin.site.register(RootMorphology, DeleteProtectedModelAdmin)
# admin.site.register(PostFireHabitatInteraction, DeleteProtectedModelAdmin)

admin.site.register(CurrentImpact, CurrentImpactAdmin)
admin.site.register(PotentialImpact, PotentialImpactAdmin)
admin.site.register(PotentialThreatOnset, PotentialThreatOnsetAdmin)
admin.site.register(ThreatAgent, ThreatAgentAdmin)


@admin.register(Kingdom)
class KingdomAdmin(admin.ModelAdmin):
    list_display = ["id", "kingdom_id", "kingdom_name", "grouptype"]
    readonly_fields = ("kingdom_id", "kingdom_name")

    def has_delete_permission(self, request, obj=None):
        return False

    def has_add_permission(self, request):
        return False


class TaxonVernacularInline(admin.TabularInline):
    model = TaxonVernacular
    list_display = ("id", "vernacular_id", "vernacular_name")
    ordering = ("-id",)
    extra = 0

    def has_add_permission(self, request, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return False


class TaxonPreviousNameInline(admin.TabularInline):
    model = TaxonPreviousName
    fk_name = "taxonomy"
    list_display = ("id", "previous_name_id", "previous_scientific_name")
    ordering = ("-id",)
    extra = 0

    def has_add_permission(self, request, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return False


class InformalGroupInline(admin.TabularInline):
    model = InformalGroup
    list_display = ("id", "classification_system_id", "classification_system_fk")
    ordering = ("-id",)
    extra = 0

    def has_add_permission(self, request, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return False


@admin.register(Taxonomy)
class TaxonomyAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "taxon_name_id",
        "scientific_name",
        "kingdom_name",
        "grouptype__name",
        "is_current",
    )
    list_filter = [
        "kingdom_fk__kingdom_name",
        "kingdom_fk__grouptype__name",
        "is_current",
    ]
    inlines = [TaxonVernacularInline, TaxonPreviousNameInline, InformalGroupInline]
    search_fields = ("taxon_name_id", "scientific_name")
    actions = None

    def get_queryset(self, request):
        # Use the all_objects manager so admin can view archived records
        return Taxonomy.all_objects.all()

    def get_readonly_fields(self, request, obj=None):
        return [f.name for f in obj._meta.fields]

    def has_delete_permission(self, request, obj=None):
        return False

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None, **kwargs):
        return False

    def grouptype__name(self, obj):
        if obj.kingdom_fk.grouptype is None:
            return ""
        else:
            return obj.kingdom_fk.grouptype.name


@admin.register(TaxonomyRank)
class TaxonomyRankAdmin(admin.ModelAdmin):
    list_display = ["id", "taxon_rank_id", "rank_name", "kingdom_fk"]
    actions = None

    def get_readonly_fields(self, request, obj=None):
        return [f.name for f in obj._meta.fields]

    def has_delete_permission(self, request, obj=None):
        return False

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None, **kwargs):
        return False


@admin.register(ClassificationSystem)
class ClassificationSystemAdmin(admin.ModelAdmin):
    list_display = ["id", "classification_system_id", "class_desc"]
    actions = None

    def get_readonly_fields(self, request, obj=None):
        return [f.name for f in obj._meta.fields]

    def has_delete_permission(self, request, obj=None):
        return False

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None, **kwargs):
        return False


class SystemEmailTabularInline(admin.TabularInline):
    model = SystemEmail
    list_display = "email"
    verbose_name = "Email"
    verbose_name_plural = "Emails"
    extra = 0


@admin.register(SystemEmailGroup)
class SystemEmailGroupAdmin(admin.ModelAdmin):
    list_display = ["label", "group_type", "area"]
    ordering = ["group_type", "area"]
    inlines = [SystemEmailTabularInline]

    def get_readonly_fields(self, request, obj=None):
        if request.user.is_superuser:
            return []
        return [f.name for f in obj._meta.fields]

    def get_actions(self, request):
        actions = super().get_actions(request)
        if "delete_selected" in actions and not request.user.is_superuser:
            del actions["delete_selected"]
        return actions

    def has_delete_permission(self, request, obj=None):
        return request.user.is_superuser

    def has_add_permission(self, request):
        return request.user.is_superuser

    def has_change_permission(self, request, obj=None, **kwargs):
        return request.user.is_superuser
