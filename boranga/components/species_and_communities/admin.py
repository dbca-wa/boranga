from django.contrib.gis import admin
from import_export.admin import ImportExportMixin
from django.db.models import Q
from boranga.components.species_and_communities.models import (
    GroupType,
    District, 
    DocumentCategory, 
    Region, 
    ThreatCategory, 
    DocumentSubCategory,
    Genus,
    Kingdom,
    # FloweringPeriod,
    # FruitingPeriod,
    FloraRecruitmentType,
    # SeedViabilityGerminationInfo,
    RootMorphology,
    # PollinatorInformation,
    PostFireHabitatInteraction,
    # FaunaBreeding,
    # BreedingPeriod,
    CurrentImpact,
    PotentialImpact,
    PotentialThreatOnset,
    ThreatAgent,
    )


# The following allow Import/Export of large lists from file.
# class ConservationListAdmin(ImportExportMixin, admin.ModelAdmin):
#     list_display = ['code', 'label']
# admin.site.register(ConservationList, ConservationListAdmin)

# class ConservationCategoryAdmin(ImportExportMixin, admin.ModelAdmin):
#     list_display = ['code', 'label']
# admin.site.register(ConservationCategory, ConservationCategoryAdmin)

# class ConservationCriteriaAdmin(ImportExportMixin, admin.ModelAdmin):
#     list_display = ['code']
# admin.site.register(ConservationCriteria, ConservationCriteriaAdmin)

@admin.register(DocumentSubCategory)
class DocumentSubCategoryAdmin(admin.ModelAdmin):
    list_display = ['document_sub_category_name', 'document_category']

# Each of the following models will be available to Django Admin.
admin.site.register(GroupType)
admin.site.register(Region)
admin.site.register(District)
admin.site.register(DocumentCategory)
admin.site.register(ThreatCategory)
# admin.site.register(Genus)
admin.site.register(Kingdom)
# admin.site.register(FloweringPeriod)
# admin.site.register(FruitingPeriod)
admin.site.register(FloraRecruitmentType)
# admin.site.register(SeedViabilityGerminationInfo)
admin.site.register(RootMorphology)
# admin.site.register(PollinatorInformation)
admin.site.register(PostFireHabitatInteraction)
# admin.site.register(FaunaBreeding)
# admin.site.register(BreedingPeriod)
admin.site.register(CurrentImpact)
admin.site.register(PotentialImpact)
admin.site.register(PotentialThreatOnset)
admin.site.register(ThreatAgent)
