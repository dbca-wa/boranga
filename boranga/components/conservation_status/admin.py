from django.contrib.gis import admin
from django.db.models import Q
from boranga.components.conservation_status import models

@admin.register(models.ConservationList)
class ConservationListAdmin(admin.ModelAdmin):
    list_display = ['code','label']

@admin.register(models.ConservationCategory)
class ConservationCategoryAdmin(admin.ModelAdmin):
    list_display = ['code','label','conservation_list']

@admin.register(models.ConservationCriteria)
class ConservationCriteriaAdmin(admin.ModelAdmin):
    list_display = ['code','label','conservation_list']

# @admin.register(models.ConservationChangeCode)
# class ConservationChangeCodeAdmin(admin.ModelAdmin):
#     list_display = ['code','label']

@admin.register(models.ProposalAmendmentReason)
class ProposalAmendmentReasonAdmin(admin.ModelAdmin):
    list_display = ['reason']

