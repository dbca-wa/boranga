import traceback
import pytz
import json
import os
import subprocess
from django.db.models import Q
from django.db import transaction
from django.core.exceptions import ValidationError
from rest_framework import viewsets, serializers, status, views
from rest_framework.decorators import action as detail_route, renderer_classes
from rest_framework.decorators import action as list_route
from rest_framework.response import Response
from rest_framework.renderers import JSONRenderer
from datetime import datetime
from ledger_api_client.settings_base import TIME_ZONE
from boranga import settings
from django.core.cache import cache
from django.http import HttpResponse
from django.urls import reverse
from django.shortcuts import redirect
from boranga.helpers import is_customer, is_internal
from rest_framework_datatables.pagination import DatatablesPageNumberPagination
from rest_framework_datatables.filters import DatatablesFilterBackend
from rest_framework_datatables.renderers import DatatablesRenderer
from copy import deepcopy
from django.shortcuts import render, redirect, get_object_or_404
import pandas as pd
from openpyxl import Workbook
from openpyxl.styles import Font
from openpyxl.utils.dataframe import dataframe_to_rows
from io import BytesIO
from django.db.models.query import QuerySet
from boranga.components.main.decorators import basic_exception_handler
from boranga.components.species_and_communities.utils import (
    species_form_submit,
    community_form_submit,
    combine_species_original_submit,
    rename_species_original_submit,
)
from boranga.components.main.related_item import RelatedItemsSerializer

from boranga.components.species_and_communities.email import (
    send_species_split_email_notification, 
    send_species_combine_email_notification,
    send_species_rename_email_notification,
)

from boranga.components.species_and_communities.models import (
    GroupType,
    Kingdom,
    Species,
    TaxonVernacular,
    ClassificationSystem,
    InformalGroup,
    ScientificName,
    Taxonomy,
    Family,
    PhylogeneticGroup,
    Genus,
    NameAuthority,
    CommunityTaxonomy,
    Community,
    Region,
    District,
    SpeciesDistribution,
    CommunityDistribution,
    FloweringPeriod,
    FruitingPeriod,
    FloraRecruitmentType,
    SeedViabilityGerminationInfo,
    RootMorphology,
    PollinatorInformation,
    PostFireHabitatInteraction,
    BreedingPeriod,
    FaunaBreeding,
    SpeciesConservationAttributes,
    CommunityConservationAttributes,
    DocumentCategory,
    DocumentSubCategory,
    SpeciesDocument,
    CommunityDocument,
    ThreatCategory,
    CurrentImpact,
    PotentialImpact,
    PotentialThreatOnset,
    ThreatAgent,
    ConservationThreat,
    CommunityUserAction,
    SpeciesUserAction,
)
from boranga.components.conservation_status.models import(
    ConservationCategory,
    ConservationCriteria,
    ConservationChangeCode,
    ConservationStatus,
    ConservationList,
    ConservationStatusUserAction,
)
from boranga.components.species_and_communities.serializers import (
    ListSpeciesSerializer,
    InternalSpeciesSerializer,
    SpeciesSerializer,
    SaveSpeciesSerializer,
    CreateSpeciesSerializer,
    SpeciesDistributionSerializer,
    SaveSpeciesDistributionSerializer,
    SpeciesConservationAttributesSerializer,
    SaveSpeciesConservationAttributesSerializer,
    TaxonomySerializer,
    CommunityTaxonomySerializer,
    SaveCommunityTaxonomySerializer,
    ListCommunitiesSerializer,
    InternalCommunitySerializer,
    CommunitySerializer,
    SaveCommunitySerializer,
    CreateCommunitySerializer,
    CommunityDistributionSerializer,
    SaveCommunityDistributionSerializer,
    CommunityConservationAttributesSerializer,
    SaveCommunityConservationAttributesSerializer,
    SpeciesDocumentSerializer,
    SaveSpeciesDocumentSerializer,
    CommunityDocumentSerializer,
    SaveCommunityDocumentSerializer,
    SpeciesLogEntrySerializer,
    SpeciesUserActionSerializer,
    ConservationThreatSerializer,
    SaveConservationThreatSerializer,
    CommunityLogEntrySerializer,
    CommunityUserActionSerializer,
)

                            

class GetGroupTypeDict(views.APIView):
    def get(self, request, format=None):
        group_type_list = []
        group_types = GroupType.objects.all()
        if group_types:
            for group in group_types:
                #group_type_list.append(group.name)
                group_type_list.append({'id': group.id,'name':group.name, 'display':group.get_name_display()});
        return Response(group_type_list)

# used for external conservation status dash
class GetSpecies(views.APIView):
    def get(self, request, format=None):
        search_term = request.GET.get('term', '')
        if search_term:
            exculde_status = ['draft']
            data = Species.objects.filter(~Q(processing_status__in=exculde_status) & ~Q(taxonomy=None))
            data = data.filter(taxonomy__scientific_name__icontains=search_term).values('id', 'taxonomy__scientific_name')[:10]
            data_transform = [{'id': taxon['id'], 'text': taxon['taxonomy__scientific_name']} for taxon in data]
            return Response({"results": data_transform})
        return Response()

# used for external conservation status dash
class GetCommunities(views.APIView):
    def get(self, request, format=None):
        search_term = request.GET.get('term', '')
        if search_term:
            exculde_status = ['draft']
            data = Community.objects.filter(~Q(processing_status__in=exculde_status))
            data = data.filter(taxonomy__community_name__icontains=search_term).values('id', 'taxonomy__community_name')[:10]
            data_transform = [{'id': taxon['id'], 'text': taxon['taxonomy__community_name']} for taxon in data]
            return Response({"results": data_transform})
        return Response()


# used on dashboards and forms
class GetScientificName(views.APIView):
    def get(self, request, format=None):
        group_type_id = request.GET.get('group_type_id', '')
        search_term = request.GET.get('term', '')
        cs_referral = request.GET.get('cs_referral', '')
        # used for conservation status form
        cs_species = request.GET.get('cs_species', '')
        cs_species_status = request.GET.get('cs_species_status', '')
        # used on combine select(+) species pop-up
        combine_species = request.GET.get('combine_species', '')
        # taxon_details is send from species profile form to get all the taxon details as well
        taxon_details = request.GET.get('taxon_details', '')
        # filter the taxon according to species processing status(e.g if draft the list should be only current= true)
        species_id = request.GET.get('species_id','')
        if search_term:
            data_transform = []
            if cs_referral != '':
                #TODO may need to change the query for referral
                data = Taxonomy.objects.filter(scientific_name__icontains=search_term, kingdom_fk__grouptype=group_type_id).values('id', 'scientific_name')[:10]
                data_transform = [{'id': taxon['id'], 'text': taxon['scientific_name']} for taxon in data]
                data_transform = sorted(data_transform, key=lambda x: x['text'])
            elif cs_species != '':
                data=None
                exculde_status = ['draft']
                # TODO do we need to check the taxonomy is_current=True as well
                # for new cs species with draft and historical should not populate
                if cs_species_status=="Draft":
                    exculde_status = ['draft','historical']
                    data = Species.objects.filter(~Q(processing_status__in=exculde_status) & ~Q(taxonomy=None))
                else:
                    exculde_status = ['draft']
                    data = Species.objects.filter(~Q(processing_status__in=exculde_status) & ~Q(taxonomy=None))
                data = data.filter(taxonomy__scientific_name__icontains=search_term, taxonomy__kingdom_fk__grouptype=group_type_id)[:10]
                data_transform = [{'id': species.id, 'text': species.taxonomy.scientific_name, 'taxon_previous_name': species.taxonomy.taxon_previous_name} for species in data]
                data_transform = sorted(data_transform, key=lambda x: x['text'])
            elif combine_species != '':
                # TODO do we need to check the taxonomy is_current=True as well
                data = Species.objects.filter(Q(processing_status='active') & Q(taxonomy__scientific_name__icontains=search_term) & Q(taxonomy__kingdom_fk__grouptype=group_type_id))[:10]
                data_transform = [{'id': species.id, 'text': species.taxonomy.scientific_name} for species in data]
                data_transform = sorted(data_transform, key=lambda x: x['text'])
            else:
                if taxon_details != '':
                    species_status = Species.objects.get(id=species_id).processing_status
                    if species_status == Species.PROCESSING_STATUS_DRAFT:
                        qs = Taxonomy.objects.filter(scientific_name__icontains=search_term, kingdom_fk__grouptype=group_type_id, name_currency=True)[:10]
                    else:
                        qs = Taxonomy.objects.filter(scientific_name__icontains=search_term, kingdom_fk__grouptype=group_type_id)[:10]
                    serializer = TaxonomySerializer(qs, context={'request': request}, many=True)
                    data_transform = serializer.data
                else:
                    data = Taxonomy.objects.filter(scientific_name__icontains=search_term, kingdom_fk__grouptype=group_type_id).values('id', 'scientific_name')[:10]
                    data_transform = [{'id': taxon['id'], 'text': taxon['scientific_name']} for taxon in data]
                    data_transform = sorted(data_transform, key=lambda x: x['text'])
            return Response({"results": data_transform})
        return Response()

class GetCommonName(views.APIView):
    def get(self, request, format=None):
        group_type_id = request.GET.get('group_type_id', '')
        search_term = request.GET.get('term', '')
        cs_referral = request.GET.get('cs_referral', '')
        if search_term:
            if cs_referral != '':
                #TODO may need to change the query for referral
                data = TaxonVernacular.objects.filter(vernacular_name__icontains=search_term, taxonomy__kingdom_fk__grouptype=group_type_id).values('id', 'vernacular_name')[:10]
            else:
                data = TaxonVernacular.objects.filter(vernacular_name__icontains=search_term, taxonomy__kingdom_fk__grouptype=group_type_id).values('id', 'vernacular_name')[:10]
            data_transform = [{'id': vern['id'], 'text': vern['vernacular_name']} for vern in data]
            return Response({"results": data_transform})
        return Response()


class GetFamily(views.APIView):
    def get(self, request, format=None):
        group_type_id = request.GET.get('group_type_id', '')
        search_term = request.GET.get('term', '')
        cs_referral = request.GET.get('cs_referral', '')
        if search_term:
            if cs_referral != '':
                #TODO may need to change the query for referral
                family_ids = Taxonomy.objects.filter(~Q(family_fk=None)).order_by().values_list('family_fk', flat=True).distinct() # fetch all distinct the family_nid(taxon_name_id) for each taxon
                data = Taxonomy.objects.filter(id__in=family_ids, scientific_name__icontains=search_term, kingdom_fk__grouptype=group_type_id).values('id', 'scientific_name')[:10]
            else:
                family_ids = Taxonomy.objects.filter(~Q(family_fk=None)).order_by().values_list('family_fk', flat=True).distinct() # fetch all distinct the family_nid(taxon_name_id) for each taxon
                data = Taxonomy.objects.filter(id__in=family_ids, scientific_name__icontains=search_term, kingdom_fk__grouptype=group_type_id).values('id', 'scientific_name')[:10]
            data_transform = [{'id': taxon['id'], 'text': taxon['scientific_name']} for taxon in data]
            return Response({"results": data_transform})
        return Response()


class GetGenera(views.APIView):
    def get(self, request, format=None):
        #  group_type_id  retrive as may need to use later
        group_type_id = request.GET.get('group_type_id', '')
        search_term = request.GET.get('term', '')
        cs_referral = request.GET.get('cs_referral', '')
        if search_term:
            if cs_referral != '':
                #TODO may need to change the query for referral
                data = Genus.objects.filter(name__icontains=search_term).values('id', 'name')[:10]
            else:
                data = Genus.objects.filter(name__icontains=search_term).values('id', 'name')[:10]
            data_transform = [{'id': taxon['id'], 'text': taxon['name']} for taxon in data]
            return Response({"results": data_transform})
        return Response()


class GetPhyloGroup(views.APIView):
    def get(self, request, format=None):
        #  group_type_id  retrive as may need to use later
        group_type_id = request.GET.get('group_type_id', '')
        search_term = request.GET.get('term', '')
        cs_referral = request.GET.get('cs_referral', '')
        if search_term:
            if cs_referral != '':
                #TODO may need to change the query for referral
                # data = ClassificationSystem.objects.filter(class_desc__icontains=search_term).values('id', 'class_desc')[:10]
                data = InformalGroup.objects.filter(classification_system_fk__class_desc__icontains=search_term, taxonomy__kingdom_fk__grouptype=group_type_id).distinct().values('classification_system_fk','classification_system_fk__class_desc')[:10]
            else:
                # data = ClassificationSystem.objects.filter(class_desc__icontains=search_term).values('id', 'class_desc')[:10]
                data = InformalGroup.objects.filter(classification_system_fk__class_desc__icontains=search_term, taxonomy__kingdom_fk__grouptype=group_type_id).distinct().values('classification_system_fk','classification_system_fk__class_desc')[:10]
            data_transform = [{'id': group['classification_system_fk'], 'text': group['classification_system_fk__class_desc']} for group in data]
            return Response({"results": data_transform})
        return Response()


class GetCommunityId(views.APIView):
    def get(self, request, format=None):
        search_term = request.GET.get('term', '')
        cs_referral = request.GET.get('cs_referral', '')
        if search_term:
            if cs_referral != '':
                #TODO may need to change the query for referral
                data = CommunityTaxonomy.objects.filter(community_migrated_id__icontains=search_term).values('id', 'community_migrated_id')[:10]
            else:
                data = CommunityTaxonomy.objects.filter(community_migrated_id__icontains=search_term).values('id', 'community_migrated_id')[:10]
            data_transform = [{'id': community['id'], 'text': community['community_migrated_id']} for community in data]
            return Response({"results": data_transform})
        return Response()


class GetCommunityName(views.APIView):
    def get(self, request, format=None):
        search_term = request.GET.get('term', '')
        cs_referral = request.GET.get('cs_referral', '')
        # taxon_details = request.GET.get('taxon_details', '')
        cs_community = request.GET.get('cs_community', '')
        if search_term:
            if cs_referral != '':
                #TODO may need to change the query for referral
                data = CommunityTaxonomy.objects.filter(community_name__icontains=search_term).values('id', 'community_name')[:10] 
                data_transform = [{'id': taxon['id'], 'text': taxon['community_name']} for taxon in data]
            elif cs_community != '':
                exculde_status = ['draft']
                data = CommunityTaxonomy.objects.filter(~Q(community__processing_status__in=exculde_status) & Q(community_name__icontains=search_term))[:10]
                data_transform = [{'id': community.community.id, 'text': community.community_name} for community in data]
            else:
                data = CommunityTaxonomy.objects.filter(community_name__icontains=search_term).values('id', 'community_name')[:10]
                data_transform = [{'id': taxon['id'], 'text': taxon['community_name']} for taxon in data]
            return Response({"results": data_transform})
        return Response()


class GetSpeciesFilterDict(views.APIView):
    def get(self, request, format=None):
        group_type = request.GET.get('group_type_name','')
        conservation_list_dict = []
        conservation_lists = ConservationList.objects.filter(applies_to_species=True)
        if conservation_lists:
            for choice in conservation_lists:
                conservation_list_dict.append({'id': choice.id,
                    'code': choice.code,
                    });
 
        conservation_category_list = []
        conservation_categories = ConservationCategory.objects.all()
        if conservation_categories:
            for choice in conservation_categories:
                conservation_category_list.append({
                    'id': choice.id,
                    'code': choice.code,
                    'conservation_list_id': choice.conservation_list_id,
                    });
        res_json = {
        "conservation_list_dict":conservation_list_dict,
        "conservation_category_list":conservation_category_list,
        }
        res_json = json.dumps(res_json)
        return HttpResponse(res_json, content_type='application/json')

class GetCommunityFilterDict(views.APIView):
    def get(self, request, format=None):
        group_type = request.GET.get('group_type_name','')
        conservation_list_dict = []
        conservation_lists = ConservationList.objects.filter(applies_to_communities=True)
        if conservation_lists:
            for choice in conservation_lists:
                conservation_list_dict.append({'id': choice.id,
                    'code': choice.code,
                    });
 
        conservation_category_list = []
        conservation_categories = ConservationCategory.objects.all()
        if conservation_categories:
            for choice in conservation_categories:
                conservation_category_list.append({
                    'id': choice.id,
                    'code': choice.code,
                    'conservation_list_id': choice.conservation_list_id,
                    });
        res_json = {
        "conservation_list_dict":conservation_list_dict,
        "conservation_category_list":conservation_category_list,
        }
        res_json = json.dumps(res_json)
        return HttpResponse(res_json, content_type='application/json')

class GetRegionDistrictFilterDict(views.APIView):
    def get(self, request, format=None):
        region_list = []
        regions = Region.objects.all()
        if regions:
            for region in regions:
                region_list.append({'id': region.id,
                    'name': region.name,
                    });
        district_list = []
        districts = District.objects.all()
        if districts:
            for district in districts:
                district_list.append({'id': district.id,
                    'name': district.name,
                    'region_id': district.region_id,
                    });
        res_json = {
        "region_list":region_list,
        "district_list":district_list,
        }
        res_json = json.dumps(res_json)
        return HttpResponse(res_json, content_type='application/json')


class TaxonomyViewSet(viewsets.ModelViewSet):
    queryset = Taxonomy.objects.all()
    serializer_class = TaxonomySerializer

    def get_queryset(self):
        qs = Taxonomy.objects.all()
        return qs

    @list_route(methods=['GET',], detail=False)
    def taxon_names(self, request, *args, **kwargs):
        qs = self.get_queryset()
        serializer = TaxonomySerializer(qs, context={'request': request}, many=True)
        return Response(serializer.data)

    #  not used for species profile now
    @list_route(methods=['GET',], detail=False)
    def flora_taxon_names(self, request, *args, **kwargs):
        qs = self.get_queryset()
        flora_kingdoms = Kingdom.objects.filter(grouptype__name=GroupType.GROUP_TYPE_FLORA).values_list('id', flat=True)
        qs = qs.filter(kingdom_fk_id__in=flora_kingdoms)
        # to filter taxon which are not used in Species yet
        # species = Species.objects.filter(~Q(taxonomy=None) , Q(group_type__name=GroupType.GROUP_TYPE_FLORA)).values_list('taxonomy', flat=True)
        # qs = qs.filter(~Q(id__in=species))
        serializer = TaxonomySerializer(qs, context={'request': request}, many=True)
        return Response(serializer.data)
    
    #  not used for species profile now
    @list_route(methods=['GET',], detail=False)
    def fauna_taxon_names(self, request, *args, **kwargs):
        qs = self.get_queryset()
        fauna_kingdoms = Kingdom.objects.filter(grouptype__name=GroupType.GROUP_TYPE_FAUNA).values_list('id', flat=True)
        qs=qs.filter(kingdom_fk_id__in=fauna_kingdoms)
        # to filter taxon which are not used in Species yet
        # species = Species.objects.filter(~Q(taxonomy=None) , Q(group_type__name=GroupType.GROUP_TYPE_FAUNA)).values_list('taxonomy', flat=True)
        # qs = qs.filter(~Q(id__in=species))
        serializer = TaxonomySerializer(qs, context={'request': request}, many=True)
        return Response(serializer.data)


class GetSpeciesProfileDict(views.APIView):
    def get(self, request, format=None):
        family_list = []
        # filter taxons that are having family_id and the fetch distinct family_id
        families_dict = Taxonomy.objects.filter(~Q(family_fk=None)).order_by().values_list('family_fk', flat=True).distinct()
        families = Taxonomy.objects.filter(id__in=families_dict)
        if families:
            for family in families:
                family_list.append({'id': family.id,
                    'name':family.scientific_name,
                    });
        phylo_group_list = []
        phylo_groups = ClassificationSystem.objects.all()
        if phylo_groups:
            for group in phylo_groups:
                phylo_group_list.append({'id': group.id,
                    'name':group.class_desc,
                    });
        genus_list = []
        generas = Genus.objects.all()
        if generas:
            for genus in generas:
                genus_list.append({'id': genus.id,
                    'name':genus.name,
                    });
        flowering_period_list = []
        periods = FloweringPeriod.objects.all()
        if periods:
            for option in periods:
                flowering_period_list.append({'id': option.id,
                    'name':option.period,
                    });
        fruiting_period_list = []
        periods = FruitingPeriod.objects.all()
        if periods:
            for option in periods:
                fruiting_period_list.append({'id': option.id,
                    'name':option.period,
                    });
        flora_recruitment_type_list = []
        types = FloraRecruitmentType.objects.all()
        if types:
            for option in types:
                flora_recruitment_type_list.append({'id': option.id,
                    'name':option.recruitment_type,
                    });
        root_morphology_list = []
        types = RootMorphology.objects.all()
        if types:
            for option in types:
                root_morphology_list.append({'id': option.id,
                    'name':option.name,
                    });
        post_fire_habitatat_interactions_list = []
        types = PostFireHabitatInteraction.objects.all()
        if types:
            for option in types:
                post_fire_habitatat_interactions_list.append({'id': option.id,
                    'name':option.name,
                    });
        breeding_period_list = []
        periods = BreedingPeriod.objects.all()
        if periods:
            for option in periods:
                breeding_period_list.append({'id': option.id,
                    'name':option.period,
                    });
        res_json = {
        "family_list": family_list,
        "genus_list": genus_list,
        "phylo_group_list": phylo_group_list,
        "flowering_period_list": flowering_period_list,
        "fruiting_period_list": fruiting_period_list,
        "flora_recruitment_type_list": flora_recruitment_type_list,
        "root_morphology_list": root_morphology_list,
        "post_fire_habitatat_interactions_list": post_fire_habitatat_interactions_list,
        "breeding_period_list": breeding_period_list,
        }
        res_json = json.dumps(res_json)
        return HttpResponse(res_json, content_type='application/json')


class CommunityTaxonomyViewSet(viewsets.ModelViewSet):
    queryset = CommunityTaxonomy.objects.all()
    serializer_class = CommunityTaxonomySerializer

    def get_queryset(self):
        qs = CommunityTaxonomy.objects.all()
        return qs

    # not used for community profile now
    @list_route(methods=['GET',], detail=False)
    def taxon_names(self, request, *args, **kwargs):
        qs = self.get_queryset()
        serializer = CommunityTaxonomySerializer(qs, context={'request': request}, many=True)
        return Response(serializer.data)

class GetCommunityProfileDict(views.APIView):
    def get(self, request, format=None):
        post_fire_habitatat_interactions_list = []
        types = PostFireHabitatInteraction.objects.all()
        if types:
            for option in types:
                post_fire_habitatat_interactions_list.append({'id': option.id,
                    'name':option.name,
                    });
        res_json = {
        "post_fire_habitatat_interactions_list": post_fire_habitatat_interactions_list,
        }
        res_json = json.dumps(res_json)
        return HttpResponse(res_json, content_type='application/json')


class SpeciesFilterBackend(DatatablesFilterBackend):
    def filter_queryset(self, request, queryset, view):
        total_count = queryset.count()
        # filter_group_type
        filter_group_type = request.GET.get('filter_group_type')
        if filter_group_type:
            queryset = queryset.filter(group_type__name=filter_group_type)
        # filter_scientific_name
        filter_scientific_name = request.GET.get('filter_scientific_name')
        if filter_scientific_name and not filter_scientific_name.lower() == 'all':
            queryset = queryset.filter(taxonomy=filter_scientific_name)

        filter_common_name = request.GET.get('filter_common_name')
        if filter_common_name and not filter_common_name.lower() == 'all':
            queryset = queryset.filter(taxonomy__vernaculars__id=filter_common_name)

        filter_phylogenetic_group = request.GET.get('filter_phylogenetic_group')
        if filter_phylogenetic_group and not filter_phylogenetic_group.lower() == 'all':
            queryset = queryset.filter(taxonomy__informal_groups__classification_system_fk_id=filter_phylogenetic_group)
        
        filter_family = request.GET.get('filter_family')
        if filter_family and not filter_family.lower() == 'all':
            queryset = queryset.filter(taxonomy__family_fk_id=filter_family)

        filter_genus = request.GET.get('filter_genus')
        if filter_genus and not filter_genus.lower() == 'all':
            queryset = queryset.filter(taxonomy__genus__id=filter_genus)
        
        filter_name_status = request.GET.get('filter_name_status')
        if filter_name_status and not filter_name_status.lower() == 'all':
            queryset = queryset.filter(taxonomy__name_currency=filter_name_status)

        filter_conservation_list = request.GET.get('filter_conservation_list')
        if filter_conservation_list and not filter_conservation_list.lower() == 'all':
            queryset = queryset.filter(conservation_status__conservation_list=filter_conservation_list).distinct()

        filter_conservation_category = request.GET.get('filter_conservation_category')
        if filter_conservation_category and not filter_conservation_category.lower() == 'all':
            queryset = queryset.filter(conservation_status__conservation_category=filter_conservation_category).distinct()

        filter_application_status = request.GET.get('filter_application_status')
        if filter_application_status and not filter_application_status.lower() == 'all':
                queryset = queryset.filter(processing_status=filter_application_status)

        filter_region = request.GET.get('filter_region')
        if filter_region and not filter_region.lower() == 'all':
            queryset = queryset.filter(region=filter_region)

        filter_district = request.GET.get('filter_district')
        if filter_district and not filter_district.lower() == 'all':
            queryset = queryset.filter(district=filter_district)

        getter = request.query_params.get
        fields = self.get_fields(getter)
        ordering = self.get_ordering(getter, fields)
        queryset = queryset.order_by(*ordering)
        if len(ordering):
            queryset = queryset.order_by(*ordering)

        try:
            queryset = super(SpeciesFilterBackend, self).filter_queryset(request, queryset, view)
        except Exception as e:
            print(e)
        setattr(view, '_datatables_total_count', total_count)
        return queryset

class SpeciesRenderer(DatatablesRenderer):
    def render(self, data, accepted_media_type=None, renderer_context=None):
        if 'view' in renderer_context and hasattr(renderer_context['view'], '_datatables_total_count'):
            data['recordsTotal'] = renderer_context['view']._datatables_total_count
        return super(SpeciesRenderer, self).render(data, accepted_media_type, renderer_context)

class SpeciesPaginatedViewSet(viewsets.ModelViewSet):
    filter_backends = (SpeciesFilterBackend,)
    pagination_class = DatatablesPageNumberPagination
    renderer_classes = (SpeciesRenderer,)
    queryset = Species.objects.none()
    serializer_class = ListSpeciesSerializer
    page_size = 10

    def get_queryset(self):
        request_user = self.request.user
        qs = Species.objects.none()

        if is_internal(self.request):
            qs = Species.objects.all()

        return qs

    @list_route(methods=['GET',], detail=False)
    def species_internal(self, request, *args, **kwargs):
        qs = self.get_queryset()
        qs = self.filter_queryset(qs)

        self.paginator.page_size = qs.count()
        result_page = self.paginator.paginate_queryset(qs, request)
        serializer = ListSpeciesSerializer(result_page, context={'request': request}, many=True)
        return self.paginator.get_paginated_response(serializer.data)
    
    @list_route(methods=['GET',], detail=False)
    def species_internal_export(self, request, *args, **kwargs):
        
        qs = self.get_queryset()
        qs = self.filter_queryset(qs)
        export_format = request.GET.get('export_format')
        allowed_fields = ['species_number', 'scientific_name', 'common_name', 'family', 'genus', 'phylogenetic_group', 'region', 'district', 'conservation_list', 'conservation_category', 'processing_status']

        serializer = ListSpeciesSerializer(qs, context={'request': request}, many=True)
        serialized_data = serializer.data

        try:
            filtered_data = []
            for obj in serialized_data:
                filtered_obj = {key: value for key, value in obj.items() if key in allowed_fields}
                filtered_data.append(filtered_obj)

            def flatten_dict(d, parent_key='', sep='_'):
                flattened_dict = {}
                for k, v in d.items():
                    new_key = parent_key + sep + k if parent_key else k
                    if isinstance(v, dict):
                        flattened_dict.update(flatten_dict(v, new_key, sep))
                    elif isinstance(v, QuerySet):
                        values = list(v.values_list('classification_system_fk_id__class_desc', flat=True))
                        flattened_dict[new_key] = ",".join(values)
                    else:
                        flattened_dict[new_key] = v
                return flattened_dict

            flattened_data = [flatten_dict(item) for item in filtered_data]
            df = pd.DataFrame(flattened_data)
            new_headings = ['Number', 'Scientific Name', 'Common Name', 'Family', 'Genera', 'Phylo Group', 'Region', 'District', 'Conservation List', 'Conservation Category', 'Processing Status']
            df.columns = new_headings
            column_order = ['Number', 'Scientific Name', 'Common Name', 'Phylo Group', 'Family', 'Genera', 'Conservation List', 'Conservation Category', 'Region', 'District', 'Processing Status']
            df = df[column_order]

            if export_format is not None:
                if export_format == "excel":
                    buffer = BytesIO()
                    workbook = Workbook()
                    sheet_name = 'Sheet1'
                    sheet = workbook.active
                    sheet.title = sheet_name

                    for row in dataframe_to_rows(df, index=False, header=True):
                        sheet.append(row)
                    for cell in sheet[1]:
                        cell.font = Font(bold=True)

                    workbook.save(buffer)
                    buffer.seek(0)
                    response = HttpResponse(buffer.read(), content_type='application/vnd.ms-excel')
                    response['Content-Disposition'] = 'attachment; filename=DBCA_Species.xlsx'
                    final_response = response
                    buffer.close()
                    return final_response
                
                elif export_format == "csv":
                    csv_data = df.to_csv(index=False)
                    response = HttpResponse(content_type='text/csv')
                    response['Content-Disposition'] = 'attachment; filename=DBCA_Species.csv'
                    response.write(csv_data)
                    return response
                
                else:
                    return Response(status=400, data="Format not valid")
        except:
            return Response(status=500, data="Internal Server Error")

class CommunitiesFilterBackend(DatatablesFilterBackend):
    def filter_queryset(self, request, queryset, view):
        total_count = queryset.count()
        
        # filter_group_type
        filter_group_type = request.GET.get('filter_group_type')
        if filter_group_type:
            queryset = queryset.filter(group_type__name=filter_group_type)
        
        #filter_community_migrated_id
        filter_community_migrated_id = request.GET.get('filter_community_migrated_id')
        if filter_community_migrated_id and not filter_community_migrated_id.lower() == 'all':
            queryset = queryset.filter(taxonomy=filter_community_migrated_id)

        # filter_community_name
        filter_community_name = request.GET.get('filter_community_name')
        if filter_community_name and not filter_community_name.lower() == 'all':
            queryset = queryset.filter(taxonomy=filter_community_name)

        filter_conservation_list = request.GET.get('filter_conservation_list')
        if filter_conservation_list and not filter_conservation_list.lower() == 'all':
            queryset = queryset.filter(conservation_status__conservation_list=filter_conservation_list).distinct()

        filter_conservation_category = request.GET.get('filter_conservation_category')
        if filter_conservation_category and not filter_conservation_category.lower() == 'all':
            queryset = queryset.filter(conservation_status__conservation_category=filter_conservation_category)

        filter_application_status = request.GET.get('filter_application_status')
        if filter_application_status and not filter_application_status.lower() == 'all':
                queryset = queryset.filter(processing_status=filter_application_status)

        filter_region = request.GET.get('filter_region')
        if filter_region and not filter_region.lower() == 'all':
            queryset = queryset.filter(region=filter_region)

        filter_district = request.GET.get('filter_district')
        if filter_district and not filter_district.lower() == 'all':
            queryset = queryset.filter(district=filter_district)

        getter = request.query_params.get
        fields = self.get_fields(getter)
        ordering = self.get_ordering(getter, fields)
        queryset = queryset.order_by(*ordering)
        if len(ordering):
            queryset = queryset.order_by(*ordering)

        try:
            queryset = super(CommunitiesFilterBackend, self).filter_queryset(request, queryset, view)
        except Exception as e:
            print(e)
        setattr(view, '_datatables_total_count', total_count)
        return queryset

class CommunitiesRenderer(DatatablesRenderer):
    def render(self, data, accepted_media_type=None, renderer_context=None):
        if 'view' in renderer_context and hasattr(renderer_context['view'], '_datatables_total_count'):
            data['recordsTotal'] = renderer_context['view']._datatables_total_count
        return super(CommunitiesRenderer, self).render(data, accepted_media_type, renderer_context)

class CommunitiesPaginatedViewSet(viewsets.ModelViewSet):
    filter_backends = (CommunitiesFilterBackend,)
    pagination_class = DatatablesPageNumberPagination
    renderer_classes = (CommunitiesRenderer,)
    queryset = Community.objects.none()
    serializer_class = ListCommunitiesSerializer
    page_size = 10

    def get_queryset(self):
        #request_user = self.request.user
        qs = Community.objects.none()

        if is_internal(self.request):
            qs = Community.objects.all()

        return qs

    @list_route(methods=['GET',], detail=False)
    def communities_internal(self, request, *args, **kwargs):
        qs = self.get_queryset()
        qs = self.filter_queryset(qs)

        self.paginator.page_size = qs.count()
        result_page = self.paginator.paginate_queryset(qs, request)
        serializer = ListCommunitiesSerializer(result_page, context={'request': request}, many=True)
        return self.paginator.get_paginated_response(serializer.data)
    
    @list_route(methods=['GET',], detail=False)
    def communities_internal_export(self, request, *args, **kwargs):
        
        qs = self.get_queryset()
        qs = self.filter_queryset(qs)
        export_format = request.GET.get('export_format')
        allowed_fields = ['conservation_status_number', 'community_number', 'community_migrated_id', 'community_name', 'region', 'district', 'conservation_list', 'conservation_category', 'processing_status']
        serializer = ListCommunitiesSerializer(qs, context={'request': request}, many=True)
        serialized_data = serializer.data

        try:
            filtered_data = []
            for obj in serialized_data:
                filtered_obj = {key: value for key, value in obj.items() if key in allowed_fields}
                filtered_data.append(filtered_obj)

            def flatten_dict(d, parent_key='', sep='_'):
                flattened_dict = {}
                for k, v in d.items():
                    new_key = parent_key + sep + k if parent_key else k
                    if isinstance(v, dict):
                        flattened_dict.update(flatten_dict(v, new_key, sep))
                    else:
                        flattened_dict[new_key] = v
                return flattened_dict

            flattened_data = [flatten_dict(item) for item in filtered_data]
            df = pd.DataFrame(flattened_data)
            new_headings = ['Number', 'Community Id', 'Community Name', 'Conservation List', 'Conservation Category', 'Region', 'District', 'Processing Status']
            df.columns = new_headings

            if export_format is not None:
                if export_format == "excel":
                    buffer = BytesIO()
                    workbook = Workbook()
                    sheet_name = 'Sheet1'
                    sheet = workbook.active
                    sheet.title = sheet_name

                    for row in dataframe_to_rows(df, index=False, header=True):
                        sheet.append(row)
                    for cell in sheet[1]:
                        cell.font = Font(bold=True)

                    workbook.save(buffer)
                    buffer.seek(0)
                    response = HttpResponse(buffer.read(), content_type='application/vnd.ms-excel')
                    response['Content-Disposition'] = 'attachment; filename=DBCA_Communities.xlsx'
                    final_response = response
                    buffer.close()
                    return final_response
                
                elif export_format == "csv":
                    csv_data = df.to_csv(index=False)
                    response = HttpResponse(content_type='text/csv')
                    response['Content-Disposition'] = 'attachment; filename=DBCA_Communities.csv'
                    response.write(csv_data)
                    return response
                
                else:
                    return Response(status=400, data="Format not valid")
        except:
            return Response(status=500, data="Internal Server Error")


class SpeciesViewSet(viewsets.ModelViewSet):
    queryset = Species.objects.none()
    serializer_class = InternalSpeciesSerializer
    lookup_field = 'id'

    def get_queryset(self):
        user = self.request.user
        if is_internal(self.request): #user.is_authenticated():
            qs= Species.objects.all()
            return qs
        return Species.objects.none()

    def get_serializer_class(self):
        try:
            return SpeciesSerializer
        except serializers.ValidationError:
            print(traceback.print_exc())
            raise
        except ValidationError as e:
            if hasattr(e,'error_dict'):
                raise serializers.ValidationError(repr(e.error_dict))
            else:
                if hasattr(e,'message'):
                    raise serializers.ValidationError(e.message)
        except Exception as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(str(e))

    @detail_route(methods=['GET',], detail=True)
    def internal_species(self, request, *args, **kwargs):
        instance = self.get_object()
        serializer = InternalSpeciesSerializer(instance,context={'request':request})
       
        res_json = {
         "species_obj":serializer.data
        }
        res_json = json.dumps(res_json)
        return HttpResponse(res_json, content_type='application/json')
        #return Response(d)
    
    @list_route(methods=['GET',], detail=False)
    def filter_list(self, request, *args, **kwargs):
        """ Used by the Related Items dashboard filters """
        related_type =  Species.RELATED_ITEM_CHOICES
        res_json = json.dumps(related_type) 
        return HttpResponse(res_json, content_type='application/json')

    # used for species field on community profile
    # not used at the moment as per requirements
    # @detail_route(methods=['GET',], detail=False)
    # @renderer_classes((JSONRenderer,))
    # def species_list(self, request, *args, **kwargs):
    #     # TODO filter Species that's approved(submitted) only 
    #     qs= Species.objects.all()
    #     qs= qs.filter(Q(processing_status='active'))
    #     serializer = SpeciesSerializer(qs, many=True)
    #     res_json = {
    #      "data":serializer.data
    #     }
    #     res_json = json.dumps(res_json)
    #     return HttpResponse(res_json, content_type='application/json')

    @detail_route(methods=['post'], detail=True)
    @renderer_classes((JSONRenderer,))
    def species_save(self, request, *args, **kwargs):
        try:
            with transaction.atomic():
                instance = self.get_object() 
                request_data = request.data
                if request_data['submitter']:
                    request.data['submitter'] = u'{}'.format(request_data['submitter'].get('id'))
                if(request_data.get('distribution')):
                    distribution_instance, created = SpeciesDistribution.objects.get_or_create(species=instance)
                    serializer = SpeciesDistributionSerializer(distribution_instance, data = request_data.get('distribution'))
                    serializer.is_valid(raise_exception=True)
                    if serializer.is_valid():
                        serializer.save()
                
                if(request_data.get('conservation_attributes')):
                    conservation_attributes_instance, created = SpeciesConservationAttributes.objects.get_or_create(species=instance)
                    serializer = SaveSpeciesConservationAttributesSerializer(conservation_attributes_instance, data = request_data.get('conservation_attributes'))
                    serializer.is_valid(raise_exception=True)
                    if serializer.is_valid():
                        serializer.save()

                serializer = SaveSpeciesSerializer(instance, data = request_data, partial=True)
                serializer.is_valid(raise_exception=True)
                if serializer.is_valid():
                    saved_instance = serializer.save()

                    instance.log_user_action(SpeciesUserAction.ACTION_SAVE_SPECIES.format(instance.species_number), request)

            return redirect(reverse('internal'))
        
        except serializers.ValidationError:
            print(traceback.print_exc())
            raise
        except ValidationError as e:
            raise serializers.ValidationError(repr(e.error_dict))
        except Exception as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(str(e))

    @detail_route(methods=['post'], detail=True)
    @renderer_classes((JSONRenderer,))
    def species_split_save(self, request, *args, **kwargs):
        try:
            with transaction.atomic():
                instance = self.get_object() 
                request_data = request.data
                if request_data['submitter']:
                    request.data['submitter'] = u'{}'.format(request_data['submitter'].get('id'))
                if(request_data.get('distribution')):
                    distribution_instance, created = SpeciesDistribution.objects.get_or_create(species=instance)
                    serializer = SpeciesDistributionSerializer(distribution_instance, data = request_data.get('distribution'))
                    serializer.is_valid(raise_exception=True)
                    if serializer.is_valid():
                        serializer.save()
                
                if(request_data.get('conservation_attributes')):
                    conservation_attributes_instance, created = SpeciesConservationAttributes.objects.get_or_create(species=instance)
                    serializer = SaveSpeciesConservationAttributesSerializer(conservation_attributes_instance, data = request_data.get('conservation_attributes'))
                    serializer.is_valid(raise_exception=True)
                    if serializer.is_valid():
                        serializer.save()

                serializer = SaveSpeciesSerializer(instance, data = request_data, partial=True)
                serializer.is_valid(raise_exception=True)
                if serializer.is_valid():
                    saved_instance = serializer.save()

                    instance.log_user_action(SpeciesUserAction.ACTION_SAVE_SPECIES.format(instance.species_number), request)

            return Response()
        
        except serializers.ValidationError:
            print(traceback.print_exc())
            raise
        except ValidationError as e:
            raise serializers.ValidationError(repr(e.error_dict))
        except Exception as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(str(e))

    @detail_route(methods=['post'], detail=True)
    @renderer_classes((JSONRenderer,))
    def submit(self, request, *args, **kwargs):
        try:
            instance = self.get_object()
            #instance.submit(request,self)
            species_form_submit(instance, request)
            instance.save()
            serializer = self.get_serializer(instance)
            return Response(serializer.data)
            # return redirect(reverse('internal'))
        except serializers.ValidationError:
            print(traceback.print_exc())
            raise
        except ValidationError as e:
            if hasattr(e,'error_dict'):
                raise serializers.ValidationError(repr(e.error_dict))
            else:
                if hasattr(e,'message'):
                    raise serializers.ValidationError(e.message)
        except Exception as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(str(e))

    # used to submit the new species created while spliting
    @detail_route(methods=['post'], detail=True)
    @renderer_classes((JSONRenderer,))
    def split_new_species_submit(self, request, *args, **kwargs):
        try:
            with transaction.atomic():
                instance = self.get_object()
                #instance.submit(request,self)
                species_form_submit(instance, request)
                # add parent id to new species instance
                parent_species_arr= request.data.get('parent_species')
                for species in parent_species_arr:
                    species_instance = Species.objects.get(id = species.get('id'))
                    instance.parent_species.add(species_instance)
                # copy/clone the original species document and create new for new split species
                instance.clone_documents(request)
                instance.clone_threats(request)
                instance.save()

            serializer = self.get_serializer(instance)
            return Response(serializer.data)
            # return redirect(reverse('internal'))
        except serializers.ValidationError:
            print(traceback.print_exc())
            raise
        except ValidationError as e:
            if hasattr(e,'error_dict'):
                raise serializers.ValidationError(repr(e.error_dict))
            else:
                if hasattr(e,'message'):
                    raise serializers.ValidationError(e.message)
        except Exception as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(str(e))

    # used to submit the new species created while combining
    @detail_route(methods=['post'], detail=True)
    @renderer_classes((JSONRenderer,))
    def combine_new_species_submit(self, request, *args, **kwargs):
        try:
            with transaction.atomic():
                instance = self.get_object()
                #instance.submit(request,self)
                species_form_submit(instance, request)
                
                # copy/clone the original species document and create new for new split species
                instance.clone_documents(request)
                instance.clone_threats(request)
                instance.save()
                # add parent ids to new species instance
                parent_species_arr= request.data.get('parent_species')
                for species in parent_species_arr:
                    parent_instance = Species.objects.get(id = species.get('id'))
                    instance.parent_species.add(parent_instance)
                    # set the original species from the combine list to historical and its conservation status to 'closed'
                    combine_species_original_submit(parent_instance,request)
                
                #  send the combine species email notification
                send_species_combine_email_notification(request, instance)

            serializer = self.get_serializer(instance)
            return Response(serializer.data)
            # return redirect(reverse('internal'))
        except serializers.ValidationError:
            print(traceback.print_exc())
            raise
        except ValidationError as e:
            if hasattr(e,'error_dict'):
                raise serializers.ValidationError(repr(e.error_dict))
            else:
                if hasattr(e,'message'):
                    raise serializers.ValidationError(e.message)
        except Exception as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(str(e))

    # Used to submit the original species after split data is submitted 
    @detail_route(methods=['post'], detail=True)
    @renderer_classes((JSONRenderer,))
    def change_status_historical(self, request, *args, **kwargs):
        try:
            species_instance = self.get_object()
            species_instance.processing_status = 'historical'

            ret1 = send_species_split_email_notification(request, species_instance)
            if ret1:
                species_instance.save()
                 # change current active conservation status of the original species to inactive
                try:
                    if species_instance.processing_status == 'historical':
                        # TODO if the cs of species is in middle of workflow, then?
                        species_cons_status = ConservationStatus.objects.get(species=species_instance, processing_status='approved')
                        if species_cons_status:
                            species_cons_status.customer_status='closed'
                            species_cons_status.processing_status='closed'
                            species_cons_status.save()
                            #add the log_user_action
                            species_cons_status.log_user_action(ConservationStatusUserAction.ACTION_CLOSE_CONSERVATIONSTATUS.format(species_cons_status.conservation_status_number),request)
                except ConservationStatus.DoesNotExist:
                    pass

                serializer = self.get_serializer(species_instance)
                return Response(serializer.data)
            # return redirect(reverse('internal'))
        except serializers.ValidationError:
            print(traceback.print_exc())
            raise
        except ValidationError as e:
            if hasattr(e,'error_dict'):
                raise serializers.ValidationError(repr(e.error_dict))
            else:
                if hasattr(e,'message'):
                    raise serializers.ValidationError(e.message)
        except Exception as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(str(e))

    @detail_route(methods=['GET',], detail=True)
    @renderer_classes((JSONRenderer,))
    def rename_deep_copy(self, request, *args, **kwargs):
        try:
            with transaction.atomic():
                instance = self.get_object()
                # related items to instance that needs to create for new rename instance as well
                instance_documents = SpeciesDocument.objects.filter(species=instance.id)
                instance_threats = ConservationThreat.objects.filter(species=instance.id)
                instance_conservation_attributes = SpeciesConservationAttributes.objects.filter(species=instance.id)
                instance_distribution = SpeciesDistribution.objects.filter(species=instance.id)

                # clone the species instance into new rename instance
                new_rename_instance = instance
                new_rename_instance.id = None
                new_rename_instance.taxonomy_id = None
                new_rename_instance.species_number = ''
                new_rename_instance.processing_status = 'draft'
                new_rename_instance.save()

                for new_document in instance_documents:
                    new_doc_instance= new_document
                    new_doc_instance.species = new_rename_instance
                    new_doc_instance.id = None
                    new_doc_instance.document_number = ''
                    new_doc_instance._file.name = u'boranga/species/{}/species_documents/{}'.format(new_rename_instance.id, new_doc_instance.name)
                    new_doc_instance.can_delete = True
                    new_doc_instance.save()
                    new_doc_instance.species.log_user_action(SpeciesUserAction.ACTION_ADD_DOCUMENT.format(new_doc_instance.document_number,new_doc_instance.species.species_number),request)

                    check_path = os.path.exists('private-media/boranga/species/{}/species_documents/'.format(new_rename_instance.id))
                    if check_path == True:
                        # copy documents on file system
                        subprocess.call('cp -p private-media/boranga/species/{}/species_documents/{}  private-media/boranga/species/{}/species_documents/'.format(instance.id, new_doc_instance.name, new_rename_instance.id), shell=True)
                    else:
                        # create new directory
                        os.makedirs('private-media/boranga/species/{}/species_documents/'.format(new_rename_instance.id), mode=0o777)
                        # then copy documents on file system
                        subprocess.call('cp -p private-media/boranga/species/{}/species_documents/{}  private-media/boranga/species/{}/species_documents/'.format(instance.id, new_doc_instance.name, new_rename_instance.id), shell=True)

                for new_threat in instance_threats:
                    new_threat_instance = new_threat
                    new_threat_instance.species = new_rename_instance
                    new_threat_instance.id = None
                    new_threat_instance.threat_number = ''
                    new_threat_instance.save()
                    new_threat_instance.species.log_user_action(SpeciesUserAction.ACTION_ADD_THREAT.format(new_threat_instance.threat_number,new_threat_instance.species.species_number),request)
                
                for new_cons_attr in instance_conservation_attributes:
                    new_cons_attr_instance = new_cons_attr
                    new_cons_attr_instance.species = new_rename_instance
                    new_cons_attr_instance.id = None
                    new_cons_attr_instance.save()
                
                for new_distribution in instance_distribution:
                    new_distribution.species = new_rename_instance
                    new_distribution.id = None
                    new_distribution.save()
                
                serializer = InternalSpeciesSerializer(new_rename_instance,context={'request':request})
                res_json = {
                "species_obj":serializer.data
                }
                res_json = json.dumps(res_json)
                return HttpResponse(res_json, content_type='application/json')

        except serializers.ValidationError:
            print(traceback.print_exc())
            raise
        except ValidationError as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(repr(e.error_dict))
        except Exception as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(str(e))
    
    # used to submit the new species created while combining
    @detail_route(methods=['post'], detail=True)
    @renderer_classes((JSONRenderer,))
    def rename_new_species_submit(self, request, *args, **kwargs):
        try:
            with transaction.atomic():
                instance = self.get_object()
                species_form_submit(instance, request)
                # add parent ids to new species instance
                parent_species_arr= request.data.get('parent_species')
                for species in parent_species_arr:
                    parent_instance = Species.objects.get(id = species.get('id'))
                    instance.parent_species.add(parent_instance)
                    # set the original species from the rename  to historical and its conservation status to 'closed'
                    rename_species_original_submit(parent_instance,request)
                
            serializer = self.get_serializer(instance)
            return Response(serializer.data)
            # return redirect(reverse('internal'))
        except serializers.ValidationError:
            print(traceback.print_exc())
            raise
        except ValidationError as e:
            if hasattr(e,'error_dict'):
                raise serializers.ValidationError(repr(e.error_dict))
            else:
                if hasattr(e,'message'):
                    raise serializers.ValidationError(e.message)
        except Exception as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(str(e))



    def create(self, request, *args, **kwargs):
        try:
            with transaction.atomic():
                request_data = request.data
                serializer = CreateSpeciesSerializer(data=request_data)
                serializer.is_valid(raise_exception=True)
                if serializer.is_valid():
                    new_instance = serializer.save()
                    new_returned = serializer.data

                    data={
                        'species_id': new_instance.id
                    }
                    
                    # create SpeciesConservationAttributes for new instance
                    serializer=SaveSpeciesConservationAttributesSerializer(data=data)
                    serializer.is_valid(raise_exception=True)
                    serializer.save()

                    # create SpeciesDistribution for new instance
                    serializer=SaveSpeciesDistributionSerializer(data=data)
                    serializer.is_valid(raise_exception=True)
                    serializer.save()
                    
                    headers = self.get_success_headers(serializer.data)
                    return Response(
                        new_returned,
                        status=status.HTTP_201_CREATED,
                        headers=headers
                    )
        except serializers.ValidationError:
            print(traceback.print_exc())
            raise
        except ValidationError as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(repr(e.error_dict))
        except Exception as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(str(e))

    @detail_route(methods=['GET',], detail=True)
    def documents(self, request, *args, **kwargs):
        try:
            instance = self.get_object()
            qs = instance.species_documents.all()
            qs = qs.order_by('-uploaded_date')
            serializer = SpeciesDocumentSerializer(qs,many=True, context={'request':request})
            return Response(serializer.data)
        except serializers.ValidationError:
            print(traceback.print_exc())
            raise
        except ValidationError as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(repr(e.error_dict))
        except Exception as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(str(e))

    @detail_route(methods=['GET',], detail=True)
    def threats(self, request, *args, **kwargs):
        try:
            instance = self.get_object()
            qs = instance.species_threats.all()
            qs = qs.order_by('-date_observed')
            serializer = ConservationThreatSerializer(qs,many=True, context={'request':request})
            return Response(serializer.data)
        except serializers.ValidationError:
            print(traceback.print_exc())
            raise
        except ValidationError as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(repr(e.error_dict))
        except Exception as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(str(e))

    @detail_route(methods=['GET',], detail=True)
    def comms_log(self, request, *args, **kwargs):
        try:
            instance = self.get_object()
            qs = instance.comms_logs.all()
            serializer = SpeciesLogEntrySerializer(qs,many=True)
            return Response(serializer.data)
        except serializers.ValidationError:
            print(traceback.print_exc())
            raise
        except ValidationError as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(repr(e.error_dict))
        except Exception as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(str(e))

    @detail_route(methods=['POST',], detail=True)
    @renderer_classes((JSONRenderer,))
    def add_comms_log(self, request, *args, **kwargs):
        try:
            with transaction.atomic():
                instance = self.get_object()
                mutable=request.data._mutable
                request.data._mutable=True
                request.data['species'] = u'{}'.format(instance.id)
                request.data['staff'] = u'{}'.format(request.user.id)
                request.data._mutable=mutable
                serializer = SpeciesLogEntrySerializer(data=request.data)
                serializer.is_valid(raise_exception=True)
                comms = serializer.save()
                # Save the files
                for f in request.FILES:
                    document = comms.documents.create()
                    document.name = str(request.FILES[f])
                    document._file = request.FILES[f]
                    document.save()
                # End Save Documents

                return Response(serializer.data)
        except serializers.ValidationError:
            print(traceback.print_exc())
            raise
        except ValidationError as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(repr(e.error_dict))
        except Exception as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(str(e))

    @detail_route(methods=['GET',], detail=True)
    def action_log(self, request, *args, **kwargs):
        try:
            instance = self.get_object()
            qs = instance.action_logs.all()
            serializer = SpeciesUserActionSerializer(qs,many=True)
            return Response(serializer.data)
        except serializers.ValidationError:
            print(traceback.print_exc())
            raise
        except ValidationError as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(repr(e.error_dict))
        except Exception as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(str(e))
    
    @detail_route(methods=["get"], detail=True)
    @basic_exception_handler
    def get_related_items(self, request, *args, **kwargs):
        instance = self.get_object()
        related_filter_type= request.GET.get('related_filter_type')
        related_items = instance.get_related_items(related_filter_type)
        serializer = RelatedItemsSerializer(related_items, many=True)
        return Response(serializer.data)

    @detail_route(methods=['POST',], detail=True)
    def upload_image(self, request, *args, **kwargs):
        try:
            instance = self.get_object()
            instance.upload_image(request)
            with transaction.atomic():
                instance.save()
                instance.log_user_action(SpeciesUserAction.ACTION_IMAGE_UPDATE.format(
                '{} '.format(instance.id)), request)
            serializer = InternalSpeciesSerializer(instance, context={'request':request}, partial=True)
            return Response(serializer.data)
        except serializers.ValidationError:
            print(traceback.print_exc())
            raise
        except ValidationError as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(repr(e.error_dict))
        except Exception as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(str(e))

    @detail_route(methods=['POST',], detail=True)
    def delete_image(self, request, *args, **kwargs):
        try:
            instance = self.get_object()
            #instance.upload_image(request)
            with transaction.atomic():
                instance.image_doc=None
                instance.save()
                instance.log_user_action(SpeciesUserAction.ACTION_IMAGE_DELETE.format(
                '{} '.format(instance.id)), request)
            serializer = InternalSpeciesSerializer(instance, context={'request':request}, partial=True)
            return Response(serializer.data)
        except serializers.ValidationError:
            print(traceback.print_exc())
            raise
        except ValidationError as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(repr(e.error_dict))
        except Exception as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(str(e))
    


class CommunityViewSet(viewsets.ModelViewSet):
    queryset = Community.objects.none()
    serializer_class = InternalCommunitySerializer
    lookup_field = 'id'

    def get_queryset(self):
        user = self.request.user
        if is_internal(self.request): #user.is_authenticated():
            qs= Community.objects.all()
            return qs
        return Community.objects.none()

    def get_serializer_class(self):
        try:
            return CommunitySerializer
        except serializers.ValidationError:
            print(traceback.print_exc())
            raise
        except ValidationError as e:
            if hasattr(e,'error_dict'):
                raise serializers.ValidationError(repr(e.error_dict))
            else:
                if hasattr(e,'message'):
                    raise serializers.ValidationError(e.message)
        except Exception as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(str(e))

    @detail_route(methods=['GET',], detail=True)
    def internal_community(self, request, *args, **kwargs):
        instance = self.get_object()
        serializer = InternalCommunitySerializer(instance,context={'request':request})

        res_json = {
         "community_obj":serializer.data
        }
        res_json = json.dumps(res_json)
        return HttpResponse(res_json, content_type='application/json')
        #return Response(d)
    
    @list_route(methods=['GET',], detail=False)
    def filter_list(self, request, *args, **kwargs):
        """ Used by the Related Items dashboard filters """
        related_type =  Community.RELATED_ITEM_CHOICES 
        res_json = json.dumps(related_type) 
        return HttpResponse(res_json, content_type='application/json')


    @detail_route(methods=['post'], detail=True)
    @renderer_classes((JSONRenderer,))
    def community_save(self, request, *args, **kwargs):
        try:
            with transaction.atomic():
                instance = self.get_object()
                request_data = request.data
                if request_data['submitter']:
                    request.data['submitter'] = u'{}'.format(request_data['submitter'].get('id'))
                # if(request_data.get('species')):
                #     species = request_data.get('species')
                #     instance.species.clear()  # first clear all the species set relatedM:M to community instance
                #     for species_id in species:
                #         species_instance = Species.objects.get(pk=species_id)
                #         instance.species.add(species_instance)

                if(request_data.get('taxonomy_details')):
                    taxonomy_instance, created = CommunityTaxonomy.objects.get_or_create(community=instance)
                    serializer = SaveCommunityTaxonomySerializer(taxonomy_instance, data = request_data.get('taxonomy_details'))
                    serializer.is_valid(raise_exception=True)
                    if serializer.is_valid():
                        serializer.save()

                if(request_data.get('distribution')):
                    distribution_instance, created = CommunityDistribution.objects.get_or_create(community=instance)
                    serializer = CommunityDistributionSerializer(distribution_instance, data = request_data.get('distribution'))
                    serializer.is_valid(raise_exception=True)
                    if serializer.is_valid():
                        serializer.save()

                if(request_data.get('conservation_attributes')):
                    conservation_attributes_instance, created = CommunityConservationAttributes.objects.get_or_create(community=instance)
                    serializer = SaveCommunityConservationAttributesSerializer(conservation_attributes_instance, data = request_data.get('conservation_attributes'))
                    serializer.is_valid(raise_exception=True)
                    if serializer.is_valid():
                        serializer.save()

                serializer = SaveCommunitySerializer(instance, data = request_data)
                serializer.is_valid(raise_exception=True)
                if serializer.is_valid():
                    saved_instance = serializer.save()
                    
                    instance.log_user_action(CommunityUserAction.ACTION_SAVE_COMMUNITY.format(instance.community_number), request)
            return redirect(reverse('internal'))
                    
        except serializers.ValidationError:
            print(traceback.print_exc())
            raise
        except ValidationError as e:
            raise serializers.ValidationError(repr(e.error_dict))
        except Exception as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(str(e))

    @detail_route(methods=['post'], detail=True)
    @renderer_classes((JSONRenderer,))
    def submit(self, request, *args, **kwargs):
        try:
            instance = self.get_object()
            #instance.submit(request,self)
            community_form_submit(instance, request)
            instance.save()
            serializer = self.get_serializer(instance)
            return Response(serializer.data)
            # return redirect(reverse('internal'))
        except serializers.ValidationError:
            print(traceback.print_exc())
            raise
        except ValidationError as e:
            if hasattr(e,'error_dict'):
                raise serializers.ValidationError(repr(e.error_dict))
            else:
                if hasattr(e,'message'):
                    raise serializers.ValidationError(e.message)
        except Exception as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(str(e))

    def create(self, request, *args, **kwargs):
        try:
            with transaction.atomic():
                request_data = request.data
                serializer = CreateCommunitySerializer(data=request_data)
                serializer.is_valid(raise_exception=True)
                if serializer.is_valid():
                    new_instance = serializer.save()
                    new_returned = serializer.data

                    data={
                        'community_id': new_instance.id
                    }
                    # create CommunityTaxonomy for new instance
                    serializer=SaveCommunityTaxonomySerializer(data=data)
                    serializer.is_valid(raise_exception=True)
                    serializer.save()

                    # create CommunityDistribution for new instance
                    serializer=SaveCommunityDistributionSerializer(data=data)
                    serializer.is_valid(raise_exception=True)
                    serializer.save()

                    # create CommunityConservationAttributes for new instance
                    serializer=SaveCommunityConservationAttributesSerializer(data=data)
                    serializer.is_valid(raise_exception=True)
                    serializer.save()
                    
                    headers = self.get_success_headers(serializer.data)
                    return Response(
                        new_returned,
                        status=status.HTTP_201_CREATED,
                        headers=headers
                    )
        except serializers.ValidationError:
            print(traceback.print_exc())
            raise
        except ValidationError as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(repr(e.error_dict))
        except Exception as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(str(e))

    @detail_route(methods=['GET',], detail=True)
    def documents(self, request, *args, **kwargs):
        try:
            instance = self.get_object()
            qs = instance.community_documents.all()
            qs = qs.order_by('-uploaded_date')
            serializer = CommunityDocumentSerializer(qs,many=True, context={'request':request})
            return Response(serializer.data)
        except serializers.ValidationError:
            print(traceback.print_exc())
            raise
        except ValidationError as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(repr(e.error_dict))
        except Exception as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(str(e))

    @detail_route(methods=['GET',], detail=True)
    def threats(self, request, *args, **kwargs):
        try:
            instance = self.get_object()
            qs = instance.community_threats.all()
            qs = qs.order_by('-date_observed')
            serializer = ConservationThreatSerializer(qs,many=True, context={'request':request})
            return Response(serializer.data)
        except serializers.ValidationError:
            print(traceback.print_exc())
            raise
        except ValidationError as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(repr(e.error_dict))
        except Exception as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(str(e))
    
    @detail_route(methods=["get"], detail=True)
    @basic_exception_handler
    def get_related_items(self, request, *args, **kwargs):
        instance = self.get_object()
        related_filter_type= request.GET.get('related_filter_type')
        related_items = instance.get_related_items(related_filter_type)
        serializer = RelatedItemsSerializer(related_items, many=True)
        return Response(serializer.data)

    @detail_route(methods=['GET',], detail=True)
    def comms_log(self, request, *args, **kwargs):
        try:
            instance = self.get_object()
            qs = instance.comms_logs.all()
            serializer = CommunityLogEntrySerializer(qs,many=True)
            return Response(serializer.data)
        except serializers.ValidationError:
            print(traceback.print_exc())
            raise
        except ValidationError as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(repr(e.error_dict))
        except Exception as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(str(e))

    @detail_route(methods=['POST',], detail=True)
    @renderer_classes((JSONRenderer,))
    def add_comms_log(self, request, *args, **kwargs):
        try:
            with transaction.atomic():
                instance = self.get_object()
                mutable=request.data._mutable
                request.data._mutable=True
                request.data['community'] = u'{}'.format(instance.id)
                request.data['staff'] = u'{}'.format(request.user.id)
                request.data._mutable=mutable
                serializer = CommunityLogEntrySerializer(data=request.data)
                serializer.is_valid(raise_exception=True)
                comms = serializer.save()
                # Save the files
                for f in request.FILES:
                    document = comms.documents.create()
                    document.name = str(request.FILES[f])
                    document._file = request.FILES[f]
                    document.save()
                # End Save Documents

                return Response(serializer.data)
        except serializers.ValidationError:
            print(traceback.print_exc())
            raise
        except ValidationError as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(repr(e.error_dict))
        except Exception as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(str(e))

    @detail_route(methods=['GET',], detail=True)
    def action_log(self, request, *args, **kwargs):
        try:
            instance = self.get_object()
            qs = instance.action_logs.all()
            serializer = CommunityUserActionSerializer(qs,many=True)
            return Response(serializer.data)
        except serializers.ValidationError:
            print(traceback.print_exc())
            raise
        except ValidationError as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(repr(e.error_dict))
        except Exception as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(str(e))

    @detail_route(methods=['POST',], detail=True)
    def upload_image(self, request, *args, **kwargs):
        try:
            instance = self.get_object()
            #import ipdb; ipdb.set_trace()
            instance.upload_image(request)
            with transaction.atomic():
                instance.save()
                instance.log_user_action(CommunityUserAction.ACTION_IMAGE_UPDATE.format(
                '{} '.format(instance.id)), request)
            serializer = InternalCommunitySerializer(instance, context={'request':request}, partial=True)
            return Response(serializer.data)
        except serializers.ValidationError:
            print(traceback.print_exc())
            raise
        except ValidationError as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(repr(e.error_dict))
        except Exception as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(str(e))

    @detail_route(methods=['POST',], detail=True)
    def delete_image(self, request, *args, **kwargs):
        try:
            instance = self.get_object()
            #import ipdb; ipdb.set_trace()
            #instance.upload_image(request)
            with transaction.atomic():
                instance.image_doc=None
                instance.save()
                instance.log_user_action(CommunityUserAction.ACTION_IMAGE_DELETE.format(
                '{} '.format(instance.id)), request)
            serializer = InternalCommunitySerializer(instance, context={'request':request}, partial=True)
            return Response(serializer.data)
        except serializers.ValidationError:
            print(traceback.print_exc())
            raise
        except ValidationError as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(repr(e.error_dict))
        except Exception as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(str(e))
    


class DocumentCategoryViewSet(viewsets.ModelViewSet):
    queryset = DocumentCategory.objects.all()

    def get_queryset(self):
        return DocumentCategory.objects.none()

    @list_route(methods=['GET', ], detail = False)    
    def document_category_choices(self, request, *args, **kwargs):
        res_obj = [] 
        for choice in DocumentCategory.objects.all():
            res_obj.append({'id': choice.id, 'name': choice.document_category_name})
        res_json = json.dumps(res_obj)
        return HttpResponse(res_json, content_type='application/json')


class DocumentSubCategoryViewSet(viewsets.ModelViewSet):
    queryset = DocumentSubCategory.objects.all()

    def get_queryset(self):
        return DocumentSubCategory.objects.none()

    @list_route(methods=['GET', ], detail = False)    
    def document_sub_category_choices(self, request, *args, **kwargs):
        res_obj = [] 
        for choice in DocumentSubCategory.objects.all():
            res_obj.append({'id': choice.id, 'name': choice.document_sub_category_name, 'category_id': choice.document_category_id,})
        res_json = json.dumps(res_obj)
        return HttpResponse(res_json, content_type='application/json')


class SpeciesDocumentViewSet(viewsets.ModelViewSet):
    queryset = SpeciesDocument.objects.all().order_by('id')
    serializer_class = SpeciesDocumentSerializer

    @detail_route(methods=['GET',], detail=True)
    def discard(self, request, *args, **kwargs):
        try:
            instance = self.get_object()
            instance.visible = False
            instance.save()
            instance.species.log_user_action(SpeciesUserAction.ACTION_DISCARD_DOCUMENT.format(instance.document_number,instance.species.species_number),request)
            serializer = self.get_serializer(instance)
            return Response(serializer.data)
        except serializers.ValidationError:
            print(traceback.print_exc())
            raise
        except ValidationError as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(repr(e.error_dict))
        except Exception as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(str(e))

    @detail_route(methods=['GET',], detail=True)
    def reinstate(self, request, *args, **kwargs):
        try:
            instance = self.get_object()
            instance.visible = True
            instance.save()
            serializer = self.get_serializer(instance)
            instance.species.log_user_action(SpeciesUserAction.ACTION_REINSTATE_DOCUMENT.format(instance.document_number,instance.species.species_number),request)
            return Response(serializer.data)
        except serializers.ValidationError:
            print(traceback.print_exc())
            raise
        except ValidationError as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(repr(e.error_dict))
        except Exception as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(str(e))

    # @detail_route(methods=['POST',], detail=True)
    # @renderer_classes((JSONRenderer,))
    # def delete_document(self, request, *args, **kwargs):
    #     try:
    #         instance = self.get_object()
    #         RequirementDocument.objects.get(id=request.data.get('id')).delete()
    #         return Response([dict(id=i.id, name=i.name,_file=i._file.url) for i in instance.requirement_documents.all()])
    #     except serializers.ValidationError:
    #         print(traceback.print_exc())
    #         raise
    #     except ValidationError as e:
    #         print(traceback.print_exc())
    #         raise serializers.ValidationError(repr(e.error_dict))
    #     except Exception as e:
    #         print(traceback.print_exc())
    #         raise serializers.ValidationError(str(e))

    def update(self, request, *args, **kwargs):
        try:
            instance = self.get_object()
            serializer = SaveSpeciesDocumentSerializer(instance, data=json.loads(request.data.get('data')))
            serializer.is_valid(raise_exception=True)
            serializer.save()
            instance.add_documents(request)
            instance.species.log_user_action(SpeciesUserAction.ACTION_UPDATE_DOCUMENT.format(instance.document_number,instance.species.species_number),request)
            return Response(serializer.data)
        except Exception as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(str(e))


    def create(self, request, *args, **kwargs):
        try:
            serializer = SaveSpeciesDocumentSerializer(data= json.loads(request.data.get('data')))
            serializer.is_valid(raise_exception = True)
            instance = serializer.save()
            instance.add_documents(request)
            instance.species.log_user_action(SpeciesUserAction.ACTION_ADD_DOCUMENT.format(instance.document_number,instance.species.species_number),request)
            return Response(serializer.data)
        except serializers.ValidationError:
            print(traceback.print_exc())
            raise
        except ValidationError as e:
            if hasattr(e,'error_dict'):
                raise serializers.ValidationError(repr(e.error_dict))
            else:
                if hasattr(e,'message'):
                    raise serializers.ValidationError(e.message)
        except Exception as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(str(e))


class CommunityDocumentViewSet(viewsets.ModelViewSet):
    queryset = CommunityDocument.objects.all().order_by('id')
    serializer_class = CommunityDocumentSerializer

    @detail_route(methods=['GET',], detail=True)
    def discard(self, request, *args, **kwargs):
        try:
            instance = self.get_object()
            instance.visible = False
            instance.save()
            instance.community.log_user_action(CommunityUserAction.ACTION_DISCARD_DOCUMENT.format(instance.document_number,instance.community.community_number),request)
            serializer = self.get_serializer(instance)
            return Response(serializer.data)
        except serializers.ValidationError:
            print(traceback.print_exc())
            raise
        except ValidationError as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(repr(e.error_dict))
        except Exception as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(str(e))

    @detail_route(methods=['GET',], detail=True)
    def reinstate(self, request, *args, **kwargs):
        try:
            instance = self.get_object()
            instance.visible = True
            instance.save()
            instance.community.log_user_action(CommunityUserAction.ACTION_REINSTATE_DOCUMENT.format(instance.document_number,instance.community.community_number),request)
            serializer = self.get_serializer(instance)
            return Response(serializer.data)
        except serializers.ValidationError:
            print(traceback.print_exc())
            raise
        except ValidationError as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(repr(e.error_dict))
        except Exception as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(str(e))

    # @detail_route(methods=['POST',], detail=True)
    # @renderer_classes((JSONRenderer,))
    # def delete_document(self, request, *args, **kwargs):
    #     try:
    #         instance = self.get_object()
    #         RequirementDocument.objects.get(id=request.data.get('id')).delete()
    #         return Response([dict(id=i.id, name=i.name,_file=i._file.url) for i in instance.requirement_documents.all()])
    #     except serializers.ValidationError:
    #         print(traceback.print_exc())
    #         raise
    #     except ValidationError as e:
    #         print(traceback.print_exc())
    #         raise serializers.ValidationError(repr(e.error_dict))
    #     except Exception as e:
    #         print(traceback.print_exc())
    #         raise serializers.ValidationError(str(e))

    def update(self, request, *args, **kwargs):
        try:
            instance = self.get_object()
            serializer = SaveCommunityDocumentSerializer(instance, data=json.loads(request.data.get('data')))
            serializer.is_valid(raise_exception=True)
            serializer.save()
            instance.add_documents(request)
            instance.community.log_user_action(CommunityUserAction.ACTION_UPDATE_DOCUMENT.format(instance.document_number,instance.community.community_number),request)
            return Response(serializer.data)
        except Exception as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(str(e))


    def create(self, request, *args, **kwargs):
        try:
            serializer = SaveCommunityDocumentSerializer(data= json.loads(request.data.get('data')))
            serializer.is_valid(raise_exception = True)
            instance = serializer.save()
            instance.add_documents(request)
            instance.community.log_user_action(CommunityUserAction.ACTION_ADD_DOCUMENT.format(instance.document_number,instance.community.community_number),request)
            return Response(serializer.data)
        except serializers.ValidationError:
            print(traceback.print_exc())
            raise
        except ValidationError as e:
            if hasattr(e,'error_dict'):
                raise serializers.ValidationError(repr(e.error_dict))
            else:
                if hasattr(e,'message'):
                    raise serializers.ValidationError(e.message)
        except Exception as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(str(e))


class ConservationThreatViewSet(viewsets.ModelViewSet):
    queryset = ConservationThreat.objects.all().order_by('id')
    serializer_class = ConservationThreatSerializer

    #used for Threat Form dropdown lists 
    @list_route(methods=['GET',], detail=False)
    def threat_list_of_values(self, request, *args, **kwargs):
        """ Used by the internal threat form """
        threat_category_lists = []
        threat_categories = ThreatCategory.objects.all()
        if threat_categories:
            for choice in threat_categories:
                threat_category_lists.append({'id': choice.id,
                    'name': choice.name,
                    });

        current_impact_lists = []
        current_impacts = CurrentImpact.objects.all()
        if current_impacts:
            for choice in current_impacts:
                current_impact_lists.append({'id': choice.id,
                    'name': choice.name,
                    });
        potential_impact_lists = []
        potential_impacts = PotentialImpact.objects.all()
        if current_impacts:
            for choice in potential_impacts:
                potential_impact_lists.append({'id': choice.id,
                    'name': choice.name,
                    });
        potential_threat_onset_lists = []
        potential_threats = PotentialThreatOnset.objects.all()
        if potential_threats:
            for choice in potential_threats:
                potential_threat_onset_lists.append({'id': choice.id,
                    'name': choice.name,
                    });
        threat_agent_lists = []
        threat_agents = ThreatAgent.objects.all()
        if threat_agents:
            for choice in threat_agents:
                threat_agent_lists.append({'id': choice.id,
                    'name': choice.name,
                    });
        res_json = {
            "threat_category_lists":threat_category_lists,
            "current_impact_lists":current_impact_lists,
            "potential_impact_lists":potential_impact_lists,
            "potential_threat_onset_lists": potential_threat_onset_lists,
            "threat_agent_lists": threat_agent_lists,
        }
        res_json = json.dumps(res_json)
        return HttpResponse(res_json, content_type='application/json')
        return Response(data)

    @detail_route(methods=['GET',], detail=True)
    def discard(self, request, *args, **kwargs):
        try:
            instance = self.get_object()
            instance.visible = False
            instance.save()
            if instance.species:
                instance.species.log_user_action(SpeciesUserAction.ACTION_DISCARD_THREAT.format(instance.threat_number,instance.species.species_number),request)
            elif instance.community:
                instance.community.log_user_action(CommunityUserAction.ACTION_DISCARD_THREAT.format(instance.threat_number,instance.community.community_number),request)
            serializer = self.get_serializer(instance)
            return Response(serializer.data)
        except serializers.ValidationError:
            print(traceback.print_exc())
            raise
        except ValidationError as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(repr(e.error_dict))
        except Exception as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(str(e))

    @detail_route(methods=['GET',], detail=True)
    def reinstate(self, request, *args, **kwargs):
        try:
            instance = self.get_object()
            instance.visible = True
            instance.save()
            if instance.species:
                instance.species.log_user_action(SpeciesUserAction.ACTION_REINSTATE_THREAT.format(instance.threat_number,instance.species.species_number),request)
            elif instance.community:
                instance.community.log_user_action(CommunityUserAction.ACTION_REINSTATE_THREAT.format(instance.threat_number,instance.community.community_number),request)
            serializer = self.get_serializer(instance)
            serializer = self.get_serializer(instance)
            return Response(serializer.data)
        except serializers.ValidationError:
            print(traceback.print_exc())
            raise
        except ValidationError as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(repr(e.error_dict))
        except Exception as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(str(e))

    def update(self, request, *args, **kwargs):
        try:
            instance = self.get_object()
            serializer = SaveConservationThreatSerializer(instance, data=json.loads(request.data.get('data')))
            serializer.is_valid(raise_exception=True)
            serializer.save()
            if instance.species:
                instance.species.log_user_action(SpeciesUserAction.ACTION_UPDATE_THREAT.format(instance.threat_number,instance.species.species_number),request)
            elif instance.community:
                instance.community.log_user_action(CommunityUserAction.ACTION_UPDATE_THREAT.format(instance.threat_number,instance.community.community_number),request)
            serializer = self.get_serializer(instance)
            return Response(serializer.data)
        except Exception as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(str(e))


    def create(self, request, *args, **kwargs):
        try:
            serializer = SaveConservationThreatSerializer(data= json.loads(request.data.get('data')))
            serializer.is_valid(raise_exception = True)
            instance = serializer.save()
            if instance.species:
                instance.species.log_user_action(SpeciesUserAction.ACTION_ADD_THREAT.format(instance.threat_number,instance.species.species_number),request)
            elif instance.community:
                instance.community.log_user_action(CommunityUserAction.ACTION_ADD_THREAT.format(instance.threat_number,instance.community.community_number),request)
            serializer = self.get_serializer(instance)
            return Response(serializer.data)
        except serializers.ValidationError:
            print(traceback.print_exc())
            raise
        except ValidationError as e:
            if hasattr(e,'error_dict'):
                raise serializers.ValidationError(repr(e.error_dict))
            else:
                if hasattr(e,'message'):
                    raise serializers.ValidationError(e.message)
        except Exception as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(str(e))