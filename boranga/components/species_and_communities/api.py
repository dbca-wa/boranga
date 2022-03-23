import traceback
import pytz
import json
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
from boranga.components.species_and_communities.models import (
    GroupType,
    Species,
    ConservationList,
    ConservationStatus,
    ConservationCategory,
    ConservationCriteria,
    Taxonomy,
    Community,
    Region,
    District,
)
from boranga.components.species_and_communities.serializers import (
    ListSpeciesSerializer,
    ListCommunitiesSerializer,
)

import logging

logger = logging.getLogger(__name__)

class GetGroupTypeDict(views.APIView):
    def get(self, request, format=None):
        group_type_list = []
        group_types = GroupType.objects.all()
        if group_types:
            for group in group_types:
                group_type_list.append(group.name)
        return Response(group_type_list)

class GetSpeciesFilterDict(views.APIView):
    def get(self, request, format=None):
        group_type = request.GET.get('group_type_name','')
        species_data_list = []
        if group_type:
            species = Species.objects.filter(group_type__name=group_type)
            if species:
                for specimen in species:
                    species_data_list.append({
                        'species_id': specimen.id,
                        'scientific_name': specimen.scientific_name,
                        'common_name':specimen.common_name,
                        'family':specimen.taxonomy.family,
                        'phylogenetic_group':specimen.taxonomy.phylogenetic_group,
                        'genus':specimen.taxonomy.genus,
                        });
        conservation_list_dict = []
        conservation_lists = ConservationStatus.objects.all()
        if conservation_lists:
            for choice in conservation_lists:
                conservation_list_dict.append({'id': choice.conservation_list_id,
                    'code': choice.conservation_list.code,
                    });

        conservation_category_list = []
        conservation_categories = ConservationCategory.objects.all()
        if conservation_categories:
            for choice in conservation_categories:
                conservation_category_list.append({'id': choice.id,
                    'code': choice.code,
                    });
        res_json = {
        "species_data_list":species_data_list,
        "conservation_list_dict":conservation_list_dict,
        "conservation_category_list":conservation_category_list,
        }
        res_json = json.dumps(res_json)
        return HttpResponse(res_json, content_type='application/json')

class GetCommunityFilterDict(views.APIView):
    def get(self, request, format=None):
        group_type = request.GET.get('group_type_name','')
        community_data_list = []
        if group_type:
            communities = Community.objects.filter(group_type__name=group_type)
            if communities:
                for community in communities:
                    community_data_list.append({
                        'id': community.id,
                        'community_id': community.community_id,
                        'community_name':community.community_name,
                        'community_status':community.community_status
                        });
        conservation_list_dict = []
        conservation_lists = ConservationStatus.objects.all()
        if conservation_lists:
            for choice in conservation_lists:
                conservation_list_dict.append({'id': choice.conservation_list_id,
                    'code': choice.conservation_list.code,
                    });

        conservation_category_list = []
        conservation_categories = ConservationCategory.objects.all()
        if conservation_categories:
            for choice in conservation_categories:
                conservation_category_list.append({'id': choice.id,
                    'code': choice.code,
                    });
        res_json = {
        "community_data_list":community_data_list,
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
                    });
        res_json = {
        "region_list":region_list,
        "district_list":district_list
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
            queryset = queryset.filter(scientific_name=filter_scientific_name)

        filter_common_name = request.GET.get('filter_common_name')
        if filter_common_name and not filter_common_name.lower() == 'all':
            queryset = queryset.filter(common_name=filter_common_name)

        filter_phylogenetic_group = request.GET.get('filter_phylogenetic_group')
        if filter_phylogenetic_group and not filter_phylogenetic_group.lower() == 'all':
            queryset = queryset.filter(taxonomy__phylogenetic_group=filter_phylogenetic_group)
        
        filter_family = request.GET.get('filter_family')
        if filter_family and not filter_family.lower() == 'all':
            queryset = queryset.filter(taxonomy__family=filter_family)

        filter_genus = request.GET.get('filter_genus')
        if filter_genus and not filter_genus.lower() == 'all':
            queryset = queryset.filter(taxonomy__genus=filter_genus)
        
        filter_conservation_list = request.GET.get('filter_conservation_list')
        if filter_conservation_list and not filter_conservation_list.lower() == 'all':
            queryset = queryset.filter(conservation_status__conservation_list=filter_conservation_list)

        filter_conservation_category = request.GET.get('filter_conservation_category')
        if filter_conservation_category and not filter_conservation_category.lower() == 'all':
            queryset = queryset.filter(conservation_status__conservation_category=filter_conservation_category)
        
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
        #request_user = self.request.user
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

class CommunitiesFilterBackend(DatatablesFilterBackend):
    def filter_queryset(self, request, queryset, view):
        total_count = queryset.count()
        
        # filter_group_type
        filter_group_type = request.GET.get('filter_group_type')
        if filter_group_type:
            queryset = queryset.filter(group_type__name=filter_group_type)
        
        #filter_community_id
        filter_community_id = request.GET.get('filter_community_id')
        if filter_community_id and not filter_community_id.lower() == 'all':
            queryset = queryset.filter(community_id=filter_community_id)

        # filter_community_name
        filter_community_name = request.GET.get('filter_community_name')
        if filter_community_name and not filter_community_name.lower() == 'all':
            queryset = queryset.filter(community_name=filter_community_name)

        # filter_community_status
        filter_community_status = request.GET.get('filter_community_status')
        if filter_community_status and not filter_community_status.lower() == 'all':
            queryset = queryset.filter(community_status=filter_community_status)

        filter_conservation_list = request.GET.get('filter_conservation_list')
        if filter_conservation_list and not filter_conservation_list.lower() == 'all':
            queryset = queryset.filter(conservation_status__conservation_list=filter_conservation_list)

        filter_conservation_category = request.GET.get('filter_conservation_category')
        if filter_conservation_category and not filter_conservation_category.lower() == 'all':
            queryset = queryset.filter(conservation_status__conservation_category=filter_conservation_category)

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