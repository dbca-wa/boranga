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
from boranga import exceptions
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

from boranga.components.species_and_communities.models import GroupType
from boranga.components.conservation_status.utils import cs_proposal_submit
from ledger_api_client.ledger_models import EmailUserRO as EmailUser
from boranga.components.main.decorators import basic_exception_handler

from boranga.components.main.related_item import RelatedItemsSerializer

import pandas as pd
from openpyxl import Workbook
from openpyxl.styles import Font
from openpyxl.utils.dataframe import dataframe_to_rows
from io import BytesIO
from django.db.models.query import QuerySet

from boranga.components.species_and_communities.models import(
    Species,
    Community,
    Taxonomy,
    CommunityTaxonomy,
    TaxonVernacular,
    PhylogeneticGroup,
    Genus,
    Family,
    ClassificationSystem,
)

from boranga.components.conservation_status.models import( 
    ConservationCategory,
    ConservationCriteria,
    ConservationChangeCode,
    ConservationStatus,
    ConservationList,
    ConservationStatusReferral,
    ConservationStatusAmendmentRequest,
    ConservationStatusUserAction,
    ConservationStatusAmendmentRequestDocument,
    ConservationStatusDocument,
    ProposalAmendmentReason,
)
from boranga.components.conservation_status.serializers import(
    SendReferralSerializer,
    DTConservationStatusReferralSerializer,
    ListSpeciesConservationStatusSerializer,
    ListCommunityConservationStatusSerializer,
    ListConservationStatusSerializer,
    ConservationStatusSerializer,
    CreateConservationStatusSerializer,
    InternalConservationStatusSerializer,
    InternalSpeciesConservationStatusSerializer,
    SaveSpeciesConservationStatusSerializer,
    CreateSpeciesConservationStatusSerializer,
    InternalCommunityConservationStatusSerializer,
    SaveCommunityConservationStatusSerializer,
    ConservationStatusLogEntrySerializer,
    ConservationStatusUserActionSerializer,
    #SpeciesConservationStatusLogEntrySerializer,
    #SpeciesConservationStatusUserActionSerializer,
    ConservationStatusReferralSerializer,
    ConservationStatusAmendmentRequestSerializer,
    ConservationStatusAmendmentRequestDisplaySerializer,
    ProposedDeclineSerializer,
    ProposedApprovalSerializer,
    ConservationStatusDocumentSerializer,
    SaveConservationStatusDocumentSerializer,
)
from boranga.components.main.utils import (
    check_db_connection,
    handle_validation_error,
)

import logging

logger = logging.getLogger(__name__)

# used for external CS Dash filter
class GetConservationListDict(views.APIView):
    def get(self, request, format=None):
        conservation_list = []
        lists = ConservationList.objects.filter(applies_to_wa=True)
        if lists:
            for choice in lists:
                conservation_list.append({'id': choice.id,
                    'code': choice.code,
                    });

        conservation_category_list = []
        conservation_categories = ConservationCategory.objects.filter(conservation_list__applies_to_wa=True)
        if conservation_categories:
            for choice in conservation_categories:
                conservation_category_list.append({
                    'id': choice.id,
                    'code': choice.code,
                    'conservation_list_id': choice.conservation_list_id,
                    });
        res_json = {
        "conservation_list":conservation_list,
        "conservation_category_list":conservation_category_list,
        }
        res_json = json.dumps(res_json)
        return HttpResponse(res_json, content_type='application/json')


class GetCSProfileDict(views.APIView):
    def get(self, request, format=None):
        group_type = request.GET.get('group_type','')
        # action is used to filter conservation list for WA or all
        action = request.GET.get('action','')
        species_list = []
        if group_type:
            exculde_status = ['draft']
            species = Species.objects.filter(~Q(processing_status__in=exculde_status) & ~Q(taxonomy=None) & Q(group_type__name=group_type))
            if species:
                for specimen in species:
                    species_list.append({
                        'id': specimen.id,
                        'name':specimen.taxonomy.scientific_name,
                        'taxon_previous_name':specimen.taxonomy.taxon_previous_name,
                        });
        community_list = []
        exculde_status = ['draft']
        communities = CommunityTaxonomy.objects.filter(~Q(community__processing_status__in=exculde_status)) # TODO remove later as every community will have community name
        if communities:
            for specimen in communities:
                community_list.append({
                    'id': specimen.community.id,
                    'name':specimen.community_name,
                    });
        conservation_list_values = []
        list = []
        if group_type and (group_type == GroupType.GROUP_TYPE_FLORA or group_type == GroupType.GROUP_TYPE_FAUNA):
            # action is used to filter conservation list for WA or all
            if action == "view":
                lists = ConservationList.objects.filter(applies_to_species=True)
            else:
                lists = ConservationList.objects.filter(applies_to_species=True, applies_to_wa=True)
        elif group_type and group_type == GroupType.GROUP_TYPE_COMMUNITY:
            # action is used to filter conservation list for WA or all
            if action == "view":
                lists = ConservationList.objects.filter(applies_to_communities=True)
            else:
                lists = ConservationList.objects.filter(applies_to_communities=True, applies_to_wa=True)
        if lists:
            for option in lists:
                conservation_list_values.append({
                    'id': option.id,
                    'code':option.code,
                    });
        conservation_category_list = []
        if group_type:
            categories = ConservationCategory.objects.filter()
            if categories:
                for option in categories:
                    conservation_category_list.append({
                        'id': option.id,
                        'code':option.code,
                        'conservation_list_id':option.conservation_list_id,
                        });
        conservation_criteria_list = []
        if group_type:
            criterias = ConservationCriteria.objects.filter()
            if criterias:
                for option in criterias:
                    conservation_criteria_list.append({
                        'id': option.id,
                        'code':option.code,
                        'conservation_list_id':option.conservation_list_id,
                        });
        res_json = {
        "species_list":species_list,
        "community_list":community_list,
        "conservation_list_values":conservation_list_values,
        "conservation_category_list":conservation_category_list,
        "conservation_criteria_list":conservation_criteria_list,
        }
        res_json = json.dumps(res_json)
        return HttpResponse(res_json, content_type='application/json')


class SpeciesConservationStatusFilterBackend(DatatablesFilterBackend):
    def filter_queryset(self, request, queryset, view):
        total_count = queryset.count()

        # filter_group_type
        filter_group_type = request.GET.get('filter_group_type')
        if queryset.model is ConservationStatus:
            if filter_group_type:
                #queryset = queryset.filter(species__group_type__name=filter_group_type)
                #changed to application_type (ie group_type)
                queryset = queryset.filter(application_type__name=filter_group_type)
        elif queryset.model is ConservationStatusReferral:
            if filter_group_type:
                #queryset = queryset.filter(species__group_type__name=filter_group_type)
                #changed to application_type (ie group_type)
                queryset = queryset.filter(conservation_status__application_type__name=filter_group_type)

        # filter_scientific_name
        filter_scientific_name = request.GET.get('filter_scientific_name')
        if queryset.model is ConservationStatus:
            if filter_scientific_name and not filter_scientific_name.lower() == 'all':
                queryset = queryset.filter(species__taxonomy__id=filter_scientific_name)
        elif queryset.model is ConservationStatusReferral:
            if filter_scientific_name and not filter_scientific_name.lower() == 'all':
                queryset = queryset.filter(conservation_status__species__taxonomy__id=filter_scientific_name)

        filter_common_name = request.GET.get('filter_common_name')
        if queryset.model is ConservationStatus:
            if filter_common_name and not filter_common_name.lower() == 'all':
                queryset = queryset.filter(species__taxonomy__vernaculars__id=filter_common_name)
        elif queryset.model is ConservationStatusReferral:
            if filter_common_name and not filter_common_name.lower() == 'all':
                queryset = queryset.filter(conservation_status__species__taxonomy__vernaculars__id=filter_common_name)

        filter_phylogenetic_group = request.GET.get('filter_phylogenetic_group')
        if queryset.model is ConservationStatus:
            if filter_phylogenetic_group and not filter_phylogenetic_group.lower() == 'all':
                queryset = queryset.filter(species__taxonomy__informal_groups__classification_system_fk_id=filter_phylogenetic_group)
        elif queryset.model is ConservationStatusReferral:
            if filter_phylogenetic_group and not filter_phylogenetic_group.lower() == 'all':
                queryset = queryset.filter(conservation_status__species__taxonomy__informal_groups__classification_system_id=filter_phylogenetic_group)
        
        filter_family = request.GET.get('filter_family')
        if queryset.model is ConservationStatus:
            if filter_family and not filter_family.lower() == 'all':
                queryset = queryset.filter(species__taxonomy__family_fk_id=filter_family)
        elif queryset.model is ConservationStatusReferral:
            if filter_family and not filter_family.lower() == 'all':
                queryset = queryset.filter(conservation_status__species__taxonomy__family_fk_id=filter_family)

        filter_genus = request.GET.get('filter_genus')
        if queryset.model is ConservationStatus:
            if filter_genus and not filter_genus.lower() == 'all':
                queryset = queryset.filter(species__taxonomy__genus__id=filter_genus)
        elif queryset.model is ConservationStatusReferral:
            if filter_genus and not filter_genus.lower() == 'all':
                queryset = queryset.filter(conservation_status__species__taxonomy__genus__id=filter_genus)
        
        filter_conservation_list = request.GET.get('filter_conservation_list')
        if queryset.model is ConservationStatus:
            if filter_conservation_list and not filter_conservation_list.lower() == 'all':
                queryset = queryset.filter(conservation_list=filter_conservation_list)
        elif queryset.model is ConservationStatusReferral:
            if filter_conservation_list and not filter_conservation_list.lower() == 'all':
                queryset = queryset.filter(conservation_status__conservation_list=filter_conservation_list)

        filter_conservation_category = request.GET.get('filter_conservation_category')
        if queryset.model is ConservationStatus:
            if filter_conservation_category and not filter_conservation_category.lower() == 'all':
                queryset = queryset.filter(conservation_category=filter_conservation_category)
        elif queryset.model is ConservationStatusReferral:
            if filter_conservation_category and not filter_conservation_category.lower() == 'all':
                queryset = queryset.filter(conservation_status__conservation_category=filter_conservation_category)
        
        filter_region = request.GET.get('filter_region')
        if queryset.model is ConservationStatus:
            if filter_region and not filter_region.lower() == 'all':
                queryset = queryset.filter(species__region=filter_region)
        elif queryset.model is ConservationStatusReferral:
            if filter_region and not filter_region.lower() == 'all':
                queryset = queryset.filter(conservation_status__species__region=filter_region)

        filter_district = request.GET.get('filter_district')
        if queryset.model is ConservationStatus:
            if filter_district and not filter_district.lower() == 'all':
                queryset = queryset.filter(species__district=filter_district)
        elif queryset.model is ConservationStatusReferral:
            if filter_district and not filter_district.lower() == 'all':
                queryset = queryset.filter(conservation_status__species__district=filter_district)

        filter_effective_from_date = request.GET.get('filter_effective_from_date')
        filter_effective_to_date = request.GET.get('filter_effective_to_date')
        if queryset.model is ConservationStatus:
            if filter_effective_from_date:
                queryset = queryset.filter(conservationstatusissuanceapprovaldetails__effective_from_date__gte=filter_effective_from_date)

            if filter_effective_to_date:
                queryset = queryset.filter(conservationstatusissuanceapprovaldetails__effective_to_date__lte=filter_effective_to_date)

        elif queryset.model is ConservationStatusReferral:
            if filter_effective_from_date:
                queryset = queryset.filter(conservation_status__conservationstatusissuanceapprovaldetails__effective_from_date__gte=filter_effective_from_date)

            if filter_effective_to_date:
                queryset = queryset.filter(conservation_status__conservationstatusissuanceapprovaldetails__effective_to_date__lte=filter_effective_to_date)

        filter_application_status = request.GET.get('filter_application_status')
        if queryset.model is ConservationStatus:
            if filter_application_status and not filter_application_status.lower() == 'all':
                queryset = queryset.filter(processing_status=filter_application_status)
        elif queryset.model is ConservationStatusReferral:
            if filter_application_status and not filter_application_status.lower() == 'all':
                queryset = queryset.filter(conservation_status__processing_status=filter_application_status)

        getter = request.query_params.get
        fields = self.get_fields(getter)
        ordering = self.get_ordering(getter, fields)
        queryset = queryset.order_by(*ordering)
        if len(ordering):
            queryset = queryset.order_by(*ordering)

        try:
            queryset = super(SpeciesConservationStatusFilterBackend, self).filter_queryset(request, queryset, view)
        except Exception as e:
            print(e)
        setattr(view, '_datatables_total_count', total_count)
        return queryset

class SpeciesConservationStatusRenderer(DatatablesRenderer):
    def render(self, data, accepted_media_type=None, renderer_context=None):
        if 'view' in renderer_context and hasattr(renderer_context['view'], '_datatables_total_count'):
            data['recordsTotal'] = renderer_context['view']._datatables_total_count
        return super(SpeciesConservationStatusRenderer, self).render(data, accepted_media_type, renderer_context)

class SpeciesConservationStatusPaginatedViewSet(viewsets.ModelViewSet):
    filter_backends = (SpeciesConservationStatusFilterBackend,)
    pagination_class = DatatablesPageNumberPagination
    renderer_classes = (SpeciesConservationStatusRenderer,)
    queryset = ConservationStatus.objects.none()
    serializer_class = ListSpeciesConservationStatusSerializer
    page_size = 10

    def get_queryset(self):
        #request_user = self.request.user
        qs = ConservationStatus.objects.none()

        if is_internal(self.request):
            qs = ConservationStatus.objects.all()

        return qs

    @list_route(methods=['GET',], detail=False)
    def species_cs_internal(self, request, *args, **kwargs):
        qs = self.get_queryset()
        qs = self.filter_queryset(qs)

        self.paginator.page_size = qs.count()
        result_page = self.paginator.paginate_queryset(qs, request)
        serializer = ListSpeciesConservationStatusSerializer(result_page, context={'request': request}, many=True)
        return self.paginator.get_paginated_response(serializer.data)
    
    # used for Meeting Agenda Modal where status is 'ready_for_agenda'
    @list_route(methods=['GET',], detail=False)
    def agenda_cs_internal(self, request, *args, **kwargs):
        qs = self.get_queryset()
        qs = qs.filter(processing_status=ConservationStatus.PROCESSING_STATUS_READY_FOR_AGENDA)
        qs = self.filter_queryset(qs)

        self.paginator.page_size = qs.count()
        result_page = self.paginator.paginate_queryset(qs, request)
        serializer = ListSpeciesConservationStatusSerializer(result_page, context={'request': request}, many=True)
        return self.paginator.get_paginated_response(serializer.data)
    
    @list_route(methods=['GET',], detail=False)
    def species_cs_internal_export(self, request, *args, **kwargs):
        
        qs = self.get_queryset()
        qs = self.filter_queryset(qs)
        export_format = request.GET.get('export_format')
        allowed_fields = ['species_number', 'scientific_name', 'common_name', 'family', 'genus', 'phylogenetic_group', 'region', 'district', 'conservation_list', 'conservation_category', 'processing_status', 'effective_from_date', 'effective_to_date', 'conservation_status_number']

        serializer = ListSpeciesConservationStatusSerializer(qs, context={'request': request}, many=True)
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
            new_headings = ['Number', 'Species', 'Scientific Name', 'Common Name', 'Family', 'Genera', 'Phylo Group', 'Region', 'District', 'Conservation List', 'Conservation Category',  'Processing Status', 'Effective From Date', 'Effective To Date']
            df.columns = new_headings
            column_order = ['Number', 'Species', 'Scientific Name', 'Common Name', 'Conservation List', 'Conservation Category', 'Region', 'District', 'Effective From Date', 'Effective To Date', 'Family', 'Genera', 'Processing Status']
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
                    response['Content-Disposition'] = 'attachment; filename=DBCA_ConservationStatus_Species.xlsx'
                    final_response = response
                    buffer.close()
                    return final_response
                
                elif export_format == "csv":
                    csv_data = df.to_csv(index=False)
                    response = HttpResponse(content_type='text/csv')
                    response['Content-Disposition'] = 'attachment; filename=DBCA_ConservationStatus_Species.csv'
                    response.write(csv_data)
                    return response
                
                else:
                    return Response(status=400, data="Format not valid")
        except:
            return Response(status=500, data="Internal Server Error")

    @list_route(methods=['GET',], detail=False)
    def species_cs_referrals_internal(self, request, *args, **kwargs):
        """
        Used by the internal Referred To Me (Flora/Fauna)dashboard

        http://localhost:8499/api/species_conservation_cstatus_paginated/species_cs_referrals_internal/?format=datatables&draw=1&length=2
        """
        self.serializer_class = ConservationStatusReferralSerializer
        qs = ConservationStatusReferral.objects.filter(referral=request.user.id) if is_internal(self.request) else ConservationStatusReferral.objects.none()
        # TODO Priya commented the below and use above qs as not sure if we need referralrecipientgroup()
        #qs = Referral.objects.filter(referral_group__in=request.user.referralrecipientgroup_set.all()) if is_internal(self.request) else Referral.objects.none()
        #qs = self.filter_queryset(self.request, qs, self)
        qs = self.filter_queryset(qs)

        self.paginator.page_size = qs.count()
        result_page = self.paginator.paginate_queryset(qs, request)
        serializer = DTConservationStatusReferralSerializer(result_page, context={'request':request}, many=True)
        return self.paginator.get_paginated_response(serializer.data)
    
    @list_route(methods=['GET',], detail=False)
    def species_cs_referrals_internal_export(self, request, *args, **kwargs):
        
        self.serializer_class = ConservationStatusReferralSerializer
        qs = ConservationStatusReferral.objects.filter(referral=request.user.id) if is_internal(self.request) else ConservationStatusReferral.objects.none()
        qs = self.filter_queryset(qs)
        export_format = request.GET.get('export_format')
        allowed_fields = ['species_number', 'scientific_name', 'family', 'genus', 'conservation_list', 'conservation_category', 'processing_status', 'conservation_status_number']

        serializer = DTConservationStatusReferralSerializer(qs, context={'request': request}, many=True)
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
            new_headings = ['Processing Status', 'Number', 'Species', 'Scientific Name', 'Conservation List', 'Conservation Category', 'Family', 'Genera']
            df.columns = new_headings
            column_order = ['Number', 'Species', 'Scientific Name', 'Conservation List', 'Conservation Category', 'Family', 'Genera', 'Processing Status']
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
                    response['Content-Disposition'] = 'attachment; filename=DBCA_ConservationStatus_Species_Referrals.xlsx'
                    final_response = response
                    buffer.close()
                    return final_response
                
                elif export_format == "csv":
                    csv_data = df.to_csv(index=False)
                    response = HttpResponse(content_type='text/csv')
                    response['Content-Disposition'] = 'attachment; filename=DBCA_ConservationStatus_Species_Referrals.csv'
                    response.write(csv_data)
                    return response
                
                else:
                    return Response(status=400, data="Format not valid")
        except:
            return Response(status=500, data="Internal Server Error")


class CommunityConservationStatusFilterBackend(DatatablesFilterBackend):
    def filter_queryset(self, request, queryset, view):
        total_count = queryset.count()
        
        # filter_group_type
        filter_group_type = request.GET.get('filter_group_type')
        if filter_group_type:
            if queryset.model is ConservationStatus:
                queryset = queryset.filter(application_type__name=filter_group_type)
            elif queryset.model is ConservationStatusReferral:
                queryset = queryset.filter(conservation_status__application_type__name=filter_group_type)
        
        #filter_community_migrated_id
        filter_community_migrated_id = request.GET.get('filter_community_migrated_id')
        if filter_community_migrated_id and not filter_community_migrated_id.lower() == 'all':
            if queryset.model is ConservationStatus:
                queryset = queryset.filter(community__taxonomy__id=filter_community_migrated_id)
            elif queryset.model is ConservationStatusReferral:
                queryset = queryset.filter(conservation_status__community__taxonomy__id=filter_community_migrated_id)

        # filter_community_name
        filter_community_name = request.GET.get('filter_community_name')
        if filter_community_name and not filter_community_name.lower() == 'all':
            if queryset.model is ConservationStatus:
                queryset = queryset.filter(community__taxonomy__id=filter_community_name)
            elif queryset.model is ConservationStatusReferral:
                queryset = queryset.filter(conservation_status__community__taxonomy__id=filter_community_name)

        filter_conservation_list = request.GET.get('filter_conservation_list')
        if filter_conservation_list and not filter_conservation_list.lower() == 'all':
            if queryset.model is ConservationStatus:
                queryset = queryset.filter(conservation_list=filter_conservation_list)
            elif queryset.model is ConservationStatusReferral:
                queryset = queryset.filter(conservation_status__conservation_list=filter_conservation_list)

        filter_conservation_category = request.GET.get('filter_conservation_category')
        if filter_conservation_category and not filter_conservation_category.lower() == 'all':
            if queryset.model is ConservationStatus:
                queryset = queryset.filter(conservation_category=filter_conservation_category)
            elif queryset.model is ConservationStatusReferral:
                queryset = queryset.filter(conservation_status__conservation_category=filter_conservation_category)

        filter_region = request.GET.get('filter_region')
        if filter_region and not filter_region.lower() == 'all':
            if queryset.model is ConservationStatus:
                queryset = queryset.filter(community__region=filter_region)
            elif queryset.model is ConservationStatusReferral:
                queryset = queryset.filter(conservation_status__community__region=filter_region)

        filter_district = request.GET.get('filter_district')
        if filter_district and not filter_district.lower() == 'all':
            if queryset.model is ConservationStatus:
                queryset = queryset.filter(community__district=filter_district)
            elif queryset.model is ConservationStatusReferral:
                queryset = queryset.filter(conservation_status__community__district=filter_district)

        filter_effective_from_date = request.GET.get('filter_effective_from_date')
        filter_effective_to_date = request.GET.get('filter_effective_to_date')
        if queryset.model is ConservationStatus:
            if filter_effective_from_date:
                queryset = queryset.filter(conservationstatusissuanceapprovaldetails__effective_from_date__gte=filter_effective_from_date)

            if filter_effective_to_date:
                queryset = queryset.filter(conservationstatusissuanceapprovaldetails__effective_to_date__lte=filter_effective_to_date)

        elif queryset.model is ConservationStatusReferral:
            if filter_effective_from_date:
                queryset = queryset.filter(conservation_status__conservationstatusissuanceapprovaldetails__effective_from_date__gte=filter_effective_from_date)

            if filter_effective_to_date:
                queryset = queryset.filter(conservation_status__conservationstatusissuanceapprovaldetails__effective_to_date__lte=filter_effective_to_date)

        filter_application_status = request.GET.get('filter_application_status')
        if filter_application_status and not filter_application_status.lower() == 'all':
            if queryset.model is ConservationStatus:
                queryset = queryset.filter(processing_status=filter_application_status)
            elif queryset.model is ConservationStatusReferral:
                queryset = queryset.filter(conservation_status__processing_status=filter_application_status)

        getter = request.query_params.get
        fields = self.get_fields(getter)
        ordering = self.get_ordering(getter, fields)
        queryset = queryset.order_by(*ordering)
        if len(ordering):
            queryset = queryset.order_by(*ordering)

        try:
            queryset = super(CommunityConservationStatusFilterBackend, self).filter_queryset(request, queryset, view)
        except Exception as e:
            print(e)
        setattr(view, '_datatables_total_count', total_count)
        return queryset

class CommunityConservationStatusRenderer(DatatablesRenderer):
    def render(self, data, accepted_media_type=None, renderer_context=None):
        if 'view' in renderer_context and hasattr(renderer_context['view'], '_datatables_total_count'):
            data['recordsTotal'] = renderer_context['view']._datatables_total_count
        return super(CommunityConservationStatusRenderer, self).render(data, accepted_media_type, renderer_context)

class CommunityConservationStatusPaginatedViewSet(viewsets.ModelViewSet):
    filter_backends = (CommunityConservationStatusFilterBackend,)
    pagination_class = DatatablesPageNumberPagination
    renderer_classes = (CommunityConservationStatusRenderer,)
    queryset = ConservationStatus.objects.none()
    serializer_class = ListCommunityConservationStatusSerializer
    page_size = 10

    def get_queryset(self):
        #request_user = self.request.user
        qs = ConservationStatus.objects.none()

        if is_internal(self.request):
            qs = ConservationStatus.objects.all()

        return qs

    @list_route(methods=['GET',], detail=False)
    def community_cs_internal(self, request, *args, **kwargs):
        qs = self.get_queryset()
        qs = self.filter_queryset(qs)

        self.paginator.page_size = qs.count()
        result_page = self.paginator.paginate_queryset(qs, request)
        serializer = ListCommunityConservationStatusSerializer(result_page, context={'request': request}, many=True)
        return self.paginator.get_paginated_response(serializer.data)
    
    @list_route(methods=['GET',], detail=False)
    def agenda_cs_internal(self, request, *args, **kwargs):
        qs = self.get_queryset()
        qs = qs.filter(processing_status=ConservationStatus.PROCESSING_STATUS_READY_FOR_AGENDA)
        qs = self.filter_queryset(qs)

        self.paginator.page_size = qs.count()
        result_page = self.paginator.paginate_queryset(qs, request)
        serializer = ListCommunityConservationStatusSerializer(result_page, context={'request': request}, many=True)
        return self.paginator.get_paginated_response(serializer.data)

    @list_route(methods=['GET',], detail=False)
    def community_cs_internal_export(self, request, *args, **kwargs):
        
        qs = self.get_queryset()
        qs = self.filter_queryset(qs)
        export_format = request.GET.get('export_format')
        allowed_fields = ['conservation_status_number', 'community_number', 'community_migrated_id', 'community_name', 'region', 'district', 'conservation_list', 'conservation_category', 'processing_status', 'effective_from_date', 'effective_to_date']

        serializer = ListCommunityConservationStatusSerializer(qs, context={'request': request}, many=True)
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
            new_headings = ['Number', 'Community', 'Community Id', 'Community Name', 'Conservation List', 'Conservation Category', 'Region', 'District', 'Processing Status', 'Effective From Date', 'Effective To Date']
            df.columns = new_headings
            column_order = ['Number', 'Community', 'Community Id', 'Community Name', 'Conservation List', 'Conservation Category', 'Region', 'District', 'Effective From Date', 'Effective To Date', 'Processing Status']
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
                    response['Content-Disposition'] = 'attachment; filename=DBCA_ConservationStatus_Communities.xlsx'
                    final_response = response
                    buffer.close()
                    return final_response
                
                elif export_format == "csv":
                    csv_data = df.to_csv(index=False)
                    response = HttpResponse(content_type='text/csv')
                    response['Content-Disposition'] = 'attachment; filename=DBCA_ConservationStatus_Communities.csv'
                    response.write(csv_data)
                    return response
                
                else:
                    return Response(status=400, data="Format not valid")
        except:
            return Response(status=500, data="Internal Server Error")

    @list_route(methods=['GET',], detail=False)
    def community_cs_referrals_internal(self, request, *args, **kwargs):
        """
        Used by the internal Referred to me dashboard

        http://localhost:8499/api/community_conservation_status_paginated/community_cs_referrals_internal/?format=datatables&draw=1&length=2
        """
        self.serializer_class = ConservationStatusReferralSerializer
        qs = ConservationStatusReferral.objects.filter(referral=request.user.id) if is_internal(self.request) else ConservationStatusReferral.objects.none()
        # TODO Priya commented the below and use above qs as not sure if we need referralrecipientgroup()
        #qs = Referral.objects.filter(referral_group__in=request.user.referralrecipientgroup_set.all()) if is_internal(self.request) else Referral.objects.none()
        #qs = self.filter_queryset(self.request, qs, self)
        qs = self.filter_queryset(qs)

        self.paginator.page_size = qs.count()
        result_page = self.paginator.paginate_queryset(qs, request)
        serializer = DTConservationStatusReferralSerializer(result_page, context={'request':request}, many=True)
        return self.paginator.get_paginated_response(serializer.data)

    @list_route(methods=['GET',], detail=False)
    def community_cs_referrals_internal_export(self, request, *args, **kwargs):
        
        self.serializer_class = ConservationStatusReferralSerializer
        qs = ConservationStatusReferral.objects.filter(referral=request.user.id) if is_internal(self.request) else ConservationStatusReferral.objects.none()
        qs = self.filter_queryset(qs)
        export_format = request.GET.get('export_format')
        allowed_fields = ['conservation_list', 'conservation_category', 'processing_status', 'community_number', 'community_migrated_id', 'community_name', 'conservation_status_number']

        serializer = DTConservationStatusReferralSerializer(qs, context={'request': request}, many=True)
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
            new_headings = ['Processing Status', 'Number', 'Conservation List', 'Conservation Category', 'Community', 'Community Id', 'Community Name']
            df.columns = new_headings
            column_order = ['Number', 'Community', 'Community Id', 'Community Name', 'Conservation List', 'Conservation Category', 'Processing Status']
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
                    response['Content-Disposition'] = 'attachment; filename=DBCA_ConservationStatus_Communities_Referrals.xlsx'
                    final_response = response
                    buffer.close()
                    return final_response
                
                elif export_format == "csv":
                    csv_data = df.to_csv(index=False)
                    response = HttpResponse(content_type='text/csv')
                    response['Content-Disposition'] = 'attachment; filename=DBCA_ConservationStatus_Communities_Referrals.csv'
                    response.write(csv_data)
                    return response
                
                else:
                    return Response(status=400, data="Format not valid")
        except:
            return Response(status=500, data="Internal Server Error")


class ConservationStatusFilterBackend(DatatablesFilterBackend):
    def filter_queryset(self, request, queryset, view):
        total_count = queryset.count()

        flora = GroupType.GROUP_TYPE_FLORA
        fauna = GroupType.GROUP_TYPE_FAUNA
        community = GroupType.GROUP_TYPE_COMMUNITY
        
        filter_group_type = request.GET.get('filter_group_type')
        # if filter_group_type and filter_group_type == flora or filter_group_type == fauna:
        #     queryset = queryset.filter(species__group_type__name=filter_group_type)
        # elif filter_group_type and filter_group_type == community:
        #     queryset = queryset.filter(community__group_type__name=filter_group_type)
        if filter_group_type and not filter_group_type.lower() == 'all':
            queryset = queryset.filter(application_type__name=filter_group_type)
        
        # filter_scientific_name is the species_id
        filter_scientific_name = request.GET.get('filter_scientific_name')
        if filter_scientific_name and not filter_scientific_name.lower() == 'all':
            queryset = queryset.filter(species=filter_scientific_name)

        # filter_community_name is the community_id
        filter_community_name = request.GET.get('filter_community_name')
        if filter_community_name and not filter_community_name.lower() == 'all':
            queryset = queryset.filter(community=filter_community_name)

        filter_conservation_list = request.GET.get('filter_conservation_list')
        if filter_conservation_list and not filter_conservation_list.lower() == 'all':
            queryset = queryset.filter(conservation_list=filter_conservation_list)

        filter_conservation_category = request.GET.get('filter_conservation_category')
        if filter_conservation_category and not filter_conservation_category.lower() == 'all':
            queryset = queryset.filter(conservation_category=filter_conservation_category)

        filter_application_status = request.GET.get('filter_application_status')
        if filter_application_status and not filter_application_status.lower() == 'all':
            queryset = queryset.filter(customer_status=filter_application_status)

        getter = request.query_params.get
        fields = self.get_fields(getter)
        ordering = self.get_ordering(getter, fields)
        queryset = queryset.order_by(*ordering)
        if len(ordering):
            queryset = queryset.order_by(*ordering)

        try:
            queryset = super(ConservationStatusFilterBackend, self).filter_queryset(request, queryset, view)
        except Exception as e:
            print(e)
        setattr(view, '_datatables_total_count', total_count)
        return queryset

class ConservationStatusRenderer(DatatablesRenderer):
    def render(self, data, accepted_media_type=None, renderer_context=None):
        if 'view' in renderer_context and hasattr(renderer_context['view'], '_datatables_total_count'):
            data['recordsTotal'] = renderer_context['view']._datatables_total_count
        return super(ConservationStatusRenderer, self).render(data, accepted_media_type, renderer_context)

class ConservationStatusPaginatedViewSet(viewsets.ModelViewSet):
    filter_backends = (ConservationStatusFilterBackend,)
    pagination_class = DatatablesPageNumberPagination
    renderer_classes = (ConservationStatusRenderer,)
    queryset = ConservationStatus.objects.none()
    serializer_class = ListConservationStatusSerializer
    page_size = 10

    def get_queryset(self):
        request_user = self.request.user
        qs = ConservationStatus.objects.all()

        if is_internal(self.request):
            qs = ConservationStatus.objects.all()
        elif is_customer(self.request):
            #user_orgs = [org.id for org in request_user.mooringlicensing_organisations.all()]
            #qs = all.filter(Q(org_applicant_id__in=user_orgs) | Q(submitter=request_user) | Q(site_licensee_email=request_user.email))
            qs = qs.filter(Q(submitter=request_user.id))
            return qs

        return qs

    @list_route(methods=['GET',], detail=False)
    def conservation_status_external(self, request, *args, **kwargs):
        qs = self.get_queryset()
        qs = qs.filter(Q(internal_application=False))
        # TODO Not Sure but to filter for only WA listed conservation lists for external
        #qs = qs.filter(Q(conservation_list__applies_to_wa=True))
        qs = self.filter_queryset(qs)

        self.paginator.page_size = qs.count()
        result_page = self.paginator.paginate_queryset(qs, request)
        serializer = ListConservationStatusSerializer(result_page, context={'request': request}, many=True)
        return self.paginator.get_paginated_response(serializer.data)


class ConservationStatusViewSet(viewsets.ModelViewSet):
    queryset = ConservationStatus.objects.none()
    serializer_class = ConservationStatusSerializer
    lookup_field = 'id'

    def get_queryset(self):
        user = self.request.user
        if is_internal(self.request): #user.is_authenticated():
            qs= ConservationStatus.objects.all()
            return qs
        elif is_customer(self.request):
            # user_orgs = [org.id for org in user.boranga_organisations.all()]
            queryset =  ConservationStatus.objects.filter( Q(submitter = user.id) )
            return queryset
        logger.warn("User is neither customer nor internal user: {} <{}>".format(user.get_full_name(), user.email))
        return ConservationStatus.objects.none()

    def get_serializer_class(self):
        try:
            # application_type = Proposal.objects.get(id=self.kwargs.get('id')).application_type.name
            # if application_type == ApplicationType.TCLASS:
            #     return ProposalSerializer
            # elif application_type == ApplicationType.FILMING:
            #     return ProposalFilmingSerializer
            # elif application_type == ApplicationType.EVENT:
            #     return ProposalEventSerializer
            return ConservationStatusSerializer
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

    def internal_serializer_class(self):
        try:
            instance = self.get_object()
            # if instance.application_type.name == GroupType.GROUP_TYPE_FLORA or instance.application_type.name == GroupType.GROUP_TYPE_FAUNA:
            #     return InternalSpeciesConservationStatusSerializer
            # elif instance.application_type.name == GroupType.GROUP_TYPE_COMMUNITY:
            #     return InternalCommunityConservationStatusSerializer
            # TODO try to use below serializer than two above
            return InternalConservationStatusSerializer
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
    def internal_conservation_status(self, request, *args, **kwargs):
        instance = self.get_object()
        serializer_class = self.internal_serializer_class()
        serializer = serializer_class(instance,context={'request':request})
       
        res_json = {
         "conservation_status_obj":serializer.data
        }
        res_json = json.dumps(res_json)
        return HttpResponse(res_json, content_type='application/json')
    
    @list_route(methods=['GET',], detail=False)
    def filter_list(self, request, *args, **kwargs):
        """ Used by the Related Items dashboard filters """
        related_type =  ConservationStatus.RELATED_ITEM_CHOICES 
        res_json = json.dumps(related_type) 
        return HttpResponse(res_json, content_type='application/json')

    @detail_route(methods=['post'], detail=True)
    @renderer_classes((JSONRenderer,))
    def conservation_status_save(self, request, *args, **kwargs):
        try:
            with transaction.atomic():
                instance = self.get_object() 
                request_data = request.data
                # to resolve error for serializer submitter id as object is received in request
                if request_data['submitter']:
                    request.data['submitter'] = u'{}'.format(request_data['submitter'].get('id'))
                if instance.application_type.name == GroupType.GROUP_TYPE_FLORA or instance.application_type.name == GroupType.GROUP_TYPE_FAUNA:
                    serializer=SaveSpeciesConservationStatusSerializer(instance, data = request_data, partial=True)
                elif instance.application_type.name == GroupType.GROUP_TYPE_COMMUNITY:
                    serializer=SaveCommunityConservationStatusSerializer(instance, data = request_data, partial=True)

                serializer.is_valid(raise_exception=True)
                if serializer.is_valid():
                    saved_instance = serializer.save()

                    # add the updated Current conservation criteria list [1,2] to the cs instance,
                    saved_instance.conservation_criteria.set(request_data.get('conservation_criteria'))

                    instance.log_user_action(ConservationStatusUserAction.ACTION_SAVE_APPLICATION.format(instance.conservation_status_number), request)

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
    def conservation_status_edit(self, request, *args, **kwargs):
        try:
            with transaction.atomic():
                instance = self.get_object()
                request_data = request.data
                if not instance.can_assess(request.user):
                    raise exceptions.ProposalNotAuthorized()
                if(instance.processing_status == 'approved'):
                    # to resolve error for serializer submitter id as object is received in request
                    if request_data['submitter']:
                        request.data['submitter'] = u'{}'.format(request_data['submitter'].get('id'))
                    if instance.application_type.name == GroupType.GROUP_TYPE_FLORA or instance.application_type.name == GroupType.GROUP_TYPE_FAUNA:
                        serializer=SaveSpeciesConservationStatusSerializer(instance, data = request_data, partial=True)
                    elif instance.application_type.name == GroupType.GROUP_TYPE_COMMUNITY:
                        serializer=SaveCommunityConservationStatusSerializer(instance, data = request_data, partial=True)

                    serializer.is_valid(raise_exception=True)
                    if serializer.is_valid():
                        saved_instance = serializer.save()

                        # add the updated Current conservation criteria list [1,2] to the cs instance,
                        saved_instance.conservation_criteria.set(request_data.get('conservation_criteria'))

                        instance.log_user_action(ConservationStatusUserAction.ACTION_EDIT_APPLICATION.format(instance.conservation_status_number), request)

            return redirect(reverse('external'))
        
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
    def draft(self, request, *args, **kwargs):
        try:
            with transaction.atomic():
                instance = self.get_object()
                request_data = request.data
                #request.data['submitter'] = u'{}'.format(request.user.id)
                if request_data['submitter']:
                    request.data['submitter'] = u'{}'.format(request_data['submitter'].get('id'))
                if instance.application_type.name == GroupType.GROUP_TYPE_FLORA or instance.application_type.name == GroupType.GROUP_TYPE_FAUNA:
                    serializer=SaveSpeciesConservationStatusSerializer(instance, data = request_data, partial=True)
                elif instance.application_type.name == GroupType.GROUP_TYPE_COMMUNITY:
                    serializer=SaveCommunityConservationStatusSerializer(instance, data = request_data, partial=True)

                serializer.is_valid(raise_exception=True)
                if serializer.is_valid():
                    saved_instance = serializer.save()

                    # add the updated Current conservation criteria list [1,2] to the cs instance,
                    saved_instance.conservation_criteria.set(request_data.get('conservation_criteria'))

            return redirect(reverse('external'))
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

    @detail_route(methods=['post'], detail=True)
    @renderer_classes((JSONRenderer,))
    def submit(self, request, *args, **kwargs):
        try:
            instance = self.get_object()
            #instance.submit(request,self)
            cs_proposal_submit(instance, request)
            instance.save()
            serializer = self.get_serializer(instance)
            return Response(serializer.data)
            #return redirect(reverse('external'))
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
        group_type_id = GroupType.objects.get(id=request.data.get('application_type_id'))
        internal_application = False
        if request.data.get('internal_application'):
                internal_application = request.data.get('internal_application')
        obj = ConservationStatus.objects.create(
                #submitter=request.user.id,
                application_type=group_type_id,
                internal_application=internal_application
                )
        serialized_obj = CreateConservationStatusSerializer(obj)
        return Response(serialized_obj.data)

    @detail_route(methods=['GET',], detail=True)
    def action_log(self, request, *args, **kwargs):
        try:
            instance = self.get_object()
            qs = instance.action_logs.all()
            serializer = ConservationStatusUserActionSerializer(qs,many=True)
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
            serializer = ConservationStatusLogEntrySerializer(qs,many=True)
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
                request.data['conservation_status'] = u'{}'.format(instance.id)
                request.data['staff'] = u'{}'.format(request.user.id)
                request.data._mutable=mutable
                serializer = ConservationStatusLogEntrySerializer(data=request.data)
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
    def amendment_request(self, request, *args, **kwargs):
        try:
            instance = self.get_object()
            qs = instance.amendment_requests
            qs = qs.filter(status = 'requested')
            serializer = ConservationStatusAmendmentRequestDisplaySerializer(qs,many=True)
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
    def assign_request_user(self, request, *args, **kwargs):
        try:
            instance = self.get_object()
            instance.assign_officer(request,request.user)
            #serializer = InternalProposalSerializer(instance,context={'request':request})
            serializer_class = self.internal_serializer_class()
            serializer = serializer_class(instance,context={'request':request})
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
    def assign_to(self, request, *args, **kwargs):
        try:
            instance = self.get_object()
            user_id = request.data.get('assessor_id',None)
            user = None
            if not user_id:
                raise serializers.ValidationError('An assessor id is required')
            try:
                user = EmailUser.objects.get(id=user_id)
            except EmailUser.DoesNotExist:
                raise serializers.ValidationError('A user with the id passed in does not exist')
            instance.assign_officer(request,user)
            #serializer = InternalProposalSerializer(instance,context={'request':request})
            serializer_class = self.internal_serializer_class()
            serializer = serializer_class(instance,context={'request':request})
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
    def unassign(self, request, *args, **kwargs):
        try:
            instance = self.get_object()
            instance.unassign(request)
            #serializer = InternalProposalSerializer(instance,context={'request':request})
            serializer_class = self.internal_serializer_class()
            serializer = serializer_class(instance,context={'request':request})
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

    @detail_route(methods=['post'], detail=True)
    def assesor_send_referral(self, request, *args, **kwargs):
        try:
            instance = self.get_object()
            serializer = SendReferralSerializer(data=request.data, context={"request": request})
            serializer.is_valid(raise_exception=True)
            #text=serializer.validated_data['text']
            #instance.send_referral(request,serializer.validated_data['email'])
            instance.send_referral(request,serializer.validated_data['email'], serializer.validated_data['text'])
            #serializer = InternalProposalSerializer(instance,context={'request':request})
            serializer_class = self.internal_serializer_class()
            serializer = serializer_class(instance,context={'request':request})
            return Response(serializer.data)
        except serializers.ValidationError:
            print(traceback.print_exc())
            raise
        except ValidationError as e:
            handle_validation_error(e)
        except Exception as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(str(e))
    
    # with new workflow not used at the moment
    @detail_route(methods=['POST',], detail=True)
    def proposed_decline(self, request, *args, **kwargs):
        try:
            instance = self.get_object()
            serializer = ProposedDeclineSerializer(data=request.data)
            serializer.is_valid(raise_exception=True)
            instance.proposed_decline(request,serializer.validated_data)
            serializer_class = self.internal_serializer_class()
            serializer = serializer_class(instance,context={'request':request})
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
    
    @detail_route(methods=['POST',], detail=True)
    def final_decline(self, request, *args, **kwargs):
        try:
            instance = self.get_object()
            serializer = ProposedDeclineSerializer(data=request.data)
            serializer.is_valid(raise_exception=True)
            instance.final_decline(request,serializer.validated_data)
            serializer_class = self.internal_serializer_class()
            serializer = serializer_class(instance,context={'request':request})
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

    # with new workflow not used at the moment
    @detail_route(methods=['POST',], detail=True)
    def proposed_approval(self, request, *args, **kwargs):
        try:
            instance = self.get_object()
            serializer = ProposedApprovalSerializer(data=request.data)
            serializer.is_valid(raise_exception=True)
            instance.proposed_approval(request,serializer.validated_data)
            #serializer = InternalProposalSerializer(instance,context={'request':request})
            serializer_class = self.internal_serializer_class()
            serializer = serializer_class(instance,context={'request':request})
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
    
    @detail_route(methods=['POST',], detail=True)
    def final_approval(self, request, *args, **kwargs):
        try:
            instance = self.get_object()
            serializer = ProposedApprovalSerializer(data= json.loads(request.data.get('data')))
            serializer.is_valid(raise_exception=True)
            instance.final_approval(request,serializer.validated_data)
            serializer_class = self.internal_serializer_class()
            serializer = serializer_class(instance,context={'request':request})
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
    
    @detail_route(methods=['POST',], detail=True)
    def switch_status(self, request, *args, **kwargs):
        try:
            instance = self.get_object()
            status = request.data.get('status')
            approver_comment = request.data.get('approver_comment')
            if not status:
                raise serializers.ValidationError('Status is required')
            else:
                if not status in ['with_assessor','with_approver']:
                    raise serializers.ValidationError('The status provided is not allowed')
            instance.move_to_status(request,status, approver_comment)
            serializer_class = self.internal_serializer_class()
            serializer = serializer_class(instance,context={'request':request})
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
    
    @detail_route(methods=['POST',], detail=True)
    def proposed_ready_for_agenda(self, request, *args, **kwargs):
        try:
            instance = self.get_object()
            instance.proposed_ready_for_agenda(request)
            #serializer = InternalProposalSerializer(instance,context={'request':request})
            serializer_class = self.internal_serializer_class()
            serializer = serializer_class(instance,context={'request':request})
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
    
    @detail_route(methods=['GET',], detail=True)
    def documents(self, request, *args, **kwargs):
        try:
            instance = self.get_object()
            qs = instance.documents.all()
            qs = qs.exclude(input_name='conservation_status_approval_doc') # TODO do we need/not to show approval doc in cs documents tab
            qs = qs.order_by('-uploaded_date')
            serializer = ConservationStatusDocumentSerializer(qs,many=True, context={'request':request})
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


class ConservationStatusReferralViewSet(viewsets.ModelViewSet):
    #queryset = Referral.objects.all()
    queryset = ConservationStatusReferral.objects.none()
    serializer_class = ConservationStatusReferralSerializer

    def get_queryset(self):
        user = self.request.user
        if user.is_authenticated and is_internal(self.request):
            #queryset =  Referral.objects.filter(referral=user)
            queryset =  ConservationStatusReferral.objects.all()
            return queryset
        return ConservationStatusReferral.objects.none()

    def get_serializer_class(self):
        try:
            return ConservationStatusReferralSerializer
        except serializers.ValidationError:
            print(traceback.print_exc())
            raise
        except ValidationError as e:
            handle_validation_error(e)
        except Exception as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(str(e))

    #used for Species (flora/Fauna)Referred to me internal dashboard filters
    @list_route(methods=['GET',], detail=False)
    def filter_list(self, request, *args, **kwargs):
        """ Used by the internal Referred to me dashboard filters """
        #qs =  self.get_queryset().filter(referral=request.user)
        qs =  self.get_queryset()
        # region_qs =  qs.filter(proposal__region__isnull=False).values_list('proposal__region__name', flat=True).distinct()
        # #district_qs =  qs.filter(proposal__district__isnull=False).values_list('proposal__district__name', flat=True).distinct()
        # activity_qs =  qs.filter(proposal__activity__isnull=False).order_by('proposal__activity').distinct('proposal__activity').values_list('proposal__activity', flat=True).distinct()
        # submitter_qs = qs.filter(proposal__submitter__isnull=False).order_by('proposal__submitter').distinct('proposal__submitter').values_list('proposal__submitter__first_name','proposal__submitter__last_name','proposal__submitter__email')
        # submitters = [dict(email=i[2], search_term='{} {} ({})'.format(i[0], i[1], i[2])) for i in submitter_qs]
        # processing_status_qs =  qs.filter(proposal__processing_status__isnull=False).order_by('proposal__processing_status').distinct('proposal__processing_status').values_list('proposal__processing_status', flat=True)
        # processing_status = [dict(value=i, name='{}'.format(' '.join(i.split('_')).capitalize())) for i in processing_status_qs]
        # application_types=ApplicationType.objects.filter(visible=True).values_list('name', flat=True)
        # data = dict(
        #     regions=region_qs,
        #     #districts=district_qs,
        #     activities=activity_qs,
        #     submitters=submitters,
        #     processing_status_choices=processing_status,
        #     application_types=application_types,
        # )

        qs =  self.get_queryset()
        group_type = request.GET.get('group_type_name','')
        species_data_list = []
        if group_type:
            species_qs = qs.filter(conservation_status__species__group_type__name=group_type).values_list('conservation_status__species', flat=True).distinct()
            species = Species.objects.filter(id__in=species_qs)
            if species:
                for i in species:
                    species_data_list.append({
                        'species_id': i.id,
                        });
        scientific_name_list = []
        if group_type:
            taxonomy_qs = qs.filter(conservation_status__species__group_type__name=group_type).values_list('conservation_status__species__taxonomy', flat=True).distinct()
            names = Taxonomy.objects.filter(id__in=taxonomy_qs) # TODO will need to filter according to  group  selection
            if names:
                for name in names:
                    scientific_name_list.append({
                        'id': name.id,
                        'name': name.scientific_name,
                        });
        common_name_list = []
        if group_type:
            taxonomy_qs = qs.filter(conservation_status__species__group_type__name=group_type).values_list('conservation_status__species__taxonomy', flat=True).distinct()
            common_names = TaxonVernacular.objects.filter(taxonomy__in=taxonomy_qs)
            if common_names:
                for name in common_names:
                    common_name_list.append({
                        'id': name.id,
                        'name': name.vernacular_name,
                        });
        family_list = []
        if group_type:
            taxonomy_qs = qs.filter(conservation_status__species__group_type__name=group_type).values_list('conservation_status__species__taxonomy', flat=True).distinct()
            # family_qs = Taxonomy.objects.filter(id__in=taxonomy_qs).values_list('family', flat=True).distinct()
            # families = Family.objects.filter(id__in=family_qs) # TODO will need to filter according to  group  selection
            families_qs = Taxonomy.objects.filter(Q(id__in=taxonomy_qs) & ~Q(family_fk=None)).order_by().values_list('family_fk', flat=True).distinct() # fetch all distinct the family_nid(taxon_name_id) for each taxon
            families = Taxonomy.objects.filter(id__in=families_qs)
            
            if families:
                for family in families:
                    family_list.append({
                        'id': family.id,
                        'name': family.scientific_name,
                        });
        phylogenetic_group_list = []
        if group_type:
            taxonomy_qs = qs.filter(conservation_status__species__group_type__name=group_type).values_list('conservation_status__species__taxonomy', flat=True).distinct()
            phylo_group_qs = Taxonomy.objects.filter(Q(id__in=taxonomy_qs) & ~Q(informal_groups=None)).values_list('informal_groups__classification_system_fk', flat=True).distinct()
            phylo_groups = ClassificationSystem.objects.filter(id__in=phylo_group_qs) # TODO will need to filter according to  group  selection
            if phylo_groups:
                for group in phylo_groups:
                    phylogenetic_group_list.append({
                        'id': group.id,
                        'name': group.class_desc,
                        });
        genus_list = []
        if group_type:
            taxonomy_qs = qs.filter(conservation_status__species__group_type__name=group_type).values_list('conservation_status__species__taxonomy', flat=True).distinct()
            genus_qs = Taxonomy.objects.filter(id__in=taxonomy_qs).values_list('genus', flat=True).distinct()
            generas = Genus.objects.filter(id__in=genus_qs) # TODO will need to filter according to  group  selection
            if generas:
                for genus in generas:
                    genus_list.append({
                        'id': genus.id,
                        'name': genus.name,
                        });
        conservation_list_dict = []
        cons_list_qs = qs.filter(conservation_status__species__group_type__name=group_type).values_list('conservation_status__conservation_list', flat=True).distinct()
        conservation_lists = ConservationList.objects.filter(id__in=cons_list_qs)
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
        processing_status_list = []
        processing_statuses =  qs.filter(conservation_status__processing_status__isnull=False).order_by('conservation_status__processing_status').distinct('conservation_status__processing_status').values_list('conservation_status__processing_status', flat=True)
        if processing_statuses:
            for status in processing_statuses:
                processing_status_list.append({
                    'value': status,
                    'name': '{}'.format(' '.join(status.split('_')).capitalize()),
                    });
        res_json = {
        "species_data_list":species_data_list,
        "scientific_name_list": scientific_name_list,
        "common_name_list": common_name_list,
        "family_list": family_list,
        "phylogenetic_group_list":phylogenetic_group_list,
        "genus_list":genus_list,
        "conservation_list_dict":conservation_list_dict,
        "conservation_category_list":conservation_category_list,
        "processing_status_list": processing_status_list,
        }
        res_json = json.dumps(res_json)
        return HttpResponse(res_json, content_type='application/json')
        return Response(data)

    @list_route(methods=['GET',], detail=False)
    def community_filter_list(self, request, *args, **kwargs):
        """ Used by the internal referred to me dashboard filters for community """
        #qs =  self.get_queryset().filter(referral=request.user)
        qs =  self.get_queryset()
        group_type = request.GET.get('group_type_name','')
        community_data_list = []
        if group_type:
            taxonomy_qs = qs.filter(conservation_status__community__isnull=False).values_list('conservation_status__community__taxonomy', flat=True).distinct()
            names = CommunityTaxonomy.objects.filter(id__in=taxonomy_qs) # TODO will need to filter according to  group  selection
            if names:
                for name in names:
                    community_data_list.append({
                        'id': name.id,
                        'community_migrated_id': name.community_migrated_id,
                        'community_name': name.community_name,
                        });
        conservation_list_dict = []
        cons_list_qs = qs.filter(conservation_status__community__isnull=False).values_list('conservation_status__conservation_list', flat=True).distinct()
        conservation_lists = ConservationList.objects.filter(id__in=cons_list_qs)
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
        processing_status_list = []
        processing_statuses =  qs.filter(conservation_status__processing_status__isnull=False).order_by('conservation_status__processing_status').distinct('conservation_status__processing_status').values_list('conservation_status__processing_status', flat=True)
        if processing_statuses:
            for status in processing_statuses:
                processing_status_list.append({
                    'value': status,
                    'name': '{}'.format(' '.join(status.split('_')).capitalize()),
                    });
        res_json = {
        "community_data_list":community_data_list,
        "conservation_list_dict":conservation_list_dict,
        "conservation_category_list":conservation_category_list,
        "processing_status_list": processing_status_list,
        }
        res_json = json.dumps(res_json)
        return HttpResponse(res_json, content_type='application/json')
        return Response(data)


    # def retrieve(self, request, *args, **kwargs):
    #     instance = self.get_object()
    #     serializer = self.get_serializer(instance, context={'request':request})
    #     return Response(serializer.data)

    # @list_route(methods=['GET',])
    # def user_list(self, request, *args, **kwargs):
    #     qs = self.get_queryset().filter(referral=request.user)
    #     serializer = DTReferralSerializer(qs, many=True)
    #     #serializer = DTReferralSerializer(self.get_queryset(), many=True)
    #     return Response(serializer.data)

    @list_route(methods=['GET',], detail=False)
    def datatable_list(self, request, *args, **kwargs):
        conservation_status = request.GET.get('conservation_status',None)
        qs = self.get_queryset().all()
        if conservation_status:
            qs = qs.filter(conservation_status_id=int(conservation_status))
        serializer = DTConservationStatusReferralSerializer(qs, many=True, context={"request": request})
        return Response(serializer.data)
    
    @detail_route(methods=['GET',], detail=True)
    def referral_list(self, request, *args, **kwargs):
        instance = self.get_object()
        qs = self.get_queryset().all()
        qs=qs.filter(sent_by=instance.referral, conservation_status=instance.conservation_status)
        serializer = DTConservationStatusReferralSerializer(qs, many=True, context={"request": request})
        #serializer = ProposalReferralSerializer(qs, many=True)
        return Response(serializer.data)
    
    @detail_route(methods=['GET', 'POST'], detail=True)
    def complete(self, request, *args, **kwargs):
        try:
            instance = self.get_object()
            #referral_comment = request.data.get('referral_comment')
            #instance.complete(request, referral_comment)
            instance.complete(request)
            serializer = self.get_serializer(instance, context={'request':request})
            return Response(serializer.data)
        except serializers.ValidationError:
            print(traceback.print_exc())
            raise
        except ValidationError as e:
            raise serializers.ValidationError(repr(e.error_dict))
        except Exception as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(str(e))

    @detail_route(methods=['GET',],detail=True,)
    def remind(self, request, *args, **kwargs):
        try:
            instance = self.get_object()
            instance.remind(request)
            serializer = InternalConservationStatusSerializer(instance.conservation_status,context={'request':request})
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

    @detail_route(methods=['GET',],detail=True,)
    def recall(self, request, *args, **kwargs):
        try:
            instance = self.get_object()
            instance.recall(request)
            serializer = InternalConservationStatusSerializer(instance.conservation_status,context={'request':request})
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

    @detail_route(methods=['GET',],detail=True,)
    def resend(self, request, *args, **kwargs):
        try:
            instance = self.get_object()
            instance.resend(request)
            serializer = InternalConservationStatusSerializer(instance.conservation_status,context={'request':request})
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

    #used on referral form
    @detail_route(methods=['post'], detail=True)
    def send_referral(self, request, *args, **kwargs):
        try:
            instance = self.get_object()
            serializer = SendReferralSerializer(data=request.data, context={"request": request})
            serializer.is_valid(raise_exception=True)
            instance.send_referral(request,serializer.validated_data['email'],serializer.validated_data['text'])
            serializer = self.get_serializer(instance, context={'request':request})
            return Response(serializer.data)
        except serializers.ValidationError:
            print(traceback.print_exc())
            raise
        except ValidationError as e:
            handle_validation_error(e)
        except Exception as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(str(e))
    
    @detail_route(methods=['post'], detail=True)
    @renderer_classes((JSONRenderer,))
    def conservation_status_referral_save(self, request, *args, **kwargs):
        try:
            with transaction.atomic():
                instance = self.get_object() 
                request_data = request.data
                instance.referral_comment=request_data.get('referral_comment')
                instance.save()
                # Create a log entry for the proposal
                instance.conservation_status.log_user_action(
                    ConservationStatusUserAction.COMMENT_REFERRAL.format(
                        instance.id,
                        instance.conservation_status.conservation_status_number,
                        '{}({})'.format(instance.referral_as_email_user.get_full_name(),instance.referral_as_email_user.email),
                    ),
                    request,
                )
            return redirect(reverse('internal'))
        
        except serializers.ValidationError:
            print(traceback.print_exc())
            raise
        except ValidationError as e:
            raise serializers.ValidationError(repr(e.error_dict))
        except Exception as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(str(e))


class ConservationStatusAmendmentRequestViewSet(viewsets.ModelViewSet):
    queryset = ConservationStatusAmendmentRequest.objects.all()
    serializer_class = ConservationStatusAmendmentRequestSerializer

    def create(self, request, *args, **kwargs):
        try:
            serializer = self.get_serializer(data= json.loads(request.data.get('data')))
            serializer.is_valid(raise_exception = True)
            instance = serializer.save()
            instance.add_documents(request)
            instance.generate_amendment(request)
            serializer = self.get_serializer(instance)
            return Response(serializer.data)
        except serializers.ValidationError:
            print(traceback.print_exc())
            raise
        except ValidationError as e:
            handle_validation_error(e)
        except Exception as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(str(e))

    @detail_route(methods=['POST',], detail=True)
    @renderer_classes((JSONRenderer,))
    def delete_document(self, request, *args, **kwargs):
        try:
            instance = self.get_object()
            ConservationStatusAmendmentRequestDocument.objects.get(id=request.data.get('id')).delete()
            return Response([dict(id=i.id, name=i.name,_file=i._file.url) for i in instance.cs_amendment_request_documents.all()])
        except serializers.ValidationError:
            print(traceback.print_exc())
            raise
        except ValidationError as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(repr(e.error_dict))
        except Exception as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(str(e))
    

class ConservationStatusDocumentViewSet(viewsets.ModelViewSet):
    queryset = ConservationStatusDocument.objects.all().order_by('id')
    serializer_class = ConservationStatusDocumentSerializer

    @detail_route(methods=['GET',], detail=True)
    def discard(self, request, *args, **kwargs):
        try:
            instance = self.get_object()
            instance.visible = False
            instance.save()
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
            serializer = SaveConservationStatusDocumentSerializer(instance, data=json.loads(request.data.get('data')))
            serializer.is_valid(raise_exception=True)
            serializer.save()
            instance.add_documents(request)
            return Response(serializer.data)
        except Exception as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(str(e))


    def create(self, request, *args, **kwargs):
        try:
            serializer = SaveConservationStatusDocumentSerializer(data= json.loads(request.data.get('data')))
            serializer.is_valid(raise_exception = True)
            instance = serializer.save()
            instance.add_documents(request)
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


class AmendmentRequestReasonChoicesView(views.APIView):

    renderer_classes = [JSONRenderer,]
    def get(self,request, format=None):
        choices_list = []
        #choices = AmendmentRequest.REASON_CHOICES
        choices=ProposalAmendmentReason.objects.all()
        if choices:
            for c in choices:
                #choices_list.append({'key': c[0],'value': c[1]})
                choices_list.append({'key': c.id,'value': c.reason})
        return Response(choices_list)
