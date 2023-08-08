from django.conf import settings
#from django.contrib import admin
from boranga.admin import admin
from django.conf.urls import url, include
from django.urls import path
from django.contrib.auth.views import LogoutView, LoginView

from django.contrib.auth import logout, login # DEV ONLY

from django.conf.urls.static import static
from rest_framework import routers
from boranga import views
#from boranga.admin import boranga_admin_site
from boranga.components.proposals import views as proposal_views
from boranga.components.organisations import views as organisation_views

from boranga.components.users import api as users_api
from boranga.components.organisations import api as org_api
from boranga.components.main import api as main_api
from boranga.components.proposals import api as proposal_api
from boranga.components.approvals import api as approval_api
from boranga.components.compliances import api as compliances_api
from boranga.components.species_and_communities import api as species_communities_api
from boranga.components.conservation_status import api as conservation_status_api
from boranga.components.conservation_plan import api as conservation_plans_api
from boranga.components.meetings import api as meeting_api
from ledger_api_client.urls import urlpatterns as ledger_patterns
from django import urls
from django.contrib.auth import views as auth_views
from django import conf

# API patterns
router = routers.DefaultRouter()
router.register(r'organisations',org_api.OrganisationViewSet)
router.register(r'proposal',proposal_api.ProposalViewSet)

router.register(r'species',species_communities_api.SpeciesViewSet)
router.register(r'community',species_communities_api.CommunityViewSet)
router.register(r'taxonomy',species_communities_api.TaxonomyViewSet)
router.register(r'community_taxonomy',species_communities_api.CommunityTaxonomyViewSet)
router.register(r'species_paginated',species_communities_api.SpeciesPaginatedViewSet)
router.register(r'communities_paginated',species_communities_api.CommunitiesPaginatedViewSet)
router.register(r'document_categories', species_communities_api.DocumentCategoryViewSet)
router.register(r'document_sub_categories', species_communities_api.DocumentSubCategoryViewSet)
router.register(r'species_documents',species_communities_api.SpeciesDocumentViewSet)
router.register(r'community_documents',species_communities_api.CommunityDocumentViewSet)
router.register(r'threat',species_communities_api.ConservationThreatViewSet)
router.register(r'species_conservation_status_paginated',conservation_status_api.SpeciesConservationStatusPaginatedViewSet)
router.register(r'community_conservation_status_paginated',conservation_status_api.CommunityConservationStatusPaginatedViewSet)
router.register(r'species_conservation_plans_paginated',conservation_plans_api.SpeciesConservationPlansPaginatedViewSet)
#router.register(r'community_conservation_plans_paginated',conservation_plans_api.CommunityConservationPlansPaginatedViewSet)
router.register(r'conservation_status_paginated',conservation_status_api.ConservationStatusPaginatedViewSet)
router.register(r'conservation_status',conservation_status_api.ConservationStatusViewSet)
router.register(r'conservation_status_documents',conservation_status_api.ConservationStatusDocumentViewSet)
router.register(r'cs_referrals',conservation_status_api.ConservationStatusReferralViewSet)
# router.register(r'species_conservation_status',conservation_status_api.SpeciesConservationStatusViewSet)
# router.register(r'community_conservation_status',conservation_status_api.CommunityConservationStatusViewSet)
router.register(r'cs_amendment_request',conservation_status_api.ConservationStatusAmendmentRequestViewSet)
router.register(r'meeting',meeting_api.MeetingViewSet)
router.register(r'meeting_paginated',meeting_api.MeetingPaginatedViewSet)
router.register(r'minutes',meeting_api.MinutesViewSet)
router.register(r'committee',meeting_api.CommitteeViewSet)
router.register(r'meeting_agenda_items',meeting_api.AgendaItemViewSet)

router.register(r'proposal_submit',proposal_api.ProposalSubmitViewSet)
router.register(r'proposal_paginated',proposal_api.ProposalPaginatedViewSet)
router.register(r'approval_paginated',approval_api.ApprovalPaginatedViewSet)
router.register(r'compliance_paginated',compliances_api.CompliancePaginatedViewSet)
router.register(r'referrals',proposal_api.ReferralViewSet)
router.register(r'approvals',approval_api.ApprovalViewSet)
router.register(r'compliances',compliances_api.ComplianceViewSet)
router.register(r'proposal_requirements',proposal_api.ProposalRequirementViewSet)
router.register(r'proposal_standard_requirements',proposal_api.ProposalStandardRequirementViewSet)
router.register(r'organisation_requests',org_api.OrganisationRequestsViewSet)
router.register(r'organisation_contacts',org_api.OrganisationContactViewSet)
router.register(r'my_organisations',org_api.MyOrganisationsViewSet)
router.register(r'users',users_api.UserViewSet)
router.register(r'amendment_request',proposal_api.AmendmentRequestViewSet)
router.register(r'compliance_amendment_request',compliances_api.ComplianceAmendmentRequestViewSet)
router.register(r'global_settings', main_api.GlobalSettingsViewSet)
#router.register(r'application_types', main_api.ApplicationTypeViewSet)
router.register(r'assessments', proposal_api.ProposalAssessmentViewSet)
router.register(r'required_documents', main_api.RequiredDocumentViewSet)
router.register(r'questions', main_api.QuestionViewSet)

api_patterns = [
    url(r'^api/profile$', users_api.GetProfile.as_view(), name='get-profile'),
    url(r'^api/countries$', users_api.GetCountries.as_view(), name='get-countries'),
    url(r'^api/department_users$', users_api.DepartmentUserList.as_view(), name='department-users-list'),
    url(r'^api/filtered_users$', users_api.UserListFilterView.as_view(), name='filtered_users'),
    #url(r'^api/filtered_organisations$', org_api.OrganisationListFilterView.as_view(), name='filtered_organisations'),
    url(r'^api/filtered_payments$', approval_api.ApprovalPaymentFilterViewSet.as_view(), name='filtered_payments'),
    url(r'^api/proposal_type$', proposal_api.GetProposalType.as_view(), name='get-proposal-type'),
    url(r'^api/empty_list$', proposal_api.GetEmptyList.as_view(), name='get-empty-list'),
    url(r'^api/organisation_access_group_members',org_api.OrganisationAccessGroupMembers.as_view(),name='organisation-access-group-members'),
    url(r'^api/',include(router.urls)),
    url(r'^api/amendment_request_reason_choices',proposal_api.AmendmentRequestReasonChoicesView.as_view(),name='amendment_request_reason_choices'),
    url(r'^api/compliance_amendment_reason_choices',compliances_api.ComplianceAmendmentReasonChoicesView.as_view(),name='amendment_request_reason_choices'),
    url(r'^api/search_keywords',proposal_api.SearchKeywordsView.as_view(),name='search_keywords'),
    url(r'^api/search_reference',proposal_api.SearchReferenceView.as_view(),name='search_reference'),
    url(r'^api/filter_lists_species',species_communities_api.GetSpeciesFilterDict.as_view(),name='get-filter_lists_species'),
    url(r'^api/group_types_dict',species_communities_api.GetGroupTypeDict.as_view(),name='get-group-types-dict'),
    url(r'^api/community_filter_dict',species_communities_api.GetCommunityFilterDict.as_view(),name='get-community-filter-dict'),
    url(r'^api/region_district_filter_dict',species_communities_api.GetRegionDistrictFilterDict.as_view(),name='get-region_district_filter_dict'),
    url(r'^api/species_profile_dict',species_communities_api.GetSpeciesProfileDict.as_view(),name='get-species-profile-dict'),
    url(r'^api/community_profile_dict',species_communities_api.GetCommunityProfileDict.as_view(),name='get-community-profile-dict'),
    url(r'^api/species_lookup$', species_communities_api.GetSpecies.as_view(), name='get-species'),
    url(r'^api/communities_lookup$', species_communities_api.GetCommunities.as_view(), name='get-communities'),
    url(r'^api/scientific_name_lookup$', species_communities_api.GetScientificName.as_view(), name='get-scientific-name'),
    url(r'^api/common_name_lookup$', species_communities_api.GetCommonName.as_view(), name='get-common-name'),
    url(r'^api/family_lookup$', species_communities_api.GetFamily.as_view(), name='get-family'),
    url(r'^api/genera_lookup$', species_communities_api.GetGenera.as_view(), name='get-genera'),
    url(r'^api/phylo_group_lookup$', species_communities_api.GetPhyloGroup.as_view(), name='get-phylo-group'),
    url(r'^api/community_id_lookup$', species_communities_api.GetCommunityId.as_view(), name='get-community_id'),
    url(r'^api/community_name_lookup$', species_communities_api.GetCommunityName.as_view(), name='get-community_name'),
    url(r'^api/cs_profile_dict$', conservation_status_api.GetCSProfileDict.as_view(), name='get-cs-profile-dict'),
    url(r'^api/conservation_list_dict',conservation_status_api.GetConservationListDict.as_view(),name='get-conservation-list-dict'),
    url(r'^api/proposal_amendment_request_reason_choices',conservation_status_api.AmendmentRequestReasonChoicesView.as_view(),name='amendment_request_reason_choices'),
    url(r'^api/meeting_dict$', meeting_api.GetMeetingDict.as_view(), name='get-meeting-dict'),
    #url(r'^api/oracle_job$',main_api.OracleJob.as_view(), name='get-oracle'),


    #url(r'^api/reports/booking_settlements$', main_api.BookingSettlementReportView.as_view(),name='booking-settlements-report'),
]

# URL Patterns
urlpatterns = [
    #url(r'^admin/', include(boranga_admin_site.urls)),
    #url(r'^admin/', boranga_admin_site.urls),
    path(r'admin/', admin.site.urls),
    #url(r'^login/', LoginView.as_view(),name='login'),
    #path('login/', login, name='login'),
    # url(r'^logout/$', LogoutView.as_view(), {'next_page': '/'}, name='logout'),
    url(r'', include(api_patterns)),
    #url(r'^$', views.BorangaRoutingView.as_view(), name='ds_home'),
    url(r'^$', views.BorangaRoutingView.as_view(), name='home'),
    url(r'^contact/', views.BorangaContactView.as_view(), name='ds_contact'),
    url(r'^further_info/', views.BorangaFurtherInformationView.as_view(), name='ds_further_info'),
    url(r'^internal/', views.InternalView.as_view(), name='internal'),
    url(r'^internal/proposal/(?P<proposal_pk>\d+)/referral/(?P<referral_pk>\d+)/$', views.ReferralView.as_view(), name='internal-referral-detail'),
    url(r'^external/', views.ExternalView.as_view(), name='external'),
    url(r'^firsttime/$', views.first_time, name='first_time'),
    url(r'^account/$', views.ExternalView.as_view(), name='manage-account'),
    url(r'^profiles/', views.ExternalView.as_view(), name='manage-profiles'),
    url(r'^help/(?P<application_type>[^/]+)/(?P<help_type>[^/]+)/$', views.HelpView.as_view(), name='help'),
    url(r'^mgt-commands/$', views.ManagementCommandsView.as_view(), name='mgt-commands'),
    #url(r'test-emails/$', proposal_views.TestEmailView.as_view(), name='test-emails'),
    url(r'^proposal/$', proposal_views.ProposalView.as_view(), name='proposal'),
    #url(r'^preview/licence-pdf/(?P<proposal_pk>\d+)',proposal_views.PreviewLicencePDFView.as_view(), name='preview_licence_pdf'),
    url(r'^private-media/', views.getPrivateFile, name='view_private_file'),

    #following url is defined so that to include url path when sending Proposal amendment request to user.
    url(r'^external/conservation_status/(?P<cs_proposal_pk>\d+)/$', views.ExternalConservationStatusView.as_view(), name='external-conservation-status-detail'),
    url(r'^internal/conservation_status/(?P<cs_proposal_pk>\d+)/$', views.InternalConservationStatusView.as_view(), name='internal-conservation-status-detail'),
    url(r'^internal/conservation-status/', views.InternalConservationStatusDashboardView.as_view(), name='internal-conservation-status-dashboard'),
    url(r'^internal/conservation_status/(?P<cs_proposal_pk>\d+)/cs_referral/(?P<referral_pk>\d+)/$', views.ConservationStatusReferralView.as_view(), name='internal-conservation-status-referral-detail'),
    url(r'^internal/species_communities/(?P<species_proposal_pk>\d+)/$', views.InternalSpeciesView.as_view(), name='internal-species-detail'),
    url(r'^internal/species_communities/(?P<community_proposal_pk>\d+)/$', views.InternalCommunityView.as_view(), name='internal-community-detail'),
    url(r'^internal/meetings/', views.InternalMeetingDashboardView.as_view(), name='internal-meeting-dashboard'),
    #url(r'^external/proposal/(?P<proposal_pk>\d+)/$', views.ExternalProposalView.as_view(), name='external-proposal-detail'),
    #url(r'^internal/proposal/(?P<proposal_pk>\d+)/$', views.InternalProposalView.as_view(), name='internal-proposal-detail'),
    #url(r'^external/compliance/(?P<compliance_pk>\d+)/$', views.ExternalComplianceView.as_view(), name='external-compliance-detail'),
    #url(r'^internal/compliance/(?P<compliance_pk>\d+)/$', views.InternalComplianceView.as_view(), name='internal-compliance-detail'),

    ##url(r'^organisations/(?P<pk>\d+)/confirm-delegate-access/(?P<uid>[0-9A-Za-z]+)-(?P<token>.+)/$', views.ConfirmDelegateAccess.as_view(), name='organisation_confirm_delegate_access'),
    ## reversion history-compare
    #url(r'^history/proposal/(?P<pk>\d+)/$', proposal_views.ProposalHistoryCompareView.as_view(), name='proposal_history'),
    #url(r'^history/filtered/(?P<pk>\d+)/$', proposal_views.ProposalFilteredHistoryCompareView.as_view(), name='proposal_filtered_history'),
    #url(r'^history/referral/(?P<pk>\d+)/$', proposal_views.ReferralHistoryCompareView.as_view(), name='referral_history'),
    #url(r'^history/approval/(?P<pk>\d+)/$', proposal_views.ApprovalHistoryCompareView.as_view(), name='approval_history'),
    #url(r'^history/compliance/(?P<pk>\d+)/$', proposal_views.ComplianceHistoryCompareView.as_view(), name='compliance_history'),
    #url(r'^history/proposaltype/(?P<pk>\d+)/$', proposal_views.ProposalTypeHistoryCompareView.as_view(), name='proposaltype_history'),
    #url(r'^history/helppage/(?P<pk>\d+)/$', proposal_views.HelpPageHistoryCompareView.as_view(), name='helppage_history'),
    #url(r'^history/organisation/(?P<pk>\d+)/$', organisation_views.OrganisationHistoryCompareView.as_view(), name='organisation_history'),


] + ledger_patterns

# if settings.EMAIL_INSTANCE != 'PROD':
#     urlpatterns.append(path('accounts/', include('django.contrib.auth.urls')))

if settings.DEBUG:  # Serve media locally in development.
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)

# DBCA Template URLs
urlpatterns.append(urls.path("logout/", auth_views.LogoutView.as_view(), {"next_page": "/"}, name="logout"))
if conf.settings.ENABLE_DJANGO_LOGIN:
    urlpatterns.append(urls.re_path(r"^ssologin/", auth_views.LoginView.as_view(), name="ssologin"))

#if settings.SHOW_DEBUG_TOOLBAR:
#    import debug_toolbar
#    urlpatterns = [
#        url('__debug__/', include(debug_toolbar.urls)),
#    ] + urlpatterns
