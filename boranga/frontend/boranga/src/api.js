var site_url = location.origin;
var group_type_flora = 'flora';
var group_type_fauna = 'fauna';
var group_type_community = 'community';

export default {
    amendment_request: '/api/amendment_request.json',
    application_types: '/api/application_types.json',
    approvals: '/api/approvals.json',
    associated_species_taxonomy: '/api/associated_species_taxonomy/',
    community_documents: '/api/community_documents.json',
    community: '/api/community',
    external_community: '/api/external_community',
    conservation_status_documents: '/api/conservation_status_documents.json',
    conservation_status: '/api/conservation_status',
    content_types: '/api/content_types/',
    contributors: '/api/contributors',
    countries: '/api/countries',
    cs_external_referee_invites: '/api/cs_external_referee_invites',
    cs_referrals: '/api/cs_referrals.json',
    districts: '/api/districts.json',
    document_categories_dict: '/api/document_categories_dict',
    filtered_organisations: '/api/filtered_organisations',
    gis_extent: '/api/gis-extent/',
    help_text_entries: '/api/help_text_entries',
    lookup_schema_types:
        '/api/occurrence_report_bulk_import_schema_columns/get_lookup_filter_types/',
    observation_times: '/api/occurrence_report/observation_times/',
    occurrence_report_bulk_imports: '/api/occurrence_report_bulk_imports/',
    occurrence_report_bulk_import_schemas:
        '/api/occurrence_report_bulk_import_schemas/',
    occurrence_report_bulk_import_schemas_by_group_type:
        '/api/occurrence_report_bulk_import_schemas/get_schema_list_by_group_type/',
    occurrences_by_species_id: '/api/occurrence/by_species_id/',
    ocr_external_referee_invites: '/api/ocr_external_referee_invites',
    ocr_referrals: '/api/ocr_referrals.json',
    organisation_access_group_members: '/api/organisation_access_group_members',
    organisation_contacts: '/api/organisation_contacts.json',
    organisation_requests: '/api/organisation_requests.json',
    organisations: '/api/organisations.json',
    overdue_invoices: '/api/overdue_invoices.json',
    profile: '/api/profile',
    proposal_requirements: '/api/proposal_requirements.json',
    proposal_standard_requirements: '/api/proposal_standard_requirements.json',
    proposal_submit: '/api/proposal_submit.json',
    proposal_type: '/api/proposal_type',
    proposals: '/api/proposal.json',
    referral_recipient_groups: '/api/referrals/user_group_list',
    referrals: '/api/referrals.json',
    regions: '/api/regions.json',
    save_submitter_information: '/api/save_submitter_information',
    save_area_of_interest: '/api/save_area_of_interest',
    outstanding_referrals: '/api/outstanding_referrals',
    species_documents: '/api/species_documents.json',
    species: '/api/species',
    external_species: '/api/external_species',
    species_roles: '/api/species_roles/',
    submitter_categories: '/api/submitter_categories',
    taxonomy: '/api/taxonomy',
    threat: '/api/threat.json',
    users_api: '/api/users',
    users: '/api/users.json',

    // used in internal and external dashboards
    approvals_paginated_external:
        '/api/approval_paginated/approvals_external/?format=datatables',
    common_name_lookup: '/api/common_name_lookup',
    common_name_lookup_ocr_select: '/api/common_name_lookup_ocr_select',
    communities_lookup: '/api/communities_lookup',
    communities_paginated_external:
        '/api/communities_paginated/communities_external/?format=datatables',
    communities_paginated_internal:
        '/api/communities_paginated/communities_internal/?format=datatables',
    community_agenda_conservation_status_paginated_internal:
        '/api/community_conservation_status_paginated/agenda_cs_internal/?format=datatables',
    community_conservation_plans_paginated_internal:
        '/api/community_conservation_plans_paginated/community_cp_internal/?format=datatables',
    community_conservation_status_paginated_internal:
        '/api/community_conservation_status_paginated/community_cs_internal/?format=datatables',
    community_conservation_status_referrals_paginated_internal:
        '/api/community_conservation_status_paginated/community_cs_referrals_internal/?format=datatables',
    community_filter_dict: '/api/community_filter_dict',
    community_id_lookup: '/api/community_id_lookup',
    community_name_lookup: '/api/community_name_lookup',
    conservation_status_paginated_external:
        '/api/conservation_status_paginated/conservation_status_external/?format=datatables',
    conservation_status_referred_to_me:
        '/api/conservation_status_paginated/referred_to_me/?format=datatables',
    family_lookup: '/api/family_lookup',
    filter_lists_species: '/api/filter_lists_species',
    genera_lookup: '/api/genera_lookup',
    group_types_dict: '/api/group_types_dict',
    meeting_agenda_items: '/api/meeting_agenda_items.json',
    meetings_paginated: '/api/meeting_paginated/?format=datatables',
    occurrence_lookup: '/api/occurrence_paginated/occurrence_lookup',
    occurrence_name_lookup: '/api/occurrence_paginated/occurrence_name_lookup',
    combine_occurrence_name_lookup:
        '/api/occurrence_paginated/combine_occurrence_name_lookup',
    occurrence_internal_export:
        '/api/occurrence_paginated/occurrence_internal_export',
    occurrence_paginated_internal:
        '/api/occurrence_paginated/occurrence_internal/?format=datatables',
    occurrence_report_external_export:
        '/api/occurrence_report_paginated/occurrence_report_external_export',
    occurrence_report_internal_export:
        '/api/occurrence_report_paginated/occurrence_report_internal_export',
    community_occurrence_report_internal_export:
        '/api/occurrence_report_paginated/community_occurrence_report_internal_export',
    occurrence_report_paginated_external:
        '/api/occurrence_report_paginated/occurrence_report_external/?format=datatables',
    occurrence_report_paginated_internal:
        '/api/occurrence_report_paginated/occurrence_report_internal/?format=datatables',
    occurrence_report_paginated_referred_to_me:
        '/api/occurrence_report_paginated/referred_to_me/?format=datatables',
    occurrence: '/api/occurrence/',
    phylo_group_lookup: '/api/phylo_group_lookup',
    proposals_paginated_external:
        '/api/proposal_paginated/proposals_external/?format=datatables',
    proposals_paginated_internal:
        '/api/proposal_paginated/proposals_internal/?format=datatables',
    referrals_paginated_internal:
        '/api/proposal_paginated/referrals_internal/?format=datatables',
    region_district_filter_dict: '/api/region_district_filter_dict',
    scientific_name_lookup_by_groupname:
        '/api/scientific_name_lookup_by_groupname',
    scientific_name_lookup: '/api/scientific_name_lookup',
    species_agenda_conservation_status_paginated_internal:
        '/api/species_conservation_status_paginated/agenda_cs_internal/?format=datatables',
    species_conservation_plans_paginated_internal:
        '/api/species_conservation_plans_paginated/species_cp_internal/?format=datatables',
    species_conservation_status_paginated_internal:
        '/api/species_conservation_status_paginated/species_cs_internal/?format=datatables',
    meeting_export: '/api/meeting_paginated/meeting_export',
    species_conservation_status_referrals_paginated_internal:
        '/api/species_conservation_status_paginated/species_cs_referrals_internal/?format=datatables',
    species_lookup: '/api/species_lookup',
    species_paginated_external:
        '/api/species_paginated/species_external/?format=datatables',
    species_paginated_internal:
        '/api/species_paginated/species_internal/?format=datatables',
    wild_status_lookup: '/api/wild_status_lookup',
    occurrence_tenure: '/api/occurrence_tenure/',
    occurrence_tenure_list_of_values:
        '/api/occurrence_tenure/occurrence_tenure_list_of_values/',
    occurrence_tenure_paginated_internal:
        '/api/occurrence_tenure_paginated/occurrence_tenure_internal/?format=datatables',
    occurrence_tenure_feature_id_lookup:
        '/api/occurrence_tenure_paginated/occurrence_tenure_feature_id_lookup',
    occurrence_tenure_vesting_lookup:
        '/api/occurrence_tenure_paginated/occurrence_tenure_vesting_lookup',
    occurrence_tenure_purpose_lookup:
        '/api/occurrence_tenure_paginated/occurrence_tenure_purpose_lookup',

    //conservation Status profile page list of value dict
    species_display: '/api/species_display',
    community_display: '/api/community_display',
    cs_profile_dict: '/api/cs_profile_dict',

    filter_list: '/api/proposal/filter_list.json',
    filter_list_approvals: '/api/approvals/filter_list.json',
    filter_list_referrals: '/api/referrals/filter_list.json',
    filter_list_district_proposals: '/api/district_proposals/filter_list.json',
    filter_list_cs_referrals: '/api/cs_referrals/filter_list.json',
    filter_list_cs_referrals_community:
        '/api/cs_referrals/community_filter_list.json',

    meeting: '/api/meeting',
    meeting_dict: '/api/meeting_dict',
    minutes: '/api/minutes.json',

    occurrence_report: '/api/occurrence_report',
    observer_detail: '/api/observer_detail.json',
    contact_detail: '/api/contact_detail.json',
    occurrence_report_documents: '/api/occurrence_report_documents.json',
    occurrence_documents: '/api/occurrence_documents.json',
    ocr_threat: '/api/ocr_threat.json',
    occ_threat: '/api/occ_threat.json',
    occ_site: '/api/occurrence_sites.json',
    occ_profile_dict: '/api/occ_profile_dict',

    combine_key_contacts_lookup:
        '/api/occurrence_paginated/combine_key_contacts_lookup/',
    combine_documents_lookup:
        '/api/occurrence_paginated/combine_documents_lookup/',
    combine_threats_lookup: '/api/occurrence_paginated/combine_threats_lookup/',
    combine_sites_lookup: '/api/occurrence_paginated/combine_sites_lookup/',
    combine_tenures_lookup: '/api/occurrence_paginated/combine_tenures_lookup/',

    tile_layer: '/api/tile_layer/',

    discard_cs_proposal: function (id) {
        return `/api/conservation_status/${id}/discard/`;
    },
    reinstate_cs_proposal: function (id) {
        return `/api/conservation_status/${id}/reinstate/`;
    },
    defer_cs_proposal: function (id) {
        return `/api/conservation_status/${id}/defer/`;
    },
    delist_cs_proposal: function (id) {
        return `/api/conservation_status/${id}/delist/`;
    },
    discard_community_proposal: function (id) {
        return `/api/community/${id}/discard/`;
    },
    reinstate_community_proposal: function (id) {
        return `/api/community/${id}/reinstate/`;
    },
    rename_community: function (id) {
        return `/api/community/${id}/rename/`;
    },
    discard_species_proposal: function (id) {
        return `/api/species/${id}/discard/`;
    },
    reinstate_species_proposal: function (id) {
        return `/api/species/${id}/reinstate/`;
    },
    remove_species_proposal: function (id) {
        return `/api/species/${id}/remove/`;
    },
    discard_meeting: function (id) {
        return `/api/meeting/${id}/discard/`;
    },
    get_empty_species_object: function (taxonomy_id) {
        return `/api/species/get_empty_species_object/?taxonomy_id=${taxonomy_id}`;
    },
    reinstate_meeting: function (id) {
        return `/api/meeting/${id}/reinstate/`;
    },
    discard_occ_proposal: function (id) {
        return `/api/occurrence/${id}/discard/`;
    },
    reinstate_occ_proposal: function (id) {
        return `/api/occurrence/${id}/reinstate/`;
    },
    discard_ocr_proposal: function (id) {
        return `/api/occurrence_report/${id}/discard/`;
    },
    reinstate_ocr_proposal: function (id) {
        return `/api/occurrence_report/${id}/reinstate/`;
    },
    discard_observer_detail: function (id) {
        return `/api/observer_detail/${id}.json`;
    },
    lookup_history_species_document: function (id) {
        return `/api/history/boranga/species-communities/SpeciesDocument/${id}/`;
    },
    lookup_history_species: function (id) {
        return `/api/history/boranga/species-communities/Species/${id}/`;
    },
    lookup_history_community_document: function (id) {
        return `/api/history/boranga/species-communities/CommunityDocument/${id}/`;
    },
    lookup_history_community: function (id) {
        return `/api/history/boranga/species-communities/Community/${id}/`;
    },

    lookup_history_conservation_status_document: function (id) {
        return `/api/history/boranga/conservation_status/ConservationStatusDocument/${id}/`;
    },
    lookup_history_conservation_status: function (id) {
        return `/api/history/boranga/conservation_status/ConservationStatus/${id}/`;
    },

    lookup_history_occurrence_report_document: function (id) {
        return `/api/history/boranga/occurrence/OccurrenceReportDocument/${id}/`;
    },
    lookup_history_occurrence_report: function (id) {
        return `/api/history/boranga/occurrence/OccurrenceReport/${id}/`;
    },

    lookup_history_ocr_observer_detail: function (id) {
        return `/api/history/boranga/occurrence/OCRObserverDetail/${id}/`;
    },

    lookup_history_occurrence_document: function (id) {
        return `/api/history/boranga/occurrence/OccurrenceDocument/${id}/`;
    },
    lookup_history_occurrence: function (id) {
        return `/api/history/boranga/occurrence/Occurrence/${id}/`;
    },

    lookup_history_conservation_threat: function (id) {
        return `/api/history/boranga/species-communities/ConservationThreat/${id}/`;
    },

    lookup_history_ocr_conservation_threat: function (id) {
        return `/api/history/boranga/occurrence/OCRConservationThreat/${id}/`;
    },

    lookup_history_occ_conservation_threat: function (id) {
        return `/api/history/boranga/occurrence/OCCConservationThreat/${id}/`;
    },

    lookup_history_occurrence_site: function (id) {
        return `/api/history/boranga/occurrence/OccurrenceSite/${id}/`;
    },

    lookup_history_occurrence_tenure: function (id) {
        return `/api/history/boranga/occurrence/OccurrenceTenure/${id}/`;
    },

    lookup_history_occ_contact_detail: function (id) {
        return `/api/history/boranga/occurrence/OCCContactDetail/${id}/`;
    },

    lookup_history_minutes: function (id) {
        return `/api/history/boranga/meeting/Minutes/${id}/`;
    },

    lookup_revision_versions: function (model, id) {
        return `/api/history/boranga/${model}/${id}/`;
    },

    lookup_ocr_section_values: function (model, id) {
        return `/api/occurrence_report/${id}/section_values/?section=${model}`;
    },

    lookup_occ_section_values: function (model, id) {
        return `/api/occurrence/${id}/section_values/?section=${model}`;
    },

    committee_members: function (committee_id) {
        return `/api/committee/${committee_id}/committee_members/`;
    },

    fields_by_model_name: function (model_name) {
        `/api/content_types/fields_by_model_name/?model_name=${model_name}`;
    },

    list_items: function (model_name) {
        return `/api/get_list_items/${model_name}/`;
    },

    group_type_community: group_type_community,
    group_type_fauna: group_type_fauna,
    group_type_flora: group_type_flora,
    group_types: ['Fauna', 'Flora', 'Communities'],
    site_url: site_url,
    system_name: 'Boranga System',
};
