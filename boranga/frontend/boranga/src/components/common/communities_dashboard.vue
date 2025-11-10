<template id="communities_dashboard">
    <div>
        <CollapsibleFilters
            ref="collapsible_filters"
            component_title="Filters"
            class="mb-2"
            :show-warning-icon="filterApplied"
        >
            <div class="row">
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="community_id_lookup">Community ID:</label>
                        <select
                            id="community_id_lookup"
                            ref="community_id_lookup"
                            name="community_id_lookup"
                            class="form-control"
                        />
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="community_name_lookup"
                            >Community Name:</label
                        >
                        <select
                            id="community_name_lookup"
                            ref="community_name_lookup"
                            name="community_name_lookup"
                            class="form-control"
                        />
                    </div>
                </div>
                <div v-if="is_internal" class="col-md-3">
                    <div class="form-group">
                        <label for="">Status:</label>
                        <select
                            v-model="filterCommunityApplicationStatus"
                            class="form-select"
                        >
                            <option value="all">All</option>
                            <option
                                v-for="status in community_status"
                                :value="status.value"
                                :key="status.value"
                            >
                                {{ status.name }}
                            </option>
                        </select>
                    </div>
                </div>
                <div v-if="is_internal" class="col-md-3">
                    <div class="form-group">
                        <label for="submitter-category"
                            >Publication Status:</label
                        >
                        <select
                            id="submitter-category"
                            v-model="filterCommunityPublicationStatus"
                            class="form-select"
                        >
                            <option value="all">All</option>
                            <option value="False">Private</option>
                            <option value="True">Public</option>
                        </select>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="">Region:</label>
                        <select
                            v-model="filterCommunityRegion"
                            class="form-select"
                            @change="filterDistrict($event)"
                        >
                            <option value="all">All</option>
                            <option
                                v-for="region in region_list"
                                :key="region.id"
                                :value="region.id"
                            >
                                {{ region.name }}
                            </option>
                        </select>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="">District:</label>
                        <select
                            v-model="filterCommunityDistrict"
                            class="form-select"
                        >
                            <option value="all">All</option>
                            <option
                                v-for="district in filtered_district_list"
                                :value="district.id"
                                :key="district.id"
                            >
                                {{ district.name }}
                            </option>
                        </select>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="wa-legislative-list"
                            >WA Legislative List:</label
                        >
                        <select
                            id="wa-legislative-list"
                            v-model="filterCommunityWALegislativeList"
                            class="form-select"
                        >
                            <option value="all">All</option>
                            <option
                                v-for="list in wa_legislative_lists"
                                :value="list.id"
                                :key="list.id"
                            >
                                {{ list.code }}
                            </option>
                        </select>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="wa-legislative-category"
                            >WA Legislative Category:</label
                        >
                        <select
                            id="wa-legislative-category"
                            v-model="filterCommunityWALegislativeCategory"
                            class="form-select"
                        >
                            <option value="all">All</option>
                            <option
                                v-for="list in wa_legislative_categories"
                                :value="list.id"
                                :key="list.id"
                            >
                                {{ list.code }}
                            </option>
                        </select>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="wa-priority-category"
                            >WA Priority Category:</label
                        >
                        <select
                            id="wa-priority-category"
                            v-model="filterCommunityWAPriorityCategory"
                            class="form-select"
                        >
                            <option value="all">All</option>
                            <option
                                v-for="list in wa_priority_categories"
                                :value="list.id"
                                :key="list.id"
                            >
                                {{ list.code }}
                            </option>
                        </select>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label
                            class="form-check-label"
                            for="commonwealth-relevance"
                            >Commonwealth Relevance</label
                        >
                        <div class="form-check form-switch mt-1">
                            <input
                                id="commonwealth-relevance"
                                v-model="filterCommunityCommonwealthRelevance"
                                class="form-check-input"
                                type="checkbox"
                                true-value="true"
                                false-value="false"
                            />
                        </div>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label
                            class="form-check-label"
                            for="international-relevance"
                            >International Relevance</label
                        >
                        <div class="form-check form-switch mt-1">
                            <input
                                id="international-relevance"
                                v-model="filterCommunityInternationalRelevance"
                                class="form-check-input"
                                type="checkbox"
                                true-value="true"
                                false-value="false"
                            />
                        </div>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label
                            class="form-check-label"
                            for="conservation-criteria"
                            >Conservation Criteria</label
                        >
                        <input
                            id="conservation-criteria"
                            v-model="filterCommunityConsevationCriteria"
                            class="form-control"
                            type="input"
                            placeholder="Enter text to search for"
                        />
                    </div>
                </div>
            </div>
        </CollapsibleFilters>
        <div v-if="newCommunityVisibility" class="col-md-12">
            <div class="text-end">
                <button
                    type="button"
                    class="btn btn-primary mb-2"
                    @click.prevent="createCommunity"
                >
                    <i class="fa-solid fa-circle-plus"></i> New Community
                </button>
            </div>
        </div>
        <div class="row">
            <div class="col-lg-12">
                <datatable
                    :id="datatable_id"
                    ref="communities_datatable"
                    :dt-options="datatable_options"
                    :dt-headers="datatable_headers"
                />
            </div>
        </div>
        <div v-if="communityHistoryId">
            <CommunityHistory
                ref="community_history"
                :key="communityHistoryId"
                :community-id="communityHistoryId"
            />
        </div>
    </div>
</template>
<script>
import { v4 as uuid } from 'uuid';
import datatable from '@/utils/vue/datatable.vue';
import CollapsibleFilters from '@/components/forms/collapsible_component.vue';
import CommunityHistory from '../internal/species_communities/community_history.vue';

import { api_endpoints, constants, helpers } from '@/utils/hooks';
export default {
    name: 'CommunitiesTable',
    components: {
        datatable,
        CollapsibleFilters,
        CommunityHistory,
    },
    props: {
        level: {
            type: String,
            required: true,
            validator: function (val) {
                let options = ['internal', 'referral', 'external'];
                return options.indexOf(val) != -1 ? true : false;
            },
        },
        group_type_name: {
            type: String,
            required: true,
        },
        group_type_id: {
            type: Number,
            required: true,
            default: 0,
        },
        url: {
            type: String,
            required: true,
        },
        profile: {
            type: Object,
            default: null,
        },
        filterCommunityMigratedId_cache: {
            type: String,
            required: false,
            default: 'filterCommunityMigratedId',
        },
        filterCommunityName_cache: {
            type: String,
            required: false,
            default: 'filterCommunityName',
        },
        filterCommunityApplicationStatus_cache: {
            type: String,
            required: false,
            default: 'filterCommunityApplicationStatus',
        },
        filterCommunityPublicationStatus_cache: {
            type: String,
            required: false,
            default: 'filterCommunityPublicationStatus',
        },
        filterCommunityRegion_cache: {
            type: String,
            required: false,
            default: 'filterCommunityRegion',
        },
        filterCommunityDistrict_cache: {
            type: String,
            required: false,
            default: 'filterCommunityDistrict',
        },
        filterCommunityWALegislativeList_cache: {
            type: String,
            required: false,
            default: 'filterCommunityWALegislativeList',
        },
        filterCommunityWALegislativeCategory_cache: {
            type: String,
            required: false,
            default: 'filterCommunityWALegislativeCategory',
        },
        filterCommunityWAPriorityCategory_cache: {
            type: String,
            required: false,
            default: 'filterCommunityWAPriorityCategory',
        },
        filterCommunityCommonwealthRelevance_cache: {
            type: String,
            required: false,
            default: 'filterCommunityCommonwealthRelevance',
        },
        filterCommunityInternationalRelevance_cache: {
            type: String,
            required: false,
            default: 'filterCommunityInternationalRelevance',
        },
        filterCommunityConsevationCriteria_cache: {
            type: String,
            required: false,
            default: 'filterCommunityConsevationCriteria',
        },
    },
    data() {
        return {
            datatable_id: 'communities-datatable-' + uuid(),
            communityHistoryId: null,
            // selected values for filtering
            filterCommunityMigratedId: sessionStorage.getItem(
                this.filterCommunityMigratedId_cache
            )
                ? sessionStorage.getItem(this.filterCommunityMigratedId_cache)
                : 'all',

            filterCommunityName: sessionStorage.getItem(
                this.filterCommunityName_cache
            )
                ? sessionStorage.getItem(this.filterCommunityName_cache)
                : 'all',

            filterCommunityApplicationStatus: sessionStorage.getItem(
                this.filterCommunityApplicationStatus_cache
            )
                ? sessionStorage.getItem(
                      this.filterCommunityApplicationStatus_cache
                  )
                : 'all',

            filterCommunityPublicationStatus: sessionStorage.getItem(
                this.filterCommunityPublicationStatus_cache
            )
                ? sessionStorage.getItem(
                      this.filterCommunityPublicationStatus_cache
                  )
                : 'all',

            filterCommunityRegion: sessionStorage.getItem(
                this.filterCommunityRegion_cache
            )
                ? sessionStorage.getItem(this.filterCommunityRegion_cache)
                : 'all',

            filterCommunityDistrict: sessionStorage.getItem(
                this.filterCommunityDistrict_cache
            )
                ? sessionStorage.getItem(this.filterCommunityDistrict_cache)
                : 'all',

            filterCommunityWALegislativeList: sessionStorage.getItem(
                this.filterCommunityWALegislativeList_cache
            )
                ? sessionStorage.getItem(
                      this.filterCommunityWALegislativeList_cache
                  )
                : 'all',

            filterCommunityWALegislativeCategory: sessionStorage.getItem(
                this.filterCommunityWALegislativeCategory_cache
            )
                ? sessionStorage.getItem(
                      this.filterCommunityWALegislativeCategory_cache
                  )
                : 'all',

            filterCommunityWAPriorityCategory: sessionStorage.getItem(
                this.filterCommunityWAPriorityCategory_cache
            )
                ? sessionStorage.getItem(
                      this.filterCommunityWAPriorityCategory_cache
                  )
                : 'all',

            filterCommunityCommonwealthRelevance: sessionStorage.getItem(
                this.filterCommunityCommonwealthRelevance_cache
            )
                ? sessionStorage.getItem(
                      this.filterCommunityCommonwealthRelevance_cache
                  )
                : 'false',

            filterCommunityInternationalRelevance: sessionStorage.getItem(
                this.filterCommunityInternationalRelevance_cache
            )
                ? sessionStorage.getItem(
                      this.filterCommunityInternationalRelevance_cache
                  )
                : 'false',

            filterCommunityConsevationCriteria: sessionStorage.getItem(
                this.filterCommunityConsevationCriteria_cache
            )
                ? sessionStorage.getItem(
                      this.filterCommunityConsevationCriteria_cache
                  )
                : '',

            //Filter list for Community select box
            filterListsCommunities: {},

            filterRegionDistrict: {},
            region_list: [],
            district_list: [],
            filtered_district_list: [],
            wa_legislative_lists: [],
            wa_legislative_categories: [],
            wa_priority_categories: [],

            // filtering options
            external_status: [
                { value: 'draft', name: 'Draft' },
                { value: 'with_assessor', name: 'Under Review' },
                { value: 'approved', name: 'Approved' },
                { value: 'declined', name: 'Declined' },
                { value: 'discarded', name: 'Discarded' },
                { value: 'awaiting_payment', name: 'Awaiting Payment' },
            ],
            internal_status: [
                { value: 'draft', name: 'Draft' },
                { value: 'active', name: 'Active' },
                { value: 'discarded', name: 'Discarded' },
                { value: 'historical', name: 'Historical' },
            ],
            community_status: [],
        };
    },
    computed: {
        filterApplied: function () {
            if (
                this.filterCommunityMigratedId === 'all' &&
                this.filterCommunityName === 'all' &&
                this.filterCommunityApplicationStatus === 'all' &&
                this.filterCommunityPublicationStatus === 'all' &&
                this.filterCommunityRegion === 'all' &&
                this.filterCommunityDistrict === 'all' &&
                this.filterCommunityWALegislativeList === 'all' &&
                this.filterCommunityWALegislativeCategory === 'all' &&
                this.filterCommunityWAPriorityCategory === 'all' &&
                this.filterCommunityCommonwealthRelevance === 'false' &&
                this.filterCommunityInternationalRelevance === 'false' &&
                this.filterCommunityConsevationCriteria === ''
            ) {
                return false;
            } else {
                return true;
            }
        },
        is_external: function () {
            return this.level == 'external';
        },
        is_internal: function () {
            return this.level == 'internal';
        },
        is_referral: function () {
            return this.level == 'referral';
        },
        newCommunityVisibility: function () {
            return (
                this.profile?.user &&
                this.profile.user.groups.includes(
                    constants.GROUPS.SPECIES_AND_COMMUNITIES_APPROVERS
                )
            );
        },
        datatable_headers: function () {
            if (this.is_external) {
                return [
                    'Id',
                    'Number',
                    'Community Id',
                    'Community Name',
                    'Regions',
                    'Districts',
                    'WA Priority Category',
                    'WA Legislative List',
                    'WA Legislative Category',
                    'Commonwealth Conservation Category',
                    'Other Conservation Assessment',
                    'Conservation Criteria',
                    'Action',
                ];
            } else {
                return [
                    'Id',
                    'Number',
                    'Community Id',
                    'Community Name',
                    'Regions',
                    'Districts',
                    'WA Priority Category',
                    'WA Legislative List',
                    'WA Legislative Category',
                    'Commonwealth Conservation Category',
                    'Other Conservation Assessment',
                    'Conservation Criteria',
                    'Status',
                    'Action',
                ];
            }
        },
        column_id: function () {
            return {
                data: 'id',
                orderable: true,
                searchable: false,
                visible: false,
                render: function (data, type, full) {
                    return full.id;
                },
                name: 'id',
            };
        },
        column_number: function () {
            return {
                data: 'community_number',
                orderable: true,
                searchable: true,
                visible: true,
                render: function (data, type, full) {
                    return full.community_number;
                },
                name: 'id',
            };
        },
        column_community_id: function () {
            return {
                data: 'community_common_id',
                orderable: true,
                searchable: true,
                visible: true,
                render: function (value, type) {
                    let result = helpers.dtPopover(value, 30, 'hover');
                    return type == 'export' ? value : result;
                },
                name: 'taxonomy__community_common_id',
            };
        },
        column_community_name: function () {
            return {
                data: 'community_name',
                orderable: true,
                searchable: true,
                visible: true,
                render: function (value, type) {
                    let result = helpers.dtPopover(value, 30, 'hover');
                    return type == 'export' ? value : result;
                },
                name: 'taxonomy__community_name',
            };
        },
        column_status: function () {
            return {
                data: 'processing_status',
                orderable: true,
                searchable: true,
                visible: true,
                render: function (data, type, full) {
                    if (full.processing_status) {
                        if (
                            ['Active', 'Historical'].includes(
                                full.processing_status
                            ) &&
                            full.publishing_status
                        ) {
                            return (
                                full.processing_status +
                                ' - ' +
                                full.publishing_status.public_status
                            );
                        }
                        return full.processing_status;
                    }
                    // Should not reach here
                    return '';
                },
                name: 'processing_status',
            };
        },
        column_region: function () {
            return {
                data: 'regions',
                orderable: true,
                searchable: false,
                visible: true,
                render: function (data, type, full) {
                    if (full.regions) {
                        return full.regions;
                    }
                    // Should not reach here
                    return '';
                },
                name: 'regions__name',
            };
        },
        column_district: function () {
            return {
                data: 'districts',
                orderable: true,
                searchable: false,
                visible: true,
                render: function (data, type, full) {
                    if (full.districts) {
                        return full.districts;
                    }
                    // Should not reach here
                    return '';
                },
                name: 'districts__name',
            };
        },
        column_wa_legislative_list: function () {
            return {
                data: 'wa_legislative_list',
                orderable: false,
                searchable: false,
                visible: true,
            };
        },
        column_wa_legislative_category: function () {
            return {
                data: 'wa_legislative_category',
                orderable: false,
                searchable: false,
                visible: true,
            };
        },
        column_wa_priority_category: function () {
            return {
                data: 'wa_priority_category',
                orderable: false,
                searchable: false,
                visible: true,
            };
        },
        column_commonwealth_conservation_category: function () {
            return {
                data: 'commonwealth_conservation_category',
                orderable: false,
                searchable: false,
                visible: true,
            };
        },
        column_other_conservation_assessment: function () {
            return {
                data: 'other_conservation_assessment',
                orderable: false,
                searchable: false,
                visible: true,
            };
        },
        column_conservation_criteria: function () {
            return {
                data: 'conservation_criteria',
                orderable: false,
                searchable: false,
                visible: true,
            };
        },
        column_action: function () {
            let vm = this;
            return {
                // 9. Action
                data: 'id',
                orderable: false,
                searchable: false,
                visible: true,
                render: function (data, type, full) {
                    let links = '';
                    if (!vm.is_external) {
                        if (full.can_user_edit) {
                            if (full.processing_status == 'Discarded') {
                                links += `<a href='#' data-reinstate-community-proposal='${full.id}'>Reinstate</a><br/>`;
                            } else {
                                links += `<a href='/internal/species-communities/${full.id}?group_type_name=${full.group_type}'>Continue</a><br/>`;
                                links += `<a href='#${full.id}' data-discard-community-proposal='${full.id}'>Discard</a><br/>`;
                                links += `<a href='#' data-history-community='${full.id}'>History</a><br>`;
                            }
                        } else {
                            if (full.user_process) {
                                links += `<a href='/internal/species-communities/${full.id}?group_type_name=${full.group_type}&action=edit'>Edit</a><br/>`;
                            }
                            links += `<a href='/internal/species-communities/${full.id}?group_type_name=${full.group_type}&action=view'>View</a><br/>`;
                            links += `<a href='#' data-history-community='${full.id}'>History</a><br>`;
                        }
                    } else {
                        links += `<a href='/external/species-communities/${full.id}?group_type_name=${full.group_type}&action=view'>View</a><br/>`;
                    }
                    return links;
                },
            };
        },
        datatable_options: function () {
            let vm = this;

            let columns = [];
            let search = null;
            let buttons = [
                {
                    extend: 'excel',
                    title: 'Boranga S&C Communities Excel Export',
                    text: '<i class="fa-solid fa-download"></i> Excel',
                    className: 'btn btn-primary me-2 rounded',
                    exportOptions: {
                        columns: ':not(.no-export)',
                        orthogonal: 'export',
                    },
                },
                {
                    extend: 'csv',
                    title: 'Boranga S&C Communities CSV Export',
                    text: '<i class="fa-solid fa-download"></i> CSV',
                    className: 'btn btn-primary rounded',
                    exportOptions: {
                        columns: ':not(.no-export)',
                        orthogonal: 'export',
                    },
                },
            ];
            if (vm.is_external) {
                columns = [
                    vm.column_id,
                    vm.column_number,
                    vm.column_community_id,
                    vm.column_community_name,
                    vm.column_region,
                    vm.column_district,
                    vm.column_wa_priority_category,
                    vm.column_wa_legislative_list,
                    vm.column_wa_legislative_category,
                    vm.column_commonwealth_conservation_category,
                    vm.column_other_conservation_assessment,
                    vm.column_conservation_criteria,
                    vm.column_action,
                ];
                search = false;
            }
            if (vm.is_internal) {
                columns = [
                    vm.column_id,
                    vm.column_number,
                    vm.column_community_id,
                    vm.column_community_name,
                    vm.column_region,
                    vm.column_district,
                    vm.column_wa_priority_category,
                    vm.column_wa_legislative_list,
                    vm.column_wa_legislative_category,
                    vm.column_commonwealth_conservation_category,
                    vm.column_other_conservation_assessment,
                    vm.column_conservation_criteria,
                    vm.column_status,
                    vm.column_action,
                ];
                search = true;
            }

            return {
                autoWidth: false,
                language: {
                    processing: constants.DATATABLE_PROCESSING_HTML,
                },
                order: [[0, 'desc']],
                lengthMenu: [
                    [10, 25, 50, 100, 100000000],
                    [10, 25, 50, 100, 'All'],
                ],
                responsive: true,
                serverSide: true,
                searching: search,
                //  to show the "Action" column always in the last position
                columnDefs: [
                    { responsivePriority: 1, targets: 0 },
                    {
                        responsivePriority: 3,
                        targets: -1,
                        className: 'no-export',
                    },
                    { responsivePriority: 2, targets: -2 },
                ],
                ajax: {
                    url: this.url,
                    dataSrc: 'data',

                    // adding extra GET params for Custom filtering
                    data: function (d) {
                        d.filter_community_common_id =
                            vm.filterCommunityMigratedId;
                        d.filter_community_name = vm.filterCommunityName;
                        d.filter_group_type = vm.group_type_name;
                        d.filter_application_status =
                            vm.filterCommunityApplicationStatus;
                        d.filter_publication_status =
                            vm.filterCommunityPublicationStatus;
                        d.filter_region = vm.filterCommunityRegion;
                        d.filter_district = vm.filterCommunityDistrict;
                        d.filter_wa_legislative_list =
                            vm.filterCommunityWALegislativeList;
                        d.filter_wa_legislative_category =
                            vm.filterCommunityWALegislativeCategory;
                        d.filter_wa_priority_category =
                            vm.filterCommunityWAPriorityCategory;
                        d.filter_commonwealth_relevance =
                            vm.filterCommunityCommonwealthRelevance;
                        d.filter_international_relevance =
                            vm.filterCommunityInternationalRelevance;
                        d.filter_conservation_criteria =
                            vm.filterCommunityConsevationCriteria;
                        d.is_internal = vm.is_internal;
                    },
                },
                dom:
                    "<'d-flex align-items-center'<'me-auto'l>fB>" +
                    "<'row'<'col-sm-12'tr>>" +
                    "<'d-flex align-items-center'<'me-auto'i>p>",
                buttons: buttons,
                columns: columns,
                processing: true,
                drawCallback: function () {
                    helpers.enablePopovers();
                },
                initComplete: function () {
                    helpers.enablePopovers();
                },
            };
        },
    },
    watch: {
        filterCommunityMigratedId: function () {
            let vm = this;
            vm.$refs.communities_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                false
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterCommunityMigratedId_cache,
                vm.filterCommunityMigratedId
            );
        },
        filterCommunityName: function () {
            let vm = this;
            vm.$refs.communities_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                false
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterCommunityName_cache,
                vm.filterCommunityName
            );
        },
        filterCommunityApplicationStatus: function () {
            let vm = this;
            vm.$refs.communities_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                false
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterCommunityApplicationStatus_cache,
                vm.filterCommunityApplicationStatus
            );
        },
        filterCommunityPublicationStatus: function () {
            let vm = this;
            vm.$refs.communities_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                false
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterCommunityPublicationStatus_cache,
                vm.filterCommunityPublicationStatus
            );
        },
        filterCommunityRegion: function () {
            let vm = this;
            vm.$refs.communities_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                false
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterCommunityRegion_cache,
                vm.filterCommunityRegion
            );
        },
        filterCommunityDistrict: function () {
            let vm = this;
            vm.$refs.communities_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                false
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterCommunityDistrict_cache,
                vm.filterCommunityDistrict
            );
        },
        filterCommunityWALegislativeList: function () {
            let vm = this;
            vm.$refs.communities_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                false
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterCommunityWALegislativeList_cache,
                vm.filterCommunityWALegislativeList
            );
        },
        filterCommunityWALegislativeCategory: function () {
            let vm = this;
            vm.$refs.communities_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                false
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterCommunityWALegislativeCategory_cache,
                vm.filterCommunityWALegislativeCategory
            );
        },
        filterCommunityWAPriorityCategory: function () {
            let vm = this;
            vm.$refs.communities_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                false
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterCommunityWAPriorityCategory_cache,
                vm.filterCommunityWAPriorityCategory
            );
        },
        filterCommunityCommonwealthRelevance: function () {
            let vm = this;
            vm.$refs.communities_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                false
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterCommunityCommonwealthRelevance_cache,
                vm.filterCommunityCommonwealthRelevance
            );
        },
        filterCommunityInternationalRelevance: function () {
            let vm = this;
            vm.$refs.communities_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                false
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterCommunityInternationalRelevance_cache,
                vm.filterCommunityInternationalRelevance
            );
        },
        filterCommunityConsevationCriteria: function () {
            let vm = this;
            vm.$refs.communities_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                false
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterCommunityConsevationCriteria_cache,
                vm.filterCommunityConsevationCriteria
            );
        },
    },
    mounted: function () {
        this.fetchFilterLists();
        let vm = this;
        $('a[data-toggle="collapse"]').on('click', function () {
            var chev = $(this).children()[0];
            window.setTimeout(function () {
                $(chev).toggleClass(
                    'glyphicon-chevron-down glyphicon-chevron-up'
                );
            }, 100);
        });
        this.$nextTick(() => {
            vm.initialiseCommunityNameLookup();
            vm.initialiseCommunityIdLookup();
            vm.addEventListeners();
            var newOption = null;
            // -- to set the select2 field with the session value if exists onload()
            if (
                sessionStorage.getItem('filterCommunityName') != 'all' &&
                sessionStorage.getItem('filterCommunityName') != null
            ) {
                // contructor new Option(text, value, defaultSelected, selected)
                newOption = new Option(
                    sessionStorage.getItem('filterCommunityNameText'),
                    vm.filterCommunityName,
                    false,
                    true
                );
                $('#community_name_lookup').append(newOption);
            }
            if (
                sessionStorage.getItem('filterCommunityMigratedId') != 'all' &&
                sessionStorage.getItem('filterCommunityMigratedId') != null
            ) {
                // contructor new Option(text, value, defaultSelected, selected)
                newOption = new Option(
                    sessionStorage.getItem('filterCommunityMigratedIdText'),
                    vm.filterCommunityMigratedId,
                    false,
                    true
                );
                $('#community_id_lookup').append(newOption);
            }
        });
    },
    methods: {
        historyDocument: function (id) {
            this.communityHistoryId = parseInt(id);
            this.uuid++;
            this.$nextTick(() => {
                this.$refs.community_history.isModalOpen = true;
            });
        },
        initialiseCommunityNameLookup: function () {
            let vm = this;
            $(vm.$refs.community_name_lookup)
                .select2({
                    minimumInputLength: 2,
                    theme: 'bootstrap-5',
                    allowClear: true,
                    placeholder: 'Select Community Name',
                    ajax: {
                        url: api_endpoints.community_name_lookup,
                        dataType: 'json',
                        data: function (params) {
                            var query = {
                                term: params.term,
                                type: 'public',
                            };
                            return query;
                        },
                    },
                })
                .on('select2:select', function (e) {
                    let data = e.params.data.id;
                    vm.filterCommunityName = data;
                    sessionStorage.setItem(
                        'filterCommunityNameText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.filterCommunityName = 'all';
                    sessionStorage.setItem('filterCommunityNameText', '');
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-community_name_lookup-results"]'
                    );
                    // move focus to select2 field
                    searchField[0].focus();
                });
        },
        initialiseCommunityIdLookup: function () {
            let vm = this;
            $(vm.$refs.community_id_lookup)
                .select2({
                    minimumInputLength: 1,
                    theme: 'bootstrap-5',
                    allowClear: true,
                    placeholder: 'Select Community ID',
                    ajax: {
                        url: api_endpoints.community_id_lookup,
                        dataType: 'json',
                        data: function (params) {
                            var query = {
                                term: params.term,
                                type: 'public',
                            };
                            return query;
                        },
                    },
                })
                .on('select2:select', function (e) {
                    let data = e.params.data.id;
                    vm.filterCommunityMigratedId = data;
                    sessionStorage.setItem(
                        'filterCommunityMigratedIdText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.filterCommunityMigratedId = 'all';
                    sessionStorage.setItem('filterCommunityMigratedIdText', '');
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-community_id_lookup-results"]'
                    );
                    // move focus to select2 field
                    searchField[0].focus();
                });
        },
        fetchFilterLists: function () {
            let vm = this;
            fetch(
                api_endpoints.community_filter_dict +
                    '?group_type_name=' +
                    vm.group_type_name
            ).then(
                async (response) => {
                    vm.filterListsCommunities = await response.json();
                    vm.filterDistrict();
                    vm.community_status = vm.internal_status
                        .slice()
                        .sort((a, b) => {
                            return a.name.trim().localeCompare(b.name.trim());
                        });
                    vm.wa_legislative_lists =
                        vm.filterListsCommunities.wa_legislative_lists;
                    vm.wa_legislative_categories =
                        vm.filterListsCommunities.wa_legislative_categories;
                    vm.wa_priority_categories =
                        vm.filterListsCommunities.wa_priority_categories;
                },
                (error) => {
                    console.log(error);
                }
            );
            fetch(api_endpoints.region_district_filter_dict).then(
                async (response) => {
                    vm.filterRegionDistrict = await response.json();
                    vm.region_list = vm.filterRegionDistrict.region_list;
                    vm.district_list = vm.filterRegionDistrict.district_list;
                },
                (error) => {
                    console.log(error);
                }
            );
        },
        //-------filter district dropdown dependent on region selected
        filterDistrict: function (event) {
            this.$nextTick(() => {
                if (event) {
                    this.filterCommunityDistrict = 'all'; //-----to remove the previous selection
                }
                this.filtered_district_list = [];
                //---filter districts as per region selected
                for (let choice of this.district_list) {
                    if (
                        choice.region_id.toString() ===
                        this.filterCommunityRegion.toString()
                    ) {
                        this.filtered_district_list.push(choice);
                    }
                }
            });
        },
        createCommunity: async function () {
            swal.fire({
                title: `Add Community`,
                text: 'Are you sure you want to add a new community?',
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Add Community',
                reverseButtons: true,
                customClass: {
                    confirmButton: 'btn btn-primary',
                    cancelButton: 'btn btn-secondary',
                },
            }).then(async (swalresult) => {
                if (swalresult.isConfirmed) {
                    let newCommunityId = null;
                    try {
                        const createUrl = api_endpoints.community + '/';
                        let payload = new Object();
                        payload.group_type_id = this.group_type_id;
                        let response = await fetch(createUrl, {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify(payload),
                        });
                        const data = await response.json();
                        if (data) {
                            newCommunityId = data.id;
                        }
                    } catch (err) {
                        console.log(err);
                        if (this.is_internal) {
                            return err;
                        }
                    }
                    this.$router.push({
                        name: 'internal-species-communities',
                        params: { species_community_id: newCommunityId },
                        query: { group_type_name: this.group_type_name },
                    });
                }
            });
        },
        discardCommunityProposal: function (species_id) {
            let vm = this;
            swal.fire({
                title: 'Discard Proposal',
                text: 'Are you sure you want to discard this proposal?',
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Discard Proposal',
                customClass: {
                    confirmButton: 'btn btn-primary',
                    cancelButton: 'btn btn-secondary',
                },
                reverseButtons: true,
            }).then(
                (result) => {
                    if (result.isConfirmed) {
                        fetch(
                            api_endpoints.discard_community_proposal(
                                species_id
                            ),
                            {
                                method: 'PATCH',
                                headers: { 'Content-Type': 'application/json' },
                            }
                        ).then(
                            async (response) => {
                                if (!response.ok) {
                                    const data = await response.json();
                                    swal.fire({
                                        title: 'Error',
                                        text: JSON.stringify(data),
                                        icon: 'error',
                                        customClass: {
                                            confirmButton: 'btn btn-primary',
                                        },
                                    });
                                    return;
                                }
                                swal.fire({
                                    title: 'Discarded',
                                    text: 'Your proposal has been discarded',
                                    icon: 'success',
                                    customClass: {
                                        confirmButton: 'btn btn-primary',
                                    },
                                });
                                vm.$refs.communities_datatable.vmDataTable.ajax.reload(
                                    helpers.enablePopovers,
                                    false
                                );
                            },
                            (error) => {
                                console.log(error);
                            }
                        );
                    }
                },
                (error) => {
                    console.log(error);
                }
            );
        },
        reinstateCommunityProposal: function (species_id) {
            let vm = this;
            swal.fire({
                title: 'Reinstate Proposal',
                text: 'Are you sure you want to reinstate this proposal?',
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Reinstate Proposal',
                customClass: {
                    confirmButton: 'btn btn-primary',
                    cancelButton: 'btn btn-secondary',
                },
                reverseButtons: true,
            }).then(
                (result) => {
                    if (result.isConfirmed) {
                        fetch(
                            api_endpoints.reinstate_community_proposal(
                                species_id
                            ),
                            {
                                method: 'PATCH',
                                headers: { 'Content-Type': 'application/json' },
                            }
                        ).then(
                            async (response) => {
                                if (!response.ok) {
                                    const data = await response.json();
                                    swal.fire({
                                        title: 'Error',
                                        text: JSON.stringify(data),
                                        icon: 'error',
                                        customClass: {
                                            confirmButton: 'btn btn-primary',
                                        },
                                    });
                                    return;
                                }
                                swal.fire({
                                    title: 'Reinstated',
                                    text: 'Your proposal has been reinstated',
                                    icon: 'success',
                                    customClass: {
                                        confirmButton: 'btn btn-primary',
                                    },
                                });
                                vm.$refs.communities_datatable.vmDataTable.ajax.reload(
                                    helpers.enablePopovers,
                                    false
                                );
                            },
                            (error) => {
                                console.log(error);
                            }
                        );
                    }
                },
                (error) => {
                    console.log(error);
                }
            );
        },
        addEventListeners: function () {
            let vm = this;
            // External Discard listener
            vm.$refs.communities_datatable.vmDataTable.on(
                'click',
                'a[data-discard-community-proposal]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-discard-community-proposal');
                    vm.discardCommunityProposal(id);
                }
            );
            vm.$refs.communities_datatable.vmDataTable.on(
                'click',
                'a[data-reinstate-community-proposal]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-reinstate-community-proposal');
                    vm.reinstateCommunityProposal(id);
                }
            );
            vm.$refs.communities_datatable.vmDataTable.on(
                'click',
                'a[data-history-community]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-history-community');
                    vm.historyDocument(id);
                }
            );
            vm.$refs.communities_datatable.vmDataTable.on(
                'childRow.dt',
                function () {
                    helpers.enablePopovers();
                }
            );
        },
    },
};
</script>
