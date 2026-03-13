<template id="communities_occ_dashboard">
    <div>
        <CollapsibleFilters
            ref="collapsible_filters"
            component_title="Filters"
            class="mb-2"
            :show-warning-icon="filterApplied"
        >
            <div class="row">
                <div class="col-md-3">
                    <div
                        id="occurrence_name_lookup_form_group_id"
                        class="form-group"
                    >
                        <label for="occurrence_name_lookup"
                            >Occurrence Name:</label
                        >
                        <select
                            id="occurrence_name_lookup"
                            ref="occurrence_name_lookup"
                            name="occurrence_name_lookup"
                            class="form-control"
                        />
                    </div>
                </div>
                <div class="col-md-3">
                    <div id="select_occ_community_name" class="form-group">
                        <label for="occ_community_name_lookup"
                            >Community Name:</label
                        >
                        <select
                            id="occ_community_name_lookup"
                            ref="occ_community_name_lookup"
                            name="occ_community_name_lookup"
                            class="form-control"
                        />
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="occ_community_id_lookup"
                            >Community ID:</label
                        >
                        <select
                            id="occ_community_id_lookup"
                            ref="occ_community_id_lookup"
                            name="occ_community_id_lookup"
                            class="form-control"
                        />
                    </div>
                </div>
                <div class="col-md-3">
                    <div id="select_status" class="form-group">
                        <label for="occ_status_lookup">Status:</label>
                        <select
                            v-model="filterOCCCommunityStatus"
                            class="form-select"
                        >
                            <option value="all">All</option>
                            <option
                                v-for="status in proposal_status"
                                :value="status.value"
                                :key="status.value"
                            >
                                {{ status.name }}
                            </option>
                        </select>
                    </div>
                </div>
            </div>
            <div class="row">
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="">Region:</label>
                        <select
                            v-model="filterOCCCommunityRegion"
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
                            v-model="filterOCCCommunityDistrict"
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
                    <div id="select_last_modified_by" class="form-group">
                        <label for="occ_last_modified_by_lookup"
                            >Last Modified By:</label
                        >
                        <select
                            id="occ_last_modified_by_lookup"
                            ref="occ_last_modified_by_lookup"
                            name="occ_last_modified_by_lookup"
                            class="form-control"
                        />
                    </div>
                </div>
            </div>
            <div class="row">
                <div class="col-md-6">
                    <label for="" class="form-label px-2"
                        >Review Due Date Range:</label
                    >
                    <div class="input-group px-2 mb-2">
                        <span class="input-group-text">From </span>
                        <input
                            v-model="filterOCCFromCommunityDueDate"
                            type="date"
                            class="form-control"
                            placeholder="DD/MM/YYYY"
                        />
                        <span class="input-group-text"> to </span>
                        <input
                            v-model="filterOCCToCommunityDueDate"
                            type="date"
                            class="form-control"
                            placeholder="DD/MM/YYYY"
                        />
                    </div>
                </div>
                <div class="col-md-6">
                    <label for="" class="form-label px-2"
                        >Created Date Range:</label
                    >
                    <div class="input-group px-2 mb-2">
                        <span class="input-group-text">From </span>
                        <input
                            v-model="filterOCCCommunityCreatedFromDate"
                            type="date"
                            class="form-control"
                            placeholder="DD/MM/YYYY"
                        />
                        <span class="input-group-text"> to </span>
                        <input
                            v-model="filterOCCCommunityCreatedToDate"
                            type="date"
                            class="form-control"
                            placeholder="DD/MM/YYYY"
                        />
                    </div>
                </div>
            </div>
            <div class="row">
                <div class="col-md-6">
                    <label for="" class="form-label px-2"
                        >Activated Date Range:</label
                    >
                    <div class="input-group px-2 mb-2">
                        <span class="input-group-text">From </span>
                        <input
                            v-model="filterOCCCommunityActivatedFromDate"
                            type="date"
                            class="form-control"
                            placeholder="DD/MM/YYYY"
                        />
                        <span class="input-group-text"> to </span>
                        <input
                            v-model="filterOCCCommunityActivatedToDate"
                            type="date"
                            class="form-control"
                            placeholder="DD/MM/YYYY"
                        />
                    </div>
                </div>
                <div class="col-md-6">
                    <label for="" class="form-label px-2"
                        >Last Modified Date Range:</label
                    >
                    <div class="input-group px-2 mb-2">
                        <span class="input-group-text">From </span>
                        <input
                            v-model="filterOCCCommunityLastModifiedFromDate"
                            type="date"
                            class="form-control"
                            placeholder="DD/MM/YYYY"
                        />
                        <span class="input-group-text"> to </span>
                        <input
                            v-model="filterOCCCommunityLastModifiedToDate"
                            type="date"
                            class="form-control"
                            placeholder="DD/MM/YYYY"
                        />
                    </div>
                </div>
            </div>
        </CollapsibleFilters>

        <div v-if="show_add_button" class="col-md-12">
            <div class="text-end">
                <button
                    type="button"
                    class="btn btn-primary mb-2"
                    @click.prevent="createCommunityOccurrence"
                >
                    <i class="bi bi-plus-circle"></i> Add Community Occurrence
                </button>
            </div>
        </div>

        <div class="row">
            <div class="col-lg-12">
                <datatable
                    :id="datatable_id"
                    ref="community_occ_datatable"
                    :dt-options="datatable_options"
                    :dt-headers="datatable_headers"
                />
            </div>
            <div v-if="occurrenceHistoryId">
                <OccurrenceReportHistory
                    ref="occurrence_history"
                    :key="occurrenceHistoryId"
                    :occurrence-id="occurrenceHistoryId"
                />
            </div>
        </div>
    </div>
</template>
<script>
import { v4 as uuid } from 'uuid';
import datatable from '@/utils/vue/datatable.vue';
import CollapsibleFilters from '@/components/forms/collapsible_component.vue';
import OccurrenceReportHistory from '../internal/occurrence/community_occurrence_history.vue';

import { api_endpoints, constants, helpers } from '@/utils/hooks';
export default {
    name: 'OccurrenceReportCommunityTable',
    components: {
        datatable,
        CollapsibleFilters,
        OccurrenceReportHistory,
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
        filterOCCCommunityMigratedId_cache: {
            type: String,
            required: false,
            default: 'filterOCCCommunityMigratedId',
        },
        filterOCCCommunityOccurrenceName_cache: {
            type: String,
            required: false,
            default: 'filterOCCCommunityOccurrenceName',
        },
        filterOCCCommunityName_cache: {
            type: String,
            required: false,
            default: 'filterOCCCommunityName',
        },
        filterOCCCommunityStatus_cache: {
            type: String,
            required: false,
            default: 'filterOCCCommunityStatus',
        },
        filterOCCCommunityRegion_cache: {
            type: String,
            required: false,
            default: 'filterOCCCommunityRegion',
        },
        filterOCCCommunityDistrict_cache: {
            type: String,
            required: false,
            default: 'filterOCCCommunityDistrict',
        },
        filterOCCCommunityLastModifiedBy_cache: {
            type: String,
            required: false,
            default: 'filterOCCCommunityLastModifiedBy',
        },
        filterOCCFromCommunityDueDate_cache: {
            type: String,
            required: false,
            default: 'filterOCCFromCommunityDueDate',
        },
        filterOCCToCommunityDueDate_cache: {
            type: String,
            required: false,
            default: 'filterOCCToCommunityDueDate',
        },
        filterOCCCommunityCreatedFromDate_cache: {
            type: String,
            required: false,
            default: 'filterOCCCommunityCreatedFromDate',
        },
        filterOCCCommunityCreatedToDate_cache: {
            type: String,
            required: false,
            default: 'filterOCCCommunityCreatedToDate',
        },
        filterOCCCommunityActivatedFromDate_cache: {
            type: String,
            required: false,
            default: 'filterOCCCommunityActivatedFromDate',
        },
        filterOCCCommunityActivatedToDate_cache: {
            type: String,
            required: false,
            default: 'filterOCCCommunityActivatedToDate',
        },
        filterOCCCommunityLastModifiedFromDate_cache: {
            type: String,
            required: false,
            default: 'filterOCCCommunityLastModifiedFromDate',
        },
        filterOCCCommunityLastModifiedToDate_cache: {
            type: String,
            required: false,
            default: 'filterOCCCommunityLastModifiedToDate',
        },
    },
    data() {
        return {
            uuid: 0,
            occurrenceHistoryId: null,
            datatable_id: 'occurrence-community-datatable-' + uuid(),

            // selected values for filtering
            filterOCCCommunityMigratedId: sessionStorage.getItem(
                this.filterOCCCommunityMigratedId_cache
            )
                ? sessionStorage.getItem(
                      this.filterOCCCommunityMigratedId_cache
                  )
                : 'all',
            filterOCCCommunityOccurrenceName: sessionStorage.getItem(
                this.filterOCCCommunityOccurrenceName_cache
            )
                ? sessionStorage.getItem(
                      this.filterOCCCommunityOccurrenceName_cache
                  )
                : 'all',

            filterOCCCommunityName: sessionStorage.getItem(
                this.filterOCCCommunityName_cache
            )
                ? sessionStorage.getItem(this.filterOCCCommunityName_cache)
                : 'all',

            filterOCCCommunityStatus: sessionStorage.getItem(
                this.filterOCCCommunityStatus_cache
            )
                ? sessionStorage.getItem(this.filterOCCCommunityStatus_cache)
                : 'all',

            filterOCCCommunityRegion: sessionStorage.getItem(
                this.filterOCCCommunityRegion_cache
            )
                ? sessionStorage.getItem(this.filterOCCCommunityRegion_cache)
                : 'all',

            filterOCCCommunityDistrict: sessionStorage.getItem(
                this.filterOCCCommunityDistrict_cache
            )
                ? sessionStorage.getItem(this.filterOCCCommunityDistrict_cache)
                : 'all',

            filterOCCCommunityLastModifiedBy: sessionStorage.getItem(
                this.filterOCCCommunityLastModifiedBy_cache
            )
                ? sessionStorage.getItem(
                      this.filterOCCCommunityLastModifiedBy_cache
                  )
                : 'all',

            filterOCCFromCommunityDueDate: sessionStorage.getItem(
                this.filterOCCFromCommunityDueDate_cache
            )
                ? sessionStorage.getItem(
                      this.filterOCCFromCommunityDueDate_cache
                  )
                : '',
            filterOCCToCommunityDueDate: sessionStorage.getItem(
                this.filterOCCToCommunityDueDate_cache
            )
                ? sessionStorage.getItem(this.filterOCCToCommunityDueDate_cache)
                : '',

            filterOCCCommunityCreatedFromDate: sessionStorage.getItem(
                this.filterOCCCommunityCreatedFromDate_cache
            )
                ? sessionStorage.getItem(
                      this.filterOCCCommunityCreatedFromDate_cache
                  )
                : '',

            filterOCCCommunityCreatedToDate: sessionStorage.getItem(
                this.filterOCCCommunityCreatedToDate_cache
            )
                ? sessionStorage.getItem(
                      this.filterOCCCommunityCreatedToDate_cache
                  )
                : '',

            filterOCCCommunityActivatedFromDate: sessionStorage.getItem(
                this.filterOCCCommunityActivatedFromDate_cache
            )
                ? sessionStorage.getItem(
                      this.filterOCCCommunityActivatedFromDate_cache
                  )
                : '',

            filterOCCCommunityActivatedToDate: sessionStorage.getItem(
                this.filterOCCCommunityActivatedToDate_cache
            )
                ? sessionStorage.getItem(
                      this.filterOCCCommunityActivatedToDate_cache
                  )
                : '',

            filterOCCCommunityLastModifiedFromDate: sessionStorage.getItem(
                this.filterOCCCommunityLastModifiedFromDate_cache
            )
                ? sessionStorage.getItem(
                      this.filterOCCCommunityLastModifiedFromDate_cache
                  )
                : '',

            filterOCCCommunityLastModifiedToDate: sessionStorage.getItem(
                this.filterOCCCommunityLastModifiedToDate_cache
            )
                ? sessionStorage.getItem(
                      this.filterOCCCommunityLastModifiedToDate_cache
                  )
                : '',

            filterListsCommunity: {},
            filterRegionDistrict: {},
            occurrence_list: [],
            community_name_list: [],
            status_list: [],
            submissions_from_list: [],
            submissions_to_list: [],
            region_list: [],
            district_list: [],
            filtered_district_list: [],

            // filtering options
            internal_status: [
                { value: 'draft', name: 'Draft' },
                { value: 'discarded', name: 'Discarded' },
                { value: 'active', name: 'Active' },
                { value: 'historical', name: 'Historical' },
            ],

            proposal_status: [],
        };
    },
    computed: {
        show_add_button: function () {
            return (
                this.profile?.user &&
                this.profile.user.groups.includes(
                    constants.GROUPS.OCCURRENCE_APPROVERS
                )
            );
        },
        filterApplied: function () {
            if (
                this.filterOCCCommunityMigratedId === 'all' &&
                this.filterOCCCommunityOccurrenceName === 'all' &&
                this.filterOCCCommunityName === 'all' &&
                this.filterOCCCommunityStatus === 'all' &&
                this.filterOCCCommunityRegion === 'all' &&
                this.filterOCCCommunityDistrict === 'all' &&
                this.filterOCCCommunityLastModifiedBy === 'all' &&
                this.filterOCCFromCommunityDueDate === '' &&
                this.filterOCCToCommunityDueDate === '' &&
                this.filterOCCCommunityCreatedFromDate === '' &&
                this.filterOCCCommunityCreatedToDate === '' &&
                this.filterOCCCommunityActivatedFromDate === '' &&
                this.filterOCCCommunityActivatedToDate === '' &&
                this.filterOCCCommunityLastModifiedFromDate === '' &&
                this.filterOCCCommunityLastModifiedToDate === ''
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
        addCommunityOCCVisibility: function () {
            let visibility = false;
            if (this.is_internal) {
                visibility = true;
            }
            return visibility;
        },
        datatable_headers: function () {
            return [
                'ID',
                'Number',
                'Occurrence Name',
                'Community Name',
                'Wild Status',
                'Number of Reports',
                'Community ID',
                'Migrated From ID',
                'Region',
                'District',
                'Review Due',
                'Last Modified By',
                'Last Modified Date',
                'Activated Date',
                'Created Date',
                'Status',
                'Action',
            ];
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
                data: 'occurrence_number',
                orderable: true,
                searchable: true,
                visible: true,
                name: 'occurrence_number',
            };
        },
        column_occurrence_name: function () {
            return {
                data: 'occurrence_name',
                orderable: true,
                searchable: true,
                visible: true,
                name: 'occurrence_name',
                render: function (data, type, full) {
                    if (full.occurrence_name) {
                        let value = full.occurrence_name;
                        let result = helpers.dtPopover(value, 30, 'hover');
                        return type == 'export' ? value : result;
                    }
                    return '';
                },
            };
        },
        column_community_common_id: function () {
            return {
                data: 'community_common_id',
                orderable: true,
                searchable: true,
                visible: true,
                render: function (data, type, full) {
                    if (full.community_common_id) {
                        let value = full.community_common_id;
                        let result = helpers.dtPopover(value, 30, 'hover');
                        return type == 'export' ? value : result;
                    }
                    return '';
                },
                name: 'community__taxonomy__community_common_id',
            };
        },
        column_community_name: function () {
            return {
                data: 'community_name',
                orderable: true,
                searchable: true,
                visible: true,
                render: function (data, type, full) {
                    if (full.community_name) {
                        let value = full.community_name;
                        let result = helpers.dtPopover(value, 30, 'hover');
                        return type == 'export' ? value : result;
                    }
                    return '';
                },
                name: 'community__taxonomy__community_name',
            };
        },
        column_number_of_reports: function () {
            return {
                data: 'number_of_reports',
                orderable: true,
                searchable: false,
                visible: true,
            };
        },
        column_migrated_from_id: function () {
            return {
                data: 'migrated_from_id',
                orderable: false,
                searchable: true,
            };
        },
        column_region: function () {
            return {
                data: 'region',
                orderable: false,
                searchable: false,
                visible: true,
                name: 'location__region__name',
            };
        },
        column_district: function () {
            return {
                data: 'district',
                orderable: false,
                searchable: false,
                visible: true,
                name: 'location__district__name',
            };
        },
        column_wild_status: function () {
            return {
                data: 'wild_status',
                orderable: false,
                searchable: false,
                visible: true,
            };
        },
        column_review_due_date: function () {
            return {
                data: 'review_due_date',
                orderable: true,
                searchable: true,
                visible: true,
                name: 'review_due_date',
            };
        },
        column_last_modified_by: function () {
            return {
                data: 'last_modified_by_name',
                orderable: false,
                searchable: false,
                visible: true,
                name: 'last_modified_by_name',
            };
        },
        column_last_modified_date: function () {
            return {
                data: 'datetime_updated_display',
                orderable: true,
                searchable: false,
                visible: true,
                name: 'datetime_updated',
            };
        },
        column_activated_date: function () {
            return {
                data: 'lodgement_date',
                orderable: true,
                searchable: false,
                visible: true,
                name: 'lodgement_date',
            };
        },
        column_created_date: function () {
            return {
                data: 'datetime_created',
                orderable: true,
                searchable: false,
                visible: true,
                name: 'datetime_created',
            };
        },
        column_status: function () {
            return {
                data: 'processing_status_display',
                orderable: true,
                searchable: true,
                visible: true,
                name: 'processing_status',
                render: function (data, type, full) {
                    let html = full.processing_status_display;
                    if (!full.show_locked_indicator) {
                        return html;
                    }
                    if (full.locked) {
                        html +=
                            '<i class="bi bi-lock-fill ms-2 text-warning"></i>';
                    } else {
                        html +=
                            '<i class="bi bi-unlock-fill ms-2 text-secondary"></i>';
                    }
                    return html;
                },
            };
        },
        column_action: function () {
            let vm = this;
            return {
                data: 'id',
                orderable: false,
                searchable: false,
                visible: true,
                render: function (data, type, full) {
                    let links = '';
                    if (vm.is_internal) {
                        if (full.can_user_edit) {
                            if (full.processing_status == 'discarded') {
                                links += `<a href='#' data-reinstate-occ-proposal='${full.id}'>Reinstate</a><br/>`;
                                return links;
                            } else {
                                links += `<a href='/internal/occurrence/${full.id}?group_type_name=${vm.group_type_name}&action=edit'>Edit</a><br/>`;
                                if (full.processing_status == 'draft') {
                                    links += `<a href='#' data-discard-occ-proposal='${full.id}'>Discard</a><br/>`;
                                }
                            }
                        } else {
                            links += `<a href='/internal/occurrence/${full.id}?group_type_name=${vm.group_type_name}&action=view'>View</a><br/>`;
                        }
                        links += `<a href='#' data-history-occurrence='${full.id}'>History</a><br>`;
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
                    title: 'Boranga OCC Communities Excel Export',
                    text: '<i class="bi bi-download"></i> Excel',
                    className: 'btn btn-primary me-2 rounded',
                    exportOptions: {
                        columns: ':not(.no-export)',
                        orthogonal: 'export',
                    },
                },
                {
                    extend: 'csv',
                    title: 'Boranga OCC Communities CSV Export',
                    text: '<i class="bi bi-download"></i> CSV',
                    className: 'btn btn-primary rounded',
                    exportOptions: {
                        columns: ':not(.no-export)',
                        orthogonal: 'export',
                    },
                },
            ];
            if (vm.is_internal) {
                columns = [
                    vm.column_id,
                    vm.column_number,
                    vm.column_occurrence_name,
                    vm.column_community_name,
                    vm.column_wild_status,
                    vm.column_number_of_reports,
                    vm.column_community_common_id,
                    vm.column_migrated_from_id,
                    vm.column_region,
                    vm.column_district,
                    vm.column_review_due_date,
                    vm.column_last_modified_by,
                    vm.column_last_modified_date,
                    vm.column_activated_date,
                    vm.column_created_date,
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
                columnDefs: [
                    { responsivePriority: 1, targets: 1 },
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
                    method: 'post',
                    headers: {
                        'X-CSRFToken': helpers.getCookie('csrftoken'),
                    },
                    data: function (d) {
                        d.filter_group_type = vm.group_type_name;
                        d.filter_community_common_id =
                            vm.filterOCCCommunityMigratedId;
                        d.filter_occurrence_name =
                            vm.filterOCCCommunityOccurrenceName;
                        d.filter_community_name = vm.filterOCCCommunityName;
                        d.filter_status = vm.filterOCCCommunityStatus;
                        d.filter_from_due_date =
                            vm.filterOCCFromCommunityDueDate;
                        d.filter_to_due_date = vm.filterOCCToCommunityDueDate;
                        d.filter_region = vm.filterOCCCommunityRegion;
                        d.filter_district = vm.filterOCCCommunityDistrict;
                        d.filter_last_modified_by =
                            vm.filterOCCCommunityLastModifiedBy;
                        d.filter_created_from_date =
                            vm.filterOCCCommunityCreatedFromDate;
                        d.filter_created_to_date =
                            vm.filterOCCCommunityCreatedToDate;
                        d.filter_activated_from_date =
                            vm.filterOCCCommunityActivatedFromDate;
                        d.filter_activated_to_date =
                            vm.filterOCCCommunityActivatedToDate;
                        d.filter_last_modified_from_date =
                            vm.filterOCCCommunityLastModifiedFromDate;
                        d.filter_last_modified_to_date =
                            vm.filterOCCCommunityLastModifiedToDate;
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
        filterOCCCommunityMigratedId: function () {
            let vm = this;
            vm.$refs.community_occ_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            );
            sessionStorage.setItem(
                vm.filterOCCCommunityMigratedId_cache,
                vm.filterOCCCommunityMigratedId
            );
        },
        filterOCCCommunityOccurrenceName: function () {
            let vm = this;
            vm.$refs.community_occ_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            );
            sessionStorage.setItem(
                vm.filterOCCCommunityOccurrenceName_cache,
                vm.filterOCCCommunityOccurrenceName
            );
        },
        filterOCCCommunityName: function () {
            let vm = this;
            vm.$refs.community_occ_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            );
            sessionStorage.setItem(
                vm.filterOCCCommunityName_cache,
                vm.filterOCCCommunityName
            );
        },
        filterOCCCommunityStatus: function () {
            let vm = this;
            vm.$refs.community_occ_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            );
            sessionStorage.setItem(
                vm.filterOCCCommunityStatus_cache,
                vm.filterOCCCommunityStatus
            );
        },
        filterOCCCommunityRegion: function () {
            let vm = this;
            vm.$refs.community_occ_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            );
            sessionStorage.setItem(
                vm.filterOCCCommunityRegion_cache,
                vm.filterOCCCommunityRegion
            );
        },
        filterOCCCommunityDistrict: function () {
            let vm = this;
            vm.$refs.community_occ_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            );
            sessionStorage.setItem(
                vm.filterOCCCommunityDistrict_cache,
                vm.filterOCCCommunityDistrict
            );
        },
        filterOCCCommunityLastModifiedBy: function () {
            let vm = this;
            vm.$refs.community_occ_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            );
            sessionStorage.setItem(
                vm.filterOCCCommunityLastModifiedBy_cache,
                vm.filterOCCCommunityLastModifiedBy
            );
        },
        filterOCCFromCommunityDueDate: function () {
            let vm = this;
            vm.$refs.community_occ_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            );
            sessionStorage.setItem(
                vm.filterOCCFromCommunityDueDate_cache,
                vm.filterOCCFromCommunityDueDate
            );
        },
        filterOCCToCommunityDueDate: function () {
            let vm = this;
            vm.$refs.community_occ_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            );
            sessionStorage.setItem(
                vm.filterOCCToCommunityDueDate_cache,
                vm.filterOCCToCommunityDueDate
            );
        },
        filterOCCCommunityCreatedFromDate: function () {
            let vm = this;
            vm.$refs.community_occ_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            );
            sessionStorage.setItem(
                vm.filterOCCCommunityCreatedFromDate_cache,
                vm.filterOCCCommunityCreatedFromDate
            );
        },
        filterOCCCommunityCreatedToDate: function () {
            let vm = this;
            vm.$refs.community_occ_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            );
            sessionStorage.setItem(
                vm.filterOCCCommunityCreatedToDate_cache,
                vm.filterOCCCommunityCreatedToDate
            );
        },
        filterOCCCommunityActivatedFromDate: function () {
            let vm = this;
            vm.$refs.community_occ_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            );
            sessionStorage.setItem(
                vm.filterOCCCommunityActivatedFromDate_cache,
                vm.filterOCCCommunityActivatedFromDate
            );
        },
        filterOCCCommunityActivatedToDate: function () {
            let vm = this;
            vm.$refs.community_occ_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            );
            sessionStorage.setItem(
                vm.filterOCCCommunityActivatedToDate_cache,
                vm.filterOCCCommunityActivatedToDate
            );
        },
        filterOCCCommunityLastModifiedFromDate: function () {
            let vm = this;
            vm.$refs.community_occ_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            );
            sessionStorage.setItem(
                vm.filterOCCCommunityLastModifiedFromDate_cache,
                vm.filterOCCCommunityLastModifiedFromDate
            );
        },
        filterOCCCommunityLastModifiedToDate: function () {
            let vm = this;
            vm.$refs.community_occ_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            );
            sessionStorage.setItem(
                vm.filterOCCCommunityLastModifiedToDate_cache,
                vm.filterOCCCommunityLastModifiedToDate
            );
        },
    },
    mounted: function () {
        this.fetchFilterLists();
        this.fetchRegionDistricts();
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
            vm.initialiseOccurrenceNameLookup();
            vm.initialiseCommunityNameLookup();
            vm.initialiseCommunityIdLookup();
            vm.initialiseLastModifiedByLookup();
            vm.addEventListeners();
            var newOption;
            if (
                sessionStorage.getItem('filterOCCCommunityOccurrenceName') !=
                    'all' &&
                sessionStorage.getItem('filterOCCCommunityOccurrenceName') !=
                    null
            ) {
                newOption = new Option(
                    sessionStorage.getItem(
                        'filterOCCCommunityOccurrenceNameText'
                    ),
                    vm.filterOCCCommunityOccurrenceName,
                    false,
                    true
                );
                $('#occurrence_name_lookup').append(newOption);
            }
            if (
                sessionStorage.getItem('filterOCCCommunityName') != 'all' &&
                sessionStorage.getItem('filterOCCCommunityName') != null
            ) {
                newOption = new Option(
                    sessionStorage.getItem('filterOCCCommunityNameText'),
                    vm.filterOCCCommunityName,
                    false,
                    true
                );
                $('#occ_community_name_lookup').append(newOption);
            }
            if (
                sessionStorage.getItem('filterOCCCommunityMigratedId') !=
                    'all' &&
                sessionStorage.getItem('filterOCCCommunityMigratedId') != null
            ) {
                newOption = new Option(
                    sessionStorage.getItem('filterOCCCommunityMigratedIdText'),
                    vm.filterOCCCommunityMigratedId,
                    false,
                    true
                );
                $('#occ_community_id_lookup').append(newOption);
            }
            if (
                sessionStorage.getItem('filterOCCCommunityLastModifiedBy') !=
                    'all' &&
                sessionStorage.getItem('filterOCCCommunityLastModifiedBy') !=
                    null
            ) {
                newOption = new Option(
                    sessionStorage.getItem(
                        'filterOCCCommunityLastModifiedByText'
                    ),
                    vm.filterOCCCommunityLastModifiedBy,
                    false,
                    true
                );
                $('#occ_last_modified_by_lookup').append(newOption);
            }
        });
    },
    methods: {
        historyDocument: function (id) {
            this.occurrenceHistoryId = parseInt(id);
            this.uuid++;
            this.$nextTick(() => {
                this.$refs.occurrence_history.isModalOpen = true;
            });
        },
        initialiseOccurrenceNameLookup: function () {
            let vm = this;
            $(vm.$refs.occurrence_name_lookup)
                .select2({
                    minimumInputLength: 2,
                    dropdownParent: $('#occurrence_name_lookup_form_group_id'),
                    theme: 'bootstrap-5',
                    allowClear: true,
                    placeholder: 'Select Occurrence Name',
                    ajax: {
                        url: api_endpoints.occurrence_name_lookup,
                        dataType: 'json',
                        data: function (params) {
                            var query = {
                                term: params.term,
                                type: 'public',
                                group_type_id: vm.group_type_id,
                                active_only: false,
                            };
                            return query;
                        },
                    },
                })
                .on('select2:select', function (e) {
                    let data = e.params.data.text;
                    vm.filterOCCCommunityOccurrenceName = data;
                    sessionStorage.setItem(
                        'filterOCCCommunityOccurrenceNameText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.filterOCCCommunityOccurrenceName = 'all';
                    sessionStorage.setItem(
                        'filterOCCCommunityOccurrenceNameText',
                        ''
                    );
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-occurrence_name_lookup-results"]'
                    );
                    searchField[0].focus();
                });
        },
        initialiseCommunityNameLookup: function () {
            let vm = this;
            $(vm.$refs.occ_community_name_lookup)
                .select2({
                    minimumInputLength: 2,
                    dropdownParent: $('#select_occ_community_name'),
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
                    vm.filterOCCCommunityName = data;
                    sessionStorage.setItem(
                        'filterOCCCommunityNameText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.filterOCCCommunityName = 'all';
                    sessionStorage.setItem('filterOCCCommunityNameText', '');
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-occ_community_name_lookup-results"]'
                    );
                    searchField[0].focus();
                });
        },
        initialiseCommunityIdLookup: function () {
            let vm = this;
            $(vm.$refs.occ_community_id_lookup)
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
                    vm.filterOCCCommunityMigratedId = data;
                    sessionStorage.setItem(
                        'filterOCCCommunityMigratedIdText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.filterOCCCommunityMigratedId = 'all';
                    sessionStorage.setItem(
                        'filterOCCCommunityMigratedIdText',
                        ''
                    );
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-occ_community_id_lookup-results"]'
                    );
                    // move focus to select2 field
                    searchField[0].focus();
                });
        },
        fetchFilterLists: function () {
            let vm = this;
            //large FilterList of Community Values object
            fetch(
                api_endpoints.community_filter_dict +
                    '?group_type_name=' +
                    vm.group_type_name
            ).then(
                async (response) => {
                    vm.filterListsCommunity = await response.json();
                    vm.occurrence_list =
                        vm.filterListsCommunity.occurrence_list;
                    vm.community_name_list =
                        vm.filterListsCommunity.community_name_list;
                    vm.status_list = vm.filterListsCommunity.status_list;
                    vm.submissions_from_list =
                        vm.filterListsCommunity.submissions_from_list;
                    vm.submissions_to_list =
                        vm.filterListsCommunity.submissions_to_list;
                    vm.proposal_status = vm.internal_status
                        .slice()
                        .sort((a, b) => {
                            return a.name.trim().localeCompare(b.name.trim());
                        });
                },
                (error) => {
                    console.log(error);
                }
            );
        },
        fetchRegionDistricts: function () {
            let vm = this;
            fetch(api_endpoints.region_district_filter_dict).then(
                async (response) => {
                    vm.filterRegionDistrict = await response.json();
                    vm.region_list = vm.filterRegionDistrict.region_list;
                    vm.district_list = vm.filterRegionDistrict.district_list;
                    vm.filterDistrict();
                },
                (error) => {
                    console.log(error);
                }
            );
        },
        filterDistrict: function (event) {
            this.$nextTick(() => {
                if (event) {
                    this.filterOCCCommunityDistrict = 'all';
                }
                this.filtered_district_list = [];
                if (this.filterOCCCommunityRegion.toString() === 'all') {
                    this.filtered_district_list = this.district_list;
                } else {
                    for (let choice of this.district_list) {
                        if (
                            choice.region_id.toString() ===
                            this.filterOCCCommunityRegion.toString()
                        ) {
                            this.filtered_district_list.push(choice);
                        }
                    }
                }
            });
        },
        initialiseLastModifiedByLookup: function () {
            let vm = this;
            $(vm.$refs.occ_last_modified_by_lookup)
                .select2({
                    minimumInputLength: 2,
                    theme: 'bootstrap-5',
                    allowClear: true,
                    placeholder: 'Search for User',
                    ajax: {
                        url:
                            api_endpoints.users_api +
                            '/get_department_users_ledger_id/',
                        dataType: 'json',
                        data: function (params) {
                            var query = {
                                term: params.term,
                            };
                            return query;
                        },
                    },
                })
                .on('select2:select', function (e) {
                    let data = e.params.data.id;
                    vm.filterOCCCommunityLastModifiedBy = data;
                    sessionStorage.setItem(
                        'filterOCCCommunityLastModifiedByText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.filterOCCCommunityLastModifiedBy = 'all';
                    sessionStorage.setItem(
                        'filterOCCCommunityLastModifiedByText',
                        ''
                    );
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-occ_last_modified_by_lookup-results"]'
                    );
                    searchField[0].focus();
                });
        },
        createCommunityOccurrence: async function () {
            swal.fire({
                title: `Add ${this.group_type_name} Occurrence`,
                text: 'Are you sure you want to add a new community occurrence?',
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Add Occurrence',
                reverseButtons: true,
                customClass: {
                    confirmButton: 'btn btn-primary',
                    cancelButton: 'btn btn-secondary',
                },
            }).then(async (swalresult) => {
                if (swalresult.isConfirmed) {
                    let newCommunityOCCId = null;
                    try {
                        const createUrl = api_endpoints.occurrence;
                        let payload = new Object();
                        payload.group_type_id = this.group_type_id;
                        payload.internal_application = true;
                        let response = await fetch(createUrl, {
                            method: 'POST',
                            headers: {
                                'Content-Type': 'application/json',
                            },
                            body: JSON.stringify(payload),
                        });
                        const data = await response.json();
                        if (data) {
                            newCommunityOCCId = data.id;
                        }
                    } catch (err) {
                        console.log(err);
                        if (this.is_internal) {
                            return err;
                        }
                    }
                    this.$router.push({
                        name: 'internal-occurrence-detail',
                        params: { occurrence_id: newCommunityOCCId },
                    });
                }
            });
        },
        discardOCC: function (occurrence_id) {
            let vm = this;
            swal.fire({
                title: 'Discard Occurrence',
                text: 'Are you sure you want to discard this occurrence?',
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Discard Occurrence',
                customClass: {
                    confirmButton: 'btn btn-primary',
                    cancelButton: 'btn btn-secondary',
                },
                reverseButtons: true,
            }).then(
                (swalresult) => {
                    if (swalresult.isConfirmed) {
                        fetch(
                            api_endpoints.discard_occ_proposal(occurrence_id),
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
                                    text: 'The occurrence has been discarded',
                                    icon: 'success',
                                    customClass: {
                                        confirmButton: 'btn btn-primary',
                                    },
                                });
                                vm.$refs.community_occ_datatable.vmDataTable.ajax.reload(
                                    helpers.enablePopovers,
                                    true
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
        reinstateOCC: function (occurrence_id) {
            let vm = this;
            swal.fire({
                title: 'Reinstate Occurrence',
                text: 'Are you sure you want to reinstate this occurrence?',
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Reinstate Occurrence',
                customClass: {
                    confirmButton: 'btn btn-primary',
                    cancelButton: 'btn btn-secondary',
                },
                reverseButtons: true,
            }).then(
                (swalresult) => {
                    if (swalresult.isConfirmed) {
                        fetch(
                            api_endpoints.reinstate_occ_proposal(occurrence_id),
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
                                    text: 'Your report has been reinstated',
                                    icon: 'success',
                                    customClass: {
                                        confirmButton: 'btn btn-primary',
                                    },
                                });
                                vm.$refs.community_occ_datatable.vmDataTable.ajax.reload(
                                    helpers.enablePopovers,
                                    true
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
            // internal Discard listener
            vm.$refs.community_occ_datatable.vmDataTable.on(
                'click',
                'a[data-discard-occ-proposal]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-discard-occ-proposal');
                    vm.discardOCC(id);
                }
            );
            vm.$refs.community_occ_datatable.vmDataTable.on(
                'click',
                'a[data-reinstate-occ-proposal]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-reinstate-occ-proposal');
                    vm.reinstateOCC(id);
                }
            );
            vm.$refs.community_occ_datatable.vmDataTable.on(
                'click',
                'a[data-history-occurrence]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-history-occurrence');
                    vm.historyDocument(id);
                }
            );
            vm.$refs.community_occ_datatable.vmDataTable.on(
                'childRow.dt',
                function () {
                    helpers.enablePopovers();
                }
            );
        },
    },
};
</script>
<style scoped>
.dt-buttons {
    float: right;
}
</style>
