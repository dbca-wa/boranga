<template id="occurrence-flora-dashboard">
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
                        id="select_scientific_name_by_groupname_occ"
                        class="form-group"
                    >
                        <label for="occ_scientific_name_lookup_by_groupname"
                            >Scientific Name:</label
                        >
                        <select
                            id="occ_scientific_name_lookup_by_groupname"
                            ref="occ_scientific_name_lookup_by_groupname"
                            name="occ_scientific_name_lookup_by_groupname"
                            class="form-control"
                        />
                    </div>
                </div>
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
                    <div class="form-group">
                        <SelectFilter
                            id="region-filter"
                            title="Region:"
                            :options="region_list"
                            :multiple="true"
                            :pre-selected-filter-item="filterOCCFloraRegion"
                            placeholder="Select Regions"
                            label="text"
                            @input="
                                (val) => {
                                    onRegionFilterChange(val || []);
                                }
                            "
                        />
                    </div>
                </div>
                <div class="col-md-3">
                    <div id="select_status" class="form-group">
                        <label for="">Status:</label>
                        <select
                            v-model="filterOCCFloraStatus"
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
                        <SelectFilter
                            id="district-filter"
                            title="District:"
                            :options="filtered_district_list"
                            :multiple="true"
                            :pre-selected-filter-item="filterOCCFloraDistrict"
                            placeholder="Select Districts"
                            label="text"
                            @input="
                                (val) => {
                                    filterOCCFloraDistrict = val || [];
                                }
                            "
                        />
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
                            v-model="filterOCCFromFloraDueDate"
                            type="date"
                            class="form-control"
                            placeholder="DD/MM/YYYY"
                        />
                        <span class="input-group-text"> to </span>
                        <input
                            v-model="filterOCCToFloraDueDate"
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
                            v-model="filterOCCFloraCreatedFromDate"
                            type="date"
                            class="form-control"
                            placeholder="DD/MM/YYYY"
                        />
                        <span class="input-group-text"> to </span>
                        <input
                            v-model="filterOCCFloraCreatedToDate"
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
                            v-model="filterOCCFloraActivatedFromDate"
                            type="date"
                            class="form-control"
                            placeholder="DD/MM/YYYY"
                        />
                        <span class="input-group-text"> to </span>
                        <input
                            v-model="filterOCCFloraActivatedToDate"
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
                            v-model="filterOCCFloraLastModifiedFromDate"
                            type="date"
                            class="form-control"
                            placeholder="DD/MM/YYYY"
                        />
                        <span class="input-group-text"> to </span>
                        <input
                            v-model="filterOCCFloraLastModifiedToDate"
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
                    @click.prevent="createFloraOccurrence"
                >
                    <i class="bi bi-plus-circle"></i> Add Flora Occurrence
                </button>
            </div>
        </div>

        <div class="row">
            <div class="col-lg-12">
                <datatable
                    :id="datatable_id"
                    ref="flora_occ_datatable"
                    :dt-options="datatable_options"
                    :dt-headers="datatable_headers"
                />
            </div>
        </div>
        <div v-if="occurrenceHistoryId">
            <OccurrenceHistory
                ref="occurrence_history"
                :key="occurrenceHistoryId"
                :occurrence-id="occurrenceHistoryId"
            />
        </div>
    </div>
</template>
<script>
import { v4 as uuid } from 'uuid';
import datatable from '@/utils/vue/datatable.vue';
import CollapsibleFilters from '@/components/forms/collapsible_component.vue';
import SelectFilter from '@/components/common/SelectFilter.vue';
import OccurrenceHistory from '../internal/occurrence/species_occurrence_history.vue';

import { api_endpoints, constants, helpers } from '@/utils/hooks';
export default {
    name: 'OccurrenceFloraDashboard',
    components: {
        datatable,
        CollapsibleFilters,
        SelectFilter,
        OccurrenceHistory,
    },
    props: {
        level: {
            type: String,
            required: true,
            validator: function (val) {
                let options = ['internal', 'external'];
                return options.indexOf(val) != -1 ? true : false;
            },
        },
        group_type_name: {
            type: String,
            required: true,
        },
        group_type_id: {
            type: Number,
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
        // for adding agendaitems for the meeting_obj.id
        meeting_obj: {
            type: Object,
            required: false,
        },
        filterOCCFloraOccurrenceName_cache: {
            type: String,
            required: false,
            default: 'filterOCCFloraOccurrenceName',
        },
        filterOCCFloraScientificName_cache: {
            type: String,
            required: false,
            default: 'filterOCCFloraScientificName',
        },
        filterOCCFloraStatus_cache: {
            type: String,
            required: false,
            default: 'filterOCCFloraStatus',
        },
        filterOCCFromFloraDueDate_cache: {
            type: String,
            required: false,
            default: 'filterOCCFromFloraDueDate',
        },
        filterOCCToFloraDueDate_cache: {
            type: String,
            required: false,
            default: 'filterOCCToFloraDueDate',
        },
        filterOCCFloraRegion_cache: {
            type: String,
            required: false,
            default: 'filterOCCFloraRegion',
        },
        filterOCCFloraDistrict_cache: {
            type: String,
            required: false,
            default: 'filterOCCFloraDistrict',
        },
        filterOCCFloraLastModifiedBy_cache: {
            type: String,
            required: false,
            default: 'filterOCCFloraLastModifiedBy',
        },
        filterOCCFloraCreatedFromDate_cache: {
            type: String,
            required: false,
            default: 'filterOCCFloraCreatedFromDate',
        },
        filterOCCFloraCreatedToDate_cache: {
            type: String,
            required: false,
            default: 'filterOCCFloraCreatedToDate',
        },
        filterOCCFloraActivatedFromDate_cache: {
            type: String,
            required: false,
            default: 'filterOCCFloraActivatedFromDate',
        },
        filterOCCFloraActivatedToDate_cache: {
            type: String,
            required: false,
            default: 'filterOCCFloraActivatedToDate',
        },
        filterOCCFloraLastModifiedFromDate_cache: {
            type: String,
            required: false,
            default: 'filterOCCFloraLastModifiedFromDate',
        },
        filterOCCFloraLastModifiedToDate_cache: {
            type: String,
            required: false,
            default: 'filterOCCFloraLastModifiedToDate',
        },
    },
    data() {
        return {
            uuid: 0,
            occurrenceHistoryId: null,
            datatable_id: 'occurrence-flora-datatable-' + uuid(),

            // selected values for filtering
            filterOCCFloraOccurrenceName: sessionStorage.getItem(
                this.filterOCCFloraOccurrenceName_cache
            )
                ? sessionStorage.getItem(
                      this.filterOCCFloraOccurrenceName_cache
                  )
                : 'all',

            filterOCCFloraScientificName: sessionStorage.getItem(
                this.filterOCCFloraScientificName_cache
            )
                ? sessionStorage.getItem(
                      this.filterOCCFloraScientificName_cache
                  )
                : 'all',

            filterOCCFloraStatus: sessionStorage.getItem(
                this.filterOCCFloraStatus_cache
            )
                ? sessionStorage.getItem(this.filterOCCFloraStatus_cache)
                : 'all',

            filterOCCFloraRegion: (() => {
                const raw = sessionStorage.getItem(
                    this.filterOCCFloraRegion_cache
                );
                if (!raw || raw === 'all') return [];
                try {
                    return JSON.parse(raw);
                } catch {
                    return [];
                }
            })(),

            filterOCCFloraDistrict: (() => {
                const raw = sessionStorage.getItem(
                    this.filterOCCFloraDistrict_cache
                );
                if (!raw || raw === 'all') return [];
                try {
                    return JSON.parse(raw);
                } catch {
                    return [];
                }
            })(),

            filterOCCFloraLastModifiedBy: sessionStorage.getItem(
                this.filterOCCFloraLastModifiedBy_cache
            )
                ? sessionStorage.getItem(
                      this.filterOCCFloraLastModifiedBy_cache
                  )
                : 'all',

            filterOCCFromFloraDueDate: sessionStorage.getItem(
                this.filterOCCFromFloraDueDate_cache
            )
                ? sessionStorage.getItem(this.filterOCCFromFloraDueDate_cache)
                : '',
            filterOCCToFloraDueDate: sessionStorage.getItem(
                this.filterOCCToFloraDueDate_cache
            )
                ? sessionStorage.getItem(this.filterOCCToFloraDueDate_cache)
                : '',

            filterOCCFloraCreatedFromDate: sessionStorage.getItem(
                this.filterOCCFloraCreatedFromDate_cache
            )
                ? sessionStorage.getItem(
                      this.filterOCCFloraCreatedFromDate_cache
                  )
                : '',
            filterOCCFloraCreatedToDate: sessionStorage.getItem(
                this.filterOCCFloraCreatedToDate_cache
            )
                ? sessionStorage.getItem(this.filterOCCFloraCreatedToDate_cache)
                : '',

            filterOCCFloraActivatedFromDate: sessionStorage.getItem(
                this.filterOCCFloraActivatedFromDate_cache
            )
                ? sessionStorage.getItem(
                      this.filterOCCFloraActivatedFromDate_cache
                  )
                : '',
            filterOCCFloraActivatedToDate: sessionStorage.getItem(
                this.filterOCCFloraActivatedToDate_cache
            )
                ? sessionStorage.getItem(
                      this.filterOCCFloraActivatedToDate_cache
                  )
                : '',

            filterOCCFloraLastModifiedFromDate: sessionStorage.getItem(
                this.filterOCCFloraLastModifiedFromDate_cache
            )
                ? sessionStorage.getItem(
                      this.filterOCCFloraLastModifiedFromDate_cache
                  )
                : '',
            filterOCCFloraLastModifiedToDate: sessionStorage.getItem(
                this.filterOCCFloraLastModifiedToDate_cache
            )
                ? sessionStorage.getItem(
                      this.filterOCCFloraLastModifiedToDate_cache
                  )
                : '',

            filterListsSpecies: {},
            filterRegionDistrict: {},
            occurrence_list: [],
            scientific_name_list: [],
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
                this.filterOCCFloraOccurrenceName === 'all' &&
                this.filterOCCFloraScientificName === 'all' &&
                this.filterOCCFloraStatus === 'all' &&
                this.filterOCCFloraRegion.length === 0 &&
                this.filterOCCFloraDistrict.length === 0 &&
                this.filterOCCFloraLastModifiedBy === 'all' &&
                this.filterOCCFromFloraDueDate === '' &&
                this.filterOCCToFloraDueDate === '' &&
                this.filterOCCFloraCreatedFromDate === '' &&
                this.filterOCCFloraCreatedToDate === '' &&
                this.filterOCCFloraActivatedFromDate === '' &&
                this.filterOCCFloraActivatedToDate === '' &&
                this.filterOCCFloraLastModifiedFromDate === '' &&
                this.filterOCCFloraLastModifiedToDate === ''
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
        addFloraOCCVisibility: function () {
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
                'Scientific Name',
                'Wild Status',
                'Number of Reports',
                'Migrated From ID',
                'Region',
                'District',
                'Review Due',
                'Last Modified By',
                'Last Modified Date',
                'Activated Date',
                'Created Date',
                'Family',
                'Informal Group(s)',
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
        column_name: function () {
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
        column_scientific_name: function () {
            return {
                data: 'scientific_name',
                orderable: true,
                searchable: true,
                visible: true,
                render: function (data, type, full) {
                    if (full.scientific_name) {
                        let value = full.scientific_name;
                        let result = helpers.dtPopover(value, 30, 'hover');
                        return type == 'export' ? value : result;
                    }
                    return '';
                },
                name: 'species__taxonomy__scientific_name',
            };
        },
        column_wild_status: function () {
            return {
                data: 'wild_status',
                orderable: true,
                searchable: true,
                visible: true,
                name: 'wild_status__name',
            };
        },
        column_number_of_reports: function () {
            return {
                data: 'number_of_reports',
                orderable: false,
                searchable: false,
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
                orderable: true,
                searchable: true,
                visible: true,
                name: 'location__region__name',
            };
        },
        column_district: function () {
            return {
                data: 'district',
                orderable: true,
                searchable: true,
                visible: true,
                name: 'location__district__name',
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
        column_family: function () {
            return {
                data: 'family',
                orderable: false,
                searchable: false,
                visible: true,
            };
        },
        column_informal_groups: function () {
            return {
                data: 'informal_groups',
                orderable: false,
                searchable: false,
                visible: true,
                render: function (data, type, full) {
                    if (full.informal_groups) {
                        let value = full.informal_groups;
                        let result = helpers.dtPopover(value, 30, 'hover');
                        return type == 'export' ? value : result;
                    }
                    return '';
                },
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
                    title: 'Boranga OCC Flora Excel Export',
                    text: '<i class="bi bi-download"></i> Excel',
                    className: 'btn btn-primary me-2 rounded',
                    exportOptions: {
                        columns: ':not(.no-export)',
                        orthogonal: 'export',
                    },
                },
                {
                    extend: 'csv',
                    title: 'Boranga OCC Flora CSV Export',
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
                    vm.column_name,
                    vm.column_scientific_name,
                    vm.column_wild_status,
                    vm.column_number_of_reports,
                    vm.column_migrated_from_id,
                    vm.column_region,
                    vm.column_district,
                    vm.column_review_due_date,
                    vm.column_last_modified_by,
                    vm.column_last_modified_date,
                    vm.column_activated_date,
                    vm.column_created_date,
                    vm.column_family,
                    vm.column_informal_groups,
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
                //  to show the "workflow Status","Action" columns always in the last position
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
                        d.filter_occurrence_name =
                            vm.filterOCCFloraOccurrenceName;
                        d.filter_scientific_name =
                            vm.filterOCCFloraScientificName;
                        d.filter_status = vm.filterOCCFloraStatus;
                        d.filter_from_due_date = vm.filterOCCFromFloraDueDate;
                        d.filter_to_due_date = vm.filterOCCToFloraDueDate;
                        d.filter_region =
                            vm.filterOCCFloraRegion.length > 0
                                ? vm.filterOCCFloraRegion.join(',')
                                : 'all';
                        d.filter_district =
                            vm.filterOCCFloraDistrict.length > 0
                                ? vm.filterOCCFloraDistrict.join(',')
                                : 'all';
                        d.filter_last_modified_by =
                            vm.filterOCCFloraLastModifiedBy;
                        d.filter_created_from_date =
                            vm.filterOCCFloraCreatedFromDate;
                        d.filter_created_to_date =
                            vm.filterOCCFloraCreatedToDate;
                        d.filter_activated_from_date =
                            vm.filterOCCFloraActivatedFromDate;
                        d.filter_activated_to_date =
                            vm.filterOCCFloraActivatedToDate;
                        d.filter_last_modified_from_date =
                            vm.filterOCCFloraLastModifiedFromDate;
                        d.filter_last_modified_to_date =
                            vm.filterOCCFloraLastModifiedToDate;
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
        filterOCCFloraOccurrenceName: function () {
            let vm = this;
            vm.$refs.flora_occ_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            );
            sessionStorage.setItem(
                vm.filterOCCFloraOccurrenceName_cache,
                vm.filterOCCFloraOccurrenceName
            );
        },
        filterOCCFloraScientificName: function () {
            let vm = this;
            vm.$refs.flora_occ_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            );
            sessionStorage.setItem(
                vm.filterOCCFloraScientificName_cache,
                vm.filterOCCFloraScientificName
            );
        },
        filterOCCFloraStatus: function () {
            let vm = this;
            vm.$refs.flora_occ_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            );
            sessionStorage.setItem(
                vm.filterOCCFloraStatus_cache,
                vm.filterOCCFloraStatus
            );
        },
        filterOCCFloraRegion: {
            handler: function () {
                let vm = this;
                vm.$refs.flora_occ_datatable.vmDataTable.ajax.reload(
                    helpers.enablePopovers,
                    true
                );
                sessionStorage.setItem(
                    vm.filterOCCFloraRegion_cache,
                    JSON.stringify(vm.filterOCCFloraRegion)
                );
            },
            deep: true,
        },
        filterOCCFloraDistrict: {
            handler: function () {
                let vm = this;
                vm.$refs.flora_occ_datatable.vmDataTable.ajax.reload(
                    helpers.enablePopovers,
                    true
                );
                sessionStorage.setItem(
                    vm.filterOCCFloraDistrict_cache,
                    JSON.stringify(vm.filterOCCFloraDistrict)
                );
            },
            deep: true,
        },
        filterOCCFloraLastModifiedBy: function () {
            let vm = this;
            vm.$refs.flora_occ_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            );
            sessionStorage.setItem(
                vm.filterOCCFloraLastModifiedBy_cache,
                vm.filterOCCFloraLastModifiedBy
            );
        },
        filterOCCFromFloraDueDate: function () {
            let vm = this;
            vm.$refs.flora_occ_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            );
            sessionStorage.setItem(
                vm.filterOCCFromFloraDueDate_cache,
                vm.filterOCCFromFloraDueDate
            );
        },
        filterOCCToFloraDueDate: function () {
            let vm = this;
            vm.$refs.flora_occ_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            );
            sessionStorage.setItem(
                vm.filterOCCToFloraDueDate_cache,
                vm.filterOCCToFloraDueDate
            );
        },
        filterOCCFloraCreatedFromDate: function () {
            let vm = this;
            vm.$refs.flora_occ_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            );
            sessionStorage.setItem(
                vm.filterOCCFloraCreatedFromDate_cache,
                vm.filterOCCFloraCreatedFromDate
            );
        },
        filterOCCFloraCreatedToDate: function () {
            let vm = this;
            vm.$refs.flora_occ_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            );
            sessionStorage.setItem(
                vm.filterOCCFloraCreatedToDate_cache,
                vm.filterOCCFloraCreatedToDate
            );
        },
        filterOCCFloraActivatedFromDate: function () {
            let vm = this;
            vm.$refs.flora_occ_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            );
            sessionStorage.setItem(
                vm.filterOCCFloraActivatedFromDate_cache,
                vm.filterOCCFloraActivatedFromDate
            );
        },
        filterOCCFloraActivatedToDate: function () {
            let vm = this;
            vm.$refs.flora_occ_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            );
            sessionStorage.setItem(
                vm.filterOCCFloraActivatedToDate_cache,
                vm.filterOCCFloraActivatedToDate
            );
        },
        filterOCCFloraLastModifiedFromDate: function () {
            let vm = this;
            vm.$refs.flora_occ_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            );
            sessionStorage.setItem(
                vm.filterOCCFloraLastModifiedFromDate_cache,
                vm.filterOCCFloraLastModifiedFromDate
            );
        },
        filterOCCFloraLastModifiedToDate: function () {
            let vm = this;
            vm.$refs.flora_occ_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            );
            sessionStorage.setItem(
                vm.filterOCCFloraLastModifiedToDate_cache,
                vm.filterOCCFloraLastModifiedToDate
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
            vm.initialiseScientificNameLookup();
            vm.initialiseLastModifiedByLookup();
            vm.addEventListeners();
            var newOption;
            if (
                sessionStorage.getItem('filterOCCFloraOccurrenceName') !=
                    'all' &&
                sessionStorage.getItem('filterOCCFloraOccurrenceName') != null
            ) {
                newOption = new Option(
                    sessionStorage.getItem('filterOCCFloraOccurrenceNameText'),
                    vm.filterOCCFloraOccurrenceName,
                    false,
                    true
                );
                $('#occurrence_name_lookup').append(newOption);
            }
            if (
                sessionStorage.getItem('filterOCCFloraScientificName') !=
                    'all' &&
                sessionStorage.getItem('filterOCCFloraScientificName') != null
            ) {
                newOption = new Option(
                    sessionStorage.getItem('filterOCCFloraScientificNameText'),
                    vm.filterOCCFloraScientificName,
                    false,
                    true
                );
                $('#occ_scientific_name_lookup_by_groupname').append(newOption);
            }
            if (
                sessionStorage.getItem('filterOCCFloraLastModifiedBy') !=
                    'all' &&
                sessionStorage.getItem('filterOCCFloraLastModifiedBy') != null
            ) {
                newOption = new Option(
                    sessionStorage.getItem('filterOCCFloraLastModifiedByText'),
                    vm.filterOCCFloraLastModifiedBy,
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
                    let data = e.params.data.id;
                    vm.filterOCCFloraOccurrenceName = data;
                    sessionStorage.setItem(
                        'filterOCCFloraOccurrenceNameText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.filterOCCFloraOccurrenceName = 'all';
                    sessionStorage.setItem(
                        'filterOCCFloraOccurrenceNameText',
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
        initialiseScientificNameLookup: function () {
            let vm = this;
            $(vm.$refs.occ_scientific_name_lookup_by_groupname)
                .select2({
                    minimumInputLength: 2,
                    dropdownParent: $(
                        '#select_scientific_name_by_groupname_occ'
                    ),
                    theme: 'bootstrap-5',
                    allowClear: true,
                    placeholder: 'Select Scientific Name',
                    ajax: {
                        url: api_endpoints.scientific_name_lookup,
                        dataType: 'json',
                        data: function (params) {
                            var query = {
                                term: params.term,
                                type: 'public',
                                group_type_id: vm.group_type_id,
                            };
                            return query;
                        },
                    },
                })
                .on('select2:select', function (e) {
                    let data = e.params.data.id;
                    vm.filterOCCFloraScientificName = data;
                    sessionStorage.setItem(
                        'filterOCCFloraScientificNameText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.filterOCCFloraScientificName = 'all';
                    sessionStorage.setItem(
                        'filterOCCFloraScientificNameText',
                        ''
                    );
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-occ_scientific_name_lookup_by_groupname-results"]'
                    );
                    searchField[0].focus();
                });
        },
        fetchFilterLists: function () {
            let vm = this;
            //large FilterList of Species Values object
            fetch(
                api_endpoints.filter_lists_species +
                    '?group_type_name=' +
                    vm.group_type_name
            ).then(
                async (response) => {
                    vm.filterListsSpecies = await response.json();
                    vm.occurrence_list = vm.filterListsSpecies.occurrence_list;
                    vm.scientific_name_list =
                        vm.filterListsSpecies.scientific_name_list;
                    vm.status_list = vm.filterListsSpecies.status_list;
                    vm.submissions_from_list =
                        vm.filterListsSpecies.submissions_from_list;
                    vm.submissions_to_list =
                        vm.filterListsSpecies.submissions_to_list;
                    // vm.filterConservationCategory();
                    // vm.filterDistrict();
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
        filterDistrict: function () {
            this.$nextTick(() => {
                this.filtered_district_list = [];
                if (this.filterOCCFloraRegion.length === 0) {
                    this.filtered_district_list = this.district_list;
                } else {
                    for (let choice of this.district_list) {
                        if (
                            this.filterOCCFloraRegion.includes(choice.region_id)
                        ) {
                            this.filtered_district_list.push(choice);
                        }
                    }
                }
                // Remove selected districts that are no longer in filtered list
                if (this.filterOCCFloraDistrict.length > 0) {
                    const validIds = this.filtered_district_list.map(
                        (d) => d.id
                    );
                    this.filterOCCFloraDistrict =
                        this.filterOCCFloraDistrict.filter((id) =>
                            validIds.includes(id)
                        );
                }
            });
        },
        onRegionFilterChange: function (val) {
            this.filterOCCFloraRegion = val;
            this.filterDistrict();
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
                    vm.filterOCCFloraLastModifiedBy = data;
                    sessionStorage.setItem(
                        'filterOCCFloraLastModifiedByText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.filterOCCFloraLastModifiedBy = 'all';
                    sessionStorage.setItem(
                        'filterOCCFloraLastModifiedByText',
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
        createFloraOccurrence: async function () {
            swal.fire({
                title: `Add ${this.group_type_name} Occurrence`,
                text: 'Are you sure you want to add a new flora occurrence?',
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
                    let newFloraOCRId = null;
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
                            newFloraOCRId = data.id;
                        }
                    } catch (err) {
                        console.log(err);
                        if (this.is_internal) {
                            return err;
                        }
                    }
                    this.$router.push({
                        name: 'internal-occurrence-detail',
                        params: { occurrence_id: newFloraOCRId },
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
                confirmButtonText: 'Discard Report',
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
                                vm.$refs.flora_occ_datatable.vmDataTable.ajax.reload(
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
                confirmButtonText: 'Reinstate Report',
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
                                    text: 'The occurrence has been reinstated',
                                    icon: 'success',
                                    customClass: {
                                        confirmButton: 'btn btn-primary',
                                    },
                                });
                                vm.$refs.flora_occ_datatable.vmDataTable.ajax.reload(
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
            vm.$refs.flora_occ_datatable.vmDataTable.on(
                'click',
                'a[data-discard-occ-proposal]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-discard-occ-proposal');
                    vm.discardOCC(id);
                }
            );
            vm.$refs.flora_occ_datatable.vmDataTable.on(
                'click',
                'a[data-reinstate-occ-proposal]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-reinstate-occ-proposal');
                    vm.reinstateOCC(id);
                }
            );
            vm.$refs.flora_occ_datatable.vmDataTable.on(
                'click',
                'a[data-history-occurrence]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-history-occurrence');
                    vm.historyDocument(id);
                }
            );
            vm.$refs.flora_occ_datatable.vmDataTable.on(
                'childRow.dt',
                function () {
                    helpers.enablePopovers();
                }
            );
        },
    },
};
</script>
