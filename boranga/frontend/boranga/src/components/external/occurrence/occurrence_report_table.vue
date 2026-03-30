<template id="external_occurrence_report_datatable">
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
                        <label for="">Type:</label>
                        <select
                            v-model="filterOCRGroupType"
                            class="form-select"
                        >
                            <option value="all">All</option>
                            <option
                                v-for="option in group_types"
                                :value="option.name"
                                :key="option.id"
                            >
                                {{ option.display }}
                            </option>
                        </select>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="ocr_scientific_name_lookup"
                            >Scientific Name:</label
                        >
                        <select
                            id="ocr_scientific_name_lookup"
                            ref="ocr_scientific_name_lookup"
                            name="ocr_scientific_name_lookup"
                            class="form-control"
                        />
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="ocr_community_name_lookup"
                            >Community Name:</label
                        >
                        <select
                            id="ocr_community_name_lookup"
                            ref="ocr_community_name_lookup"
                            name="ocr_community_name_lookup"
                            class="form-control"
                        />
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="">Status:</label>
                        <select
                            v-model="filterOCRApplicationStatus"
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
                    <div id="ext_select_common_name" class="form-group">
                        <label for="ext_ocr_common_name_lookup"
                            >Common Name:</label
                        >
                        <select
                            id="ext_ocr_common_name_lookup"
                            ref="ext_ocr_common_name_lookup"
                            name="ext_ocr_common_name_lookup"
                            class="form-control"
                        />
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="ext_ocr_community_id_lookup"
                            >Community ID:</label
                        >
                        <select
                            id="ext_ocr_community_id_lookup"
                            ref="ext_ocr_community_id_lookup"
                            name="ext_ocr_community_id_lookup"
                            class="form-control"
                        />
                    </div>
                </div>
                <div class="col-md-3">
                    <div id="ext_select_occurrence_name" class="form-group">
                        <label for="ext_ocr_occurrence_name_lookup"
                            >Occurrence Name:</label
                        >
                        <select
                            id="ext_ocr_occurrence_name_lookup"
                            ref="ext_ocr_occurrence_name_lookup"
                            name="ext_ocr_occurrence_name_lookup"
                            class="form-control"
                        />
                    </div>
                </div>
                <div class="col-md-3">
                    <div id="ext_select_occurrence" class="form-group">
                        <label for="ext_ocr_occurrence_lookup"
                            >Occurrence Number:</label
                        >
                        <select
                            id="ext_ocr_occurrence_lookup"
                            ref="ext_ocr_occurrence_lookup"
                            name="ext_ocr_occurrence_lookup"
                            class="form-control"
                        />
                    </div>
                </div>
            </div>
            <div class="row">
                <div class="col-md-3">
                    <div class="form-group">
                        <SelectFilter
                            id="ext-region-filter"
                            title="Region:"
                            :options="region_list"
                            :multiple="true"
                            :pre-selected-filter-item="filterOCRRegion"
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
                    <div class="form-group">
                        <SelectFilter
                            id="ext-district-filter"
                            title="District:"
                            :options="filtered_district_list"
                            :multiple="true"
                            :pre-selected-filter-item="filterOCRDistrict"
                            placeholder="Select Districts"
                            label="text"
                            @input="
                                (val) => {
                                    filterOCRDistrict = val || [];
                                }
                            "
                        />
                    </div>
                </div>
            </div>
            <div class="row">
                <div class="col-md-6">
                    <label for="" class="form-label px-2"
                        >Observation Date Range:</label
                    >
                    <div class="input-group px-2 mb-2">
                        <span class="input-group-text">From </span>
                        <input
                            v-model="filterOCRObservationFromDate"
                            type="date"
                            class="form-control"
                            placeholder="DD/MM/YYYY"
                        />
                        <span class="input-group-text"> to </span>
                        <input
                            v-model="filterOCRObservationToDate"
                            type="date"
                            class="form-control"
                            placeholder="DD/MM/YYYY"
                        />
                    </div>
                </div>
                <div class="col-md-6">
                    <label for="" class="form-label px-2"
                        >Submitted Date Range:</label
                    >
                    <div class="input-group px-2 mb-2">
                        <span class="input-group-text">From </span>
                        <input
                            v-model="filterOCRSubmittedFromDate"
                            type="date"
                            class="form-control"
                            placeholder="DD/MM/YYYY"
                        />
                        <span class="input-group-text"> to </span>
                        <input
                            v-model="filterOCRSubmittedToDate"
                            type="date"
                            class="form-control"
                            placeholder="DD/MM/YYYY"
                        />
                    </div>
                </div>
            </div>
        </CollapsibleFilters>
        <div v-if="addOCRVisibility" class="col-md-12 dropdown">
            <div class="text-end">
                <button
                    id="ocr_type"
                    class="btn btn-primary dropdown-toggle mb-2"
                    type="button"
                    data-bs-toggle="dropdown"
                    aria-expanded="false"
                >
                    Report Occurrence
                </button>
                <ul class="dropdown-menu" aria-labelledby="ocr_type">
                    <li v-for="group in group_types" :key="group.id">
                        <a
                            class="dropdown-item"
                            role="button"
                            @click.prevent="
                                createOccurrenceReport(group.id, group.display)
                            "
                            >{{ group.display }}
                        </a>
                    </li>
                </ul>
            </div>
        </div>
        <div class="row">
            <div class="col-lg-12">
                <datatable
                    :id="datatable_id"
                    ref="occurrence_report_datatable"
                    :dt-options="datatable_options"
                    :dt-headers="datatable_headers"
                />
            </div>
        </div>
    </div>
</template>
<script>
import { v4 as uuid } from 'uuid';
import datatable from '@/utils/vue/datatable.vue';
import CollapsibleFilters from '@/components/forms/collapsible_component.vue';
import SelectFilter from '@/components/common/SelectFilter.vue';

import { constants, api_endpoints, helpers } from '@/utils/hooks';
export default {
    name: 'ExternalOccurrenceReportDatatable',
    components: {
        datatable,
        CollapsibleFilters,
        SelectFilter,
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
        url: {
            type: String,
            required: true,
        },
        profile: {
            type: Object,
            required: false,
            default: function () {
                return null;
            },
        },
        filterOCRGroupType_cache: {
            type: String,
            required: false,
            default: 'filterOCRGroupType',
        },
        filterOCRScientificName_cache: {
            type: String,
            required: false,
            default: 'filterOCRScientificName',
        },
        filterOCRExCommunityName_cache: {
            type: String,
            required: false,
            default: 'filterOCRExCommunityName',
        },
        filterOCRApplicationStatus_cache: {
            type: String,
            required: false,
            default: 'filterOCRApplicationStatus',
        },
        filterOCRCommonName_cache: {
            type: String,
            required: false,
            default: 'filterOCRCommonName',
        },
        filterOCRCommunityId_cache: {
            type: String,
            required: false,
            default: 'filterOCRCommunityId',
        },
        filterOCROccurrenceName_cache: {
            type: String,
            required: false,
            default: 'filterOCROccurrenceName',
        },
        filterOCROccurrence_cache: {
            type: String,
            required: false,
            default: 'filterOCROccurrence',
        },
        filterOCRRegion_cache: {
            type: String,
            required: false,
            default: 'filterOCRRegion',
        },
        filterOCRDistrict_cache: {
            type: String,
            required: false,
            default: 'filterOCRDistrict',
        },
        filterOCRObservationFromDate_cache: {
            type: String,
            required: false,
            default: 'filterOCRObservationFromDate',
        },
        filterOCRObservationToDate_cache: {
            type: String,
            required: false,
            default: 'filterOCRObservationToDate',
        },
        filterOCRSubmittedFromDate_cache: {
            type: String,
            required: false,
            default: 'filterOCRSubmittedFromDate',
        },
        filterOCRSubmittedToDate_cache: {
            type: String,
            required: false,
            default: 'filterOCRSubmittedToDate',
        },
    },
    data() {
        return {
            datatable_id: 'ocuurrence-report-datatable-' + uuid(),

            // selected values for filtering
            filterOCRGroupType: sessionStorage.getItem(
                this.filterOCRGroupType_cache
            )
                ? sessionStorage.getItem(this.filterOCRGroupType_cache)
                : 'all',

            filterOCRScientificName: sessionStorage.getItem(
                this.filterOCRScientificName_cache
            )
                ? sessionStorage.getItem(this.filterOCRScientificName_cache)
                : 'all',

            filterOCRExCommunityName: sessionStorage.getItem(
                this.filterOCRExCommunityName_cache
            )
                ? sessionStorage.getItem(this.filterOCRExCommunityName_cache)
                : 'all',

            filterOCRApplicationStatus: sessionStorage.getItem(
                this.filterOCRApplicationStatus_cache
            )
                ? sessionStorage.getItem(this.filterOCRApplicationStatus_cache)
                : 'all',

            filterOCRCommonName: sessionStorage.getItem(
                this.filterOCRCommonName_cache
            )
                ? sessionStorage.getItem(this.filterOCRCommonName_cache)
                : 'all',

            filterOCRCommunityId: sessionStorage.getItem(
                this.filterOCRCommunityId_cache
            )
                ? sessionStorage.getItem(this.filterOCRCommunityId_cache)
                : 'all',

            filterOCROccurrenceName: sessionStorage.getItem(
                this.filterOCROccurrenceName_cache
            )
                ? sessionStorage.getItem(this.filterOCROccurrenceName_cache)
                : 'all',

            filterOCROccurrence: sessionStorage.getItem(
                this.filterOCROccurrence_cache
            )
                ? sessionStorage.getItem(this.filterOCROccurrence_cache)
                : 'all',

            filterOCRRegion: (() => {
                const raw = sessionStorage.getItem(this.filterOCRRegion_cache);
                if (!raw || raw === 'all') return [];
                try {
                    return JSON.parse(raw);
                } catch {
                    return [];
                }
            })(),

            filterOCRDistrict: (() => {
                const raw = sessionStorage.getItem(
                    this.filterOCRDistrict_cache
                );
                if (!raw || raw === 'all') return [];
                try {
                    return JSON.parse(raw);
                } catch {
                    return [];
                }
            })(),

            filterOCRObservationFromDate: sessionStorage.getItem(
                this.filterOCRObservationFromDate_cache
            )
                ? sessionStorage.getItem(
                      this.filterOCRObservationFromDate_cache
                  )
                : '',

            filterOCRObservationToDate: sessionStorage.getItem(
                this.filterOCRObservationToDate_cache
            )
                ? sessionStorage.getItem(this.filterOCRObservationToDate_cache)
                : '',

            filterOCRSubmittedFromDate: sessionStorage.getItem(
                this.filterOCRSubmittedFromDate_cache
            )
                ? sessionStorage.getItem(this.filterOCRSubmittedFromDate_cache)
                : '',

            filterOCRSubmittedToDate: sessionStorage.getItem(
                this.filterOCRSubmittedToDate_cache
            )
                ? sessionStorage.getItem(this.filterOCRSubmittedToDate_cache)
                : '',

            //Filter list for scientific name and common name
            group_types: [],
            region_list: [],
            district_list: [],
            filtered_district_list: [],
            filterRegionDistrict: {},

            // filtering options
            external_status: [
                { value: 'draft', name: 'Draft' },
                { value: 'discarded', name: 'Discarded' },
                { value: 'with_assessor', name: 'Under Review' },
                { value: 'approved', name: 'Approved' },
                { value: 'declined', name: 'Declined' },
                { value: 'closed', name: 'Closed' },
            ],
            proposal_status: [],
        };
    },
    computed: {
        filterApplied: function () {
            if (
                this.filterOCRGroupType === 'all' &&
                this.filterOCRScientificName === 'all' &&
                this.filterOCRExCommunityName === 'all' &&
                this.filterOCRApplicationStatus === 'all' &&
                this.filterOCRCommonName === 'all' &&
                this.filterOCRCommunityId === 'all' &&
                this.filterOCROccurrenceName === 'all' &&
                this.filterOCROccurrence === 'all' &&
                this.filterOCRRegion.length === 0 &&
                this.filterOCRDistrict.length === 0 &&
                this.filterOCRObservationFromDate === '' &&
                this.filterOCRObservationToDate === '' &&
                this.filterOCRSubmittedFromDate === '' &&
                this.filterOCRSubmittedToDate === ''
            ) {
                return false;
            } else {
                return true;
            }
        },
        addOCRVisibility: function () {
            let visibility;
            visibility = true;
            return visibility;
        },
        datatable_headers: function () {
            return [
                'ID',
                'Number',
                'Type',
                'Scientific Name',
                'Community Name',
                'Common Name',
                'Community ID',
                'Occurrence Name',
                'Occurrence Number',
                'Region',
                'District',
                'Observation Date',
                'Submitted Date',
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
                name: 'id',
            };
        },
        column_number: function () {
            return {
                data: 'occurrence_report_number',
                orderable: true,
                searchable: true,
                visible: true,
                name: 'id',
            };
        },
        column_type: function () {
            return {
                data: 'group_type',
                orderable: true,
                searchable: true,
                visible: true,
                name: 'group_type__name',
            };
        },
        column_scientific_name: function () {
            return {
                data: 'scientific_name',
                orderable: true,
                searchable: true,
                visible: true,
                render: function (value, type) {
                    let result = helpers.dtPopover(value, 30, 'hover');
                    return type == 'export' ? value : result;
                },
                name: 'species__taxonomy__scientific_name',
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
                name: 'community__taxonomy__community_name',
            };
        },
        column_community_common_id: function () {
            return {
                data: 'community_common_id',
                orderable: false,
                searchable: false,
                visible: true,
                name: 'community__taxonomy__community_common_id',
            };
        },
        column_common_name: function () {
            return {
                data: 'common_name',
                orderable: false,
                searchable: false,
                visible: true,
                name: 'common_name',
            };
        },
        column_occurrence: function () {
            return {
                data: 'occurrence_name',
                orderable: true,
                searchable: true,
                visible: true,
                render: function (data, type, full) {
                    if (full.occurrence_name) {
                        return full.occurrence_name;
                    }
                    return '';
                },
                name: 'occurrence__occurrence_number',
            };
        },
        column_occurrence_name_text: function () {
            return {
                data: 'occurrence_name_text',
                orderable: true,
                searchable: true,
                visible: true,
                name: 'occurrence__occurrence_name',
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
        column_observation_date: function () {
            return {
                data: 'observation_date',
                orderable: true,
                searchable: true,
                visible: true,
                name: 'observation_date',
            };
        },
        column_lodgement_date: function () {
            return {
                data: 'lodgement_date',
                orderable: true,
                searchable: true,
                visible: true,
                name: 'lodgement_date',
            };
        },
        column_status: function () {
            return {
                data: 'customer_status',
                orderable: true,
                searchable: true,
                visible: true,
                name: 'customer_status',
            };
        },
        column_action: function () {
            return {
                data: 'id',
                orderable: false,
                searchable: false,
                visible: true,
                render: function (data, type, full) {
                    let links = '';
                    if (full.can_user_edit) {
                        if (full.processing_status == 'discarded') {
                            links += `<a href='#${full.id}' data-reinstate-ocr-proposal='${full.id}'>Reinstate</a><br/>`;
                        } else {
                            links += `<a href='/external/occurrence-report/${full.id}'>Continue</a><br/>`;
                            links += `<a href='#${full.id}' data-discard-ocr-proposal='${full.id}'>Discard</a><br/>`;
                        }
                    } else if (full.can_user_view) {
                        links += `<a href='/external/occurrence-report/${full.id}'>View</a><br />`;
                    }
                    links += `<a href='#${full.id}' data-copy-ocr-proposal='${full.id}'>Copy</a>`;
                    return links;
                },
            };
        },
        datatable_options: function () {
            let vm = this;
            let columns;
            let buttons = [
                {
                    extend: 'excel',
                    title: 'Boranga Occurrence Report Excel Export',
                    text: '<i class="bi bi-download"></i> Excel',
                    className: 'btn btn-primary me-2 rounded',
                    exportOptions: {
                        columns: ':not(.no-export)',
                        orthogonal: 'export',
                    },
                },
                {
                    extend: 'csv',
                    title: 'Boranga Occurrence Report CSV Export',
                    text: '<i class="bi bi-download"></i> CSV',
                    className: 'btn btn-primary rounded',
                    exportOptions: {
                        columns: ':not(.no-export)',
                        orthogonal: 'export',
                    },
                },
            ];
            columns = [
                vm.column_id,
                vm.column_number,
                vm.column_type,
                vm.column_scientific_name,
                vm.column_community_name,
                vm.column_common_name,
                vm.column_community_common_id,
                vm.column_occurrence_name_text,
                vm.column_occurrence,
                vm.column_region,
                vm.column_district,
                vm.column_observation_date,
                vm.column_lodgement_date,
                vm.column_status,
                vm.column_action,
            ];

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
                    method: 'POST',
                    headers: {
                        'X-CSRFToken': helpers.getCookie('csrftoken'),
                    },
                    data: function (d) {
                        d.filter_group_type = vm.filterOCRGroupType;
                        d.filter_scientific_name = vm.filterOCRScientificName;
                        d.filter_community_name = vm.filterOCRExCommunityName;
                        d.filter_application_status =
                            vm.filterOCRApplicationStatus;
                        d.is_internal = vm.is_internal;
                        d.filter_common_name = vm.filterOCRCommonName;
                        d.filter_community_common_id = vm.filterOCRCommunityId;
                        d.filter_occurrence_name = vm.filterOCROccurrenceName;
                        d.filter_occurrence = vm.filterOCROccurrence;
                        d.filter_region =
                            vm.filterOCRRegion.length > 0
                                ? vm.filterOCRRegion.join(',')
                                : 'all';
                        d.filter_district =
                            vm.filterOCRDistrict.length > 0
                                ? vm.filterOCRDistrict.join(',')
                                : 'all';
                        d.filter_observation_from_date =
                            vm.filterOCRObservationFromDate;
                        d.filter_observation_to_date =
                            vm.filterOCRObservationToDate;
                        d.filter_submitted_from_date =
                            vm.filterOCRSubmittedFromDate;
                        d.filter_submitted_to_date =
                            vm.filterOCRSubmittedToDate;
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
        filterOCRGroupType: function () {
            let vm = this;
            vm.$refs.occurrence_report_datatable.vmDataTable.ajax.reload(); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterOCRGroupType_cache,
                vm.filterOCRGroupType
            );
        },
        filterOCRScientificName: function () {
            let vm = this;
            vm.$refs.occurrence_report_datatable.vmDataTable.ajax.reload(); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterOCRScientificName_cache,
                vm.filterOCRScientificName
            );
        },
        filterOCRExCommunityName: function () {
            let vm = this;
            vm.$refs.occurrence_report_datatable.vmDataTable.ajax.reload(); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterOCRExCommunityName_cache,
                vm.filterOCRExCommunityName
            );
        },
        filterOCRApplicationStatus: function () {
            let vm = this;
            vm.$refs.occurrence_report_datatable.vmDataTable.ajax.reload(); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterOCRApplicationStatus_cache,
                vm.filterOCRApplicationStatus
            );
        },
        filterOCRCommonName: function () {
            let vm = this;
            vm.$refs.occurrence_report_datatable.vmDataTable.ajax.reload();
            sessionStorage.setItem(
                vm.filterOCRCommonName_cache,
                vm.filterOCRCommonName
            );
        },
        filterOCRCommunityId: function () {
            let vm = this;
            vm.$refs.occurrence_report_datatable.vmDataTable.ajax.reload();
            sessionStorage.setItem(
                vm.filterOCRCommunityId_cache,
                vm.filterOCRCommunityId
            );
        },
        filterOCROccurrenceName: function () {
            let vm = this;
            vm.$refs.occurrence_report_datatable.vmDataTable.ajax.reload();
            sessionStorage.setItem(
                vm.filterOCROccurrenceName_cache,
                vm.filterOCROccurrenceName
            );
        },
        filterOCROccurrence: function () {
            let vm = this;
            vm.$refs.occurrence_report_datatable.vmDataTable.ajax.reload();
            sessionStorage.setItem(
                vm.filterOCROccurrence_cache,
                vm.filterOCROccurrence
            );
        },
        filterOCRRegion: {
            handler: function () {
                let vm = this;
                vm.$refs.occurrence_report_datatable.vmDataTable.ajax.reload();
                sessionStorage.setItem(
                    vm.filterOCRRegion_cache,
                    JSON.stringify(vm.filterOCRRegion)
                );
            },
            deep: true,
        },
        filterOCRDistrict: {
            handler: function () {
                let vm = this;
                vm.$refs.occurrence_report_datatable.vmDataTable.ajax.reload();
                sessionStorage.setItem(
                    vm.filterOCRDistrict_cache,
                    JSON.stringify(vm.filterOCRDistrict)
                );
            },
            deep: true,
        },
        filterOCRObservationFromDate: function () {
            let vm = this;
            vm.$refs.occurrence_report_datatable.vmDataTable.ajax.reload();
            sessionStorage.setItem(
                vm.filterOCRObservationFromDate_cache,
                vm.filterOCRObservationFromDate
            );
        },
        filterOCRObservationToDate: function () {
            let vm = this;
            vm.$refs.occurrence_report_datatable.vmDataTable.ajax.reload();
            sessionStorage.setItem(
                vm.filterOCRObservationToDate_cache,
                vm.filterOCRObservationToDate
            );
        },
        filterOCRSubmittedFromDate: function () {
            let vm = this;
            vm.$refs.occurrence_report_datatable.vmDataTable.ajax.reload();
            sessionStorage.setItem(
                vm.filterOCRSubmittedFromDate_cache,
                vm.filterOCRSubmittedFromDate
            );
        },
        filterOCRSubmittedToDate: function () {
            let vm = this;
            vm.$refs.occurrence_report_datatable.vmDataTable.ajax.reload();
            sessionStorage.setItem(
                vm.filterOCRSubmittedToDate_cache,
                vm.filterOCRSubmittedToDate
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
            vm.initialiseScientificNameLookup();
            vm.initialiseCommunityNameLookup();
            vm.initialiseCommonNameLookup();
            vm.initialiseCommunityIdLookup();
            vm.initialiseOccurrenceNameLookup();
            vm.initialiseOccurrenceLookup();
            vm.addEventListeners();
            var newOption;
            if (
                sessionStorage.getItem('filterOCRScientificName') != 'all' &&
                sessionStorage.getItem('filterOCRScientificName') != null
            ) {
                newOption = new Option(
                    sessionStorage.getItem('filterOCRScientificNameText'),
                    vm.filterOCRScientificName,
                    false,
                    true
                );
                $('#ocr_scientific_name_lookup').append(newOption);
            }
            if (
                sessionStorage.getItem('filterOCRExCommunityName') != 'all' &&
                sessionStorage.getItem('filterOCRExCommunityName') != null
            ) {
                newOption = new Option(
                    sessionStorage.getItem('filterOCRExCommunityNameText'),
                    vm.filterOCRExCommunityName,
                    false,
                    true
                );
                $('#ocr_community_name_lookup').append(newOption);
            }
            if (
                sessionStorage.getItem('filterOCRCommonName') != 'all' &&
                sessionStorage.getItem('filterOCRCommonName') != null
            ) {
                newOption = new Option(
                    sessionStorage.getItem('filterOCRCommonNameText'),
                    vm.filterOCRCommonName,
                    false,
                    true
                );
                $('#ext_ocr_common_name_lookup').append(newOption);
            }
            if (
                sessionStorage.getItem('filterOCRCommunityId') != 'all' &&
                sessionStorage.getItem('filterOCRCommunityId') != null
            ) {
                newOption = new Option(
                    sessionStorage.getItem('filterOCRCommunityIdText'),
                    vm.filterOCRCommunityId,
                    false,
                    true
                );
                $('#ext_ocr_community_id_lookup').append(newOption);
            }
            if (
                sessionStorage.getItem('filterOCROccurrenceName') != 'all' &&
                sessionStorage.getItem('filterOCROccurrenceName') != null
            ) {
                newOption = new Option(
                    sessionStorage.getItem('filterOCROccurrenceNameText'),
                    vm.filterOCROccurrenceName,
                    false,
                    true
                );
                $('#ext_ocr_occurrence_name_lookup').append(newOption);
            }
            if (
                sessionStorage.getItem('filterOCROccurrence') != 'all' &&
                sessionStorage.getItem('filterOCROccurrence') != null
            ) {
                newOption = new Option(
                    sessionStorage.getItem('filterOCROccurrenceText'),
                    vm.filterOCROccurrence,
                    false,
                    true
                );
                $('#ext_ocr_occurrence_lookup').append(newOption);
            }
        });
    },
    methods: {
        initialiseScientificNameLookup: function () {
            let vm = this;
            $(vm.$refs.ocr_scientific_name_lookup)
                .select2({
                    minimumInputLength: 2,
                    theme: 'bootstrap-5',
                    allowClear: true,
                    placeholder: 'Select Scientific Name',
                    ajax: {
                        url: api_endpoints.species_lookup,
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
                    vm.filterOCRScientificName = data;
                    sessionStorage.setItem(
                        'filterOCRScientificNameText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.filterOCRScientificName = 'all';
                    sessionStorage.setItem('filterOCRScientificNameText', '');
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-ocr_scientific_name_lookup-results"]'
                    );
                    // move focus to select2 field
                    searchField[0].focus();
                });
        },
        initialiseCommunityNameLookup: function () {
            let vm = this;
            $(vm.$refs.ocr_community_name_lookup)
                .select2({
                    minimumInputLength: 2,
                    theme: 'bootstrap-5',
                    allowClear: true,
                    placeholder: 'Select Community Name',
                    ajax: {
                        url: api_endpoints.communities_lookup,
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
                    vm.filterOCRExCommunityName = data;
                    sessionStorage.setItem(
                        'filterOCRExCommunityNameText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.filterOCRExCommunityName = 'all';
                    sessionStorage.setItem('filterOCRExCommunityNameText', '');
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-ocr_community_name_lookup-results"]'
                    );
                    // move focus to select2 field
                    searchField[0].focus();
                });
        },
        fetchFilterLists: function () {
            let vm = this;
            vm.proposal_status = vm.external_status;
            fetch(api_endpoints.group_types_dict).then(
                async (response) => {
                    vm.group_types = await response.json();
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
                    vm.filtered_district_list = vm.district_list;
                    vm.filterDistrict();
                },
                (error) => {
                    console.log(error);
                }
            );
        },
        filterDistrict: function () {
            let vm = this;
            if (vm.filterOCRRegion.length === 0) {
                vm.filtered_district_list = vm.district_list;
            } else {
                vm.filtered_district_list = vm.district_list.filter((d) =>
                    vm.filterOCRRegion.includes(d.region_id)
                );
            }
            if (vm.filterOCRDistrict.length > 0) {
                const validIds = vm.filtered_district_list.map((d) => d.id);
                vm.filterOCRDistrict = vm.filterOCRDistrict.filter((id) =>
                    validIds.includes(id)
                );
            }
        },
        onRegionFilterChange: function (val) {
            this.filterOCRRegion = val;
            this.filterDistrict();
        },
        initialiseCommonNameLookup: function () {
            let vm = this;
            $(vm.$refs.ext_ocr_common_name_lookup)
                .select2({
                    minimumInputLength: 2,
                    dropdownParent: $('#ext_select_common_name'),
                    theme: 'bootstrap-5',
                    allowClear: true,
                    placeholder: 'Select Common Name',
                    ajax: {
                        url: api_endpoints.common_name_lookup,
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
                    vm.filterOCRCommonName = data;
                    sessionStorage.setItem(
                        'filterOCRCommonNameText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.filterOCRCommonName = 'all';
                    sessionStorage.setItem('filterOCRCommonNameText', '');
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-ext_ocr_common_name_lookup-results"]'
                    );
                    searchField[0].focus();
                });
        },
        initialiseCommunityIdLookup: function () {
            let vm = this;
            $(vm.$refs.ext_ocr_community_id_lookup)
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
                    vm.filterOCRCommunityId = data;
                    sessionStorage.setItem(
                        'filterOCRCommunityIdText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.filterOCRCommunityId = 'all';
                    sessionStorage.setItem('filterOCRCommunityIdText', '');
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-ext_ocr_community_id_lookup-results"]'
                    );
                    searchField[0].focus();
                });
        },
        initialiseOccurrenceNameLookup: function () {
            let vm = this;
            $(vm.$refs.ext_ocr_occurrence_name_lookup)
                .select2({
                    minimumInputLength: 2,
                    dropdownParent: $('#ext_select_occurrence_name'),
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
                                active_only: false,
                            };
                            return query;
                        },
                    },
                })
                .on('select2:select', function (e) {
                    let data = e.params.data.id;
                    vm.filterOCROccurrenceName = data;
                    sessionStorage.setItem(
                        'filterOCROccurrenceNameText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.filterOCROccurrenceName = 'all';
                    sessionStorage.setItem('filterOCROccurrenceNameText', '');
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-ext_ocr_occurrence_name_lookup-results"]'
                    );
                    searchField[0].focus();
                });
        },
        initialiseOccurrenceLookup: function () {
            let vm = this;
            $(vm.$refs.ext_ocr_occurrence_lookup)
                .select2({
                    minimumInputLength: 2,
                    dropdownParent: $('#ext_select_occurrence'),
                    theme: 'bootstrap-5',
                    allowClear: true,
                    placeholder: 'Select Occurrence',
                    ajax: {
                        url: api_endpoints.occurrence_lookup,
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
                    vm.filterOCROccurrence = data;
                    sessionStorage.setItem(
                        'filterOCROccurrenceText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.filterOCROccurrence = 'all';
                    sessionStorage.setItem('filterOCROccurrenceText', '');
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-ext_ocr_occurrence_lookup-results"]'
                    );
                    searchField[0].focus();
                });
        },
        createOccurrenceReport: async function (group_type, group_type_name) {
            swal.fire({
                title: `Add ${group_type_name} Occurrence Report`,
                text: `Are you sure you want to add a new ${group_type_name} Occurrence Report?`,
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: `Add ${group_type_name} Occurrence Report`,
                reverseButtons: true,
                customClass: {
                    confirmButton: 'btn btn-primary',
                },
            }).then(async (swalresult) => {
                if (swalresult.isConfirmed) {
                    let newOCRId = null;
                    try {
                        const createUrl = api_endpoints.occurrence_report + '/';
                        let payload = new Object();
                        payload.group_type_id = group_type;
                        let response = await fetch(createUrl, {
                            method: 'POST',
                            headers: {
                                'Content-Type': 'application/json',
                            },
                            body: JSON.stringify(payload),
                        });
                        const data = await response.json();
                        if (data) {
                            newOCRId = data;
                        }
                    } catch (err) {
                        console.log(err);
                    }
                    this.$router.push({
                        name: 'draft_ocr_proposal',
                        params: { occurrence_report_id: newOCRId.id },
                    });
                }
            });
        },
        discardOCR: function (occurrence_report_id) {
            let vm = this;
            swal.fire({
                title: 'Discard Occurrence Report',
                text: 'Are you sure you want to discard this occurrence report?',
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Discard Occurrence Report',
                customClass: {
                    confirmButton: 'btn btn-primary',
                    cancelButton: 'btn btn-secondary',
                },
                reverseButtons: true,
            }).then((swalresult) => {
                if (swalresult.isConfirmed) {
                    fetch(
                        api_endpoints.discard_ocr_proposal(
                            occurrence_report_id
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
                                title: 'Occurrence Report Discarded',
                                text: 'The occurrence report has been discarded',
                                icon: 'success',
                                customClass: {
                                    confirmButton: 'btn btn-primary',
                                },
                            });
                            vm.$refs.occurrence_report_datatable.vmDataTable.ajax.reload();
                        },
                        (error) => {
                            console.log(error);
                        }
                    );
                }
            });
        },
        reinstateOCRProposal: function (occurrence_report_id) {
            let vm = this;
            swal.fire({
                title: 'Reinstate Report',
                text: 'Are you sure you want to reinstate this report?',
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
                            api_endpoints.reinstate_ocr_proposal(
                                occurrence_report_id
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
                                    text: 'Your report has been reinstated',
                                    icon: 'success',
                                    customClass: {
                                        confirmButton: 'btn btn-primary',
                                    },
                                });
                                vm.$refs.occurrence_report_datatable.vmDataTable.ajax.reload(
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
        copyOCRProposal: function (occurrence_report_id) {
            let vm = this;
            swal.fire({
                title: 'Copy Occurrence Report',
                text: `Are you sure you want to make a copy of occurrence report ${constants.MODELS.OCCURRENCE_REPORT.MODEL_PREFIX}${occurrence_report_id}?`,
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Copy Occurrence Report',
                customClass: {
                    confirmButton: 'btn btn-primary',
                    cancelButton: 'btn btn-secondary',
                },
                reverseButtons: true,
            }).then((swalresult) => {
                if (swalresult.isConfirmed) {
                    fetch(
                        helpers.add_endpoint_json(
                            api_endpoints.occurrence_report,
                            occurrence_report_id + '/copy'
                        ),
                        {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                        }
                    ).then(
                        async (response) => {
                            if (!response.ok) {
                                const errData = await response
                                    .json()
                                    .catch(() => ({}));
                                swal.fire({
                                    title: 'Error',
                                    text:
                                        errData.detail ||
                                        'An error occurred while copying the occurrence report.',
                                    icon: 'error',
                                    customClass: {
                                        confirmButton: 'btn btn-primary',
                                    },
                                });
                                return;
                            }
                            const ocr_copy = await response.json();
                            swal.fire({
                                title: 'Copied',
                                text: `The occurrence report has been copied to ${ocr_copy.occurrence_report_number}. When you click OK, the new occurrence report will open in a new window.`,
                                icon: 'success',
                                customClass: {
                                    confirmButton: 'btn btn-primary',
                                },
                                didClose: () => {
                                    vm.$refs.occurrence_report_datatable.vmDataTable.ajax.reload();
                                    const routeData = this.$router.resolve({
                                        name: 'draft_ocr_proposal',
                                        params: {
                                            occurrence_report_id: ocr_copy.id,
                                        },
                                        query: { action: 'edit' },
                                    });
                                    window.open(routeData.href, '_blank');
                                },
                            });
                        },
                        (error) => {
                            console.log(error);
                        }
                    );
                }
            });
        },
        addEventListeners: function () {
            let vm = this;
            // External Discard listener
            vm.$refs.occurrence_report_datatable.vmDataTable.on(
                'click',
                'a[data-discard-ocr-proposal]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-discard-ocr-proposal');
                    vm.discardOCR(id);
                }
            );
            vm.$refs.occurrence_report_datatable.vmDataTable.on(
                'click',
                'a[data-reinstate-ocr-proposal]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-reinstate-ocr-proposal');
                    vm.reinstateOCRProposal(id);
                }
            );
            vm.$refs.occurrence_report_datatable.vmDataTable.on(
                'click',
                'a[data-copy-ocr-proposal]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-copy-ocr-proposal');
                    vm.copyOCRProposal(id);
                }
            );
            vm.$refs.occurrence_report_datatable.vmDataTable.on(
                'childRow.dt',
                function () {
                    helpers.enablePopovers();
                }
            );
        },
        check_assessor: function (proposal) {
            let vm = this;
            if (proposal.assigned_officer) {
                {
                    if (proposal.assigned_officer == vm.profile.user.full_name)
                        return true;
                    else return false;
                }
            } else {
                var assessor = proposal.allowed_assessors.filter(
                    function (elem) {
                        return (elem.id = vm.profile.user.id);
                    }
                );

                if (assessor.length > 0) return true;
                else return false;
            }
        },
    },
};
</script>
<style scoped>
.dt-buttons {
    float: right;
}

.collapse-icon {
    cursor: pointer;
}

.collapse-icon::before {
    top: 5px;
    left: 4px;
    height: 14px;
    width: 14px;
    border-radius: 14px;
    line-height: 14px;
    border: 2px solid white;
    line-height: 14px;
    content: '-';
    color: white;
    background-color: #d33333;
    display: inline-block;
    box-shadow: 0px 0px 3px #444;
    box-sizing: content-box;
    text-align: center;
    text-indent: 0 !important;
    font-family:
        'Courier New',
        Courier monospace;
    margin: 5px;
}

.expand-icon {
    cursor: pointer;
}

.expand-icon::before {
    top: 5px;
    left: 4px;
    height: 14px;
    width: 14px;
    border-radius: 14px;
    line-height: 14px;
    border: 2px solid white;
    line-height: 14px;
    content: '+';
    color: white;
    background-color: #337ab7;
    display: inline-block;
    box-shadow: 0px 0px 3px #444;
    box-sizing: content-box;
    text-align: center;
    text-indent: 0 !important;
    font-family:
        'Courier New',
        Courier monospace;
    margin: 5px;
}
</style>
