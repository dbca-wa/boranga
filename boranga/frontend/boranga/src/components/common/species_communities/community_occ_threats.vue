<template lang="html">
    <div id="community_occ_threats">
        <CollapsibleFilters
            ref="collapsible_filters"
            component_title="Filters"
            class="mb-2"
        >
            <div class="row">
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="">Threat Source:</label>
                        <select
                            v-model="filterThreatSource"
                            class="form-select"
                        >
                            <option value="all">All</option>
                            <option
                                v-for="option in threat_source_filter_list"
                                :value="option"
                                :key="option"
                            >
                                {{ option }}
                            </option>
                        </select>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="">Category:</label>
                        <select
                            v-model="filterThreatCategory"
                            class="form-select"
                        >
                            <option value="all">All</option>
                            <option
                                v-for="option in threat_category_filter_list"
                                :value="option.id"
                                :key="option.id"
                            >
                                {{ option.name }}
                            </option>
                        </select>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="">Current Impact:</label>
                        <select
                            v-model="filterThreatCurrentImpact"
                            class="form-select"
                        >
                            <option value="all">All</option>
                            <option
                                v-for="option in threat_current_impact_filter_list"
                                :value="option.id"
                                :key="option.id"
                            >
                                {{ option.name }}
                            </option>
                        </select>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="">Potential Impact:</label>
                        <select
                            v-model="filterThreatPotentialImpact"
                            class="form-select"
                        >
                            <option value="all">All</option>
                            <option
                                v-for="option in threat_potential_impact_filter_list"
                                :value="option.id"
                                :key="option.id"
                            >
                                {{ option.name }}
                            </option>
                        </select>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="">Status:</label>
                        <select
                            v-model="filterThreatStatus"
                            class="form-select"
                        >
                            <option value="all">All</option>
                            <option
                                v-for="option in threat_status_filter_list"
                                :value="option.id"
                                :key="option.id"
                            >
                                {{ option.name }}
                            </option>
                        </select>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="">Date Observed From:</label>
                        <input
                            id="observed_from_date"
                            v-model="filterObservedFromDate"
                            type="date"
                            class="form-control"
                            placeholder="DD/MM/YYYY"
                        />
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="">Date Observed To:</label>
                        <input
                            id="observed_to_date"
                            v-model="filterObservedToDate"
                            type="date"
                            class="form-control"
                            placeholder="DD/MM/YYYY"
                        />
                    </div>
                </div>
            </div>
        </CollapsibleFilters>
        <div>
            <datatable
                :id="panelBody"
                ref="threats_datatable"
                :dt-options="threats_options"
                :dt-headers="threats_headers"
            />
        </div>

        <ThreatDetail
            ref="threat_detail"
            :url="occ_threat_url"
            @refresh-from-response="refreshFromResponse"
        >
        </ThreatDetail>
        <div v-if="occConservationThreatHistoryId">
            <ConservationThreatHistory
                ref="occ_conservation_threat_history"
                :key="occConservationThreatHistoryId"
                :threat-id="occConservationThreatHistoryId"
            />
        </div>
    </div>
</template>
<script>
import { v4 as uuid } from 'uuid';
import datatable from '@vue-utils/datatable.vue';
import ThreatDetail from '@/components/common/species_communities/add_threat.vue';
import ConservationThreatHistory from '../../internal/occurrence/occ_conservation_threat_history.vue';
import CollapsibleFilters from '@/components/forms/collapsible_component.vue';
import { constants, api_endpoints, helpers } from '@/utils/hooks';

export default {
    name: 'OCCThreats',
    components: {
        datatable,
        ThreatDetail,
        ConservationThreatHistory,
        CollapsibleFilters,
    },
    props: {
        community_obj: {
            type: Object,
            required: true,
        },
        is_internal: {
            type: Boolean,
            default: false,
        },
    },
    data: function () {
        let vm = this;
        const communityEndpointBase = vm.is_internal
            ? api_endpoints.community
            : api_endpoints.external_community;
        return {
            uuid: 0,
            occConservationThreatHistoryId: null,
            showExisting: false,
            threatBody: 'threatBody' + uuid(),
            panelBody: 'community-threats-' + uuid(),
            values: null,
            occ_threat_url: api_endpoints.occ_threat,
            community_occurrence_endpoint_base: communityEndpointBase,

            filterThreatSource: 'all',
            filterThreatCategory: 'all',
            filterThreatCurrentImpact: 'all',
            filterThreatPotentialImpact: 'all',
            filterThreatStatus: 'all',
            filterObservedFromDate: '',
            filterObservedToDate: '',

            threat_source_filter_list: [],
            threat_category_filter_list: [],
            threat_current_impact_filter_list: [],
            threat_potential_impact_filter_list: [],

            threat_status_filter_list: [
                { id: 'active', name: 'Active' },
                { id: 'removed', name: 'Removed' },
            ],

            threats_headers: [
                'Number',
                'Original Report',
                'Category',
                'Date Observed',
                'Threat Agent',
                'Comments',
                'Current Impact',
                'Potential Impact',
                'Threat Source',
                'Action',
            ],
            threats_options: {
                autowidth: false,
                language: {
                    processing: constants.DATATABLE_PROCESSING_HTML,
                },
                responsive: true,
                searching: true,
                //  to show the "workflow Status","Action" columns always in the last position
                columnDefs: [
                    { responsivePriority: 1, targets: 0 },
                    { responsivePriority: 2, targets: -1 },
                ],
                ajax: {
                    url: helpers.add_endpoint_json(
                        communityEndpointBase,
                        vm.community_obj.id + '/occurrence_threats'
                    ),
                    dataSrc: '',
                    data: function (d) {
                        d.filter_threat_source = vm.filterThreatSource;
                        d.filter_threat_category = vm.filterThreatCategory;
                        d.filter_threat_current_impact =
                            vm.filterThreatCurrentImpact;
                        d.filter_threat_potential_impact =
                            vm.filterThreatPotentialImpact;
                        d.filter_threat_status = vm.filterThreatStatus;
                        d.filter_observed_from_date = vm.filterObservedFromDate;
                        d.filter_observed_to_date = vm.filterObservedToDate;
                    },
                },
                oorder: [[0, 'desc']],
                dom:
                    "<'d-flex align-items-center'<'me-auto'l>fB>" +
                    "<'row'<'col-sm-12'tr>>" +
                    "<'d-flex align-items-center'<'me-auto'i>p>",
                buttons: [
                    {
                        extend: 'excel',
                        title: 'Boranga Community Occurrence Threats Excel Export',
                        text: '<i class="fa-solid fa-download"></i> Excel',
                        className: 'btn btn-primary me-2 rounded',
                        exportOptions: {
                            orthogonal: 'export',
                        },
                    },
                    {
                        extend: 'csv',
                        title: 'Boranga Community Occurrence Threats CSV Export',
                        text: '<i class="fa-solid fa-download"></i> CSV',
                        className: 'btn btn-primary rounded',
                        exportOptions: {
                            orthogonal: 'export',
                        },
                    },
                ],
                columns: [
                    {
                        data: 'threat_number',
                        orderable: true,
                        searchable: true,
                        mRender: function (data, type, full) {
                            if (full.visible) {
                                return (
                                    'OCC' +
                                    full.occurrence +
                                    ' - ' +
                                    full.threat_number
                                );
                            } else {
                                return (
                                    '<s>' +
                                    'OCC' +
                                    full.occurrence +
                                    ' - ' +
                                    full.threat_number +
                                    '</s>'
                                );
                            }
                        },
                    },
                    {
                        data: 'original_report',
                        orderable: true,
                        searchable: true,
                        mRender: function (data, type, full) {
                            if (full.visible && full.original_report != null) {
                                return (
                                    full.original_report +
                                    ' - ' +
                                    full.original_threat
                                );
                            } else {
                                return '';
                            }
                        },
                    },
                    {
                        data: 'threat_category',
                        orderable: true,
                        searchable: true,
                        mRender: function (data, type, full) {
                            if (full.visible) {
                                return full.threat_category;
                            } else {
                                return '<s>' + full.threat_category + '</s>';
                            }
                        },
                    },
                    {
                        data: 'date_observed',
                        mRender: function (data, type, full) {
                            if (full.visible) {
                                return data != '' && data != null
                                    ? moment(data).format('DD/MM/YYYY')
                                    : '';
                            } else {
                                return data != '' && data != null
                                    ? '<s>' + moment(data).format('DD/MM/YYYY')
                                    : '' + '</s>';
                            }
                        },
                    },
                    {
                        data: 'threat_agent',
                        orderable: true,
                        searchable: true,
                        mRender: function (data, type, full) {
                            if (full.visible) {
                                return full.threat_agent;
                            } else {
                                return '<s>' + full.threat_agent + '</s>';
                            }
                        },
                    },
                    {
                        data: 'comment',
                        orderable: true,
                        searchable: true,
                        render: function (value, type, full) {
                            let result = helpers.dtPopover(value, 30, 'hover');
                            if (full.visible) {
                                return type == 'export' ? value : result;
                            } else {
                                return type == 'export'
                                    ? '<s>' + value + '</s>'
                                    : '<s>' + result + '</s>';
                            }
                        },
                    },
                    {
                        data: 'current_impact_name',
                        orderable: true,
                        searchable: true,
                        mRender: function (data, type, full) {
                            if (full.visible) {
                                return full.current_impact_name;
                            } else {
                                return (
                                    '<s>' + full.current_impact_name + '</s>'
                                );
                            }
                        },
                    },
                    {
                        data: 'potential_impact_name',
                        orderable: true,
                        searchable: true,
                        mRender: function (data, type, full) {
                            if (full.visible) {
                                return full.potential_impact_name;
                            } else {
                                return (
                                    '<s>' + full.potential_impact_name + '</s>'
                                );
                            }
                        },
                    },
                    {
                        data: 'source',
                        orderable: true,
                        searchable: true,
                        mRender: function (data, type, full) {
                            if (full.visible) {
                                return full.source;
                            } else {
                                return '<s>' + full.source + '</s>';
                            }
                        },
                    },
                    {
                        data: 'id',
                        mRender: function (data, type, full) {
                            let links = '';
                            if (full.visible) {
                                links += `<a href='#${full.id}' data-view-threat='${full.id}'>View</a><br/>`;
                            }
                            if (vm.is_internal) {
                                links += `<a href='#' data-history-threat='${full.id}'>History</a><br>`;
                            }
                            return links;
                        },
                    },
                ],
                processing: true,
                drawCallback: function () {
                    helpers.enablePopovers();
                },
                initComplete: function () {
                    helpers.enablePopovers();
                    // another option to fix the responsive table overflow css on tab switch
                    // vm.$refs.threats_datatable.vmDataTable.draw('page');
                    setTimeout(function () {
                        vm.adjust_table_width();
                    }, 100);
                },
            },
        };
    },
    computed: {
        filterApplied: function () {
            if (
                this.filterThreatSource === 'all' &&
                this.filterThreatCategory === 'all' &&
                this.filterThreatCurrentImpact === 'all' &&
                this.filterThreatPotentialImpact === 'all' &&
                this.filterThreatStatus === 'all' &&
                this.filterObservedFromDate === '' &&
                this.filterObservedToDate === ''
            ) {
                return false;
            } else {
                return true;
            }
        },
    },
    watch: {
        filterThreatSource: function () {
            let vm = this;
            vm.$refs.threats_datatable.vmDataTable.ajax.reload(); // This calls ajax() backend call.
        },
        filterThreatCategory: function () {
            let vm = this;
            vm.$refs.threats_datatable.vmDataTable.ajax.reload(); // This calls ajax() backend call.
        },
        filterThreatCurrentImpact: function () {
            let vm = this;
            vm.$refs.threats_datatable.vmDataTable.ajax.reload(); // This calls ajax() backend call.
        },
        filterThreatPotentialImpact: function () {
            let vm = this;
            vm.$refs.threats_datatable.vmDataTable.ajax.reload(); // This calls ajax() backend call.
        },
        filterThreatStatus: function () {
            let vm = this;
            vm.$refs.threats_datatable.vmDataTable.ajax.reload(); // This calls ajax() backend call.
        },
        filterObservedFromDate: function () {
            let vm = this;
            vm.$refs.threats_datatable.vmDataTable.ajax.reload(); // This calls ajax() backend call.
        },
        filterObservedToDate: function () {
            let vm = this;
            vm.$refs.threats_datatable.vmDataTable.ajax.reload(); // This calls ajax() backend call.
        },
    },
    mounted: function () {
        this.fetchFilterLists();
        let vm = this;
        this.$nextTick(() => {
            vm.addEventListeners();
        });
    },
    methods: {
        fetchFilterLists: function () {
            let vm = this;

            //Threat Source filter list (specific to instance)
            fetch(
                helpers.add_endpoint_json(
                    vm.community_occurrence_endpoint_base,
                    vm.community_obj.id + '/occurrence_threat_source_list'
                )
            ).then(
                async (response) => {
                    vm.threat_source_filter_list = await response.json();
                },
                (error) => {
                    console.log(error);
                }
            );

            //Category, Current Impact, Potential Impact (generic to all threats)
            fetch('/api/threat/threat_list_of_values/').then(
                async (response) => {
                    const data = await response.json();
                    vm.threat_category_filter_list =
                        data['threat_category_lists'];
                    vm.threat_current_impact_filter_list =
                        data['current_impact_lists'];
                    vm.threat_potential_impact_filter_list =
                        data['potential_impact_lists'];
                },
                (error) => {
                    console.log(error);
                }
            );
        },
        viewThreat: function (id) {
            this.$refs.threat_detail.threat_id = id;
            this.$refs.threat_detail.threat_action = 'view';
            fetch(helpers.add_endpoint_json(api_endpoints.occ_threat, id)).then(
                async (response) => {
                    const data = await response.json();
                    this.$refs.threat_detail.threatObj = data;
                    this.$refs.threat_detail.threatObj.date_observed =
                        data.date_observed != null &&
                        data.date_observed != undefined
                            ? moment(data.date_observed).format('yyyy-MM-DD')
                            : '';
                },
                (err) => {
                    console.log(err);
                }
            );
            this.$refs.threat_detail.isModalOpen = true;
        },
        historyThreat: function (id) {
            this.occConservationThreatHistoryId = parseInt(id);
            this.uuid++;
            this.$nextTick(() => {
                this.$refs.occ_conservation_threat_history.isModalOpen = true;
            });
        },
        updatedThreats() {
            this.$refs.threats_datatable.vmDataTable.ajax.reload();
        },
        addEventListeners: function () {
            let vm = this;
            vm.$refs.threats_datatable.vmDataTable.on(
                'click',
                'a[data-view-threat]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-view-threat');
                    vm.viewThreat(id);
                }
            );
            vm.$refs.threats_datatable.vmDataTable.on(
                'click',
                'a[data-history-threat]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-history-threat');
                    vm.historyThreat(id);
                }
            );
            vm.$refs.threats_datatable.vmDataTable.on(
                'childRow.dt',
                function () {
                    helpers.enablePopovers();
                }
            );
        },
        refreshFromResponse: function () {
            this.$refs.threats_datatable.vmDataTable.ajax.reload();
        },
        adjust_table_width: function () {
            if (this.$refs.threats_datatable !== undefined) {
                this.$refs.threats_datatable.vmDataTable.columns
                    .adjust()
                    .responsive.recalc();
            }
        },
    },
};
</script>

<style lang="css" scoped>
/*ul, li {
        zoom:1;
        display: inline;
    }*/
fieldset.scheduler-border {
    border: 1px groove #ddd !important;
    padding: 0 1.4em 1.4em 1.4em !important;
    margin: 0 0 1.5em 0 !important;
    -webkit-box-shadow: 0px 0px 0px 0px #000;
    box-shadow: 0px 0px 0px 0px #000;
}

legend.scheduler-border {
    width: inherit;
    /* Or auto */
    padding: 0 10px;
    /* To give a bit of padding on the left and right */
    border-bottom: none;
}
</style>
