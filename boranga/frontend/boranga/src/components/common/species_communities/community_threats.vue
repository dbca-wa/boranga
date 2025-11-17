<template lang="html">
    <div id="community_threats">
        <FormSection :form-collapse="false" label="Threats" Index="threats">
            <CollapsibleFilters
                ref="collapsible_filters"
                component_title="Filters"
                class="mb-2"
                :show-warning-icon="filterApplied"
            >
                <div class="row">
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
            <form class="form-horizontal" action="index.html" method="post">
                <div
                    v-if="!species_community.readonly && is_internal"
                    class="col-sm-12"
                >
                    <div class="text-end">
                        <button
                            type="button"
                            class="btn btn-primary mb-2"
                            @click.prevent="newThreat"
                        >
                            <i class="fa-solid fa-circle-plus"></i>
                            Add Threat
                        </button>
                    </div>
                </div>
                <div>
                    <datatable
                        :id="panelBody"
                        ref="threats_datatable"
                        :dt-options="threats_options"
                        :dt-headers="threats_headers"
                    />
                </div>
            </form>
        </FormSection>
        <FormSection
            v-if="showOccurrenceThreats"
            :form-collapse="false"
            label="Occurrence Threats"
            :Index="occThreatBody"
        >
            <CommunityOCCThreats
                :community_obj="species_community"
                :is_internal="is_internal"
            />
        </FormSection>

        <ThreatDetail
            ref="threat_detail"
            :url="threat_url"
            @refresh-from-response="refreshFromResponse"
        >
        </ThreatDetail>
        <div v-if="conservationThreatHistoryId">
            <ConservationThreatHistory
                ref="conservation_threat_history"
                :key="conservationThreatHistoryId"
                :threat-id="conservationThreatHistoryId"
            />
        </div>
    </div>
</template>
<script>
import { v4 as uuid } from 'uuid';
import datatable from '@vue-utils/datatable.vue';
import ThreatDetail from './add_threat.vue';
import FormSection from '@/components/forms/section_toggle.vue';
import ConservationThreatHistory from '../../internal/species_communities/conservation_threat_history.vue';
import CommunityOCCThreats from '@/components/common/species_communities/community_occ_threats.vue';
import CollapsibleFilters from '@/components/forms/collapsible_component.vue';
import { constants, api_endpoints, helpers } from '@/utils/hooks';

export default {
    name: 'CommunityThreats',
    components: {
        FormSection,
        datatable,
        ThreatDetail,
        ConservationThreatHistory,
        CommunityOCCThreats,
        CollapsibleFilters,
    },
    props: {
        species_community: {
            type: Object,
            required: true,
        },
        is_readonly: {
            type: Boolean,
            default: false,
        },
        is_internal: {
            type: Boolean,
            default: false,
        },
    },
    data: function () {
        let vm = this;
        let url = '';
        if (vm.is_internal) {
            url = helpers.add_endpoint_json(
                api_endpoints.community,
                vm.species_community.id + '/threats'
            );
        } else {
            url = helpers.add_endpoint_json(
                api_endpoints.external_community,
                vm.species_community.id + '/threats'
            );
        }
        return {
            uuid: 0,
            conservationThreatHistoryId: null,
            panelBody: 'community-threats-' + uuid(),
            values: null,
            occThreatBody: 'occThreatBody' + uuid(),
            threat_url: api_endpoints.threat,

            filterThreatCategory: 'all',
            filterThreatCurrentImpact: 'all',
            filterThreatPotentialImpact: 'all',
            filterThreatStatus: 'all',
            filterObservedFromDate: '',
            filterObservedToDate: '',

            threat_category_filter_list: [],
            threat_current_impact_filter_list: [],
            threat_potential_impact_filter_list: [],

            threat_status_filter_list: [
                { id: 'active', name: 'Active' },
                { id: 'removed', name: 'Discarded' },
            ],

            threats_headers: [
                'Number',
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
                    url: url,
                    dataSrc: '',
                    data: function (d) {
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
                order: [[0, 'desc']],
                dom:
                    "<'d-flex align-items-center'<'me-auto'l>fB>" +
                    "<'row'<'col-sm-12'tr>>" +
                    "<'d-flex align-items-center'<'me-auto'i>p>",
                buttons: [
                    {
                        extend: 'excel',
                        title: 'Boranga Community Threats Excel Export',
                        text: '<i class="fa-solid fa-download"></i> Excel',
                        className: 'btn btn-primary me-2 rounded',
                        exportOptions: {
                            orthogonal: 'export',
                        },
                    },
                    {
                        extend: 'csv',
                        title: 'Boranga Community Threats CSV Export',
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
                                    'C' +
                                    full.community +
                                    ' - ' +
                                    full.threat_number
                                );
                            } else {
                                return (
                                    '<s>C' +
                                    full.community +
                                    ' - ' +
                                    full.threat_number +
                                    '</s>'
                                );
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
                            if (vm.is_internal) {
                                links += `<a href='#${full.id}' data-view-threat='${full.id}'>View</a><br/>`;
                                if (!vm.is_readonly) {
                                    if (full.visible) {
                                        links += `<a href='#${full.id}' data-edit-threat='${full.id}'>Edit</a><br/>`;
                                        links += `<a href='#' data-discard-threat='${full.id}'>Discard</a><br>`;
                                    } else {
                                        links += `<a href='#' data-reinstate-threat='${full.id}'>Reinstate</a><br>`;
                                    }
                                }
                                links += `<a href='#' data-history-threat='${full.id}'>History</a><br>`;
                            } else {
                                links += `<a href='#${full.id}' data-view-threat='${full.id}'>View</a><br/>`;
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
                    // to fix the responsive table overflow css on tab switch
                    // vm.$refs.documents_datatable.vmDataTable.draw('page');
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
        showOccurrenceThreats: function () {
            if (this.is_internal) {
                return true;
            }
            const publishingStatus =
                this.species_community &&
                this.species_community.publishing_status;
            if (!publishingStatus) {
                return false;
            }
            return (
                publishingStatus.community_public &&
                publishingStatus.threats_public
            );
        },
    },
    watch: {
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
        newThreat: function () {
            let vm = this;
            this.$refs.threat_detail.threat_id = '';
            //-----for adding new community threat
            var new_community_threat_another = {
                community: vm.species_community.id,
                source: vm.species_community.id,
                threat_category: '',
                threat_agent: '',
                comment: '',
                current_impact: '',
                potential_impact: '',
                potential_threat_onset: '',
                date_observed: null,
            };
            this.$refs.threat_detail.threatObj = new_community_threat_another;
            this.$refs.threat_detail.threat_action = 'add';
            this.$refs.threat_detail.isModalOpen = true;
        },
        editThreat: function (id) {
            this.$refs.threat_detail.threat_id = id;
            this.$refs.threat_detail.threat_action = 'edit';
            fetch(helpers.add_endpoint_json(api_endpoints.threat, id)).then(
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
        viewThreat: function (id) {
            this.$refs.threat_detail.threat_id = id;
            this.$refs.threat_detail.threat_action = 'view';
            fetch(helpers.add_endpoint_json(api_endpoints.threat, id)).then(
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
            this.conservationThreatHistoryId = parseInt(id);
            this.uuid++;
            this.$nextTick(() => {
                this.$refs.conservation_threat_history.isModalOpen = true;
            });
        },
        refreshSpeciesCommunity: function () {
            let vm = this;
            vm.$parent.refreshSpeciesCommunity();
        },
        discardThreat: function (id) {
            let vm = this;
            let public_message = '';
            if (
                vm.species_community.publishing_status.community_public &&
                vm.species_community.publishing_status.threats_public
            ) {
                public_message =
                    ' Doing so will make the Species Record Private';
            }
            swal.fire({
                title: 'Discard Threat',
                text:
                    'Are you sure you want to discard this Threat?' +
                    public_message,
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Discard Threat',
                customClass: {
                    confirmButton: 'btn btn-primary',
                    cancelButton: 'btn btn-secondary',
                },
                reverseButtons: true,
            }).then((result) => {
                if (result.isConfirmed) {
                    fetch(
                        helpers.add_endpoint_json(
                            api_endpoints.threat,
                            id + '/discard'
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
                                text: 'Your threat has been discarded',
                                icon: 'success',
                                customClass: {
                                    confirmButton: 'btn btn-primary',
                                },
                            });
                            vm.$refs.threats_datatable.vmDataTable.ajax.reload();
                            vm.refreshSpeciesCommunity();
                        },
                        (error) => {
                            console.log(error);
                        }
                    );
                }
            });
        },
        reinstateThreat: function (id) {
            let vm = this;
            let public_message = '';
            if (
                vm.species_community.publishing_status.community_public &&
                vm.species_community.publishing_status.threats_public
            ) {
                public_message =
                    ' Doing so will make the Species Record Private';
            }
            swal.fire({
                title: 'Reinstate Threat',
                text:
                    'Are you sure you want to Reinstate this Threat?' +
                    public_message,
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Reinstate Threat',
                customClass: {
                    confirmButton: 'btn btn-primary',
                    cancelButton: 'btn btn-secondary',
                },
                reverseButtons: true,
            }).then((result) => {
                if (result.isConfirmed) {
                    fetch(
                        helpers.add_endpoint_json(
                            api_endpoints.threat,
                            id + '/reinstate'
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
                                text: 'Your threat has been reinstated',
                                icon: 'success',
                                customClass: {
                                    confirmButton: 'btn btn-primary',
                                },
                            });
                            vm.$refs.threats_datatable.vmDataTable.ajax.reload();
                            vm.refreshSpeciesCommunity();
                        },
                        (error) => {
                            console.log(error);
                        }
                    );
                }
            });
        },
        updatedThreats() {
            this.$refs.threats_datatable.vmDataTable.ajax.reload();
            this.refreshSpeciesCommunity();
        },
        addEventListeners: function () {
            let vm = this;
            vm.$refs.threats_datatable.vmDataTable.on(
                'click',
                'a[data-edit-threat]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-edit-threat');
                    vm.editThreat(id);
                }
            );
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
            // External Discard listener
            vm.$refs.threats_datatable.vmDataTable.on(
                'click',
                'a[data-discard-threat]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-discard-threat');
                    vm.discardThreat(id);
                }
            );
            // External Reinstate listener
            vm.$refs.threats_datatable.vmDataTable.on(
                'click',
                'a[data-reinstate-threat]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-reinstate-threat');
                    vm.reinstateThreat(id);
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
