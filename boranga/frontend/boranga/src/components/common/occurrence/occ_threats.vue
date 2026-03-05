<template lang="html">
    <div id="occ_threats">
        <FormSection :form-collapse="false" label="Threats" :Index="threatBody">
            <CollapsibleFilters
                ref="collapsible_filters"
                component_title="Filters"
                class="mb-2"
                :show-warning-icon="filterApplied"
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
            <form class="form-horizontal" action="index.html" method="post">
                <div class="col-sm-12">
                    <div class="text-end">
                        <button
                            :disabled="isReadOnly"
                            type="button"
                            class="btn btn-primary mb-2"
                            @click.prevent="newThreat"
                        >
                            <i class="bi bi-plus-circle"></i>
                            Add New Threat
                        </button>
                        &nbsp;
                        <button
                            :disabled="isReadOnly"
                            type="button"
                            class="btn btn-primary mb-2"
                            @click.prevent="existingThreat"
                        >
                            <i class="bi bi-plus-circle"></i>
                            Add Threat from
                            {{ ocr_model_prefix }}
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
        <ThreatDetail
            ref="threat_detail"
            :url="occ_threat_url"
            @refresh-from-response="refreshFromResponse"
        >
        </ThreatDetail>
        <div v-if="showExisting">
            <ExistingThreat
                ref="existing_threats"
                :occurrence-id="occurrence_obj.id"
            />
        </div>
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
import ExistingThreat from '@/components/common/occurrence/occ_ocr_existing_threats.vue';
import FormSection from '@/components/forms/section_toggle.vue';
import ConservationThreatHistory from '../../internal/occurrence/occ_conservation_threat_history.vue';
import CollapsibleFilters from '@/components/forms/collapsible_component.vue';
import { constants, api_endpoints, helpers } from '@/utils/hooks';

export default {
    name: 'OCCThreats',
    components: {
        FormSection,
        datatable,
        ThreatDetail,
        ConservationThreatHistory,
        ExistingThreat,
        CollapsibleFilters,
    },
    props: {
        occurrence_obj: {
            type: Object,
            required: true,
        },
        // this prop is only send from split species form to make the original species readonly
        is_readonly: {
            type: Boolean,
            default: false,
        },
    },
    data: function () {
        let vm = this;
        return {
            uuid: 0,
            occConservationThreatHistoryId: null,
            showExisting: false,
            threatBody: 'threatBody' + uuid(),
            panelBody: 'species-threats-' + uuid(),
            values: null,
            occ_threat_url: api_endpoints.occ_threat,

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
                { id: 'removed', name: 'Discarded' },
            ],

            threats_headers: [
                'Number',
                'Original Report',
                'Category',
                'Date Observed',
                'Threat Agent',
                'Comments',
                'Threat Source',
                'Current Impact',
                'Potential Impact',
                'Action',
            ],
            threats_options: {
                autowidth: false,
                language: {
                    processing: constants.DATATABLE_PROCESSING_HTML,
                },
                responsive: true,
                serverSide: false,
                searching: true,
                //  to show the "workflow Status","Action" columns always in the last position
                columnDefs: [
                    { responsivePriority: 1, targets: 0 },
                    { responsivePriority: 2, targets: -1 },
                ],
                ajax: {
                    url: helpers.add_endpoint_json(
                        api_endpoints.occurrence,
                        vm.occurrence_obj.id + '/threats'
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
                order: [[0, 'desc']],
                dom:
                    "<'d-flex align-items-center'<'me-auto'l>fB>" +
                    "<'row'<'col-sm-12'tr>>" +
                    "<'d-flex align-items-center'<'me-auto'i>p>",
                buttons: [
                    {
                        extend: 'excel',
                        title: 'Boranga OCC Threats Excel Export',
                        text: '<i class="bi bi-download"></i> Excel',
                        className: 'btn btn-primary me-2 rounded',
                        exportOptions: {
                            orthogonal: 'export',
                        },
                    },
                    {
                        extend: 'csv',
                        title: 'Boranga OCC Threats CSV Export',
                        text: '<i class="bi bi-download"></i> CSV',
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
                            if (full.original_report != null) {
                                if (full.visible) {
                                    return (
                                        full.original_report +
                                        ' - ' +
                                        full.original_threat
                                    );
                                } else {
                                    return (
                                        '<s>' +
                                        full.original_report +
                                        ' - ' +
                                        full.original_threat +
                                        '</s>'
                                    );
                                }
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
                        data: 'id',
                        mRender: function (data, type, full) {
                            let links = '';
                            if (full.visible) {
                                links += `<a href='#${full.id}' data-view-threat='${full.id}'>View</a><br/>`;
                                if (!vm.isReadOnly) {
                                    links += `<a href='#${full.id}' data-edit-threat='${full.id}'>Edit</a><br/>`;
                                    links += `<a href='#' data-discard-threat='${full.id}'>Discard</a><br>`;
                                }
                            } else {
                                links += `<a href='#' data-reinstate-threat='${full.id}'>Reinstate</a><br>`;
                            }
                            links += `<a href='#' data-history-threat='${full.id}'>History</a><br>`;
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
            ocr_model_prefix: constants.MODELS.OCCURRENCE_REPORT.MODEL_PREFIX,
        };
    },
    computed: {
        isReadOnly: function () {
            return !this.occurrence_obj.can_user_edit;
        },
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
                    api_endpoints.occurrence,
                    vm.occurrence_obj.id + '/threat_source_list'
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
        newThreat: function () {
            let vm = this;
            this.$refs.threat_detail.threat_id = '';
            //----for adding new species Threat
            var new_occ_threat = {
                occurrence: vm.occurrence_obj.id,
                threat_category: '',
                threat_agent: '',
                comment: '',
                source: vm.occurrence_obj.occurrence_number,
                current_impact: '',
                potential_impact: '',
                potential_threat_onset: '',
                date_observed: null,
            };
            this.$refs.threat_detail.threatObj = new_occ_threat;
            this.$refs.threat_detail.threat_action = 'add';
            this.$refs.threat_detail.isModalOpen = true;
        },
        existingThreat: function () {
            this.showExisting = true;
            this.uuid++;
            this.$nextTick(() => {
                this.$refs.existing_threats.isModalOpen = true;
            });
        },
        editThreat: function (id) {
            this.$refs.threat_detail.threat_id = id;
            this.$refs.threat_detail.threat_action = 'edit';
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
        discardThreat: function (id) {
            let vm = this;
            swal.fire({
                title: 'Discard Threat',
                text: 'Are you sure you want to discard this Threat?',
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
                            api_endpoints.occ_threat,
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
            swal.fire({
                title: 'Reinstate Threat',
                text: 'Are you sure you want to Reinstate this Threat?',
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Reinstate Threat',
                customClass: {
                    confirmButton: 'btn btn-primary',
                },
            }).then((result) => {
                if (result.isConfirmed) {
                    fetch(
                        helpers.add_endpoint_json(
                            api_endpoints.occ_threat,
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
