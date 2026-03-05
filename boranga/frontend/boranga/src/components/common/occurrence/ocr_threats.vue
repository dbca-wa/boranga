<template lang="html">
    <div id="ocr_threats">
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
        <ThreatDetail
            ref="threat_detail"
            :url="ocr_threat_url"
            :date_observed_maximum="
                occurrence_report_obj.observation_date
                    ? occurrence_report_obj.observation_date.slice(0, 10)
                    : null
            "
            @refresh-from-response="refreshFromResponse"
        >
        </ThreatDetail>
        <div v-if="ocrConservationThreatHistoryId">
            <ConservationThreatHistory
                ref="ocr_conservation_threat_history"
                :key="ocrConservationThreatHistoryId"
                :threat-id="ocrConservationThreatHistoryId"
            />
        </div>
    </div>
</template>
<script>
import { v4 as uuid } from 'uuid';
import datatable from '@vue-utils/datatable.vue';
import ThreatDetail from '@/components/common/species_communities/add_threat.vue';
import FormSection from '@/components/forms/section_toggle.vue';
import ConservationThreatHistory from '../../internal/occurrence/ocr_conservation_threat_history.vue';
import CollapsibleFilters from '@/components/forms/collapsible_component.vue';
import { constants, api_endpoints, helpers } from '@/utils/hooks';

export default {
    name: 'OCRThreats',
    components: {
        FormSection,
        datatable,
        ThreatDetail,
        ConservationThreatHistory,
        CollapsibleFilters,
    },
    props: {
        occurrence_report_obj: {
            type: Object,
            required: true,
        },
        // this prop is only send from split species form to make the original species readonly
        is_readonly: {
            type: Boolean,
            default: false,
        },
        is_external: {
            type: Boolean,
            default: false,
        },
    },
    data: function () {
        let vm = this;
        return {
            uuid: 0,
            ocrConservationThreatHistoryId: null,
            threatBody: 'threatBody' + uuid(),
            panelBody: 'species-threats-' + uuid(),
            values: null,
            ocr_threat_url: api_endpoints.ocr_threat,

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
                searching: true,
                //  to show the "workflow Status","Action" columns always in the last position
                columnDefs: [
                    { responsivePriority: 1, targets: 0 },
                    { responsivePriority: 2, targets: -1 },
                ],
                ajax: {
                    url: helpers.add_endpoint_json(
                        api_endpoints.occurrence_report,
                        vm.occurrence_report_obj.id + '/threats'
                    ),
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
                        title: 'Boranga Occurrence Report Threats Excel Export',
                        text: '<i class="bi bi-download"></i> Excel',
                        className: 'btn btn-primary me-2 rounded',
                        exportOptions: {
                            orthogonal: 'export',
                        },
                    },
                    {
                        extend: 'csv',
                        title: 'Boranga Occurrence Report Threats CSV Export',
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
                                    constants.MODELS.OCCURRENCE_REPORT
                                        .MODEL_PREFIX +
                                    full.occurrence_report +
                                    ' - ' +
                                    full.threat_number
                                );
                            } else {
                                return (
                                    '<s>' +
                                    constants.MODELS.OCCURRENCE_REPORT
                                        .MODEL_PREFIX +
                                    full.occurrence_report +
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
                            if (!vm.isReadOnly) {
                                if (full.visible) {
                                    links += `<a href='#${full.id}' data-view-threat='${full.id}'>View</a><br/>`;
                                    links += `<a href='#${full.id}' data-edit-threat='${full.id}'>Edit</a><br/>`;
                                    links += `<a href='#' data-discard-threat='${full.id}'>Discard</a><br>`;
                                } else {
                                    links += `<a href='#' data-reinstate-threat='${full.id}'>Reinstate</a><br>`;
                                }
                            } else {
                                links += `<a href='#${full.id}' data-view-threat='${full.id}'>View</a><br/>`;
                            }
                            if (!vm.is_external) {
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
                    vm.$nextTick(() => {
                        vm.adjust_table_width();
                    });
                },
            },
        };
    },
    computed: {
        isReadOnly: function () {
            //override for split reports
            if (this.is_readonly) {
                return this.is_readonly;
            }
            return this.occurrence_report_obj.readonly;
        },
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
            //----for adding new species Threat
            var new_ocr_threat = {
                occurrence_report: vm.occurrence_report_obj.id,
                threat_category: '',
                threat_agent: '',
                comment: '',
                source: vm.occurrence_report_obj.occurrence_report_number,
                current_impact: '',
                potential_impact: '',
                potential_threat_onset: '',
                date_observed: null,
            };
            this.$refs.threat_detail.threatObj = new_ocr_threat;
            this.$refs.threat_detail.threat_action = 'add';
            this.$refs.threat_detail.isModalOpen = true;
        },
        editThreat: function (id) {
            this.$refs.threat_detail.threat_id = id;
            this.$refs.threat_detail.threat_action = 'edit';
            fetch(helpers.add_endpoint_json(api_endpoints.ocr_threat, id)).then(
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
            fetch(helpers.add_endpoint_json(api_endpoints.ocr_threat, id)).then(
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
            this.ocrConservationThreatHistoryId = parseInt(id);
            this.uuid++;
            this.$nextTick(() => {
                this.$refs.ocr_conservation_threat_history.isModalOpen = true;
            });
        },
        discardThreat: function (id) {
            let vm = this;
            swal.fire({
                title: 'Remove Threat',
                text: 'Are you sure you want to remove this Threat?',
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Remove Threat',
                customClass: {
                    confirmButton: 'btn btn-primary',
                    cancelButton: 'btn btn-secondary me-2',
                },
                reverseButtons: true,
            }).then((result) => {
                if (result.isConfirmed) {
                    fetch(
                        helpers.add_endpoint_json(
                            api_endpoints.ocr_threat,
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
                            }).then(() => {
                                vm.$refs.threats_datatable.vmDataTable.ajax.reload();
                                if (
                                    vm.occurrence_report_obj
                                        .processing_status == 'Unlocked'
                                ) {
                                    vm.$router.go();
                                }
                            });
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
                            api_endpoints.ocr_threat,
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
                            }).then(() => {
                                vm.$refs.threats_datatable.vmDataTable.ajax.reload();
                                if (
                                    vm.occurrence_report_obj
                                        .processing_status == 'Unlocked'
                                ) {
                                    vm.$router.go();
                                }
                            });
                        },
                        (error) => {
                            console.log(error);
                        }
                    );
                }
            });
        },
        updatedThreats() {
            let vm = this;
            this.$refs.threats_datatable.vmDataTable.ajax.reload();
            if (vm.occurrence_report_obj.processing_status == 'Unlocked') {
                vm.$router.go();
            }
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
