<template id="communities_ocr_dashboard">
    <div>
        <CollapsibleFilters
            ref="collapsible_filters"
            component_title="Filters"
            class="mb-2"
            :show-warning-icon="filterApplied"
        >
            <div class="row">
                <div class="col-md-3">
                    <div id="select_occurrence" class="form-group">
                        <label for="ocr_occurrence_lookup">Occurrence:</label>
                        <select
                            id="ocr_occurrence_lookup"
                            ref="ocr_occurrence_lookup"
                            name="ocr_occurrence_lookup"
                            class="form-control"
                        />
                    </div>
                </div>
                <div class="col-md-3">
                    <div id="select_ocr_community_name" class="form-group">
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
                    <div id="select_status" class="form-group">
                        <label for="ocr_status_lookup">Status:</label>
                        <select
                            v-model="filterOCRCommunityStatus"
                            class="form-select"
                        >
                            <option value="all">All</option>
                            <option
                                v-for="status in processing_statuses"
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
                        <label for="">Observation Date Range:</label>
                        <input
                            id="observation_from_date"
                            v-model="filterOCRCommunityObservationFromDate"
                            type="date"
                            class="form-control"
                            placeholder="DD/MM/YYYY"
                        />
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for=""></label>
                        <input
                            id="observation_from_date"
                            v-model="filterOCRCommunityObservationToDate"
                            type="date"
                            class="form-control"
                            placeholder="DD/MM/YYYY"
                        />
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="">Submitted Date Range:</label>
                        <input
                            id="submitted_from_date"
                            v-model="filterOCRCommunitySubmittedFromDate"
                            type="date"
                            class="form-control"
                            placeholder="DD/MM/YYYY"
                        />
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for=""></label>
                        <input
                            id="submitted_from_date"
                            v-model="filterOCRCommunitySubmittedToDate"
                            type="date"
                            class="form-control"
                            placeholder="DD/MM/YYYY"
                        />
                    </div>
                </div>
            </div>
            <div class="row">
                <div class="col-md-3">
                    <div id="select_assessor" class="form-group">
                        <label for="ocr_assessor_lookup">Assessor:</label>
                        <select
                            id="ocr_assessor_lookup"
                            ref="ocr_assessor_lookup"
                            name="ocr_assessor_lookup"
                            class="form-control"
                        />
                    </div>
                </div>
                <div class="col-md-3">
                    <div id="select_submitter" class="form-group">
                        <label for="ocr_submitter_lookup">Submitter:</label>
                        <select
                            id="ocr_submitter_lookup"
                            ref="ocr_submitter_lookup"
                            name="ocr_submitter_lookup"
                            class="form-control"
                        />
                    </div>
                </div>
            </div>
        </CollapsibleFilters>

        <div v-if="addCommunityOCRVisibility" class="col-md-12">
            <div class="text-end">
                <a
                    type="button"
                    role="button"
                    class="btn btn-primary mb-2 me-2"
                    :href="`/internal/occurrence-report/bulk_import/?group_type=community`"
                    ><i class="bi bi-download pe-2"></i>Bulk Import</a
                >
                <button
                    type="button"
                    class="btn btn-primary mb-2"
                    @click.prevent="createCommunityOccurrenceReport"
                >
                    <i class="fa-solid fa-circle-plus"></i> Add Community
                    Occurrence Report
                </button>
            </div>
        </div>

        <div class="row">
            <div class="col-lg-12">
                <datatable
                    :id="datatable_id"
                    ref="community_ocr_datatable"
                    :dt-options="datatable_options"
                    :dt-headers="datatable_headers"
                />
            </div>
            <div v-if="occurrenceReportHistoryId">
                <OccurrenceReportHistory
                    ref="occurrence_report_history"
                    :key="occurrenceReportHistoryId"
                    :occurrence-report-id="occurrenceReportHistoryId"
                />
            </div>
        </div>
    </div>
</template>
<script>
import { v4 as uuid } from 'uuid';
import datatable from '@/utils/vue/datatable.vue';
import CollapsibleFilters from '@/components/forms/collapsible_component.vue';
import OccurrenceReportHistory from '../internal/occurrence/community_occurrence_report_history.vue';

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
        filterOCRCommunityOccurrence_cache: {
            type: String,
            required: false,
            default: 'filterOCRCommunityOccurrence',
        },
        filterOCRCommunityName_cache: {
            type: String,
            required: false,
            default: 'filterOCRCommunityName',
        },
        filterOCRCommunityMigratedId_cache: {
            type: String,
            required: false,
            default: 'filterOCRCommunityMigratedId',
        },
        filterOCRCommunityStatus_cache: {
            type: String,
            required: false,
            default: 'filterOCRCommunityStatus',
        },
        filterOCRCommunityObservationFromDate_cache: {
            type: String,
            required: false,
            default: 'filterOCRCommunityObservationFromDate',
        },
        filterOCRCommunityObservationToDate_cache: {
            type: String,
            required: false,
            default: 'filterOCRCommunityObservationToDate',
        },
        filterOCRCommunitySubmittedFromDate_cache: {
            type: String,
            required: false,
            default: 'filterOCRCommunitySubmittedFromDate',
        },
        filterOCRCommunitySubmittedToDate_cache: {
            type: String,
            required: false,
            default: 'filterOCRCommunitySubmittedToDate',
        },
        filterOCRFromCommunityDueDate_cache: {
            type: String,
            required: false,
            default: 'filterOCRFromCommunityDueDate',
        },
        filterOCRToCommunityDueDate_cache: {
            type: String,
            required: false,
            default: 'filterOCRToCommunityDueDate',
        },
        filterOCRCommunityAssessor_cache: {
            type: String,
            required: false,
            default: 'filterOCRCommunityAssessor',
        },
        filterOCRCommunitySubmitter_cache: {
            type: String,
            required: false,
            default: 'filterOCRCommunitySubmitter',
        },
    },
    data() {
        return {
            uuid: 0,
            occurrenceReportHistoryId: null,
            datatable_id: 'occurrence-report-community-datatable-' + uuid(),

            // selected values for filtering
            filterOCRCommunityOccurrence: sessionStorage.getItem(
                this.filterOCRCommunityOccurrence_cache
            )
                ? sessionStorage.getItem(
                      this.filterOCRCommunityOccurrence_cache
                  )
                : 'all',

            filterOCRCommunityName: sessionStorage.getItem(
                this.filterOCRCommunityName_cache
            )
                ? sessionStorage.getItem(this.filterOCRCommunityName_cache)
                : 'all',

            filterOCRCommunityMigratedId: sessionStorage.getItem(
                this.filterOCRCommunityMigratedId_cache
            )
                ? sessionStorage.getItem(
                      this.filterOCRCommunityMigratedId_cache
                  )
                : 'all',

            filterOCRCommunityStatus: sessionStorage.getItem(
                this.filterOCRCommunityStatus_cache
            )
                ? sessionStorage.getItem(this.filterOCRCommunityStatus_cache)
                : 'all',

            filterOCRCommunityObservationFromDate: sessionStorage.getItem(
                this.filterOCRCommunityObservationFromDate_cache
            )
                ? sessionStorage.getItem(
                      this.filterOCRCommunityObservationFromDate_cache
                  )
                : '',

            filterOCRCommunityObservationToDate: sessionStorage.getItem(
                this.filterOCRCommunityObservationToDate_cache
            )
                ? sessionStorage.getItem(
                      this.filterOCRCommunityObservationToDate_cache
                  )
                : '',

            filterOCRCommunitySubmittedFromDate: sessionStorage.getItem(
                this.filterOCRCommunitySubmittedFromDate_cache
            )
                ? sessionStorage.getItem(
                      this.filterOCRCommunitySubmittedFromDate_cache
                  )
                : '',

            filterOCRCommunitySubmittedToDate: sessionStorage.getItem(
                this.filterOCRCommunitySubmittedToDate_cache
            )
                ? sessionStorage.getItem(
                      this.filterOCRCommunitySubmittedToDate_cache
                  )
                : '',

            filterOCRFromCommunityDueDate: sessionStorage.getItem(
                this.filterOCRFromCommunityDueDate_cache
            )
                ? sessionStorage.getItem(
                      this.filterOCRFromCommunityDueDate_cache
                  )
                : '',
            filterOCRToCommunityDueDate: sessionStorage.getItem(
                this.filterOCRToCommunityDueDate_cache
            )
                ? sessionStorage.getItem(this.filterOCRToCommunityDueDate_cache)
                : '',

            filterOCRCommunityAssessor: sessionStorage.getItem(
                this.filterOCRCommunityAssessor_cache
            )
                ? sessionStorage.getItem(this.filterOCRCommunityAssessor_cache)
                : 'all',

            filterOCRCommunitySubmitter: sessionStorage.getItem(
                this.filterOCRCommunitySubmitter_cache
            )
                ? sessionStorage.getItem(this.filterOCRCommunitySubmitter_cache)
                : 'all',

            filterListsCommunity: {},
            occurrence_list: [],
            community_name_list: [],
            status_list: [],
            submissions_from_list: [],
            submissions_to_list: [],

            // filtering options
            processing_statuses: [
                { value: 'draft', name: 'Draft' },
                { value: 'discarded', name: 'Discarded' },
                { value: 'with_assessor', name: 'With Assessor' },
                { value: 'with_referral', name: 'With Referral' },
                { value: 'with_approver', name: 'With Approver' },
                { value: 'approved', name: 'Approved' },
                { value: 'declined', name: 'Declined' },
            ],
        };
    },
    computed: {
        filterApplied: function () {
            if (
                this.filterOCRCommunityOccurrence === 'all' &&
                this.filterOCRCommunityName === 'all' &&
                this.filterOCRCommunityMigratedId === 'all' &&
                this.filterOCRCommunityStatus === 'all' &&
                this.filterOCRCommunityObservationFromDate === '' &&
                this.filterOCRCommunityObservationToDate === '' &&
                this.filterOCRCommunitySubmittedFromDate === '' &&
                this.filterOCRCommunitySubmittedToDate === '' &&
                this.filterOCRFromCommunityDueDate === '' &&
                this.filterOCRToCommunityDueDate === '' &&
                this.filterOCRCommunityAssessor === 'all' &&
                this.filterOCRCommunitySubmitter === 'all'
            ) {
                return false;
            } else {
                return true;
            }
        },
        addCommunityOCRVisibility: function () {
            return (
                this.profile?.user &&
                this.profile.user.groups.includes(
                    constants.GROUPS.INTERNAL_CONTRIBUTORS
                )
            );
        },
        datatable_headers: function () {
            return [
                'ID',
                'Number',
                'Occurrence',
                'Community Name',
                'Community ID',
                'Observation Date',
                'Main Observer',
                'Migrated From ID',
                'Submitted on',
                'Submitter',
                'Assessor',
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
                render: function (data, type, full) {
                    let value = full.occurrence_report_number;
                    if (full.is_new_contributor) {
                        value +=
                            ' <span class="badge bg-warning">New Contributor</span>';
                    }
                    return value;
                },
                name: 'id',
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
                    return 'NOT SET';
                },
                name: 'occurrence__occurrence_number',
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
        column_observation_date_time: function () {
            return {
                data: 'observation_date',
                orderable: true,
                searchable: true,
                visible: true,
                name: 'observation_date',
            };
        },
        column_main_observer: function () {
            return {
                data: 'main_observer',
                orderable: false,
                searchable: false,
                visible: true,
                name: 'main_observer',
                render: function (data, type, full) {
                    return full.main_observer;
                },
            };
        },
        column_migrated_from_id: function () {
            return {
                data: 'migrated_from_id',
                orderable: false,
                searchable: true,
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
        column_submitter: function () {
            return {
                data: 'submitter',
                orderable: false,
                searchable: false,
                visible: true,
                name: 'submitter__first_name, submitter__last_name',
            };
        },
        column_assessor: function () {
            return {
                data: 'assessor',
                orderable: false,
                searchable: false,
                visible: true,
                name: 'assessor__first_name, assessor__last_name',
            };
        },
        column_status: function () {
            return {
                data: 'processing_status_display',
                orderable: true,
                searchable: true,
                visible: true,
                name: 'processing_status',
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
                    if (full.internal_user_edit) {
                        if (full.processing_status == 'discarded') {
                            links += `<a href='#${full.id}' data-reinstate-ocr-proposal='${full.id}'>Reinstate</a><br/>`;
                        } else {
                            links += `<a href='/internal/occurrence-report/${full.id}?action=edit'>Continue</a><br/>`;
                            links += `<a href='#${full.id}' data-discard-ocr-proposal='${full.id}'>Discard</a><br/>`;
                            links += `<a href='#' data-history-occurrence-report='${full.id}'>History</a><br>`;
                        }
                    } else {
                        if (full.can_user_assess || full.can_user_approve) {
                            links += `<a href='/internal/occurrence-report/${full.id}?action=edit'>Process</a><br/>`;
                        } else {
                            links += `<a href='/internal/occurrence-report/${full.id}?action=view'>View</a><br/>`;
                        }
                        links += `<a href='#' data-history-occurrence-report='${full.id}'>History</a><br>`;
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
                    title: `Boranga ${constants.MODELS.OCCURRENCE_REPORT.MODEL_PREFIX} Communities Excel Export`,
                    text: '<i class="fa-solid fa-download"></i> Excel',
                    className: 'btn btn-primary me-2 rounded',
                    exportOptions: {
                        columns: ':not(.no-export)',
                        orthogonal: 'export',
                    },
                },
                {
                    extend: 'csv',
                    title: `Boranga ${constants.MODELS.OCCURRENCE_REPORT.MODEL_PREFIX} Communities CSV Export`,
                    text: '<i class="fa-solid fa-download"></i> CSV',
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
                vm.column_occurrence,
                vm.column_community_name,
                vm.column_community_common_id,
                vm.column_observation_date_time,
                vm.column_main_observer,
                vm.column_migrated_from_id,
                vm.column_lodgement_date,
                vm.column_submitter,
                vm.column_assessor,
                vm.column_status,
                vm.column_action,
            ];
            search = true;

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
                    data: function (d) {
                        d.filter_group_type = vm.group_type_name;
                        d.filter_occurrence = vm.filterOCRCommunityOccurrence;
                        d.filter_community_name = vm.filterOCRCommunityName;
                        d.filter_community_common_id =
                            vm.filterOCRCommunityMigratedId;
                        d.filter_status = vm.filterOCRCommunityStatus;
                        d.filter_observation_from_date =
                            vm.filterOCRCommunityObservationFromDate;
                        d.filter_observation_to_date =
                            vm.filterOCRCommunityObservationToDate;
                        d.filter_submitted_from_date =
                            vm.filterOCRCommunitySubmittedFromDate;
                        d.filter_submitted_to_date =
                            vm.filterOCRCommunitySubmittedToDate;
                        d.filter_from_due_date =
                            vm.filterOCRFromCommunityDueDate;
                        d.filter_to_due_date = vm.filterOCRToCommunityDueDate;
                        d.filter_assessor = vm.filterOCRCommunityAssessor;
                        d.filter_submitter = vm.filterOCRCommunitySubmitter;
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
        filterOCRCommunityOccurrence: function () {
            let vm = this;
            vm.$refs.community_ocr_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterOCRCommunityOccurrence_cache,
                vm.filterOCRCommunityOccurrence
            );
        },
        filterOCRCommunityName: function () {
            let vm = this;
            vm.$refs.community_ocr_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterOCRCommunityName_cache,
                vm.filterOCRCommunityName
            );
        },
        filterOCRCommunityMigratedId: function () {
            let vm = this;
            vm.$refs.community_ocr_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterOCRCommunityMigratedId_cache,
                vm.filterOCRCommunityMigratedId
            );
        },
        filterOCRCommunityStatus: function () {
            let vm = this;
            vm.$refs.community_ocr_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterOCRCommunityStatus_cache,
                vm.filterOCRCommunityStatus
            );
        },
        filterOCRCommunityObservationFromDate: function () {
            let vm = this;
            vm.$refs.community_ocr_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterOCRCommunityObservationFromDate_cache,
                vm.filterOCRCommunityObservationFromDate
            );
        },
        filterOCRCommunityObservationToDate: function () {
            let vm = this;
            vm.$refs.community_ocr_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterOCRCommunityObservationToDate_cache,
                vm.filterOCRCommunityObservationToDate
            );
        },
        filterOCRCommunitySubmittedFromDate: function () {
            let vm = this;
            vm.$refs.community_ocr_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterOCRCommunitySubmittedFromDate_cache,
                vm.filterOCRCommunitySubmittedFromDate
            );
        },
        filterOCRCommunitySubmittedToDate: function () {
            let vm = this;
            vm.$refs.community_ocr_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterOCRCommunitySubmittedToDate_cache,
                vm.filterOCRCommunitySubmittedToDate
            );
        },
        filterOCRFromCommunityDueDate: function () {
            let vm = this;
            vm.$refs.community_ocr_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterOCRFromCommunityDueDate_cache,
                vm.filterOCRFromCommunityDueDate
            );
        },
        filterOCRToCommunityDueDate: function () {
            let vm = this;
            vm.$refs.community_ocr_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterOCRToCommunityDueDate_cache,
                vm.filterOCRToCommunityDueDate
            );
        },
        filterOCRCommunityAssessor: function () {
            let vm = this;
            vm.$refs.community_ocr_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterOCRCommunityAssessor_cache,
                vm.filterOCRCommunityAssessor
            );
        },
        filterOCRCommunitySubmitter: function () {
            let vm = this;
            vm.$refs.community_ocr_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterOCRCommunitySubmitter_cache,
                vm.filterOCRCommunitySubmitter
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
            vm.initialiseOccurrenceLookup();
            vm.initialiseCommunityNameLookup();
            vm.initialiseCommunityIdLookup();
            vm.initialiseAssessorLookup();
            vm.initialiseSubmitterLookup();
            vm.addEventListeners();
            var newOption = null;
            if (
                sessionStorage.getItem('filterOCRCommunityOccurrence') !=
                    'all' &&
                sessionStorage.getItem('filterOCRCommunityOccurrence') != null
            ) {
                newOption = new Option(
                    sessionStorage.getItem('filterOCRCommunityOccurrenceText'),
                    vm.filterOCRCommunityOccurrence,
                    false,
                    true
                );
                $('#ocr_occurrence_lookup').append(newOption);
            }
            if (
                sessionStorage.getItem('filterOCRCommunityName') != 'all' &&
                sessionStorage.getItem('filterOCRCommunityName') != null
            ) {
                newOption = new Option(
                    sessionStorage.getItem('filterOCRCommunityNameText'),
                    vm.filterOCRCommunityName,
                    false,
                    true
                );
                $('#ocr_community_name_lookup').append(newOption);
            }
            if (
                sessionStorage.getItem('filterOCRCommunityMigratedId') !=
                    'all' &&
                sessionStorage.getItem('filterOCRCommunityMigratedId') != null
            ) {
                // contructor new Option(text, value, defaultSelected, selected)
                newOption = new Option(
                    sessionStorage.getItem('filterOCRCommunityMigratedIdText'),
                    vm.filterOCRCommunityMigratedId,
                    false,
                    true
                );
                $('#community_id_lookup').append(newOption);
            }
            if (
                sessionStorage.getItem('filterOCRCommunityAssessor') != 'all' &&
                sessionStorage.getItem('filterOCRCommunityAssessor') != null
            ) {
                newOption = new Option(
                    sessionStorage.getItem('filterOCRCommunityAssessorText'),
                    vm.filterOCRCommunityAssessor,
                    false,
                    true
                );
                $('#ocr_assessor_lookup').append(newOption);
            }
            if (
                sessionStorage.getItem('filterOCRCommunitySubmitter') !=
                    'all' &&
                sessionStorage.getItem('filterOCRCommunitySubmitter') != null
            ) {
                newOption = new Option(
                    sessionStorage.getItem('filterOCRCommunitySubmitterText'),
                    vm.filterOCRCommunitySubmitter,
                    false,
                    true
                );
                $('#ocr_submitter_lookup').append(newOption);
            }
        });
    },
    methods: {
        historyDocument: function (id) {
            this.occurrenceReportHistoryId = parseInt(id);
            this.uuid++;
            this.$nextTick(() => {
                this.$refs.occurrence_report_history.isModalOpen = true;
            });
        },
        initialiseOccurrenceLookup: function () {
            let vm = this;
            $(vm.$refs.ocr_occurrence_lookup)
                .select2({
                    minimumInputLength: 2,
                    dropdownParent: $('#select_occurrence'),
                    theme: 'bootstrap-5',
                    allowClear: true,
                    placeholder: 'Select Occurrence',
                    ajax: {
                        url: api_endpoints.community_lookup,
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
                    vm.filterOCRCommunityOccurrence = data;
                    sessionStorage.setItem(
                        'filterOCRCommunityOccurrenceText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.filterOCRCommunityOccurrence = 'all';
                    sessionStorage.setItem(
                        'filterOCRCommunityOccurrenceText',
                        ''
                    );
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-ocr_occurrence_lookup-results"]'
                    );
                    searchField[0].focus();
                });
        },
        initialiseCommunityNameLookup: function () {
            let vm = this;
            $(vm.$refs.ocr_community_name_lookup)
                .select2({
                    minimumInputLength: 2,
                    dropdownParent: $('#select_ocr_community_name'),
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
                    vm.filterOCRCommunityName = data;
                    sessionStorage.setItem(
                        'filterOCRCommunityNameText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.filterOCRCommunityName = 'all';
                    sessionStorage.setItem('filterOCRCommunityNameText', '');
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-ocr_community_name_lookup-results"]'
                    );
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
                    vm.filterOCRCommunityMigratedId = data;
                    sessionStorage.setItem(
                        'filterOCRCommunityMigratedIdText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.filterOCRCommunityMigratedId = 'all';
                    sessionStorage.setItem(
                        'filterOCRCommunityMigratedIdText',
                        ''
                    );
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-community_id_lookup-results"]'
                    );
                    // move focus to select2 field
                    searchField[0].focus();
                });
        },
        initialiseAssessorLookup: function () {
            let vm = this;
            $(vm.$refs.ocr_assessor_lookup)
                .select2({
                    minimumInputLength: 2,
                    theme: 'bootstrap-5',
                    allowClear: true,
                    placeholder: 'Search for Assessor',
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
                    vm.filterOCRCommunityAssessor = data;
                    sessionStorage.setItem(
                        'filterOCRCommunityAssessorText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.filterOCRCommunityAssessor = 'all';
                    sessionStorage.setItem(
                        'filterOCRCommunityAssessorText',
                        ''
                    );
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-ocr_assessor_lookup-results"]'
                    );
                    searchField[0].focus();
                });
        },
        initialiseSubmitterLookup: function () {
            let vm = this;
            $(vm.$refs.ocr_submitter_lookup)
                .select2({
                    minimumInputLength: 2,
                    theme: 'bootstrap-5',
                    allowClear: true,
                    placeholder: 'Search for Submitter',
                    ajax: {
                        url: api_endpoints.users_api + '/get_users_ledger_id/',
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
                    vm.filterOCRCommunitySubmitter = data;
                    sessionStorage.setItem(
                        'filterOCRCommunitySubmitterText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.selected_referral = null;
                    vm.filterOCRCommunitySubmitter = 'all';
                    sessionStorage.setItem(
                        'filterOCRCommunitySubmitterText',
                        ''
                    );
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-ocr_submitter_lookup-results"]'
                    );
                    searchField[0].focus();
                });
        },
        fetchFilterLists: function () {
            let vm = this;
            //large FilterList of Community Values object
            fetch(api_endpoints.community_filter_dict).then(
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
                },
                (error) => {
                    console.log(error);
                }
            );
        },
        createCommunityOccurrenceReport: async function () {
            swal.fire({
                title: `Add ${this.group_type_name} Occurrence Report`,
                text: 'Are you sure you want to add a new occurrence report?',
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Add Occurrence Report',
                reverseButtons: true,
                customClass: {
                    confirmButton: 'btn btn-primary',
                    cancelButton: 'btn btn-secondary',
                },
            }).then(async (swalresult) => {
                if (swalresult.isConfirmed) {
                    let newCommunityOCRId = null;
                    try {
                        const createUrl = api_endpoints.occurrence_report + '/';
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
                            newCommunityOCRId = data.id;
                        }
                    } catch (err) {
                        console.log(err);
                    }
                    this.$router.push({
                        name: 'internal-occurrence-report-detail',
                        params: { occurrence_report_id: newCommunityOCRId },
                    });
                }
            });
        },
        discardOCRProposal: function (occurrence_report_id) {
            let vm = this;
            swal.fire({
                title: 'Discard Report',
                text: 'Are you sure you want to discard this report?',
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
                                    title: 'Discarded',
                                    text: 'Your report has been discarded',
                                    icon: 'success',
                                    customClass: {
                                        confirmButton: 'btn btn-primary',
                                    },
                                });
                                vm.$refs.community_ocr_datatable.vmDataTable.ajax.reload(
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
                                vm.$refs.community_ocr_datatable.vmDataTable.ajax.reload(
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
            vm.$refs.community_ocr_datatable.vmDataTable.on(
                'click',
                'a[data-discard-ocr-proposal]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-discard-ocr-proposal');
                    vm.discardOCRProposal(id);
                }
            );
            vm.$refs.community_ocr_datatable.vmDataTable.on(
                'click',
                'a[data-reinstate-ocr-proposal]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-reinstate-ocr-proposal');
                    vm.reinstateOCRProposal(id);
                }
            );
            vm.$refs.community_ocr_datatable.vmDataTable.on(
                'click',
                'a[data-history-occurrence-report]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-history-occurrence-report');
                    vm.historyDocument(id);
                }
            );
            vm.$refs.community_ocr_datatable.vmDataTable.on(
                'childRow.dt',
                function () {
                    helpers.enablePopovers();
                }
            );
        },
    },
};
</script>
