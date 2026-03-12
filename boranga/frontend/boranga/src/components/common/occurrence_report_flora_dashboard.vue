<template id="species_flora_ocr_dashboard">
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
                        id="select_scientific_name_by_groupname"
                        class="form-group"
                    >
                        <label for="ocr_scientific_name_lookup_by_groupname"
                            >Scientific Name:</label
                        >
                        <select
                            id="ocr_scientific_name_lookup_by_groupname"
                            ref="ocr_scientific_name_lookup_by_groupname"
                            name="ocr_scientific_name_lookup_by_groupname"
                            class="form-control"
                        />
                    </div>
                </div>
                <div class="col-md-3">
                    <div id="select_occurrence_name" class="form-group">
                        <label for="ocr_occurrence_name_lookup"
                            >Occurrence Name:</label
                        >
                        <select
                            id="ocr_occurrence_name_lookup"
                            ref="ocr_occurrence_name_lookup"
                            name="ocr_occurrence_name_lookup"
                            class="form-control"
                        />
                    </div>
                </div>
                <div class="col-md-3">
                    <div id="select_occurrence" class="form-group">
                        <label for="ocr_occurrence_lookup"
                            >Occurrence Number:</label
                        >
                        <select
                            id="ocr_occurrence_lookup"
                            ref="ocr_occurrence_lookup"
                            name="ocr_occurrence_lookup"
                            class="form-control"
                        />
                    </div>
                </div>
                <div class="col-md-3">
                    <div id="select_status" class="form-group">
                        <label for="">Status:</label>
                        <select
                            v-model="filterOCRFloraStatus"
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
                        <label for="">Region:</label>
                        <select
                            v-model="filterOCRFloraRegion"
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
                            v-model="filterOCRFloraDistrict"
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
            </div>
            <div class="row">
                <div class="col-md-3">
                    <div id="select_last_modified_by" class="form-group">
                        <label for="ocr_last_modified_by_lookup"
                            >Last Modified By:</label
                        >
                        <select
                            id="ocr_last_modified_by_lookup"
                            ref="ocr_last_modified_by_lookup"
                            name="ocr_last_modified_by_lookup"
                            class="form-control"
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
                            id="observation_from_date"
                            v-model="filterOCRFloraObservationFromDate"
                            type="date"
                            class="form-control"
                            placeholder="DD/MM/YYYY"
                        />
                        <span class="input-group-text"> to </span>
                        <input
                            v-model="filterOCRFloraObservationToDate"
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
                            v-model="filterOCRFloraSubmittedFromDate"
                            type="date"
                            class="form-control"
                            placeholder="DD/MM/YYYY"
                        />
                        <span class="input-group-text"> to </span>
                        <input
                            v-model="filterOCRFloraSubmittedToDate"
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
                        >Approved Date Range:</label
                    >
                    <div class="input-group px-2 mb-2">
                        <span class="input-group-text">From </span>
                        <input
                            v-model="filterOCRFloraApprovedFromDate"
                            type="date"
                            class="form-control"
                            placeholder="DD/MM/YYYY"
                        />
                        <span class="input-group-text"> to </span>
                        <input
                            v-model="filterOCRFloraApprovedToDate"
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
                            v-model="filterOCRFloraLastModifiedFromDate"
                            type="date"
                            class="form-control"
                            placeholder="DD/MM/YYYY"
                        />
                        <span class="input-group-text"> to </span>
                        <input
                            v-model="filterOCRFloraLastModifiedToDate"
                            type="date"
                            class="form-control"
                            placeholder="DD/MM/YYYY"
                        />
                    </div>
                </div>
            </div>
        </CollapsibleFilters>
        <div v-if="addFloraOCRVisibility" class="col-md-12">
            <div class="text-end">
                <a
                    type="button"
                    role="button"
                    class="btn btn-primary mb-2 me-2"
                    :href="`/internal/occurrence-report/bulk_import/?group_type=flora`"
                    ><i class="bi bi-download pe-2"></i>Bulk Import</a
                >

                <button
                    type="button"
                    class="btn btn-primary mb-2"
                    @click.prevent="createFloraOccurrenceReport"
                >
                    <i class="bi bi-plus-circle"></i> Add Flora Occurrence
                    Report
                </button>
            </div>
        </div>
        <div class="row">
            <div class="col-lg-12">
                <datatable
                    :id="datatable_id"
                    ref="flora_ocr_datatable"
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
import OccurrenceReportHistory from '../internal/occurrence/species_occurrence_report_history.vue';

import { api_endpoints, constants, helpers } from '@/utils/hooks';
export default {
    name: 'OccurrenceReportFloraTable',
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
        // for adding agendaitems for the meeting_obj.id
        meeting_obj: {
            type: Object,
            required: false,
        },
        filterOCRFloraOccurrence_cache: {
            type: String,
            required: false,
            default: 'filterOCRFloraOccurrence',
        },
        filterOCRFloraScientificName_cache: {
            type: String,
            required: false,
            default: 'filterOCRFloraScientificName',
        },
        filterOCRFloraStatus_cache: {
            type: String,
            required: false,
            default: 'filterOCRFloraStatus',
        },
        filterOCRFloraObservationFromDate_cache: {
            type: String,
            required: false,
            default: 'filterOCRFloraObservationFromDate',
        },
        filterOCRFloraObservationToDate_cache: {
            type: String,
            required: false,
            default: 'filterOCRFloraObservationToDate',
        },
        filterOCRFloraSubmittedFromDate_cache: {
            type: String,
            required: false,
            default: 'filterOCRFloraSubmittedFromDate',
        },
        filterOCRFloraSubmittedToDate_cache: {
            type: String,
            required: false,
            default: 'filterOCRFloraSubmittedToDate',
        },
        filterOCRFromFloraDueDate_cache: {
            type: String,
            required: false,
            default: 'filterOCRFromFloraDueDate',
        },
        filterOCRToFloraDueDate_cache: {
            type: String,
            required: false,
            default: 'filterOCRToFloraDueDate',
        },
        filterOCRFloraAssessor_cache: {
            type: String,
            required: false,
            default: 'filterOCRFloraAssessor',
        },
        filterOCRFloraSubmitter_cache: {
            type: String,
            required: false,
            default: 'filterOCRFloraSubmitter',
        },
        filterOCRFloraOccurrenceName_cache: {
            type: String,
            required: false,
            default: 'filterOCRFloraOccurrenceName',
        },
        filterOCRFloraRegion_cache: {
            type: String,
            required: false,
            default: 'filterOCRFloraRegion',
        },
        filterOCRFloraDistrict_cache: {
            type: String,
            required: false,
            default: 'filterOCRFloraDistrict',
        },
        filterOCRFloraLastModifiedBy_cache: {
            type: String,
            required: false,
            default: 'filterOCRFloraLastModifiedBy',
        },
        filterOCRFloraApprovedFromDate_cache: {
            type: String,
            required: false,
            default: 'filterOCRFloraApprovedFromDate',
        },
        filterOCRFloraApprovedToDate_cache: {
            type: String,
            required: false,
            default: 'filterOCRFloraApprovedToDate',
        },
        filterOCRFloraLastModifiedFromDate_cache: {
            type: String,
            required: false,
            default: 'filterOCRFloraLastModifiedFromDate',
        },
        filterOCRFloraLastModifiedToDate_cache: {
            type: String,
            required: false,
            default: 'filterOCRFloraLastModifiedToDate',
        },
    },
    data() {
        return {
            uuid: 0,
            occurrenceReportHistoryId: null,
            datatable_id: 'occurrence-report-flora-datatable-' + uuid(),

            // selected values for filtering
            filterOCRFloraOccurrence: sessionStorage.getItem(
                this.filterOCRFloraOccurrence_cache
            )
                ? sessionStorage.getItem(this.filterOCRFloraOccurrence_cache)
                : 'all',

            filterOCRFloraScientificName: sessionStorage.getItem(
                this.filterOCRFloraScientificName_cache
            )
                ? sessionStorage.getItem(
                      this.filterOCRFloraScientificName_cache
                  )
                : 'all',

            filterOCRFloraStatus: sessionStorage.getItem(
                this.filterOCRFloraStatus_cache
            )
                ? sessionStorage.getItem(this.filterOCRFloraStatus_cache)
                : 'all',

            filterOCRFloraObservationFromDate: sessionStorage.getItem(
                this.filterOCRFloraObservationFromDate_cache
            )
                ? sessionStorage.getItem(
                      this.filterOCRFloraObservationFromDate_cache
                  )
                : '',

            filterOCRFloraObservationToDate: sessionStorage.getItem(
                this.filterOCRFloraObservationToDate_cache
            )
                ? sessionStorage.getItem(
                      this.filterOCRFloraObservationToDate_cache
                  )
                : '',

            filterOCRFloraSubmittedFromDate: sessionStorage.getItem(
                this.filterOCRFloraSubmittedFromDate_cache
            )
                ? sessionStorage.getItem(
                      this.filterOCRFloraSubmittedFromDate_cache
                  )
                : '',

            filterOCRFloraSubmittedToDate: sessionStorage.getItem(
                this.filterOCRFloraSubmittedToDate_cache
            )
                ? sessionStorage.getItem(
                      this.filterOCRFloraSubmittedToDate_cache
                  )
                : '',

            filterOCRFromFloraDueDate: sessionStorage.getItem(
                this.filterOCRFromFloraDueDate_cache
            )
                ? sessionStorage.getItem(this.filterOCRFromFloraDueDate_cache)
                : '',
            filterOCRToFloraDueDate: sessionStorage.getItem(
                this.filterOCRToFloraDueDate_cache
            )
                ? sessionStorage.getItem(this.filterOCRToFloraDueDate_cache)
                : '',

            filterOCRFloraAssessor: sessionStorage.getItem(
                this.filterOCRFloraAssessor_cache
            )
                ? sessionStorage.getItem(this.filterOCRFloraAssessor_cache)
                : 'all',

            filterOCRFloraSubmitter: sessionStorage.getItem(
                this.filterOCRFloraSubmitter_cache
            )
                ? sessionStorage.getItem(this.filterOCRFloraSubmitter_cache)
                : 'all',

            filterOCRFloraOccurrenceName: sessionStorage.getItem(
                this.filterOCRFloraOccurrenceName_cache
            )
                ? sessionStorage.getItem(
                      this.filterOCRFloraOccurrenceName_cache
                  )
                : 'all',

            filterOCRFloraRegion: sessionStorage.getItem(
                this.filterOCRFloraRegion_cache
            )
                ? sessionStorage.getItem(this.filterOCRFloraRegion_cache)
                : 'all',

            filterOCRFloraDistrict: sessionStorage.getItem(
                this.filterOCRFloraDistrict_cache
            )
                ? sessionStorage.getItem(this.filterOCRFloraDistrict_cache)
                : 'all',

            filterOCRFloraLastModifiedBy: sessionStorage.getItem(
                this.filterOCRFloraLastModifiedBy_cache
            )
                ? sessionStorage.getItem(
                      this.filterOCRFloraLastModifiedBy_cache
                  )
                : 'all',

            filterOCRFloraApprovedFromDate: sessionStorage.getItem(
                this.filterOCRFloraApprovedFromDate_cache
            )
                ? sessionStorage.getItem(
                      this.filterOCRFloraApprovedFromDate_cache
                  )
                : '',

            filterOCRFloraApprovedToDate: sessionStorage.getItem(
                this.filterOCRFloraApprovedToDate_cache
            )
                ? sessionStorage.getItem(
                      this.filterOCRFloraApprovedToDate_cache
                  )
                : '',

            filterOCRFloraLastModifiedFromDate: sessionStorage.getItem(
                this.filterOCRFloraLastModifiedFromDate_cache
            )
                ? sessionStorage.getItem(
                      this.filterOCRFloraLastModifiedFromDate_cache
                  )
                : '',

            filterOCRFloraLastModifiedToDate: sessionStorage.getItem(
                this.filterOCRFloraLastModifiedToDate_cache
            )
                ? sessionStorage.getItem(
                      this.filterOCRFloraLastModifiedToDate_cache
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
                this.filterOCRFloraOccurrence === 'all' &&
                this.filterOCRFloraScientificName === 'all' &&
                this.filterOCRFloraStatus === 'all' &&
                this.filterOCRFloraObservationFromDate === '' &&
                this.filterOCRFloraObservationToDate === '' &&
                this.filterOCRFloraSubmittedFromDate === '' &&
                this.filterOCRFloraSubmittedToDate === '' &&
                this.filterOCRFromFloraDueDate === '' &&
                this.filterOCRToFloraDueDate === '' &&
                this.filterOCRFloraAssessor === 'all' &&
                this.filterOCRFloraSubmitter === 'all' &&
                this.filterOCRFloraOccurrenceName === 'all' &&
                this.filterOCRFloraRegion === 'all' &&
                this.filterOCRFloraDistrict === 'all' &&
                this.filterOCRFloraLastModifiedBy === 'all' &&
                this.filterOCRFloraApprovedFromDate === '' &&
                this.filterOCRFloraApprovedToDate === '' &&
                this.filterOCRFloraLastModifiedFromDate === '' &&
                this.filterOCRFloraLastModifiedToDate === ''
            ) {
                return false;
            } else {
                return true;
            }
        },
        addFloraOCRVisibility: function () {
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
                'Occurrence Name',
                'Scientific Name',
                'Observation Date',
                'Main Observer',
                'Migrated From ID',
                'Region',
                'District',
                'Submitted on',
                'Submitter',
                'Approved Date',
                'Assessor',
                'Last Modified By',
                'Last Modified Date',
                'Family',
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
        column_lodgement_date: function () {
            return {
                data: 'lodgement_date',
                orderable: true,
                searchable: true,
                visible: true,
                name: 'lodgement_date',
            };
        },
        column_migrated_from_id: function () {
            return {
                data: 'migrated_from_id',
                orderable: false,
                searchable: true,
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
        column_occurrence_name_text: function () {
            return {
                data: 'occurrence_name_text',
                orderable: true,
                searchable: false,
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
        column_approved_date: function () {
            return {
                data: 'datetime_approved',
                orderable: true,
                searchable: false,
                visible: true,
                name: 'datetime_approved',
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
                data: 'datetime_updated',
                orderable: true,
                searchable: false,
                visible: true,
                name: 'datetime_updated',
            };
        },
        column_family: function () {
            return {
                data: 'family',
                orderable: false,
                searchable: false,
                visible: true,
                name: 'family',
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
                data: 'internal_user_edit',
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
            let columns;
            let search;
            let buttons = [
                {
                    extend: 'excel',
                    title: `Boranga ${constants.MODELS.OCCURRENCE_REPORT.MODEL_PREFIX} Flora Excel Export`,
                    text: '<i class="bi bi-download"></i> Excel',
                    className: 'btn btn-primary me-2 rounded',
                    exportOptions: {
                        columns: ':not(.no-export)',
                        orthogonal: 'export',
                    },
                },
                {
                    extend: 'csv',
                    title: `Boranga ${constants.MODELS.OCCURRENCE_REPORT.MODEL_PREFIX} Flora CSV Export`,
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
                vm.column_occurrence,
                vm.column_occurrence_name_text,
                vm.column_scientific_name,
                vm.column_observation_date_time,
                vm.column_main_observer,
                vm.column_migrated_from_id,
                vm.column_region,
                vm.column_district,
                vm.column_lodgement_date,
                vm.column_submitter,
                vm.column_approved_date,
                vm.column_assessor,
                vm.column_last_modified_by,
                vm.column_last_modified_date,
                vm.column_family,
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
                responsive: {
                    details: {
                        renderer: function (api, rowIdx, columns) {
                            var hidden = columns.filter(function (col) {
                                return col.hidden;
                            });
                            if (!hidden.length) return false;
                            var cells = hidden
                                .map(function (col) {
                                    return (
                                        '<span class="me-3"><strong>' +
                                        col.title +
                                        ':</strong> ' +
                                        (col.data !== null &&
                                        col.data !== undefined
                                            ? col.data
                                            : '') +
                                        '</span>'
                                    );
                                })
                                .join('');
                            return $(
                                '<div class="p-2 d-flex flex-wrap"/>'
                            ).append(cells);
                        },
                    },
                },
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
                        d.filter_occurrence = vm.filterOCRFloraOccurrence;
                        d.filter_scientific_name =
                            vm.filterOCRFloraScientificName;
                        d.filter_status = vm.filterOCRFloraStatus;
                        d.filter_observation_from_date =
                            vm.filterOCRFloraObservationFromDate;
                        d.filter_observation_to_date =
                            vm.filterOCRFloraObservationToDate;
                        d.filter_submitted_from_date =
                            vm.filterOCRFloraSubmittedFromDate;
                        d.filter_submitted_to_date =
                            vm.filterOCRFloraSubmittedToDate;
                        d.filter_from_due_date = vm.filterOCRFromFloraDueDate;
                        d.filter_to_due_date = vm.filterOCRToFloraDueDate;
                        d.filter_assessor = vm.filterOCRFloraAssessor;
                        d.filter_submitter = vm.filterOCRFloraSubmitter;
                        d.filter_occurrence_name =
                            vm.filterOCRFloraOccurrenceName;
                        d.filter_region = vm.filterOCRFloraRegion;
                        d.filter_district = vm.filterOCRFloraDistrict;
                        d.filter_last_modified_by =
                            vm.filterOCRFloraLastModifiedBy;
                        d.filter_approved_from_date =
                            vm.filterOCRFloraApprovedFromDate;
                        d.filter_approved_to_date =
                            vm.filterOCRFloraApprovedToDate;
                        d.filter_last_modified_from_date =
                            vm.filterOCRFloraLastModifiedFromDate;
                        d.filter_last_modified_to_date =
                            vm.filterOCRFloraLastModifiedToDate;
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
        filterOCRFloraOccurrence: function () {
            let vm = this;
            vm.$refs.flora_ocr_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterOCRFloraOccurrence_cache,
                vm.filterOCRFloraOccurrence
            );
        },
        filterOCRFloraScientificName: function () {
            let vm = this;
            vm.$refs.flora_ocr_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterOCRFloraScientificName_cache,
                vm.filterOCRFloraScientificName
            );
        },
        filterOCRFloraStatus: function () {
            let vm = this;
            vm.$refs.flora_ocr_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterOCRFloraStatus_cache,
                vm.filterOCRFloraStatus
            );
        },
        filterOCRFloraObservationFromDate: function () {
            let vm = this;
            vm.$refs.flora_ocr_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterOCRFloraObservationFromDate_cache,
                vm.filterOCRFloraObservationFromDate
            );
        },
        filterOCRFloraObservationToDate: function () {
            let vm = this;
            vm.$refs.flora_ocr_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterOCRFloraObservationToDate_cache,
                vm.filterOCRFloraObservationToDate
            );
        },
        filterOCRFloraSubmittedFromDate: function () {
            let vm = this;
            vm.$refs.flora_ocr_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterOCRFloraSubmittedFromDate_cache,
                vm.filterOCRFloraSubmittedFromDate
            );
        },
        filterOCRFloraSubmittedToDate: function () {
            let vm = this;
            vm.$refs.flora_ocr_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterOCRFloraSubmittedToDate_cache,
                vm.filterOCRFloraSubmittedToDate
            );
        },
        filterOCRFromFloraDueDate: function () {
            let vm = this;
            vm.$refs.flora_ocr_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterOCRFromFloraDueDate_cache,
                vm.filterOCRFromFloraDueDate
            );
        },
        filterOCRToFloraDueDate: function () {
            let vm = this;
            vm.$refs.flora_ocr_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterOCRToFloraDueDate_cache,
                vm.filterOCRToFloraDueDate
            );
        },
        filterOCRFloraAssessor: function () {
            let vm = this;
            vm.$refs.flora_ocr_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterOCRFloraAssessor_cache,
                vm.filterOCRFloraAssessor
            );
        },
        filterOCRFloraSubmitter: function () {
            let vm = this;
            vm.$refs.flora_ocr_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterOCRFloraSubmitter_cache,
                vm.filterOCRFloraSubmitter
            );
        },
        filterOCRFloraOccurrenceName: function () {
            let vm = this;
            vm.$refs.flora_ocr_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            );
            sessionStorage.setItem(
                vm.filterOCRFloraOccurrenceName_cache,
                vm.filterOCRFloraOccurrenceName
            );
        },
        filterOCRFloraRegion: function () {
            let vm = this;
            vm.$refs.flora_ocr_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            );
            sessionStorage.setItem(
                vm.filterOCRFloraRegion_cache,
                vm.filterOCRFloraRegion
            );
        },
        filterOCRFloraDistrict: function () {
            let vm = this;
            vm.$refs.flora_ocr_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            );
            sessionStorage.setItem(
                vm.filterOCRFloraDistrict_cache,
                vm.filterOCRFloraDistrict
            );
        },
        filterOCRFloraLastModifiedBy: function () {
            let vm = this;
            vm.$refs.flora_ocr_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            );
            sessionStorage.setItem(
                vm.filterOCRFloraLastModifiedBy_cache,
                vm.filterOCRFloraLastModifiedBy
            );
        },
        filterOCRFloraApprovedFromDate: function () {
            let vm = this;
            vm.$refs.flora_ocr_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            );
            sessionStorage.setItem(
                vm.filterOCRFloraApprovedFromDate_cache,
                vm.filterOCRFloraApprovedFromDate
            );
        },
        filterOCRFloraApprovedToDate: function () {
            let vm = this;
            vm.$refs.flora_ocr_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            );
            sessionStorage.setItem(
                vm.filterOCRFloraApprovedToDate_cache,
                vm.filterOCRFloraApprovedToDate
            );
        },
        filterOCRFloraLastModifiedFromDate: function () {
            let vm = this;
            vm.$refs.flora_ocr_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            );
            sessionStorage.setItem(
                vm.filterOCRFloraLastModifiedFromDate_cache,
                vm.filterOCRFloraLastModifiedFromDate
            );
        },
        filterOCRFloraLastModifiedToDate: function () {
            let vm = this;
            vm.$refs.flora_ocr_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            );
            sessionStorage.setItem(
                vm.filterOCRFloraLastModifiedToDate_cache,
                vm.filterOCRFloraLastModifiedToDate
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
            vm.initialiseOccurrenceLookup();
            vm.initialiseScientificNameLookup();
            vm.initialiseOccurrenceNameLookup();
            vm.initialiseAssessorLookup();
            vm.initialiseSubmitterLookup();
            vm.initialiseLastModifiedByLookup();
            vm.addEventListeners();
            var newOption;
            if (
                sessionStorage.getItem('filterOCRFloraOccurrence') != 'all' &&
                sessionStorage.getItem('filterOCRFloraOccurrence') != null
            ) {
                newOption = new Option(
                    sessionStorage.getItem('filterOCRFloraOccurrenceText'),
                    vm.filterOCRFloraOccurrence,
                    false,
                    true
                );
                $('#ocr_occurrence_lookup').append(newOption);
            }
            if (
                sessionStorage.getItem('filterOCRFloraScientificName') !=
                    'all' &&
                sessionStorage.getItem('filterOCRFloraScientificName') != null
            ) {
                newOption = new Option(
                    sessionStorage.getItem('filterOCRFloraScientificNameText'),
                    vm.filterOCRFloraScientificName,
                    false,
                    true
                );
                $('#ocr_scientific_name_lookup').append(newOption);
            }
            if (
                sessionStorage.getItem('filterOCRFloraOccurrenceName') !=
                    'all' &&
                sessionStorage.getItem('filterOCRFloraOccurrenceName') != null
            ) {
                newOption = new Option(
                    sessionStorage.getItem('filterOCRFloraOccurrenceNameText'),
                    vm.filterOCRFloraOccurrenceName,
                    false,
                    true
                );
                $('#ocr_occurrence_name_lookup').append(newOption);
            }
            if (
                sessionStorage.getItem('filterOCRFloraAssessor') != 'all' &&
                sessionStorage.getItem('filterOCRFloraAssessor') != null
            ) {
                newOption = new Option(
                    sessionStorage.getItem('filterOCRFloraAssessorText'),
                    vm.filterOCRFloraAssessor,
                    false,
                    true
                );
                $('#ocr_assessor_lookup').append(newOption);
            }
            if (
                sessionStorage.getItem('filterOCRFloraSubmitter') != 'all' &&
                sessionStorage.getItem('filterOCRFloraSubmitter') != null
            ) {
                newOption = new Option(
                    sessionStorage.getItem('filterOCRFloraSubmitterText'),
                    vm.filterOCRFloraSubmitter,
                    false,
                    true
                );
                $('#ocr_submitter_lookup').append(newOption);
            }
            if (
                sessionStorage.getItem('filterOCRFloraLastModifiedBy') !=
                    'all' &&
                sessionStorage.getItem('filterOCRFloraLastModifiedBy') != null
            ) {
                newOption = new Option(
                    sessionStorage.getItem('filterOCRFloraLastModifiedByText'),
                    vm.filterOCRFloraLastModifiedBy,
                    false,
                    true
                );
                $('#ocr_last_modified_by_lookup').append(newOption);
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
                        url: api_endpoints.occurrence_lookup,
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
                    vm.filterOCRFloraOccurrence = data;
                    sessionStorage.setItem(
                        'filterOCRFloraOccurrenceText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.filterOCRFloraOccurrence = 'all';
                    sessionStorage.setItem('filterOCRFloraOccurrenceText', '');
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-ocr_occurrence_lookup-results"]'
                    );
                    searchField[0].focus();
                });
        },
        initialiseScientificNameLookup: function () {
            let vm = this;
            $(vm.$refs.ocr_scientific_name_lookup_by_groupname)
                .select2({
                    minimumInputLength: 2,
                    dropdownParent: $('#select_scientific_name_by_groupname'),
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
                    vm.filterOCRFloraScientificName = data;
                    sessionStorage.setItem(
                        'filterOCRFloraScientificNameText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.filterOCRFloraScientificName = 'all';
                    sessionStorage.setItem(
                        'filterOCRFloraScientificNameText',
                        ''
                    );
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-ocr_scientific_name_lookup_by_groupname-results"]'
                    );
                    searchField[0].focus();
                });
        },
        initialiseOccurrenceNameLookup: function () {
            let vm = this;
            $(vm.$refs.ocr_occurrence_name_lookup)
                .select2({
                    minimumInputLength: 2,
                    dropdownParent: $('#select_occurrence_name'),
                    theme: 'bootstrap-5',
                    allowClear: true,
                    placeholder: 'Search Occurrence Name',
                    ajax: {
                        url: api_endpoints.occurrence_name_lookup,
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
                    vm.filterOCRFloraOccurrenceName = data;
                    sessionStorage.setItem(
                        'filterOCRFloraOccurrenceNameText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.filterOCRFloraOccurrenceName = 'all';
                    sessionStorage.setItem(
                        'filterOCRFloraOccurrenceNameText',
                        ''
                    );
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-ocr_occurrence_name_lookup-results"]'
                    );
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
                    vm.filterOCRFloraAssessor = data;
                    sessionStorage.setItem(
                        'filterOCRFloraAssessorText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.filterOCRFloraAssessor = 'all';
                    sessionStorage.setItem('filterOCRFloraAssessorText', '');
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
                    vm.filterOCRFloraSubmitter = data;
                    sessionStorage.setItem(
                        'filterOCRFloraSubmitterText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.selected_referral = null;
                    vm.filterOCRFloraSubmitter = 'all';
                    sessionStorage.setItem('filterOCRFloraSubmitterText', '');
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-ocr_submitter_lookup-results"]'
                    );
                    searchField[0].focus();
                });
        },
        initialiseLastModifiedByLookup: function () {
            let vm = this;
            $(vm.$refs.ocr_last_modified_by_lookup)
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
                    vm.filterOCRFloraLastModifiedBy = data;
                    sessionStorage.setItem(
                        'filterOCRFloraLastModifiedByText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.filterOCRFloraLastModifiedBy = 'all';
                    sessionStorage.setItem(
                        'filterOCRFloraLastModifiedByText',
                        ''
                    );
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-ocr_last_modified_by_lookup-results"]'
                    );
                    searchField[0].focus();
                });
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
                    this.filterOCRFloraDistrict = 'all';
                }
                this.filtered_district_list = [];
                if (this.filterOCRFloraRegion.toString() === 'all') {
                    this.filtered_district_list = this.district_list;
                } else {
                    for (let choice of this.district_list) {
                        if (
                            choice.region_id.toString() ===
                            this.filterOCRFloraRegion.toString()
                        ) {
                            this.filtered_district_list.push(choice);
                        }
                    }
                }
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
                },
                (error) => {
                    console.log(error);
                }
            );
        },
        createFloraOccurrenceReport: async function () {
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
                    let newFloraOCRId = null;
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
                            newFloraOCRId = data.id;
                        }
                    } catch (err) {
                        console.log(err);
                    }
                    this.$router.push({
                        name: 'internal-occurrence-report-detail',
                        params: { occurrence_report_id: newFloraOCRId },
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
                                vm.$refs.flora_ocr_datatable.vmDataTable.ajax.reload(
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
                                vm.$refs.flora_ocr_datatable.vmDataTable.ajax.reload(
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
            vm.$refs.flora_ocr_datatable.vmDataTable.on(
                'click',
                'a[data-discard-ocr-proposal]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-discard-ocr-proposal');
                    vm.discardOCRProposal(id);
                }
            );
            vm.$refs.flora_ocr_datatable.vmDataTable.on(
                'click',
                'a[data-reinstate-ocr-proposal]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-reinstate-ocr-proposal');
                    vm.reinstateOCRProposal(id);
                }
            );
            vm.$refs.flora_ocr_datatable.vmDataTable.on(
                'click',
                'a[data-history-occurrence-report]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-history-occurrence-report');
                    vm.historyDocument(id);
                }
            );
            vm.$refs.flora_ocr_datatable.vmDataTable.on(
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
