<template id="species_fauna_ocr_dashboard">
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
                    <div id="select_common_name" class="form-group">
                        <label for="ocr_common_name_lookup">Common Name:</label>
                        <select
                            id="ocr_common_name_lookup"
                            ref="ocr_common_name_lookup"
                            name="ocr_common_name_lookup"
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
                    <div id="select_status" class="form-group">
                        <label for="ocr_status_lookup">Status:</label>
                        <select
                            v-model="filterOCRFaunaStatus"
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
                    <div class="form-group">
                        <label for="">Region:</label>
                        <select
                            v-model="filterOCRFaunaRegion"
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
                            v-model="filterOCRFaunaDistrict"
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
                            v-model="filterOCRFaunaObservationFromDate"
                            type="date"
                            class="form-control"
                            placeholder="DD/MM/YYYY"
                        />
                        <span class="input-group-text"> to </span>
                        <input
                            v-model="filterOCRFaunaObservationToDate"
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
                            v-model="filterOCRFaunaSubmittedFromDate"
                            type="date"
                            class="form-control"
                            placeholder="DD/MM/YYYY"
                        />
                        <span class="input-group-text"> to </span>
                        <input
                            v-model="filterOCRFaunaSubmittedToDate"
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
                            v-model="filterOCRFaunaApprovedFromDate"
                            type="date"
                            class="form-control"
                            placeholder="DD/MM/YYYY"
                        />
                        <span class="input-group-text"> to </span>
                        <input
                            v-model="filterOCRFaunaApprovedToDate"
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
                            v-model="filterOCRFaunaLastModifiedFromDate"
                            type="date"
                            class="form-control"
                            placeholder="DD/MM/YYYY"
                        />
                        <span class="input-group-text"> to </span>
                        <input
                            v-model="filterOCRFaunaLastModifiedToDate"
                            type="date"
                            class="form-control"
                            placeholder="DD/MM/YYYY"
                        />
                    </div>
                </div>
            </div>
        </CollapsibleFilters>
        <div v-if="addFaunaOCRVisibility" class="col-md-12">
            <div class="text-end">
                <a
                    type="button"
                    role="button"
                    class="btn btn-primary mb-2 me-2"
                    :href="`/internal/occurrence-report/bulk_import/?group_type=fauna`"
                    ><i class="bi bi-download pe-2"></i>Bulk Import</a
                >
                <button
                    type="button"
                    class="btn btn-primary mb-2"
                    @click.prevent="createFaunaOccurrenceReport"
                >
                    <i class="bi bi-plus-circle"></i> Add Fauna Occurrence
                    Report
                </button>
            </div>
        </div>
        <div class="row">
            <div class="col-lg-12">
                <datatable
                    :id="datatable_id"
                    ref="fauna_ocr_datatable"
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
    name: 'OccurrenceReportFaunaTable',
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
        filterOCRFaunaOccurrence_cache: {
            type: String,
            required: false,
            default: 'filterOCRFaunaOccurrence',
        },
        filterOCRFaunaScientificName_cache: {
            type: String,
            required: false,
            default: 'filterOCRFaunaScientificName',
        },
        filterOCRFaunaStatus_cache: {
            type: String,
            required: false,
            default: 'filterOCRFaunaStatus',
        },
        filterOCRFaunaObservationFromDate_cache: {
            type: String,
            required: false,
            default: 'filterOCRFaunaObservationFromDate',
        },
        filterOCRFaunaObservationToDate_cache: {
            type: String,
            required: false,
            default: 'filterOCRFaunaObservationToDate',
        },
        filterOCRFaunaSubmittedFromDate_cache: {
            type: String,
            required: false,
            default: 'filterOCRFaunaSubmittedFromDate',
        },
        filterOCRFaunaSubmittedToDate_cache: {
            type: String,
            required: false,
            default: 'filterOCRFaunaSubmittedToDate',
        },
        filterOCRFromFaunaDueDate_cache: {
            type: String,
            required: false,
            default: 'filterOCRFromFaunaDueDate',
        },
        filterOCRToFaunaDueDate_cache: {
            type: String,
            required: false,
            default: 'filterOCRToFaunaDueDate',
        },
        filterOCRFaunaAssessor_cache: {
            type: String,
            required: false,
            default: 'filterOCRFaunaAssessor',
        },
        filterOCRFaunaSubmitter_cache: {
            type: String,
            required: false,
            default: 'filterOCRFaunaSubmitter',
        },
        filterOCRFaunaCommonName_cache: {
            type: String,
            required: false,
            default: 'filterOCRFaunaCommonName',
        },
        filterOCRFaunaOccurrenceName_cache: {
            type: String,
            required: false,
            default: 'filterOCRFaunaOccurrenceName',
        },
        filterOCRFaunaRegion_cache: {
            type: String,
            required: false,
            default: 'filterOCRFaunaRegion',
        },
        filterOCRFaunaDistrict_cache: {
            type: String,
            required: false,
            default: 'filterOCRFaunaDistrict',
        },
        filterOCRFaunaLastModifiedBy_cache: {
            type: String,
            required: false,
            default: 'filterOCRFaunaLastModifiedBy',
        },
        filterOCRFaunaApprovedFromDate_cache: {
            type: String,
            required: false,
            default: 'filterOCRFaunaApprovedFromDate',
        },
        filterOCRFaunaApprovedToDate_cache: {
            type: String,
            required: false,
            default: 'filterOCRFaunaApprovedToDate',
        },
        filterOCRFaunaLastModifiedFromDate_cache: {
            type: String,
            required: false,
            default: 'filterOCRFaunaLastModifiedFromDate',
        },
        filterOCRFaunaLastModifiedToDate_cache: {
            type: String,
            required: false,
            default: 'filterOCRFaunaLastModifiedToDate',
        },
    },
    data() {
        return {
            uuid: 0,
            occurrenceReportHistoryId: null,
            datatable_id: 'occurrence-report-fauna-datatable-' + uuid(),

            // selected values for filtering
            filterOCRFaunaOccurrence: sessionStorage.getItem(
                this.filterOCRFaunaOccurrence_cache
            )
                ? sessionStorage.getItem(this.filterOCRFaunaOccurrence_cache)
                : 'all',

            filterOCRFaunaScientificName: sessionStorage.getItem(
                this.filterOCRFaunaScientificName_cache
            )
                ? sessionStorage.getItem(
                      this.filterOCRFaunaScientificName_cache
                  )
                : 'all',

            filterOCRFaunaStatus: sessionStorage.getItem(
                this.filterOCRFaunaStatus_cache
            )
                ? sessionStorage.getItem(this.filterOCRFaunaStatus_cache)
                : 'all',

            filterOCRFaunaObservationFromDate: sessionStorage.getItem(
                this.filterOCRFaunaObservationFromDate_cache
            )
                ? sessionStorage.getItem(
                      this.filterOCRFaunaObservationFromDate_cache
                  )
                : '',

            filterOCRFaunaObservationToDate: sessionStorage.getItem(
                this.filterOCRFaunaObservationToDate_cache
            )
                ? sessionStorage.getItem(
                      this.filterOCRFaunaObservationToDate_cache
                  )
                : '',

            filterOCRFaunaSubmittedFromDate: sessionStorage.getItem(
                this.filterOCRFaunaSubmittedFromDate_cache
            )
                ? sessionStorage.getItem(
                      this.filterOCRFaunaSubmittedFromDate_cache
                  )
                : '',

            filterOCRFaunaSubmittedToDate: sessionStorage.getItem(
                this.filterOCRFaunaSubmittedToDate_cache
            )
                ? sessionStorage.getItem(
                      this.filterOCRFaunaSubmittedToDate_cache
                  )
                : '',

            filterOCRFromFaunaDueDate: sessionStorage.getItem(
                this.filterOCRFromFaunaDueDate_cache
            )
                ? sessionStorage.getItem(this.filterOCRFromFaunaDueDate_cache)
                : '',
            filterOCRToFaunaDueDate: sessionStorage.getItem(
                this.filterOCRToFaunaDueDate_cache
            )
                ? sessionStorage.getItem(this.filterOCRToFaunaDueDate_cache)
                : '',

            filterOCRFaunaAssessor: sessionStorage.getItem(
                this.filterOCRFaunaAssessor_cache
            )
                ? sessionStorage.getItem(this.filterOCRFaunaAssessor_cache)
                : 'all',

            filterOCRFaunaSubmitter: sessionStorage.getItem(
                this.filterOCRFaunaSubmitter_cache
            )
                ? sessionStorage.getItem(this.filterOCRFaunaSubmitter_cache)
                : 'all',

            filterOCRFaunaCommonName: sessionStorage.getItem(
                this.filterOCRFaunaCommonName_cache
            )
                ? sessionStorage.getItem(this.filterOCRFaunaCommonName_cache)
                : 'all',

            filterOCRFaunaOccurrenceName: sessionStorage.getItem(
                this.filterOCRFaunaOccurrenceName_cache
            )
                ? sessionStorage.getItem(
                      this.filterOCRFaunaOccurrenceName_cache
                  )
                : 'all',

            filterOCRFaunaRegion: sessionStorage.getItem(
                this.filterOCRFaunaRegion_cache
            )
                ? sessionStorage.getItem(this.filterOCRFaunaRegion_cache)
                : 'all',

            filterOCRFaunaDistrict: sessionStorage.getItem(
                this.filterOCRFaunaDistrict_cache
            )
                ? sessionStorage.getItem(this.filterOCRFaunaDistrict_cache)
                : 'all',

            filterOCRFaunaLastModifiedBy: sessionStorage.getItem(
                this.filterOCRFaunaLastModifiedBy_cache
            )
                ? sessionStorage.getItem(
                      this.filterOCRFaunaLastModifiedBy_cache
                  )
                : 'all',

            filterOCRFaunaApprovedFromDate: sessionStorage.getItem(
                this.filterOCRFaunaApprovedFromDate_cache
            )
                ? sessionStorage.getItem(
                      this.filterOCRFaunaApprovedFromDate_cache
                  )
                : '',

            filterOCRFaunaApprovedToDate: sessionStorage.getItem(
                this.filterOCRFaunaApprovedToDate_cache
            )
                ? sessionStorage.getItem(
                      this.filterOCRFaunaApprovedToDate_cache
                  )
                : '',

            filterOCRFaunaLastModifiedFromDate: sessionStorage.getItem(
                this.filterOCRFaunaLastModifiedFromDate_cache
            )
                ? sessionStorage.getItem(
                      this.filterOCRFaunaLastModifiedFromDate_cache
                  )
                : '',

            filterOCRFaunaLastModifiedToDate: sessionStorage.getItem(
                this.filterOCRFaunaLastModifiedToDate_cache
            )
                ? sessionStorage.getItem(
                      this.filterOCRFaunaLastModifiedToDate_cache
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
                this.filterOCRFaunaOccurrence === 'all' &&
                this.filterOCRFaunaScientificName === 'all' &&
                this.filterOCRFaunaStatus === 'all' &&
                this.filterOCRFaunaObservationFromDate === '' &&
                this.filterOCRFaunaObservationToDate === '' &&
                this.filterOCRFaunaSubmittedFromDate === '' &&
                this.filterOCRFaunaSubmittedToDate === '' &&
                this.filterOCRFromFaunaDueDate === '' &&
                this.filterOCRToFaunaDueDate === '' &&
                this.filterOCRFaunaAssessor === 'all' &&
                this.filterOCRFaunaSubmitter === 'all' &&
                this.filterOCRFaunaCommonName === 'all' &&
                this.filterOCRFaunaOccurrenceName === 'all' &&
                this.filterOCRFaunaRegion === 'all' &&
                this.filterOCRFaunaDistrict === 'all' &&
                this.filterOCRFaunaLastModifiedBy === 'all' &&
                this.filterOCRFaunaApprovedFromDate === '' &&
                this.filterOCRFaunaApprovedToDate === '' &&
                this.filterOCRFaunaLastModifiedFromDate === '' &&
                this.filterOCRFaunaLastModifiedToDate === ''
            ) {
                return false;
            } else {
                return true;
            }
        },
        addFaunaOCRVisibility: function () {
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
                'Common Name',
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
                'Fauna Group',
                'Fauna Sub Group',
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
        column_occurrence_name_text: function () {
            return {
                data: 'occurrence_name_text',
                orderable: true,
                searchable: false,
                visible: true,
                name: 'occurrence__occurrence_name',
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
        column_fauna_group: function () {
            return {
                data: 'fauna_group',
                orderable: false,
                searchable: false,
                visible: true,
                name: 'fauna_group',
            };
        },
        column_fauna_sub_group: function () {
            return {
                data: 'fauna_sub_group',
                orderable: false,
                searchable: false,
                visible: true,
                name: 'fauna_sub_group',
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
            let columns;
            let search;
            let buttons = [
                {
                    extend: 'excel',
                    title: `Boranga ${constants.MODELS.OCCURRENCE_REPORT.MODEL_PREFIX} Fauna Excel Export`,
                    text: '<i class="bi bi-download"></i> Excel',
                    className: 'btn btn-primary me-2 rounded',
                    exportOptions: {
                        columns: ':not(.no-export)',
                        orthogonal: 'export',
                    },
                },
                {
                    extend: 'csv',
                    title: `Boranga ${constants.MODELS.OCCURRENCE_REPORT.MODEL_PREFIX} Fauna CSV Export`,
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
                vm.column_common_name,
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
                vm.column_fauna_group,
                vm.column_fauna_sub_group,
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
                    method: 'post',
                    headers: {
                        'X-CSRFToken': helpers.getCookie('csrftoken'),
                    },
                    // adding extra params for Custom filtering
                    data: function (d) {
                        d.filter_group_type = vm.group_type_name;
                        d.filter_occurrence = vm.filterOCRFaunaOccurrence;
                        d.filter_scientific_name =
                            vm.filterOCRFaunaScientificName;
                        d.filter_status = vm.filterOCRFaunaStatus;
                        d.filter_observation_from_date =
                            vm.filterOCRFaunaObservationFromDate;
                        d.filter_observation_to_date =
                            vm.filterOCRFaunaObservationToDate;
                        d.filter_submitted_from_date =
                            vm.filterOCRFaunaSubmittedFromDate;
                        d.filter_submitted_to_date =
                            vm.filterOCRFaunaSubmittedToDate;
                        d.filter_from_due_date = vm.filterOCRFromFaunaDueDate;
                        d.filter_to_due_date = vm.filterOCRToFaunaDueDate;
                        d.filter_assessor = vm.filterOCRFaunaAssessor;
                        d.filter_submitter = vm.filterOCRFaunaSubmitter;
                        d.filter_occurrence_name =
                            vm.filterOCRFaunaOccurrenceName;
                        d.filter_common_name = vm.filterOCRFaunaCommonName;
                        d.filter_region = vm.filterOCRFaunaRegion;
                        d.filter_district = vm.filterOCRFaunaDistrict;
                        d.filter_last_modified_by =
                            vm.filterOCRFaunaLastModifiedBy;
                        d.filter_approved_from_date =
                            vm.filterOCRFaunaApprovedFromDate;
                        d.filter_approved_to_date =
                            vm.filterOCRFaunaApprovedToDate;
                        d.filter_last_modified_from_date =
                            vm.filterOCRFaunaLastModifiedFromDate;
                        d.filter_last_modified_to_date =
                            vm.filterOCRFaunaLastModifiedToDate;
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
        filterOCRFaunaOccurrence: function () {
            let vm = this;
            vm.$refs.fauna_ocr_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterOCRFaunaOccurrence_cache,
                vm.filterOCRFaunaOccurrence
            );
        },
        filterOCRFaunaScientificName: function () {
            let vm = this;
            vm.$refs.fauna_ocr_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterOCRFaunaScientificName_cache,
                vm.filterOCRFaunaScientificName
            );
        },
        filterOCRFaunaStatus: function () {
            let vm = this;
            vm.$refs.fauna_ocr_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterOCRFaunaStatus_cache,
                vm.filterOCRFaunaStatus
            );
        },
        filterOCRFaunaObservationFromDate: function () {
            let vm = this;
            vm.$refs.fauna_ocr_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterOCRFaunaObservationFromDate_cache,
                vm.filterOCRFaunaObservationFromDate
            );
        },
        filterOCRFaunaObservationToDate: function () {
            let vm = this;
            vm.$refs.fauna_ocr_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterOCRFaunaObservationToDate_cache,
                vm.filterOCRFaunaObservationToDate
            );
        },
        filterOCRFaunaSubmittedFromDate: function () {
            let vm = this;
            vm.$refs.fauna_ocr_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterOCRFaunaSubmittedFromDate_cache,
                vm.filterOCRFaunaSubmittedFromDate
            );
        },
        filterOCRFaunaSubmittedToDate: function () {
            let vm = this;
            vm.$refs.fauna_ocr_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterOCRFaunaSubmittedToDate_cache,
                vm.filterOCRFaunaSubmittedToDate
            );
        },
        filterOCRFromFaunaDueDate: function () {
            let vm = this;
            vm.$refs.fauna_ocr_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterOCRFromFaunaDueDate_cache,
                vm.filterOCRFromFaunaDueDate
            );
        },
        filterOCRToFaunaDueDate: function () {
            let vm = this;
            vm.$refs.fauna_ocr_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterOCRToFaunaDueDate_cache,
                vm.filterOCRToFaunaDueDate
            );
        },
        filterOCRFaunaAssessor: function () {
            let vm = this;
            vm.$refs.fauna_ocr_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterOCRFaunaAssessor_cache,
                vm.filterOCRFaunaAssessor
            );
        },
        filterOCRFaunaSubmitter: function () {
            let vm = this;
            vm.$refs.fauna_ocr_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterOCRFaunaSubmitter_cache,
                vm.filterOCRFaunaSubmitter
            );
        },
        filterOCRFaunaCommonName: function () {
            let vm = this;
            vm.$refs.fauna_ocr_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            );
            sessionStorage.setItem(
                vm.filterOCRFaunaCommonName_cache,
                vm.filterOCRFaunaCommonName
            );
        },
        filterOCRFaunaOccurrenceName: function () {
            let vm = this;
            vm.$refs.fauna_ocr_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            );
            sessionStorage.setItem(
                vm.filterOCRFaunaOccurrenceName_cache,
                vm.filterOCRFaunaOccurrenceName
            );
        },
        filterOCRFaunaRegion: function () {
            let vm = this;
            vm.$refs.fauna_ocr_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            );
            sessionStorage.setItem(
                vm.filterOCRFaunaRegion_cache,
                vm.filterOCRFaunaRegion
            );
            vm.filterDistrict();
        },
        filterOCRFaunaDistrict: function () {
            let vm = this;
            vm.$refs.fauna_ocr_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            );
            sessionStorage.setItem(
                vm.filterOCRFaunaDistrict_cache,
                vm.filterOCRFaunaDistrict
            );
        },
        filterOCRFaunaLastModifiedBy: function () {
            let vm = this;
            vm.$refs.fauna_ocr_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            );
            sessionStorage.setItem(
                vm.filterOCRFaunaLastModifiedBy_cache,
                vm.filterOCRFaunaLastModifiedBy
            );
        },
        filterOCRFaunaApprovedFromDate: function () {
            let vm = this;
            vm.$refs.fauna_ocr_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            );
            sessionStorage.setItem(
                vm.filterOCRFaunaApprovedFromDate_cache,
                vm.filterOCRFaunaApprovedFromDate
            );
        },
        filterOCRFaunaApprovedToDate: function () {
            let vm = this;
            vm.$refs.fauna_ocr_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            );
            sessionStorage.setItem(
                vm.filterOCRFaunaApprovedToDate_cache,
                vm.filterOCRFaunaApprovedToDate
            );
        },
        filterOCRFaunaLastModifiedFromDate: function () {
            let vm = this;
            vm.$refs.fauna_ocr_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            );
            sessionStorage.setItem(
                vm.filterOCRFaunaLastModifiedFromDate_cache,
                vm.filterOCRFaunaLastModifiedFromDate
            );
        },
        filterOCRFaunaLastModifiedToDate: function () {
            let vm = this;
            vm.$refs.fauna_ocr_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            );
            sessionStorage.setItem(
                vm.filterOCRFaunaLastModifiedToDate_cache,
                vm.filterOCRFaunaLastModifiedToDate
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
            vm.initialiseCommonNameLookup();
            vm.initialiseOccurrenceNameLookup();
            vm.initialiseAssessorLookup();
            vm.initialiseSubmitterLookup();
            vm.initialiseLastModifiedByLookup();
            vm.addEventListeners();
            var newOption;
            if (
                sessionStorage.getItem('filterOCRFaunaOccurrence') != 'all' &&
                sessionStorage.getItem('filterOCRFaunaOccurrence') != null
            ) {
                newOption = new Option(
                    sessionStorage.getItem('filterOCRFaunaOccurrenceText'),
                    vm.filterOCRFaunaOccurrence,
                    false,
                    true
                );
                $('#ocr_occurrence_lookup').append(newOption);
            }
            if (
                sessionStorage.getItem('filterOCRFaunaScientificName') !=
                    'all' &&
                sessionStorage.getItem('filterOCRFaunaScientificName') != null
            ) {
                newOption = new Option(
                    sessionStorage.getItem('filterOCRFaunaScientificNameText'),
                    vm.filterOCRFaunaScientificName,
                    false,
                    true
                );
                $('#ocr_scientific_name_lookup').append(newOption);
            }
            if (
                sessionStorage.getItem('filterOCRFaunaCommonName') != 'all' &&
                sessionStorage.getItem('filterOCRFaunaCommonName') != null
            ) {
                newOption = new Option(
                    sessionStorage.getItem('filterOCRFaunaCommonNameText'),
                    vm.filterOCRFaunaCommonName,
                    false,
                    true
                );
                $('#ocr_common_name_lookup').append(newOption);
            }
            if (
                sessionStorage.getItem('filterOCRFaunaOccurrenceName') !=
                    'all' &&
                sessionStorage.getItem('filterOCRFaunaOccurrenceName') != null
            ) {
                newOption = new Option(
                    sessionStorage.getItem('filterOCRFaunaOccurrenceNameText'),
                    vm.filterOCRFaunaOccurrenceName,
                    false,
                    true
                );
                $('#ocr_occurrence_name_lookup').append(newOption);
            }
            if (
                sessionStorage.getItem('filterOCRFaunaAssessor') != 'all' &&
                sessionStorage.getItem('filterOCRFaunaAssessor') != null
            ) {
                newOption = new Option(
                    sessionStorage.getItem('filterOCRFaunaAssessorText'),
                    vm.filterOCRFaunaAssessor,
                    false,
                    true
                );
                $('#ocr_assessor_lookup').append(newOption);
            }
            if (
                sessionStorage.getItem('filterOCRFaunaSubmitter') != 'all' &&
                sessionStorage.getItem('filterOCRFaunaSubmitter') != null
            ) {
                newOption = new Option(
                    sessionStorage.getItem('filterOCRFaunaSubmitterText'),
                    vm.filterOCRFaunaSubmitter,
                    false,
                    true
                );
                $('#ocr_submitter_lookup').append(newOption);
            }
            if (
                sessionStorage.getItem('filterOCRFaunaLastModifiedBy') !=
                    'all' &&
                sessionStorage.getItem('filterOCRFaunaLastModifiedBy') != null
            ) {
                newOption = new Option(
                    sessionStorage.getItem('filterOCRFaunaLastModifiedByText'),
                    vm.filterOCRFaunaLastModifiedBy,
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
                    vm.filterOCRFaunaOccurrence = data;
                    sessionStorage.setItem(
                        'filterOCRFaunaOccurrenceText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.filterOCRFaunaOccurrence = 'all';
                    sessionStorage.setItem('filterOCRFaunaOccurrenceText', '');
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
                    vm.filterOCRFaunaScientificName = data;
                    sessionStorage.setItem(
                        'filterOCRFaunaScientificNameText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.filterOCRFaunaScientificName = 'all';
                    sessionStorage.setItem(
                        'filterOCRFaunaScientificNameText',
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
                    vm.filterOCRFaunaAssessor = data;
                    sessionStorage.setItem(
                        'filterOCRFaunaAssessorText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.filterOCRFaunaAssessor = 'all';
                    sessionStorage.setItem('filterOCRFaunaAssessorText', '');
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
                    vm.filterOCRFaunaSubmitter = data;
                    sessionStorage.setItem(
                        'filterOCRFaunaSubmitterText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.selected_referral = null;
                    vm.filterOCRFaunaSubmitter = 'all';
                    sessionStorage.setItem('filterOCRFaunaSubmitterText', '');
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
            if (vm.filterOCRFaunaRegion === 'all') {
                vm.filtered_district_list = vm.district_list;
            } else {
                vm.filtered_district_list = vm.district_list.filter(
                    (d) => d.region_id === parseInt(vm.filterOCRFaunaRegion)
                );
            }
        },
        initialiseCommonNameLookup: function () {
            let vm = this;
            $(vm.$refs.ocr_common_name_lookup)
                .select2({
                    minimumInputLength: 2,
                    dropdownParent: $('#select_common_name'),
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
                                group_type_id: vm.group_type_id,
                            };
                            return query;
                        },
                    },
                })
                .on('select2:select', function (e) {
                    let data = e.params.data.id;
                    vm.filterOCRFaunaCommonName = data;
                    sessionStorage.setItem(
                        'filterOCRFaunaCommonNameText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.filterOCRFaunaCommonName = 'all';
                    sessionStorage.setItem('filterOCRFaunaCommonNameText', '');
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-ocr_common_name_lookup-results"]'
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
                    placeholder: 'Select Occurrence Name',
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
                    vm.filterOCRFaunaOccurrenceName = data;
                    sessionStorage.setItem(
                        'filterOCRFaunaOccurrenceNameText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.filterOCRFaunaOccurrenceName = 'all';
                    sessionStorage.setItem(
                        'filterOCRFaunaOccurrenceNameText',
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
                    vm.filterOCRFaunaLastModifiedBy = data;
                    sessionStorage.setItem(
                        'filterOCRFaunaLastModifiedByText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.filterOCRFaunaLastModifiedBy = 'all';
                    sessionStorage.setItem(
                        'filterOCRFaunaLastModifiedByText',
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
        createFaunaOccurrenceReport: async function () {
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
                    let newFaunaOCRId = null;
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
                            newFaunaOCRId = data.id;
                        }
                    } catch (err) {
                        console.log(err);
                    }
                    this.$router.push({
                        name: 'internal-occurrence-report-detail',
                        params: { occurrence_report_id: newFaunaOCRId },
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
                                vm.$refs.fauna_ocr_datatable.vmDataTable.ajax.reload(
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
                                vm.$refs.fauna_ocr_datatable.vmDataTable.ajax.reload(
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
            vm.$refs.fauna_ocr_datatable.vmDataTable.on(
                'click',
                'a[data-discard-ocr-proposal]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-discard-ocr-proposal');
                    vm.discardOCRProposal(id);
                }
            );
            vm.$refs.fauna_ocr_datatable.vmDataTable.on(
                'click',
                'a[data-reinstate-ocr-proposal]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-reinstate-ocr-proposal');
                    vm.reinstateOCRProposal(id);
                }
            );
            vm.$refs.fauna_ocr_datatable.vmDataTable.on(
                'click',
                'a[data-history-occurrence-report]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-history-occurrence-report');
                    vm.historyDocument(id);
                }
            );
            vm.$refs.fauna_ocr_datatable.vmDataTable.on(
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
