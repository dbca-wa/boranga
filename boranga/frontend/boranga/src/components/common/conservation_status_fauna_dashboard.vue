<template id="species_fauna_cs_dashboard">
    <div>
        <CollapsibleFilters
            ref="collapsible_filters"
            component_title="Filters"
            class="mb-2"
            :show-warning-icon="filterApplied"
        >
            <div class="row">
                <div class="col-md-3">
                    <div id="select_scientific_name" class="form-group">
                        <label for="cs_scientific_name_lookup"
                            >Scientific Name:</label
                        >
                        <select
                            id="cs_scientific_name_lookup"
                            ref="cs_scientific_name_lookup"
                            name="cs_scientific_name_lookup"
                            class="form-control"
                        />
                    </div>
                </div>
                <div class="col-md-3">
                    <div id="select_common_name" class="form-group">
                        <label for="cs_common_name_lookup">Common Name:</label>
                        <select
                            id="cs_common_name_lookup"
                            ref="cs_common_name_lookup"
                            name="cs_common_name_lookup"
                            class="form-control"
                        />
                    </div>
                </div>
                <div class="col-md-3">
                    <div id="select_phylo_group" class="form-group">
                        <label for="cs_phylo_group_lookup"
                            >Informal Group:</label
                        >
                        <select
                            id="cs_phylo_group_lookup"
                            ref="cs_phylo_group_lookup"
                            name="cs_phylo_group_lookup"
                            class="form-control"
                        />
                    </div>
                </div>
                <div class="col-md-3">
                    <div id="select_family" class="form-group">
                        <label for="cs_family_lookup">Family:</label>
                        <select
                            id="cs_family_lookup"
                            ref="cs_family_lookup"
                            name="cs_family_lookup"
                            class="form-control"
                        />
                    </div>
                </div>
                <div class="col-md-3">
                    <div id="select_genera" class="form-group">
                        <label for="cs_genera_lookup">Genus:</label>
                        <select
                            id="cs_genera_lookup"
                            ref="cs_genera_lookup"
                            name="cs_genera_lookup"
                            class="form-control"
                        />
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="change-type">Change Type:</label>
                        <select
                            id="change-type"
                            v-model="filterCSFaunaChangeCode"
                            class="form-select"
                        >
                            <option value="all">All</option>
                            <option
                                v-for="change_code in change_codes"
                                :value="change_code.id"
                                :key="change_code.id"
                            >
                                {{ change_code.code }}
                            </option>
                        </select>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="wa-legislative-list"
                            >WA Legislative List:</label
                        >
                        <select
                            id="wa-legislative-list"
                            v-model="filterCSFaunaWALegislativeList"
                            class="form-select"
                        >
                            <option value="all">All</option>
                            <option
                                v-for="list in wa_legislative_lists"
                                :value="list.id"
                                :key="list.id"
                            >
                                {{ list.code }}
                            </option>
                        </select>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="wa-legislative-category"
                            >WA Legislative Category:</label
                        >
                        <select
                            id="wa-legislative-category"
                            v-model="filterCSFaunaWALegislativeCategory"
                            class="form-select"
                        >
                            <option value="all">All</option>
                            <option
                                v-for="list in wa_legislative_categories"
                                :value="list.id"
                                :key="list.id"
                            >
                                {{ list.code }}
                            </option>
                        </select>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="wa-priority-category"
                            >WA Priority Category:</label
                        >
                        <select
                            id="wa-priority-category"
                            v-model="filterCSFaunaWAPriorityCategory"
                            class="form-select"
                        >
                            <option value="all">All</option>
                            <option
                                v-for="list in wa_priority_categories"
                                :value="list.id"
                                :key="list.id"
                            >
                                {{ list.code }}
                            </option>
                        </select>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label
                            class="form-check-label"
                            for="commonwealth-relevance"
                            >Commonwealth Relevance</label
                        >
                        <div class="form-check form-switch mt-1">
                            <input
                                id="commonwealth-relevance"
                                v-model="filterCSFaunaCommonwealthRelevance"
                                class="form-check-input"
                                type="checkbox"
                                true-value="true"
                                false-value="false"
                            />
                        </div>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label
                            class="form-check-label"
                            for="international-relevance"
                            >International Relevance</label
                        >
                        <div class="form-check form-switch mt-1">
                            <input
                                id="international-relevance"
                                v-model="filterCSFaunaInternationalRelevance"
                                class="form-check-input"
                                type="checkbox"
                                true-value="true"
                                false-value="false"
                            />
                        </div>
                    </div>
                </div>
                <div v-show="!is_for_agenda" class="col-md-3">
                    <div class="form-group">
                        <label for="">Status:</label>
                        <select
                            v-model="filterCSFaunaApplicationStatus"
                            class="form-select"
                        >
                            <option value="all">All</option>
                            <option
                                v-for="status in processing_statuses"
                                :value="status.value"
                                :class="status.className"
                                :key="status.value"
                            >
                                <template
                                    v-if="status.className == 'optionChild'"
                                    >&nbsp;&nbsp;</template
                                >{{ status.name }}
                            </option>
                        </select>
                    </div>
                </div>
                <div class="col-md-3">
                    <div id="select_assessor" class="form-group">
                        <label for="cs_assessor_lookup">Assessor:</label>
                        <select
                            id="cs_assessor_lookup"
                            ref="cs_assessor_lookup"
                            name="cs_assessor_lookup"
                            class="form-control"
                        />
                    </div>
                </div>
                <div class="col-md-3">
                    <div id="select_submitter" class="form-group">
                        <label for="cs_submitter_lookup">Submitter:</label>
                        <select
                            id="cs_submitter_lookup"
                            ref="cs_submitter_lookup"
                            name="cs_submitter_lookup"
                            class="form-control"
                        />
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="submitter-category"
                            >Submitter Category:</label
                        >
                        <select
                            id="submitter-category"
                            v-model="filterCSFaunaSubmitterCategory"
                            class="form-select"
                        >
                            <option value="all">All</option>
                            <option
                                v-for="submitter_category in submitter_categories"
                                :value="submitter_category.id"
                                :key="submitter_category.id"
                            >
                                {{ submitter_category.name }}
                            </option>
                        </select>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="submitter-category">Locked:</label>
                        <select
                            id="submitter-category"
                            v-model="filterCSFaunaLocked"
                            class="form-select"
                        >
                            <option value="all">All</option>
                            <option value="true">Yes</option>
                            <option value="false">No</option>
                        </select>
                    </div>
                </div>
                <div v-show="!is_for_agenda" class="col-md-6">
                    <label for="" class="form-label px-2"
                        >Effective From Date Range:</label
                    >
                    <div class="input-group px-2 mb-2">
                        <span class="input-group-text">From </span>
                        <input
                            id="from_effective_from_date"
                            v-model="filterCSFromFaunaEffectiveFromDate"
                            type="date"
                            class="form-control"
                            placeholder="DD/MM/YYYY"
                        />
                        <span class="input-group-text"> to </span>
                        <input
                            id="to_effective_from_date"
                            v-model="filterCSToFaunaEffectiveFromDate"
                            type="date"
                            class="form-control"
                            placeholder="DD/MM/YYYY"
                        />
                    </div>
                </div>
                <div v-show="!is_for_agenda" class="col-md-6">
                    <label for="" class="form-label px-2"
                        >Effective To Date Range:</label
                    >
                    <div class="input-group px-2">
                        <span class="input-group-text">From </span>
                        <input
                            id="from_effective_to_date"
                            v-model="filterCSFromFaunaEffectiveToDate"
                            type="date"
                            class="form-control"
                            placeholder="DD/MM/YYYY"
                        />
                        <span class="input-group-text"> to </span>
                        <input
                            id="to_effective_to_date"
                            v-model="filterCSToFaunaEffectiveToDate"
                            type="date"
                            class="form-control"
                            placeholder="DD/MM/YYYY"
                        />
                    </div>
                </div>
                <div v-show="!is_for_agenda" class="col-md-6">
                    <label for="from_review_due_date" class="form-label px-2"
                        >Review Due Date Range:</label
                    >
                    <div class="input-group px-2">
                        <span class="input-group-text">From </span>
                        <input
                            id="from_review_due_date"
                            v-model="filterCSFromFaunaReviewDueDate"
                            type="date"
                            class="form-control"
                            placeholder="DD/MM/YYYY"
                        />
                        <span class="input-group-text"> to </span>
                        <input
                            id="to_review_due_date"
                            v-model="filterCSToFaunaReviewDueDate"
                            type="date"
                            class="form-control"
                            placeholder="DD/MM/YYYY"
                        />
                    </div>
                </div>
            </div>
        </CollapsibleFilters>

        <div
            v-if="addFaunaCSVisibility && is_for_agenda == false"
            class="col-md-12"
        >
            <div class="text-end">
                <button
                    type="button"
                    class="btn btn-primary mb-2"
                    @click.prevent="createFaunaConservationStatus"
                >
                    <i class="fa-solid fa-circle-plus"></i> Propose Conservation
                    Status
                </button>
            </div>
        </div>

        <div class="row">
            <div class="col-lg-12">
                <datatable
                    :id="datatable_id"
                    ref="fauna_cs_datatable"
                    :dt-options="datatable_options"
                    :dt-headers="datatable_headers"
                />
            </div>
            <div v-if="speciesConservationStatusHistoryId">
                <SpeciesConservationStatusHistory
                    ref="species_conservation_status_history"
                    :key="speciesConservationStatusHistoryId"
                    :conservation-status-id="speciesConservationStatusHistoryId"
                    :species-id="speciesHistoryId"
                    :conservation-list-id="listHistoryId"
                />
            </div>
        </div>
    </div>
</template>
<script>
import { v4 as uuid } from 'uuid';
import datatable from '@/utils/vue/datatable.vue';
import CollapsibleFilters from '@/components/forms/collapsible_component.vue';
import SpeciesConservationStatusHistory from '../internal/conservation_status/species_conservation_status_history.vue';

import { api_endpoints, constants, helpers } from '@/utils/hooks';
export default {
    name: 'ConservationStatusFaunaTable',
    components: {
        datatable,
        CollapsibleFilters,
        SpeciesConservationStatusHistory,
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
        // when the datable need to be shown for agenda_items in meeting check this variable is true
        is_for_agenda: {
            type: Boolean,
            default: false,
        },
        // for adding agendaitems for the meeting_obj.id
        meeting_obj: {
            type: Object,
            required: false,
        },
        filterCSFaunaScientificName_cache: {
            type: String,
            required: false,
            default: 'filterCSFaunaScientificName',
        },
        filterCSFaunaCommonName_cache: {
            type: String,
            required: false,
            default: 'filterCSFaunaCommonName',
        },
        filterCSFaunaPhylogeneticGroup_cache: {
            type: String,
            required: false,
            default: 'filterCSFaunaPhylogeneticGroup',
        },
        filterCSFaunaFamily_cache: {
            type: String,
            required: false,
            default: 'filterCSFaunaFamily',
        },
        filterCSFaunaGenus_cache: {
            type: String,
            required: false,
            default: 'filterCSFaunaGenus',
        },
        filterCSFaunaChangeCode_cache: {
            type: String,
            required: false,
            default: 'filterCSFaunaChangeCode',
        },
        filterCSFaunaWALegislativeList_cache: {
            type: String,
            required: false,
            default: 'filterCSFaunaWALegislativeList',
        },
        filterCSFaunaWALegislativeCategory_cache: {
            type: String,
            required: false,
            default: 'filterCSFaunaWALegislativeCategory',
        },
        filterCSFaunaWAPriorityCategory_cache: {
            type: String,
            required: false,
            default: 'filterCSFaunaWAPriorityCategory',
        },
        filterCSFaunaCommonwealthRelevance_cache: {
            type: String,
            required: false,
            default: 'filterCSFaunaCommonwealthRelevance',
        },
        filterCSFaunaInternationalRelevance_cache: {
            type: String,
            required: false,
            default: 'filterCSFaunaInternationalRelevance',
        },
        filterCSFaunaAssessor_cache: {
            type: String,
            required: false,
            default: 'filterCSFaunaAssessor',
        },
        filterCSFaunaSubmitter_cache: {
            type: String,
            required: false,
            default: 'filterCSFaunaSubmitter',
        },
        filterCSFaunaSubmitterCategory_cache: {
            type: String,
            required: false,
            default: 'filterCSFaunaSubmitterCategory',
        },
        filterCSFaunaApplicationStatus_cache: {
            type: String,
            required: false,
            default: 'filterCSFaunaApplicationStatus',
        },
        filterCSFromFaunaEffectiveFromDate_cache: {
            type: String,
            required: false,
            default: 'filterCSFromFaunaEffectiveFromDate',
        },
        filterCSToFaunaEffectiveFromDate_cache: {
            type: String,
            required: false,
            default: 'filterCSToFaunaEffectiveFromDate',
        },
        filterCSFromFaunaEffectiveToDate_cache: {
            type: String,
            required: false,
            default: 'filterCSFromFaunaEffectiveToDate',
        },
        filterCSToFaunaEffectiveToDate_cache: {
            type: String,
            required: false,
            default: 'filterCSToFaunaEffectiveToDate',
        },
        filterCSFromFaunaReviewDueDate_cache: {
            type: String,
            required: false,
            default: 'filterCSFromFaunaReviewDueDate',
        },
        filterCSToFaunaReviewDueDate_cache: {
            type: String,
            required: false,
            default: 'filterCSToFaunaReviewDueDate',
        },
        filterCSFaunaLocked_cache: {
            type: String,
            required: false,
            default: 'filterCSFaunaLocked',
        },
    },
    data() {
        return {
            datatable_id: 'species_fauna_cs-datatable-' + uuid(),
            speciesConservationStatusHistoryId: null,
            speciesHistoryId: null,
            listHistoryId: null,

            // selected values for filtering
            filterCSFaunaScientificName: sessionStorage.getItem(
                this.filterCSFaunaScientificName_cache
            )
                ? sessionStorage.getItem(this.filterCSFaunaScientificName_cache)
                : 'all',

            filterCSFaunaCommonName: sessionStorage.getItem(
                this.filterCSFaunaCommonName_cache
            )
                ? sessionStorage.getItem(this.filterCSFaunaCommonName_cache)
                : 'all',

            filterCSFaunaPhylogeneticGroup: sessionStorage.getItem(
                this.filterCSFaunaPhylogeneticGroup_cache
            )
                ? sessionStorage.getItem(
                      this.filterCSFaunaPhylogeneticGroup_cache
                  )
                : 'all',

            filterCSFaunaFamily: sessionStorage.getItem(
                this.filterCSFaunaFamily_cache
            )
                ? sessionStorage.getItem(this.filterCSFaunaFamily_cache)
                : 'all',

            filterCSFaunaGenus: sessionStorage.getItem(
                this.filterCSFaunaGenus_cache
            )
                ? sessionStorage.getItem(this.filterCSFaunaGenus_cache)
                : 'all',

            filterCSFaunaChangeCode: sessionStorage.getItem(
                this.filterCSFaunaChangeCode_cache
            )
                ? sessionStorage.getItem(this.filterCSFaunaChangeCode_cache)
                : 'all',

            filterCSFaunaWALegislativeList: sessionStorage.getItem(
                this.filterCSFaunaWALegislativeList_cache
            )
                ? sessionStorage.getItem(
                      this.filterCSFaunaWALegislativeList_cache
                  )
                : 'all',

            filterCSFaunaWALegislativeCategory: sessionStorage.getItem(
                this.filterCSFaunaWALegislativeCategory_cache
            )
                ? sessionStorage.getItem(
                      this.filterCSFaunaWALegislativeCategory_cache
                  )
                : 'all',

            filterCSFaunaWAPriorityCategory: sessionStorage.getItem(
                this.filterCSFaunaWAPriorityCategory_cache
            )
                ? sessionStorage.getItem(
                      this.filterCSFaunaWAPriorityCategory_cache
                  )
                : 'all',

            filterCSFaunaCommonwealthRelevance: sessionStorage.getItem(
                this.filterCSFaunaCommonwealthRelevance_cache
            )
                ? sessionStorage.getItem(
                      this.filterCSFaunaCommonwealthRelevance_cache
                  )
                : 'false',

            filterCSFaunaInternationalRelevance: sessionStorage.getItem(
                this.filterCSFaunaInternationalRelevance_cache
            )
                ? sessionStorage.getItem(
                      this.filterCSFaunaInternationalRelevance_cache
                  )
                : 'false',

            filterCSFaunaAssessor: sessionStorage.getItem(
                this.filterCSFaunaAssessor_cache
            )
                ? sessionStorage.getItem(this.filterCSFaunaAssessor_cache)
                : 'all',

            filterCSFaunaSubmitter: sessionStorage.getItem(
                this.filterCSFaunaSubmitter_cache
            )
                ? sessionStorage.getItem(this.filterCSFaunaSubmitter_cache)
                : 'all',

            filterCSFaunaSubmitterCategory: sessionStorage.getItem(
                this.filterCSFaunaSubmitterCategory_cache
            )
                ? sessionStorage.getItem(
                      this.filterCSFaunaSubmitterCategory_cache
                  )
                : 'all',

            //filterCSFaunaApplicationStatus: sessionStorage.getItem(this.filterCSFaunaApplicationStatus_cache) ?
            //    sessionStorage.getItem(this.filterCSFaunaApplicationStatus_cache) : 'approved',
            filterCSFaunaApplicationStatus: sessionStorage.getItem(
                this.filterCSFaunaApplicationStatus_cache
            )
                ? sessionStorage.getItem(
                      this.filterCSFaunaApplicationStatus_cache
                  )
                : this.is_for_agenda
                  ? 'ready_for_agenda'
                  : 'approved',

            filterCSFromFaunaEffectiveFromDate: sessionStorage.getItem(
                this.filterCSFromFaunaEffectiveFromDate_cache
            )
                ? sessionStorage.getItem(
                      this.filterCSFromFaunaEffectiveFromDate_cache
                  )
                : '',
            filterCSToFaunaEffectiveFromDate: sessionStorage.getItem(
                this.filterCSToFaunaEffectiveFromDate_cache
            )
                ? sessionStorage.getItem(
                      this.filterCSToFaunaEffectiveFromDate_cache
                  )
                : '',

            filterCSFromFaunaEffectiveToDate: sessionStorage.getItem(
                this.filterCSFromFaunaEffectiveToDate_cache
            )
                ? sessionStorage.getItem(
                      this.filterCSFromFaunaEffectiveToDate_cache
                  )
                : '',
            filterCSToFaunaEffectiveToDate: sessionStorage.getItem(
                this.filterCSToFaunaEffectiveToDate_cache
            )
                ? sessionStorage.getItem(
                      this.filterCSToFaunaEffectiveToDate_cache
                  )
                : '',

            filterCSFromFaunaReviewDueDate: sessionStorage.getItem(
                this.filterCSFromFaunaReviewDueDate_cache
            )
                ? sessionStorage.getItem(
                      this.filterCSFromFaunaReviewDueDate_cache
                  )
                : '',
            filterCSToFaunaReviewDueDate: sessionStorage.getItem(
                this.filterCSToFaunaReviewDueDate_cache
            )
                ? sessionStorage.getItem(
                      this.filterCSToFaunaReviewDueDate_cache
                  )
                : '',

            filterCSFaunaLocked: sessionStorage.getItem(
                this.filterCSFaunaLocked_cache
            )
                ? sessionStorage.getItem(this.filterCSFaunaLocked_cache)
                : 'all',

            //Filter list for scientific name and common name
            filterListsSpecies: {},
            scientific_name_list: [],
            common_name_list: [],
            family_list: [],
            change_codes: [],
            submitter_categories: [],
            wa_legislative_lists: [],
            wa_legislative_categories: [],
            wa_priority_categories: [],

            // filtering options
            external_status: [
                { value: 'draft', name: 'Draft' },
                { value: 'with_assessor', name: 'Under Review' },
                { value: 'approved', name: 'Approved' },
                { value: 'declined', name: 'Declined' },
                { value: 'discarded', name: 'Discarded' },
                { value: 'awaiting_payment', name: 'Awaiting Payment' },
            ],
            processing_statuses: [
                { value: 'draft', name: 'Draft', className: '' },
                { value: 'discarded', name: 'Discarded', className: '' },
                {
                    value: 'active',
                    name: 'Active',
                    className: 'optionGroup',
                },
                {
                    value: 'approved',
                    name: 'Approved',
                    className: 'optionChild',
                },
                {
                    value: 'delisted',
                    name: 'Delisted',
                    className: 'optionChild',
                },
                {
                    value: 'awaiting_assessor_action',
                    name: 'Awaiting Assessor Action',
                    className: 'optionGroup',
                },
                {
                    value: 'with_assessor',
                    name: 'With Assessor',
                    className: 'optionChild',
                },
                {
                    value: 'with_referral',
                    name: 'With Referral',
                    className: 'optionChild',
                },
                {
                    value: 'deferred',
                    name: 'Deferred',
                    className: 'optionChild',
                },
                {
                    value: 'awaiting_approver_action',
                    name: 'Awaiting Approver Action',
                    className: 'optionGroup',
                },
                {
                    value: 'proposed_for_agenda',
                    name: 'Proposed For Agenda',
                    className: 'optionChild',
                },
                {
                    value: 'ready_for_agenda',
                    name: 'Ready For Agenda',
                    className: 'optionChild',
                },
                {
                    value: 'on_agenda',
                    name: 'On Agenda',
                    className: 'optionChild',
                },
                {
                    value: 'deferred',
                    name: 'Deferred',
                    className: 'optionChild',
                },
                {
                    value: 'inactive',
                    name: 'Inactive',
                    className: 'optionGroup',
                },
                {
                    value: 'declined',
                    name: 'Declined',
                    className: 'optionChild',
                },
                { value: 'closed', name: 'Closed', className: 'optionChild' },
            ],
        };
    },
    computed: {
        defaultApplicationStatus: function () {
            return this.is_for_agenda ? 'ready_for_agenda' : 'all';
        },
        filterApplied: function () {
            if (
                this.filterCSFaunaScientificName === 'all' &&
                this.filterCSFaunaCommonName === 'all' &&
                this.filterCSFaunaPhylogeneticGroup === 'all' &&
                this.filterCSFaunaFamily === 'all' &&
                this.filterCSFaunaGenus === 'all' &&
                this.filterCSFaunaChangeCode === 'all' &&
                this.filterCSFaunaWALegislativeList === 'all' &&
                this.filterCSFaunaWALegislativeCategory === 'all' &&
                this.filterCSFaunaWAPriorityCategory === 'all' &&
                this.filterCSFaunaCommonwealthRelevance === 'false' &&
                this.filterCSFaunaInternationalRelevance === 'false' &&
                this.filterCSFaunaAssessor === 'all' &&
                this.filterCSFaunaSubmitter === 'all' &&
                this.filterCSFaunaSubmitterCategory === 'all' &&
                this.filterCSFaunaApplicationStatus ===
                    this.defaultApplicationStatus &&
                this.filterCSFromFaunaEffectiveFromDate === '' &&
                this.filterCSToFaunaEffectiveFromDate === '' &&
                this.filterCSFromFaunaEffectiveToDate === '' &&
                this.filterCSToFaunaEffectiveToDate === '' &&
                this.filterCSFromFaunaReviewDueDate === '' &&
                this.filterCSToFaunaReviewDueDate === '' &&
                this.filterCSFaunaLocked === 'all'
            ) {
                return false;
            } else {
                return true;
            }
        },
        is_referral: function () {
            return this.level == 'referral';
        },
        addFaunaCSVisibility: function () {
            return (
                this.profile?.user &&
                this.profile.user.groups.find((i) =>
                    [constants.GROUPS.INTERNAL_CONTRIBUTORS].includes(i)
                )
            );
        },
        datatable_headers: function () {
            return [
                'Number',
                'Species',
                'Scientific Name',
                'Common Name',
                'Family',
                'Genus',
                'Informal Group(s)',
                'Change Type',
                'WA Priority List',
                'WA Priority Category',
                'WA Legislative List',
                'WA Legislative Category',
                'Commonwealth Conservation Category',
                'Other Conservation Assessment',
                'Conservation Criteria',
                'Submitter Name',
                'Submitter Category',
                'Submitter Organisation',
                'Assessor Name',
                'Effective From Date',
                'Effective To Date',
                'Review Due Date',
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
                data: 'conservation_status_number',
                orderable: true,
                searchable: true,
                visible: true,
                render: function (data, type, full) {
                    let value = full.conservation_status_number;
                    if (full.is_new_contributor) {
                        value +=
                            ' <span class="badge bg-warning">New Contributor</span>';
                    }
                    return value;
                },
                name: 'id',
            };
        },
        column_species_number: function () {
            return {
                data: 'species_number',
                orderable: true,
                searchable: true,
                visible: true,
                render: function (data, type, full) {
                    return full.species_number;
                },
                name: 'species__species_number',
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
        column_common_name: function () {
            return {
                data: 'common_name',
                orderable: true,
                searchable: true,
                visible: true,
                render: function (value, type) {
                    let result = helpers.dtPopover(value, 30, 'hover');
                    return type == 'export' ? value : result;
                },
                name: 'species__taxonomy__vernaculars__vernacular_name',
            };
        },
        column_family: function () {
            return {
                data: 'family',
                orderable: true,
                searchable: true,
                visible: true,
                render: function (value, type) {
                    let result = helpers.dtPopover(value, 30, 'hover');
                    return type == 'export' ? value : result;
                },
                name: 'species__taxonomy__family_name',
            };
        },
        column_genera: function () {
            return {
                data: 'genus',
                orderable: true,
                searchable: true,
                visible: true,
                render: function (value, type) {
                    let result = helpers.dtPopover(value, 30, 'hover');
                    return type == 'export' ? value : result;
                },
                name: 'species__taxonomy__genera_name',
            };
        },
        column_phylo_group: function () {
            return {
                data: 'informal_group',
                orderable: true,
                searchable: false,
                visible: true,
                name: 'species_taxonomy__informal_group',
                render: function (data, type, full) {
                    let html = '';
                    if (full.informal_group) {
                        for (let i = 0; i < full.informal_group.length; i++) {
                            html += `<span class="badge bg-primary me-2">${full.informal_group[i]}</span>`;
                        }
                    }
                    return html;
                },
            };
        },
        column_change_code: function () {
            return {
                data: 'change_code',
                orderable: true,
                searchable: false,
                visible: true,
                name: 'change_code__code',
            };
        },
        column_wa_priority_list: function () {
            return {
                data: 'wa_priority_list',
                orderable: true,
                searchable: false,
                visible: true,
                name: 'wa_priority_list__code',
            };
        },
        column_wa_priority_category: function () {
            return {
                data: 'wa_priority_category',
                orderable: true,
                searchable: false,
                visible: true,
                name: 'wa_priority_category__code',
            };
        },
        column_wa_legislative_list: function () {
            return {
                data: 'wa_legislative_list',
                orderable: true,
                searchable: false,
                visible: true,
                name: 'wa_legislative_list__code',
            };
        },
        column_wa_legislative_category: function () {
            return {
                data: 'wa_legislative_category',
                orderable: true,
                searchable: false,
                visible: true,
                name: 'wa_legislative_category__code',
            };
        },
        column_commonwealth_conservation_category: function () {
            return {
                data: 'commonwealth_conservation_category',
                orderable: true,
                searchable: false,
                visible: true,
                name: 'commonwealth_conservation_category',
            };
        },
        column_other_conservation_assessment: function () {
            return {
                data: 'other_conservation_assessment',
                orderable: true,
                searchable: true,
                visible: true,
                name: 'other_conservation_assessment__code',
            };
        },
        column_conservation_criteria: function () {
            return {
                data: 'conservation_criteria',
                orderable: true,
                searchable: true,
                visible: true,
                name: 'conservation_criteria',
            };
        },
        column_submitter_name: function () {
            return {
                data: 'submitter_name',
                orderable: true,
                searchable: true,
                visible: true,
                name: 'submitter_information__name',
            };
        },
        column_submitter_category: function () {
            return {
                data: 'submitter_category',
                orderable: true,
                searchable: false,
                visible: true,
                name: 'submitter_information__submitter_category__name',
            };
        },
        column_submitter_organisation: function () {
            return {
                data: 'submitter_organisation',
                orderable: true,
                searchable: true,
                visible: true,
                name: 'submitter_information__organisation',
            };
        },
        column_assessor_name: function () {
            return {
                data: 'assessor_name',
                orderable: true,
                searchable: false,
                visible: true,
            };
        },
        column_status: function () {
            return {
                // 9. Workflow Status
                data: 'processing_status',
                orderable: true,
                searchable: true,
                visible: true,
                render: function (data, type, full) {
                    let html = full.processing_status;
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
                name: 'processing_status',
            };
        },
        column_effective_from: function () {
            return {
                data: 'effective_from',
                orderable: true,
                searchable: true,
                visible: true,
                name: 'effective_from',
            };
        },
        column_effective_to: function () {
            return {
                data: 'effective_to',
                orderable: true,
                searchable: true,
                visible: true,
                name: 'effective_to',
            };
        },
        column_review_due_date: function () {
            return {
                data: 'review_due_date',
                orderable: true,
                searchable: false,
                visible: true,
            };
        },
        column_action: function () {
            let vm = this;
            return {
                // 10. Action
                data: 'id',
                orderable: false,
                searchable: false,
                visible: true,
                render: function (data, type, full) {
                    let links = '';
                    if (vm.is_for_agenda == false) {
                        if (full.internal_user_edit) {
                            links += `<a href='/internal/conservation-status/${full.id}'>Continue</a><br/>`;
                            links += `<a href='#${full.id}' data-discard-cs-proposal='${full.id}'>Discard</a><br/>`;
                            links += `<a href='#' data-history-conservation-status-species='${full.id}'
                                data-history-species='${full.species_number}'
                                data-history-conservation-list='${full.conservation_list}'>History</a><br>`;
                        } else {
                            if (full.assessor_process) {
                                links += `<a href='/internal/conservation-status/${full.id}'>Process</a><br/>`;
                                links += `<a href='#' data-history-conservation-status-species='${full.id}'
                                        data-history-species='${full.species_number}'
                                        data-history-conservation-list='${full.conservation_list}'>History</a><br>`;
                            } else {
                                if (
                                    full.processing_status ==
                                    constants.PROPOSAL_STATUS.DISCARDED.TEXT
                                ) {
                                    links += `<a href='#' data-reinstate-conservation-status-species='${full.id}'>Reinstate</a><br/>`;
                                }
                                links += `<a href='/internal/conservation-status/${full.id}?action=view'>View</a><br/>`;
                                links += `<a href='#' data-history-conservation-status-species='${full.id}'
                                    data-history-species='${full.species_number}'
                                    data-history-conservation-list='${full.conservation_list}'>History</a><br>`;
                            }
                        }
                    } else {
                        if (vm.meeting_obj.agenda_items_arr.includes(full.id)) {
                            links += `<a>Added</a><br/>`;
                        } else {
                            links += `<a href='#${full.id}' data-add-to-agenda='${full.id}'>Add</a><br/>`;
                        }
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
                    title: 'Boranga CS Fauna Excel Export',
                    text: '<i class="fa-solid fa-download"></i> Excel',
                    className: 'btn btn-primary me-2 rounded',
                    exportOptions: {
                        columns: ':not(.no-export)',
                        orthogonal: 'export',
                    },
                },
                {
                    extend: 'csv',
                    title: 'Boranga CS Fauna CSV Export',
                    text: '<i class="fa-solid fa-download"></i> CSV',
                    className: 'btn btn-primary rounded',
                    exportOptions: {
                        columns: ':not(.no-export)',
                        orthogonal: 'export',
                    },
                },
            ];

            columns = [
                vm.column_number,
                vm.column_species_number,
                vm.column_scientific_name,
                vm.column_common_name,
                vm.column_family,
                vm.column_genera,
                vm.column_phylo_group,
                vm.column_change_code,
                vm.column_wa_priority_list,
                vm.column_wa_priority_category,
                vm.column_wa_legislative_list,
                vm.column_wa_legislative_category,
                vm.column_commonwealth_conservation_category,
                vm.column_other_conservation_assessment,
                vm.column_conservation_criteria,
                vm.column_submitter_name,
                vm.column_submitter_category,
                vm.column_submitter_organisation,
                vm.column_assessor_name,
                vm.column_effective_from,
                vm.column_effective_to,
                vm.column_review_due_date,
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
                    { responsivePriority: 1, targets: 0 },
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
                    // adding extra GET params for Custom filtering
                    data: function (d) {
                        d.filter_group_type = vm.group_type_name;
                        d.filter_scientific_name =
                            vm.filterCSFaunaScientificName;
                        d.filter_common_name = vm.filterCSFaunaCommonName;
                        d.filter_informal_group =
                            vm.filterCSFaunaPhylogeneticGroup;
                        d.filter_family = vm.filterCSFaunaFamily;
                        d.filter_genus = vm.filterCSFaunaGenus;
                        d.filter_change_code = vm.filterCSFaunaChangeCode;
                        d.filter_wa_legislative_list =
                            vm.filterCSFaunaWALegislativeList;
                        d.filter_wa_legislative_category =
                            vm.filterCSFaunaWALegislativeCategory;
                        d.filter_wa_priority_category =
                            vm.filterCSFaunaWAPriorityCategory;
                        d.filter_commonwealth_relevance =
                            vm.filterCSFaunaCommonwealthRelevance;
                        d.filter_international_relevance =
                            vm.filterCSFaunaInternationalRelevance;
                        d.filter_assessor = vm.filterCSFaunaAssessor;
                        d.filter_submitter = vm.filterCSFaunaSubmitter;
                        d.filter_submitter_category =
                            vm.filterCSFaunaSubmitterCategory;
                        d.filter_application_status =
                            vm.filterCSFaunaApplicationStatus;
                        d.filter_from_effective_from_date =
                            vm.filterCSFromFaunaEffectiveFromDate;
                        d.filter_to_effective_from_date =
                            vm.filterCSToFaunaEffectiveFromDate;
                        d.filter_from_effective_to_date =
                            vm.filterCSFromFaunaEffectiveToDate;
                        d.filter_to_effective_to_date =
                            vm.filterCSToFaunaEffectiveToDate;
                        d.filter_from_review_due_date =
                            vm.filterCSFromFaunaReviewDueDate;
                        d.filter_to_review_due_date =
                            vm.filterCSToFaunaReviewDueDate;
                        d.filter_locked = vm.filterCSFaunaLocked;
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
        filterCSFaunaScientificName: function () {
            let vm = this;
            vm.$refs.fauna_cs_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterCSFaunaScientificName_cache,
                vm.filterCSFaunaScientificName
            );
        },
        filterCSFaunaCommonName: function () {
            let vm = this;
            vm.$refs.fauna_cs_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterCSFaunaCommonName_cache,
                vm.filterCSFaunaCommonName
            );
        },
        filterCSFaunaPhylogeneticGroup: function () {
            let vm = this;
            vm.$refs.fauna_cs_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterCSFaunaPhylogeneticGroup_cache,
                vm.filterCSFaunaPhylogeneticGroup
            );
        },
        filterCSFaunaFamily: function () {
            let vm = this;
            vm.$refs.fauna_cs_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterCSFaunaFamily_cache,
                vm.filterCSFaunaFamily
            );
        },
        filterCSFaunaGenus: function () {
            let vm = this;
            vm.$refs.fauna_cs_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterCSFaunaGenus_cache,
                vm.filterCSFaunaGenus
            );
        },
        filterCSFaunaChangeCode: function () {
            let vm = this;
            vm.$refs.fauna_cs_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterCSFaunaChangeCode_cache,
                vm.filterCSFaunaChangeCode
            );
        },
        filterCSFaunaWALegislativeList: function () {
            let vm = this;
            vm.$refs.fauna_cs_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterCSFaunaWALegislativeList_cache,
                vm.filterCSFaunaWALegislativeList
            );
        },
        filterCSFaunaWALegislativeCategory: function () {
            let vm = this;
            vm.$refs.fauna_cs_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterCSFaunaWALegislativeCategory_cache,
                vm.filterCSFaunaWALegislativeCategory
            );
        },
        filterCSFaunaWAPriorityCategory: function () {
            let vm = this;
            vm.$refs.fauna_cs_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterCSFaunaWAPriorityCategory_cache,
                vm.filterCSFaunaWAPriorityCategory
            );
        },
        filterCSFaunaCommonwealthRelevance: function () {
            let vm = this;
            vm.$refs.fauna_cs_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterCSFaunaCommonwealthRelevance_cache,
                vm.filterCSFaunaCommonwealthRelevance
            );
        },
        filterCSFaunaInternationalRelevance: function () {
            let vm = this;
            vm.$refs.fauna_cs_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterCSFaunaInternationalRelevance_cache,
                vm.filterCSFaunaInternationalRelevance
            );
        },
        filterCSFaunaAssessor: function () {
            let vm = this;
            vm.$refs.fauna_cs_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterCSFaunaAssessor_cache,
                vm.filterCSFaunaAssessor
            );
        },
        filterCSFaunaSubmitter: function () {
            let vm = this;
            vm.$refs.fauna_cs_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterCSFaunaSubmitter_cache,
                vm.filterCSFaunaSubmitter
            );
        },
        filterCSFaunaSubmitterCategory: function () {
            let vm = this;
            vm.$refs.fauna_cs_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterCSFaunaSubmitterCategory_cache,
                vm.filterCSFaunaSubmitterCategory
            );
        },
        filterCSFromFaunaEffectiveFromDate: function () {
            let vm = this;
            vm.$refs.fauna_cs_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterCSFromFaunaEffectiveFromDate_cache,
                vm.filterCSFromFaunaEffectiveFromDate
            );
        },
        filterCSToFaunaEffectiveFromDate: function () {
            let vm = this;
            vm.$refs.fauna_cs_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterCSToFaunaEffectiveFromDate_cache,
                vm.filterCSToFaunaEffectiveFromDate
            );
        },
        filterCSFromFaunaEffectiveToDate: function () {
            let vm = this;
            vm.$refs.fauna_cs_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterCSFromFaunaEffectiveToDate_cache,
                vm.filterCSFromFaunaEffectiveToDate
            );
        },
        filterCSToFaunaEffectiveToDate: function () {
            let vm = this;
            vm.$refs.fauna_cs_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterCSToFaunaEffectiveToDate_cache,
                vm.filterCSToFaunaEffectiveToDate
            );
        },
        filterCSFromFaunaReviewDueDate: function () {
            let vm = this;
            vm.$refs.fauna_cs_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterCSFromFaunaReviewDueDate_cache,
                vm.filterCSFromFaunaReviewDueDate
            );
        },
        filterCSToFaunaReviewDueDate: function () {
            let vm = this;
            vm.$refs.fauna_cs_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterCSToFaunaReviewDueDate_cache,
                vm.filterCSToFaunaReviewDueDate
            );
        },
        filterCSFaunaLocked: function () {
            let vm = this;
            vm.$refs.fauna_cs_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterCSFaunaLocked_cache,
                vm.filterCSFaunaLocked
            );
        },
        filterCSFaunaApplicationStatus: function () {
            let vm = this;
            vm.$refs.fauna_cs_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterCSFaunaApplicationStatus_cache,
                vm.filterCSFaunaApplicationStatus
            );
        },
    },
    mounted: function () {
        let vm = this;
        if (
            vm.profile?.user &&
            vm.profile.user.groups.includes(
                constants.GROUPS.INTERNAL_CONTRIBUTORS
            )
        ) {
            vm.processing_statuses.splice(2, 0, {
                value: 'discarded_by_me',
                name: 'Discarded By Me',
                className: '',
            });
        }
        vm.fetchFilterLists();
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
            vm.initialiseCommonNameLookup();
            vm.initialisePhyloGroupLookup();
            vm.initialiseFamilyLookup();
            vm.initialiseGeneraLookup();
            vm.initialiseAssessorLookup();
            vm.initialiseSubmitterLookup();
            vm.addEventListeners();
            var newOption = null;
            // -- to set the select2 field with the session value if exists onload()
            if (
                sessionStorage.getItem('filterCSFaunaScientificName') !=
                    'all' &&
                sessionStorage.getItem('filterCSFaunaScientificName') != null
            ) {
                newOption = new Option(
                    sessionStorage.getItem('filterCSFaunaScientificNameText'),
                    vm.filterCSFaunaScientificName,
                    false,
                    true
                );
                $('#cs_scientific_name_lookup').append(newOption);
            }
            if (
                sessionStorage.getItem('filterCSFaunaCommonName') != 'all' &&
                sessionStorage.getItem('filterCSFaunaCommonName') != null
            ) {
                newOption = new Option(
                    sessionStorage.getItem('filterCSFaunaCommonNameText'),
                    vm.filterCSFaunaCommonName,
                    false,
                    true
                );
                $('#cs_common_name_lookup').append(newOption);
            }
            if (
                sessionStorage.getItem('filterCSFaunaPhylogeneticGroup') !=
                    'all' &&
                sessionStorage.getItem('filterCSFaunaPhylogeneticGroup') != null
            ) {
                newOption = new Option(
                    sessionStorage.getItem(
                        'filterCSFaunaPhylogeneticGroupText'
                    ),
                    vm.filterCSFaunaPhylogeneticGroup,
                    false,
                    true
                );
                $('#cs_phylo_group_lookup').append(newOption);
            }
            if (
                sessionStorage.getItem('filterCSFaunaFamily') != 'all' &&
                sessionStorage.getItem('filterCSFaunaFamily') != null
            ) {
                newOption = new Option(
                    sessionStorage.getItem('filterCSFaunaFamilyText'),
                    vm.filterCSFaunaFamily,
                    false,
                    true
                );
                $('#cs_family_lookup').append(newOption);
            }
            if (
                sessionStorage.getItem('filterCSFaunaGenus') != 'all' &&
                sessionStorage.getItem('filterCSFaunaGenus') != null
            ) {
                newOption = new Option(
                    sessionStorage.getItem('filterCSFaunaGenusText'),
                    vm.filterCSFaunaGenus,
                    false,
                    true
                );
                $('#cs_genera_lookup').append(newOption);
            }
            if (
                sessionStorage.getItem('filterCSFaunaAssessor') != 'all' &&
                sessionStorage.getItem('filterCSFaunaAssessor') != null
            ) {
                newOption = new Option(
                    sessionStorage.getItem('filterCSFaunaAssessorText'),
                    vm.filterCSFaunaAssessor,
                    false,
                    true
                );
                $('#cs_assessor_lookup').append(newOption);
            }
            if (
                sessionStorage.getItem('filterCSFaunaSubmitter') != 'all' &&
                sessionStorage.getItem('filterCSFaunaSubmitter') != null
            ) {
                newOption = new Option(
                    sessionStorage.getItem('filterCSFaunaSubmitterText'),
                    vm.filterCSFaunaSubmitter,
                    false,
                    true
                );
                $('#cs_submitter_lookup').append(newOption);
            }
        });
    },
    methods: {
        historyDocument: function (id, list, species) {
            this.speciesConservationStatusHistoryId = parseInt(id);
            this.listHistoryId = list ? list : 'List not specified';
            this.speciesHistoryId = species ? species : 'not specified';
            this.uuid++;
            this.$nextTick(() => {
                this.$refs.species_conservation_status_history.isModalOpen = true;
            });
        },
        initialiseScientificNameLookup: function () {
            let vm = this;
            $(vm.$refs.cs_scientific_name_lookup)
                .select2({
                    minimumInputLength: 2,
                    dropdownParent: $('#select_scientific_name'),
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
                    vm.filterCSFaunaScientificName = data; // this is id session
                    sessionStorage.setItem(
                        'filterCSFaunaScientificNameText',
                        e.params.data.text
                    ); // this is name session
                })
                .on('select2:unselect', function () {
                    vm.filterCSFaunaScientificName = 'all';
                    sessionStorage.setItem(
                        'filterCSFaunaScientificNameText',
                        ''
                    );
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-cs_scientific_name_lookup-results"]'
                    );
                    // move focus to select2 field
                    searchField[0].focus();
                });
        },
        initialiseCommonNameLookup: function () {
            let vm = this;
            $(vm.$refs.cs_common_name_lookup)
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
                    vm.filterCSFaunaCommonName = data;
                    sessionStorage.setItem(
                        'filterCSFaunaCommonNameText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.filterCSFaunaCommonName = 'all';
                    sessionStorage.setItem('filterCSFaunaCommonNameText', '');
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-cs_common_name_lookup-results"]'
                    );
                    // move focus to select2 field
                    searchField[0].focus();
                });
        },
        initialisePhyloGroupLookup: function () {
            let vm = this;
            $(vm.$refs.cs_phylo_group_lookup)
                .select2({
                    minimumInputLength: 2,
                    dropdownParent: $('#select_phylo_group'),
                    theme: 'bootstrap-5',
                    allowClear: true,
                    placeholder: 'Select Informal Group',
                    ajax: {
                        url: api_endpoints.phylo_group_lookup,
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
                    vm.filterCSFaunaPhylogeneticGroup = data;
                    sessionStorage.setItem(
                        'filterCSFaunaPhylogeneticGroupText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.filterCSFaunaPhylogeneticGroup = 'all';
                    sessionStorage.setItem(
                        'filterCSFaunaPhylogeneticGroupText',
                        ''
                    );
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-cs_phylo_group_lookup-results"]'
                    );
                    searchField[0].focus();
                });
        },
        initialiseFamilyLookup: function () {
            let vm = this;
            $(vm.$refs.cs_family_lookup)
                .select2({
                    minimumInputLength: 2,
                    dropdownParent: $('#select_family'),
                    theme: 'bootstrap-5',
                    allowClear: true,
                    placeholder: 'Select Family',
                    ajax: {
                        url: api_endpoints.family_lookup,
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
                    vm.filterCSFaunaFamily = data;
                    sessionStorage.setItem(
                        'filterCSFaunaFamilyText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.filterCSFaunaFamily = 'all';
                    sessionStorage.setItem('filterCSFaunaFamilyText', '');
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-cs_family_lookup-results"]'
                    );
                    searchField[0].focus();
                });
        },
        initialiseGeneraLookup: function () {
            let vm = this;
            $(vm.$refs.cs_genera_lookup)
                .select2({
                    minimumInputLength: 2,
                    dropdownParent: $('#select_genera'),
                    theme: 'bootstrap-5',
                    allowClear: true,
                    placeholder: 'Select Genus',
                    ajax: {
                        url: api_endpoints.genera_lookup,
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
                    vm.filterCSFaunaGenus = data;
                    sessionStorage.setItem(
                        'filterCSFaunaGenusText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.filterCSFaunaGenus = 'all';
                    sessionStorage.setItem('filterCSFaunaGenusText', '');
                })
                .on('select2:open', function () {
                    //const searchField = $(".select2-search__field")
                    const searchField = $(
                        '[aria-controls="select2-cs_genera_lookup-results"]'
                    );
                    // move focus to select2 field
                    searchField[0].focus();
                });
        },
        initialiseAssessorLookup: function () {
            let vm = this;
            $(vm.$refs.cs_assessor_lookup)
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
                    vm.filterCSFaunaAssessor = data;
                    sessionStorage.setItem(
                        'filterCSFaunaAssessorText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.filterCSFaunaAssessor = 'all';
                    sessionStorage.setItem('filterCSFaunaAssessorText', '');
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-cs_assessor_lookup-results"]'
                    );
                    searchField[0].focus();
                });
        },
        initialiseSubmitterLookup: function () {
            let vm = this;
            $(vm.$refs.cs_submitter_lookup)
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
                    vm.filterCSFaunaSubmitter = data;
                    sessionStorage.setItem(
                        'filterCSFaunaSubmitterText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.selected_referral = null;
                    vm.filterCSFaunaSubmitter = 'all';
                    sessionStorage.setItem('filterCSFaunaSubmitterText', '');
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-cs_submitter_lookup-results"]'
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
                    vm.scientific_name_list =
                        vm.filterListsSpecies.scientific_name_list;
                    vm.common_name_list =
                        vm.filterListsSpecies.common_name_list;
                    vm.family_list = vm.filterListsSpecies.family_list;
                    vm.wa_legislative_lists =
                        vm.filterListsSpecies.wa_legislative_lists;
                    vm.wa_legislative_categories =
                        vm.filterListsSpecies.wa_legislative_categories;
                    vm.wa_priority_lists =
                        vm.filterListsSpecies.wa_priority_lists;
                    vm.wa_priority_categories =
                        vm.filterListsSpecies.wa_priority_categories;
                    vm.change_codes = vm.filterListsSpecies.change_codes;
                    vm.submitter_categories =
                        vm.filterListsSpecies.submitter_categories;
                },
                (error) => {
                    console.log(error);
                }
            );
        },
        createFaunaConservationStatus: async function () {
            swal.fire({
                title: `Propose New Fauna Conservation Status`,
                text: 'Are you sure you want to propose a new fauna conservation status?',
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Propose New Conservation Status',
                reverseButtons: true,
                customClass: {
                    confirmButton: 'btn btn-primary',
                    cancelButton: 'btn btn-secondary',
                },
            }).then(async (swalresult) => {
                if (swalresult.isConfirmed) {
                    let newFaunaCSId = null;
                    try {
                        const createUrl =
                            api_endpoints.conservation_status + '/';
                        let payload = new Object();
                        payload.application_type_id = this.group_type_id;
                        payload.internal_application = true;
                        let response = await fetch(createUrl, {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify(payload),
                        });
                        const data = await response.json();
                        if (data) {
                            newFaunaCSId = data.id;
                        }
                    } catch (err) {
                        console.log(err);
                    }
                    this.$router.push({
                        name: 'internal-conservation-status',
                        params: { conservation_status_id: newFaunaCSId },
                    });
                }
            });
        },
        discardCSProposal: function (conservation_status_id) {
            let vm = this;
            swal.fire({
                title: 'Discard Conservation Status Proposal',
                text: 'Are you sure you want to discard this proposal?',
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Discard Proposal',
                customClass: {
                    confirmButton: 'btn btn-primary',
                    cancelButton: 'btn btn-secondary',
                },
                reverseButtons: true,
            }).then(
                (result) => {
                    if (result.isConfirmed) {
                        fetch(
                            api_endpoints.discard_cs_proposal(
                                conservation_status_id
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
                                    text: 'Your proposal has been discarded',
                                    icon: 'success',
                                    customClass: {
                                        confirmButton: 'btn btn-primary',
                                    },
                                });
                                vm.$refs.fauna_cs_datatable.vmDataTable.ajax.reload(
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
        reinstateCSProposal: function (conservation_status_id) {
            let vm = this;
            swal.fire({
                title: 'Reinstate Conservation Status Proposal',
                text: 'Are you sure you want to reinstate this proposal?',
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Reinstate Proposal',
                customClass: {
                    confirmButton: 'btn btn-primary',
                    cancelButton: 'btn btn-secondary',
                },
                reverseButtons: true,
            }).then(
                (result) => {
                    if (result.isConfirmed) {
                        fetch(
                            api_endpoints.reinstate_cs_proposal(
                                conservation_status_id
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
                                    text: 'Your proposal has been reinstated',
                                    icon: 'success',
                                    customClass: {
                                        confirmButton: 'btn btn-primary',
                                    },
                                });
                                vm.$refs.fauna_cs_datatable.vmDataTable.ajax.reload(
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
        addToMeetingAgenda: function (conservation_status_id) {
            let vm = this;
            let payload = new Object();
            payload.conservation_status_id = conservation_status_id;
            fetch(`/api/meeting/${vm.meeting_obj.id}/add_agenda_item.json`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload),
            }).then(
                async (response) => {
                    vm.meeting_obj.agenda_items_arr = await response.json();
                    vm.$refs.fauna_cs_datatable.vmDataTable.ajax.reload(
                        helpers.enablePopovers,
                        true
                    );
                    this.$emit('updateAgendaItems');
                },
                (err) => {
                    console.log(err);
                }
            );
        },
        addEventListeners: function () {
            let vm = this;
            // External Discard listener
            vm.$refs.fauna_cs_datatable.vmDataTable.on(
                'click',
                'a[data-discard-cs-proposal]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-discard-cs-proposal');
                    vm.discardCSProposal(id);
                }
            );
            vm.$refs.fauna_cs_datatable.vmDataTable.on(
                'click',
                'a[data-reinstate-conservation-status-species]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr(
                        'data-reinstate-conservation-status-species'
                    );
                    vm.reinstateCSProposal(id);
                }
            );
            vm.$refs.fauna_cs_datatable.vmDataTable.on(
                'click',
                'a[data-add-to-agenda]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-add-to-agenda');
                    vm.addToMeetingAgenda(id);
                }
            );
            vm.$refs.fauna_cs_datatable.vmDataTable.on(
                'click',
                'a[data-history-conservation-status-species]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr(
                        'data-history-conservation-status-species'
                    );
                    var list = $(this).attr('data-history-conservation-list');
                    var species = $(this).attr('data-history-species');
                    vm.historyDocument(id, list, species);
                }
            );
            vm.$refs.fauna_cs_datatable.vmDataTable.on(
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
