<template id="species_flora_dashboard">
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
                        <label for="scientific_name_lookup"
                            >Scientific Name:</label
                        >
                        <select
                            id="scientific_name_lookup"
                            ref="scientific_name_lookup"
                            name="scientific_name_lookup"
                            class="form-control"
                        />
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="common_name_lookup">Common Name:</label>
                        <select
                            id="common_name_lookup"
                            ref="common_name_lookup"
                            name="common_name_lookup"
                            class="form-control"
                        />
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="phylo_group_lookup">Informal Group:</label>
                        <select
                            id="phylo_group_lookup"
                            ref="phylo_group_lookup"
                            name="phylo_group_lookup"
                            class="form-control"
                        />
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="family_lookup">Family:</label>
                        <select
                            id="family_lookup"
                            ref="family_lookup"
                            name="family_lookup"
                            class="form-control"
                        />
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="genera_lookup">Genus:</label>
                        <select
                            id="genera_lookup"
                            ref="genera_lookup"
                            name="genera_lookup"
                            class="form-control"
                        />
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="">Name Status:</label>
                        <select
                            v-model="filterFloraNameStatus"
                            class="form-select"
                        >
                            <option value="all">All</option>
                            <option value="True">Current</option>
                            <option value="False">Non Current</option>
                        </select>
                    </div>
                </div>
                <div v-if="is_internal" class="col-md-3">
                    <div class="form-group">
                        <label for="">Status:</label>
                        <select
                            v-model="filterFloraApplicationStatus"
                            class="form-select"
                        >
                            <option value="all">All</option>
                            <option
                                v-for="status in species_status"
                                :value="status.value"
                                :key="status.value"
                            >
                                {{ status.name }}
                            </option>
                        </select>
                    </div>
                </div>
                <div v-if="is_internal" class="col-md-3">
                    <div class="form-group">
                        <label for="submitter-category"
                            >Publication Status:</label
                        >
                        <select
                            id="submitter-category"
                            v-model="filterFloraPublicationStatus"
                            class="form-select"
                        >
                            <option value="all">All</option>
                            <option value="False">Private</option>
                            <option value="True">Public</option>
                        </select>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="">Region:</label>
                        <select
                            v-model="filterFloraRegion"
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
                            v-model="filterFloraDistrict"
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
                    <div class="form-group">
                        <label for="wa-legislative-list"
                            >WA Legislative List:</label
                        >
                        <select
                            id="wa-legislative-list"
                            v-model="filterFloraWALegislativeList"
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
                            v-model="filterFloraWALegislativeCategory"
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
                            v-model="filterFloraWAPriorityCategory"
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
                                v-model="filterFloraCommonwealthRelevance"
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
                                v-model="filterFloraInternationalRelevance"
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
                            for="conservation-criteria"
                            >Conservation Criteria</label
                        >
                        <input
                            id="conservation-criteria"
                            v-model="filterFloraConsevationCriteria"
                            class="form-control"
                            type="input"
                            placeholder="Enter text to search for"
                        />
                    </div>
                </div>
            </div>
        </CollapsibleFilters>

        <div v-if="newFloraVisibility" class="col-md-12">
            <div class="text-end">
                <button
                    type="button"
                    class="btn btn-primary mb-2"
                    @click.prevent="createFlora"
                >
                    <i class="fa-solid fa-circle-plus"></i> New Flora
                </button>
            </div>
        </div>

        <div class="row">
            <div class="col-lg-12">
                <datatable
                    :id="datatable_id"
                    ref="flora_datatable"
                    :dt-options="datatable_options"
                    :dt-headers="datatable_headers"
                />
            </div>
        </div>
        <div v-if="speciesHistoryId">
            <SpeciesHistory
                ref="species_history"
                :key="speciesHistoryId"
                :species-id="speciesHistoryId"
            />
        </div>
    </div>
</template>
<script>
import { v4 as uuid } from 'uuid';
import datatable from '@/utils/vue/datatable.vue';
import CollapsibleFilters from '@/components/forms/collapsible_component.vue';
import SpeciesHistory from '../internal/species_communities/species_history.vue';

import { api_endpoints, constants, helpers } from '@/utils/hooks';
export default {
    name: 'SpeciesFloraTable',
    components: {
        datatable,
        CollapsibleFilters,
        SpeciesHistory,
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
        filterFloraScientificName_cache: {
            type: String,
            required: false,
            default: 'filterFloraScientificName',
        },
        filterFloraCommonName_cache: {
            type: String,
            required: false,
            default: 'filterFloraCommonName',
        },
        filterFloraFamily_cache: {
            type: String,
            required: false,
            default: 'filterFloraFamily',
        },
        filterFloraPhylogeneticGroup_cache: {
            type: String,
            required: false,
            default: 'filterFloraPhylogeneticGroup',
        },
        filterFloraGenus_cache: {
            type: String,
            required: false,
            default: 'filterFloraGenus',
        },
        filterFloraNameStatus_cache: {
            type: String,
            required: false,
            default: 'filterFloraNameStatus',
        },
        filterFloraApplicationStatus_cache: {
            type: String,
            required: false,
            default: 'filterFloraApplicationStatus',
        },
        filterFloraPublicationStatus_cache: {
            type: String,
            required: false,
            default: 'filterFloraPublicationStatus',
        },
        filterFloraRegion_cache: {
            type: String,
            required: false,
            default: 'filterFloraRegion',
        },
        filterFloraDistrict_cache: {
            type: String,
            required: false,
            default: 'filterFloraDistrict',
        },
        filterFloraWALegislativeList_cache: {
            type: String,
            required: false,
            default: 'filterFloraWALegislativeList',
        },
        filterFloraWALegislativeCategory_cache: {
            type: String,
            required: false,
            default: 'filterFloraWALegislativeCategory',
        },
        filterFloraWAPriorityCategory_cache: {
            type: String,
            required: false,
            default: 'filterFloraWAPriorityCategory',
        },
        filterFloraCommonwealthRelevance_cache: {
            type: String,
            required: false,
            default: 'filterFloraCommonwealthRelevance',
        },
        filterFloraInternationalRelevance_cache: {
            type: String,
            required: false,
            default: 'filterFloraInternationalRelevance',
        },
        filterFloraConsevationCriteria_cache: {
            type: String,
            required: false,
            default: 'filterFloraConsevationCriteria',
        },
    },
    data() {
        return {
            uuid: 0,
            speciesHistoryId: null,
            datatable_id: 'species_flora-datatable-' + uuid(),

            // selected values for filtering
            filterFloraScientificName: sessionStorage.getItem(
                this.filterFloraScientificName_cache
            )
                ? sessionStorage.getItem(this.filterFloraScientificName_cache)
                : 'all',

            filterFloraCommonName: sessionStorage.getItem(
                this.filterFloraCommonName_cache
            )
                ? sessionStorage.getItem(this.filterFloraCommonName_cache)
                : 'all',

            filterFloraFamily: sessionStorage.getItem(
                this.filterFloraFamily_cache
            )
                ? sessionStorage.getItem(this.filterFloraFamily_cache)
                : 'all',

            filterFloraPhylogeneticGroup: sessionStorage.getItem(
                this.filterFloraPhylogeneticGroup_cache
            )
                ? sessionStorage.getItem(
                      this.filterFloraPhylogeneticGroup_cache
                  )
                : 'all',

            filterFloraGenus: sessionStorage.getItem(
                this.filterFloraGenus_cache
            )
                ? sessionStorage.getItem(this.filterFloraGenus_cache)
                : 'all',

            filterFloraNameStatus: sessionStorage.getItem(
                this.filterFloraNameStatus_cache
            )
                ? sessionStorage.getItem(this.filterFloraNameStatus_cache)
                : 'all',

            filterFloraPublicationStatus: sessionStorage.getItem(
                this.filterFloraPublicationStatus_cache
            )
                ? sessionStorage.getItem(
                      this.filterFloraPublicationStatus_cache
                  )
                : 'all',

            filterFloraApplicationStatus: sessionStorage.getItem(
                this.filterFloraApplicationStatus_cache
            )
                ? sessionStorage.getItem(
                      this.filterFloraApplicationStatus_cache
                  )
                : 'all',

            filterFloraRegion: sessionStorage.getItem(
                this.filterFloraRegion_cache
            )
                ? sessionStorage.getItem(this.filterFloraRegion_cache)
                : 'all',

            filterFloraDistrict: sessionStorage.getItem(
                this.filterFloraDistrict_cache
            )
                ? sessionStorage.getItem(this.filterFloraDistrict_cache)
                : 'all',

            filterFloraWALegislativeList: sessionStorage.getItem(
                this.filterFloraWALegislativeList_cache
            )
                ? sessionStorage.getItem(
                      this.filterFloraWALegislativeList_cache
                  )
                : 'all',

            filterFloraWALegislativeCategory: sessionStorage.getItem(
                this.filterFloraWALegislativeCategory_cache
            )
                ? sessionStorage.getItem(
                      this.filterFloraWALegislativeCategory_cache
                  )
                : 'all',

            filterFloraWAPriorityCategory: sessionStorage.getItem(
                this.filterFloraWAPriorityCategory_cache
            )
                ? sessionStorage.getItem(
                      this.filterFloraWAPriorityCategory_cache
                  )
                : 'all',

            filterFloraCommonwealthRelevance: sessionStorage.getItem(
                this.filterFloraCommonwealthRelevance_cache
            )
                ? sessionStorage.getItem(
                      this.filterFloraCommonwealthRelevance_cache
                  )
                : 'false',

            filterFloraInternationalRelevance: sessionStorage.getItem(
                this.filterFloraInternationalRelevance_cache
            )
                ? sessionStorage.getItem(
                      this.filterFloraInternationalRelevance_cache
                  )
                : 'false',

            filterFloraConsevationCriteria: sessionStorage.getItem(
                this.filterFloraConsevationCriteria_cache
            )
                ? sessionStorage.getItem(
                      this.filterFloraConsevationCriteria_cache
                  )
                : '',

            //Filter list for scientific name and common name
            filterListsSpecies: {},
            scientific_name_list: [],
            common_name_list: [],
            family_list: [],
            informal_group_list: [],
            filterRegionDistrict: {},
            region_list: [],
            district_list: [],
            filtered_district_list: [],
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
            internal_status: [
                { value: 'draft', name: 'Draft' },
                { value: 'discarded', name: 'Discarded' },
                { value: 'active', name: 'Active' },
                { value: 'historical', name: 'Historical' },
            ],

            species_status: [],
        };
    },
    computed: {
        filterApplied: function () {
            if (
                this.filterFloraScientificName === 'all' &&
                this.filterFloraCommonName === 'all' &&
                this.filterFloraFamily === 'all' &&
                this.filterFloraPhylogeneticGroup === 'all' &&
                this.filterFloraGenus === 'all' &&
                this.filterFloraNameStatus === 'all' &&
                this.filterFloraPublicationStatus === 'all' &&
                this.filterFloraApplicationStatus === 'all' &&
                this.filterFloraRegion === 'all' &&
                this.filterFloraDistrict === 'all' &&
                this.filterFloraWALegislativeList === 'all' &&
                this.filterFloraWALegislativeCategory === 'all' &&
                this.filterFloraWAPriorityCategory === 'all' &&
                this.filterFloraCommonwealthRelevance === 'false' &&
                this.filterFloraInternationalRelevance === 'false' &&
                this.filterFloraConsevationCriteria === ''
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
        is_referral: function () {
            return this.level == 'referral';
        },
        newFloraVisibility: function () {
            return (
                this.profile?.user &&
                this.profile.user.groups.includes(
                    constants.GROUPS.SPECIES_AND_COMMUNITIES_APPROVERS
                )
            );
        },
        datatable_headers: function () {
            if (this.is_external) {
                return [
                    'Id',
                    'Number',
                    'Scientific Name',
                    'Common Name',
                    'Informal Group(s)',
                    'Family',
                    'Genus',
                    'Region(s)',
                    'District(s)',
                    'WA Priority Category',
                    'WA Legislative List',
                    'WA Legislative Category',
                    'Commonwealth Conservation Category',
                    'Other Conservation Assessment',
                    'Conservation Criteria',
                    'Action',
                ];
            } else {
                return [
                    'Id',
                    'Number',
                    'Scientific Name',
                    'Common Name',
                    'Informal Group(s)',
                    'Family',
                    'Genus',
                    'Region(s)',
                    'District(s)',
                    'WA Priority Category',
                    'WA Legislative List',
                    'WA Legislative Category',
                    'Commonwealth Conservation Category',
                    'Other Conservation Assessment',
                    'Conservation Criteria',
                    'Status',
                    'Action',
                ];
            }
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
                data: 'species_number',
                orderable: true,
                searchable: true,
                visible: true,
                render: function (data, type, full) {
                    return full.species_number;
                },
                name: 'id',
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
                name: 'taxonomy__scientific_name',
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
                name: 'taxonomy__vernaculars__vernacular_name',
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
                name: 'taxonomy__family_name',
            };
        },
        column_informal_group: function () {
            return {
                data: 'informal_group',
                orderable: true,
                searchable: true,
                visible: true,
                render: function (data, type, full) {
                    let html = '';
                    if (full.informal_group) {
                        for (let i = 0; i < full.informal_group.length; i++) {
                            html += `<span class="badge bg-primary me-2">${full.informal_group[i]}</span>`;
                        }
                    }
                    return html;
                },
                name: 'taxonomy__informal_groups__classification_system_fk__class_desc',
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
                name: 'taxonomy__genera_name',
            };
        },
        column_wa_legislative_list: function () {
            return {
                data: 'wa_legislative_list',
                orderable: false,
                searchable: false,
                visible: true,
            };
        },
        column_wa_legislative_category: function () {
            return {
                data: 'wa_legislative_category',
                orderable: false,
                searchable: false,
                visible: true,
            };
        },
        column_wa_priority_category: function () {
            return {
                data: 'wa_priority_category',
                orderable: false,
                searchable: false,
                visible: true,
            };
        },
        column_commonwealth_conservation_category: function () {
            return {
                data: 'commonwealth_conservation_category',
                orderable: false,
                searchable: false,
                visible: true,
            };
        },
        column_other_conservation_assessment: function () {
            return {
                data: 'other_conservation_assessment',
                orderable: false,
                searchable: false,
                visible: true,
            };
        },
        column_conservation_criteria: function () {
            return {
                data: 'conservation_criteria',
                orderable: false,
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
                    if (full.processing_status) {
                        if (
                            ['Active', 'Historical'].includes(
                                full.processing_status
                            ) &&
                            full.publishing_status
                        ) {
                            return (
                                full.processing_status +
                                ' - ' +
                                full.publishing_status.public_status
                            );
                        }
                        return full.processing_status;
                    }
                    // Should not reach here
                    return '';
                },
                name: 'processing_status',
            };
        },
        column_region: function () {
            return {
                data: 'regions',
                orderable: true,
                searchable: false,
                visible: true,
                name: 'regions__name',
            };
        },
        column_district: function () {
            return {
                data: 'districts',
                orderable: true,
                searchable: false,
                visible: true,
                name: 'districts__name',
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
                    if (!vm.is_external) {
                        if (full.can_user_edit) {
                            if (full.processing_status == 'Discarded') {
                                links += `<a href='#${full.id}' data-reinstate-species-proposal='${full.id}'>Reinstate</a><br/>`;
                            } else {
                                links += `<a href='/internal/species-communities/${full.id}?group_type_name=${full.group_type}'>Continue</a><br/>`;
                                links += `<a href='#${full.id}' data-discard-species-proposal='${full.id}'>Discard</a><br/>`;
                                links += `<a href='#' data-history-species='${full.id}'>History</a><br>`;
                            }
                        } else {
                            if (full.user_process) {
                                links += `<a href='/internal/species-communities/${full.id}?group_type_name=${full.group_type}&action=edit'>Edit</a><br/>`;
                            }
                            links += `<a href='/internal/species-communities/${full.id}?group_type_name=${full.group_type}&action=view'>View</a><br/>`;
                            links += `<a href='#' data-history-species='${full.id}'>History</a><br>`;
                        }
                    } else {
                        links += `<a href='/external/species-communities/${full.id}?group_type_name=${full.group_type}&action=view'>View</a><br/>`;
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
                    title: 'Boranga S&C Flora Excel Export',
                    text: '<i class="fa-solid fa-download"></i> Excel',
                    className: 'btn btn-primary me-2 rounded',
                    exportOptions: {
                        columns: ':not(.no-export)',
                        orthogonal: 'export',
                    },
                },
                {
                    extend: 'csv',
                    title: 'Boranga S&C Flora CSV Export',
                    text: '<i class="fa-solid fa-download"></i> CSV',
                    className: 'btn btn-primary rounded',
                    exportOptions: {
                        columns: ':not(.no-export)',
                        orthogonal: 'export',
                    },
                },
            ];
            if (vm.is_external) {
                columns = [
                    vm.column_id,
                    vm.column_number,
                    vm.column_scientific_name,
                    vm.column_common_name,
                    vm.column_informal_group,
                    vm.column_family,
                    vm.column_genera,
                    vm.column_region,
                    vm.column_district,
                    vm.column_wa_priority_category,
                    vm.column_wa_legislative_list,
                    vm.column_wa_legislative_category,
                    vm.column_commonwealth_conservation_category,
                    vm.column_other_conservation_assessment,
                    vm.column_conservation_criteria,
                    vm.column_action,
                ];
                search = false;
            }
            if (vm.is_internal) {
                columns = [
                    vm.column_id,
                    vm.column_number,
                    vm.column_scientific_name,
                    vm.column_common_name,
                    vm.column_informal_group,
                    vm.column_family,
                    vm.column_genera,
                    vm.column_region,
                    vm.column_district,
                    vm.column_wa_priority_category,
                    vm.column_wa_legislative_list,
                    vm.column_wa_legislative_category,
                    vm.column_commonwealth_conservation_category,
                    vm.column_other_conservation_assessment,
                    vm.column_conservation_criteria,
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
                //  to show the "Action" column always in the last position
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
                        d.filter_scientific_name = vm.filterFloraScientificName;
                        d.filter_common_name = vm.filterFloraCommonName;
                        d.filter_family = vm.filterFloraFamily;
                        d.filter_informal_group =
                            vm.filterFloraPhylogeneticGroup;
                        d.filter_genus = vm.filterFloraGenus;
                        d.filter_name_status = vm.filterFloraNameStatus;
                        d.filter_publication_status =
                            vm.filterFloraPublicationStatus;
                        d.filter_application_status =
                            vm.filterFloraApplicationStatus;
                        d.filter_region = vm.filterFloraRegion;
                        d.filter_district = vm.filterFloraDistrict;
                        d.filter_wa_legislative_list =
                            vm.filterFloraWALegislativeList;
                        d.filter_wa_legislative_category =
                            vm.filterFloraWALegislativeCategory;
                        d.filter_wa_priority_category =
                            vm.filterFloraWAPriorityCategory;
                        d.filter_commonwealth_relevance =
                            vm.filterFloraCommonwealthRelevance;
                        d.filter_international_relevance =
                            vm.filterFloraInternationalRelevance;
                        d.filter_conservation_criteria =
                            vm.filterFloraConsevationCriteria;
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
        filterFloraScientificName: function () {
            let vm = this;
            vm.$refs.flora_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterFloraScientificName_cache,
                vm.filterFloraScientificName
            );
        },
        filterFloraCommonName: function () {
            let vm = this;
            vm.$refs.flora_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterFloraCommonName_cache,
                vm.filterFloraCommonName
            );
        },
        filterFloraFamily: function () {
            let vm = this;
            vm.$refs.flora_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterFloraFamily_cache,
                vm.filterFloraFamily
            );
        },
        filterFloraPhylogeneticGroup: function () {
            let vm = this;
            vm.$refs.flora_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterFloraPhylogeneticGroup_cache,
                vm.filterFloraPhylogeneticGroup
            );
        },
        filterFloraGenus: function () {
            let vm = this;
            vm.$refs.flora_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterFloraGenus_cache,
                vm.filterFloraGenus
            );
        },
        filterFloraNameStatus: function () {
            let vm = this;
            vm.$refs.flora_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterFloraNameStatus_cache,
                vm.filterFloraNameStatus
            );
        },
        filterFloraPublicationStatus: function () {
            let vm = this;
            vm.$refs.flora_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterFloraPublicationStatus_cache,
                vm.filterFloraPublicationStatus
            );
        },
        filterFloraApplicationStatus: function () {
            let vm = this;
            vm.$refs.flora_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterFloraApplicationStatus_cache,
                vm.filterFloraApplicationStatus
            );
        },
        filterFloraRegion: function () {
            let vm = this;
            vm.$refs.flora_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterFloraRegion_cache,
                vm.filterFloraRegion
            );
        },
        filterFloraDistrict: function () {
            let vm = this;
            vm.$refs.flora_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterFloraDistrict_cache,
                vm.filterFloraDistrict
            );
        },
        filterFloraWALegislativeList: function () {
            let vm = this;
            vm.$refs.flora_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterFloraWALegislativeList_cache,
                vm.filterFloraWALegislativeList
            );
        },
        filterFloraWALegislativeCategory: function () {
            let vm = this;
            vm.$refs.flora_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterFloraWALegislativeCategory_cache,
                vm.filterFloraWALegislativeCategory
            );
        },
        filterFloraWAPriorityCategory: function () {
            let vm = this;
            vm.$refs.flora_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterFloraWAPriorityCategory_cache,
                vm.filterFloraWAPriorityCategory
            );
        },
        filterFloraCommonwealthRelevance: function () {
            let vm = this;
            vm.$refs.flora_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterFloraCommonwealthRelevance_cache,
                vm.filterFloraCommonwealthRelevance
            );
        },
        filterFloraInternationalRelevance: function () {
            let vm = this;
            vm.$refs.flora_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterFloraInternationalRelevance_cache,
                vm.filterFloraInternationalRelevance
            );
        },
        filterFloraConsevationCriteria: function () {
            let vm = this;
            vm.$refs.flora_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterFloraConsevationCriteria_cache,
                vm.filterFloraConsevationCriteria
            );
        },
    },

    mounted: function () {
        let vm = this;
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
            vm.addEventListeners();
            var newOption = null;
            // -- to set the select2 field with the session value if exists onload()
            if (
                sessionStorage.getItem('filterFloraScientificName') != 'all' &&
                sessionStorage.getItem('filterFloraScientificName') != null
            ) {
                // contructor new Option(text, value, defaultSelected, selected)
                newOption = new Option(
                    sessionStorage.getItem('filterFloraScientificNameText'),
                    vm.filterFloraScientificName,
                    false,
                    true
                );
                $('#scientific_name_lookup').append(newOption);
                //$('#scientific_name_lookup').append(newOption).trigger('change');
            }
            if (
                sessionStorage.getItem('filterFloraCommonName') != 'all' &&
                sessionStorage.getItem('filterFloraCommonName') != null
            ) {
                // contructor new Option(text, value, defaultSelected, selected)
                newOption = new Option(
                    sessionStorage.getItem('filterFloraCommonNameText'),
                    vm.filterFloraCommonName,
                    false,
                    true
                );
                $('#common_name_lookup').append(newOption);
            }
            if (
                sessionStorage.getItem('filterFloraPhylogeneticGroup') !=
                    'all' &&
                sessionStorage.getItem('filterFloraPhylogeneticGroup') != null
            ) {
                // contructor new Option(text, value, defaultSelected, selected)
                newOption = new Option(
                    sessionStorage.getItem('filterFloraPhylogeneticGroupText'),
                    vm.filterFloraPhylogeneticGroup,
                    false,
                    true
                );
                $('#phylo_group_lookup').append(newOption);
            }
            if (
                sessionStorage.getItem('filterFloraFamily') != 'all' &&
                sessionStorage.getItem('filterFloraFamily') != null
            ) {
                // contructor new Option(text, value, defaultSelected, selected)
                newOption = new Option(
                    sessionStorage.getItem('filterFloraFamilyText'),
                    vm.filterFloraFamily,
                    false,
                    true
                );
                $('#family_lookup').append(newOption);
            }
            if (
                sessionStorage.getItem('filterFloraGenus') != 'all' &&
                sessionStorage.getItem('filterFloraGenus') != null
            ) {
                // contructor new Option(text, value, defaultSelected, selected)
                newOption = new Option(
                    sessionStorage.getItem('filterFloraGenusText'),
                    vm.filterFloraGenus,
                    false,
                    true
                );
                $('#genera_lookup').append(newOption);
            }
        });
    },
    methods: {
        historyDocument: function (id) {
            this.speciesHistoryId = parseInt(id);
            this.uuid++;
            this.$nextTick(() => {
                this.$refs.species_history.isModalOpen = true;
            });
        },
        initialiseScientificNameLookup: function () {
            let vm = this;
            $(vm.$refs.scientific_name_lookup)
                .select2({
                    minimumInputLength: 2,
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
                    vm.filterFloraScientificName = data;
                    sessionStorage.setItem(
                        'filterFloraScientificNameText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.filterFloraScientificName = 'all';
                    sessionStorage.setItem('filterFloraScientificNameText', '');
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-scientific_name_lookup-results"]'
                    );
                    searchField[0].focus();
                });
        },
        initialiseCommonNameLookup: function () {
            let vm = this;
            $(vm.$refs.common_name_lookup)
                .select2({
                    minimumInputLength: 2,
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
                    vm.filterFloraCommonName = data;
                    sessionStorage.setItem(
                        'filterFloraCommonNameText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.filterFloraCommonName = 'all';
                    sessionStorage.setItem('filterFloraCommonNameText', '');
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-common_name_lookup-results"]'
                    );
                    // move focus to select2 field
                    searchField[0].focus();
                });
        },
        initialiseFamilyLookup: function () {
            let vm = this;
            $(vm.$refs.family_lookup)
                .select2({
                    minimumInputLength: 2,
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
                    vm.filterFloraFamily = data;
                    sessionStorage.setItem(
                        'filterFloraFamilyText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.filterFloraFamily = 'all';
                    sessionStorage.setItem('filterFloraFamilyText', '');
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-family_lookup-results"]'
                    );
                    searchField[0].focus();
                });
        },
        initialiseGeneraLookup: function () {
            let vm = this;
            $(vm.$refs.genera_lookup)
                .select2({
                    minimumInputLength: 2,
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
                    vm.filterFloraGenus = data;
                    sessionStorage.setItem(
                        'filterFloraGenusText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.filterFloraGenus = 'all';
                    sessionStorage.setItem('filterFloraGenusText', '');
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-genera_lookup-results"]'
                    );
                    searchField[0].focus();
                });
        },
        initialisePhyloGroupLookup: function () {
            let vm = this;
            $(vm.$refs.phylo_group_lookup)
                .select2({
                    minimumInputLength: 2,
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
                    vm.filterFloraPhylogeneticGroup = data;
                    sessionStorage.setItem(
                        'filterFloraPhylogeneticGroupText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.filterFloraPhylogeneticGroup = 'all';
                    sessionStorage.setItem(
                        'filterFloraPhylogeneticGroupText',
                        ''
                    );
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-phylo_group_lookup-results"]'
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
                    vm.informal_group_list =
                        vm.filterListsSpecies.informal_group_list;
                    vm.filterDistrict();
                    vm.species_status = vm.internal_status
                        .slice()
                        .sort((a, b) => {
                            return a.name.trim().localeCompare(b.name.trim());
                        });
                    vm.wa_legislative_lists =
                        vm.filterListsSpecies.wa_legislative_lists;
                    vm.wa_legislative_categories =
                        vm.filterListsSpecies.wa_legislative_categories;
                    vm.wa_priority_categories =
                        vm.filterListsSpecies.wa_priority_categories;
                },
                (error) => {
                    console.log(error);
                }
            );
            fetch(api_endpoints.region_district_filter_dict).then(
                async (response) => {
                    vm.filterRegionDistrict = await response.json();
                    vm.region_list = vm.filterRegionDistrict.region_list;
                    vm.district_list = vm.filterRegionDistrict.district_list;
                },
                (error) => {
                    console.log(error);
                }
            );
        },
        //-------filter district dropdown dependent on region selected
        filterDistrict: function (event) {
            this.$nextTick(() => {
                if (event) {
                    this.filterFloraDistrict = 'all'; //-----to remove the previous selection
                }
                this.filtered_district_list = [];
                //---filter districts as per region selected
                for (let choice of this.district_list) {
                    if (
                        choice.region_id.toString() ===
                        this.filterFloraRegion.toString()
                    ) {
                        this.filtered_district_list.push(choice);
                    }
                }
            });
        },
        createFlora: async function () {
            swal.fire({
                title: `Add Flora`,
                text: 'Are you sure you want to add a new flora?',
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Add Flora',
                reverseButtons: true,
                customClass: {
                    confirmButton: 'btn btn-primary',
                    cancelButton: 'btn btn-secondary',
                },
            }).then(async (swalresult) => {
                if (swalresult.isConfirmed) {
                    let newFloraId = null;
                    try {
                        const createUrl = api_endpoints.species + '/';
                        let payload = new Object();
                        payload.group_type_id = this.group_type_id;
                        let response = await fetch(createUrl, {
                            method: 'POST',
                            headers: {
                                'Content-Type': 'application/json',
                            },
                            body: JSON.stringify(payload),
                        });
                        const data = await response.json();
                        if (data) {
                            newFloraId = data.id;
                        }
                    } catch (err) {
                        console.log(err);
                        if (this.is_internal) {
                            return err;
                        }
                    }
                    this.$router.push({
                        name: 'internal-species-communities',
                        params: { species_community_id: newFloraId },
                        query: { group_type_name: this.group_type_name },
                    });
                }
            });
        },
        discardSpecies: function (species_id) {
            let vm = this;
            swal.fire({
                title: 'Discard Flora',
                text: 'Are you sure you want to discard this flora record?',
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Discard Flora',
                customClass: {
                    confirmButton: 'btn btn-primary',
                    cancelButton: 'btn btn-secondary',
                },
                reverseButtons: true,
            }).then(
                (result) => {
                    if (result.isConfirmed) {
                        fetch(
                            api_endpoints.discard_species_proposal(species_id),
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
                                    text: 'The flora record has been discarded',
                                    icon: 'success',
                                    customClass: {
                                        confirmButton: 'btn btn-primary',
                                    },
                                });
                                vm.$refs.flora_datatable.vmDataTable.ajax.reload(
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
        reinstateSpecies: function (species_id) {
            let vm = this;
            swal.fire({
                title: 'Reinstate Flora',
                text: 'Are you sure you want to reinstate this flora record?',
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Reinstate Flora',
                customClass: {
                    confirmButton: 'btn btn-primary',
                    cancelButton: 'btn btn-secondary',
                },
                reverseButtons: true,
            }).then(
                (result) => {
                    if (result.isConfirmed) {
                        fetch(
                            api_endpoints.reinstate_species_proposal(
                                species_id
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
                                    text: 'The flora record has been reinstated',
                                    icon: 'success',
                                    customClass: {
                                        confirmButton: 'btn btn-primary',
                                    },
                                });
                                vm.$refs.flora_datatable.vmDataTable.ajax.reload(
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
            // External Discard listener
            vm.$refs.flora_datatable.vmDataTable.on(
                'click',
                'a[data-discard-species-proposal]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-discard-species-proposal');
                    vm.discardSpecies(id);
                }
            );
            vm.$refs.flora_datatable.vmDataTable.on(
                'click',
                'a[data-reinstate-species-proposal]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-reinstate-species-proposal');
                    vm.reinstateSpecies(id);
                }
            );
            vm.$refs.flora_datatable.vmDataTable.on(
                'click',
                'a[data-history-species]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-history-species');
                    vm.historyDocument(id);
                }
            );
            vm.$refs.flora_datatable.vmDataTable.on('childRow.dt', function () {
                helpers.enablePopovers();
            });
        },
        delay(callback, ms) {
            var timer = 0;

            return function () {
                var context = this,
                    args = arguments;
                clearTimeout(timer);
                timer = setTimeout(function () {
                    callback.apply(context, args);
                }, ms || 0);
            };
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
