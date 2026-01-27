<template id="species_fauna_dashboard">
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
                        <label for="fauna_group">Fauna Group:</label>
                        <select
                            id="fauna_group"
                            v-model="filterFaunaGroup"
                            class="form-select"
                        >
                            <option value="all">All</option>
                            <option
                                v-for="fauna_group in fauna_groups"
                                :value="fauna_group.id"
                                :key="fauna_group.id"
                            >
                                {{ fauna_group.name }}
                            </option>
                        </select>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="fauna_sub_group">Fauna Sub Group:</label>
                        <select
                            id="fauna_sub_group"
                            v-model="filterFaunaSubGroup"
                            class="form-select"
                        >
                            <option value="all">All</option>
                            <option
                                v-for="fauna_sub_group in faunaSubGroupsFilteredByFaunaGroup"
                                :value="fauna_sub_group.id"
                                :key="fauna_sub_group.id"
                            >
                                {{ fauna_sub_group.name }}
                            </option>
                        </select>
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
                            v-model="filterFaunaNameStatus"
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
                            v-model="filterFaunaApplicationStatus"
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
                            v-model="filterFaunaPublicationStatus"
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
                            v-model="filterFaunaRegion"
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
                            v-model="filterFaunaDistrict"
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
                            v-model="filterFaunaWALegislativeList"
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
                            v-model="filterFaunaWALegislativeCategory"
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
                            v-model="filterFaunaWAPriorityCategory"
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
                                v-model="filterFaunaCommonwealthRelevance"
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
                                v-model="filterFaunaInternationalRelevance"
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
                            v-model="filterFaunaConsevationCriteria"
                            class="form-control"
                            type="input"
                            placeholder="Enter text to search for"
                        />
                    </div>
                </div>
            </div>
        </CollapsibleFilters>

        <div v-if="newFaunaVisibility" class="col-md-12">
            <div class="text-end">
                <button
                    type="button"
                    class="btn btn-primary mb-2"
                    @click.prevent="createFauna"
                >
                    <i class="fa-solid fa-circle-plus"></i> New Fauna
                </button>
            </div>
        </div>

        <div class="row">
            <div class="col-lg-12">
                <datatable
                    :id="datatable_fauna_id"
                    ref="fauna_datatable"
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
    name: 'SpeciesFaunaTable',
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
        filterFaunaScientificName_cache: {
            type: String,
            required: false,
            default: 'filterFaunaScientificName',
        },
        filterFaunaCommonName_cache: {
            type: String,
            required: false,
            default: 'filterFaunaCommonName',
        },
        filterFaunaGroup_cache: {
            type: String,
            required: false,
            default: 'filterFaunaGroup',
        },
        filterFaunaSubGroup_cache: {
            type: String,
            required: false,
            default: 'filterFaunaSubGroup',
        },
        filterFaunaFamily_cache: {
            type: String,
            required: false,
            default: 'filterFaunaFamily',
        },
        filterFaunaGenus_cache: {
            type: String,
            required: false,
            default: 'filterFaunaGenus',
        },
        filterFaunaNameStatus_cache: {
            type: String,
            required: false,
            default: 'filterFaunaNameStatus',
        },
        filterFaunaApplicationStatus_cache: {
            type: String,
            required: false,
            default: 'filterFaunaApplicationStatus',
        },
        filterFaunaPublicationStatus_cache: {
            type: String,
            required: false,
            default: 'filterFaunaPublicationStatus',
        },
        filterFaunaRegion_cache: {
            type: String,
            required: false,
            default: 'filterFaunaRegion',
        },
        filterFaunaDistrict_cache: {
            type: String,
            required: false,
            default: 'filterFaunaDistrict',
        },
        filterFaunaWALegislativeList_cache: {
            type: String,
            required: false,
            default: 'filterFaunaWALegislativeList',
        },
        filterFaunaWALegislativeCategory_cache: {
            type: String,
            required: false,
            default: 'filterFaunaWALegislativeCategory',
        },
        filterFaunaWAPriorityCategory_cache: {
            type: String,
            required: false,
            default: 'filterFaunaWAPriorityCategory',
        },
        filterFaunaCommonwealthRelevance_cache: {
            type: String,
            required: false,
            default: 'filterFaunaCommonwealthRelevance',
        },
        filterFaunaInternationalRelevance_cache: {
            type: String,
            required: false,
            default: 'filterFaunaInternationalRelevance',
        },
        filterFaunaConsevationCriteria_cache: {
            type: String,
            required: false,
            default: 'filterFaunaConsevationCriteria',
        },
    },
    data() {
        return {
            uuid: 0,
            speciesHistoryId: null,
            datatable_fauna_id: 'species_fauna-datatable-' + uuid(),

            //Profile to check if user has access to process Proposal

            // selected values for filtering
            filterFaunaScientificName: sessionStorage.getItem(
                this.filterFaunaScientificName_cache
            )
                ? sessionStorage.getItem(this.filterFaunaScientificName_cache)
                : 'all',

            filterFaunaCommonName: sessionStorage.getItem(
                this.filterFaunaCommonName_cache
            )
                ? sessionStorage.getItem(this.filterFaunaCommonName_cache)
                : 'all',

            filterFaunaGroup: sessionStorage.getItem(
                this.filterFaunaGroup_cache
            )
                ? sessionStorage.getItem(this.filterFaunaGroup_cache)
                : 'all',

            filterFaunaSubGroup: sessionStorage.getItem(
                this.filterFaunaSubGroup_cache
            )
                ? sessionStorage.getItem(this.filterFaunaSubGroup_cache)
                : 'all',

            filterFaunaFamily: sessionStorage.getItem(
                this.filterFaunaFamily_cache
            )
                ? sessionStorage.getItem(this.filterFaunaFamily_cache)
                : 'all',

            filterFaunaGenus: sessionStorage.getItem(
                this.filterFaunaGenus_cache
            )
                ? sessionStorage.getItem(this.filterFaunaGenus_cache)
                : 'all',

            filterFaunaNameStatus: sessionStorage.getItem(
                this.filterFaunaNameStatus_cache
            )
                ? sessionStorage.getItem(this.filterFaunaNameStatus_cache)
                : 'all',

            filterFaunaApplicationStatus: sessionStorage.getItem(
                this.filterFaunaApplicationStatus_cache
            )
                ? sessionStorage.getItem(
                      this.filterFaunaApplicationStatus_cache
                  )
                : 'all',

            filterFaunaPublicationStatus: sessionStorage.getItem(
                this.filterFaunaPublicationStatus_cache
            )
                ? sessionStorage.getItem(
                      this.filterFaunaPublicationStatus_cache
                  )
                : 'all',

            filterFaunaRegion: sessionStorage.getItem(
                this.filterFaunaRegion_cache
            )
                ? sessionStorage.getItem(this.filterFaunaRegion_cache)
                : 'all',

            filterFaunaDistrict: sessionStorage.getItem(
                this.filterFaunaDistrict_cache
            )
                ? sessionStorage.getItem(this.filterFaunaDistrict_cache)
                : 'all',

            filterFaunaWALegislativeList: sessionStorage.getItem(
                this.filterFaunaWALegislativeList_cache
            )
                ? sessionStorage.getItem(
                      this.filterFaunaWALegislativeList_cache
                  )
                : 'all',

            filterFaunaWALegislativeCategory: sessionStorage.getItem(
                this.filterFaunaWALegislativeCategory_cache
            )
                ? sessionStorage.getItem(
                      this.filterFaunaWALegislativeCategory_cache
                  )
                : 'all',

            filterFaunaWAPriorityCategory: sessionStorage.getItem(
                this.filterFaunaWAPriorityCategory_cache
            )
                ? sessionStorage.getItem(
                      this.filterFaunaWAPriorityCategory_cache
                  )
                : 'all',

            filterFaunaCommonwealthRelevance: sessionStorage.getItem(
                this.filterFaunaCommonwealthRelevance_cache
            )
                ? sessionStorage.getItem(
                      this.filterFaunaCommonwealthRelevance_cache
                  )
                : 'false',

            filterFaunaInternationalRelevance: sessionStorage.getItem(
                this.filterFaunaInternationalRelevance_cache
            )
                ? sessionStorage.getItem(
                      this.filterFaunaInternationalRelevance_cache
                  )
                : 'false',

            filterFaunaConsevationCriteria: sessionStorage.getItem(
                this.filterFaunaConsevationCriteria_cache
            )
                ? sessionStorage.getItem(
                      this.filterFaunaConsevationCriteria_cache
                  )
                : '',

            //Filter list for scientific name and common name
            filterListsSpecies: {},
            common_name_list: [],
            scientific_name_list: [],
            family_list: [],
            filterRegionDistrict: {},
            region_list: [],
            district_list: [],
            filtered_district_list: [],
            wa_legislative_lists: [],
            wa_legislative_categories: [],
            wa_priority_categories: [],
            fauna_groups: [],
            fauna_sub_groups: [],

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
                this.filterFaunaScientificName === 'all' &&
                this.filterFaunaCommonName === 'all' &&
                this.filterFaunaGroup === 'all' &&
                this.filterFaunaSubGroup === 'all' &&
                this.filterFaunaFamily === 'all' &&
                this.filterFaunaGenus === 'all' &&
                this.filterFaunaNameStatus === 'all' &&
                this.filterFaunaApplicationStatus === 'all' &&
                this.filterFaunaPublicationStatus === 'all' &&
                this.filterFaunaRegion === 'all' &&
                this.filterFaunaDistrict === 'all' &&
                this.filterFaunaWALegislativeList === 'all' &&
                this.filterFaunaWALegislativeCategory === 'all' &&
                this.filterFaunaWAPriorityCategory === 'all' &&
                this.filterFaunaCommonwealthRelevance === 'false' &&
                this.filterFaunaInternationalRelevance === 'false' &&
                this.filterFaunaConsevationCriteria === ''
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
        newFaunaVisibility: function () {
            return (
                this.profile?.user &&
                this.profile.user.groups.includes(
                    constants.GROUPS.SPECIES_AND_COMMUNITIES_APPROVERS
                )
            );
        },
        faunaSubGroupsFilteredByFaunaGroup: function () {
            let vm = this;
            if (vm.filterFaunaGroup !== 'all') {
                return vm.fauna_sub_groups.filter(
                    (subGroup) =>
                        subGroup.fauna_group_id === vm.filterFaunaGroup
                );
            } else {
                return vm.fauna_sub_groups;
            }
        },
        datatable_headers: function () {
            if (this.is_external) {
                return [
                    'Id',
                    'Number',
                    'Scientific Name',
                    'Common Name',
                    'Informal Group(s)',
                    'Fauna Group',
                    'Fauna Sub Group',
                    'Family',
                    'Genus',
                    'Region',
                    'District',
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
                    'Fauna Group',
                    'Fauna Sub Group',
                    'Family',
                    'Genus',
                    'Region',
                    'District',
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
        column_fauna_group: function () {
            return {
                data: 'fauna_group_name',
                orderable: true,
                searchable: true,
                visible: true,
                name: 'fauna_group__name',
            };
        },
        column_fauna_sub_group: function () {
            return {
                data: 'fauna_sub_group_name',
                orderable: true,
                searchable: true,
                visible: true,
                name: 'fauna_sub_group__name',
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
        column_genera: function () {
            return {
                data: 'genus',
                orderable: true,
                searchable: true,
                visible: true,
                name: 'taxonomy__genera_name',
            };
        },
        column_status: function () {
            return {
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
                searchable: false, // handles by filter_queryset override method - class ProposalFilterBackend
                visible: true,
                name: 'regions__name',
            };
        },
        column_district: function () {
            return {
                data: 'districts',
                orderable: true,
                searchable: false, // handles by filter_queryset override method - class ProposalFilterBackend
                visible: true,
                name: 'districts__name',
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
        column_action: function () {
            let vm = this;
            return {
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
                    title: 'Boranga S&C Fauna Excel Export',
                    text: '<i class="fa-solid fa-download"></i> Excel',
                    className: 'btn btn-primary me-2 rounded',
                    exportOptions: {
                        columns: ':not(.no-export)',
                        orthogonal: 'export',
                    },
                },
                {
                    extend: 'csv',
                    title: 'Boranga S&C Fauna CSV Export',
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
                    vm.column_fauna_group,
                    vm.column_fauna_sub_group,
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
                    vm.column_fauna_group,
                    vm.column_fauna_sub_group,
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
                        d.filter_scientific_name = vm.filterFaunaScientificName;
                        d.filter_common_name = vm.filterFaunaCommonName;
                        d.filter_fauna_group = vm.filterFaunaGroup;
                        d.filter_fauna_sub_group = vm.filterFaunaSubGroup;
                        d.filter_family = vm.filterFaunaFamily;
                        d.filter_genus = vm.filterFaunaGenus;
                        d.filter_name_status = vm.filterFaunaNameStatus;
                        d.filter_conservation_list =
                            vm.filterFaunaConservationList;
                        d.filter_conservation_category =
                            vm.filterFaunaConservationCategory;
                        d.filter_application_status =
                            vm.filterFaunaApplicationStatus;
                        d.filter_publication_status =
                            vm.filterFaunaPublicationStatus;
                        d.filter_region = vm.filterFaunaRegion;
                        d.filter_district = vm.filterFaunaDistrict;
                        d.filter_wa_legislative_list =
                            vm.filterFaunaWALegislativeList;
                        d.filter_wa_legislative_category =
                            vm.filterFaunaWALegislativeCategory;
                        d.filter_wa_priority_category =
                            vm.filterFaunaWAPriorityCategory;
                        d.filter_commonwealth_relevance =
                            vm.filterFaunaCommonwealthRelevance;
                        d.filter_international_relevance =
                            vm.filterFaunaInternationalRelevance;
                        d.filter_conservation_criteria =
                            vm.filterFaunaConsevationCriteria;
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
        filterFaunaScientificName: function () {
            let vm = this;
            vm.$refs.fauna_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                false
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterFaunaScientificName_cache,
                vm.filterFaunaScientificName
            );
        },
        filterFaunaCommonName: function () {
            let vm = this;
            vm.$refs.fauna_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                false
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterFaunaCommonName_cache,
                vm.filterFaunaCommonName
            );
        },
        filterFaunaGroup: function () {
            let vm = this;
            vm.$refs.fauna_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                false
            ); // This calls ajax() backend call.
            vm.filterFaunaSubGroup = 'all'; // Reset sub-group when group changes
            sessionStorage.setItem(
                vm.filterFaunaGroup_cache,
                vm.filterFaunaGroup
            );
        },
        filterFaunaSubGroup: function () {
            let vm = this;
            vm.$refs.fauna_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                false
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterFaunaSubGroup_cache,
                vm.filterFaunaSubGroup
            );
        },
        filterFaunaFamily: function () {
            let vm = this;
            vm.$refs.fauna_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                false
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterFaunaFamily_cache,
                vm.filterFaunaFamily
            );
        },
        filterFaunaGenus: function () {
            let vm = this;
            vm.$refs.fauna_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                false
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterFaunaGenus_cache,
                vm.filterFaunaGenus
            );
        },
        filterFaunaNameStatus: function () {
            let vm = this;
            vm.$refs.fauna_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                false
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterFaunaNameStatus_cache,
                vm.filterFaunaNameStatus
            );
        },
        filterFaunaApplicationStatus: function () {
            let vm = this;
            vm.$refs.fauna_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                false
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterFaunaApplicationStatus_cache,
                vm.filterFaunaApplicationStatus
            );
        },
        filterFaunaPublicationStatus: function () {
            let vm = this;
            vm.$refs.fauna_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                false
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterFaunaPublicationStatus_cache,
                vm.filterFaunaPublicationStatus
            );
        },
        filterFaunaRegion: function () {
            let vm = this;
            vm.$refs.fauna_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                false
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterFaunaRegion_cache,
                vm.filterFaunaRegion
            );
        },
        filterFaunaDistrict: function () {
            let vm = this;
            vm.$refs.fauna_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                false
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterFaunaDistrict_cache,
                vm.filterFaunaDistrict
            );
        },
        filterFaunaWALegislativeList: function () {
            let vm = this;
            vm.$refs.fauna_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                false
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterFaunaWALegislativeList_cache,
                vm.filterFaunaWALegislativeList
            );
        },
        filterFaunaWALegislativeCategory: function () {
            let vm = this;
            vm.$refs.fauna_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                false
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterFaunaWALegislativeCategory_cache,
                vm.filterFaunaWALegislativeCategory
            );
        },
        filterFaunaWAPriorityCategory: function () {
            let vm = this;
            vm.$refs.fauna_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                false
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterFaunaWAPriorityCategory_cache,
                vm.filterFaunaWAPriorityCategory
            );
        },
        filterFaunaCommonwealthRelevance: function () {
            let vm = this;
            vm.$refs.fauna_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                false
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterFaunaCommonwealthRelevance_cache,
                vm.filterFaunaCommonwealthRelevance
            );
        },
        filterFaunaInternationalRelevance: function () {
            let vm = this;
            vm.$refs.fauna_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                false
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterFaunaInternationalRelevance_cache,
                vm.filterFaunaInternationalRelevance
            );
        },
        filterFaunaConsevationCriteria: function () {
            let vm = this;
            vm.$refs.fauna_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                false
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterFaunaConsevationCriteria_cache,
                vm.filterFaunaConsevationCriteria
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
            vm.initialiseScientificNameLookup();
            vm.initialiseCommonNameLookup();
            vm.initialiseFamilyLookup();
            vm.initialiseGeneraLookup();
            vm.addEventListeners();
            var newOption = null;
            // -- to set the select2 field with the session value if exists onload()
            if (
                sessionStorage.getItem('filterFaunaScientificName') != 'all' &&
                sessionStorage.getItem('filterFaunaScientificName') != null
            ) {
                // contructor new Option(text, value, defaultSelected, selected)
                newOption = new Option(
                    sessionStorage.getItem('filterFaunaScientificNameText'),
                    vm.filterFaunaScientificName,
                    false,
                    true
                );
                $('#scientific_name_lookup').append(newOption);
            }
            if (
                sessionStorage.getItem('filterFaunaCommonName') != 'all' &&
                sessionStorage.getItem('filterFaunaCommonName') != null
            ) {
                // contructor new Option(text, value, defaultSelected, selected)
                newOption = new Option(
                    sessionStorage.getItem('filterFaunaCommonNameText'),
                    vm.filterFaunaCommonName,
                    false,
                    true
                );
                $('#common_name_lookup').append(newOption);
            }
            if (
                sessionStorage.getItem('filterFaunaFamily') != 'all' &&
                sessionStorage.getItem('filterFaunaFamily') != null
            ) {
                // contructor new Option(text, value, defaultSelected, selected)
                newOption = new Option(
                    sessionStorage.getItem('filterFaunaFamilyText'),
                    vm.filterFaunaFamily,
                    false,
                    true
                );
                $('#family_lookup').append(newOption);
            }
            if (
                sessionStorage.getItem('filterFaunaGenus') != 'all' &&
                sessionStorage.getItem('filterFaunaGenus') != null
            ) {
                // contructor new Option(text, value, defaultSelected, selected)
                newOption = new Option(
                    sessionStorage.getItem('filterFaunaGenusText'),
                    vm.filterFaunaGenus,
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
                    vm.filterFaunaScientificName = data; // this is id session
                    sessionStorage.setItem(
                        'filterFaunaScientificNameText',
                        e.params.data.text
                    ); // this is name session
                })
                .on('select2:unselect', function () {
                    vm.filterFaunaScientificName = 'all';
                    sessionStorage.setItem('filterFaunaScientificNameText', '');
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-scientific_name_lookup-results"]'
                    );
                    // move focus to select2 field
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
                    vm.filterFaunaCommonName = data;
                    sessionStorage.setItem(
                        'filterFaunaCommonNameText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.filterFaunaCommonName = 'all';
                    sessionStorage.setItem('filterFaunaCommonNameText', '');
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
                    vm.filterFaunaFamily = data;
                    sessionStorage.setItem(
                        'filterFaunaFamilyText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.filterFaunaFamily = 'all';
                    sessionStorage.setItem('filterFaunaFamilyText', '');
                })
                .on('select2:open', function () {
                    //const searchField = $(".select2-search__field")
                    const searchField = $(
                        '[aria-controls="select2-family_lookup-results"]'
                    );
                    // move focus to select2 field
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
                    vm.filterFaunaGenus = data;
                    sessionStorage.setItem(
                        'filterFaunaGenusText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.filterFaunaGenus = 'all';
                    sessionStorage.setItem('filterFaunaGenusText', '');
                })
                .on('select2:open', function () {
                    //const searchField = $(".select2-search__field")
                    const searchField = $(
                        '[aria-controls="select2-genera_lookup-results"]'
                    );
                    // move focus to select2 field
                    searchField[0].focus();
                });
        },
        fetchFilterLists: function () {
            let vm = this;

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
            fetch('/api/fauna_group_dict').then(
                async (response) => {
                    const data = await response.json();
                    if (!response.ok) {
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
                    vm.fauna_groups = data.fauna_group_list;
                    vm.fauna_sub_groups = data.fauna_sub_group_list;
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
                    this.filterFaunaDistrict = 'all'; //-----to remove the previous selection
                }
                this.filtered_district_list = [];
                //---filter districts as per region selected
                for (let choice of this.district_list) {
                    if (
                        choice.region_id.toString() ===
                        this.filterFaunaRegion.toString()
                    ) {
                        this.filtered_district_list.push(choice);
                    }
                }
            });
        },
        createFauna: async function () {
            swal.fire({
                title: `Add Fauna`,
                text: 'Are you sure you want to add a new fauna?',
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Add Fauna',
                reverseButtons: true,
                customClass: {
                    confirmButton: 'btn btn-primary',
                    cancelButton: 'btn btn-secondary',
                },
            }).then(async (swalresult) => {
                if (swalresult.isConfirmed) {
                    let newFaunaId = null;
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
                            newFaunaId = data.id;
                        }
                    } catch (err) {
                        console.log(err);
                        if (this.is_internal) {
                            return err;
                        }
                    }
                    this.$router.push({
                        name: 'internal-species-communities',
                        params: { species_community_id: newFaunaId },
                        query: { group_type_name: this.group_type_name },
                    });
                }
            });
        },
        discardSpecies: function (species_id) {
            let vm = this;
            swal.fire({
                title: 'Discard Fauna',
                text: 'Are you sure you want to discard this fauna record?',
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Discard Fauna',
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
                                    text: 'The fauna record has been discarded',
                                    icon: 'success',
                                    customClass: {
                                        confirmButton: 'btn btn-primary',
                                    },
                                });
                                vm.$refs.fauna_datatable.vmDataTable.ajax.reload(
                                    helpers.enablePopovers,
                                    false
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
                title: 'Reinstate Fauna',
                text: 'Are you sure you want to reinstate this fauna record?',
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Reinstate Fauna',
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
                                    text: 'The fauna record has been reinstated',
                                    icon: 'success',
                                    customClass: {
                                        confirmButton: 'btn btn-primary',
                                    },
                                });
                                vm.$refs.fauna_datatable.vmDataTable.ajax.reload(
                                    helpers.enablePopovers,
                                    false
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
            vm.$refs.fauna_datatable.vmDataTable.on(
                'click',
                'a[data-discard-species-proposal]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-discard-species-proposal');
                    vm.discardSpecies(id);
                }
            );
            vm.$refs.fauna_datatable.vmDataTable.on(
                'click',
                'a[data-reinstate-species-proposal]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-reinstate-species-proposal');
                    vm.reinstateSpecies(id);
                }
            );
            vm.$refs.fauna_datatable.vmDataTable.on(
                'click',
                'a[data-history-species]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-history-species');
                    vm.historyDocument(id);
                }
            );
            vm.$refs.fauna_datatable.vmDataTable.on('childRow.dt', function () {
                helpers.enablePopovers();
            });
        },
    },
};
</script>
<style scoped>
.dt-buttons {
    float: right;
}
</style>
