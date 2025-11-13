<template lang="html">
    <div id="speciesOccurrenceReport">
        <FormSection
            :form-collapse="false"
            label="Occurrence Report Form"
            Index="occurrence_report"
        >
            <fieldset
                id="occurrence-report-profile-fieldset"
                @change="saveOccurrenceReport"
            >
                <template v-if="!is_external">
                    <CollapsibleComponent
                        ref="assessment_comments"
                        component_title="Assessment Comments"
                        :collapsed="false"
                    >
                        <div class="row">
                            <div class="col rounded">
                                <div class="row">
                                    <div class="col mt-3">
                                        <div class="form-floating m-3 mt-1">
                                            <textarea
                                                id="assessor_comment"
                                                v-model="
                                                    occurrence_report_obj.assessor_data
                                                "
                                                :disabled="
                                                    assessor_comment_readonly
                                                "
                                                class="form-control"
                                                rows="3"
                                                placeholder="Assessor Comments"
                                            />
                                            <label
                                                for=""
                                                class="col-sm-4 col-form-label"
                                                >Assessor Comments</label
                                            >
                                        </div>
                                    </div>
                                </div>
                                <div v-if="referral_comments_boxes.length > 0">
                                    <div>
                                        <div class="row mt-2">
                                            <div class="col ms-3">
                                                <h6 class="text-muted">
                                                    Referral Comments
                                                </h6>
                                            </div>
                                        </div>
                                        <template
                                            v-for="ref in referral_comments_boxes"
                                            :key="ref.name"
                                        >
                                            <div class="row mb-3">
                                                <div class="col">
                                                    <div
                                                        class="form-floating m-3 mt-1"
                                                    >
                                                        <textarea
                                                            v-if="!ref.readonly"
                                                            :id="ref.name"
                                                            v-model="
                                                                referral.referral_comment
                                                            "
                                                            :disabled="true"
                                                            :name="ref.name"
                                                            class="form-control"
                                                            :placeholder="
                                                                ref.label
                                                            "
                                                        />
                                                        <textarea
                                                            v-else
                                                            :disabled="true"
                                                            :name="ref.name"
                                                            :value="
                                                                ref.value || ''
                                                            "
                                                            class="form-control"
                                                            :placeholder="
                                                                ref.label
                                                            "
                                                        />
                                                        <label
                                                            :for="ref.name"
                                                            class="form-label"
                                                            >{{
                                                                ref.label
                                                            }}</label
                                                        >
                                                    </div>
                                                </div>
                                            </div>
                                        </template>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </CollapsibleComponent>
                </template>
                <div v-show="!isCommunity">
                    <div class="row mb-3">
                        <label for="" class="col-sm-3 col-form-label fw-bold"
                            >Scientific Name:
                            <span class="text-danger">*</span></label
                        >
                        <div :id="select_scientific_name" class="col-sm-9">
                            <select
                                :id="scientific_name_lookup"
                                :ref="scientific_name_lookup"
                                :disabled="isReadOnly"
                                :name="scientific_name_lookup"
                                class="form-control"
                            />
                        </div>
                    </div>
                    <div class="row mb-3">
                        <label for="" class="col-sm-3 col-form-label"></label>
                        <div class="col-sm-9">
                            <textarea
                                id="species_display"
                                v-model="species_display"
                                disabled
                                class="form-control"
                                rows="2"
                            />
                        </div>
                    </div>
                    <div
                        v-if="
                            !occurrence_report_obj.species_id ||
                            (occurrence_report_obj.common_names &&
                                occurrence_report_obj.common_names.length > 0)
                        "
                        class="row mb-3"
                    >
                        <label for="" class="col-sm-3 col-form-label fw-bold"
                            >Common Name<template
                                v-if="
                                    occurrence_report_obj.species_id &&
                                    occurrence_report_obj.common_names &&
                                    occurrence_report_obj.common_names.length >
                                        1
                                "
                                >s</template
                            ><template
                                v-else-if="!occurrence_report_obj.species_id"
                            >
                                Lookup</template
                            >:</label
                        >
                        <div :id="select_common_name" class="col-sm-9">
                            <template v-if="!occurrence_report_obj.species_id">
                                <select
                                    :id="common_name_lookup"
                                    :ref="common_name_lookup"
                                    :disabled="isReadOnly"
                                    :name="common_name_lookup"
                                    class="form-control"
                                />
                            </template>
                            <template
                                v-else-if="
                                    occurrence_report_obj.common_names &&
                                    occurrence_report_obj.common_names.length >
                                        0
                                "
                            >
                                <template
                                    v-for="commonName in occurrence_report_obj.common_names"
                                    :key="commonName"
                                >
                                    <h5 class="d-inline">
                                        <span class="badge bg-primary me-2">{{
                                            commonName
                                        }}</span>
                                    </h5></template
                                >
                            </template>
                        </div>
                    </div>
                    <div class="row mb-3">
                        <label for="" class="col-sm-3 col-form-label"
                            >Previous Name:</label
                        >
                        <div class="col-sm-9">
                            <input
                                id="previous_name"
                                v-model="taxon_previous_name"
                                readonly
                                type="text"
                                class="form-control"
                                placeholder=""
                            />
                        </div>
                    </div>
                </div>
                <div v-show="isCommunity">
                    <div class="row mb-3">
                        <label for="" class="col-sm-3 col-form-label"
                            >Community Name:</label
                        >
                        <div :id="select_community_name" class="col-sm-9">
                            <select
                                :id="community_name_lookup"
                                :ref="community_name_lookup"
                                :disabled="isReadOnly"
                                :name="community_name_lookup"
                                class="form-control"
                            />
                        </div>
                    </div>
                    <div class="row mb-3">
                        <label for="" class="col-sm-3 col-form-label"></label>
                        <div class="col-sm-9">
                            <textarea
                                id="community_display"
                                v-model="community_display"
                                disabled
                                class="form-control"
                                rows="2"
                            />
                        </div>
                    </div>
                    <div
                        v-if="
                            !occurrence_report_obj.community_id ||
                            occurrence_report_obj.community_common_id
                        "
                        class="row mb-3"
                    >
                        <label for="" class="col-sm-3 col-form-label"
                            >Community ID<template
                                v-if="!occurrence_report_obj.community_id"
                            >
                                Lookup</template
                            >:</label
                        >
                        <div :id="select_community_id" class="col-sm-9">
                            <template
                                v-if="!occurrence_report_obj.community_id"
                            >
                                <select
                                    :id="community_id_lookup"
                                    :ref="community_id_lookup"
                                    :disabled="isReadOnly"
                                    :name="community_id_lookup"
                                    class="form-control"
                                />
                            </template>
                            <template
                                v-else-if="
                                    occurrence_report_obj.community_common_id
                                "
                            >
                                <input
                                    id="community_common_id"
                                    :value="
                                        occurrence_report_obj.community_common_id
                                    "
                                    disabled
                                    type="text"
                                    class="form-control"
                                />
                            </template>
                        </div>
                    </div>
                </div>
                <div
                    v-if="occurrence_report_obj.migrated_from_id"
                    class="row mb-3"
                >
                    <label
                        for="migrated_from_id"
                        class="col-sm-3 col-form-label"
                        >Migrated From ID:</label
                    >
                    <div class="col-sm-9">
                        <input
                            id="migrated_from_id"
                            :value="occurrence_report_obj.migrated_from_id"
                            disabled
                            type="text"
                            class="form-control"
                        />
                    </div>
                </div>
                <div class="row mb-3">
                    <label for="" class="col-sm-3 col-form-label"
                        >Site Name:</label
                    >
                    <div class="col-sm-9">
                        <textarea
                            id="site"
                            v-model="occurrence_report_obj.site"
                            :disabled="isReadOnly"
                            class="form-control"
                            rows="1"
                            placeholder=""
                        />
                    </div>
                </div>
                <div class="row mb-3">
                    <label
                        for="occurrence_report_is_for_occurrence_number"
                        class="col-sm-3 col-form-label"
                        >ORF is for OCC Number:
                        <HelpText
                            section_id="occurrence_report_is_for_occurrence_number"
                        />
                    </label>
                    <div class="col-sm-9">
                        <input
                            id="occurrence_report_is_for_occurrence_number"
                            v-model="occurrence_report_obj.ocr_for_occ_number"
                            type="text"
                            :disabled="isReadOnly"
                            class="form-control"
                            maxlength="9"
                            autocomplete="new-password"
                        />
                    </div>
                </div>
                <div class="row mb-3">
                    <label
                        for="occurrence_report_for_occurrence_name"
                        class="col-sm-3 col-form-label"
                        >Occurrence Name:
                        <HelpText
                            section_id="occurrence_report_for_occurrence_name"
                        />
                    </label>
                    <div class="col-sm-9">
                        <input
                            id="occurrence_report_for_occurrence_name"
                            v-model="occurrence_report_obj.ocr_for_occ_name"
                            type="text"
                            :disabled="isReadOnly"
                            class="form-control"
                            autocomplete="new-password"
                        />
                    </div>
                </div>
                <div class="row mb-3">
                    <label for="" class="col-sm-3 col-form-label fw-bold"
                        >Observation Date:
                        <span class="text-danger">*</span></label
                    >
                    <div class="col-sm-9">
                        <input
                            v-model="occurrence_report_obj.observation_date"
                            :disabled="isReadOnly"
                            type="date"
                            class="form-control"
                            name="start_date"
                            :max="new Date().toISOString().slice(0, 10)"
                            min="1990-01-01"
                            @change.stop
                            @blur="checkObservationDate"
                        />
                    </div>
                </div>
                <div
                    v-if="occurrence_report_obj.group_type == 'fauna'"
                    class="row mb-3"
                >
                    <label for="" class="col-sm-3 control-label"
                        >Observation Time:</label
                    >
                    <div class="col-sm-9">
                        <template v-if="!isReadOnly">
                            <template
                                v-if="
                                    observationTimes &&
                                    observationTimes.length > 0 &&
                                    occurrence_report_obj.observation_time_id &&
                                    !observationTimes
                                        .map((d) => d.id)
                                        .includes(
                                            occurrence_report_obj.observation_time_id
                                        )
                                "
                            >
                                <input
                                    v-if="
                                        occurrence_report_obj.observation_time
                                    "
                                    type="text"
                                    class="form-control mb-3"
                                    :value="
                                        occurrence_report_obj.observation_time +
                                        ' (Now Archived)'
                                    "
                                    disabled
                                />
                                <div class="mb-3 text-muted">
                                    Change observation time to:
                                </div>
                            </template>
                            <select
                                v-model="
                                    occurrence_report_obj.observation_time_id
                                "
                                class="form-select"
                            >
                                <option :value="null">
                                    Select an Observation Time if Applicable
                                </option>
                                <option
                                    v-for="observationTime in observationTimes"
                                    :key="observationTime.id"
                                    :value="observationTime.id"
                                >
                                    {{ observationTime.name }}
                                </option>
                            </select>
                        </template>
                        <template v-else>
                            <input
                                v-model="occurrence_report_obj.observation_time"
                                class="form-control"
                                type="text"
                                :disabled="isReadOnly"
                            />
                        </template>
                    </div>
                </div>

                <div class="row mb-3">
                    <label for="" class="col-sm-3 col-form-label"
                        >Record Source:
                    </label>
                    <div class="col-sm-9">
                        <textarea
                            v-model="occurrence_report_obj.record_source"
                            :disabled="isReadOnly"
                            class="form-control"
                            name="record_source"
                            rows="7"
                        />
                    </div>
                </div>
                <div class="row mb-3">
                    <label for="" class="col-sm-3 col-form-label"
                        >Comments:
                    </label>
                    <div class="col-sm-9">
                        <textarea
                            v-model="occurrence_report_obj.comments"
                            :disabled="isReadOnly"
                            class="form-control"
                            name="comments"
                            rows="7"
                        />
                    </div>
                </div>
            </fieldset>
        </FormSection>
        <FormSection
            :form-collapse="false"
            label="Observer Details"
            Index="observer_details"
        >
            <ObserverDatatable
                ref="observer_datatable"
                :occurrence_report_obj="occurrence_report_obj"
                :is_external="is_external"
                :is-read-only="isReadOnly"
                :show_observer_contact_information="
                    show_observer_contact_information
                "
                @refresh-occurrence-report="refreshOccurrenceReport()"
            >
            </ObserverDatatable>
        </FormSection>
    </div>
</template>

<script>
import { v4 as uuid } from 'uuid';
import FormSection from '@/components/forms/section_toggle.vue';
import ObserverDatatable from './observer_datatable.vue';
import CollapsibleComponent from '@/components/forms/collapsible_component.vue';
import HelpText from '@/components/common/help_text.vue';

import { api_endpoints, helpers } from '@/utils/hooks';

export default {
    name: 'OccurrenceReportProfile',
    components: {
        FormSection,
        HelpText,
        ObserverDatatable,
        CollapsibleComponent,
    },
    props: {
        occurrence_report_obj: {
            type: Object,
            required: true,
        },
        referral: {
            type: Object,
            required: false,
        },
        is_external: {
            type: Boolean,
            default: false,
        },
        show_observer_contact_information: {
            type: Boolean,
            default: true,
        },
    },
    emits: ['refreshOccurrenceReport', 'saveOccurrenceReport'],
    data: function () {
        let vm = this;
        return {
            uuid: null,
            scientific_name_lookup: 'scientific_name_lookup' + uuid(),
            select_scientific_name: 'select_scientific_name' + uuid(),
            common_name_lookup: 'common_name_lookup' + uuid(),
            community_name_lookup: 'community_name_lookup' + uuid(),
            community_id_lookup: 'community_id_lookup' + uuid(),
            observationTimes: null,
            select_community_name: 'select_community_name' + uuid(),
            select_common_name: 'select_common_name' + uuid(),
            select_community_id: 'select_community_id' + uuid(),
            isFauna:
                vm.occurrence_report_obj.group_type === 'fauna' ? true : false,
            isCommunity:
                vm.occurrence_report_obj.group_type === 'community'
                    ? true
                    : false,
            species_display: '',
            community_display: '',
            taxon_previous_name: '',
            referral_comments_boxes: [],
        };
    },
    computed: {
        isReadOnly: function () {
            return this.occurrence_report_obj.readonly;
        },
        csrf_token: function () {
            return helpers.getCookie('csrftoken');
        },
        isAssignedOfficer: function () {
            return (
                this.occurrence_report_obj &&
                this.occurrence_report_obj.assigned_officer ==
                    this.occurrence_report_obj.current_assessor.id
            );
        },
        assessor_comment_readonly: function () {
            return !this.is_external &&
                !this.occurrence_report_obj.can_user_edit &&
                this.occurrence_report_obj.assessor_mode.assessor_level ==
                    'assessor' &&
                this.occurrence_report_obj.assessor_mode.has_assessor_mode &&
                this.isAssignedOfficer &&
                !this.occurrence_report_obj.assessor_mode
                    .status_without_assessor
                ? false
                : true;
        },
    },
    watch: {
        'occurrence_report_obj.observation_date': function () {
            let vm = this;
            if (vm.isFauna) {
                if (
                    vm.occurrence_report_obj &&
                    vm.occurrence_report_obj.animal_observation &&
                    vm.occurrence_report_obj.observation_date
                ) {
                    vm.occurrence_report_obj.animal_observation.obs_date =
                        vm.occurrence_report_obj.observation_date;
                }
            } else if (vm.isCommunity) {
                if (
                    vm.occurrence_report_obj &&
                    vm.occurrence_report_obj.habitat_condition &&
                    vm.occurrence_report_obj.observation_date
                ) {
                    vm.occurrence_report_obj.habitat_condition.obs_date =
                        vm.occurrence_report_obj.observation_date;
                }
            } else {
                if (
                    vm.occurrence_report_obj &&
                    vm.occurrence_report_obj.plant_count &&
                    vm.occurrence_report_obj.observation_date
                ) {
                    vm.occurrence_report_obj.plant_count.obs_date =
                        vm.occurrence_report_obj.observation_date;
                }
            }
        },
    },
    created: async function () {
        let vm = this;
        this.uuid = uuid();
        //------fetch list of values according to action
        let action = this.$route.query.action;
        let dict_url =
            action == 'view'
                ? api_endpoints.cs_profile_dict +
                  '?group_type=' +
                  vm.occurrence_report_obj.group_type +
                  '&action=' +
                  action
                : api_endpoints.cs_profile_dict +
                  '?group_type=' +
                  vm.occurrence_report_obj.group_type;
        fetch(dict_url).then(
            async (response) => {
                vm.cs_profile_dict = await response.json();
                if (!vm.isCommunity) {
                    this.getSpeciesDisplay();
                } else {
                    this.getCommunityDisplay();
                }
            },
            (error) => {
                console.log(error);
            }
        );
        vm.fetchObservationTimes();
        if (!vm.is_external) {
            this.generateReferralCommentBoxes();
        }
    },
    mounted: function () {
        let vm = this;
        this.$nextTick(() => {
            vm.initialiseScientificNameLookup();
            vm.initialiseCommunityNameLookup();
            vm.initialiseCommonNameLookup();
            vm.initialiseCommunityIDLookup();
        });
    },
    methods: {
        initialiseScientificNameLookup: function () {
            let vm = this;
            $(vm.$refs[vm.scientific_name_lookup])
                .select2({
                    minimumInputLength: 2,
                    dropdownParent: $('#' + vm.select_scientific_name),
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
                                group_type_id:
                                    vm.occurrence_report_obj.group_type_id,
                                has_species: true,
                            };
                            return query;
                        },
                    },
                })
                .on('select2:select', function (e) {
                    // eslint-disable-next-line no-unused-vars
                    var selected = $(e.currentTarget);
                    vm.occurrence_report_obj.species_id =
                        e.params.data.species_id;
                    vm.species_display = e.params.data.text;
                    vm.occurrence_report_obj.common_names =
                        e.params.data.common_names_list;
                    vm.taxon_previous_name = e.params.data.taxon_previous_name;
                    // Unfortunate to call this twice but the change event on the fieldset fires before
                    // the select2:select event
                    vm.$emit('saveOccurrenceReport');
                    $(vm.$refs[vm.common_name_lookup]).select2('destroy');
                })
                .on('select2:unselect', function (e) {
                    // eslint-disable-next-line no-unused-vars
                    var selected = $(e.currentTarget);
                    vm.occurrence_report_obj.species_id = null;
                    vm.species_display = '';
                    vm.taxon_previous_name = '';
                    vm.$emit('saveOccurrenceReport');
                    vm.$nextTick(() => {
                        vm.initialiseCommonNameLookup();
                    });
                })
                // eslint-disable-next-line no-unused-vars
                .on('select2:open', function (e) {
                    const searchField = $(
                        '[aria-controls="select2-' +
                            vm.scientific_name_lookup +
                            '-results"]'
                    );
                    // move focus to select2 field
                    searchField[0].focus();
                });
        },
        initialiseCommonNameLookup: function () {
            let vm = this;
            $(vm.$refs[vm.common_name_lookup])
                .select2({
                    minimumInputLength: 2,
                    dropdownParent: $('#' + vm.select_common_name),
                    theme: 'bootstrap-5',
                    allowClear: true,
                    placeholder: 'Select Common Name',
                    ajax: {
                        url: api_endpoints.common_name_lookup_ocr_select,
                        dataType: 'json',
                        data: function (params) {
                            var query = {
                                term: params.term,
                                type: 'public',
                                group_type_id:
                                    vm.occurrence_report_obj.group_type_id,
                                has_species: true,
                            };
                            return query;
                        },
                    },
                })
                .on('select2:select', function (e) {
                    vm.occurrence_report_obj.species_id =
                        e.params.data.species_id;
                    vm.occurrence_report_obj.species_taxonomy_id =
                        e.params.data.id;
                    vm.occurrence_report_obj.common_names =
                        e.params.data.common_names_list;
                    // Unfortunate to call this twice but the change event on the fieldset fires before
                    // the select2:select event
                    vm.$emit('saveOccurrenceReport');
                    var newOption = new Option(
                        e.params.data.scientific_name,
                        e.params.data.id,
                        false,
                        true
                    );
                    $('#' + vm.scientific_name_lookup)
                        .append(newOption)
                        .trigger('change');

                    vm.species_display = e.params.data.scientific_name;
                    vm.taxon_previous_name = e.params.data.taxon_previous_name;
                    $(vm.$refs[vm.common_name_lookup]).select2('destroy');
                })
                .on('select2:unselect', function (e) {
                    // eslint-disable-next-line no-unused-vars
                    var selected = $(e.currentTarget);
                    vm.occurrence_report_obj.species_id = null;
                    vm.species_display = '';
                    vm.taxon_previous_name = '';
                    vm.$emit('saveOccurrenceReport');
                })
                // eslint-disable-next-line no-unused-vars
                .on('select2:open', function (e) {
                    const searchField = $(
                        '[aria-controls="select2-' +
                            vm.common_name_lookup +
                            '-results"]'
                    );
                    // move focus to select2 field
                    searchField[0].focus();
                });
        },
        initialiseCommunityIDLookup: function () {
            let vm = this;
            $(vm.$refs[vm.community_id_lookup])
                .select2({
                    minimumInputLength: 2,
                    dropdownParent: $('#' + vm.select_community_id),
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
                                group_type_id:
                                    vm.occurrence_report_obj.group_type_id,
                                has_species: true,
                            };
                            return query;
                        },
                    },
                })
                .on('select2:select', function (e) {
                    vm.occurrence_report_obj.community_id =
                        e.params.data.community_id;
                    vm.occurrence_report_obj.community_name =
                        e.params.data.community_name;
                    vm.occurrence_report_obj.community_common_id =
                        e.params.data.text;
                    vm.community_display = e.params.data.community_name;
                    // the select2:select event // Unfortunate to call this twice but the change event on the fieldset fires before
                    vm.$emit('saveOccurrenceReport');
                    var newOption = new Option(
                        vm.occurrence_report_obj.community_name,
                        vm.occurrence_report_obj.community_id,
                        false,
                        true
                    );
                    $('#' + vm.community_name_lookup)
                        .append(newOption)
                        .trigger('change');

                    $(vm.$refs[vm.community_id_lookup]).select2('destroy');
                })
                .on('select2:unselect', function (e) {
                    // eslint-disable-next-line no-unused-vars
                    var selected = $(e.currentTarget);
                    vm.occurrence_report_obj.community_id = null;
                    vm.occurrence_report_obj.community_name = null;
                    vm.occurrence_report_obj.community_common_id = null;
                    vm.community_display = '';
                    vm.$emit('saveOccurrenceReport');
                })
                // eslint-disable-next-line no-unused-vars
                .on('select2:open', function (e) {
                    const searchField = $(
                        '[aria-controls="select2-' +
                            vm.community_id_lookup +
                            '-results"]'
                    );
                    // move focus to select2 field
                    searchField[0].focus();
                });
        },
        getSpeciesDisplay: function () {
            let vm = this;
            if (vm.occurrence_report_obj.species_taxonomy_id != null) {
                let species_display_url =
                    api_endpoints.species_display +
                    '?taxon_id=' +
                    vm.occurrence_report_obj.species_taxonomy_id;
                fetch(species_display_url).then(async (response) => {
                    const data = await response.json();
                    var newOption = new Option(data.name, data.id, false, true);
                    $('#' + vm.scientific_name_lookup).append(newOption);
                    vm.species_display = data.name;
                    vm.taxon_previous_name = data.taxon_previous_name;
                });
            }
        },
        initialiseCommunityNameLookup: function () {
            let vm = this;
            $(vm.$refs[vm.community_name_lookup])
                .select2({
                    minimumInputLength: 2,
                    dropdownParent: $('#' + vm.select_community_name),
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
                                cs_community: true,
                            };
                            return query;
                        },
                    },
                })
                .on('select2:select', function (e) {
                    vm.occurrence_report_obj.community_id = e.params.data.id;
                    vm.occurrence_report_obj.community_common_id =
                        e.params.data.community_common_id;
                    vm.community_display = e.params.data.text;
                    vm.$emit('saveOccurrenceReport');
                    $(vm.$refs[vm.community_id_lookup]).select2('destroy');
                })
                .on('select2:unselect', function () {
                    vm.occurrence_report_obj.community_id = null;
                    vm.occurrence_report_obj.community_name = null;
                    vm.occurrence_report_obj.community_common_id = null;
                    vm.community_display = '';
                    vm.$nextTick(() => {
                        vm.initialiseCommunityIDLookup();
                    });
                    vm.$emit('saveOccurrenceReport');
                })
                // eslint-disable-next-line no-unused-vars
                .on('select2:open', function (e) {
                    const searchField = $(
                        '[aria-controls="select2-' +
                            vm.community_name_lookup +
                            '-results"]'
                    );
                    // move focus to select2 field
                    searchField[0].focus();
                });
        },
        getCommunityDisplay: function () {
            let vm = this;
            if (vm.occurrence_report_obj?.community_id) {
                let community_display_url =
                    api_endpoints.community_display +
                    '?community_id=' +
                    vm.occurrence_report_obj.community_id;
                fetch(community_display_url).then(async (response) => {
                    const data = await response.json();
                    var newOption = new Option(data.name, data.id, false, true);
                    $('#' + vm.community_name_lookup).append(newOption);
                    vm.community_display = data.name;
                });
            }
        },
        incrementComponentMapKey: function () {
            this.uuid = uuid();
        },
        checkObservationDate: function () {
            if (this.occurrence_report_obj.observation_date === '') {
                this.occurrence_report_obj.observation_date = null;
                this.$emit('saveOccurrenceReport');
                return;
            }
            if (
                this.occurrence_report_obj.observation_date === null ||
                isNaN(new Date(this.occurrence_report_obj.observation_date))
            ) {
                return;
            }
            if (
                new Date(this.occurrence_report_obj.observation_date) >
                new Date()
            ) {
                this.occurrence_report_obj.observation_date = new Date()
                    .toISOString()
                    .slice(0, 10);
                this.$nextTick(() => {
                    this.$refs.observation_date.focus();
                });
                swal.fire({
                    title: 'Error',
                    text: 'Last data curation date cannot be in the future',
                    icon: 'error',
                    customClass: {
                        confirmButton: 'btn btn-primary',
                    },
                });
            }
            if (
                new Date(this.occurrence_report_obj.observation_date) <
                new Date('1990-01-01')
            ) {
                this.occurrence_report_obj.observation_date = new Date(
                    '1990-01-01'
                )
                    .toISOString()
                    .slice(0, 10);
                this.$nextTick(() => {
                    this.$refs.observation_date.focus();
                });
                swal.fire({
                    title: 'Error',
                    text: 'Last data curation date cannot be before 01/01/1990',
                    icon: 'error',
                    customClass: {
                        confirmButton: 'btn btn-primary',
                    },
                });
            }
            // Save after validation
            this.$emit('saveOccurrenceReport');
        },
        has_comment_value: function () {
            let has_value = false;
            for (let i = 0; i < this.referral_comments_boxes.length; i++) {
                if (Object.hasOwn(this.referral_comments_boxes[i], 'value')) {
                    if (
                        this.referral_comments_boxes[i].value != null &&
                        this.referral_comments_boxes[i].value != undefined &&
                        this.referral_comments_boxes[i].value != ''
                    ) {
                        has_value = true;
                    }
                }
            }
            return has_value;
        },
        generateReferralCommentBoxes: function () {
            var box_visibility =
                this.occurrence_report_obj.assessor_mode.assessor_box_view;
            var assessor_mode =
                this.occurrence_report_obj.assessor_mode.assessor_level;
            if (!this.occurrence_report_obj.can_user_edit) {
                // eslint-disable-next-line no-unused-vars
                let current_referral_present = false;

                $.each(this.occurrence_report_obj.referrals, (i, v) => {
                    var referral_name = `comment-field-Referral-${v.referral.email}`;
                    var referral_visibility =
                        assessor_mode == 'referral' &&
                        this.occurrence_report_obj.assessor_mode
                            .assessor_can_assess &&
                        this.referral.referral == v.referral.id
                            ? false
                            : true;
                    var referral_label = `${v.referral.fullname}`;
                    var referral_comment_val = `${v.referral_comment}`;
                    this.referral_comments_boxes.push({
                        box_view: box_visibility,
                        name: referral_name,
                        label: referral_label,
                        readonly: referral_visibility,
                        value: referral_comment_val,
                    });
                });
            }
        },
        refreshOccurrenceReport: function () {
            this.$emit('refreshOccurrenceReport');
        },
        saveOccurrenceReport: function (e) {
            // For the select2 we emit after the select/unselect event as otherwise the value is not yet updated
            if (!e.target.className.includes('select2-')) {
                this.$emit('saveOccurrenceReport');
            }
        },
        fetchObservationTimes: function () {
            fetch(api_endpoints.observation_times)
                .then(async (response) => {
                    if (!response.ok) {
                        swal.fire({
                            title: 'Error',
                            text: 'Failed to fetch observer times',
                            icon: 'error',
                            customClass: {
                                confirmButton: 'btn btn-primary',
                            },
                        });
                        return;
                    }
                    const data = await response.json();
                    this.observationTimes = data;
                })
                .catch((error) => {
                    console.error('Error fetching observer times:', error);
                });
        },
    },
};
</script>

<style lang="css" scoped>
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

input[type='text'],
select {
    width: 100%;
    padding: 0.375rem 2.25rem 0.375rem 0.75rem;
}

input[type='number'] {
    width: 50%;
}
</style>
