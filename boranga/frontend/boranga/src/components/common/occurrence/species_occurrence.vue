<template lang="html">
    <div id="speciesOccurrence">
        <FormSection
            :form-collapse="false"
            label="Occurrence"
            Index="occurrence"
        >
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label fw-bold"
                    >Occurrence Name: <span class="text-danger">*</span></label
                >
                <div class="col-sm-9">
                    <textarea
                        id="occurrence_name"
                        v-model="occurrence_obj.occurrence_name"
                        class="form-control"
                        :disabled="isReadOnly"
                        rows="1"
                        placeholder=""
                    />
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label fw-bold"
                    >Scientific Name: <span class="text-danger">*</span></label
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
            <div v-if="occurrence_obj.migrated_from_id" class="row mb-3">
                <label for="migrated_from_id" class="col-sm-3 control-label"
                    >Migrated From ID:</label
                >
                <div class="col-sm-9">
                    <input
                        id="migrated_from_id"
                        :value="occurrence_obj.migrated_from_id"
                        disabled
                        type="text"
                        class="form-control"
                    />
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label"
                    >Common Name<template v-if="common_names.length > 1"
                        >s</template
                    >:</label
                >
                <div class="col-sm-9">
                    <template v-if="common_names.length > 0">
                        <template v-for="name in common_names" :key="name">
                            <h5 class="d-inline">
                                <span class="badge bg-primary me-2">{{
                                    name
                                }}</span>
                            </h5>
                        </template>
                    </template>
                    <template v-else>
                        <h5 class="d-inline">
                            <span class="badge bg-secondary me-2"
                                ><i class="bi bi-ban"></i
                            ></span>
                            <span class="visually-hidden">N/A</span>
                        </h5>
                    </template>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label"
                    >Occurrence Source:</label
                >
                <div
                    v-for="source in occurrence_source_list"
                    :key="source[0]"
                    class="col-sm-auto"
                >
                    <input
                        :id="source[1]"
                        v-model="occurrence_obj.occurrence_source"
                        :disabled="isReadOnly"
                        type="checkbox"
                        :value="source[0]"
                    />
                    {{ source[1] }}
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label"
                    >Wild Status:</label
                >
                <div class="col-sm-9">
                    <template
                        v-if="
                            wild_status_list &&
                            wild_status_list.length > 0 &&
                            !wild_status_list
                                .map((d) => d.id)
                                .includes(occurrence_obj.wild_status)
                        "
                    >
                        <input
                            v-if="occurrence_obj.wild_status_name"
                            type="text"
                            class="form-control mb-3"
                            :value="
                                occurrence_obj.wild_status_name +
                                ' (Now Archived)'
                            "
                            disabled
                        />
                        <div
                            v-if="occurrence_obj.wild_status"
                            class="mb-3 text-muted"
                        >
                            Change wild status to:
                        </div>
                    </template>
                    <select
                        v-model="occurrence_obj.wild_status"
                        :disabled="isReadOnly"
                        class="form-select"
                    >
                        <option :value="null" selected disabled>
                            Select Wild Status
                        </option>
                        <option
                            v-for="option in wild_status_list"
                            :key="option.id"
                            :value="option.id"
                        >
                            {{ option.name }}
                        </option>
                    </select>
                </div>
            </div>
            <div class="row mb-3">
                <label class="col-sm-3 control-label" for=""
                    >Review Due Date:</label
                >
                <div class="col-sm-3">
                    <input
                        v-model="occurrence_obj.review_due_date"
                        type="date"
                        class="form-control"
                        placeholder="DD/MM/YYYY"
                    />
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Comments:</label>
                <div class="col-sm-9">
                    <textarea
                        id="occurrence_comments"
                        v-model="occurrence_obj.comment"
                        class="form-control"
                        :disabled="isReadOnly"
                        placeholder=""
                        rows="4"
                    />
                </div>
            </div>

            <ContactDatatable
                ref="contact_datatable"
                :occurrence_obj="occurrence_obj"
                :is-read-only="isReadOnly"
                @refresh-occurrence="refreshOccurrence()"
            >
            </ContactDatatable>

            <RelatedReports
                :is-read-only="isReadOnly"
                :occurrence_obj="occurrence_obj"
                label="Related Occurrence Report Forms"
            />
        </FormSection>
    </div>
</template>

<script>
import { v4 as uuid } from 'uuid';
import FormSection from '@/components/forms/section_toggle.vue';
import ContactDatatable from './contact_datatable.vue';
import RelatedReports from '@/components/common/occurrence/occ_related_ocr_table.vue';
import { api_endpoints, helpers } from '@/utils/hooks';

export default {
    name: 'SpeciesOccurrence',
    components: {
        FormSection,
        RelatedReports,
        ContactDatatable,
    },
    props: {
        occurrence_obj: {
            type: Object,
            required: true,
        },
    },
    data: function () {
        return {
            scientific_name_lookup: 'scientific_name_lookup' + uuid(),
            select_scientific_name: 'select_scientific_name' + uuid(),
            select_wild_status: 'select_wild_status' + uuid(),
            common_names: [],
            occ_profile_dict: {},
            wild_status_list: [],
            occurrence_source_list: [],
        };
    },
    computed: {
        isReadOnly: function () {
            return !this.occurrence_obj.can_user_edit;
        },
    },
    watch: {},
    created: async function () {
        let vm = this;
        //------fetch list of values according to action
        let action = this.$route.query.action;
        let dict_url =
            action == 'view'
                ? api_endpoints.occ_profile_dict +
                  '?group_type=' +
                  vm.occurrence_obj.group_type +
                  '&action=' +
                  action
                : api_endpoints.occ_profile_dict +
                  '?group_type=' +
                  vm.occurrence_obj.group_type;
        fetch(dict_url).then(
            async (response) => {
                vm.occ_profile_dict = await response.json();
                vm.wild_status_list = vm.occ_profile_dict.wild_status_list;
                vm.occurrence_source_list =
                    vm.occ_profile_dict.occurrence_source_list;
                this.getSpeciesDisplay();
            },
            (error) => {
                console.log(error);
            }
        );
    },
    mounted: function () {
        let vm = this;
        this.$nextTick(() => {
            vm.initialiseScientificNameLookup();
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
                                group_type_id: vm.occurrence_obj.group_type_id,
                                has_species: true,
                            };
                            return query;
                        },
                        // results: function (data, page) { // parse the results into the format expected by Select2.
                        //     // since we are using custom formatting functions we do not need to alter remote JSON data
                        //     return {results: data};
                        // },
                    },
                })
                .on('select2:select', function (e) {
                    vm.occurrence_obj.species = e.params.data.species_id;
                    vm.species_display = e.params.data.text;
                    vm.taxon_previous_name = e.params.data.taxon_previous_name;
                    vm.common_names = e.params.data.common_names_list;
                })
                .on('select2:unselect', function () {
                    vm.occurrence_obj.species = null;
                    //vm.occurrence_obj.species_id = null
                    vm.species_display = '';
                    vm.taxon_previous_name = '';
                    vm.common_names = [];
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-' +
                            vm.scientific_name_lookup +
                            '-results"]'
                    );
                    // move focus to select2 field
                    searchField[0].focus();
                })
                .on('select2:selecting select2:unselecting', function (e) {
                    const hasRelatedReports =
                        vm.occurrence_obj.id &&
                        vm.occurrence_obj.occurrence_reports &&
                        vm.occurrence_obj.occurrence_reports.length > 0;
                    helpers.confirmChangeOfSpeciesOrCommunityName(
                        e,
                        vm.$refs[vm.scientific_name_lookup],
                        'species',
                        hasRelatedReports
                    );
                });
        },
        getSpeciesDisplay: function () {
            let vm = this;
            if (vm.occurrence_obj.species_taxonomy_id != null) {
                let species_display_url =
                    api_endpoints.species_display +
                    '?taxon_id=' +
                    vm.occurrence_obj.species_taxonomy_id;
                fetch(species_display_url).then(async (response) => {
                    const data = await response.json();
                    var newOption = new Option(data.name, data.id, false, true);
                    $('#' + vm.scientific_name_lookup).append(newOption);
                    vm.species_display = data.name;
                    vm.taxon_previous_name = data.taxon_previous_name;
                    vm.common_names = data.common_names_list;
                });
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

input[type='text'],
select {
    width: 100%;
    padding: 0.375rem 2.25rem 0.375rem 0.75rem;
}

input[type='number'] {
    width: 50%;
}
</style>
