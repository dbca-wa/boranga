<template lang="html">
    <div id="add-combine-species">
        <modal
            id="add-species-to-combine-modal"
            :style="{ '--bs-modal-margin': '4rem auto auto auto' }"
            class="mt-3"
            transition="modal fade"
            :title="title"
            large
            @ok="ok()"
            @cancel="close()"
            :data-loss-warning-on-cancel="false"
            stacked
        >
            <div class="container-fluid">
                <div class="row">
                    <form class="form-horizontal" name="add-species-to-combine">
                        <alert v-if="errorString" type="danger"
                            ><strong>{{ errorString }}</strong></alert
                        >
                        <div>
                            <div class="row mb-3">
                                <label for="" class="col-sm-3 control-label"
                                    >Species:</label
                                >
                                <div
                                    :id="selectScientificName"
                                    class="col-sm-9"
                                >
                                    <select
                                        :id="scientificNameLookup"
                                        :ref="scientificNameLookup"
                                        :name="scientificNameLookup"
                                        class="form-control"
                                        required
                                    />
                                </div>
                            </div>
                        </div>
                    </form>
                </div>
            </div>
            <template #footer>
                <div>
                    <button
                        type="button"
                        class="btn btn-secondary me-2"
                        @click="close()"
                    >
                        Cancel
                    </button>
                    <button class="btn btn-primary" @click.prevent="ok()">
                        Confirm Selection
                    </button>
                </div>
            </template>
        </modal>
    </div>
</template>

<script>
import { v4 as uuid } from 'uuid';
import modal from '@vue-utils/bootstrap-modal.vue';
import alert from '@vue-utils/alert.vue';
import { helpers, api_endpoints } from '@/utils/hooks.js';
export default {
    name: 'AddCombineSpecies',
    components: {
        modal,
        alert,
    },
    data: function () {
        return {
            scientificNameLookup: 'scientific-name-lookup' + uuid(),
            selectScientificName: 'select-scientific-name' + uuid(),
            isModalOpen: false,
            errorString: '',
            selectedSpeciesObject: null,
        };
    },
    computed: {
        title: function () {
            return 'Select a Scientific Name To Add it to the Combine';
        },
    },
    mounted: function () {
        let vm = this;
        this.$nextTick(() => {
            vm.initialiseScientificNameLookup();
        });
    },
    watch: {
        isModalOpen: function (newValue) {
            if (newValue) {
                // open the select2 dropdown
                this.$nextTick(() => {
                    $(this.$refs[this.scientificNameLookup]).select2('open');
                });
            }
        },
    },
    methods: {
        ok: function () {
            if ($(document.forms['add-species-to-combine']).valid()) {
                this.addSpeciesToCombineList();
            }
        },
        close: function () {
            this.isModalOpen = false;
            this.errorString = '';
        },
        addSpeciesToCombineList: function () {
            this.$parent.addSpeciesObjectToCombineList(
                this.selectedSpeciesObject
            );
            this.$nextTick(() => {
                this.$parent.showLastCombineSpeciesTab();
            });
            $(this.$refs[this.scientificNameLookup])
                .val(null)
                .trigger('change');
            this.close();
        },
        initialiseScientificNameLookup: function () {
            let vm = this;
            $(vm.$refs[vm.scientificNameLookup])
                .select2({
                    minimumInputLength: 2,
                    language: {
                        inputTooShort: function () {
                            return 'Use % for wildcard search';
                        },
                    },
                    dropdownParent: $('#' + vm.selectScientificName),
                    theme: 'bootstrap-5',
                    allowClear: true,
                    placeholder:
                        'Search for the Scientific Name to add to the Combine',
                    ajax: {
                        url: api_endpoints.scientific_name_lookup,
                        dataType: 'json',
                        data: function (params) {
                            var query = {
                                term: params.term,
                                type: 'public',
                                group_type_id:
                                    vm.$parent.species_community.group_type_id,
                                has_species: true,
                                active_draft_and_historical_only: true,
                            };
                            return query;
                        },
                    },
                })
                .on('select2:select', function (e) {
                    let speciesId = e.params.data.species_id;
                    let taxonomyId = e.params.data.id;
                    if (speciesId) {
                        // A boranga profile already exists with this taxonomy ID
                        vm.fetchSpeciesData(speciesId);
                    } else {
                        // No boranga profile exists, create an empty one with just the taxonomy data
                        // populated
                        vm.getEmptySpeciesObject(taxonomyId);
                    }
                })
                .on('select2:unselect', function () {
                    vm.selectedSpeciesObject = null;
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-' +
                            vm.scientificNameLookup +
                            '-results"]'
                    );
                    searchField[0].focus();
                });
        },
        fetchSpeciesData: function (speciesId) {
            fetch(helpers.add_endpoint_json(api_endpoints.species, speciesId))
                .then((response) => response.json())
                .then((data) => {
                    this.selectedSpeciesObject = JSON.parse(
                        JSON.stringify(data)
                    );
                })
                .catch((error) => {
                    console.error('Error fetching species data:', error);
                });
        },
        getEmptySpeciesObject: function (taxonomyId) {
            fetch(
                helpers.add_endpoint_json(
                    api_endpoints.get_empty_species_object,
                    taxonomyId
                )
            )
                .then((response) => response.json())
                .then((data) => {
                    this.selectedSpeciesObject = JSON.parse(
                        JSON.stringify(data)
                    );
                })
                .catch((error) => {
                    console.error(
                        'Error fetching empty species object data:',
                        error
                    );
                });
        },
    },
};
</script>
<style scoped>
/* In your parent or global CSS */
:deep(.modal-dialog-stacked) {
    margin-top: 6rem !important;
}
</style>
