<template lang="html">
    <div id="renameSpecies">
        <modal
            id="species-rename-modal"
            transition="modal fade"
            :title="title"
            extra-large
            @ok="ok()"
            @cancel="cancel()"
        >
            <div class="container-fluid">
                <div class="row">
                    <form class="form-horizontal" name="renameSpeciesForm">
                        <alert v-if="errorString" type="danger"
                            ><strong>{{ errorString }}</strong></alert
                        >
                        <div>
                            <div class="col-md-12">
                                <SpeciesCommunitiesComponent
                                    v-if="species_community_original_copy"
                                    id="rename_species"
                                    ref="rename_species"
                                    :species_community_original="
                                        species_community_original_copy
                                    "
                                    :species_community="
                                        species_community_original_copy
                                    "
                                    :is_internal="true"
                                    :is_readonly="true"
                                    :rename_species="true"
                                >
                                    // rename=true used to make only taxon
                                    select editable on form
                                </SpeciesCommunitiesComponent>
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
                        :disabled="submitSpeciesRename"
                        @click="cancel"
                    >
                        Cancel
                    </button>
                    <button
                        v-if="submitSpeciesRename"
                        class="btn btn-primary pull-right"
                        style="margin-top: 5px"
                        disabled
                    >
                        Rename Species
                        <span
                            class="spinner-border spinner-border-sm"
                            role="status"
                            aria-hidden="true"
                        ></span>
                        <span class="visually-hidden">Loading...</span>
                    </button>
                    <button
                        v-else
                        class="btn btn-primary"
                        :disabled="submitSpeciesRename"
                        @click.prevent="ok()"
                    >
                        Rename Species
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
import SpeciesCommunitiesComponent from '@/components/form_species_communities.vue';
import { helpers, api_endpoints } from '@/utils/hooks.js';
export default {
    name: 'SpeciesRename',
    components: {
        modal,
        alert,
        SpeciesCommunitiesComponent,
    },
    props: {
        species_community_original: {
            type: Object,
            required: true,
        },
        is_internal: {
            type: Boolean,
            required: true,
        },
    },
    data: function () {
        return {
            newSpeciesBody: 'newSpeciesBody' + uuid(),
            speciesBody: 'speciesBody' + uuid(),
            species_community_original_copy: null,
            submitSpeciesRename: false,
            isModalOpen: false,
            form: null,
            errorString: '',
        };
    },
    watch: {
        isModalOpen: function (newValue) {
            if (newValue) {
                this.species_community_original_copy = Object.assign(
                    {},
                    this.species_community_original
                );
                this.$nextTick(() => {
                    const selectEl =
                        this.$refs.rename_species.$refs.species_information
                            .$refs.scientific_name_lookup_rename;
                    $(selectEl).val(null).trigger('change');
                    $(selectEl).select2('open');
                });
            }
        },
    },
    computed: {
        title: function () {
            if (!this.species_community_original_copy) {
                return 'Rename Species';
            }
            return `Rename Species ${this.species_community_original_copy.species_number} ${
                this.species_community_original_copy.taxonomy_details
                    .scientific_name
            }`;
        },
    },
    mounted: function () {
        let vm = this;
        vm.form = document.forms.renameSpeciesForm;
    },
    methods: {
        ok: function () {
            let vm = this;
            if ($(vm.form).valid()) {
                vm.sendData();
            }
        },
        cancel: function () {
            swal.fire({
                title: 'Are you sure you want to close this pop-up?',
                text: 'You will lose any unsaved changes.',
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Yes, close it',
                cancelButtonText: 'Return to pop-up',
                reverseButtons: true,
                customClass: {
                    confirmButton: 'btn btn-primary',
                    cancelButton: 'btn btn-secondary',
                },
            }).then((result) => {
                if (result.isConfirmed) {
                    this.close();
                }
            });
        },
        close: function () {
            this.isModalOpen = false;
            this.errorString = '';
        },
        save_before_submit: async function (new_species) {
            let vm = this;
            vm.saveError = false;
            let payload = new Object();
            Object.assign(payload, new_species);
            const result = await fetch(
                `/api/species/${new_species.id}/species_save.json`,
                {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify(payload),
                }
            )
                .then(async (response) => {
                    const data = await response.json();
                    if (!response.ok) {
                        swal.fire({
                            title: 'Submit Error',
                            text: JSON.stringify(data),
                            icon: 'error',
                            customClass: {
                                confirmButton: 'btn btn-primary',
                            },
                        });
                        vm.saveError = true;
                        return;
                    }
                    return true;
                })
                .finally(() => {
                    vm.submitSpeciesRename = false;
                });
            return result;
        },
        sendData: async function () {
            let vm = this;
            if (!vm.species_community_original_copy.taxonomy_id) {
                swal.fire({
                    title: 'Please fix following errors',
                    text: 'Please select a species by searching for the scientific name',
                    icon: 'error',
                    customClass: {
                        confirmButton: 'btn btn-primary',
                    },
                });
                return false;
            }

            if (
                vm.species_community_original_copy.taxonomy_id ==
                vm.species_community_original.taxonomy_id
            ) {
                swal.fire({
                    title: 'Please fix following errors',
                    text: 'The selected species is the same as the original species.',
                    icon: 'error',
                    customClass: {
                        confirmButton: 'btn btn-primary',
                    },
                });
                return false;
            }

            const swalresult = await swal.fire({
                title: 'Rename Species',
                html: '<p>Are you sure you want to rename this species?</p><p class="small text-muted">Note: Renaming a species that has many occurrences can take up to a minute</p>',
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Rename Species',
                customClass: {
                    confirmButton: 'btn btn-primary',
                    cancelButton: 'btn btn-secondary',
                },
                reverseButtons: true,
            });

            if (!swalresult.isConfirmed) {
                return;
            }

            vm.submitSpeciesRename = true;
            try {
                const response = await fetch(
                    helpers.add_endpoint_json(
                        api_endpoints.species,
                        vm.species_community_original.id + '/rename_species'
                    ),
                    {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json',
                        },
                        body: JSON.stringify(
                            vm.species_community_original_copy
                        ),
                    }
                );
                const data = await response.json();
                if (!response.ok) {
                    swal.fire({
                        title: 'Submit Error',
                        text: JSON.stringify(data),
                        icon: 'error',
                        customClass: {
                            confirmButton: 'btn btn-primary',
                        },
                    });
                    vm.saveError = true;
                    return;
                }
                vm.new_species = data;
                vm.$router.push({
                    name: 'internal-species-communities-dash',
                });
            } catch (err) {
                swal.fire({
                    title: 'Submit Error',
                    text: helpers.apiVueResourceError(err),
                    icon: 'error',
                    customClass: {
                        confirmButton: 'btn btn-primary',
                    },
                });
                vm.saveError = true;
            } finally {
                vm.submitSpeciesRename = false;
            }
        },
    },
};
</script>
