<template lang="html">
    <div id="splitSpecies">
        <modal
            id="species-split-modal"
            transition="modal fade"
            :title="title"
            extra-large
            @ok="ok()"
            @cancel="cancel()"
        >
            <div class="container-fluid">
                <div class="row">
                    <form class="form-horizontal" name="splitSpeciesForm">
                        <alert v-if="errorString" type="danger"
                            ><strong>{{ errorString }}</strong></alert
                        >
                        <div>
                            <div class="col-md-12">
                                <ul
                                    v-if="is_internal"
                                    id="split-pills-tab"
                                    class="nav nav-pills"
                                    role="tablist"
                                >
                                    <li class="nav-item me-2">
                                        <a
                                            id="pills-original-tab"
                                            class="nav-link small py-2"
                                            data-bs-toggle="pill"
                                            :href="'#' + originalBody"
                                            role="tab"
                                            :aria-controls="originalBody"
                                            aria-selected="true"
                                        >
                                            Original
                                        </a>
                                    </li>
                                    <li
                                        v-for="(
                                            species, index
                                        ) in split_species_list"
                                        :key="'li' + species.id"
                                        class="nav-item d-flex align-items-center"
                                    >
                                        <a
                                            :id="
                                                'pills-species-' +
                                                index +
                                                '-tab'
                                            "
                                            class="nav-link split-species-tab small py-2"
                                            data-bs-toggle="pill"
                                            :href="'#species-body-' + index"
                                            role="tab"
                                            :aria-controls="
                                                'species-body-' + index
                                            "
                                            aria-selected="false"
                                        >
                                            Split {{ index + 1 }}
                                            <span
                                                v-if="index > 1"
                                                :id="index"
                                                class="ms-2"
                                                @click.stop.prevent="
                                                    removeSpeciesTab(index)
                                                "
                                                ><i
                                                    class="bi bi-trash3-fill"
                                                ></i
                                            ></span>
                                        </a>
                                    </li>
                                    <li class="nav-item me-2">
                                        <a
                                            id="btnAdd"
                                            href="#"
                                            role="button"
                                            class="nav-link small py-2"
                                            @click.prevent="addSpeciesTab"
                                            ><i class="bi bi-window-plus"></i>
                                            Add</a
                                        >
                                    </li>
                                    <li class="nav-item me-2">
                                        <a
                                            id="assign-occurrences"
                                            class="nav-link small py-2"
                                            data-bs-toggle="pill"
                                            href="#assign-occurrences-tab-pane"
                                            role="tab"
                                            aria-controls="assign-occurrences-tab-pane"
                                            aria-selected="false"
                                            ><i class="bi bi-list-check"></i>
                                            Assign OCCs</a
                                        >
                                    </li>
                                    <li class="nav-item">
                                        <a
                                            id="finalise-split"
                                            class="nav-link small py-2"
                                            data-bs-toggle="pill"
                                            href="#finalise-split-tab-pane"
                                            role="tab"
                                            aria-controls="finalise-split-tab-pane"
                                            aria-selected="false"
                                            ><i class="bi bi-check2-circle"></i>
                                            Finalise Split</a
                                        >
                                    </li>
                                </ul>
                                <div
                                    id="split-pills-tabContent"
                                    class="tab-content border p-3"
                                >
                                    <!-- the fade show active was creating the problem of rendering two thing on tab -->
                                    <div
                                        :id="originalBody"
                                        class="tab-pane"
                                        role="tabpanel"
                                        aria-labelledby="pills-original-tab"
                                    >
                                        <SpeciesCommunitiesComponent
                                            v-if="
                                                species_community_original !=
                                                null
                                            "
                                            id="species_original"
                                            ref="species_communities_original"
                                            :species_community_original="
                                                species_community_original
                                            "
                                            :species_community="
                                                species_community_original
                                            "
                                            :is_internal="true"
                                            :is_readonly="true"
                                        >
                                            <!-- this prop is only send from split species form to make the original species readonly -->
                                        </SpeciesCommunitiesComponent>
                                    </div>
                                    <div
                                        v-for="(
                                            species, index
                                        ) in split_species_list"
                                        :id="'species-body-' + index"
                                        :key="'div' + species.id"
                                        class="tab-pane fade"
                                        role="tabpanel"
                                        :aria-labelledby="
                                            'pills-species-' + index + '-tab'
                                        "
                                    >
                                        <SpeciesSplitForm
                                            :id="'species-' + index"
                                            :ref="
                                                'species_communities_species' +
                                                index
                                            "
                                            :species_community="species"
                                            :species_original="
                                                species_community_original
                                            "
                                            :split-species-list-contains-original-taxonomy="
                                                splitSpeciesListContainsOriginalTaxonomy
                                            "
                                            :is_internal="true"
                                            :selected-taxonomies="
                                                split_species_taxonomy_ids
                                            "
                                            :split_index="index"
                                        >
                                        </SpeciesSplitForm>
                                    </div>
                                    <div
                                        id="assign-occurrences-tab-pane"
                                        class="tab-pane fade"
                                        role="tabpanel"
                                        aria-labelledby="assign-occurrences"
                                    >
                                        <div
                                            v-if="
                                                species_community_original &&
                                                uniqueScientificNames
                                            "
                                        >
                                            <div
                                                v-if="
                                                    occurrences &&
                                                    occurrences.length > 0
                                                "
                                                class="border rounded p-1"
                                                :class="
                                                    allOccurrencesAssigned
                                                        ? 'border-3 border-success'
                                                        : ''
                                                "
                                            >
                                                <div
                                                    class="overflow-auto"
                                                    style="max-height: 60vh"
                                                >
                                                    <table
                                                        class="table table-sm"
                                                        style="
                                                            table-layout: fixed;
                                                            width: 100%;
                                                        "
                                                    >
                                                        <colgroup>
                                                            <col
                                                                style="
                                                                    width: 25%;
                                                                "
                                                            />
                                                            <col
                                                                v-for="i in split_species_taxonomy_ids.length"
                                                                :key="i"
                                                                :style="{
                                                                    width:
                                                                        abbrColPercent +
                                                                        '%',
                                                                }"
                                                            />
                                                        </colgroup>
                                                        <thead>
                                                            <tr>
                                                                <th>
                                                                    Occs for
                                                                    {{
                                                                        species_community_original
                                                                            .taxonomy_details
                                                                            .scientific_name
                                                                    }}
                                                                </th>
                                                                <th
                                                                    v-for="name in uniqueScientificNames"
                                                                    :key="
                                                                        name.scientificName
                                                                    "
                                                                    :title="
                                                                        name.full
                                                                    "
                                                                    class="text-center"
                                                                    :style="{
                                                                        width:
                                                                            abbrColPercent +
                                                                            '%',
                                                                    }"
                                                                >
                                                                    {{
                                                                        name.abbr
                                                                    }}
                                                                </th>
                                                            </tr>
                                                        </thead>
                                                        <tbody>
                                                            <tr
                                                                class="border-bottom border-dark"
                                                            >
                                                                <td
                                                                    class="text-muted"
                                                                >
                                                                    Select All
                                                                </td>
                                                                <td
                                                                    v-for="(
                                                                        taxonomy_id,
                                                                        i
                                                                    ) in split_species_taxonomy_ids"
                                                                    :key="i"
                                                                    class="text-center"
                                                                    :style="{
                                                                        width:
                                                                            abbrColPercent +
                                                                            '%',
                                                                    }"
                                                                >
                                                                    <div
                                                                        class="form-check form-check-inline"
                                                                    >
                                                                        <input
                                                                            type="checkbox"
                                                                            class="form-check-input mt-2"
                                                                            @change="
                                                                                toggleSelectAll(
                                                                                    $event,
                                                                                    taxonomy_id
                                                                                )
                                                                            "
                                                                            :checked="
                                                                                selectAllCheckedState[
                                                                                    uniqueScientificNames[
                                                                                        i
                                                                                    ]
                                                                                        .slug
                                                                                ]
                                                                            "
                                                                        />
                                                                    </div>
                                                                </td>
                                                            </tr>
                                                            <tr
                                                                v-for="occurrence in occurrences"
                                                                :key="
                                                                    occurrence.id
                                                                "
                                                                :id="
                                                                    'occurrence-' +
                                                                    occurrence.id
                                                                "
                                                                class="occurrence-assignment-row"
                                                            >
                                                                <td
                                                                    class="abbr-nowrap small"
                                                                    :title="
                                                                        occurrence.occurrence_name
                                                                    "
                                                                >
                                                                    {{
                                                                        occurrence.occurrence_number
                                                                    }}
                                                                    -
                                                                    {{
                                                                        occurrence.occurrence_name
                                                                    }}
                                                                </td>
                                                                <td
                                                                    v-for="(
                                                                        taxonomyId,
                                                                        index
                                                                    ) in split_species_taxonomy_ids"
                                                                    :key="index"
                                                                    class="text-center"
                                                                    :style="{
                                                                        width:
                                                                            abbrColPercent +
                                                                            '%',
                                                                    }"
                                                                >
                                                                    <div
                                                                        class="form-check form-check-inline"
                                                                    >
                                                                        <input
                                                                            type="checkbox"
                                                                            :name="
                                                                                'occurrence:' +
                                                                                occurrence.id
                                                                            "
                                                                            :checked="
                                                                                assignmentCheckedState[
                                                                                    occurrence
                                                                                        .id
                                                                                ] ===
                                                                                taxonomyId
                                                                            "
                                                                            @change="
                                                                                onOccurrenceCheckboxChange(
                                                                                    occurrence.id,
                                                                                    taxonomyId,
                                                                                    $event
                                                                                )
                                                                            "
                                                                            class="form-check-input mt-2"
                                                                            :data-slug="
                                                                                uniqueScientificNames[
                                                                                    index
                                                                                ]
                                                                                    .slug
                                                                            "
                                                                        />
                                                                    </div>
                                                                </td>
                                                            </tr>
                                                        </tbody>
                                                    </table>
                                                </div>
                                            </div>
                                            <template v-else>
                                                <div
                                                    class="alert alert-info pb-0"
                                                >
                                                    <p>
                                                        The original species ({{
                                                            species_community_original.species_number
                                                        }}
                                                        -
                                                        {{
                                                            species_community_original
                                                                .taxonomy_details
                                                                .scientific_name
                                                        }}) has no occurrences
                                                        to assign.
                                                    </p>
                                                    <p>
                                                        You can continue to the
                                                        finalise split tab.
                                                    </p>
                                                </div>
                                            </template>
                                        </div>
                                    </div>
                                    <div
                                        v-if="
                                            species_community_original &&
                                            split_species_list &&
                                            split_species_list.length > 0
                                        "
                                        id="finalise-split-tab-pane"
                                        class="tab-pane"
                                        role="tabpanel"
                                        aria-labelledby="finalise-split"
                                    >
                                        <p class="border-bottom mb-3">
                                            <HelpText
                                                section_id="species_split_finalise"
                                            />
                                        </p>

                                        <p>
                                            You are about to split the species:
                                        </p>

                                        <div class="border-bottom mb-3 pb-3">
                                            <span
                                                class="badge bg-light text-primary border p-2 fs-6"
                                                >{{
                                                    species_community_original.species_number
                                                }}
                                                <template
                                                    v-if="
                                                        species_community_original
                                                            .taxonomy_details
                                                            .scientific_name
                                                    "
                                                    >-
                                                    {{
                                                        species_community_original
                                                            .taxonomy_details
                                                            .scientific_name
                                                    }}</template
                                                ></span
                                            >
                                        </div>

                                        <p>Into the following species:</p>

                                        <div class="border-bottom mb-3 pb-3">
                                            <ul class="mb-3">
                                                <li
                                                    v-for="species in split_species_list"
                                                    :key="species.id"
                                                    class="text-secondary mb-3"
                                                >
                                                    <span
                                                        v-if="
                                                            species &&
                                                            species.taxonomy_details
                                                        "
                                                        class="badge bg-light text-primary border p-2 fs-6 me-2"
                                                    >
                                                        {{
                                                            species
                                                                .taxonomy_details
                                                                .scientific_name
                                                        }}
                                                    </span>
                                                </li>
                                            </ul>
                                        </div>

                                        <button
                                            class="button btn btn-primary"
                                            :disabled="finalise_split_loading"
                                            @click.prevent="ok()"
                                        >
                                            <i class="bi bi-check2-circle"></i>
                                            Finalise Split
                                            <template
                                                v-if="finalise_split_loading"
                                                ><span
                                                    class="spinner-border spinner-border-sm"
                                                    role="status"
                                                    aria-hidden="true"
                                                ></span>
                                                <span class="visually-hidden"
                                                    >Loading...</span
                                                ></template
                                            >
                                        </button>
                                    </div>
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
                        @click="cancel"
                    >
                        Cancel
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
import SpeciesSplitForm from '@/components/form_species_split.vue';
import HelpText from '@/components/common/help_text.vue';

import { helpers, api_endpoints } from '@/utils/hooks.js';
export default {
    name: 'SpeciesSplit',
    components: {
        modal,
        alert,
        SpeciesCommunitiesComponent,
        SpeciesSplitForm,
        HelpText,
    },
    props: {
        species_community: {
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
            originalBody: 'originalBody' + uuid(),
            species2Body: 'species2Body' + uuid(),
            species_community_original: JSON.parse(
                JSON.stringify(this.species_community)
            ),
            submitSpeciesSplit: false,
            assignmentCheckedState: {},
            occurrences: null,
            isModalOpen: false,
            finalise_split_loading: false,
            split_species_list: [],
            form: null,
            errorString: '',
        };
    },
    watch: {
        isModalOpen: function (newVal) {
            if (newVal) {
                this.$nextTick(() => {
                    // Show the first tab by default
                    let originalTab = bootstrap.Tab.getOrCreateInstance(
                        document.querySelector('#pills-original-tab')
                    );
                    originalTab.show();
                    // Create two new species tabs if the original species is provided
                    this.addSpeciesTab();
                    // We fetch occurrences here and also when the assign occurrences tab is shown
                    // to ensure we have the latest occurrences when the modal is opened
                    this.occurrences = this.fetchOccurrencesOfOriginalSpecies();

                    this.addSpeciesTab();
                    // Select the first split species tab
                    this.$nextTick(() => {
                        // Show the first split species tab
                        let splitSpeciesTabs =
                            document.querySelectorAll('.split-species-tab');
                        if (splitSpeciesTabs.length > 0) {
                            let firstTabEl = splitSpeciesTabs[0];
                            let firstTab =
                                bootstrap.Tab.getOrCreateInstance(firstTabEl);
                            firstTab.show();
                        }
                    });
                });
            }
        },
    },
    computed: {
        csrf_token: function () {
            return helpers.getCookie('csrftoken');
        },
        title: function () {
            return this.species_community_original != null
                ? 'Split Species ' +
                      this.species_community_original.species_number
                : 'Split Species';
        },
        split_species_taxonomy_ids: function () {
            return this.split_species_list
                .filter(
                    (species) =>
                        species != null &&
                        species.taxonomy_id &&
                        species.taxonomy_details != null
                )
                .map((species) => species.taxonomy_id);
        },
        splitSpeciesListContainsOriginalTaxonomy: function () {
            return (
                this.split_species_list.length > 0 &&
                this.split_species_list.some(
                    (species) =>
                        species.taxonomy_id ===
                        this.species_community_original.taxonomy_id
                )
            );
        },
        uniqueScientificNames: function () {
            return this.abbreviateUnique(
                this.split_species_list
                    .filter((species) => species.taxonomy_details != null)
                    .map((species) => species.taxonomy_details.scientific_name)
                    .filter(
                        (scientificName) =>
                            scientificName !== '' && scientificName != null
                    ),
                7
            );
        },
        abbrColPercent() {
            // 1 for the first column, rest for split species
            const n = this.split_species_list.length;
            if (n === 0) return 0;
            // For example, reserve 25% for the first column, rest for split species
            const abbrCols = n;
            const abbrPercent = Math.floor(75 / abbrCols); // 75% divided among split species
            return abbrPercent;
        },
        selectAllCheckedState() {
            const state = {};
            if (this.occurrences === null) {
                // Occurrence not yet fetched
                return state;
            }
            this.uniqueScientificNames.forEach((name, i) => {
                const taxonomy_id = this.split_species_taxonomy_ids[i];
                state[name.slug] =
                    this.occurrences.length > 0 &&
                    this.occurrences.every(
                        (occurrence) =>
                            this.assignmentCheckedState[occurrence.id] ===
                            taxonomy_id
                    );
            });
            return state;
        },
        allOccurrencesAssigned: function () {
            if (this.occurrences == null) {
                // Occurrence not yet fetched
                return false;
            }
            if (this.occurrences.length === 0) {
                return true;
            }
            return (
                this.occurrences.length > 0 &&
                this.assignmentCheckedState &&
                Object.values(this.assignmentCheckedState).length > 0 &&
                Object.values(this.assignmentCheckedState).every(
                    (value) =>
                        value !== null &&
                        value !== '' &&
                        value !== undefined &&
                        value !== false
                )
            );
        },
    },
    mounted: function () {
        let vm = this;
        vm.form = document.forms.splitSpeciesForm;
        vm.beforeShowFinaliseTab();
        vm.beforeShowAssignOccurrencesTab();
    },
    methods: {
        ok: function () {
            let vm = this;
            if ($(vm.form).valid()) {
                vm.processSplitSpecies();
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
            this.split_species_list = [];
            this.errorString = '';
        },
        can_submit: function () {
            let vm = this;
            let blank_fields = [];
            for (let index = 0; index < vm.split_species_list.length; index++) {
                if (
                    vm.split_species_list[index].taxonomy_id == null ||
                    vm.split_species_list[index].taxonomy_id == ''
                ) {
                    blank_fields.push(
                        ' Split Species ' +
                            (index + 1) +
                            ' Scientific Name is missing'
                    );
                }
                if (
                    vm.split_species_list[index].distribution.distribution ==
                        null ||
                    vm.split_species_list[index].distribution.distribution == ''
                ) {
                    blank_fields.push(
                        ' Split Species ' +
                            (index + 1) +
                            ' Distribution is missing'
                    );
                }
                if (
                    vm.split_species_list[index].regions == null ||
                    vm.split_species_list[index].regions == '' ||
                    vm.split_species_list[index].regions.length == 0
                ) {
                    blank_fields.push(
                        ' Split Species ' + (index + 1) + ' Region is missing'
                    );
                }
                if (
                    vm.split_species_list[index].districts == null ||
                    vm.split_species_list[index].districts == '' ||
                    vm.split_species_list[index].districts.length == 0
                ) {
                    blank_fields.push(
                        ' Split Species ' + (index + 1) + ' District is missing'
                    );
                }
            }
            if (blank_fields.length == 0) {
                return true;
            } else {
                return blank_fields;
            }
        },
        processSplitSpecies: async function () {
            let vm = this;

            var missing_data = vm.can_submit();
            if (missing_data != true) {
                swal.fire({
                    title: 'Please fix following errors before submitting',
                    text: missing_data,
                    icon: 'error',
                    customClass: {
                        confirmButton: 'btn btn-primary',
                    },
                });
                return false;
            }
            let payload = {
                split_species_list: vm.split_species_list,
                occurrence_assignments: vm.assignmentCheckedState,
            };
            vm.submitSpeciesSplit = true;
            swal.fire({
                title: 'Split Species',
                text: 'Are you sure you want to split this species?',
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Split Species',
                customClass: {
                    confirmButton: 'btn btn-primary',
                    cancelButton: 'btn btn-secondary',
                },
                reverseButtons: true,
            })
                .then(async (swalresult) => {
                    if (swalresult.isConfirmed) {
                        vm.finalise_split_loading = true;
                        let submit_url = helpers.add_endpoint_json(
                            api_endpoints.species,
                            vm.species_community_original.id + '/split_species'
                        );
                        fetch(submit_url, {
                            method: 'POST',
                            headers: {
                                'Content-Type': 'application/json',
                            },
                            body: JSON.stringify(payload),
                        }).then(
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
                                vm.$router.push({
                                    name: 'internal-species-communities-dash',
                                });
                            },
                            (err) => {
                                swal.fire({
                                    title: 'Submit Error',
                                    text: helpers.apiVueResourceError(err),
                                    icon: 'error',
                                    customClass: {
                                        confirmButton: 'btn btn-primary',
                                    },
                                });
                                vm.saveError = true;
                            }
                        );
                    }
                })
                .finally(() => {
                    vm.finalise_split_loading = false;
                    vm.submitSpeciesSplit = false;
                });
        },
        addSpeciesTab: function () {
            let vm = this;
            let newSpecies = JSON.parse(
                JSON.stringify(vm.species_community_original)
            );
            newSpecies.id = null;
            newSpecies.species_number = '';
            newSpecies.taxonomy_id = null;
            newSpecies.taxonomy_details = {};
            newSpecies.copy_all_documents = true;
            newSpecies.copy_all_threats = true;
            newSpecies.threat_ids_to_copy = [];
            newSpecies.document_ids_to_copy = [];
            newSpecies.index = vm.split_species_list.length;
            vm.split_species_list.push(newSpecies);
            vm.$nextTick(() => {
                // Show the last remaining split species tab
                let splitSpeciesTabs =
                    document.querySelectorAll('.split-species-tab');
                let lastTabEl = splitSpeciesTabs[splitSpeciesTabs.length - 1];
                let lastTab = bootstrap.Tab.getOrCreateInstance(lastTabEl);
                lastTab.show();
            });
        },
        removeSpeciesTab: function (index) {
            let vm = this;
            swal.fire({
                title: 'Remove Species',
                text: `Are you sure you want to remove species ${index + 1} from the split?`,
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Remove Species',
                reverseButtons: true,
                customClass: {
                    confirmButton: 'btn btn-primary',
                    cancelButton: 'btn btn-secondary',
                },
            }).then(async (swalresult) => {
                if (swalresult.isConfirmed) {
                    vm.split_species_list.splice(index, 1);
                    vm.$nextTick(() => {
                        // Check if there are any split species tabs left
                        if (vm.split_species_list.length === 0) {
                            // Show the original species tab if no split species left
                            let originalTabEl = document.querySelector(
                                '#pills-original-tab'
                            );
                            let originalTab =
                                bootstrap.Tab.getOrCreateInstance(
                                    originalTabEl
                                );
                            originalTab.show();
                        } else {
                            // Show the last remaining split species tab
                            let splitSpeciesTabs =
                                document.querySelectorAll('.split-species-tab');
                            let lastTabEl =
                                splitSpeciesTabs[splitSpeciesTabs.length - 1];
                            let lastTab =
                                bootstrap.Tab.getOrCreateInstance(lastTabEl);
                            lastTab.show();
                        }
                    });
                }
            });
        },
        validateAtLeastTwoSplitSpecies: function (event, action) {
            if (this.split_species_list.length < 2) {
                event.preventDefault(); // Prevent the tab from being shown
                swal.fire({
                    title: 'Add at Least Two Split Species',
                    text: `You must have at least two split species before you can ${action}.`,
                    icon: 'info',
                    customClass: {
                        confirmButton: 'btn btn-primary',
                    },
                });
                return false;
            }
            return true;
        },
        validateAllSplitSpeciesHaveTaxonomy: function (event, action) {
            for (const species of this.split_species_list) {
                if (!species.taxonomy_id || species.taxonomy_id === '') {
                    event.preventDefault();
                    swal.fire({
                        title: 'Missing Scientific Name',
                        text: `Each Split Species ${species.species_number} must have a scientific name before ${action}`,
                        icon: 'info',
                        customClass: {
                            confirmButton: 'btn btn-primary',
                        },
                    });
                    return false;
                }
            }
            return true;
        },
        beforeShowFinaliseTab: function () {
            let vm = this;
            // Add a bootstrap event before the finalise tab is shown
            let tabEl = document.querySelector('#finalise-split');
            tabEl.addEventListener('show.bs.tab', function (event) {
                if (
                    !vm.validateAtLeastTwoSplitSpecies(
                        event,
                        'finalising the split'
                    )
                ) {
                    return;
                }
                if (
                    !vm.validateAllSplitSpeciesHaveTaxonomy(
                        event,
                        'finalising the split'
                    )
                ) {
                    return;
                }
                if (!vm.allOccurrencesAssigned) {
                    event.preventDefault(); // Prevent the tab from being shown
                    swal.fire({
                        title: 'Unassigned Occurrences',
                        text: `You have unassigned occurrences. Please assign all occurrences before finalising the split.`,
                        icon: 'warning',
                        customClass: {
                            confirmButton: 'btn btn-primary',
                        },
                        didClose: function () {
                            let assignTabEl = document.querySelector(
                                '#assign-occurrences'
                            );
                            let tab =
                                bootstrap.Tab.getOrCreateInstance(assignTabEl);
                            tab.show();
                        },
                    });
                }
            });
        },
        beforeShowAssignOccurrencesTab: function () {
            let vm = this;
            // Add a bootstrap event before the assign occurrences tab is shown
            let tabEl = document.querySelector('#assign-occurrences');
            tabEl.addEventListener('show.bs.tab', async function (event) {
                if (
                    !vm.validateAtLeastTwoSplitSpecies(
                        event,
                        'assigning occurrences'
                    )
                ) {
                    return false;
                }
                if (
                    !vm.validateAllSplitSpeciesHaveTaxonomy(
                        event,
                        'assigning occurrences'
                    )
                ) {
                    return false;
                }
                vm.occurrences = vm.fetchOccurrencesOfOriginalSpecies();
            });
        },
        abbreviateUnique: function (scientificNames, minChars = 1) {
            function abbreviate(name, restLens = []) {
                // Remove brackets, dots, and commas from the name
                const cleaned = name.trim().replace(/[[\]().,]/g, '');
                const words = cleaned.split(/\s+/);
                if (words.length === 1) {
                    // Single word: abbreviate to minChars or more as needed
                    return words[0].slice(0, Math.max(minChars, 1));
                }
                // First word in full, rest abbreviated (unless number)
                let abbr = [words[0]];
                for (let i = 1; i < words.length; i++) {
                    const word = words[i];
                    // If the word is a number, do not abbreviate it
                    if (/^\d+$/.test(word)) {
                        abbr.push(word);
                    } else {
                        const len = restLens[i - 1] || 1;
                        abbr.push(word.slice(0, len));
                    }
                }
                return abbr.join(' ');
            }

            // Start with 1 letter for each word after the first
            let restLensArr = scientificNames.map((name) => {
                const cleaned = name.trim().replace(/[[\]().,]/g, '');
                const words = cleaned.split(/\s+/);
                return Array(words.length - 1).fill(1);
            });
            let abbrs = scientificNames.map((name, i) =>
                abbreviate(name, restLensArr[i])
            );

            let unique = false;
            while (!unique) {
                unique = true;
                let seen = {};
                for (let i = 0; i < abbrs.length; i++) {
                    if (seen[abbrs[i]] !== undefined) {
                        unique = false;
                        // For all with this abbreviation, increase the last abbreviated word by 1 (if not a number)
                        for (let j = 0; j < abbrs.length; j++) {
                            if (abbrs[j] === abbrs[i]) {
                                let restLens = restLensArr[j];
                                const cleaned = scientificNames[j]
                                    .trim()
                                    .replace(/[[\]().,]/g, '');
                                const words = cleaned.split(/\s+/);
                                for (let k = restLens.length - 1; k >= 0; k--) {
                                    if (!/^\d+$/.test(words[k + 1])) {
                                        restLens[k]++;
                                        break;
                                    }
                                }
                                abbrs[j] = abbreviate(
                                    scientificNames[j],
                                    restLens
                                );
                            }
                        }
                    } else {
                        seen[abbrs[i]] = i;
                    }
                }
            }
            // Return array of objects with both full and abbreviated names
            return scientificNames.map((full, idx) => ({
                full,
                abbr: abbrs[idx],
                slug: full
                    .toLowerCase()
                    .replace(/[\s[\]().,]+/g, '-')
                    .replace(/-+/g, '-')
                    .replace(/^-|-$/g, ''),
            }));
        },
        fetchOccurrencesOfOriginalSpecies: async function () {
            const prevAssignments = { ...this.assignmentCheckedState };
            fetch(api_endpoints.occurrences_by_species_id, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    species_id: this.species_community_original.id,
                }),
            })
                .then((response) => response.json())
                .then((data) => {
                    this.occurrences = data;
                    // Only update assignmentCheckedState for new/removed occurrences
                    const newAssignments = {};
                    this.occurrences.forEach((occurrence) => {
                        // Preserve previous assignment if exists, else set to false
                        newAssignments[occurrence.id] =
                            prevAssignments[occurrence.id] !== undefined
                                ? prevAssignments[occurrence.id]
                                : false;
                    });
                    this.assignmentCheckedState = newAssignments;
                })
                .catch((error) => {
                    console.error('Error fetching occurrences:', error);
                    this.errorString = 'Error fetching occurrences';
                });
        },
        toggleSelectAll: function (event, taxonomyId) {
            const shouldCheck = event.target.checked;
            this.occurrences.forEach((occurrence) => {
                if (shouldCheck) {
                    // Assign all to this species
                    this.assignmentCheckedState[occurrence.id] = taxonomyId;
                } else {
                    // Unassign all occurrences currently assigned to this species
                    if (
                        this.assignmentCheckedState[occurrence.id] ===
                        taxonomyId
                    ) {
                        this.assignmentCheckedState[occurrence.id] = false;
                    }
                }
            });
        },
        onOccurrenceCheckboxChange(occurrenceId, taxonomyId, event) {
            if (event.target.checked) {
                // Only one per row: assign this taxonomyId
                this.assignmentCheckedState[occurrenceId] = taxonomyId;
            } else {
                // Unchecked: clear assignment
                this.assignmentCheckedState[occurrenceId] = false;
            }
        },
    },
};
</script>

<style scoped>
.bi.bi-trash3-fill:hover {
    cursor: pointer;
    color: red;
}

.abbr-nowrap {
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    vertical-align: middle;
}
</style>
