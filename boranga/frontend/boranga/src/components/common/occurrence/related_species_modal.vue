<template lang="html">
    <div id="related-species-modal">
        <modal
            transition="modal fade"
            :title="title"
            large
            :data-loss-warning-on-cancel="!(isReadOnly || viewMode)"
            @ok="ok()"
            @cancel="cancel"
        >
            <div
                v-if="
                    associatedSpeciesTaxonomy &&
                    !fetchingAssociatedSpeciesTaxonomy
                "
                class="container-fluid"
            >
                <div class="row">
                    <form class="form-horizontal" name="threatForm">
                        <alert v-if="errors" type="danger"
                            ><strong>{{ errors }}</strong></alert
                        >
                        <div class="col-sm-12">
                            <div class="form-group">
                                <div class="row mb-3">
                                    <div class="col-sm-3">
                                        <label class="control-label"
                                            >Scientific Name:</label
                                        >
                                    </div>
                                    <div class="col-sm-9">
                                        <input
                                            v-model="
                                                associatedSpeciesTaxonomy.scientific_name
                                            "
                                            type="text"
                                            class="form-control"
                                            readonly
                                        />
                                    </div>
                                </div>
                                <div class="row mb-3">
                                    <div class="col-sm-3">
                                        <label class="control-label"
                                            >Common Name<template
                                                v-if="
                                                    associatedSpeciesTaxonomy.common_name &&
                                                    associatedSpeciesTaxonomy
                                                        .common_name.length > 1
                                                "
                                                >s</template
                                            >:</label
                                        >
                                    </div>
                                    <div class="col-sm-9">
                                        <template
                                            v-if="
                                                associatedSpeciesTaxonomy.common_name &&
                                                associatedSpeciesTaxonomy
                                                    .common_name.length > 0
                                            "
                                        >
                                            <span
                                                class="badge bg-primary me-2 p-2"
                                                v-for="common_name in associatedSpeciesTaxonomy.common_name"
                                                :key="common_name"
                                                >{{ common_name }}</span
                                            >
                                        </template>
                                        <template v-else>
                                            <textarea
                                                id="no-common-name"
                                                :disabled="true"
                                                class="form-control"
                                                rows="1"
                                                value=""
                                            />
                                        </template>
                                    </div>
                                </div>
                                <div v-if="speciesRoles" class="row mb-3">
                                    <div class="col-sm-3">
                                        <label class="control-label"
                                            >Species Role:</label
                                        >
                                    </div>
                                    <div class="col-sm-9">
                                        <template
                                            v-if="!isReadOnly && !viewMode"
                                        >
                                            <template
                                                v-if="
                                                    speciesRoles &&
                                                    speciesRoles.length > 0 &&
                                                    associatedSpeciesTaxonomy.species_role_id &&
                                                    !speciesRoles
                                                        .map((d) => d.id)
                                                        .includes(
                                                            associatedSpeciesTaxonomy.species_role_id
                                                        )
                                                "
                                            >
                                                <input
                                                    v-if="
                                                        associatedSpeciesTaxonomy.species_role
                                                    "
                                                    type="text"
                                                    class="form-control mb-3"
                                                    :value="
                                                        associatedSpeciesTaxonomy.species_role +
                                                        ' (Now Archived)'
                                                    "
                                                    disabled
                                                />
                                                <div class="mb-3 text-muted">
                                                    Change threat species role
                                                    to:
                                                </div>
                                            </template>
                                            <select
                                                v-model="
                                                    associatedSpeciesTaxonomy.species_role_id
                                                "
                                                class="form-select"
                                            >
                                                <option :value="null">
                                                    Select the species role
                                                </option>
                                                <option
                                                    v-for="speciesRole in speciesRoles"
                                                    :key="speciesRole.id"
                                                    :value="speciesRole.id"
                                                >
                                                    {{ speciesRole.name }}
                                                </option>
                                            </select>
                                        </template>
                                        <template v-else>
                                            <input
                                                v-model="
                                                    associatedSpeciesTaxonomy.species_role
                                                "
                                                type="text"
                                                class="form-control"
                                                readonly
                                            />
                                        </template>
                                    </div>
                                </div>
                                <div class="row mb-3">
                                    <div class="col-sm-3">
                                        <label class="control-label"
                                            >Comments:</label
                                        >
                                    </div>
                                    <div class="col-sm-9">
                                        <textarea
                                            v-model="
                                                associatedSpeciesTaxonomy.comments
                                            "
                                            :disabled="isReadOnly || viewMode"
                                            class="form-control"
                                        ></textarea>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </form>
                </div>
            </div>
            <div v-if="fetchingAssociatedSpeciesTaxonomy">
                <div class="text-center">
                    <span
                        class="spinner-border spinner-border-lg text-primary"
                        style="width: 3rem; height: 3rem"
                        role="status"
                        aria-hidden="true"
                    ></span>
                    <span class="visually-hidden">Loading...</span>
                </div>
            </div>
            <template #footer>
                <div>
                    <button
                        type="button"
                        class="btn btn-secondary me-2"
                        @click="cancel()"
                    >
                        Cancel
                    </button>
                    <template v-if="!isReadOnly && !viewMode">
                        <button
                            v-if="updating"
                            type="button"
                            disabled
                            class="btn btn-primary"
                            @click="ok"
                        >
                            Updating
                            <span
                                class="spinner-border spinner-border-sm"
                                role="status"
                                aria-hidden="true"
                            ></span>
                            <span class="visually-hidden">Loading...</span>
                        </button>
                        <button
                            v-else
                            type="button"
                            class="btn btn-primary"
                            @click="ok"
                        >
                            Update
                        </button>
                    </template>
                </div>
            </template>
        </modal>
    </div>
</template>

<script>
import modal from '@vue-utils/bootstrap-modal.vue';
import alert from '@vue-utils/alert.vue';
import { api_endpoints, helpers } from '@/utils/hooks.js';
export default {
    name: 'RelatedSpeciesModal',
    components: {
        modal,
        alert,
    },
    props: {
        associatedSpeciesTaxonomyId: {
            type: Number,
            required: false,
        },
        isReadOnly: {
            type: Boolean,
            default: false,
        },
    },
    data: function () {
        return {
            isModalOpen: false,
            form: null,
            associatedSpeciesTaxonomy: null,
            speciesRoles: null,
            viewMode: true,
            fetchingAssociatedSpeciesTaxonomy: false,
            updating: false,
            errors: null,
            originalAssociatedSpeciesTaxonomy: null,
        };
    },
    watch: {
        isModalOpen: function (newVal) {
            if (newVal && this.associatedSpeciesTaxonomyId) {
                this.fetchAssociatedSpeciesTaxonomy();
            }
        },
    },
    computed: {
        title: function () {
            let title = 'Related Species';
            if (this.isReadOnly || this.viewMode) {
                title = 'View ' + title;
            } else {
                title = 'Edit ' + title;
            }
            if (this.associatedSpeciesTaxonomy) {
                title += ' - ' + this.associatedSpeciesTaxonomy.scientific_name;
            }
            return title;
        },
    },
    created: async function () {
        this.fetchSpeciesRoles();
    },
    mounted: function () {
        let vm = this;
        vm.form = document.forms.threatForm;
    },
    methods: {
        hasUnsavedChanges: function () {
            return (
                JSON.stringify(this.associatedSpeciesTaxonomy) !==
                JSON.stringify(this.originalAssociatedSpeciesTaxonomy)
            );
        },
        fetchAssociatedSpeciesTaxonomy: function () {
            let vm = this;
            vm.fetchingAssociatedSpeciesTaxonomy = true;
            fetch(
                api_endpoints.associated_species_taxonomy +
                    vm.associatedSpeciesTaxonomyId +
                    '/'
            )
                .then(async (response) => {
                    const data = await response.json();
                    if (!response.ok) {
                        vm.errors = data;
                        return;
                    }
                    vm.associatedSpeciesTaxonomy = data;
                    vm.originalAssociatedSpeciesTaxonomy = JSON.parse(
                        JSON.stringify(data)
                    );
                })
                .catch((error) => {
                    console.error('Error fetching associated species:', error);
                })
                .finally(() => {
                    vm.fetchingAssociatedSpeciesTaxonomy = false;
                });
        },
        fetchSpeciesRoles: function () {
            let vm = this;
            fetch(api_endpoints.species_roles + 'no-pagination/')
                .then(async (response) => {
                    const data = await response.json();
                    if (!response.ok) {
                        vm.errors = data;
                        return;
                    }
                    vm.speciesRoles = data;
                })
                .catch((error) => {
                    console.error('Error fetching species roles:', error);
                });
        },
        ok: function () {
            let vm = this;
            if ($(vm.form).valid()) {
                vm.sendData();
            }
        },
        cancel: function () {
            if (this.isReadOnly || this.viewMode) {
                this.close();
                return;
            }
            if (!this.hasUnsavedChanges()) {
                this.close();
                return;
            }
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
            this.errors = null;
            this.viewMode = true;
            $('.has-error').removeClass('has-error');
        },
        sendData: function () {
            let vm = this;
            vm.errors = null;
            vm.associatedSpeciesTaxonomy.date_observed =
                vm.associatedSpeciesTaxonomy.date_observed == ''
                    ? null
                    : vm.associatedSpeciesTaxonomy.date_observed;
            let associatedSpeciesTaxonomy = JSON.parse(
                JSON.stringify(vm.associatedSpeciesTaxonomy)
            );

            vm.updating = true;
            fetch(
                helpers.add_endpoint_json(
                    api_endpoints.associated_species_taxonomy,
                    associatedSpeciesTaxonomy.id
                ),
                {
                    method: 'PUT',
                    body: JSON.stringify(associatedSpeciesTaxonomy),
                }
            )
                .then(async (response) => {
                    let data = await response.json();
                    if (!response.ok) {
                        vm.errors = data;
                        return;
                    }
                    vm.$parent.updatedRelatedSpecies();
                    swal.fire({
                        icon: 'success',
                        title: 'Success',
                        text: 'Related species updated successfully.',
                        confirmButtonText: 'OK',
                        buttonsStyling: false,
                        customClass: {
                            confirmButton: 'btn btn-primary',
                        },
                    }).then((result) => {
                        if (result.isConfirmed) {
                            vm.close();
                        }
                    });
                })
                .finally(() => {
                    vm.updating = false;
                });
        },
    },
};
</script>
