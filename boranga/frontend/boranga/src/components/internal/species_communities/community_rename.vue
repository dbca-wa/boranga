<template lang="html">
    <div id="rename-community">
        <modal
            id="rename-community-modal"
            transition="modal fade"
            :title="title"
            extra-large
            @ok="ok()"
            @cancel="cancel"
        >
            <div class="container-fluid">
                <div class="row">
                    <alert
                        type="primary"
                        class="d-flex align-items-center py-1 mb-3"
                    >
                        <div class="float-start pe-3">
                            <span class="align-middle"
                                ><i
                                    class="bi bi-info-circle-fill text-primary fs-4"
                                ></i
                            ></span>
                        </div>
                        <ul class="list-group list-group-flush">
                            <li class="list-group-item bg-transparent">
                                All fields, documents and threats will be copied
                                from
                                <span class="fw-bold"
                                    >{{
                                        species_community_original.community_number
                                    }}
                                    -
                                    {{
                                        species_community_original
                                            .taxonomy_details.community_name
                                    }}</span
                                >
                                to the resulting community record
                            </li>
                            <li class="list-group-item bg-transparent">
                                All occurrences of
                                <span class="fw-bold"
                                    >{{
                                        species_community_original.community_number
                                    }}
                                    -
                                    {{
                                        species_community_original
                                            .taxonomy_details.community_name
                                    }}</span
                                >
                                will be reassigned to the new community record
                            </li>
                            <li class="list-group-item bg-transparent">
                                If
                                <span class="fw-bold"
                                    >{{
                                        species_community_original.community_number
                                    }}
                                    -
                                    {{
                                        species_community_original
                                            .taxonomy_details.community_name
                                    }}</span
                                >
                                is made historical, it may be reactivated using
                                rename at a later date
                            </li>
                        </ul>
                    </alert>
                </div>
                <FormSection
                    v-if="rename_community"
                    :form-collapse="false"
                    label="Select Community to Rename To"
                    Index="community-taxonomy-information"
                >
                    <form class="form-horizontal" name="rename-community-form">
                        <div class="row mb-3 align-items-center">
                            <label
                                class="col-form-label col-sm-3 form-label fw-bold d-block"
                                >Rename to:</label
                            >
                            <div class="col-sm-9">
                                <div
                                    class="d-flex flex-wrap align-items-center gap-4"
                                >
                                    <div class="form-check form-check-inline">
                                        <input
                                            class="form-check-input"
                                            type="radio"
                                            name="renameOption"
                                            id="renameExisting2"
                                            value="existing"
                                            v-model="renameOption"
                                        />
                                        <label
                                            class="form-check-label"
                                            for="renameExisting2"
                                            >Existing Community</label
                                        >
                                    </div>
                                    <div class="form-check form-check-inline">
                                        <input
                                            class="form-check-input"
                                            type="radio"
                                            name="renameOption"
                                            id="renameNew2"
                                            value="new"
                                            v-model="renameOption"
                                        />
                                        <label
                                            class="form-check-label"
                                            for="renameNew2"
                                            >New Community</label
                                        >
                                    </div>
                                </div>
                            </div>
                        </div>
                        <alert v-if="errors" type="danger"
                            ><strong>{{ errors }}</strong></alert
                        >
                        <template v-if="renameOption === 'existing'">
                            <div class="row mb-3">
                                <label
                                    for="community_name_lookup_rename_community"
                                    class="col-sm-3 control-label fw-bold"
                                    >Community Name:
                                    <span class="text-danger">*</span>
                                </label>
                                <div class="col-sm-9">
                                    <div id="rename_community_select2_parent">
                                        <select
                                            id="community_name_lookup_rename_community"
                                            ref="community_name_lookup_rename_community"
                                            class="form-select"
                                        ></select>
                                    </div>
                                </div>
                            </div>
                            <div
                                v-if="renameCommunityFetched"
                                class="row mb-3 border-top pt-3"
                            >
                                <div class="col">
                                    <FormSection
                                        :form-collapse="false"
                                        label="Selected Existing Community"
                                        Index="selected-rename-taxonomy"
                                    >
                                        <div class="row mb-3">
                                            <label
                                                for="community_name"
                                                class="col-sm-3 control-label"
                                                >Community Name:
                                            </label>
                                            <div class="col-sm-9">
                                                <textarea
                                                    id="community_name"
                                                    v-model="
                                                        rename_community
                                                            .taxonomy_details
                                                            .community_name
                                                    "
                                                    :disabled="true"
                                                    class="form-control"
                                                    rows="1"
                                                    placeholder=""
                                                />
                                            </div>
                                        </div>
                                        <div class="row mb-3">
                                            <label
                                                for="community_common_id"
                                                class="col-sm-3 control-label"
                                                >Community ID:
                                            </label>
                                            <div class="col-sm-9">
                                                <input
                                                    id="community_common_id"
                                                    v-model="
                                                        rename_community
                                                            .taxonomy_details
                                                            .community_common_id
                                                    "
                                                    type="text"
                                                    class="form-control"
                                                    :disabled="true"
                                                    placeholder=""
                                                />
                                            </div>
                                        </div>
                                        <div class="row mb-3">
                                            <label
                                                class="col-sm-3 control-label"
                                                >Status:
                                            </label>
                                            <div class="col-sm-9">
                                                <span
                                                    class="badge"
                                                    :class="
                                                        renameCommunityBadgeClass
                                                    "
                                                >
                                                    {{
                                                        rename_community.processing_status
                                                    }}
                                                </span>
                                                <template
                                                    v-if="
                                                        [
                                                            'Active',
                                                            'Historical',
                                                        ].includes(
                                                            rename_community.processing_status
                                                        )
                                                    "
                                                >
                                                    -
                                                    <span
                                                        class="badge"
                                                        :class="
                                                            rename_community
                                                                .publishing_status
                                                                .public_status ===
                                                            'Public'
                                                                ? 'bg-success'
                                                                : 'bg-secondary'
                                                        "
                                                    >
                                                        {{
                                                            rename_community
                                                                .publishing_status
                                                                .public_status
                                                        }}
                                                    </span>
                                                </template>
                                            </div>
                                        </div>
                                    </FormSection>
                                </div>
                            </div>
                        </template>
                        <template v-if="renameOption === 'new'">
                            <div class="row border-bottom mb-4 pb-3">
                                <label
                                    for=""
                                    class="col-sm-3 control-label fw-bold"
                                ></label>
                                <div class="col-sm-6">
                                    <button
                                        class="btn btn-primary float-end"
                                        @click.prevent="resetForm"
                                    >
                                        Reset form
                                    </button>
                                </div>
                                <div class="col-sm-3">
                                    <button
                                        class="btn btn-primary w-100"
                                        @click.prevent="
                                            populateFromOriginalCommunity
                                        "
                                    >
                                        Populate all from
                                        {{
                                            species_community_original.community_number
                                        }}
                                    </button>
                                </div>
                            </div>
                            <div class="row mb-3">
                                <label
                                    for=""
                                    class="col-sm-3 control-label fw-bold"
                                    >Community Name:
                                    <span class="text-danger">*</span></label
                                >
                                <div class="col-sm-6">
                                    <textarea
                                        id="community_name"
                                        ref="community_name"
                                        v-model="
                                            rename_community.taxonomy_details
                                                .community_name
                                        "
                                        class="form-control"
                                        rows="1"
                                        placeholder=""
                                    />
                                </div>
                            </div>
                            <div class="row mb-3">
                                <label
                                    for=""
                                    class="col-sm-3 control-label fw-bold"
                                    >Community ID:
                                    <span class="text-danger">*</span></label
                                >
                                <div class="col-sm-6">
                                    <input
                                        id="community_common_id"
                                        ref="community_common_id"
                                        v-model="
                                            rename_community.taxonomy_details
                                                .community_common_id
                                        "
                                        type="text"
                                        class="form-control"
                                        placeholder=""
                                    />
                                </div>
                            </div>
                            <div class="row mb-3">
                                <label for="" class="col-sm-3 control-label"
                                    >Community Description:</label
                                >
                                <div class="col-sm-6">
                                    <textarea
                                        id="community_description"
                                        v-model="
                                            rename_community.taxonomy_details
                                                .community_description
                                        "
                                        class="form-control"
                                        rows="2"
                                        placeholder=""
                                    />
                                </div>
                                <div class="col-sm-3">
                                    <button
                                        v-if="
                                            species_community_original
                                                .taxonomy_details
                                                .community_description
                                        "
                                        class="btn btn-primary w-100"
                                        @click.prevent="
                                            rename_community.taxonomy_details.community_description =
                                                species_community_original.taxonomy_details.community_description
                                        "
                                    >
                                        Populate from
                                        {{
                                            species_community_original.community_number
                                        }}
                                    </button>
                                </div>
                            </div>
                            <div class="row mb-3">
                                <label for="" class="col-sm-3 control-label"
                                    >Previous Name:</label
                                >
                                <div class="col-sm-6">
                                    <textarea
                                        id="community_previous_name"
                                        v-model="
                                            rename_community.taxonomy_details
                                                .previous_name
                                        "
                                        class="form-control"
                                        placeholder=""
                                    />
                                </div>
                                <div class="col-sm-3">
                                    <button
                                        v-if="
                                            species_community_original
                                                .taxonomy_details.previous_name
                                        "
                                        class="btn btn-primary w-100"
                                        @click.prevent="
                                            rename_community.taxonomy_details.previous_name =
                                                species_community_original.taxonomy_details.previous_name
                                        "
                                    >
                                        Populate from
                                        {{
                                            species_community_original.community_number
                                        }}
                                    </button>
                                </div>
                            </div>
                            <div class="row mb-3">
                                <label for="" class="col-sm-3 control-label"
                                    >Name Authority:</label
                                >
                                <div class="col-sm-6">
                                    <textarea
                                        id="name_authority"
                                        v-model="
                                            rename_community.taxonomy_details
                                                .name_authority
                                        "
                                        rows="1"
                                        class="form-control"
                                        placeholder=""
                                    />
                                </div>
                                <div class="col-sm-3">
                                    <button
                                        v-if="
                                            species_community_original
                                                .taxonomy_details.name_authority
                                        "
                                        class="btn btn-primary w-100"
                                        @click.prevent="
                                            rename_community.taxonomy_details.name_authority =
                                                species_community_original.taxonomy_details.name_authority
                                        "
                                    >
                                        Populate from
                                        {{
                                            species_community_original.community_number
                                        }}
                                    </button>
                                </div>
                            </div>
                            <div class="row mb-3">
                                <label for="" class="col-sm-3 control-label"
                                    >Name Comments:</label
                                >
                                <div class="col-sm-6">
                                    <textarea
                                        id="community_comment"
                                        v-model="
                                            rename_community.taxonomy_details
                                                .name_comments
                                        "
                                        class="form-control"
                                        placeholder=""
                                    />
                                </div>
                                <div class="col-sm-3">
                                    <button
                                        v-if="
                                            species_community_original
                                                .taxonomy_details.name_comments
                                        "
                                        class="btn btn-primary w-100"
                                        @click.prevent="
                                            rename_community.taxonomy_details.name_comments =
                                                species_community_original.taxonomy_details.name_comments
                                        "
                                    >
                                        Populate from
                                        {{
                                            species_community_original.community_number
                                        }}
                                    </button>
                                </div>
                            </div>
                        </template>
                        <div class="row mb-3 align-items-center">
                            <label class="col-form-label col-sm-7 fw-bold"
                                >What status would you like the original to have
                                after the rename?:
                                <span class="text-danger">*</span></label
                            >
                            <div class="col-sm-5">
                                <select
                                    class="form-select"
                                    v-model="
                                        processingStatusForOriginalAfterRename
                                    "
                                >
                                    <option disabled :value="null">
                                        Select status...
                                    </option>
                                    <option value="historical">
                                        Make Historical
                                    </option>
                                    <option value="active">Leave Active</option>
                                </select>
                            </div>
                        </div>
                        <div class="row mb-2">
                            <div class="col-sm-3"></div>
                            <div class="col-sm-9">
                                <button
                                    class="btn btn-primary float-end mt-2"
                                    :disabled="
                                        !rename_community.taxonomy_details
                                            .community_name ||
                                        !rename_community.taxonomy_details
                                            .community_common_id ||
                                        !processingStatusForOriginalAfterRename
                                    "
                                    @click.prevent="finaliseRenameCommunity"
                                >
                                    <i class="bi bi-check2-circle"></i>
                                    Finalise Rename Community<template
                                        v-if="finaliseCommunityLoading"
                                    >
                                        <span
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
                    </form>
                </FormSection>
                <FormSection
                    :form-collapse="false"
                    :label="`Original Community - ${species_community_original.community_number} - ${species_community_original.taxonomy_details.community_name}`"
                    Index="original-community"
                >
                    <div>
                        <div class="col-md-12">
                            <FormSpeciesCommunities
                                v-if="species_community_original"
                                id="rename_community"
                                ref="rename_community"
                                :species_community_original="
                                    species_community_original
                                "
                                :species_community="species_community_original"
                                :is_internal="true"
                                :is_readonly="true"
                                :rename_species="true"
                            >
                                // rename=true used to make only taxon select
                                editable on form
                            </FormSpeciesCommunities>
                        </div>
                    </div>
                </FormSection>
            </div>
            <template #footer>
                <div>
                    <button
                        type="button"
                        class="btn btn-secondary"
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
import modal from '@vue-utils/bootstrap-modal.vue';
import alert from '@vue-utils/alert.vue';
import FormSpeciesCommunities from '@/components/form_species_communities.vue';
import FormSection from '@/components/forms/section_toggle.vue';
import { api_endpoints } from '@/utils/hooks.js';
import { toRaw } from 'vue';

export default {
    name: 'CommunityRename',
    components: {
        modal,
        alert,
        FormSection,
        FormSpeciesCommunities,
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
            rename_community: null,
            isModalOpen: false,
            renameOption: 'existing',
            community_display: '',
            taxon_previous_name: '',
            finaliseCommunityLoading: false,
            select2Initialised: false,
            renameCommunityFetched: false,
            processingStatusForOriginalAfterRename: null,
            errors: null,
            originalRenameCommunity: null,
        };
    },
    computed: {
        original_community_display: function () {
            return `${this.species_community_original.community_number} - ${this.species_community_original.taxonomy_details.community_name}`;
        },
        title: function () {
            return `Rename Community ${this.original_community_display}`;
        },
        renameCommunityBadgeClass: function () {
            if (!this.rename_community) return {};
            return {
                'bg-primary':
                    this.rename_community.processing_status === 'Active',
                'bg-secondary':
                    this.rename_community.processing_status === 'Draft',
                'bg-dark':
                    this.rename_community.processing_status === 'Historical',
            };
        },
    },
    watch: {
        isModalOpen(val) {
            if (val) {
                this.$nextTick(() => {
                    this.rename_community = structuredClone(
                        toRaw(this.species_community_original)
                    );
                    this.rename_community.id = null;
                    this.rename_community.community_number = '';
                    this.rename_community.taxonomy_details.community_id = null;
                    this.rename_community.taxonomy_details.community_name =
                        null;
                    this.rename_community.taxonomy_details.community_common_id =
                        null;
                    this.rename_community.taxonomy_details.previous_name =
                        this.species_community_original.taxonomy_details.community_name;
                    this.$nextTick(() => {
                        if (this.renameOption === 'existing') {
                            this.select2Initialised = false;
                            this.initialiseCommunityNameLookup(() => {
                                this.openAndFocusSelect2();
                            });
                        }
                        this.originalRenameCommunity = JSON.parse(
                            JSON.stringify(this.rename_community)
                        );
                    });
                });
            } else {
                this.destroySelect2();
            }
        },
        renameOption(val, oldVal) {
            if (oldVal === 'existing' && val !== 'existing') {
                this.destroySelect2();
            }
            if (val === 'existing') {
                this.$nextTick(() => {
                    this.select2Initialised = false;
                    this.initialiseCommunityNameLookup(() => {
                        this.openAndFocusSelect2();
                    });
                });
            } else {
                this.renameCommunityFetched = false;
                this.$nextTick(() => {
                    this.resetForm();
                    this.$refs.community_name &&
                        this.$refs.community_name.focus();
                });
            }
        },
    },
    beforeUnmount() {
        this.destroySelect2();
    },
    methods: {
        hasUnsavedChanges: function () {
            return (
                JSON.stringify(this.rename_community) !==
                JSON.stringify(this.originalRenameCommunity)
            );
        },
        initialiseCommunityNameLookup(callback) {
            const el = this.$refs.community_name_lookup_rename_community;
            if (!el) return;
            if (this.select2Initialised) {
                if (callback) this.$nextTick(callback);
                return;
            }
            const vm = this;
            $(el)
                .select2({
                    minimumInputLength: 2,
                    dropdownParent: $('#rename_community_select2_parent'),
                    theme: 'bootstrap-5',
                    allowClear: true,
                    placeholder: 'Select Community Name',
                    ajax: {
                        url: api_endpoints.community_name_lookup,
                        dataType: 'json',
                        data(params) {
                            return {
                                term: params.term,
                                type: 'public',
                                group_type_id:
                                    vm.species_community_original.group_type_id,
                            };
                        },
                    },
                })
                .on('select2:select', function (e) {
                    vm.fetchCommunity(e.params.data.id);
                })
                .on('select2:unselect', function () {
                    vm.resetForm();
                    vm.renameCommunityFetched = false;
                })
                .on('select2:open', function () {
                    const searchField = document.querySelector(
                        '[aria-controls="select2-community_name_lookup_rename_community-results"]'
                    );
                    searchField && searchField.focus();
                });
            this.select2Initialised = true;
            this.$nextTick(() => callback && callback());
        },
        openAndFocusSelect2() {
            const el = this.$refs.community_name_lookup_rename_community;
            if (!el) return;
            // Ensure open after any reflow
            this.$nextTick(() => {
                try {
                    $(el).select2('open');
                } catch {
                    /* ignore */
                }
            });
        },
        destroySelect2() {
            const el = this.$refs.community_name_lookup_rename_community;
            if (el && this.select2Initialised) {
                try {
                    $(el).off().select2('destroy');
                } catch {
                    /* ignore */
                }
            }
            this.select2Initialised = false;
        },
        fetchCommunity: function (community_id) {
            let vm = this;
            fetch(api_endpoints.community + '/' + community_id)
                .then(async (response) => {
                    const data = await response.json();
                    if (!response.ok) {
                        swal.fire({
                            title: 'Error fetching community',
                            text: data,
                            icon: 'error',
                            customClass: {
                                confirmButton: 'btn btn-primary',
                            },
                        });
                        return;
                    }
                    vm.rename_community.id = data.id;
                    vm.rename_community.taxonomy_id = data.taxonomy_id;
                    vm.rename_community.processing_status =
                        data.processing_status;
                    vm.rename_community.publishing_status.public_status =
                        data.publishing_status.public_status;
                    vm.rename_community.taxonomy_details =
                        data.taxonomy_details;
                    vm.renameCommunityFetched = true;
                })
                .catch(async (response) => {
                    this.errors = await response.json();
                });
        },
        resetForm: function () {
            this.rename_community.id = null;
            this.rename_community.taxonomy_id = null;
            this.rename_community.taxonomy_details = {
                community_name: '',
                community_common_id: '',
                community_description: '',
                previous_name:
                    this.species_community_original.taxonomy_details
                        .community_name,
                name_authority: '',
                name_comments: '',
            };
            if (this.$refs.community_name) {
                this.$refs.community_name.focus();
            }
        },
        populateFromOriginalCommunity: function () {
            this.rename_community.taxonomy_details.community_name =
                this.species_community_original.taxonomy_details.community_name;
            this.rename_community.taxonomy_details.community_common_id =
                this.species_community_original.taxonomy_details.community_common_id;
            this.rename_community.taxonomy_details.community_description =
                this.species_community_original.taxonomy_details.community_description;
            this.rename_community.taxonomy_details.previous_name =
                this.species_community_original.taxonomy_details.previous_name;
            this.rename_community.taxonomy_details.name_authority =
                this.species_community_original.taxonomy_details.name_authority;
            this.rename_community.taxonomy_details.name_comments =
                this.species_community_original.taxonomy_details.name_comments;
            this.$refs.community_name.focus();
        },
        cancel: function () {
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
        },
        finaliseRenameCommunity: function () {
            let vm = this;
            if (
                this.rename_community.taxonomy_details.community_name ===
                this.species_community_original.taxonomy_details.community_name
            ) {
                swal.fire({
                    title: `Community name must be different from the original community`,
                    icon: 'error',
                    customClass: {
                        confirmButton: 'btn btn-primary',
                    },
                    didClose: () => {
                        vm.$refs.community_name.focus();
                    },
                });
                return;
            }
            if (
                this.rename_community.taxonomy_details.community_common_id ===
                this.species_community_original.taxonomy_details
                    .community_common_id
            ) {
                swal.fire({
                    title: `Community ID must be different from the original community`,
                    icon: 'error',
                    customClass: {
                        confirmButton: 'btn btn-primary',
                    },
                    didClose: () => {
                        vm.$refs.community_common_id.focus();
                    },
                });
                return;
            }
            let html = `<p>Are you sure you want to rename community '${this.species_community_original.taxonomy_details.community_name}' to '${this.rename_community.taxonomy_details.community_name}'?</p>`;
            if (vm.rename_community.id) {
                html += `<p>Taxonomy details for the existing community will be overwritten from the original community.</p>`;
            }

            if (vm.processingStatusForOriginalAfterRename == 'historical') {
                html += `<p>The original community will be made historical (and if an approved conservation status exists, it will be closed).</p>`;
            } else {
                html += `<p>The original community will remain active (and if an approved conservation status exists, it will remain approved).</p>`;
            }

            html += `<p>Any and all occurrences from the original community will be moved to the resulting community.</p>`;
            swal.fire({
                title: `Rename Community`,
                html: html,
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Rename Community',
                reverseButtons: true,
                customClass: {
                    confirmButton: 'btn btn-primary',
                    cancelButton: 'btn btn-secondary',
                },
            })
                .then((swalresult) => {
                    vm.finaliseCommunityLoading = true;
                    vm.rename_community.processing_status_for_original_after_rename =
                        vm.processingStatusForOriginalAfterRename;
                    if (swalresult.isConfirmed) {
                        fetch(
                            api_endpoints.rename_community(
                                vm.species_community_original.id
                            ),
                            {
                                method: 'POST',
                                headers: {
                                    'Content-Type': 'application/json',
                                },
                                body: JSON.stringify(vm.rename_community),
                            }
                        ).then(async (response) => {
                            const data = await response.json();
                            if (!response.ok) {
                                swal.fire({
                                    title: 'Error renaming community',
                                    text: JSON.stringify(data),
                                    icon: 'error',
                                    customClass: {
                                        confirmButton: 'btn btn-primary',
                                    },
                                });
                                return;
                            }
                            vm.$router.push({
                                name: 'internal-species-communities',
                                params: {
                                    species_community_id: data.id,
                                },
                                query: {
                                    group_type_name: data.group_type,
                                },
                            });
                            vm.$router.go();
                            vm.isModalOpen = false;
                        });
                    }
                })
                .finally(() => {
                    vm.finaliseCommunityLoading = false;
                });
        },
    },
};
</script>
