<template>
    <div id="submitter-information">
        <FormSection
            :form-collapse="false"
            label="Submitter Information"
            Index="submitter_information"
        >
            <form
                id="submitter-information-form"
                class="needs-validation"
                novalidate
            >
                <fieldset :disabled="disabled">
                    <div class="row mb-3">
                        <label
                            for="submitter_category"
                            class="col-sm-2 col-form-label fw-bold"
                            >Category <span class="text-danger">*</span></label
                        >
                        <div class="col-sm-6">
                            <template v-if="!disabled">
                                <template
                                    v-if="
                                        submitter_categories &&
                                        submitter_categories.length > 0 &&
                                        submitter_information.submitter_category &&
                                        !submitter_categories
                                            .map((d) => d.id)
                                            .includes(
                                                submitter_information.submitter_category
                                            )
                                    "
                                >
                                    <input
                                        v-if="
                                            submitter_information.submitter_category_name
                                        "
                                        type="text"
                                        class="form-control mb-3"
                                        :value="
                                            submitter_information.submitter_category_name +
                                            ' (Now Archived)'
                                        "
                                        disabled
                                    />
                                    <div class="mb-3 text-muted">
                                        Change submitter category to:
                                    </div>
                                </template>
                                <select
                                    id="submitter_category"
                                    ref="submitter_category"
                                    v-model="
                                        submitter_information.submitter_category
                                    "
                                    class="form-select"
                                >
                                    <option :value="null" disabled>
                                        Please select a submitter category
                                    </option>
                                    <option
                                        v-for="submitter_category in submitter_categories"
                                        :key="submitter_category.id"
                                        :value="submitter_category.id"
                                    >
                                        {{ submitter_category.name }}
                                    </option>
                                </select>
                            </template>
                            <template v-else>
                                <input
                                    v-model="
                                        submitter_information.submitter_category_name
                                    "
                                    class="form-control"
                                    type="text"
                                    :disabled="disabled"
                                />
                            </template>
                        </div>
                    </div>
                    <div class="row mb-3">
                        <label for="name" class="col-sm-2 col-form-label"
                            >Name</label
                        >
                        <div class="col-sm-6">
                            <input
                                id="name"
                                v-model="submitter_information.name"
                                type="text"
                                maxlength="100"
                                class="form-control"
                            />
                        </div>
                    </div>
                    <div
                        v-if="
                            show_submitter_contact_details &&
                            Object.hasOwn(
                                submitter_information,
                                'contact_details'
                            )
                        "
                        class="row mb-3 pb-3"
                        :class="
                            show_organisation_section ? 'border-bottom' : ''
                        "
                    >
                        <label
                            for="contact_details"
                            class="col-sm-2 col-form-label"
                            >Contact Details</label
                        >
                        <div class="col-sm-6">
                            <textarea
                                id="contact_details"
                                v-model="submitter_information.contact_details"
                                class="form-control"
                            />
                        </div>
                    </div>
                    <div
                        v-if="show_organisation_section"
                        class="row"
                        :class="show_organisation_information ? 'mb-3' : ''"
                    >
                        <label for="" class="col-sm-4 col-form-label"
                            >Are you submitting on behalf of an
                            organisation?</label
                        >
                        <div class="col-sm-6 d-flex align-items-center">
                            <div
                                class="form-check form-check-inline"
                                @click="checkOrganisationDetails"
                            >
                                <input
                                    id="organisation_no"
                                    v-model="
                                        submitting_on_behalf_of_organisation
                                    "
                                    type="radio"
                                    class="form-check-input"
                                    value="no"
                                    :disabled="organisation_details_entered"
                                />
                                <label for="name" class="form-check-label"
                                    >No</label
                                >
                            </div>
                            <div class="form-check form-check-inline">
                                <input
                                    id="organisation_yes"
                                    v-model="
                                        submitting_on_behalf_of_organisation
                                    "
                                    type="radio"
                                    class="form-check-input"
                                    value="yes"
                                    @change.prevent="focusOrganisation"
                                />
                                <label for="name" class="form-check-label"
                                    >Yes</label
                                >
                            </div>
                        </div>
                    </div>
                    <template v-if="show_organisation_information">
                        <div class="row mb-3">
                            <label
                                for="organisation"
                                class="col-sm-2 col-form-label"
                                >Organisation</label
                            >
                            <div class="col-sm-6">
                                <input
                                    id="organisation"
                                    ref="organisation"
                                    v-model="submitter_information.organisation"
                                    type="text"
                                    maxlength="100"
                                    class="form-control"
                                />
                            </div>
                        </div>
                        <div
                            v-if="show_organisation_information"
                            class="row mb-3"
                        >
                            <label
                                for="position"
                                class="col-sm-2 col-form-label"
                                >Position / Role</label
                            >
                            <div class="col-sm-6">
                                <input
                                    id="position"
                                    v-model="submitter_information.position"
                                    type="text"
                                    maxlength="100"
                                    class="form-control"
                                />
                            </div>
                        </div>
                    </template>
                </fieldset>
            </form>
        </FormSection>
    </div>
</template>

<script>
import FormSection from '@/components/forms/section_toggle.vue';

import { api_endpoints } from '@/utils/hooks.js';

export default {
    name: 'SubmitterInformation',
    components: {
        FormSection,
    },
    props: {
        submitter_information: {
            type: Object,
            default: null,
        },
        disabled: {
            type: Boolean,
            default: false,
        },
        show_submitter_contact_details: {
            type: Boolean,
            default: true,
        },
        show_organisation_section: {
            type: Boolean,
            default: true,
        },
    },
    data: function () {
        return {
            submitter_categories: null,
            submitting_on_behalf_of_organisation: 'no',
        };
    },
    computed: {
        organisation_details_entered: function () {
            return !!(
                this.submitter_information.organisation ||
                this.submitter_information.position
            );
        },
        show_organisation_information: function () {
            return (
                this.submitting_on_behalf_of_organisation == 'yes' ||
                this.organisation_details_entered
            );
        },
    },
    created() {
        this.fetchSubmitterCategories();
        if (this.show_organisation_information) {
            this.submitting_on_behalf_of_organisation = 'yes';
        }
    },
    mounted() {
        let vm = this;
        var form = document.getElementById('submitter-information-form');
        form.addEventListener(
            'change',
            function (event) {
                if (!form.checkValidity()) {
                    event.preventDefault();
                    event.stopPropagation();
                    form.classList.add('was-validated');
                } else {
                    vm.saveSubmitterInformation();
                }
            },
            false
        );
    },
    methods: {
        focusOrganisation() {
            this.$nextTick(() => {
                this.$refs.organisation.focus();
            });
        },
        fetchSubmitterCategories: function () {
            fetch(api_endpoints.submitter_categories)
                .then(async (response) => {
                    this.submitter_categories = await response.json();
                })
                .catch((error) => {
                    console.log(error);
                });
        },
        saveSubmitterInformation: function () {
            fetch(api_endpoints.save_submitter_information, {
                method: 'PUT',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(this.submitter_information),
            })
                .then(async (response) => {
                    if (!response.ok) {
                        const data = await response.json();
                        throw new Error(
                            'Failed to save submitter information: ' + data
                        );
                    }
                    return await response.json();
                })
                .catch((error) => {
                    console.log(error);
                });
        },
        checkOrganisationDetails: function () {
            if (this.organisation_details_entered) {
                swal.fire({
                    title: 'Remove Organisation Details',
                    text: "To change the answer to 'No' please remove the organisation details first.",
                    icon: 'info',
                    customClass: {
                        confirmButton: 'btn btn-primary',
                    },
                });
            }
        },
    },
};
</script>

<style scoped></style>
