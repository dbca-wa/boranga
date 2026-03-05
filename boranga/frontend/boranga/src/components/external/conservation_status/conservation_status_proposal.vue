<template lang="html">
    <div class="container">
        <form
            :action="cs_proposal_form_url"
            method="post"
            name="new_cs_proposal"
            enctype="multipart/form-data"
        >
            <div v-if="!cs_proposal_readonly">
                <div v-if="hasAmendmentRequest" class="row">
                    <div class="col-lg-12 pull-right">
                        <FormSection
                            :form-collapse="false"
                            label="One or more amendments have been requested for this Conservation Status Proposal"
                            Index="amendment_request"
                            custom-color="red"
                        >
                            <ul class="list-group list-group-numbered ps-2">
                                <li
                                    v-for="a in amendment_request"
                                    :key="a.id"
                                    class="list-group-item d-flex justify-content-between align-items-start"
                                >
                                    <div class="ms-4 me-auto">
                                        <div class="mb-3">
                                            <span class="fw-bold">Reason:</span>
                                            {{ a.reason_text }}
                                        </div>
                                        <p
                                            v-for="t in splitText(a.text)"
                                            :key="t"
                                        >
                                            {{ t }}
                                        </p>
                                        <template
                                            v-if="
                                                a.cs_amendment_request_documents &&
                                                a.cs_amendment_request_documents
                                                    .length > 0
                                            "
                                        >
                                            <div class="fw-bold mb-1">
                                                Documents:
                                            </div>
                                            <ul
                                                class="list-group list-group-numbered mb-2"
                                            >
                                                <li
                                                    v-for="document in a.cs_amendment_request_documents"
                                                    :key="document.id"
                                                    class="list-group-item"
                                                >
                                                    <i
                                                        class="bi bi-file-earmark-text-fill text-secondary"
                                                    ></i>
                                                    <a
                                                        :href="document._file"
                                                        target="_blank"
                                                        >{{ document.name }}</a
                                                    >
                                                </li>
                                            </ul>
                                        </template>
                                    </div>
                                </li>
                            </ul>
                        </FormSection>
                    </div>
                </div>
            </div>
            <div
                v-if="conservation_status_obj"
                id="scrollspy-heading"
                class="col-lg-12 mb-3"
            >
                <h4>
                    Conservation List - {{ display_group_type }} Application:
                    {{ conservation_status_obj.conservation_status_number }}
                </h4>
            </div>
            <ProposalConservationStatus
                v-if="conservation_status_obj"
                id="ConservationStatusStart"
                ref="conservation_status"
                :conservation_status_obj="conservation_status_obj"
                :can-edit-status="canEditStatus"
                :is_external="true"
                @save-conservation-status="save_wo_confirm"
            >
            </ProposalConservationStatus>
            <div>
                <input
                    type="hidden"
                    name="csrfmiddlewaretoken"
                    :value="csrf_token"
                />
                <input type="hidden" name="conservation_status_id" :value="1" />

                <div class="row" style="margin-bottom: 50px">
                    <div class="container">
                        <div class="row" style="margin-bottom: 50px">
                            <div
                                class="navbar fixed-bottom"
                                style="background-color: #f5f5f5"
                            >
                                <div
                                    v-if="
                                        conservation_status_obj &&
                                        !conservation_status_obj.readonly
                                    "
                                    class="container"
                                >
                                    <div
                                        class="col-md-6"
                                        style="margin-top: 5px"
                                    >
                                        <p
                                            class="pull-right"
                                            style="margin-top: 5px"
                                        >
                                            <router-link
                                                class="btn btn-primary"
                                                :to="{
                                                    name: 'external-conservation-status-dash',
                                                }"
                                                >Back to Dashboard</router-link
                                            >
                                        </p>
                                    </div>
                                    <div
                                        class="col-md-6 text-end"
                                        style="margin-top: 5px"
                                    >
                                        <button
                                            v-if="savingCSProposal"
                                            type="button"
                                            class="btn btn-primary me-2"
                                            disabled
                                        >
                                            Save and Continue
                                            <span
                                                class="spinner-border spinner-border-sm"
                                                role="status"
                                                aria-hidden="true"
                                            ></span>
                                            <span class="visually-hidden"
                                                >Loading...</span
                                            >
                                        </button>
                                        <input
                                            v-else
                                            type="button"
                                            class="btn btn-primary me-2"
                                            value="Save and Continue"
                                            :disabled="
                                                saveExitCSProposal ||
                                                paySubmitting
                                            "
                                            @click.prevent="save"
                                        />

                                        <button
                                            v-if="saveExitCSProposal"
                                            type="button"
                                            class="btn btn-primary me-2"
                                            disabled
                                        >
                                            Save and Exit
                                            <span
                                                class="spinner-border spinner-border-sm"
                                                role="status"
                                                aria-hidden="true"
                                            ></span>
                                            <span class="visually-hidden"
                                                >Loading...</span
                                            >
                                        </button>
                                        <input
                                            v-else
                                            type="button"
                                            class="btn btn-primary me-2"
                                            value="Save and Exit"
                                            :disabled="
                                                savingCSProposal ||
                                                paySubmitting
                                            "
                                            @click.prevent="save_exit"
                                        />

                                        <button
                                            v-if="paySubmitting"
                                            type="button"
                                            class="btn btn-primary"
                                            disabled
                                        >
                                            Submit
                                            <span
                                                class="spinner-border spinner-border-sm"
                                                role="status"
                                                aria-hidden="true"
                                            ></span>
                                            <span class="visually-hidden"
                                                >Loading...</span
                                            >
                                        </button>
                                        <input
                                            v-else
                                            type="button"
                                            class="btn btn-primary"
                                            :value="'Submit'"
                                            :disabled="
                                                saveExitCSProposal ||
                                                savingCSProposal
                                            "
                                            @click.prevent="submit"
                                        />
                                        <input
                                            id="save_and_continue_btn"
                                            type="hidden"
                                            class="btn btn-primary"
                                            value="Save Without Confirmation"
                                            @click.prevent="save_wo_confirm"
                                        />
                                    </div>
                                </div>
                                <div v-else class="container">
                                    <p
                                        class="pull-right"
                                        style="margin-top: 5px"
                                    >
                                        <router-link
                                            class="btn btn-primary"
                                            :to="{
                                                name: 'external-conservation-status-dash',
                                            }"
                                            >Back to Dashboard</router-link
                                        >
                                    </p>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </form>
    </div>
</template>
<script>
import ProposalConservationStatus from '@/components/form_conservation_status.vue';
import FormSection from '@/components/forms/section_toggle.vue';
import { api_endpoints, helpers } from '@/utils/hooks';
export default {
    name: 'ExternalConservationStatusProposal',
    components: {
        ProposalConservationStatus,
        FormSection,
    },
    beforeRouteEnter: function (to, from, next) {
        if (to.params.conservation_status_id) {
            fetch(
                `/api/conservation_status/${to.params.conservation_status_id}.json`
            ).then(
                async (response) => {
                    next(async (vm) => {
                        const data = await response.json();
                        vm.conservation_status_obj = data;
                        vm.setdata(vm.conservation_status_obj.readonly);
                        fetch(
                            helpers.add_endpoint_json(
                                api_endpoints.conservation_status,
                                to.params.conservation_status_id +
                                    '/amendment_request'
                            )
                        ).then(
                            async (response) => {
                                const data = await response.json();
                                vm.setAmendmentData(data);
                            },
                            (err) => {
                                console.log(err);
                            }
                        );
                    });
                },
                (err) => {
                    console.log(err);
                }
            );
        } else {
            fetch('/api/conservation_status.json', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
            }).then(
                async (response) => {
                    next(async (vm) => {
                        vm.conservation_status_obj = await response.json();
                    });
                },
                (err) => {
                    console.log(err);
                }
            );
        }
    },
    data: function () {
        return {
            proposal: null,
            conservation_status_obj: null,
            form: null,
            amendment_request: [],
            saveError: false,
            proposal_readonly: true,
            cs_proposal_readonly: true,
            hasAmendmentRequest: false,
            submitting: false,
            saveExitCSProposal: false,
            savingCSProposal: false,
            paySubmitting: false,
            newText: '',
            missing_fields: [],
            proposal_parks: null,
            isSaved: false,
        };
    },
    computed: {
        csrf_token: function () {
            return helpers.getCookie('csrftoken');
        },
        cs_proposal_form_url: function () {
            return this.conservation_status_obj
                ? `/api/conservation_status/${this.conservation_status_obj.id}/draft.json`
                : '';
        },
        canEditStatus: function () {
            return this.conservation_status_obj
                ? this.conservation_status_obj.can_user_edit
                : 'false';
        },
        display_group_type: function () {
            let group_type_string = this.conservation_status_obj.group_type;
            // to Capitalize only first character
            return (
                group_type_string.charAt(0).toUpperCase() +
                group_type_string.slice(1)
            );
        },
    },
    created: function () {
        if (!this.conservation_status_obj) {
            fetch(
                `/api/conservation_status/${this.$route.params.conservation_status_id}.json`
            ).then(
                async (response) => {
                    this.conservation_status_obj = await response.json();
                },
                (err) => {
                    console.log(err);
                }
            );
        }
    },
    mounted: function () {
        let vm = this;
        vm.form = document.forms.new_cs_proposal;
        window.addEventListener('beforeunload', vm.leaving);
    },
    beforeUnmount: function () {
        window.removeEventListener('beforeunload', this.leaving);
    },
    methods: {
        set_formData: function () {
            let vm = this;
            let formData = new FormData(vm.form);
            return formData;
        },
        save: async function () {
            let vm = this;
            var missing_data = vm.can_submit('');
            if (missing_data != true) {
                swal.fire({
                    title: 'Please fix following errors before saving',
                    text: missing_data,
                    icon: 'error',
                    customClass: {
                        confirmButton: 'btn btn-primary',
                    },
                });
                return false;
            }
            vm.savingCSProposal = true;
            vm.isSaved = false;
            let payload = new Object();
            Object.assign(payload, vm.conservation_status_obj);
            await fetch(vm.cs_proposal_form_url, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(payload),
            }).then(
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
                    let swalHtml =
                        '<p>Your conservation status proposal has been saved as a draft.</p>';
                    if (vm.saveExitCSProposal) {
                        swalHtml +=
                            '<p>It has <span class="fw-bold">NOT</span> been submitted.</p>';
                        swalHtml +=
                            '<p>You can find the draft on the conservation status dashboard to continue working on the proposal later.</p>';
                    }
                    swal.fire({
                        title: 'Conservation Status Proposal Saved',
                        html: swalHtml,
                        icon: 'success',
                        customClass: {
                            confirmButton: 'btn btn-primary',
                        },
                    });
                    vm.savingCSProposal = false;
                    vm.isSaved = true;
                },
                (err) => {
                    var errorText = helpers.apiVueResourceError(err);
                    swal.fire({
                        title: 'Save Error',
                        text: errorText,
                        icon: 'error',
                        customClass: {
                            confirmButton: 'btn btn-primary',
                        },
                    });
                    vm.savingCSProposal = false;
                    vm.isSaved = false;
                }
            );
        },
        save_exit: async function (e) {
            let vm = this;
            var missing_data = vm.can_submit('');
            if (missing_data != true) {
                swal.fire({
                    title: 'Please fix following errors before saving',
                    text: missing_data,
                    icon: 'error',
                    customClass: {
                        confirmButton: 'btn btn-primary',
                    },
                });
                return false;
            }
            this.submitting = true;
            this.saveExitCSProposal = true;
            await this.save(e).then(() => {
                if (vm.isSaved === true) {
                    // redirect back to dashboard
                    vm.$router.push({
                        name: 'external-conservation-status-dash',
                    });
                } else {
                    this.saveExitCSProposal = false;
                }
            });
        },
        save_wo_confirm: function () {
            const vm = this;
            fetch(this.cs_proposal_form_url, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(this.conservation_status_obj),
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
                    vm.conservation_status_obj = data;
                },
                (err) => {
                    console.log(err);
                }
            );
        },
        save_before_submit: async function () {
            let vm = this;
            vm.saveError = false;
            await fetch(vm.cs_proposal_form_url, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(vm.conservation_status_obj),
            }).then(
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
                },
                (err) => {
                    vm.saveError = true;
                    swal.fire({
                        title: 'Save Error',
                        text: helpers.apiVueResourceError(err),
                        icon: 'error',
                        customClass: {
                            confirmButton: 'btn btn-primary',
                        },
                    });
                }
            );
        },
        setdata: function (readonly) {
            this.cs_proposal_readonly = readonly;
        },
        setAmendmentData: function (amendment_request) {
            this.amendment_request = amendment_request;

            if (amendment_request.length > 0) this.hasAmendmentRequest = true;
        },
        splitText: function (aText) {
            let newText;
            newText = aText.split('\n');
            return newText;
        },
        leaving: function (e) {
            let vm = this;
            var dialogText = 'You have some unsaved changes.';
            if (!vm.cs_proposal_readonly && !vm.submitting) {
                e.returnValue = dialogText;
                return dialogText;
            } else {
                return null;
            }
        },
        highlight_missing_fields: function () {
            let vm = this;
            for (var missing_field of vm.missing_fields) {
                $('#' + missing_field.id).css('color', 'red');
            }
        },
        validateConservationStatusListsCategories: function (blank_fields) {
            let required_fields = [
                {
                    id: 'wa_legislative_list_id',
                    display: 'WA Legislative List',
                },
                { id: 'wa_priority_list_id', display: 'WA Priority List' },
            ];
            for (let field of required_fields) {
                if (
                    this.conservation_status_obj[field.id] != null &&
                    this.conservation_status_obj[field.id] != ''
                ) {
                    return blank_fields;
                }
            }
            blank_fields.push(
                `At least one of the following fields are required: ${required_fields.map((f) => f.display).join(', ')}`
            );
            return blank_fields;
        },
        can_submit: function (check_action) {
            let vm = this;
            let blank_fields;
            blank_fields = vm.can_submit_conservation_status(check_action);

            if (blank_fields.length == 0) {
                return true;
            } else {
                return blank_fields;
            }
        },
        can_submit_conservation_status: function (check_action) {
            let vm = this;
            let blank_fields = [];
            if (
                vm.conservation_status_obj.group_type == 'flora' ||
                vm.conservation_status_obj.group_type == 'fauna'
            ) {
                if (
                    vm.conservation_status_obj.species_taxonomy_id == null ||
                    vm.conservation_status_obj.species_taxonomy_id == ''
                ) {
                    blank_fields.push(' Scientific Name is required');
                }
            } else {
                if (
                    vm.conservation_status_obj.community_id == null ||
                    vm.conservation_status_obj.community_id == ''
                ) {
                    blank_fields.push(' Community Name is required');
                }
            }
            if (
                !vm.conservation_status_obj.submitter_information
                    .submitter_category
            ) {
                blank_fields.push(' Please select a submitter category');
            }
            if (
                check_action == 'submit' &&
                (vm.conservation_status_obj.species_taxonomy_id ||
                    vm.conservation_status_obj.community_id)
            ) {
                vm.validateConservationStatusListsCategories(blank_fields);
                if (
                    vm.conservation_status_obj.comment == null ||
                    vm.conservation_status_obj.comment == ''
                ) {
                    blank_fields.push(
                        ' Please enter some comments regarding your conservation status proposal.'
                    );
                }
            }

            return blank_fields;
        },
        submit: function () {
            let vm = this;

            var missing_data = vm.can_submit('submit');
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

            // remove the confirm prompt when navigating away from window (on button 'Submit' click)
            vm.submitting = true;
            vm.paySubmitting = true;

            swal.fire({
                title: 'Submit Proposal',
                text: 'Are you sure you want to submit this conservation status proposal?',
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Submit Proposal',
                customClass: {
                    confirmButton: 'btn btn-primary',
                    cancelButton: 'btn btn-secondary',
                },
                reverseButtons: true,
            }).then(
                async (result) => {
                    if (result.isConfirmed) {
                        await vm.save_before_submit();
                        if (!vm.saveError) {
                            fetch(
                                helpers.add_endpoint_json(
                                    api_endpoints.conservation_status,
                                    vm.conservation_status_obj.id + '/submit'
                                ),
                                {
                                    method: 'POST',
                                    headers: {
                                        'Content-Type': 'application/json',
                                    },
                                    body: JSON.stringify(
                                        vm.conservation_status_obj
                                    ),
                                }
                            ).then(
                                async (response) => {
                                    const data = await response.json();
                                    if (!response.ok) {
                                        swal.fire({
                                            title: 'Submission Error',
                                            text: data,
                                            icon: 'error',
                                            customClass: {
                                                confirmButton:
                                                    'btn btn-primary',
                                            },
                                        });
                                        return;
                                    }
                                    vm.conservation_status_obj = data;
                                    vm.$router.push({
                                        name: 'submit_cs_proposal',
                                        state: {
                                            conservation_status_obj:
                                                JSON.stringify(
                                                    vm.conservation_status_obj
                                                ),
                                        },
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
                                }
                            );
                        }
                    } else {
                        vm.submitting = false;
                        vm.paySubmitting = false;
                    }
                },
                () => {
                    vm.submitting = true;
                    vm.paySubmitting = false;
                }
            );
        },
    },
};
</script>
