<template lang="html">
    <div id="proposedIssuanceApproval">
        <modal
            transition="modal fade"
            :title="title"
            large
            @ok="ok()"
            @cancel="cancel()"
        >
            <div class="container-fluid">
                <div class="row">
                    <form class="form-horizontal" name="approvalForm">
                        <alert v-if="showError" type="danger"
                            ><ErrorRenderer :errors="errorString"
                        /></alert>
                        <alert v-if="!isEffectiveDateValid" type="danger"
                            ><strong
                                >Please select Effective To Date that is after
                                Effective From Date</strong
                            ></alert
                        >
                        <div class="col-sm-12">
                            <div class="form-group">
                                <div class="row mb-3">
                                    <div class="col-sm-4">
                                        <label
                                            class="control-label fw-bold"
                                            for="Name"
                                            >Effective From Date
                                            <span class="text-danger"
                                                >*</span
                                            ></label
                                        >
                                    </div>
                                    <div class="col-sm-8">
                                        <div
                                            class="input-group date"
                                            style="width: 70%"
                                        >
                                            <input
                                                ref="start_date"
                                                v-model="
                                                    approval.effective_from_date
                                                "
                                                type="date"
                                                class="form-control"
                                                :max="
                                                    new Date()
                                                        .toISOString()
                                                        .slice(0, 10)
                                                "
                                            />
                                        </div>
                                        <small
                                            v-show="showstartDateError"
                                            style="color: red"
                                            >This field is required</small
                                        >
                                    </div>
                                </div>
                            </div>
                            <div class="form-group">
                                <div class="row mb-3">
                                    <div class="col-sm-4">
                                        <label class="control-label" for="Name"
                                            >Details</label
                                        >
                                    </div>
                                    <div class="col-sm-8">
                                        <textarea
                                            ref="approval_details"
                                            v-model="approval.details"
                                            name="approval_details"
                                            class="form-control"
                                            style="width: 70%"
                                        ></textarea>
                                    </div>
                                </div>
                            </div>
                            <div class="form-group">
                                <div class="row mb-3">
                                    <div class="col-sm-4">
                                        <label class="control-label" for="Name"
                                            >CC email</label
                                        >
                                    </div>
                                    <div class="col-sm-8">
                                        <input
                                            v-model="approval.cc_email"
                                            type="text"
                                            class="form-control"
                                            name="approval_cc"
                                            style="width: 70%"
                                        />
                                    </div>
                                </div>
                            </div>
                            <div
                                v-if="
                                    processing_status == 'With Assessor' ||
                                    processing_status == 'On Agenda'
                                "
                                class="form-group"
                            >
                                <div class="row mb-3">
                                    <div class="col-sm-4">
                                        <label class="control-label"
                                            >Approval Document</label
                                        >
                                    </div>
                                    <div class="col-sm-8">
                                        <FileField2
                                            ref="filefield"
                                            :proposal_id="
                                                conservation_status_id
                                            "
                                            :is-repeatable="false"
                                            name="cs_approval_file"
                                        />
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
                    <button
                        v-if="issuingApproval"
                        type="button"
                        disabled
                        class="btn btn-primary"
                        @click="ok"
                    >
                        Processing
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
                        {{ ok_button_text }}
                    </button>
                </div>
            </template>
        </modal>
    </div>
</template>

<script>
import modal from '@vue-utils/bootstrap-modal.vue';
import alert from '@vue-utils/alert.vue';
import FileField2 from '@/components/forms/filefield.vue';
import { helpers, api_endpoints } from '@/utils/hooks.js';
import ErrorRenderer from '@/utils/vue/ErrorRenderer.vue';
export default {
    name: 'ProposedApproval',
    components: {
        modal,
        alert,
        ErrorRenderer,
        FileField2,
    },
    props: {
        conservation_status_id: {
            type: Number,
            required: true,
        },
        processing_status: {
            type: String,
            required: true,
        },
        change_code: {
            type: String,
            default: null,
        },
    },
    data: function () {
        return {
            isModalOpen: false,
            approval: {
                effective_from_date: new Date().toISOString().slice(0, 10),
            },
            uploadedFile: null,
            state: 'proposed_approval',
            issuingApproval: false,
            validation_form: null,
            errors: false,
            showtoDateError: false,
            showstartDateError: false,
            errorString: '',
            successString: '',
            success: false,
            datepickerOptions: {
                format: 'DD/MM/YYYY',
                showClear: true,
                useCurrent: false,
                keepInvalid: true,
                allowInputToggle: true,
            },
        };
    },
    computed: {
        csrf_token: function () {
            return helpers.getCookie('csrftoken');
        },
        showError: function () {
            var vm = this;
            return vm.errors;
        },
        isEffectiveDateValid: function () {
            if (
                this.approval.effective_from_date &&
                this.approval.effective_to_date
            ) {
                const fromDate = new Date(this.approval.effective_from_date);
                const toDate = new Date(this.approval.effective_to_date);
                return fromDate < toDate;
            } else {
                return true;
            }
        },
        ok_button_text: function () {
            let action = this.change_code === 'DeList' ? 'Delist' : 'Approve';
            if (this.state == 'proposed_approval') {
                return `Propose to ${action}`;
            }
            return action;
        },
        title: function () {
            let action = this.change_code === 'DeList' ? 'Delist' : 'Approve';
            if (this.state == 'proposed_approval') {
                return `Propose to ${action} Conservation Status CS${this.conservation_status_id}`;
            }
            return `${action} Conservation Status CS${this.conservation_status_id}`;
        },
        can_preview: function () {
            return this.processing_status == 'Proposed DeListed' &&
                this.approval.effective_from_date &&
                this.approval.effective_to_date
                ? true
                : false;
        },
    },
    watch: {
        isModalOpen: function (val) {
            if (val) {
                this.$nextTick(() => {
                    this.$refs.start_date.focus();
                });
            }
        },
    },
    methods: {
        ok: function () {
            let vm = this;
            let errors = vm.isError();
            if (!errors && vm.isEffectiveDateValid) {
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
            this.approval = {
                effective_from_date: new Date().toISOString().slice(0, 10),
            };
            this.errors = false;
        },
        sendData: function () {
            let vm = this;
            vm.errors = false;
            let approval = JSON.parse(JSON.stringify(vm.approval));
            vm.issuingApproval = true;
            if (vm.state == 'proposed_approval') {
                fetch(
                    helpers.add_endpoint_json(
                        api_endpoints.conservation_status,
                        vm.conservation_status_id + '/proposed_approval'
                    ),
                    {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json',
                        },
                        body: JSON.stringify(approval),
                    }
                )
                    .then(
                        async (response) => {
                            if (!response.ok) {
                                vm.errors = true;
                                const data = await response.json();
                                vm.errorString = data;
                                return;
                            }
                            vm.close();
                            vm.$emit('refreshFromResponse', response);
                            vm.$router.push({
                                path: '/internal/conservation-status/',
                            }); //Navigate to dashboard page after Propose issue.
                        },
                        (error) => {
                            vm.errors = true;
                            vm.errorString = helpers.apiVueResourceError(error);
                        }
                    )
                    .finally(() => {
                        vm.issuingApproval = false;
                    });
            } else if (vm.state == 'final_approval') {
                let formData = new FormData();
                var files = vm.$refs.filefield.files;
                vm.uploadedFile = files.length > 0 ? files[0].file : null;
                formData.append('proposal_approval_document', vm.uploadedFile);
                formData.append('data', JSON.stringify(approval));

                fetch(
                    helpers.add_endpoint_json(
                        api_endpoints.conservation_status,
                        vm.conservation_status_id + '/final_approval'
                    ),
                    {
                        method: 'POST',
                        body: formData,
                    }
                )
                    .then(
                        async (response) => {
                            if (!response.ok) {
                                vm.errors = true;
                                const data = await response.json();
                                vm.errorString = data;
                                return;
                            }
                            vm.close();
                            vm.$emit('refreshFromResponse', response);
                        },
                        (error) => {
                            vm.errors = true;
                            vm.errorString = helpers.apiVueResourceError(error);
                        }
                    )
                    .finally(() => {
                        vm.issuingApproval = false;
                    });
            }
        },
        validateEffectiveFromDate: function (event) {
            let vm = this;
            const value = event.target.value;
            if (!value) {
                vm.showstartDateError = true;
            } else {
                vm.showstartDateError = false;
            }
        },
        isError: function () {
            let vm = this;
            let hasError = false;
            if (!vm.approval.effective_from_date) {
                vm.showstartDateError = true;
                hasError = true;
            }
            return hasError;
        },
    },
};
</script>
