<template lang="html">
    <div id="proposal-proposed-decline">
        <modal
            transition="modal fade"
            :title="title"
            large
            @ok="ok()"
            @cancel="cancel()"
        >
            <div class="container-fluid">
                <div class="row">
                    <form class="form-horizontal" name="declineForm">
                        <alert v-if="errorString" type="danger"
                            ><strong>{{ errorString }}</strong></alert
                        >
                        <div class="col-sm-12">
                            <div class="form-group">
                                <div class="row mb-3">
                                    <div class="col-sm-12">
                                        <label
                                            class="control-label fw-bold"
                                            for="Name"
                                            >Details / Reason
                                            <span class="text-danger"
                                                >*</span
                                            ></label
                                        >
                                        <textarea
                                            ref="reason"
                                            v-model="decline.reason"
                                            style="width: 70%"
                                            class="form-control"
                                            name="reason"
                                            required
                                        ></textarea>
                                    </div>
                                </div>
                            </div>
                            <div class="form-group">
                                <div class="row" mb-3>
                                    <div class="col-sm-12">
                                        <label class="control-label" for="Name"
                                            >CC email</label
                                        >
                                        <input
                                            v-model="decline.cc_email"
                                            type="text"
                                            style="width: 70%"
                                            class="form-control"
                                            name="cc_email"
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
                        v-if="decliningProposal"
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
                        Decline
                    </button>
                </div>
            </template>
        </modal>
    </div>
</template>

<script>
import modal from '@vue-utils/bootstrap-modal.vue';
import alert from '@vue-utils/alert.vue';
import { helpers, api_endpoints } from '@/utils/hooks.js';
export default {
    name: 'DeclineProposal',
    components: {
        modal,
        alert,
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
    },
    data: function () {
        return {
            isModalOpen: false,
            form: null,
            decline: {},
            decliningProposal: false,
            errorString: '',
        };
    },
    computed: {
        title: function () {
            return `Decline Conservation Status CS${this.conservation_status_id}`;
        },
    },
    watch: {
        isModalOpen: function (val) {
            if (val) {
                this.$nextTick(() => {
                    this.$refs['reason'].focus();
                });
            }
        },
    },
    mounted: function () {
        let vm = this;
        vm.form = document.forms.declineForm;
    },
    methods: {
        ok: function () {
            let vm = this;
            if ($(vm.form).valid()) {
                vm.sendData();
            } else {
                // focs the first invalid field
                let firstInvalid = $(vm.form).find('.error').first();
                if (firstInvalid.length) {
                    firstInvalid.focus();
                }
            }
        },
        cancel: function () {
            swal.fire({
                title: 'Are you sure you want to close this modal?',
                text: 'You will lose any unsaved changes.',
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Yes, close it',
                cancelButtonText: 'Return to modal',
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
            this.decline = {};
            this.errorString = '';
            $('.has-error').removeClass('has-error');
        },
        sendData: function () {
            let vm = this;
            vm.errorString = '';
            let decline = JSON.parse(JSON.stringify(vm.decline));
            vm.decliningProposal = true;
            if (
                vm.processing_status == 'With Assessor' ||
                vm.processing_status == 'On Agenda'
            ) {
                fetch(
                    helpers.add_endpoint_json(
                        api_endpoints.conservation_status,
                        vm.conservation_status_id + '/final_decline'
                    ),
                    {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json',
                        },
                        body: JSON.stringify(decline),
                    }
                )
                    .then(async (response) => {
                        const data = await response.json();
                        if (!response.ok) {
                            vm.errorString = data;
                            return;
                        }
                        vm.close();
                        vm.$emit('refreshFromResponse', response);
                        vm.$router.push({
                            path: '/internal/conservation-status/',
                        }); //Navigate to dashboard after propose decline.
                    })
                    .finally(() => {
                        vm.decliningProposal = false;
                    });
            }
        },
    },
};
</script>
