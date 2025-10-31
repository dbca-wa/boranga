<template lang="html">
    <div id="internal-conservation-status-proposal-back-to-assessor">
        <modal
            id="back-to-assessor-modal"
            transition="modal fade"
            :title="`Send CS${conservation_status_id} Back to Assessor`"
            ok-text="Back to Assessor"
            large
            @ok="ok()"
            @cancel="close()"
        >
            <div class="container-fluid">
                <div class="row">
                    <form
                        id="back-to-assessor-form"
                        class="form-horizontal needs-validation"
                        name="back-to-assessor-form"
                        novalidate
                    >
                        <alert v-if="errors" type="danger"
                            ><strong>{{ errors }}</strong></alert
                        >
                        <div class="col-sm-12">
                            <div class="row mb-3">
                                <div class="col">
                                    <div class="form-group">
                                        <label
                                            class="control-label mb-3 fw-bold"
                                            for="Name"
                                            >Reason / Comments <span class="text-danger">*</span></label
                                        >
                                        <textarea
                                            ref="approver_comment"
                                            v-model="approver_comment"
                                            class="form-control"
                                            name="approver_comment"
                                            required
                                        ></textarea>
                                        <div class="invalid-feedback">
                                            Please enter the reason for sending
                                            the Conservation Status Proposal
                                            back to the Assessor.
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </form>
                </div>
            </div>
        </modal>
    </div>
</template>

<script>
import modal from '@vue-utils/bootstrap-modal.vue';
import alert from '@vue-utils/alert.vue';
import { helpers, api_endpoints } from '@/utils/hooks.js';

export default {
    name: 'BackToAssessor',
    components: {
        modal,
        alert,
    },
    props: {
        conservation_status_id: {
            type: Number,
        },
    },
    emits: ['refreshFromResponse'],
    data: function () {
        return {
            approver_comment: null,
            isModalOpen: false,
            errors: null,
        };
    },
    watch: {
        isModalOpen: function (val) {
            if (val) {
                this.$nextTick(() => {
                    $(this.$refs.approver_comment).focus();
                });
            }
        },
    },
    methods: {
        ok: function () {
            let vm = this;
            var form = document.getElementById('back-to-assessor-form');
            if (form.checkValidity()) {
                vm.backToAssessor();
            } else {
                form.classList.add('was-validated');
                $('#back-to-assessor-form').find(':invalid').first().focus();
            }
            return false;
        },
        close: function () {
            this.approver_comment = null;
            this.errors = null;
            this.isModalOpen = false;
        },
        backToAssessor: function () {
            let vm = this;
            vm.errors = null;
            let data = {
                status: 'with_assessor',
                approver_comment: vm.approver_comment,
            };
            fetch(
                helpers.add_endpoint_json(
                    api_endpoints.conservation_status,
                    vm.conservation_status_id + '/switch_status'
                ),
                {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify(data),
                }
            ).then(
                (response) => {
                    vm.$emit('refreshFromResponse', response);
                    this.close();
                },
                (error) => {
                    swal.fire({
                        title: 'Application Error',
                        text: helpers.apiVueResourceError(error),
                        icon: 'error',
                        customClass: {
                            confirmButton: 'btn btn-primary',
                        },
                    });
                }
            );
        },
    },
};
</script>
