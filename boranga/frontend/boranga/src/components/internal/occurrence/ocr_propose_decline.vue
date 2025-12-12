<template lang="html">
    <div id="internal-ocr-propose-decline-request">
        <modal
            id="ocr-propose-decline-modal"
            transition="modal fade"
            ok-text="Propose Decline"
            :title="`Propose Decline ${occurrence_report_number}`"
            large
            @ok="ok()"
            @cancel="close()"
        >
            <div class="container-fluid">
                <div class="row">
                    <form class="form-horizontal" name="propose-decline-form">
                        <alert v-if="errorString" type="danger"
                            ><strong>{{ errorString }}</strong></alert
                        >
                        <div class="row mb-3">
                            <div class="col-sm-12">
                                <label class="control-label mb-3" for="reason"
                                    >Reason</label
                                >
                                <textarea
                                    id="reason"
                                    ref="reason"
                                    v-model="propose_decline.reason"
                                    class="form-control"
                                    name="reason"
                                    required
                                ></textarea>
                            </div>
                        </div>
                        <div class="row">
                            <div class="col-sm-12">
                                <label class="control-label" for="cc_email"
                                    >CC email</label
                                >
                                <input
                                    v-model="propose_decline.cc_email"
                                    type="text"
                                    style="width: 70%"
                                    class="form-control"
                                    name="cc_email"
                                />
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
    name: 'OcrProposeDecline',
    components: {
        modal,
        alert,
    },
    props: {
        occurrence_report_id: {
            type: Number,
        },
        occurrence_report_number: {
            type: String,
        },
    },
    data: function () {
        return {
            isModalOpen: false,
            form: null,
            propose_decline: {
                reason: '',
                cc_email: '',
            },
            errorString: '',
            originalProposeDecline: null,
        };
    },
    watch: {
        isModalOpen: function (val) {
            if (val) {
                this.$nextTick(() => {
                    this.$refs.reason.focus();
                    this.originalProposeDecline = JSON.parse(
                        JSON.stringify(this.propose_decline)
                    );
                });
            }
        },
    },
    mounted: function () {
        this.form = document.forms['propose-decline-form'];
    },
    methods: {
        hasUnsavedChanges: function () {
            return (
                JSON.stringify(this.propose_decline) !==
                JSON.stringify(this.originalProposeDecline)
            );
        },
        ok: function () {
            let vm = this;
            if (vm.form.checkValidity()) {
                vm.proposeDecline();
            } else {
                vm.form.reportValidity();
            }
        },
        close: function () {
            this.isModalOpen = false;
            this.propose_decline = {
                reason: '',
                occurrence_report: this.occurrence_report_id,
            };
            this.errorString = '';
            $(this.$refs.reason).val(null).trigger('change');
            this.form.reset();
        },
        proposeDecline: function () {
            let vm = this;
            swal.fire({
                title: 'Propose Decline Application',
                text: `Are you sure you want to propose to decline occurrence report ${vm.occurrence_report_number}?`,
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Propose Decline',
                customClass: {
                    confirmButton: 'btn btn-primary',
                    cancelButton: 'btn btn-secondary me-2',
                },
                reverseButtons: true,
            }).then((swalresult) => {
                if (swalresult.isConfirmed) {
                    fetch(
                        helpers.add_endpoint_join(
                            api_endpoints.occurrence_report,
                            '/' +
                                (vm.occurrence_report_id + '/propose_decline/')
                        ),
                        {
                            method: 'POST',
                            headers: {
                                'Content-Type': 'application/json',
                            },
                            body: JSON.stringify(vm.propose_decline),
                        }
                    ).then(
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
                            swal.fire({
                                title: 'Proposal to Decline Successful',
                                text: `Your proposal to decline occurrence report ${vm.occurrence_report_number} has been successfully submitted.`,
                                icon: 'success',
                                customClass: {
                                    confirmButton: 'btn btn-primary',
                                },
                            }).then(() => {
                                vm.$router.go();
                            });
                        },
                        (error) => {
                            console.log(error);
                            vm.errorString = helpers.apiVueResourceError(error);
                            vm.amendingProposal = true;
                        }
                    );
                }
            });
        },
    },
};
</script>
