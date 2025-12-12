<template lang="html">
    <div id="internal-ocr-decline-request">
        <modal
            id="ocr-decline-modal"
            transition="modal fade"
            ok-text="Decline"
            :title="`Decline ${occurrence_report_number}`"
            large
            @ok="ok()"
            @cancel="close()"
        >
            <div class="container-fluid">
                <div class="row">
                    <form class="form-horizontal" name="decline-form">
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
                                    v-model="decline.reason"
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
                                    v-model="decline.cc_email"
                                    type="text"
                                    readonly
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
    name: 'OcrDecline',
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
        declined_details: {
            type: Object,
        },
    },
    data: function () {
        return {
            isModalOpen: false,
            form: null,
            decline: {
                reason: '',
                cc_email: '',
            },
            errorString: '',
            originalDecline: null,
        };
    },
    watch: {
        isModalOpen: function (val) {
            if (val) {
                this.$nextTick(() => {
                    this.$refs.reason.focus();
                    if (this.declined_details) {
                        this.decline.reason = this.declined_details.reason;
                        this.decline.cc_email = this.declined_details.cc_email;
                    }
                    this.originalDecline = JSON.parse(
                        JSON.stringify(this.decline)
                    );
                });
            }
        },
    },
    mounted: function () {
        this.form = document.forms['decline-form'];
    },
    methods: {
        hasUnsavedChanges: function () {
            return (
                JSON.stringify(this.decline) !==
                JSON.stringify(this.originalDecline)
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
            this.decline = {
                reason: '',
                occurrence_report: this.occurrence_report_id,
            };
            this.errorString = '';
            this.form.reset();
        },
        proposeDecline: function () {
            let vm = this;
            swal.fire({
                title: 'Decline Occurrence Report',
                text: `Are you sure you want to decline occurrence report ${vm.occurrence_report_number}?`,
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Decline',
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
                            '/' + (vm.occurrence_report_id + '/decline/')
                        ),
                        {
                            method: 'POST',
                            headers: {
                                'Content-Type': 'application/json',
                            },
                            body: JSON.stringify(vm.decline),
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
                                title: 'Occurrence Report Successfully Declined',
                                text: `Occurrence Report ${vm.occurrence_report_number} has been successfully declined. The proponent has been notified via email.`,
                                icon: 'success',
                                customClass: {
                                    confirmButton: 'btn btn-primary',
                                },
                            });
                            vm.$router.push({
                                name: 'internal-occurrence-dash',
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
