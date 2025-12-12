<template lang="html">
    <div id="internal-ocr-back-to-assessor">
        <modal
            id="back-to-assessor-modal"
            transition="modal fade"
            title="Back to Assessor"
            ok-text="Back to Assessor"
            large
            @ok="ok()"
            @cancel="cancel()"
        >
            <div class="container-fluid">
                <div class="row">
                    <form class="form-horizontal" name="back-to-assessor-form">
                        <alert v-if="errorString" type="danger"
                            ><strong>{{ errorString }}</strong></alert
                        >
                        <div class="col-sm-12">
                            <div class="row mb-3">
                                <div class="col">
                                    <div class="form-group">
                                        <label
                                            class="control-label mb-3"
                                            for="reason"
                                            >Reason</label
                                        >
                                        <textarea
                                            id="reason"
                                            ref="reason"
                                            v-model="back_to_assessor.reason"
                                            class="form-control"
                                            name="reason"
                                            required
                                        ></textarea>
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
            back_to_assessor: {
                reason: '',
            },
            errorString: '',
            originalBackToAssessor: null,
        };
    },
    watch: {
        isModalOpen: function (val) {
            if (val) {
                this.$nextTick(() => {
                    this.$refs.reason.focus();
                    this.originalBackToAssessor = JSON.parse(
                        JSON.stringify(this.back_to_assessor)
                    );
                });
            }
        },
    },
    mounted: function () {
        this.form = document.forms['back-to-assessor-form'];
    },
    methods: {
        hasUnsavedChanges: function () {
            return (
                JSON.stringify(this.back_to_assessor) !==
                JSON.stringify(this.originalBackToAssessor)
            );
        },
        ok: function () {
            if (this.form.checkValidity()) {
                this.backToAssessor();
            } else {
                this.form.reportValidity();
            }
        },
        cancel: function () {
            this.close();
        },
        close: function () {
            this.isModalOpen = false;
            this.back_to_assessor = {
                reason: '',
            };
            this.errorString = '';
        },
        backToAssessor: function () {
            let vm = this;
            vm.errorString = '';
            fetch(
                api_endpoints.occurrence_report +
                    `/${vm.occurrence_report_id}/back_to_assessor/`,
                {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify(vm.back_to_assessor),
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
                        title: 'Sent Back',
                        text: `${vm.occurrence_report_number} has been sent back to the assessor with the provided reason.`,
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
                }
            );
        },
    },
};
</script>
