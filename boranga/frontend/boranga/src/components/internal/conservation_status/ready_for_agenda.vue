<template lang="html">
    <div id="internal-conservation-status-proposal-ready-for-agenda">
        <modal
            id="ready-for-agenda-modal"
            transition="modal fade"
            :title="`CS${conservation_status_id} Ready for Agenda`"
            ok-text="Confirm Ready for Agenda"
            large
            @ok="ok()"
            @cancel="close()"
        >
            <div class="container-fluid">
                <div class="row">
                    <form
                        id="ready-for-agenda-form"
                        class="form-horizontal needs-validation"
                        name="ready-for-agenda-form"
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
                                            >Approver Comments
                                            <span class="text-danger"
                                                >*</span
                                            ></label
                                        >
                                        <textarea
                                            ref="assessor_comment"
                                            v-model="assessor_comment"
                                            class="form-control"
                                            name="assessor_comment"
                                            required
                                        ></textarea>
                                        <div class="invalid-feedback">
                                            Please enter any relevant comments.
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
    name: 'ReadyForAgenda',
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
            assessor_comment: null,
            isModalOpen: false,
            errors: null,
        };
    },
    watch: {
        isModalOpen: function (val) {
            if (val) {
                this.$nextTick(() => {
                    $(this.$refs.assessor_comment).focus();
                });
            }
        },
    },
    methods: {
        ok: function () {
            let vm = this;
            var form = document.getElementById('ready-for-agenda-form');
            if (form.checkValidity()) {
                vm.readyForAgenda();
            } else {
                form.classList.add('was-validated');
                $('#ready-for-agenda-form').find(':invalid').first().focus();
            }
            return false;
        },
        close: function () {
            this.assessor_comment = null;
            this.errors = null;
            this.isModalOpen = false;
        },
        readyForAgenda: function () {
            let vm = this;
            fetch(
                helpers.add_endpoint_json(
                    api_endpoints.conservation_status,
                    vm.conservation_status_id + '/ready_for_agenda'
                ),
                {
                    method: 'PATCH',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify({
                        assessor_comment: vm.assessor_comment,
                    }),
                }
            ).then(
                () => {
                    vm.$router.push({ path: '/internal/conservation-status/' });
                },
                (error) => {
                    vm.errors = true;
                    vm.errorString = helpers.apiVueResourceError(error);
                }
            );
        },
    },
};
</script>
