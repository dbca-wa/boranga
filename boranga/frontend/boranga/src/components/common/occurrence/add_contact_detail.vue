<template lang="html">
    <div id="contactDetail">
        <modal
            transition="modal fade"
            :title="title"
            large
            :data-loss-warning-on-cancel="contact_detail_action !== 'view'"
            @ok="ok()"
        >
            <div class="container-fluid">
                <div class="row">
                    <form
                        class="form-horizontal needs-validation"
                        name="contactDetailForm"
                        novalidate
                    >
                        <alert v-if="errorString" type="danger"
                            ><strong>{{ errorString }}</strong></alert
                        >
                        <div class="col-sm-12">
                            <div class="form-group">
                                <div class="row modal-input-row mb-3">
                                    <div class="col-sm-3">
                                        <label class="control-label pull-left"
                                            >Name</label
                                        >
                                    </div>
                                    <div class="col-sm-9">
                                        <input
                                            ref="contact_name"
                                            v-model="contactObj.contact_name"
                                            type="text"
                                            class="form-control"
                                            :disabled="isReadOnly"
                                            required
                                        />
                                    </div>
                                </div>
                                <div class="row mb-3">
                                    <div class="col-sm-3">
                                        <label class="control-label pull-left"
                                            >Role</label
                                        >
                                    </div>
                                    <div class="col-sm-9">
                                        <input
                                            v-model="contactObj.role"
                                            type="text"
                                            class="form-control"
                                            :disabled="isReadOnly"
                                        />
                                    </div>
                                </div>
                                <div class="row mb-3">
                                    <div class="col-sm-3">
                                        <label class="control-label pull-left"
                                            >Organisation</label
                                        >
                                    </div>
                                    <div class="col-sm-9">
                                        <input
                                            v-model="contactObj.organisation"
                                            type="text"
                                            class="form-control"
                                            :disabled="isReadOnly"
                                        />
                                    </div>
                                </div>
                                <div class="row mb-3">
                                    <div class="col-sm-3">
                                        <label class="control-label pull-left"
                                            >Contact Details</label
                                        >
                                    </div>
                                    <div class="col-sm-9">
                                        <textarea
                                            id="contact_details"
                                            v-model="contactObj.contact"
                                            class="form-control"
                                            :disabled="isReadOnly"
                                            rows="4"
                                        />
                                    </div>
                                </div>
                                <div class="row mb-3">
                                    <div class="col-sm-3">
                                        <label class="control-label pull-left"
                                            >Notes</label
                                        >
                                    </div>
                                    <div class="col-sm-9">
                                        <textarea
                                            id="notes"
                                            v-model="contactObj.notes"
                                            class="form-control"
                                            :disabled="isReadOnly"
                                            rows="4"
                                        />
                                    </div>
                                </div>

                                <div class="row mb-3">
                                    <div class="col-sm-3">
                                        <label class="control-label pull-left"
                                            >&nbsp;</label
                                        >
                                    </div>
                                    <div v-if="!isReadOnly" class="col-sm-9">
                                        <button
                                            type="button"
                                            class="btn btn-primary mb-2"
                                            @click="clearForm"
                                        >
                                            Clear Form</button
                                        ><br />
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
                        {{ isReadOnly ? 'Close' : 'Cancel' }}
                    </button>
                    <template v-if="!isReadOnly">
                        <template v-if="contact_detail_id">
                            <button
                                v-if="updatingContact"
                                type="button"
                                disabled
                                class="btn btn-primary"
                                @click="ok"
                            >
                                Updating
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
                                Update Contact
                            </button>
                        </template>
                        <template v-else>
                            <button
                                v-if="addingContact"
                                type="button"
                                disabled
                                class="btn btn-primary"
                                @click="ok"
                            >
                                Adding
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
                                Add Contact
                            </button>
                        </template>
                    </template>
                </div>
            </template>
        </modal>
    </div>
</template>

<script>
import modal from '@vue-utils/bootstrap-modal.vue';
import alert from '@vue-utils/alert.vue';
import { helpers } from '@/utils/hooks.js';
import swal from 'sweetalert2';

export default {
    name: 'ContactDetail',
    components: {
        modal,
        alert,
    },
    props: {
        url: {
            type: String,
            required: true,
        },
        occurrence: {
            type: Object,
            required: true,
        },
    },
    data: function () {
        return {
            isModalOpen: false,
            form: null,
            contact_detail_id: String,
            contact_detail_action: String,
            contactObj: {},
            addingContact: false,
            updatingContact: false,
            validation_form: null,
            type: '1',
            errorString: '',
            successString: '',
            success: false,
            validDate: false,
        };
    },
    computed: {
        title: function () {
            var action = this.contact_detail_action;
            if (typeof action === 'string' && action.length > 0) {
                var capitalizedAction =
                    action.charAt(0).toUpperCase() + action.slice(1);
                return `${capitalizedAction} Contact to ${this.occurrence.occurrence_number}`;
            } else {
                return 'Invalid Contact detail action'; // Or handle the error in an appropriate way
            }
        },
        isReadOnly: function () {
            return this.contact_detail_action === 'view' ? true : false;
        },
    },
    watch: {
        isModalOpen: function (val) {
            if (val) {
                this.$nextTick(() => {
                    this.$refs.contact_name.focus();
                });
            }
        },
    },
    mounted: function () {
        let vm = this;
        vm.form = document.forms.contactDetailForm;
    },
    methods: {
        ok: function () {
            let vm = this;
            if ($(vm.form).valid()) {
                vm.sendData();
            }
        },
        cancel: function () {
            if (this.isReadOnly) {
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
            this.contactObj = {};
            this.errorString = '';
            $('.has-error').removeClass('has-error');
        },
        sendData: function () {
            let vm = this;
            vm.errorString = '';
            let contactObj = JSON.parse(JSON.stringify(vm.contactObj));
            let formData = new FormData();

            if (vm.contactObj.id) {
                vm.updatingContact = true;
                formData.append('data', JSON.stringify(contactObj));
                fetch(helpers.add_endpoint_json(vm.url, contactObj.id), {
                    method: 'PUT',
                    body: formData,
                })
                    .then(async (response) => {
                        const data = await response.json();
                        if (!response.ok) {
                            vm.errorString = data;
                            return;
                        }
                        vm.updatingContact = false;
                        vm.$parent.updatedContactDetails();
                        vm.close();
                    })
                    .finally(() => {
                        vm.updatingContact = false;
                    });
            } else {
                vm.addingContact = true;
                formData.append('data', JSON.stringify(contactObj));
                fetch(vm.url, {
                    method: 'POST',
                    body: formData,
                })
                    .then(async (response) => {
                        const data = await response.json();
                        if (!response.ok) {
                            vm.errorString = data;
                            return;
                        }
                        vm.addingContact = false;
                        vm.$parent.updatedContactDetails();
                        vm.close();
                    })
                    .finally(() => {
                        vm.addingContact = false;
                    });
            }
        },
        clearForm: function () {
            this.contactObj = {
                occurrence: this.occurrence.id,
            };
            this.$refs.contact_name.focus();
        },
    },
};
</script>

<style lang="css">
.error {
    color: red;
}
.modal-input-row {
    margin-bottom: 20px;
}

input[type='text'],
select {
    padding: 0.375rem 2.25rem 0.375rem 0.75rem;
}
</style>
