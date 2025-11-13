<template lang="html">
    <div id="occurrence_tenure_modal">
        <modal
            transition="modal fade"
            :title="modalTitle"
            large
            @ok="ok()"
            @cancel="cancel()"
        >
            <div class="container-fluid">
                <div class="row">
                    <form class="form-horizontal" name="modalForm">
                        <alert v-if="errorString" type="danger"
                            ><strong>{{ errorString }}</strong></alert
                        >
                        <div class="col-sm-12">
                            <div class="form-group">
                                <div class="row mb-3">
                                    <div class="col-sm-3">
                                        <label class="control-label pull-left"
                                            >Status</label
                                        >
                                    </div>
                                    <div class="col-sm-3">
                                        <!-- status_display -->
                                        <input
                                            id="status_id"
                                            v-model="tenureObj.status"
                                            :disabled="
                                                isReadOnly ||
                                                isAlwaysReadOnly('status')
                                            "
                                            type="text"
                                            class="form-control"
                                            placeholder="Status"
                                        />
                                    </div>
                                </div>
                            </div>
                            <div class="row mb-3">
                                <label for="" class="col-sm-3 control-label"
                                    >Tenure Area Id</label
                                >
                                <div class="col-sm-9">
                                    <input
                                        id="tenure_area_id"
                                        v-model="tenureObj.tenure_area_id"
                                        :disabled="
                                            isReadOnly ||
                                            isAlwaysReadOnly('tenure_area_id')
                                        "
                                        type="text"
                                        class="form-control"
                                        placeholder="Tenure Area Id"
                                    />
                                </div>
                            </div>
                            <div class="row mb-3">
                                <div class="col-sm-3">
                                    <label class="control-label pull-left"
                                        >Owner/Manager</label
                                    >
                                </div>
                                <div class="col-sm-9">
                                    <input
                                        id="owner_name"
                                        v-model="tenureObj.owner_name"
                                        :disabled="
                                            isReadOnly ||
                                            isAlwaysReadOnly('owner_name')
                                        "
                                        type="text"
                                        class="form-control"
                                        placeholder="Owner Name"
                                    />
                                </div>
                            </div>
                            <div class="row mb-3">
                                <div class="col-sm-3">
                                    <label class="control-label pull-left"
                                        >Vesting</label
                                    >
                                </div>
                                <div class="col-sm-9">
                                    <select
                                        ref="vesting_select"
                                        v-model="tenureObj.vesting_id"
                                        :disabled="
                                            isReadOnly ||
                                            isAlwaysReadOnly('vesting_id')
                                        "
                                        style="width: 100%"
                                        class="form-select input-sm"
                                    >
                                        <option :value="null" selected>
                                            Select a Vesting
                                        </option>
                                        <option
                                            v-for="vesting in vestings"
                                            :key="vesting.id"
                                            :value="vesting.id"
                                        >
                                            {{ vesting.code }} -
                                            {{ vesting.label }}
                                        </option>
                                    </select>
                                </div>
                            </div>
                            <div class="row mb-3">
                                <div class="col-sm-3">
                                    <label class="control-label pull-left"
                                        >Purpose</label
                                    >
                                </div>
                                <div class="col-sm-9">
                                    <template v-if="!isReadOnly">
                                        <template
                                            v-if="
                                                purposes &&
                                                purposes.length > 0 &&
                                                tenureObj.purpose_id &&
                                                !purposes
                                                    .map((d) => d.id)
                                                    .includes(
                                                        tenureObj.purpose_id
                                                    )
                                            "
                                        >
                                            <input
                                                v-if="tenureObj.purpose"
                                                type="text"
                                                class="form-control mb-3"
                                                :value="
                                                    tenureObj.purpose +
                                                    ' (Now Archived)'
                                                "
                                                disabled
                                            />
                                            <div class="mb-3 text-muted">
                                                Change purpose to:
                                            </div>
                                        </template>
                                        <select
                                            v-model="tenureObj.purpose_id"
                                            class="form-select"
                                        >
                                            <option :value="null" selected>
                                                Select a Purpose
                                            </option>
                                            <option
                                                v-for="purpose in purposes"
                                                :key="purpose.id"
                                                :value="purpose.id"
                                            >
                                                {{ purpose.code }} -
                                                {{ purpose.label }}
                                            </option>
                                        </select>
                                    </template>
                                    <template v-else>
                                        <input
                                            v-model="tenureObj.purpose"
                                            class="form-control"
                                            type="text"
                                            :disabled="isReadOnly"
                                        />
                                    </template>
                                </div>
                            </div>
                            <div class="row mb-3">
                                <div class="col-sm-3">
                                    <label
                                        class="control-label pull-left fw-bold"
                                        >Significant to Occurrence
                                        <span class="text-danger"
                                            >*</span
                                        ></label
                                    >
                                </div>
                                <div class="col-sm-3">
                                    <select
                                        ref="significant_to_occurrence_select"
                                        v-model="
                                            tenureObj.significant_to_occurrence
                                        "
                                        :disabled="
                                            isReadOnly ||
                                            isAlwaysReadOnly(
                                                'significant_to_occurrence'
                                            )
                                        "
                                        style="width: 100%"
                                        class="form-select input-sm"
                                    >
                                        <option :value="'Yes'">Yes</option>
                                        <option :value="'No'">No</option>
                                    </select>
                                </div>
                            </div>
                            <div class="form-group">
                                <div class="row mb-3">
                                    <div class="col-sm-3">
                                        <label class="control-label pull-left"
                                            >Comments</label
                                        >
                                    </div>
                                    <div class="col-sm-9">
                                        <textarea
                                            v-model="tenureObj.comments"
                                            :disabled="
                                                isReadOnly ||
                                                isAlwaysReadOnly('comments')
                                            "
                                            rows="2"
                                            class="form-control"
                                        >
                                        </textarea>
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
                    <template v-if="modal_action != 'view'">
                        <template v-if="object_id">
                            <button
                                v-if="updatingEntry"
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
                                Update
                            </button>
                        </template>
                        <template v-else>
                            <button
                                v-if="addingEntry"
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
                                Add Entry
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
import { helpers, api_endpoints } from '@/utils/hooks.js';
export default {
    name: 'OccurrenceTenureDatatable',
    components: {
        modal,
        alert,
    },
    props: {
        url: {
            type: String,
            required: true,
        },
        title: {
            type: String,
            required: false,
            default: 'Object',
        },
        occurrenceId: {
            type: Number,
            required: true,
        },
        alwaysReadOnly: {
            type: Array,
            required: false,
            default: () => [],
        },
    },
    emits: ['refreshFromResponse'],
    data: function () {
        return {
            isModalOpen: false,
            form: null,
            object_id: String,
            modal_action: String,
            tenureObj: {},
            addingEntry: false,
            updatingEntry: false,
            validation_form: null,
            type: '1',
            errors: false,
            errorString: '',
            successString: '',
            success: false,
            purposes: [],
            vestings: [],
        };
    },
    computed: {
        modalTitle: function () {
            var action = this.modal_action;
            if (typeof action === 'string' && action.length > 0) {
                let capitalizedAction =
                    action.charAt(0).toUpperCase() + action.slice(1);
                return `${capitalizedAction} ${this.title}`;
            } else {
                return `Invalid ${this.title} action`; // Or handle the error in an appropriate way
            }
        },
        isReadOnly: function () {
            return this.modal_action === 'view' ? true : false;
        },
    },
    watch: {},
    created: async function () {
        this.fetchSelectionValues(
            api_endpoints.occurrence_tenure_list_of_values,
            {},
            ['purposes', 'vestings']
        );
    },
    mounted: function () {
        let vm = this;
        vm.form = document.forms.modalForm;
    },
    methods: {
        fetchSelectionValues: function (url, payload, target_list_names) {
            fetch(url, payload)
                .then(async (response) => {
                    if (!response.ok) {
                        return await response.json().then((json) => {
                            throw new Error(json);
                        });
                    }
                    return response.json();
                })
                .then((data) => {
                    target_list_names.forEach((list_name) => {
                        this[list_name] = data[list_name];
                    });
                })
                .catch((error) => {
                    console.error('Error:', error);
                });
        },
        ok: function () {
            let vm = this;
            if ($(vm.form).valid()) {
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
            this.tenureObj = {};
            this.errors = false;
            $('.has-error').removeClass('has-error');
        },
        sendData: function () {
            let vm = this;
            vm.errors = false;
            let tenureObj = JSON.parse(JSON.stringify(vm.tenureObj));

            if (vm.tenureObj.id) {
                vm.updatingEntry = true;
                const data = { data: tenureObj };

                const url =
                    helpers.add_endpoint_join(vm.url, tenureObj.id) + '/';
                const payload = {
                    method: 'PUT',
                    body: JSON.stringify(data),
                    headers: {
                        'X-CSRFToken': helpers.getCookie('csrftoken'),
                        'Content-Type': 'application/json',
                    },
                };
                fetch(url, payload)
                    .then(async (response) => {
                        const data = await response.json();
                        if (!response.ok) {
                            vm.errorString = data;
                            return;
                        }
                        vm.$emit('refreshFromResponse', data);
                        vm.close();
                    })
                    .finally(() => {
                        vm.updatingEntry = false;
                    });
            }
        },
        isAlwaysReadOnly(fieldName) {
            return this.alwaysReadOnly.includes(fieldName);
        },
    },
};
</script>
