<template lang="html">
    <div id="threat_detail">
        <modal
            transition="modal fade"
            :title="title"
            large
            :data-loss-warning-on-cancel="threat_action !== 'view'"
            @ok="ok()"
        >
            <div class="container-fluid">
                <div class="row">
                    <form class="form-horizontal" name="threatForm">
                        <alert v-if="errorString" type="danger"
                            ><strong>{{ errorString }}</strong></alert
                        >
                        <alert
                            v-if="change_warning && !isReadOnly"
                            type="warning"
                            ><strong>{{ change_warning }}</strong>
                        </alert>
                        <div class="col-sm-12">
                            <div class="form-group">
                                <div class="row mb-3">
                                    <div class="col-sm-3">
                                        <label class="control-label fw-bold"
                                            >Category:
                                            <span class="text-danger"
                                                >*</span
                                            ></label
                                        >
                                    </div>
                                    <div class="col-sm-9">
                                        <template v-if="!isReadOnly">
                                            <template
                                                v-if="
                                                    threat_category_list &&
                                                    threat_category_list.length >
                                                        0 &&
                                                    threatObj.threat_category_id &&
                                                    !threat_category_list
                                                        .map((d) => d.id)
                                                        .includes(
                                                            threatObj.threat_category_id
                                                        )
                                                "
                                            >
                                                <input
                                                    v-if="
                                                        threatObj.threat_category
                                                    "
                                                    type="text"
                                                    class="form-control mb-3"
                                                    :value="
                                                        threatObj.threat_category +
                                                        ' (Now Archived)'
                                                    "
                                                    disabled
                                                />
                                                <div class="mb-3 text-muted">
                                                    Change threat category to:
                                                </div>
                                            </template>
                                            <select
                                                v-model="
                                                    threatObj.threat_category_id
                                                "
                                                class="form-select"
                                            >
                                                <option
                                                    v-for="category in threat_category_list"
                                                    :key="category.id"
                                                    :value="category.id"
                                                >
                                                    {{ category.name }}
                                                </option>
                                            </select>
                                        </template>
                                        <template v-else>
                                            <input
                                                v-model="
                                                    threatObj.threat_category
                                                "
                                                type="text"
                                                class="form-control"
                                                disabled
                                            />
                                        </template>
                                    </div>
                                </div>
                                <div class="row mb-3">
                                    <div class="col-sm-3">
                                        <label class="control-label"
                                            >Threat Agent:</label
                                        >
                                    </div>
                                    <div class="col-sm-9">
                                        <template v-if="!isReadOnly">
                                            <template
                                                v-if="
                                                    threat_agent_list &&
                                                    threat_agent_list.length >
                                                        0 &&
                                                    threatObj.threat_agent_id &&
                                                    !threat_agent_list
                                                        .map((d) => d.id)
                                                        .includes(
                                                            threatObj.threat_agent_id
                                                        )
                                                "
                                            >
                                                <input
                                                    v-if="
                                                        threatObj.threat_agent
                                                    "
                                                    type="text"
                                                    class="form-control mb-3"
                                                    :value="
                                                        threatObj.threat_agent +
                                                        ' (Now Archived)'
                                                    "
                                                    disabled
                                                />
                                                <div class="mb-3 text-muted">
                                                    Change threat agent to:
                                                </div>
                                            </template>
                                            <select
                                                v-model="
                                                    threatObj.threat_agent_id
                                                "
                                                class="form-select"
                                            >
                                                <option
                                                    v-for="agent in threat_agent_list"
                                                    :key="agent.id"
                                                    :value="agent.id"
                                                >
                                                    {{ agent.name }}
                                                </option>
                                            </select>
                                        </template>
                                        <template v-else>
                                            <input
                                                v-model="threatObj.threat_agent"
                                                type="text"
                                                class="form-control"
                                                disabled
                                            />
                                        </template>
                                    </div>
                                </div>
                                <div class="row mb-3">
                                    <div class="col-sm-3">
                                        <label class="control-label"
                                            >Threat Comments:</label
                                        >
                                    </div>
                                    <div class="col-sm-9">
                                        <textarea
                                            v-model="threatObj.comment"
                                            :disabled="isReadOnly"
                                            class="form-control"
                                        ></textarea>
                                    </div>
                                </div>
                                <div class="row mb-3">
                                    <div class="col-sm-3">
                                        <label class="control-label fw-bold"
                                            >Current Impact:
                                            <span class="text-danger"
                                                >*</span
                                            ></label
                                        >
                                    </div>
                                    <div class="col-sm-9">
                                        <template v-if="!isReadOnly">
                                            <template
                                                v-if="
                                                    current_impact_list &&
                                                    current_impact_list.length >
                                                        0 &&
                                                    threatObj.current_impact &&
                                                    !current_impact_list
                                                        .map((d) => d.id)
                                                        .includes(
                                                            threatObj.current_impact
                                                        )
                                                "
                                            >
                                                <input
                                                    v-if="
                                                        threatObj.current_impact_name
                                                    "
                                                    type="text"
                                                    class="form-control mb-3"
                                                    :value="
                                                        threatObj.current_impact_name +
                                                        ' (Now Archived)'
                                                    "
                                                    disabled
                                                />
                                                <div class="mb-3 text-muted">
                                                    Change current impact to:
                                                </div>
                                            </template>
                                            <template
                                                v-if="
                                                    current_impact_list &&
                                                    current_impact_list.length >
                                                        0
                                                "
                                            >
                                                <div
                                                    v-for="current_impact in current_impact_list"
                                                    :key="current_impact.id"
                                                    class="form-check form-check-inline"
                                                >
                                                    <input
                                                        :id="
                                                            'current_impact_' +
                                                            current_impact.id
                                                        "
                                                        :key="current_impact.id"
                                                        v-model="
                                                            threatObj.current_impact
                                                        "
                                                        :disabled="isReadOnly"
                                                        type="radio"
                                                        class="form-check-input"
                                                        :value="
                                                            current_impact.id
                                                        "
                                                    />
                                                    <label
                                                        :for="
                                                            'current_impact_' +
                                                            current_impact.id
                                                        "
                                                        >{{
                                                            current_impact.name
                                                        }}</label
                                                    >
                                                </div>
                                            </template>
                                            <template v-else>
                                                <div>
                                                    There are no current impact
                                                    options available
                                                </div>
                                            </template>
                                        </template>
                                        <template v-else>
                                            <input
                                                v-model="
                                                    threatObj.current_impact_name
                                                "
                                                class="form-control"
                                                type="text"
                                                :disabled="isReadOnly"
                                            />
                                        </template>
                                    </div>
                                </div>
                                <div class="row mb-3">
                                    <div class="col-sm-3">
                                        <label class="control-label"
                                            >Potential Impact:</label
                                        >
                                    </div>
                                    <div class="col-sm-9">
                                        <template v-if="!isReadOnly">
                                            <template
                                                v-if="
                                                    potential_impact_list &&
                                                    potential_impact_list.length >
                                                        0 &&
                                                    threatObj.potential_impact &&
                                                    !potential_impact_list
                                                        .map((d) => d.id)
                                                        .includes(
                                                            threatObj.potential_impact
                                                        )
                                                "
                                            >
                                                <input
                                                    v-if="
                                                        threatObj.potential_impact_name
                                                    "
                                                    type="text"
                                                    class="form-control mb-3"
                                                    :value="
                                                        threatObj.potential_impact_name +
                                                        ' (Now Archived)'
                                                    "
                                                    disabled
                                                />
                                                <div class="mb-3 text-muted">
                                                    Change potential impact to:
                                                </div>
                                            </template>
                                            <template
                                                v-if="
                                                    potential_impact_list &&
                                                    potential_impact_list.length >
                                                        0
                                                "
                                            >
                                                <div
                                                    v-for="potential_impact in potential_impact_list"
                                                    :key="potential_impact.id"
                                                    class="form-check form-check-inline"
                                                >
                                                    <input
                                                        :id="
                                                            'potential_impact_' +
                                                            potential_impact.id
                                                        "
                                                        :key="
                                                            potential_impact.id
                                                        "
                                                        v-model="
                                                            threatObj.potential_impact
                                                        "
                                                        :disabled="isReadOnly"
                                                        type="radio"
                                                        class="form-check-input"
                                                        :value="
                                                            potential_impact.id
                                                        "
                                                    />
                                                    <label
                                                        :for="
                                                            'potential_impact_' +
                                                            potential_impact.id
                                                        "
                                                        >{{
                                                            potential_impact.name
                                                        }}</label
                                                    >
                                                </div>
                                            </template>
                                            <template v-else>
                                                <div>
                                                    There are no potential
                                                    impact options available
                                                </div>
                                            </template>
                                        </template>
                                        <template v-else>
                                            <input
                                                v-model="
                                                    threatObj.potential_impact_name
                                                "
                                                class="form-control"
                                                type="text"
                                                :disabled="isReadOnly"
                                            />
                                        </template>
                                    </div>
                                </div>
                                <div class="row mb-3">
                                    <div class="col-sm-3">
                                        <label class="control-label"
                                            >Potential Threat Onset:</label
                                        >
                                    </div>
                                    <div class="col-sm-9">
                                        <template v-if="!isReadOnly">
                                            <template
                                                v-if="
                                                    potential_threat_onset_list &&
                                                    potential_threat_onset_list.length >
                                                        0 &&
                                                    threatObj.potential_threat_onset &&
                                                    !potential_threat_onset_list
                                                        .map((d) => d.id)
                                                        .includes(
                                                            threatObj.potential_threat_onset
                                                        )
                                                "
                                            >
                                                <input
                                                    v-if="
                                                        threatObj.potential_threat_onset_name
                                                    "
                                                    type="text"
                                                    class="form-control mb-3"
                                                    :value="
                                                        threatObj.potential_threat_onset_name +
                                                        ' (Now Archived)'
                                                    "
                                                    disabled
                                                />
                                                <div class="mb-3 text-muted">
                                                    Change potential threat
                                                    onset to:
                                                </div>
                                            </template>
                                            <template
                                                v-if="
                                                    potential_threat_onset_list &&
                                                    potential_threat_onset_list.length >
                                                        0
                                                "
                                            >
                                                <div
                                                    v-for="potential_threat_onset in potential_threat_onset_list"
                                                    :key="
                                                        potential_threat_onset.id
                                                    "
                                                    class="form-check form-check-inline"
                                                >
                                                    <input
                                                        :id="
                                                            'potential_threat_onset_' +
                                                            potential_threat_onset.id
                                                        "
                                                        :key="
                                                            potential_threat_onset.id
                                                        "
                                                        v-model="
                                                            threatObj.potential_threat_onset
                                                        "
                                                        :disabled="isReadOnly"
                                                        type="radio"
                                                        class="form-check-input"
                                                        :value="
                                                            potential_threat_onset.id
                                                        "
                                                    />
                                                    <label
                                                        :for="
                                                            'potential_threat_onset_' +
                                                            potential_threat_onset.id
                                                        "
                                                        >{{
                                                            potential_threat_onset.name
                                                        }}</label
                                                    >
                                                </div>
                                            </template>
                                            <template v-else>
                                                <div>
                                                    There are no potential
                                                    threat onset options
                                                    available
                                                </div>
                                            </template>
                                        </template>
                                        <template v-else>
                                            <input
                                                v-model="
                                                    threatObj.potential_threat_onset_name
                                                "
                                                class="form-control"
                                                type="text"
                                                :disabled="isReadOnly"
                                            />
                                        </template>
                                    </div>
                                </div>
                                <div class="row mb-3">
                                    <div class="col-sm-3">
                                        <label class="control-label"
                                            >Threat Source:</label
                                        >
                                    </div>
                                    <div class="col-sm-9">
                                        <input
                                            v-model="threatObj.source"
                                            type="text"
                                            class="form-control"
                                            disabled
                                        />
                                    </div>
                                </div>
                                <div class="row mb-3">
                                    <div class="col-sm-3">
                                        <label
                                            for=""
                                            class="control-label fw-bold"
                                            >Date observed:
                                            <span class="text-danger">*</span>
                                        </label>
                                    </div>
                                    <div class="col-sm-9">
                                        <input
                                            ref="date_observed"
                                            v-model="threatObj.date_observed"
                                            :disabled="isReadOnly"
                                            type="date"
                                            min="1990-01-01"
                                            :max="
                                                date_observed_maximum
                                                    ? date_observed_maximum
                                                    : new Date()
                                                          .toISOString()
                                                          .split('T')[0]
                                            "
                                            class="form-control"
                                            name="date_observed"
                                            @blur="checkDate()"
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
                        {{ isReadOnly ? 'Close' : 'Cancel' }}
                    </button>
                    <template v-if="threat_action != 'view'">
                        <template v-if="threat_id">
                            <button
                                v-if="updatingThreat"
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
                                v-if="addingThreat"
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
                                Add Threat
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
    name: 'ThreatDetail',
    components: {
        modal,
        alert,
    },
    props: {
        url: {
            type: String,
            required: true,
        },
        change_warning: {
            type: String,
            required: false,
        },
        // 'YYYY-MM-DD' format
        date_observed_maximum: {
            type: String,
        },
    },
    data: function () {
        return {
            isModalOpen: false,
            form: null,
            threat_id: String,
            threat_action: String,
            threatObj: Object,
            threat_category_list: [],
            threat_agent_list: [],
            current_impact_list: [],
            potential_impact_list: [],
            potential_threat_onset_list: [],
            addingThreat: false,
            updatingThreat: false,
            errorString: '',
            originalThreatObj: null,
        };
    },
    computed: {
        title: function () {
            var action = this.threat_action;
            if (typeof action === 'string' && action.length > 0) {
                var capitalizedAction =
                    action.charAt(0).toUpperCase() + action.slice(1);
                return capitalizedAction + ' Threat';
            } else {
                return 'Invalid threat action'; // Or handle the error in an appropriate way
            }
        },
        isReadOnly: function () {
            return this.threat_action === 'view' ? true : false;
        },
    },
    watch: {
        isModalOpen: function (val) {
            if (val) {
                this.$nextTick(() => {
                    this.originalThreatObj = JSON.parse(
                        JSON.stringify(this.threatObj)
                    );
                });
            }
        },
    },
    created: async function () {
        let response = await fetch('/api/threat/threat_list_of_values/');
        let threat_list_of_values_res = {};
        const data = await response.json();
        Object.assign(threat_list_of_values_res, data);
        this.threat_category_list =
            threat_list_of_values_res.active_threat_category_lists;
        this.threat_category_list.splice(0, 0, {
            id: null,
            name: null,
        });
        this.current_impact_list =
            threat_list_of_values_res.active_current_impact_lists;
        this.potential_impact_list =
            threat_list_of_values_res.active_potential_impact_lists;
        this.potential_threat_onset_list =
            threat_list_of_values_res.potential_threat_onset_lists; // Returns only active potential threat onsets
        this.threat_agent_list = threat_list_of_values_res.threat_agent_lists;
        this.threat_agent_list.splice(0, 0, {
            id: null,
            name: null,
        });
    },
    mounted: function () {
        let vm = this;
        vm.form = document.forms.threatForm;
    },
    methods: {
        hasUnsavedChanges: function () {
            return (
                JSON.stringify(this.threatObj) !==
                JSON.stringify(this.originalThreatObj)
            );
        },
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
            if (!this.hasUnsavedChanges()) {
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
            this.threatObj = {};
            this.errorString = '';
            $('.has-error').removeClass('has-error');
        },
        sendData: function () {
            let vm = this;
            vm.errorString = '';
            vm.threatObj.date_observed =
                vm.threatObj.date_observed == ''
                    ? null
                    : vm.threatObj.date_observed;
            let threatObj = JSON.parse(JSON.stringify(vm.threatObj));
            let formData = new FormData();

            if (vm.threatObj.id) {
                vm.updatingThreat = true;
                formData.append('data', JSON.stringify(threatObj));
                fetch(helpers.add_endpoint_json(vm.url, threatObj.id), {
                    method: 'PUT',
                    body: formData,
                })
                    .then(async (response) => {
                        let data = await response.json();
                        if (!response.ok) {
                            vm.errorString = data;
                            return;
                        }
                        vm.$parent.updatedThreats();
                        vm.close();
                    })
                    .finally(() => {
                        vm.updatingThreat = false;
                    });
            } else {
                vm.addingThreat = true;
                formData.append('data', JSON.stringify(threatObj));
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
                        vm.close();
                        vm.$parent.updatedThreats();
                    })
                    .finally(() => {
                        vm.addingThreat = false;
                    });
            }
        },
        checkDate: function () {
            if (this.threatObj.date_observed === '') {
                this.threatObj.date_observed = null;
                return;
            }
            if (
                this.threatObj.observation_date === null ||
                isNaN(new Date(this.threatObj.date_observed))
            ) {
                return;
            }
            if (new Date(this.threatObj.date_observed) > new Date()) {
                this.threatObj.date_observed = new Date()
                    .toISOString()
                    .split('T')[0];
                this.$nextTick(() => {
                    this.$refs.last_data_curation_date.focus();
                });
                swal.fire({
                    title: 'Error',
                    text: 'Date observed cannot be in the future',
                    icon: 'error',
                    customClass: {
                        confirmButton: 'btn btn-primary',
                    },
                });
            }
            if (this.date_observed_maximum) {
                if (
                    new Date(this.threatObj.date_observed) >
                    new Date(new Date(this.date_observed_maximum))
                ) {
                    this.threatObj.date_observed = new Date(
                        this.date_observed_maximum
                    )
                        .toISOString()
                        .split('T')[0];
                    this.$nextTick(() => {
                        this.$refs.date_observed.focus();
                    });
                    swal.fire({
                        title: 'Error',
                        text:
                            'Date observed cannot be after ' +
                            new Date(
                                this.date_observed_maximum
                            ).toLocaleDateString('en-AU'),
                        icon: 'error',
                        customClass: {
                            confirmButton: 'btn btn-primary',
                        },
                    });
                    return;
                }
            } else if (
                new Date(this.threatObj.date_observed) < new Date('1990-01-01')
            ) {
                this.threatObj.date_observed = new Date('1990-01-01')
                    .toISOString()
                    .split('T')[0];
                this.$nextTick(() => {
                    this.$refs.date_observed.focus();
                });
                swal.fire({
                    title: 'Error',
                    text:
                        'Date observed cannot be before ' +
                        new Date(this.date_observed_maximum).toLocaleDateString(
                            'en-AU'
                        ),
                    icon: 'error',
                    customClass: {
                        confirmButton: 'btn btn-primary',
                    },
                });
            }
        },
    },
};
</script>
