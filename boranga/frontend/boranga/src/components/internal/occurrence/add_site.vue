<template lang="html">
    <div id="site_detail">
        <modal
            transition="modal fade"
            :title="title"
            large
            :data-loss-warning-on-cancel="!isReadOnly"
            @ok="ok()"
            @cancel="cancel()"
        >
            <div class="container-fluid">
                <div class="row">
                    <form class="form-horizontal" name="siteForm">
                        <alert v-if="errorString" type="danger"
                            ><strong>{{ errorString }}</strong></alert
                        >
                        <alert
                            v-if="change_warning && !isReadOnly"
                            type="warning"
                            ><strong>{{ change_warning }}</strong>
                        </alert>
                        <div class="col-sm-12">
                            <div class="row mb-3">
                                <div class="col-sm-3">
                                    <label class="control-label"
                                        >Site Name</label
                                    >
                                </div>
                                <div class="col-sm-9">
                                    <input
                                        ref="site_name"
                                        v-model="siteObj.site_name"
                                        :disabled="isReadOnly"
                                        type="text"
                                        class="form-control"
                                    />
                                </div>
                            </div>
                            <div class="row mb-3">
                                <label for="" class="col-sm-3 control-label"
                                    >Point Coordinate</label
                                >
                                <div class="col-sm-9">
                                    <div class="input-group">
                                        <span class="input-group-text"
                                            >Latitude</span
                                        >
                                        <input
                                            id="point_coord2"
                                            v-model="siteObj.point_coord2"
                                            :disabled="isReadOnly"
                                            type="number"
                                            class="form-control"
                                            placeholder=""
                                            :max="-10.000001"
                                            :min="-34.999999"
                                            step="0.000001"
                                        />
                                        <span class="input-group-text"
                                            >Longitude</span
                                        >
                                        <input
                                            id="point_coord1"
                                            v-model="siteObj.point_coord1"
                                            :disabled="isReadOnly"
                                            type="number"
                                            :min="96.000001"
                                            :max="128.999999"
                                            step="0.000001"
                                            class="form-control"
                                            placeholder=""
                                        />
                                    </div>
                                </div>
                            </div>
                            <div class="row mb-3">
                                <div class="col-sm-3">
                                    <label class="control-label">Datum</label>
                                </div>
                                <div class="col-sm-9">
                                    <template v-if="!isReadOnly">
                                        <template
                                            v-if="
                                                datum_list &&
                                                datum_list.length > 0 &&
                                                siteObj.datum &&
                                                !datum_list
                                                    .map((d) => d.srid)
                                                    .includes(siteObj.datum)
                                            "
                                        >
                                            <input
                                                v-if="siteObj.datum_name"
                                                type="text"
                                                class="form-control mb-3"
                                                :value="
                                                    siteObj.datum_name +
                                                    ' (Now Archived)'
                                                "
                                                disabled
                                            />
                                            <div class="mb-3 text-muted">
                                                Change datum to:
                                            </div>
                                        </template>
                                        <select
                                            v-model="siteObj.datum"
                                            class="form-select"
                                        >
                                            <option
                                                v-for="datum in datum_list"
                                                :key="datum.srid"
                                                :value="datum.srid"
                                            >
                                                {{ datum.name }}
                                            </option>
                                        </select>
                                    </template>
                                    <template v-else>
                                        <input
                                            v-model="siteObj.datum_name"
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
                                        >Site Type</label
                                    >
                                </div>
                                <div class="col-sm-9">
                                    <template v-if="!isReadOnly">
                                        <template
                                            v-if="
                                                site_type_list &&
                                                site_type_list.length > 0 &&
                                                siteObj.site_type &&
                                                !site_type_list
                                                    .map((d) => d.id)
                                                    .includes(siteObj.site_type)
                                            "
                                        >
                                            <input
                                                v-if="siteObj.site_type_name"
                                                type="text"
                                                class="form-control mb-3"
                                                :value="
                                                    siteObj.site_type_name +
                                                    ' (Now Archived)'
                                                "
                                                disabled
                                            />
                                            <div class="mb-3 text-muted">
                                                Change site type to:
                                            </div>
                                        </template>
                                        <select
                                            v-model="siteObj.site_type"
                                            class="form-select"
                                        >
                                            <option
                                                v-for="site_type in site_type_list"
                                                :key="site_type.id"
                                                :value="site_type.id"
                                            >
                                                {{ site_type.name }}
                                            </option>
                                        </select>
                                    </template>
                                    <template v-else>
                                        <input
                                            v-model="siteObj.site_type_name"
                                            class="form-control"
                                            type="text"
                                            :disabled="isReadOnly"
                                        />
                                    </template>
                                </div>
                            </div>
                            <div class="form-group">
                                <div class="row mb-3">
                                    <div class="col-sm-3">
                                        <label class="control-label"
                                            >Occurrence Reports</label
                                        >
                                    </div>
                                    <div class="col-sm-9">
                                        <div
                                            id="select_occurrence_reports"
                                            class="form-group"
                                        >
                                            <select
                                                ref="occurrence_report_select"
                                                v-model="
                                                    siteObj.related_occurrence_reports
                                                "
                                                :disabled="isReadOnly"
                                                style="width: 100%"
                                                class="form-select input-sm"
                                            >
                                                <option
                                                    v-for="option in occurrence_obj.occurrence_reports"
                                                    :key="option.id"
                                                    :value="option.id"
                                                >
                                                    {{
                                                        option.occurrence_report_number
                                                    }}
                                                </option>
                                            </select>
                                        </div>
                                    </div>
                                </div>
                            </div>
                            <div class="form-group">
                                <div class="row mb-3">
                                    <div class="col-sm-3">
                                        <label class="control-label"
                                            >Comments</label
                                        >
                                    </div>
                                    <div class="col-sm-9">
                                        <textarea
                                            v-model="siteObj.comments"
                                            :disabled="isReadOnly"
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
                        {{ isReadOnly ? 'Close' : 'Cancel' }}
                    </button>
                    <template v-if="site_action != 'view'">
                        <template v-if="site_id">
                            <button
                                v-if="updatingSite"
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
                                v-if="addingSite"
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
                                :disabled="!isFormValid"
                                @click="ok"
                            >
                                Add Site
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
    name: 'SiteDetail',
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
        occurrence_obj: {
            type: Object,
            required: false,
        },
    },
    data: function () {
        return {
            isModalOpen: false,
            form: null,
            site_id: String,
            site_action: String,
            siteObj: {
                related_occurrence_reports: [],
            },
            addingSite: false,
            updatingSite: false,
            validation_form: null,
            type: '1',
            errorString: '',
            successString: '',
            success: false,
            site_type_list: [],
            datum_list: [],
        };
    },
    computed: {
        title: function () {
            var action = this.site_action;
            if (typeof action === 'string' && action.length > 0) {
                var capitalizedAction =
                    action.charAt(0).toUpperCase() + action.slice(1);
                return capitalizedAction + ' Site';
            } else {
                return 'Invalid site action'; // Or handle the error in an appropriate way
            }
        },
        isReadOnly: function () {
            return this.site_action === 'view' ? true : false;
        },
        isFormValid: function () {
            // form is assigned in mounted()
            if (!this.form) return false;
            try {
                return $(this.form).valid();
            } catch {
                return false;
            }
        },
    },
    watch: {
        isModalOpen: function (newVal) {
            if (newVal) {
                this.$nextTick(() => {
                    this.$refs.site_name.focus();
                });
            }
        },
        siteObj: function () {
            let vm = this;
            vm.reinitialiseOCRLookup();
        },
    },
    created: async function () {
        let response = await fetch(
            '/api/occurrence_sites/site_list_of_values/'
        );
        const data = await response.json();
        let site_list_of_values_res = {};
        Object.assign(site_list_of_values_res, data);
        this.site_type_list = site_list_of_values_res.site_type_list;
        this.site_type_list.splice(0, 0, {
            id: null,
            name: null,
        });
        this.datum_list = site_list_of_values_res.datum_list;
        this.datum_list.splice(0, 0, {
            srid: null,
            name: null,
        });
    },
    mounted: function () {
        let vm = this;
        vm.form = document.forms.siteForm;

        this.$nextTick(() => {
            vm.initialiseOCRSelect();
        });
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
            this.siteObj = {
                related_occurrence_reports: [],
            };
            this.errorString = '';
        },
        reinitialiseOCRLookup: function () {
            let vm = this;
            vm.$nextTick(() => {
                $(vm.$refs.occurrence_report_select).select2('destroy');
                vm.initialiseOCRSelect();
            });
        },
        initialiseOCRSelect: function () {
            let vm = this;
            // Initialise select2 for proposed Conservation Criteria
            $(vm.$refs.occurrence_report_select)
                .select2({
                    theme: 'bootstrap-5',
                    dropdownParent: $('#select_occurrence_reports'),
                    allowClear: true,
                    multiple: true,
                    placeholder: 'Select Occurrence Report',
                })
                .on('select2:select', function (e) {
                    var selected = $(e.currentTarget);
                    vm.siteObj.related_occurrence_reports = selected.val();
                })
                .on('select2:unselect', function (e) {
                    var selected = $(e.currentTarget);
                    vm.siteObj.related_occurrence_reports = selected.val();
                });
        },
        prepareNewSiteAtCoordinates: function (coordinates) {
            this.siteObj.point_coord1 = coordinates[0];
            this.siteObj.point_coord2 = coordinates[1];
            this.siteObj.related_occurrence_reports = [];
        },
        sendData: function () {
            let vm = this;
            vm.errorString = '';
            let siteObj = JSON.parse(JSON.stringify(vm.siteObj));
            let formData = new FormData();

            if (vm.siteObj.id) {
                vm.updatingSite = true;
                formData.append('data', JSON.stringify(siteObj));
                fetch(helpers.add_endpoint_json(vm.url, siteObj.id), {
                    method: 'PUT',
                    body: formData,
                })
                    .then(async (response) => {
                        const data = await response.json();
                        if (!response.ok) {
                            vm.errorString = data;
                            return;
                        }
                        vm.$parent.updatedSites();
                        vm.close();
                    })
                    .finally(() => {
                        vm.updatingSite = false;
                    });
            } else {
                vm.addingSite = true;
                formData.append('data', JSON.stringify(siteObj));
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
                        vm.$parent.updatedSites();
                        vm.close();
                    })
                    .finally(() => {
                        vm.addingSite = false;
                    });
            }
        },
    },
};
</script>
