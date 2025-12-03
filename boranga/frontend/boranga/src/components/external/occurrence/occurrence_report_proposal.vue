<template lang="html">
    <div class="container">
        <form
            :action="ocr_proposal_form_url"
            method="post"
            name="new_ocr_proposal"
            enctype="multipart/form-data"
        >
            <div v-if="!ocr_proposal_readonly">
                <div v-if="hasAmendmentRequest" class="row">
                    <div class="col-lg-12 pull-right">
                        <FormSection
                            :form-collapse="false"
                            label="One or more amendments have been requested for this Occurrence Report"
                            Index="amendment_request"
                            custom-color="red"
                        >
                            <ul class="list-group list-group-numbered ps-2">
                                <li
                                    v-for="a in amendment_request"
                                    :key="a.id"
                                    class="list-group-item d-flex justify-content-between align-items-start"
                                >
                                    <div class="ms-4 me-auto">
                                        <div class="mb-3">
                                            <span class="fw-bold">Reason:</span>
                                            {{ a.reason_text }}
                                        </div>
                                        <p
                                            v-for="t in splitText(a.text)"
                                            :key="t"
                                        >
                                            {{ t }}
                                        </p>
                                        <template
                                            v-if="
                                                a.amendment_request_documents &&
                                                a.amendment_request_documents
                                                    .length > 0
                                            "
                                        >
                                            <div class="fw-bold mb-1">
                                                Documents:
                                            </div>
                                            <ul
                                                class="list-group list-group-numbered mb-2"
                                            >
                                                <li
                                                    v-for="document in a.amendment_request_documents"
                                                    :key="document.id"
                                                    class="list-group-item"
                                                >
                                                    <i
                                                        class="bi bi-file-earmark-text-fill text-secondary"
                                                    ></i>
                                                    <a
                                                        :href="document._file"
                                                        target="_blank"
                                                        >{{ document.name }}</a
                                                    >
                                                </li>
                                            </ul>
                                        </template>
                                    </div>
                                </li>
                            </ul>
                        </FormSection>
                    </div>
                </div>
            </div>

            <div
                v-if="occurrence_report_obj"
                id="scrollspy-heading"
                class="col-lg-12"
            >
                <h3>
                    Occurrence Report Form - {{ display_group_type }}:
                    {{ occurrence_report_obj.occurrence_report_number }}
                </h3>
            </div>

            <ProposalOccurrenceReport
                v-if="occurrence_report_obj"
                id="OccurrenceReportStart"
                ref="occurrence_report"
                :occurrence_report_obj="occurrence_report_obj"
                :can-edit-status="canEditStatus"
                :is_external="true"
                @refresh-occurrence-report="refreshOccurrenceReport()"
                @refresh-from-response="refreshFromResponse"
                @save-occurrence-report="save_before_submit()"
                @dirty="isDirty = $event"
            >
            </ProposalOccurrenceReport>

            <div>
                <input
                    type="hidden"
                    name="csrfmiddlewaretoken"
                    :value="csrf_token"
                />
                <input type="hidden" name="occurrence_report_id" :value="1" />
                <div class="row" style="margin-bottom: 50px">
                    <div class="container">
                        <div class="row" style="margin-bottom: 50px">
                            <div
                                class="navbar fixed-bottom"
                                style="background-color: #f5f5f5"
                            >
                                <div class="container">
                                    <div
                                        class="col-md-6"
                                        style="margin-top: 5px"
                                    >
                                        <p
                                            class="pull-right"
                                            style="margin-top: 5px"
                                        >
                                            <button
                                                class="btn btn-primary"
                                                @click.prevent="backToDashboard"
                                            >
                                                Back to Dashboard
                                            </button>
                                        </p>
                                    </div>
                                    <div
                                        v-if="
                                            occurrence_report_obj &&
                                            !occurrence_report_obj.readonly
                                        "
                                        class="col-md-6 text-end"
                                        style="margin-top: 5px"
                                    >
                                        <button
                                            v-if="savingOCRProposal"
                                            type="button"
                                            class="btn btn-primary me-2"
                                            disabled
                                        >
                                            Save and Continue
                                            <span
                                                class="spinner-border spinner-border-sm"
                                                role="status"
                                                aria-hidden="true"
                                            ></span>
                                            <span class="visually-hidden"
                                                >Loading...</span
                                            >
                                        </button>
                                        <input
                                            v-else
                                            type="button"
                                            class="btn btn-primary me-2"
                                            value="Save and Continue"
                                            :disabled="
                                                saveExitOCRProposal ||
                                                submitting
                                            "
                                            @click.prevent="save"
                                        />

                                        <button
                                            v-if="saveExitOCRProposal"
                                            type="button"
                                            class="btn btn-primary me-2"
                                            disabled
                                        >
                                            Save and Exit
                                            <span
                                                class="spinner-border spinner-border-sm"
                                                role="status"
                                                aria-hidden="true"
                                            ></span>
                                            <span class="visually-hidden"
                                                >Loading...</span
                                            >
                                        </button>
                                        <input
                                            v-else
                                            type="button"
                                            class="btn btn-primary me-2"
                                            value="Save and Exit"
                                            :disabled="
                                                savingOCRProposal || submitting
                                            "
                                            @click.prevent="save_exit"
                                        />

                                        <button
                                            v-if="submitting"
                                            type="button"
                                            class="btn btn-primary"
                                            disabled
                                        >
                                            {{ submit_text() }}
                                            <span
                                                class="spinner-border spinner-border-sm"
                                                role="status"
                                                aria-hidden="true"
                                            ></span>
                                            <span class="visually-hidden"
                                                >Loading...</span
                                            >
                                        </button>
                                        <input
                                            v-else
                                            type="button"
                                            class="btn btn-primary"
                                            :value="submit_text()"
                                            :disabled="
                                                saveExitOCRProposal ||
                                                savingOCRProposal
                                            "
                                            @click.prevent="submit"
                                        />
                                        <input
                                            id="save_and_continue_btn"
                                            type="hidden"
                                            class="btn btn-primary"
                                            value="Save Without Confirmation"
                                            @click.prevent="save_wo_confirm"
                                        />
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </form>
    </div>
</template>
<script>
import ProposalOccurrenceReport from '@/components/form_occurrence_report.vue';
import FormSection from '@/components/forms/section_toggle.vue';
import { api_endpoints, helpers } from '@/utils/hooks';
export default {
    name: 'ExternalOccurrenceReportProposal',
    components: {
        ProposalOccurrenceReport,
        FormSection,
    },
    beforeRouteEnter: function (to, from, next) {
        if (to.params.occurrence_report_id) {
            fetch(
                `/api/occurrence_report/${to.params.occurrence_report_id}.json`
            ).then(
                async (response) => {
                    next(async (vm) => {
                        const data = await response.json();
                        vm.occurrence_report_obj = data;
                        vm.setdata(vm.occurrence_report_obj.readonly);

                        // fetch any amendment requests for this occurrence report
                        fetch(
                            helpers.add_endpoint_json(
                                api_endpoints.occurrence_report,
                                to.params.occurrence_report_id +
                                    '/amendment_request'
                            )
                        ).then(
                            async (response) => {
                                const amendData = await response.json();
                                vm.setAmendmentData(amendData);
                            },
                            (err) => {
                                console.log(err);
                            }
                        );
                    });
                },
                (err) => {
                    console.log(err);
                }
            );
        } else {
            fetch('/api/occurrence_report.json', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
            }).then(
                async (response) => {
                    next(async (vm) => {
                        vm.occurrence_report_obj = await response.json();
                    });
                },
                (err) => {
                    console.log(err);
                }
            );
        }
    },
    data: function () {
        return {
            proposal: null,
            occurrence_report_obj: null,
            loading: [],
            form: null,
            amendment_request: [],
            saveError: false,
            ocr_proposal_readonly: true,
            hasAmendmentRequest: false,
            submitting: false,
            saveExitOCRProposal: false,
            savingOCRProposal: false,
            paySubmitting: false,
            newText: '',
            pBody: 'pBody',
            missing_fields: [],
            isDirty: false,
            isSaved: false,
        };
    },
    computed: {
        isLoading: function () {
            return this.loading.length > 0;
        },
        csrf_token: function () {
            return helpers.getCookie('csrftoken');
        },
        ocr_proposal_form_url: function () {
            return this.occurrence_report_obj
                ? `/api/occurrence_report/${this.occurrence_report_obj.id}/draft.json`
                : '';
        },
        canEditStatus: function () {
            return this.occurrence_report_obj
                ? this.occurrence_report_obj.can_user_edit
                : 'false';
        },
        display_group_type: function () {
            let group_type_string = this.occurrence_report_obj.group_type;
            // to Capitalize only first character
            return (
                group_type_string.charAt(0).toUpperCase() +
                group_type_string.slice(1)
            );
        },
    },
    created: function () {
        if (!this.occurrence_report_obj) {
            this.fetchOccurrenceReport(this.$route.params.occurrence_report_id);
        }
    },
    mounted: function () {
        let vm = this;
        vm.form = document.forms.new_ocr_proposal;
        window.addEventListener('beforeunload', vm.leaving);
    },
    beforeUnmount: function () {
        window.removeEventListener('beforeunload', this.leaving);
    },
    methods: {
        submit_text: function () {
            return 'Submit';
        },
        set_formData: function () {
            let vm = this;
            let formData = new FormData(vm.form);
            return formData;
        },
        save: async function () {
            let vm = this;
            var missing_data = await vm.can_submit('');
            if (missing_data != true) {
                swal.fire({
                    title: 'Please fix following errors before saving',
                    text: missing_data,
                    icon: 'error',
                    customClass: {
                        confirmButton: 'btn btn-primary',
                    },
                });
                return false;
            }
            vm.savingOCRProposal = true;
            vm.isSaved = false;
            // add map geometry to the occurrence_report_obj
            if (
                vm.$refs.occurrence_report.$refs.ocr_location.$refs
                    .component_map
            ) {
                vm.occurrence_report_obj.ocr_geometry =
                    vm.$refs.occurrence_report.$refs.ocr_location.$refs.component_map.getJSONFeatures();
            }
            let payload = { proposal: vm.occurrence_report_obj };

            let deleted_features =
                this.$refs.occurrence_report.$refs.ocr_location.$refs.component_map.deletedFeaturesProperty();
            // Save right away if there are no deleted features, otherwise ask for confirmation
            let commence_saving = deleted_features.length == 0 ? true : false;

            let warning_text = `${deleted_features.length} ${
                deleted_features.length == 1 ? 'feature' : 'features'
            } will be deleted. Are you sure?`;
            if (deleted_features.length > 0) {
                await swal
                    .fire({
                        title: 'Save Report',
                        text: warning_text,
                        icon: 'warning',
                        showCancelButton: true,
                        confirmButtonText: 'Continue',
                    })
                    .then(async (result) => {
                        if (result.isConfirmed) {
                            // When Yes
                            commence_saving = true;
                        }
                    });
            }

            if (!commence_saving) {
                vm.savingOCRProposal = false;
                return;
            }

            if (vm.submitting) {
                // Provide an action to have the backend lock the geometry
                payload.action = 'submit';
            }
            await fetch(vm.ocr_proposal_form_url, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(payload),
            }).then(async (response) => {
                const data = await response.json();
                if (!response.ok) {
                    swal.fire({
                        title: 'Save Error',
                        text: JSON.stringify(data),
                        icon: 'error',
                        customClass: {
                            confirmButton: 'btn btn-primary',
                        },
                    });
                    vm.savingOCRProposal = false;
                    vm.isSaved = false;
                    return;
                }

                let swalHtml =
                    '<p>Your occurrence report has been saved as a draft.</p>';
                if (vm.saveExitOCRProposal) {
                    swalHtml +=
                        '<p>It has <span class="fw-bold">NOT</span> been submitted.</p><p>You can find the draft on the occurrence report dashboard to continue working on the report later.</p>';
                }

                swal.fire({
                    title: 'Occurrence Report Saved',
                    html: swalHtml,
                    icon: 'success',
                    buttonsStyling: false,
                    customClass: {
                        confirmButton: 'btn btn-primary',
                    },
                });
                vm.savingOCRProposal = false;
                this.occurrence_report_obj = Object.assign({}, data);
                this.$nextTick(async () => {
                    this.$refs.occurrence_report.$refs.ocr_location.incrementComponentMapKey();
                });
                vm.isSaved = true;
                vm.$nextTick(() => {
                    vm.resetDirtyState();
                });
                return data;
            });
        },
        resetDirtyState: function () {
            this.$refs.occurrence_report.resetDirtyState();
        },
        save_exit: async function () {
            let vm = this;
            // this.submitting = true;
            var missing_data = await vm.can_submit('');
            if (missing_data != true) {
                swal.fire({
                    title: 'Please fix following errors before saving',
                    text: missing_data,
                    icon: 'error',
                    customClass: {
                        confirmButton: 'btn btn-primary',
                    },
                });
                return false;
            }
            this.saveExitOCRProposal = true;
            // this also resolves the bug of not updating the datatable when router is pushed back to dashboard
            await this.save().then(() => {
                if (vm.isSaved === true) {
                    vm.$router.push({
                        name: 'external-occurrence-report-dash',
                    });
                } else {
                    this.saveExitOCRProposal = false;
                }
            });
        },
        save_wo_confirm: function () {
            let vm = this;
            fetch(vm.ocr_proposal_form_url, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
            });
        },
        save_before_submit: async function () {
            console.log('save before submit');
            let vm = this;
            vm.saveError = false;
            // add map geometry to the occurrence_report_obj
            if (
                vm.$refs.occurrence_report.$refs.ocr_location.$refs
                    .component_map
            ) {
                vm.occurrence_report_obj.ocr_geometry =
                    vm.$refs.occurrence_report.$refs.ocr_location.$refs.component_map.getJSONFeatures();
            }
            let payload = { proposal: vm.occurrence_report_obj };
            let deleted_features =
                this.$refs.occurrence_report.$refs.ocr_location.$refs.component_map.deletedFeaturesProperty();
            // Save right away if there are no deleted features, otherwise ask for confirmation
            let commence_saving = deleted_features.length == 0 ? true : false;

            let warning_text = `${deleted_features.length} ${
                deleted_features.length == 1 ? 'feature' : 'features'
            } will be deleted. Are you sure?`;
            if (deleted_features.length > 0) {
                await swal
                    .fire({
                        title: 'Save Report',
                        text: warning_text,
                        icon: 'warning',
                        showCancelButton: true,
                        confirmButtonText: 'Continue',
                    })
                    .then(async (result) => {
                        if (result.isConfirmed) {
                            // When Yes
                            commence_saving = true;
                        }
                    });
            }

            if (!commence_saving) {
                vm.submitting = false;
                return;
            }

            if (vm.submitting) {
                // Provide an action to have the backend lock the geometry
                payload.action = 'submit';
            }
            const result = await fetch(vm.ocr_proposal_form_url, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(payload),
            }).then(async (response) => {
                let data = await response.json();
                if (!response.ok) {
                    vm.submitting = false;
                    vm.saveError = true;
                    swal.fire({
                        title: 'Save Error',
                        text: JSON.stringify(data),
                        icon: 'error',
                        customClass: {
                            confirmButton: 'btn btn-primary',
                        },
                    });
                    return;
                }
                console.log('saved before submit');
            });
            return result;
        },
        setdata: function (readonly) {
            this.ocr_proposal_readonly = readonly;
        },
        setAmendmentData: function (amendment_request) {
            this.amendment_request = amendment_request;

            if (amendment_request.length > 0) this.hasAmendmentRequest = true;
        },
        splitText: function (aText) {
            let newText = '';
            newText = aText.split('\n');
            return newText;
        },
        leaving: function (e) {
            if (this.isDirty) {
                e.preventDefault();
                e.returnValue = ''; // Required for Chrome
                // The browser will show its own confirmation dialog
            }
        },
        highlight_missing_fields: function () {
            let vm = this;
            for (var missing_field of vm.missing_fields) {
                $('#' + missing_field.id).css('color', 'red');
            }
        },
        validate: function () {
            let vm = this;

            // reset default colour
            for (var field of vm.missing_fields) {
                $('#' + field.id).css('color', '#515151');
            }
            vm.missing_fields = [];

            // get all required fields, that are not hidden in the DOM
            var required_fields = $(
                'input[type=text]:required, textarea:required, input[type=checkbox]:required, input[type=radio]:required, input[type=file]:required, select:required'
            ).not(':hidden');

            // loop through all (non-hidden) required fields, and check data has been entered
            required_fields.each(function () {
                var id = 'id_' + this.name;
                var text = null;
                if (this.type == 'radio') {
                    if (!$('input[name=' + this.name + ']').is(':checked')) {
                        text = $('#' + id).text();
                        console.log(
                            'radio not checked: ' + this.type + ' ' + text
                        );
                        vm.missing_fields.push({ id: id, label: text });
                    }
                }

                if (this.type == 'checkbox') {
                    id = 'id_' + this.classList['value'];
                    if (
                        $('[class=' + this.classList['value'] + ']:checked')
                            .length == 0
                    ) {
                        text = $('#' + id).text();
                        console.log(
                            'checkbox not checked: ' + this.type + ' ' + text
                        );
                        vm.missing_fields.push({ id: id, label: text });
                    }
                }

                if (this.type == 'select-one') {
                    if ($(this).val() == '') {
                        text = $('#' + id).text(); // this is the (question) label
                        id = 'id_' + $(this).prop('name'); // the label id
                        console.log(
                            'selector not selected: ' + this.type + ' ' + text
                        );
                        vm.missing_fields.push({ id: id, label: text });
                    }
                }

                if (this.type == 'file') {
                    var num_files = $('#' + id).attr('num_files');
                    if (num_files == '0') {
                        text = $('#' + id).text();
                        console.log(
                            'file not uploaded: ' + this.type + ' ' + this.name
                        );
                        vm.missing_fields.push({ id: id, label: text });
                    }
                }

                if (this.type == 'text') {
                    if (this.value == '') {
                        text = $('#' + id).text();
                        console.log(
                            'text not provided: ' + this.type + ' ' + this.name
                        );
                        vm.missing_fields.push({ id: id, label: text });
                    }
                }

                if (this.type == 'textarea') {
                    if (this.value == '') {
                        text = $('#' + id).text();
                        console.log(
                            'textarea not provided: ' +
                                this.type +
                                ' ' +
                                this.name
                        );
                        vm.missing_fields.push({ id: id, label: text });
                    }
                }
            });

            return vm.missing_fields.length;
        },
        can_submit: async function (check_action) {
            let vm = this;
            let blank_fields = [];
            blank_fields = await vm.can_submit_occurrence_report(check_action);

            if (blank_fields.length == 0) {
                return true;
            } else {
                return blank_fields;
            }
        },
        can_submit_occurrence_report: async function (check_action) {
            let vm = this;
            let blank_fields = [];
            if (
                vm.occurrence_report_obj.group_type == 'flora' ||
                vm.occurrence_report_obj.group_type == 'fauna'
            ) {
                if (
                    vm.occurrence_report_obj.species_id == null ||
                    vm.occurrence_report_obj.species_id == ''
                ) {
                    blank_fields.push(' Scientific Name is missing');
                }
            } else {
                if (
                    vm.occurrence_report_obj.community_id == null ||
                    vm.occurrence_report_obj.community_id == ''
                ) {
                    blank_fields.push(' Community Name is missing');
                }
            }
            if (check_action == 'submit') {
                await vm.save_before_submit();

                if (
                    !vm.occurrence_report_obj.submitter_information
                        .submitter_category
                ) {
                    blank_fields.push(' Please select a submitter category');
                }

                if (!vm.occurrence_report_obj.observation_date) {
                    blank_fields.push(' Please enter the observation date');
                }

                if (
                    !vm.occurrence_report_obj.number_of_observers ||
                    vm.occurrence_report_obj.number_of_observers == 0
                ) {
                    blank_fields.push(
                        ' Please add the details for at least one observer'
                    );
                }

                if (
                    !vm.occurrence_report_obj.location ||
                    !vm.occurrence_report_obj.location.location_description
                ) {
                    blank_fields.push(' Please enter the location description');
                }
                let ocr_geometry = vm.occurrence_report_obj.ocr_geometry;
                if (typeof ocr_geometry == 'string') {
                    ocr_geometry = JSON.parse(ocr_geometry);
                }
                if (
                    !Array.isArray(ocr_geometry.features) ||
                    ocr_geometry.features.length == 0
                ) {
                    blank_fields.push(
                        ' Please add at least one location on the map'
                    );
                }
            }
            return blank_fields;
        },
        submit: async function () {
            let vm = this;

            var missing_data = await vm.can_submit('submit');
            if (missing_data != true) {
                swal.fire({
                    title: 'Please fix following errors before submitting',
                    text: missing_data,
                    icon: 'error',
                    customClass: {
                        confirmButton: 'btn btn-primary',
                    },
                });
                return false;
            }

            // remove the confirm prompt when navigating away from window (on button 'Submit' click)
            vm.submitting = true;

            swal.fire({
                title: vm.submit_text() + ' Occurrence Report',
                text:
                    'Are you sure you want to ' +
                    vm.submit_text().toLowerCase() +
                    ' this occurrence report?',
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: vm.submit_text(),
                customClass: {
                    confirmButton: 'btn btn-primary',
                    cancelButton: 'btn btn-secondary me-2',
                },
                reverseButtons: true,
                buttonsStyling: false,
            }).then(
                async (swalresult) => {
                    if (swalresult.isConfirmed) {
                        /* just save and submit - no payment required (probably application was pushed back by assessor for amendment */
                        await vm.save_before_submit();
                        if (!vm.saveError) {
                            let payload = new Object();
                            Object.assign(payload, vm.occurrence_report_obj);
                            fetch(
                                helpers.add_endpoint_json(
                                    api_endpoints.occurrence_report,
                                    vm.occurrence_report_obj.id + '/submit'
                                ),
                                {
                                    method: 'POST',
                                    headers: {
                                        'Content-Type': 'application/json',
                                    },
                                    body: JSON.stringify(payload),
                                }
                            ).then(async (response) => {
                                let data = await response.json();
                                if (!response.ok) {
                                    vm.submitting = false;
                                    swal.fire({
                                        title: 'Submit Error',
                                        text: JSON.stringify(data),
                                        icon: 'error',
                                        customClass: {
                                            confirmButton: 'btn btn-primary',
                                        },
                                    });
                                    return;
                                }
                                vm.occurrence_report_obj = data;
                                vm.$router.push({
                                    name: 'submit_ocr_proposal',
                                    state: {
                                        occurrence_report_obj: JSON.stringify(
                                            vm.occurrence_report_obj
                                        ),
                                    },
                                });
                            });
                        }
                    }
                    vm.submitting = false;
                },
                () => {
                    vm.submitting = false;
                }
            );
        },
        refreshFromResponse: function (data) {
            this.occurrence_report_obj = Object.assign({}, data);
        },
        fetchOccurrenceReport: function (id) {
            let vm = this;
            fetch(`/api/occurrence_report/${id}/`).then(
                async (response) => {
                    const data = await response.json();
                    vm.occurrence_report_obj = Object.assign({}, data);
                },
                (err) => {
                    console.log(err);
                }
            );
        },
        refreshOccurrenceReport: function () {
            this.fetchOccurrenceReport(this.$route.params.occurrence_report_id);
        },
        backToDashboard: function () {
            if (this.isDirty) {
                swal.fire({
                    title: 'Unsaved Changes',
                    text: 'You have unsaved changes. Are you sure you want to go back to the dashboard?',
                    icon: 'question',
                    showCancelButton: true,
                    reverseButtons: true,
                    confirmButtonText: 'Back to Dashboard',
                    cancelButtonText: 'Cancel',
                    customClass: {
                        confirmButton: 'btn btn-primary',
                        cancelButton: 'btn btn-secondary me-2',
                    },
                    buttonsStyling: false,
                }).then((result) => {
                    if (result.isConfirmed) {
                        this.$router.push({
                            name: 'external-occurrence-report-dash',
                        });
                    }
                });
                return;
            }
            this.$router.push({
                name: 'external-occurrence-report-dash',
            });
        },
    },
};
</script>
