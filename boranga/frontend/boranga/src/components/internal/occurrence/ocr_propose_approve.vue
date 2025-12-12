<template lang="html">
    <div id="internal-ocr-propose-approve-request">
        <modal
            id="ocr-propose-approve-modal"
            transition="modal fade"
            ok-text="Propose Approve"
            :title="`Propose Approve ${occurrence_report_number}`"
            large
            @ok="ok()"
            @cancel="close()"
        >
            <div class="container">
                <form id="propose-approve-form">
                    <div v-if="errorString" class="row mb-3">
                        <div class="col">
                            <alert type="danger"
                                ><strong>{{ errorString }}</strong></alert
                            >
                        </div>
                    </div>
                    <div class="row mb-3">
                        <div class="col">
                            <div class="form-check form-check-inline">
                                <input
                                    id="create_new_occurrence_new"
                                    v-model="
                                        propose_approve.create_new_occurrence
                                    "
                                    class="form-check-input"
                                    type="radio"
                                    name="create_new_occurrence"
                                    :value="true"
                                    @change="resetSelectedOccurrence"
                                />
                                <label
                                    class="form-check-label"
                                    for="create_new_occurrence_new"
                                    >Create New Occurrence</label
                                >
                            </div>
                            <div class="form-check form-check-inline">
                                <input
                                    id="create_new_occurrence_existing"
                                    v-model="
                                        propose_approve.create_new_occurrence
                                    "
                                    class="form-check-input"
                                    type="radio"
                                    name="create_new_occurrence"
                                    :value="false"
                                    @change="reinitialiseOccurrenceNameLookup"
                                />
                                <label
                                    class="form-check-label"
                                    for="create_new_occurrence_existing"
                                    >Add to Existing Occurrence</label
                                >
                            </div>
                            <hr />
                            <div
                                v-if="!propose_approve.create_new_occurrence"
                                class="mt-3"
                            >
                                <div
                                    v-if="occurrence_report.ocr_for_occ_number"
                                    class="form-group mb-3"
                                >
                                    <label
                                        for="occurrence_report_is_for_occurrence_number"
                                        >Proposed / Suggested OCC Number:</label
                                    >
                                    <input
                                        id="occurrence_report_is_for_occurrence_number"
                                        v-model="
                                            occurrence_report.ocr_for_occ_number
                                        "
                                        type="text"
                                        :disabled="true"
                                        class="form-control"
                                        autocomplete="new-password"
                                    />
                                </div>
                                <div
                                    v-if="occurrence_report.ocr_for_occ_name"
                                    class="form-group mb-3"
                                >
                                    <label
                                        for="occurrence_report_for_occurrence_name"
                                        >Proposed / Suggested Occurrence
                                        Name:</label
                                    >
                                    <input
                                        id="occurrence_report_for_occurrence_name"
                                        v-model="
                                            occurrence_report.ocr_for_occ_name
                                        "
                                        type="text"
                                        :disabled="true"
                                        class="form-control"
                                        autocomplete="new-password"
                                    />
                                </div>
                                <div
                                    id="occurrence_name_lookup_propose_approve_form_group_id"
                                    class="form-group mb-3"
                                >
                                    <label
                                        class="mb-3"
                                        for="occurrence_name_lookup_propose_approve"
                                        >Existing Occurrence:</label
                                    >
                                    <select
                                        id="occurrence_name_lookup_propose_approve"
                                        ref="occurrence_name_lookup_propose_approve"
                                        name="occurrence_name_lookup_propose_approve"
                                        class="form-control"
                                        required
                                    />
                                </div>
                                <div
                                    id="copy_ocr_comments_section"
                                    v-if="
                                        occurrence_report.comments &&
                                        propose_approve.occurrence_id
                                    "
                                >
                                    <div>
                                        <label
                                            class="mb-3"
                                            for="copy_ocr_comments_to_occ_comments"
                                            >Copy Occurrence Report Comments to
                                            Occurrence?</label
                                        >
                                        <div class="form-check form-switch">
                                            <input
                                                v-model="
                                                    propose_approve.copy_ocr_comments_to_occ_comments
                                                "
                                                class="form-check-input"
                                                type="checkbox"
                                                role="switch"
                                                id="copy_ocr_comments_to_occ_comments"
                                            />
                                        </div>
                                    </div>
                                    <div
                                        class="border rounded p-3 mt-3"
                                        v-if="
                                            propose_approve.copy_ocr_comments_to_occ_comments
                                        "
                                    >
                                        <label
                                            class="mb-3"
                                            for="occ_final_comment_preview"
                                            >Resulting Occurrence Comment
                                            Preview</label
                                        >
                                        <textarea
                                            v-model="occ_final_comment_preview"
                                            type="date"
                                            class="form-control"
                                            name="details"
                                            id="occ_final_comment_preview"
                                            rows="10"
                                            disabled
                                        ></textarea>
                                    </div>
                                </div>
                            </div>
                            <div v-else class="mt-3">
                                <label class="mb-3" for="new_occurrence_name"
                                    >New Occurrence Name:</label
                                >
                                <input
                                    id="new_occurrence_name"
                                    ref="new_occurrence_name"
                                    v-model="
                                        propose_approve.new_occurrence_name
                                    "
                                    type="text"
                                    class="form-control"
                                    name="new_occurrence_name"
                                    required
                                />
                            </div>
                            <div class="mt-3">
                                <label class="form-label" for="details"
                                    >Details for Approver</label
                                >
                                <textarea
                                    id="details"
                                    v-model="propose_approve.details"
                                    type="date"
                                    class="form-control"
                                    name="details"
                                ></textarea>
                            </div>
                            <div class="mt-3">
                                <div class="col-sm-12">
                                    <label class="control-label" for="cc_email"
                                        >CC email</label
                                    >
                                    <input
                                        v-model="propose_approve.cc_email"
                                        type="text"
                                        style="width: 70%"
                                        class="form-control"
                                        name="cc_email"
                                    />
                                </div>
                            </div>
                            <div class="mt-3">
                                some blurb to let the user know to go through
                                the report and add sections to the occurrence.
                            </div>
                        </div>
                    </div>
                </form>
            </div>
        </modal>
    </div>
</template>

<script>
import modal from '@vue-utils/bootstrap-modal.vue';
import alert from '@vue-utils/alert.vue';

import { helpers, api_endpoints } from '@/utils/hooks.js';
export default {
    name: 'OcrProposeApprove',
    components: {
        modal,
        alert,
    },
    props: {
        occurrence_report: {
            type: Object,
        },
        occurrence_report_number: {
            type: String,
        },
        group_type_id: {
            type: Number,
        },
        occurrence: {
            type: Object,
        },
        ocr_for_occ_name: {
            type: String,
            required: false,
        },
    },
    data: function () {
        return {
            isModalOpen: false,
            form: null,
            propose_approve: {
                occurrence_id: null,
                occurrence_name: '',
                create_new_occurrence: false,
                new_occurrence_name: '',
                copy_ocr_comments_to_occ_comments: true,
                details: '',
                cc_email: null,
            },
            occurrence_comment: '',
            errorString: '',
            originalProposeApprove: null,
        };
    },
    computed: {
        occ_final_comment_preview: function () {
            let comment = this.occurrence_report.comments;
            if (this.occurrence_comment) {
                comment =
                    this.occurrence_comment +
                    '\n\n' +
                    this.occurrence_report.comments;
            }
            return comment;
        },
    },
    watch: {
        isModalOpen: function (value) {
            if (value) {
                if (
                    this.occurrence !== null &&
                    this.occurrence.id !== undefined &&
                    ((this.occurrence_report.species_id !== null &&
                        this.occurrence_report.species_id ===
                            this.occurrence.species) ||
                        (this.occurrence_report.community_id !== null &&
                            this.occurrence_report.community_id ===
                                this.occurrence.community))
                ) {
                    var newOption = new Option(
                        this.occurrence.occurrence_number +
                            ' - ' +
                            this.occurrence.occurrence_name +
                            ' (' +
                            this.occurrence.group_type +
                            ')',
                        this.occurrence.id,
                        false,
                        true
                    );
                    $(this.$refs.occurrence_name_lookup_propose_approve).append(
                        newOption
                    );

                    this.propose_approve.occurrence_id = this.occurrence.id;
                    this.propose_approve.occurrence_name =
                        this.occurrence.occurrence_name;
                } else {
                    this.$nextTick(() => {
                        $(
                            this.$refs.occurrence_name_lookup_propose_approve
                        ).select2('open');
                    });
                }
                this.$nextTick(() => {
                    this.originalProposeApprove = JSON.parse(
                        JSON.stringify(this.propose_approve)
                    );
                });
            }
        },
        'propose_approve.create_new_occurrence': {
            handler: function () {
                if (this.propose_approve.create_new_occurrence) {
                    if (this.ocr_for_occ_name) {
                        this.propose_approve.new_occurrence_name =
                            this.ocr_for_occ_name;
                    }
                } else {
                    this.propose_approve.new_occurrence_name = '';
                }
            },
            deep: true,
        },
    },
    mounted: function () {
        this.form = document.forms['propose-approve-form'];
        this.initialiseOccurrenceNameLookup();
    },
    methods: {
        hasUnsavedChanges: function () {
            return (
                JSON.stringify(this.propose_approve) !==
                JSON.stringify(this.originalProposeApprove)
            );
        },
        ok: function () {
            let vm = this;
            if (vm.form.checkValidity()) {
                vm.proposeApprove();
            } else {
                vm.form.reportValidity();
            }
        },
        close: function () {
            this.propose_approve = {
                occurrence_id: null,
                occurrence_name: '',
                create_new_occurrence: false,
                new_occurrence_name: '',
                copy_ocr_comments_to_occ_comments: true,
                details: '',
            };
            $(this.$refs.occurrence_name_lookup_propose_approve)
                .empty()
                .trigger('change');
            this.errorString = '';
            this.isModalOpen = false;
        },
        proposeApprove: function () {
            let vm = this;
            let confirmation = `Are you sure you want to propose to approve occurrence report ${vm.occurrence_report_number}`;
            if (this.propose_approve.create_new_occurrence) {
                confirmation += ` (and propose to create a new occurrence based on it)?`;
            } else {
                confirmation += ` (and propose to add it to the existing occurrence ${vm.propose_approve.occurrence_name})?`;
            }
            swal.fire({
                title: `Propose Approve Occurrence Report ${vm.occurrence_report_number}`,
                text: confirmation,
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Propose Approve',
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
                                (vm.occurrence_report.id + '/propose_approve/')
                        ),
                        {
                            method: 'POST',
                            headers: {
                                'Content-Type': 'application/json',
                            },
                            body: JSON.stringify(vm.propose_approve),
                        }
                    ).then(
                        async (response) => {
                            if (!response.ok) {
                                vm.errorString = await response.json();
                                console.log(vm.errorString);
                                return;
                            }
                            swal.fire({
                                title: 'Proposal to Approve Successful',
                                text: `Your proposal to approve occurrence report ${vm.occurrence_report_number} has been successfully submitted.`,
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
        resetSelectedOccurrence: function () {
            this.propose_approve.occurrence_id = null;
            this.propose_approve.occurrence_name = '';
            this.$nextTick(() => {
                this.$refs['new_occurrence_name'].focus();
            });
        },
        reinitialiseOccurrenceNameLookup: function () {
            let vm = this;
            vm.$nextTick(() => {
                $(vm.$refs.occurrence_name_lookup_propose_approve).select2(
                    'destroy'
                );
                vm.initialiseOccurrenceNameLookup();
            });
        },
        initialiseOccurrenceNameLookup: function () {
            let vm = this;
            $(vm.$refs.occurrence_name_lookup_propose_approve)
                .select2({
                    width: '100%',
                    minimumInputLength: 2,
                    dropdownParent: $(
                        '#occurrence_name_lookup_propose_approve_form_group_id'
                    ),
                    theme: 'bootstrap-5',
                    allowClear: true,
                    placeholder: 'Search Name of Occurrence',
                    ajax: {
                        url: api_endpoints.occurrence_name_lookup,
                        dataType: 'json',
                        data: function (params) {
                            var query = {
                                term: params.term,
                                type: 'public',
                                group_type_id: vm.group_type_id,
                                occurrence_report_id: vm.occurrence_report.id,
                            };
                            return query;
                        },
                    },
                })
                .on('select2:select', function (e) {
                    vm.propose_approve.occurrence_id = e.params.data.id;
                    vm.propose_approve.occurrence_name = e.params.data.text;
                    vm.occurrence_comment = e.params.data.occurrence_comment;
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-occurrence_name_lookup_propose_approve-results"]'
                    );
                    searchField[0].focus();
                })
                .on('select2:unselect', function () {
                    vm.propose_approve.occurrence_id = null;
                    vm.propose_approve.occurrence_name = null;
                });
        },
    },
};
</script>
