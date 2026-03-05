<template lang="html">
    <div id="ocr_documents">
        <FormSection :form-collapse="false" label="Documents" Index="documents">
            <alert type="warning"
                ><i class="bi bi-ban fs-6 fw-bold me-2"></i>Do not upload
                Management or Recovery Plans here</alert
            >
            <form class="form-horizontal" action="index.html" method="post">
                <div
                    v-if="!isReadOnly || currentUserIsReferralAssessor"
                    class="col-sm-12"
                >
                    <div class="text-end">
                        <button
                            type="button"
                            class="btn btn-primary mb-2"
                            @click.prevent="newDocument"
                        >
                            <i class="bi bi-plus-circle"></i>
                            Add Document
                        </button>
                    </div>
                </div>
                <div>
                    <datatable
                        :id="panelBody"
                        ref="documents_datatable"
                        :dt-options="documents_options"
                        :dt-headers="documents_headers"
                    />
                </div>
            </form>
        </FormSection>
        <DocumentDetail
            ref="document_detail"
            :url="ocr_document_url"
            :is_internal="is_internal"
            @refresh-from-response="refreshFromResponse"
        >
        </DocumentDetail>
        <ViewDocument
            ref="view_document"
            :is_internal="is_internal"
        ></ViewDocument>
        <div v-if="occurenceReportDocumentHistoryId">
            <OccurenceReportDocumentHistory
                ref="ocr_document_history"
                :key="occurenceReportDocumentHistoryId"
                :document-id="occurenceReportDocumentHistoryId"
                :occurrence-report-id="occurrence_report_obj.id"
            />
        </div>
    </div>
</template>
<script>
import { v4 as uuid } from 'uuid';
import alert from '@vue-utils/alert.vue';
import datatable from '@vue-utils/datatable.vue';
import ViewDocument from '@/components/common/view_document.vue';
import DocumentDetail from '@/components/common/add_document.vue';
import FormSection from '@/components/forms/section_toggle.vue';
import OccurenceReportDocumentHistory from '../../internal/occurrence/ocr_document_history.vue';
import { constants, api_endpoints, helpers } from '@/utils/hooks';

export default {
    name: 'OCRDocuments',
    components: {
        alert,
        FormSection,
        datatable,
        DocumentDetail,
        OccurenceReportDocumentHistory,
        ViewDocument,
    },
    props: {
        occurrence_report_obj: {
            type: Object,
            required: true,
        },
        is_external: {
            type: Boolean,
            default: false,
        },
        is_internal: {
            type: Boolean,
            default: false,
        },
        is_readonly: {
            type: Boolean,
            default: false,
        },
    },
    data: function () {
        let vm = this;
        return {
            uuid: 0,
            occurenceReportDocumentHistoryId: null,
            panelBody: 'ocr-documents-' + uuid(),
            values: null,
            ocr_document_url: api_endpoints.occurrence_report_documents,
            documents_headers: [
                'Number',
                'Category',
                'Sub Category',
                'Document',
                'Description',
                'Date/Time',
                'Action',
            ],
            documents_options: {
                autowidth: true,
                language: {
                    processing: constants.DATATABLE_PROCESSING_HTML,
                },
                responsive: true,
                searching: true,
                //  to show the "workflow Status","Action" columns always in the last position
                columnDefs: [
                    { responsivePriority: 1, targets: 0 },
                    { responsivePriority: 2, targets: -1 },
                ],
                ajax: {
                    url: helpers.add_endpoint_json(
                        api_endpoints.occurrence_report,
                        vm.occurrence_report_obj.id + '/documents'
                    ),
                    dataSrc: '',
                },
                order: [],
                dom:
                    "<'d-flex align-items-center'<'me-auto'l>fB>" +
                    "<'row'<'col-sm-12'tr>>" +
                    "<'d-flex align-items-center'<'me-auto'i>p>",
                buttons: [
                    {
                        extend: 'excel',
                        title: 'Boranga Occurrence Report Documents Excel Export',
                        text: '<i class="bi bi-download"></i> Excel',
                        className: 'btn btn-primary me-2 rounded',
                        exportOptions: {
                            orthogonal: 'export',
                        },
                    },
                    {
                        extend: 'csv',
                        title: 'Boranga Occurrence Report Documents CSV Export',
                        text: '<i class="bi bi-download"></i> CSV',
                        className: 'btn btn-primary rounded',
                        exportOptions: {
                            orthogonal: 'export',
                        },
                    },
                ],
                columns: [
                    {
                        data: 'document_number',
                        orderable: true,
                        searchable: true,
                        mRender: function (data, type, full) {
                            if (full.active) {
                                return full.document_number;
                            } else {
                                return '<s>' + full.document_number + '</s>';
                            }
                        },
                    },
                    {
                        data: 'document_category_name',
                        orderable: true,
                        searchable: true,
                        mRender: function (data, type, full) {
                            if (full.active) {
                                return full.document_category_name;
                            } else {
                                return (
                                    '<s>' + full.document_category_name + '</s>'
                                );
                            }
                        },
                    },
                    {
                        data: 'document_sub_category_name',
                        orderable: true,
                        searchable: true,
                        mRender: function (data, type, full) {
                            if (full.active) {
                                return full.document_sub_category_name;
                            } else {
                                return (
                                    '<s>' +
                                    full.document_sub_category_name +
                                    '</s>'
                                );
                            }
                        },
                    },
                    {
                        data: 'name',
                        orderable: true,
                        searchable: true,
                        mRender: function (data, type, full) {
                            let links = '';
                            if (full.active) {
                                let value = full.name;
                                let result = helpers.dtPopoverSplit(
                                    value,
                                    30,
                                    'hover'
                                );
                                links +=
                                    '<span><a href="' +
                                    full._file +
                                    '" target="_blank">' +
                                    result.text +
                                    '</a> ' +
                                    result.link +
                                    '</span>';
                            } else {
                                let value = full.name;
                                let result = helpers.dtPopover(
                                    value,
                                    30,
                                    'hover'
                                );
                                links +=
                                    type == 'export'
                                        ? value
                                        : '<s>' + result + '</s>';
                            }
                            return links;
                        },
                    },
                    {
                        data: 'description',
                        orderable: true,
                        searchable: true,
                        render: function (value, type, full) {
                            let result = helpers.dtPopover(value, 30, 'hover');
                            if (full.active) {
                                return type == 'export' ? value : result;
                            } else {
                                return type == 'export'
                                    ? value
                                    : '<s>' + result + '</s>';
                            }
                        },
                    },
                    {
                        data: 'uploaded_date',
                        mRender: function (data, type, full) {
                            if (full.active) {
                                return data != '' && data != null
                                    ? moment(data).format('DD/MM/YYYY HH:mm')
                                    : '';
                            } else {
                                return data != '' && data != null
                                    ? '<s>' +
                                          moment(data).format(
                                              'DD/MM/YYYY HH:mm'
                                          ) +
                                          '</s>'
                                    : '';
                            }
                        },
                    },
                    {
                        data: 'id',
                        mRender: function (data, type, full) {
                            let links = '';
                            // to restrict submitter to edit doc when the report is in workflow
                            links += `<a href='#' data-view-document='${full.id}'>View</a><br>`;
                            if (
                                !vm.isReadOnly ||
                                vm.currentUserIsReferralAssessor
                            ) {
                                if (full.active) {
                                    // Only show edit and discard options if the user is a referree who has uploaded the document
                                    // or if the document is accessible to the user
                                    if (
                                        !vm.currentUserIsReferralAssessor ||
                                        full.can_referee_access
                                    ) {
                                        links += `<a href='#${full.id}' data-edit-document='${full.id}'>Edit</a><br/>`;
                                        links += `<a href='#' data-discard-document='${full.id}'>Discard</a><br>`;
                                    }
                                } else {
                                    if (!vm.currentUserIsReferralAssessor) {
                                        links += `<a href='#' data-reinstate-document='${full.id}'>Reinstate</a><br>`;
                                    }
                                }
                            }
                            if (vm.is_internal) {
                                links += `<a href='#' data-history-document='${full.id}'>History</a><br>`;
                            }
                            return links;
                        },
                    },
                ],
                processing: true,
                drawCallback: function () {
                    helpers.enablePopovers();
                },
                initComplete: function () {
                    helpers.enablePopovers();
                    vm.$nextTick(() => {
                        vm.adjust_table_width();
                    });
                },
            },
        };
    },
    computed: {
        isReadOnly: function () {
            //override for split reports
            if (this.is_readonly) {
                return this.is_readonly;
            }
            return this.occurrence_report_obj.readonly;
        },
        currentUserIsReferralAssessor: function () {
            // Check if the current user is a referral who can assess
            if (
                this.occurrence_report_obj.processing_status != 'With Referral'
            ) {
                return false;
            }

            const assessor_mode = this.occurrence_report_obj.assessor_mode;
            if (!assessor_mode) {
                return false;
            }
            return (
                assessor_mode.assessor_mode &&
                assessor_mode.assessor_can_assess &&
                assessor_mode.assessor_level == 'referral'
            );
        },
    },
    mounted: function () {
        let vm = this;
        this.$nextTick(() => {
            vm.addEventListeners();
        });
    },
    methods: {
        viewDocument: function (document) {
            this.$refs.view_document.document = document;
            this.$refs.view_document.isModalOpen = true;
        },
        newDocument: function () {
            let vm = this;
            this.$refs.document_detail.document_id = '';
            //this.$refs.edit_park.fetchPark(id);
            var new_document_another = {
                occurrence_report: vm.occurrence_report_obj.id,
                input_name: 'occurrence_report_doc',
                description: '',
                document_category: '',
                document_sub_category: '',
                uploaded_date: null,
                can_submitter_access: false,
            };
            this.$refs.document_detail.documentObj = new_document_another;
            this.$refs.document_detail.uploaded_document = [];
            this.$refs.document_detail.document_action = 'add';
            this.$refs.document_detail.title = 'Add a new Document';
            this.$refs.document_detail.isModalOpen = true;
        },
        editDocument: function (id) {
            this.$refs.document_detail.document_id = id;
            this.$refs.document_detail.document_action = 'edit';
            this.$refs.document_detail.title = 'Edit a Document';
            fetch(
                helpers.add_endpoint_json(
                    api_endpoints.occurrence_report_documents,
                    id
                )
            ).then(
                async (response) => {
                    const data = await response.json();
                    this.$refs.document_detail.documentObj = data;
                    this.$refs.document_detail.documentObj.uploaded_date =
                        data.uploaded_date != null &&
                        data.uploaded_date != undefined
                            ? moment(data.uploaded_date).format(
                                  'yyyy-MM-DDTHH:mm'
                              )
                            : '';
                    this.$refs.document_detail.uploaded_document = [data];
                    //-----this method is called as it wasn't fetching subcategory
                    this.$refs.document_detail.fetchSubCategory(
                        data.document_category
                    );
                },
                (err) => {
                    console.log(err);
                }
            );
            this.$refs.document_detail.isModalOpen = true;
        },
        historyDocument: function (id) {
            this.occurenceReportDocumentHistoryId = parseInt(id);
            this.uuid++;
            this.$nextTick(() => {
                this.$refs.ocr_document_history.isModalOpen = true;
            });
        },
        discardDocument: function (id) {
            let vm = this;
            swal.fire({
                title: 'Discard Document',
                text: 'Are you sure you want to discard this Document?',
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Discard Document',
                customClass: {
                    confirmButton: 'btn btn-primary',
                    cancelButton: 'btn btn-secondary me-2',
                },
                reverseButtons: true,
            }).then((result) => {
                if (result.isConfirmed) {
                    fetch(
                        helpers.add_endpoint_json(
                            api_endpoints.occurrence_report_documents,
                            id + '/discard'
                        ),
                        {
                            method: 'PATCH',
                            headers: { 'Content-Type': 'application/json' },
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
                                title: 'Discarded',
                                text: 'The document has been discarded',
                                icon: 'success',
                                customClass: {
                                    confirmButton: 'btn btn-primary',
                                },
                            }).then(() => {
                                vm.$refs.documents_datatable.vmDataTable.ajax.reload();
                                if (
                                    vm.occurrence_report_obj
                                        .processing_status == 'Unlocked'
                                ) {
                                    vm.$router.go();
                                }
                            });
                        },
                        (error) => {
                            console.log(error);
                        }
                    );
                }
            });
        },
        reinstateDocument: function (id) {
            let vm = this;
            swal.fire({
                title: 'Reinstate Document',
                text: 'Are you sure you want to Reinstate this Document?',
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Reinstate Document',
                customClass: {
                    confirmButton: 'btn btn-primary',
                    cancelButton: 'btn btn-secondary me-2',
                },
            }).then((result) => {
                if (result.isConfirmed) {
                    fetch(
                        helpers.add_endpoint_json(
                            api_endpoints.occurrence_report_documents,
                            id + '/reinstate'
                        ),
                        {
                            method: 'PATCH',
                            headers: { 'Content-Type': 'application/json' },
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
                                title: 'Reinstated',
                                text: 'Your document has been reinstated',
                                icon: 'success',
                                customClass: {
                                    confirmButton: 'btn btn-primary',
                                },
                            }).then(() => {
                                vm.$refs.documents_datatable.vmDataTable.ajax.reload();
                                if (
                                    vm.occurrence_report_obj
                                        .processing_status == 'Unlocked'
                                ) {
                                    vm.$router.go();
                                }
                            });
                        },
                        (error) => {
                            console.log(error);
                        }
                    );
                }
            });
        },
        updatedDocuments() {
            let vm = this;
            this.$refs.documents_datatable.vmDataTable.ajax.reload();
            if (vm.occurrence_report_obj.processing_status == 'Unlocked') {
                vm.$router.go();
            }
        },
        addEventListeners: function () {
            let vm = this;
            vm.$refs.documents_datatable.vmDataTable.on(
                'click',
                'a[data-view-document]',
                function (e) {
                    e.preventDefault();
                    var document = vm.$refs.documents_datatable.vmDataTable
                        .row($(this).parent())
                        .data();
                    vm.viewDocument(document);
                }
            );
            vm.$refs.documents_datatable.vmDataTable.on(
                'click',
                'a[data-edit-document]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-edit-document');
                    vm.editDocument(id);
                }
            );
            vm.$refs.documents_datatable.vmDataTable.on(
                'click',
                'a[data-history-document]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-history-document');
                    vm.historyDocument(id);
                }
            );
            // External Discard listener
            vm.$refs.documents_datatable.vmDataTable.on(
                'click',
                'a[data-discard-document]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-discard-document');
                    vm.discardDocument(id);
                }
            );
            // External Reinstate listener
            vm.$refs.documents_datatable.vmDataTable.on(
                'click',
                'a[data-reinstate-document]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-reinstate-document');
                    vm.reinstateDocument(id);
                }
            );
            vm.$refs.documents_datatable.vmDataTable.on(
                'childRow.dt',
                function () {
                    helpers.enablePopovers();
                }
            );
        },
        refreshFromResponse: function () {
            this.$refs.documents_datatable.vmDataTable.ajax.reload();
        },
        adjust_table_width: function () {
            if (this.$refs.documents_datatable !== undefined) {
                this.$refs.documents_datatable.vmDataTable.columns
                    .adjust()
                    .responsive.recalc();
            }
        },
    },
};
</script>
