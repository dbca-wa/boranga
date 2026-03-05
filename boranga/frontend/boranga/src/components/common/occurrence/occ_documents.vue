<template lang="html">
    <div id="occ_documents">
        <FormSection :form-collapse="false" label="Documents" Index="documents">
            <alert type="warning"
                ><i class="bi bi-ban fs-6 fw-bold me-2"></i>Do not upload
                Management or Recovery Plans here</alert
            >
            <form class="form-horizontal" action="index.html" method="post">
                <div class="col-sm-12">
                    <div class="text-end">
                        <button
                            :disabled="isReadOnly"
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
            :url="occ_document_url"
            :is_internal="is_internal"
            @refresh-from-response="refreshFromResponse"
        ></DocumentDetail>
        <div v-if="occurenceDocumentHistoryId">
            <OccurenceDocumentHistory
                ref="occ_document_history"
                :key="occurenceDocumentHistoryId"
                :document-id="occurenceDocumentHistoryId"
                :occurrence-id="occurrence_obj.id"
            />
        </div>
    </div>
</template>
<script>
import { v4 as uuid } from 'uuid';
import alert from '@vue-utils/alert.vue';
import datatable from '@vue-utils/datatable.vue';
import DocumentDetail from '@/components/common/add_document.vue';
import FormSection from '@/components/forms/section_toggle.vue';
import OccurenceDocumentHistory from '../../internal/occurrence/occ_document_history.vue';
import { constants, api_endpoints, helpers } from '@/utils/hooks';

export default {
    name: 'OCCDocuments',
    components: {
        alert,
        FormSection,
        datatable,
        DocumentDetail,
        OccurenceDocumentHistory,
    },
    props: {
        occurrence_obj: {
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
    },
    data: function () {
        let vm = this;
        return {
            uuid: 0,
            occurenceDocumentHistoryId: null,
            panelBody: 'occ-documents-' + uuid(),
            values: null,
            occ_document_url: api_endpoints.occurrence_documents,
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
                        api_endpoints.occurrence,
                        vm.occurrence_obj.id + '/documents'
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
                        title: 'Boranga OCC Documents Excel Export',
                        text: '<i class="bi bi-download"></i> Excel',
                        className: 'btn btn-primary me-2 rounded',
                        exportOptions: {
                            orthogonal: 'export',
                        },
                    },
                    {
                        extend: 'csv',
                        title: 'Boranga OCC Documents CSV Export',
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
                            let value = full.name;
                            if (type == 'export') {
                                return value;
                            }
                            let links = '';
                            if (full.active) {
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
                                let result = helpers.dtPopover(
                                    value,
                                    30,
                                    'hover'
                                );
                                links += '<s>' + result + '</s>';
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

                            if (!vm.isReadOnly) {
                                if (full.active) {
                                    links += `<a href='#${full.id}' data-edit-document='${full.id}'>Edit</a><br/>`;
                                    links += `<a href='#' data-discard-document='${full.id}'>Discard</a><br>`;
                                } else {
                                    links += `<a href='#' data-reinstate-document='${full.id}'>Reinstate</a><br>`;
                                }
                            }
                            links += `<a href='#' data-history-document='${full.id}'>History</a><br>`;

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
                    // to fix the responsive table overflow css on tab switch
                    // vm.$refs.documents_datatable.vmDataTable.draw('page');
                    setTimeout(function () {
                        vm.adjust_table_width();
                    }, 100);
                },
            },
        };
    },
    computed: {
        isReadOnly: function () {
            return !this.occurrence_obj.can_user_edit;
        },
    },
    watch: {},
    mounted: function () {
        let vm = this;
        this.$nextTick(() => {
            vm.addEventListeners();
        });
    },
    methods: {
        newDocument: function () {
            let vm = this;
            this.$refs.document_detail.document_id = '';
            //this.$refs.edit_park.fetchPark(id);
            var new_document_another = {
                occurrence: vm.occurrence_obj.id,
                input_name: 'occurrence_doc',
                description: '',
                document_category: '',
                document_sub_category: '',
                uploaded_date: null,
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
                    api_endpoints.occurrence_documents,
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
            //this.$refs.document_detail.fetchSpeciesDocument(id);
            this.$refs.document_detail.isModalOpen = true;
        },
        historyDocument: function (id) {
            this.occurenceDocumentHistoryId = parseInt(id);
            this.uuid++;
            this.$nextTick(() => {
                this.$refs.occ_document_history.isModalOpen = true;
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
                    cancelButton: 'btn btn-secondary',
                },
                reverseButtons: true,
            }).then((result) => {
                if (result.isConfirmed) {
                    fetch(
                        helpers.add_endpoint_json(
                            api_endpoints.occurrence_documents,
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
                            });
                            vm.$refs.documents_datatable.vmDataTable.ajax.reload();
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
                    cancelButton: 'btn btn-secondary',
                },
                reverseButtons: true,
            }).then((result) => {
                if (result.isConfirmed) {
                    fetch(
                        helpers.add_endpoint_json(
                            api_endpoints.occurrence_documents,
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
                            });
                            vm.$refs.documents_datatable.vmDataTable.ajax.reload();
                        },
                        (error) => {
                            console.log(error);
                        }
                    );
                }
            });
        },
        updatedDocuments() {
            this.$refs.documents_datatable.vmDataTable.ajax.reload();
        },
        addEventListeners: function () {
            let vm = this;
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
            vm.$refs.documents_datatable.vmDataTable.on(
                'click',
                'a[data-discard-document]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-discard-document');
                    vm.discardDocument(id);
                }
            );
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

<style lang="css" scoped>
/*ul, li {
        zoom:1;
        display: inline;
    }*/
fieldset.scheduler-border {
    border: 1px groove #ddd !important;
    padding: 0 1.4em 1.4em 1.4em !important;
    margin: 0 0 1.5em 0 !important;
    -webkit-box-shadow: 0px 0px 0px 0px #000;
    box-shadow: 0px 0px 0px 0px #000;
}

legend.scheduler-border {
    width: inherit;
    /* Or auto */
    padding: 0 10px;
    /* To give a bit of padding on the left and right */
    border-bottom: none;
}
</style>
