<template lang="html">
    <div id="occurrenceDocumentHistory">
        <modal
            transition="modal fade"
            :title="
                'Occurrence  OCC' +
                occurrenceId +
                ' - Document D' +
                documentId +
                ' - History '
            "
            :large="true"
            :full="true"
            :show-o-k="false"
            cancel-text="Close"
            :data-loss-warning-on-cancel="false"
            @cancel="close()"
        >
            <div class="container-fluid">
                <div class="row">
                    <alert v-if="errorString" type="danger"
                        ><strong>{{ errorString }}</strong></alert
                    >
                    <div class="col-sm-12">
                        <div class="form-group">
                            <div class="row">
                                <div v-if="documentId" class="col-lg-12">
                                    <datatable
                                        :id="datatable_id"
                                        ref="history_datatable"
                                        :dt-options="datatable_options"
                                        :dt-headers="datatable_headers"
                                    />
                                    <div v-if="historyId">
                                        <DisplayHistory
                                            ref="display_history"
                                            :key="historyId"
                                            :primary_model_number="
                                                'D' + documentId
                                            "
                                            :revision_id="historyId"
                                            :revision_sequence="historySequence"
                                            :primary_model="'OccurrenceDocument'"
                                        />
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </modal>
    </div>
</template>
<script>
import modal from '@vue-utils/bootstrap-modal.vue';
import alert from '@vue-utils/alert.vue';
import { helpers, api_endpoints, constants } from '@/utils/hooks.js';
import datatable from '@/utils/vue/datatable.vue';
import DisplayHistory from '../../common/display_history.vue';
import { v4 as uuid } from 'uuid';

export default {
    name: 'OccurrenceDocumentHistory',
    components: {
        modal,
        alert,
        datatable,
        DisplayHistory,
    },
    props: {
        documentId: {
            type: Number,
            required: true,
        },
        occurrenceId: {
            type: Number,
            required: true,
        },
    },
    data: function () {
        return {
            historyId: null,
            historySequence: null,
            datatable_id: 'history-datatable-' + uuid(),
            documentDetails: {},
            isModalOpen: false,
            errorString: '',
            successString: '',
            success: false,
        };
    },
    computed: {
        csrf_token: function () {
            return helpers.getCookie('csrftoken');
        },
        datatable_headers: function () {
            return [
                'Number',
                'Date Modified',
                'Modified By',
                'Category',
                'Sub Category',
                'Document',
                'Description',
                'Action',
            ];
        },
        column_data: function () {
            return {
                // 0. data
                data: 'data.data',
                orderable: false,
                searchable: false,
                visible: false,
                render: function (row, type, full) {
                    return full.data.data.occurrencedocument;
                },
                name: 'data',
            };
        },
        column_sequence: function () {
            return {
                data: 'revision_sequence',
                orderable: true,
                searchable: false,
                visible: true,
                render: function (row, type, full) {
                    if (full.data.occurrencedocument.fields.document_number) {
                        return (
                            full.data.occurrencedocument.fields
                                .document_number +
                            '-' +
                            full.revision_sequence
                        );
                    } else {
                        return (
                            'D' +
                            full.data.occurrencedocument.pk +
                            '-' +
                            full.revision_sequence
                        );
                    }
                },
                name: 'revision_sequence',
            };
        },
        column_id: function () {
            return {
                // 1. ID
                data: 'data.data.occurrencedocument.pk',
                orderable: false,
                searchable: false,
                visible: false,
                render: function (row, type, full) {
                    return full.data.occurrencedocument.pk;
                },
                name: 'id',
            };
        },
        column_number: function () {
            return {
                // 2. Number
                data: 'data.data.occurrencedocument.fields.document_number',
                defaultContent: '',
                orderable: false,
                searchable: false,
                visible: true,
                render: function (row, type, full) {
                    return (
                        full.data.occurrencedocument.fields.document_number +
                        '-' +
                        full.revision_sequence
                    );
                },
                name: 'document_number',
            };
        },
        column_revision_id: function () {
            return {
                data: 'revision_id',
                orderable: true,
                searchable: true,
                visible: true,
                render: function (row, type, full) {
                    return full.revision_id;
                },
                name: 'revision_id',
            };
        },
        column_revision_date: function () {
            return {
                data: 'date_created',
                orderable: true,
                searchable: true,
                visible: true,
                render: function (row, type, full) {
                    return full.date_created;
                },
                name: 'revision_date',
            };
        },
        column_revision_user: function () {
            return {
                data: 'revision_user',
                orderable: false,
                searchable: false,
                visible: true,
                render: function (row, type, full) {
                    return full.revision_user;
                },
                name: 'revision_user',
            };
        },
        column_category: function () {
            return {
                data: 'data.data.occurrencedocument.fields.document_category',
                defaultContent: '',
                orderable: false,
                searchable: false,
                visible: true,
                render: function (row, type, full) {
                    if (full.data.occurrencedocument.fields.document_category) {
                        return full.data.occurrencedocument.fields
                            .document_category.document_category_name;
                    } else {
                        return '';
                    }
                },
                name: 'document_category',
            };
        },
        column_sub_category: function () {
            return {
                data: 'data.data.occurrencedocument.fields.document_sub_category',
                defaultContent: '',
                orderable: false,
                searchable: false,
                visible: true,
                render: function (row, type, full) {
                    if (
                        full.data.occurrencedocument.fields
                            .document_sub_category
                    ) {
                        return full.data.occurrencedocument.fields
                            .document_sub_category.document_sub_category_name;
                    } else {
                        return '';
                    }
                },
                name: 'document_sub_category',
            };
        },
        column_file: function () {
            return {
                // 3. File
                data: 'data.data.occurrencedocument.fields.name',
                defaultContent: '',
                orderable: false,
                searchable: true,
                visible: true,
                mRender: function (row, type, full) {
                    let links = '';
                    if (full.data.occurrencedocument.fields.active) {
                        let value = full.data.occurrencedocument.fields.name;
                        let result = helpers.dtPopoverSplit(value, 30, 'hover');
                        links +=
                            '<span><a href="/private-media/' +
                            full.data.occurrencedocument.fields._file +
                            '" target="_blank">' +
                            result.text +
                            '</a> ' +
                            result.link +
                            '</span>';
                    } else {
                        let value = full.data.occurrencedocument.fields.name;
                        let result = helpers.dtPopover(value, 30, 'hover');
                        links +=
                            type == 'export' ? value : '<s>' + result + '</s>';
                    }
                    return links;
                },
                name: 'name',
            };
        },
        column_description: function () {
            return {
                // 4. Description
                data: 'data.data.occurrencedocument.fields.description',
                defaultContent: '',
                orderable: false,
                searchable: true,
                visible: true,
                render: function (row, type, full) {
                    let value = full.data.occurrencedocument.fields.description;
                    let result = helpers.dtPopover(value, 30, 'hover');
                    return type == 'export' ? value : result;
                },
                name: 'description',
            };
        },
        column_action: function () {
            return {
                data: 'revision_id',
                orderable: false,
                searchable: false,
                visible: true,
                mRender: function (data, type, full) {
                    let links = '';
                    links += `<a href='#' data-view-history='${full.revision_id}' data-view-history-seq='${full.revision_sequence}'>View</a><br>`;
                    return links;
                },
            };
        },
        datatable_options: function () {
            let vm = this;
            let columns = [
                vm.column_sequence,
                //vm.column_number,
                vm.column_revision_date,
                vm.column_revision_user,
                vm.column_category,
                vm.column_sub_category,
                vm.column_file,
                vm.column_description,
                vm.column_action,
            ];
            return {
                autoWidth: false,
                language: {
                    processing: constants.DATATABLE_PROCESSING_HTML,
                },
                responsive: true,
                searching: true,
                ordering: true,
                order: [[0, 'asc']],
                serverSide: true,
                ajax: {
                    url:
                        api_endpoints.lookup_history_occurrence_document(
                            this.documentId
                        ) + '?format=datatables',
                    dataSrc: 'data',
                },
                buttons: [
                    {
                        extend: 'excel',
                        title: 'Boranga OCC Document History Excel Export',
                        text: '<i class="fa-solid fa-download"></i> Excel',
                        className: 'btn btn-primary me-2 rounded',
                        exportOptions: {
                            orthogonal: 'export',
                        },
                    },
                    {
                        extend: 'csv',
                        title: 'Boranga OCC Document History CSV Export',
                        text: '<i class="fa-solid fa-download"></i> CSV',
                        className: 'btn btn-primary rounded',
                        exportOptions: {
                            orthogonal: 'export',
                        },
                    },
                ],
                dom:
                    "<'d-flex align-items-center'<'me-auto'l>fB>" +
                    "<'row'<'col-sm-12'tr>>" +
                    "<'d-flex align-items-center'<'me-auto'i>p>",
                columns: columns,
                processing: true,
                drawCallback: function () {
                    helpers.enablePopovers();
                },
                initComplete: function () {
                    helpers.enablePopovers();
                },
            };
        },
    },
    watch: {
        isModalOpen() {
            let vm = this;
            if (this.isModalOpen) {
                vm.$refs.history_datatable.vmDataTable.ajax.reload();
            }
        },
    },
    mounted: function () {
        let vm = this;
        this.$nextTick(() => {
            vm.addEventListeners();
        });
    },
    methods: {
        close: function () {
            this.errorString = '';
            this.isModalOpen = false;
            $('.has-error').removeClass('has-error');
        },
        viewHistory: function (id, seq) {
            console.log('viewHistory');
            this.historyId = parseInt(id);
            this.historySequence = parseInt(seq);
            this.uuid++;
            this.$nextTick(() => {
                this.$refs.display_history.isModalOpen = true;
            });
        },
        addEventListeners: function () {
            let vm = this;
            vm.$refs.history_datatable.vmDataTable.on(
                'click',
                'a[data-view-history]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-view-history');
                    var seq = $(this).attr('data-view-history-seq');
                    vm.viewHistory(id, seq);
                }
            );
            vm.$refs.history_datatable.vmDataTable.on(
                'childRow.dt',
                function () {
                    helpers.enablePopovers();
                }
            );
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
    width: inherit; /* Or auto */
    padding: 0 10px; /* To give a bit of padding on the left and right */
    border-bottom: none;
}
</style>
