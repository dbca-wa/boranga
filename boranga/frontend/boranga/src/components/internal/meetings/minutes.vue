<template lang="html">
    <div id="minutes">
        <FormSection
            :form-collapse="false"
            label="Meeting Documents"
            :Index="minutesBody"
        >
            <form class="form-horizontal" action="index.html" method="post">
                <div v-if="meeting_obj.can_user_add_minutes" class="col-sm-12">
                    <div class="text-end">
                        <button
                            type="button"
                            class="btn btn-primary mb-2"
                            @click.prevent="addMinutes"
                        >
                            <i class="fa-solid fa-circle-plus"></i>
                            Add Document
                        </button>
                    </div>
                </div>
                <div>
                    <datatable
                        :id="panelBody"
                        ref="minutes_datatable"
                        :dt-options="minutes_options"
                        :dt-headers="minutes_headers"
                    />
                </div>
            </form>
        </FormSection>
        <DocumentDetail
            ref="document_detail"
            :url="minutes_url"
            :is_internal="is_internal"
            @refresh-from-response="refreshFromResponse"
        >
        </DocumentDetail>
        <div v-if="minutesHistoryId">
            <MinutesHistory
                ref="minutes_history"
                :key="minutesHistoryId"
                :minutes-id="minutesHistoryId"
                :meeting-id="meeting_obj.id"
            />
        </div>
    </div>
</template>
<script>
import { v4 as uuid } from 'uuid';
import datatable from '@vue-utils/datatable.vue';
import DocumentDetail from '@/components/common/add_document.vue';
import FormSection from '@/components/forms/section_toggle.vue';
import MinutesHistory from '../../internal/meetings/minutes_history.vue';
import { constants, api_endpoints, helpers } from '@/utils/hooks';

export default {
    name: 'MinutesComponent',
    components: {
        FormSection,
        datatable,
        DocumentDetail,
        MinutesHistory,
    },
    props: {
        meeting_obj: {
            type: Object,
            required: true,
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
            minutesHistoryId: null,
            minutesBody: 'minutesBody' + uuid(),
            panelBody: 'meeting-minutes' + uuid(),
            values: null,
            minutes_url: api_endpoints.minutes,
            minutes_headers: [
                'Number',
                'Category',
                'Sub Category',
                'Description',
                'Document',
                'Date/Time',
                'Action',
            ],
            minutes_options: {
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
                        api_endpoints.meeting,
                        vm.meeting_obj.id + '/minutes'
                    ),
                    dataSrc: '',
                },
                order: [],
                dom:
                    "<'d-flex align-items-center'<'me-auto'l>fB>" +
                    "<'row'<'col-sm-12'tr>>" +
                    "<'d-flex align-items-center'<'me-auto'i>p>",
                buttons: [],
                columns: [
                    {
                        data: 'minutes_number',
                        orderable: true,
                        searchable: true,
                        mRender: function (data, type, full) {
                            if (full.active) {
                                return full.minutes_number;
                            } else {
                                return '<s>' + full.minutes_number + '</s>';
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
                            if (vm.meeting_obj.can_user_edit) {
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
                },
            },
        };
    },
    mounted: function () {
        let vm = this;
        this.$nextTick(() => {
            vm.addEventListeners();
        });
    },
    methods: {
        addMinutes: function () {
            let vm = this;
            this.$refs.document_detail.document_id = '';
            var new_document_another = {
                meeting: vm.meeting_obj.id,
                input_name: 'meeting_minutes_doc',
                description: '',
                document_category: '',
                document_sub_category: '',
                uploaded_date: null,
            };
            this.$refs.document_detail.documentObj = new_document_another;
            this.$refs.document_detail.uploaded_document = [];
            this.$refs.document_detail.document_action = 'add';
            this.$refs.document_detail.title = 'Add a new Minute';
            this.$refs.document_detail.isModalOpen = true;
        },
        editDocument: function (id) {
            this.$refs.document_detail.document_id = id;
            this.$refs.document_detail.document_action = 'edit';
            this.$refs.document_detail.title = 'Edit a Minute';
            fetch(helpers.add_endpoint_json(api_endpoints.minutes, id)).then(
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
            this.minutesHistoryId = parseInt(id);
            this.uuid++;
            this.$nextTick(() => {
                this.$refs.minutes_history.isModalOpen = true;
            });
        },
        discardDocument: function (id) {
            let vm = this;
            swal.fire({
                title: 'Discard Minutes',
                text: 'Are you sure you want to discard these minutes?',
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Discard Minutes',
                customClass: {
                    confirmButton: 'btn btn-primary',
                    cancelButton: 'btn btn-secondary',
                },
                reverseButtons: true,
            }).then(
                (result) => {
                    if (result.isConfirmed) {
                        fetch(
                            helpers.add_endpoint_json(
                                api_endpoints.minutes,
                                id + '/discard'
                            ),
                            {
                                method: 'PATCH',
                                headers: {
                                    'Content-Type': 'application/json',
                                },
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
                                    text: 'The minutes have been discarded',
                                    icon: 'success',
                                    customClass: {
                                        confirmButton: 'btn btn-primary',
                                    },
                                });
                                vm.$refs.minutes_datatable.vmDataTable.ajax.reload();
                            },
                            (error) => {
                                console.log(error);
                            }
                        );
                    }
                },
                (error) => {
                    console.log(error);
                }
            );
        },
        reinstateDocument: function (id) {
            let vm = this;
            swal.fire({
                title: 'Reinstate Minutes',
                text: 'Are you sure you want to reinstate these minutes?',
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Reinstate Minutes',
                customClass: {
                    confirmButton: 'btn btn-primary',
                    cancelButton: 'btn btn-secondary',
                },
                reverseButtons: true,
            }).then(
                (result) => {
                    if (result.isConfirmed) {
                        fetch(
                            helpers.add_endpoint_json(
                                api_endpoints.minutes,
                                id + '/reinstate'
                            ),
                            {
                                method: 'PATCH',
                                headers: {
                                    'Content-Type': 'application/json',
                                },
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
                                    text: 'The minutes have been reinstated',
                                    icon: 'success',
                                    customClass: {
                                        confirmButton: 'btn btn-primary',
                                    },
                                });
                                vm.$refs.minutes_datatable.vmDataTable.ajax.reload();
                            },
                            (error) => {
                                console.log(error);
                            }
                        );
                    }
                },
                (error) => {
                    console.log(error);
                }
            );
        },
        updatedDocuments() {
            this.$refs.minutes_datatable.vmDataTable.ajax.reload();
        },
        addEventListeners: function () {
            let vm = this;
            vm.$refs.minutes_datatable.vmDataTable.on(
                'click',
                'a[data-edit-document]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-edit-document');
                    vm.editDocument(id);
                }
            );
            vm.$refs.minutes_datatable.vmDataTable.on(
                'click',
                'a[data-history-document]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-history-document');
                    vm.historyDocument(id);
                }
            );
            // External Discard listener
            vm.$refs.minutes_datatable.vmDataTable.on(
                'click',
                'a[data-discard-document]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-discard-document');
                    vm.discardDocument(id);
                }
            );
            // External Reinstate listener
            vm.$refs.minutes_datatable.vmDataTable.on(
                'click',
                'a[data-reinstate-document]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-reinstate-document');
                    vm.reinstateDocument(id);
                }
            );
            vm.$refs.minutes_datatable.vmDataTable.on(
                'childRow.dt',
                function () {
                    helpers.enablePopovers();
                }
            );
        },
        refreshFromResponse: function () {
            this.$refs.minutes_datatable.vmDataTable.ajax.reload();
        },
    },
};
</script>

<style lang="css" scoped>
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
