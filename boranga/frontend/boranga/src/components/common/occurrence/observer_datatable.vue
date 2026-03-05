<template lang="html">
    <div id="observerTable">
        <div v-if="!isReadOnly" class="row mb-3">
            <label for="" class="col-sm-6 control-label"
                ><span class="text-danger fw-bold">*</span>
                <span class="text-muted ms-1"
                    >You must add at least one observer</span
                ></label
            >

            <div class="col-sm-6 text-end">
                <button
                    :disabled="isReadOnly"
                    type="button"
                    class="btn btn-primary mb-2"
                    @click.prevent="newObserverDetail"
                >
                    <i class="bi bi-plus-circle"></i>
                    Add Observer
                </button>
            </div>
        </div>
        <div class="row mb-3">
            <datatable
                id="observerDetailTable"
                ref="observer_detail_datatable"
                :dt-options="observer_detail_options"
                :dt-headers="observer_detail_headers"
            />
        </div>
        <ObserverDetail
            v-if="occurrence_report_obj"
            ref="observer_detail"
            :url="observer_detail_url"
            :occurrence_report="occurrence_report_obj"
            @refresh-from-response="refreshFromResponse"
        >
        </ObserverDetail>
        <div v-if="ocrObserverDetailHistoryId">
            <OCRObserverDetailHistory
                ref="ocr_observer_detail_history"
                :key="ocrObserverDetailHistoryId"
                :observer-id="ocrObserverDetailHistoryId"
            />
        </div>
    </div>
</template>
<script>
import datatable from '@vue-utils/datatable.vue';
import ObserverDetail from './add_observer_detail.vue';
import OCRObserverDetailHistory from '../../internal/occurrence/ocr_observer_detail_history.vue';
import { constants, api_endpoints, helpers } from '@/utils/hooks';

export default {
    name: 'ObserverTable',
    components: {
        datatable,
        ObserverDetail,
        OCRObserverDetailHistory,
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
        isReadOnly: {
            type: Boolean,
            default: true,
        },
        show_observer_contact_information: {
            type: Boolean,
            default: true,
        },
    },
    emits: ['refreshOccurrenceReport'],
    data: function () {
        let vm = this;
        let columns = [
            {
                data: 'observer_name',
                orderable: true,
                searchable: true,
            },
            {
                data: 'contact',
                orderable: true,
                searchable: true,
                mRender: function (data, type, full) {
                    let value = full.contact;
                    let result = helpers.dtPopover(value, 30, 'hover');
                    return type == 'export' ? value : result;
                },
            },
            {
                data: 'role',
                orderable: true,
                searchable: true,
            },
            {
                data: 'category',
                orderable: true,
                searchable: true,
            },
            {
                data: 'organisation',
                orderable: true,
                searchable: true,
            },
            {
                data: 'main_observer',
                orderable: true,
                searchable: true,
                render: function (data, type, full) {
                    return full.main_observer ? 'Yes' : 'No';
                },
            },
            {
                data: 'id',
                mRender: function (data, type, full) {
                    let links = '';
                    if (full.visible) {
                        if (!vm.isReadOnly) {
                            links += `<a href='#${full.id}' data-edit-observer_det='${full.id}'>Edit</a><br/>`;
                            links += `<a href='#' data-delete-observer_det='${full.id}'>Discard</a><br>`;
                        } else {
                            links += `<a href='#${full.id}' data-view-observer_det='${full.id}'>View</a><br/>`;
                        }
                    } else if (!vm.isReadOnly) {
                        links += `<a href='#' data-reinstate-observer_det='${full.id}'>Reinstate</a><br>`;
                    }
                    if (full.can_action && !vm.is_external) {
                        links += `<a href='#' data-history-observer='${full.id}'>History</a><br>`;
                    }
                    return links;
                },
            },
        ];
        if (!vm.show_observer_contact_information) {
            columns.splice(1, 1);
        }
        return {
            ocrObserverDetailHistoryId: null,
            observer_detail_url: api_endpoints.observer_detail,
            observer_detail_headers: vm.show_observer_contact_information
                ? [
                      'Observer Name',
                      'Contact Detail',
                      'Observer Role',
                      'Observer Category',
                      'Organisation',
                      'Main Observer',
                      'Action',
                  ]
                : [
                      'Observer Name',
                      'Observer Role',
                      'Observer Category',
                      'Organisation',
                      'Main Observer',
                      'Action',
                  ],
            observer_detail_options: {
                autowidth: false,
                language: {
                    processing: constants.DATATABLE_PROCESSING_HTML,
                },
                responsive: true,
                searching: true,
                //  to show the "workflow Status","Action" columns always in the last position
                columnDefs: [
                    { responsivePriority: 1, targets: 0 },
                    {
                        responsivePriority: 2,
                        className: 'actions',
                        targets: -1,
                    },
                ],
                ajax: {
                    url: helpers.add_endpoint_json(
                        api_endpoints.occurrence_report,
                        vm.occurrence_report_obj.id + '/observer_details'
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
                        title: 'Boranga Occurrence Report Observers Excel Export',
                        text: '<i class="bi bi-download"></i> Excel',
                        className: 'btn btn-primary me-2 rounded',
                        exportOptions: {
                            orthogonal: 'export',
                        },
                    },
                    {
                        extend: 'csv',
                        title: 'Boranga Occurrence Report Observers CSV Export',
                        text: '<i class="bi bi-download"></i> CSV',
                        className: 'btn btn-primary rounded',
                        exportOptions: {
                            orthogonal: 'export',
                        },
                    },
                ],
                columns: columns,
                processing: true,
                rowCallback: function (row, data) {
                    if (!data.visible) {
                        $(row)
                            .children('td:not(.actions)')
                            .addClass('text-decoration-line-through');
                    }
                },
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
    watch: {
        isReadOnly: function () {
            this.$refs.observer_detail_datatable.vmDataTable.ajax.reload();
        },
    },
    mounted: function () {
        let vm = this;
        this.$nextTick(() => {
            vm.eventListeners();
        });
    },
    methods: {
        eventListeners: function () {
            let vm = this;
            vm.$refs.observer_detail_datatable.vmDataTable.on(
                'click',
                'a[data-edit-observer_det]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-edit-observer_det');
                    vm.editObserverDetail(id);
                }
            );
            vm.$refs.observer_detail_datatable.vmDataTable.on(
                'click',
                'a[data-view-observer_det]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-view-observer_det');
                    vm.viewObserverDetail(id);
                }
            );
            // External Discard listener
            vm.$refs.observer_detail_datatable.vmDataTable.on(
                'click',
                'a[data-delete-observer_det]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-delete-observer_det');
                    vm.deleteObserverDetail(id);
                }
            );
            vm.$refs.observer_detail_datatable.vmDataTable.on(
                'click',
                'a[data-reinstate-observer_det]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-reinstate-observer_det');
                    vm.reinstateObserverDetail(id);
                }
            );
            vm.$refs.observer_detail_datatable.vmDataTable.on(
                'childRow.dt',
                function () {
                    helpers.enablePopovers();
                }
            );
            vm.$refs.observer_detail_datatable.vmDataTable.on(
                'click',
                'a[data-history-observer]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-history-observer');
                    vm.historyObserverDetail(id);
                }
            );
        },
        refreshFromResponse: function () {
            let vm = this;
            vm.$refs.observer_detail_datatable.vmDataTable.ajax.reload();
        },
        adjust_table_width: function () {
            let vm = this;
            if (vm.$refs.observer_detail_datatable !== undefined) {
                vm.$refs.observer_detail_datatable.vmDataTable.columns
                    .adjust()
                    .responsive.recalc();
            }
        },
        newObserverDetail: function () {
            this.$refs.observer_detail.observer_detail_action = 'add';
            this.$refs.observer_detail.isModalOpen = true;
        },
        editObserverDetail: async function (id) {
            this.$refs.observer_detail.observer_detail_action = 'edit';
            await fetch(
                helpers.add_endpoint_json(api_endpoints.observer_detail, id)
            ).then(
                async (response) => {
                    this.$refs.observer_detail.observerObj =
                        await response.json();
                    this.$refs.observer_detail.observerObj.id = id;
                },
                (err) => {
                    console.log(err);
                }
            );
            this.$refs.observer_detail.isModalOpen = true;
        },
        viewObserverDetail: async function (id) {
            this.$refs.observer_detail.observer_detail_id = id;
            this.$refs.observer_detail.observer_detail_action = 'view';
            await fetch(
                helpers.add_endpoint_json(api_endpoints.observer_detail, id)
            ).then(
                async (response) => {
                    this.$refs.observer_detail.observerObj =
                        await response.json();
                    this.$refs.observer_detail.observerObj.id = id;
                },
                (err) => {
                    console.log(err);
                }
            );
            this.$refs.observer_detail.isModalOpen = true;
        },
        deleteObserverDetail: function (id) {
            let vm = this;
            swal.fire({
                title: 'Discard Observer',
                text: 'Are you sure you want to discard this Observer?',
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Discard Observer',
                customClass: {
                    confirmButton: 'btn btn-primary',
                    cancelButton: 'btn btn-secondary me-2',
                },
                reverseButtons: true,
            }).then((result) => {
                if (result.isConfirmed) {
                    fetch(
                        helpers.add_endpoint_json(
                            api_endpoints.observer_detail,
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
                                text: 'The Observer has been discarded',
                                icon: 'success',
                                customClass: {
                                    confirmButton: 'btn btn-primary',
                                },
                            }).then(() => {
                                vm.$refs.observer_detail_datatable.vmDataTable.ajax.reload();
                                vm.$emit('refreshOccurrenceReport');
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
        reinstateObserverDetail: function (id) {
            let vm = this;
            fetch(
                helpers.add_endpoint_json(
                    api_endpoints.observer_detail,
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
                        text: 'The Observer has been reinstated',
                        icon: 'success',
                        customClass: {
                            confirmButton: 'btn btn-primary',
                        },
                    }).then(() => {
                        vm.$refs.observer_detail_datatable.vmDataTable.ajax.reload();
                        vm.$emit('refreshOccurrenceReport');
                        if (
                            vm.occurrence_report_obj.processing_status ==
                            'Unlocked'
                        ) {
                            vm.$router.go();
                        }
                    });
                },
                (error) => {
                    var errorText = helpers.apiVueResourceError(error);
                    swal.fire({
                        title: 'Error',
                        text: errorText,
                        icon: 'error',
                        customClass: {
                            confirmButton: 'btn btn-primary',
                        },
                    });
                }
            );
        },
        updatedObserverDetails() {
            let vm = this;
            this.$refs.observer_detail_datatable.vmDataTable.ajax.reload();
            vm.$emit('refreshOccurrenceReport');
            if (vm.occurrence_report_obj.processing_status == 'Unlocked') {
                vm.$router.go();
            }
        },
        historyObserverDetail: function (id) {
            this.ocrObserverDetailHistoryId = parseInt(id);
            this.$nextTick(() => {
                this.$refs.ocr_observer_detail_history.isModalOpen = true;
            });
        },
    },
};
</script>
