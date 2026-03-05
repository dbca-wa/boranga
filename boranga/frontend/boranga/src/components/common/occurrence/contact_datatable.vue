<template lang="html">
    <div id="contactTable">
        <div class="row mb-3">
            <label for="" class="col-sm-3 control-label fw-bold"
                >Key Contacts:</label
            >
            <div class="col-sm-9 text-end">
                <button
                    :disabled="isReadOnly"
                    type="button"
                    class="btn btn-primary mb-2"
                    @click.prevent="newContactDetail"
                >
                    <i class="bi bi-plus-circle"></i>
                    Add Contact
                </button>
            </div>
        </div>
        <div class="row mb-3">
            <datatable
                id="contactDetailTable"
                ref="contact_detail_datatable"
                :dt-options="contact_detail_options"
                :dt-headers="contact_detail_headers"
            />
        </div>
        <ContactDetail
            v-if="occurrence_obj"
            ref="contact_detail"
            :url="contact_detail_url"
            :occurrence="occurrence_obj"
            @refresh-from-response="refreshFromResponse"
        >
        </ContactDetail>
        <div v-if="occContactDetailHistoryId">
            <OCCContactDetailHistory
                ref="occ_contact_detail_history"
                :key="occContactDetailHistoryId"
                :contact-id="occContactDetailHistoryId"
            />
        </div>
    </div>
</template>
<script>
import datatable from '@vue-utils/datatable.vue';
import ContactDetail from './add_contact_detail.vue';
import OCCContactDetailHistory from '../../internal/occurrence/occ_contact_detail_history.vue';
import { constants, api_endpoints, helpers } from '@/utils/hooks';

export default {
    name: 'ContactTable',
    components: {
        datatable,
        ContactDetail,
        OCCContactDetailHistory,
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
        isReadOnly: {
            type: Boolean,
            default: true,
        },
    },
    emits: ['refreshOccurrenceReport'],
    data: function () {
        let vm = this;
        return {
            occContactDetailHistoryId: null,
            contact_detail_url: api_endpoints.contact_detail,
            contact_detail_headers: [
                'Contact Name',
                'Contact Role',
                'Contact Details',
                'Organisation',
                'Notes',
                'Action',
            ],
            contact_detail_options: {
                autowidth: false,
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
                        vm.occurrence_obj.id + '/contact_details'
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
                        title: 'Boranga Occurrence Contacts Excel Export',
                        text: '<i class="bi bi-download"></i> Excel',
                        className: 'btn btn-primary me-2 rounded',
                        exportOptions: {
                            orthogonal: 'export',
                        },
                    },
                    {
                        extend: 'csv',
                        title: 'Boranga Occurrence Contacts CSV Export',
                        text: '<i class="bi bi-download"></i> CSV',
                        className: 'btn btn-primary rounded',
                        exportOptions: {
                            orthogonal: 'export',
                        },
                    },
                ],
                columns: [
                    {
                        data: 'contact_name',
                        orderable: true,
                        searchable: true,
                        mRender: function (data, type, full) {
                            if (full.visible) {
                                return full.contact_name;
                            } else {
                                return '<s>' + full.contact_name + '</s>';
                            }
                        },
                    },
                    {
                        data: 'role',
                        orderable: true,
                        searchable: true,
                        mRender: function (data, type, full) {
                            if (full.visible) {
                                return full.role;
                            } else {
                                return '<s>' + full.role + '</s>';
                            }
                        },
                    },
                    {
                        data: 'contact',
                        orderable: true,
                        searchable: true,
                        mRender: function (data, type, full) {
                            if (full.visible) {
                                let value = full.contact;
                                let result = helpers.dtPopover(
                                    value,
                                    30,
                                    'hover'
                                );
                                return type == 'export' ? value : result;
                            } else {
                                let value = full.contact;
                                let result = helpers.dtPopover(
                                    value,
                                    30,
                                    'hover'
                                );
                                return type == 'export'
                                    ? value
                                    : '<s>' + result + '</s>';
                            }
                        },
                    },
                    {
                        data: 'organisation',
                        orderable: true,
                        searchable: true,
                        mRender: function (data, type, full) {
                            if (full.visible) {
                                return full.organisation;
                            } else {
                                return '<s>' + full.organisation + '</s>';
                            }
                        },
                    },
                    {
                        data: 'notes',
                        orderable: true,
                        searchable: true,
                        mRender: function (data, type, full) {
                            let value = full.notes;
                            if (type == 'export') {
                                return value;
                            }
                            let result = helpers.dtPopover(value, 60, 'hover');
                            if (full.visible) {
                                return result;
                            } else {
                                return '<s>' + result + '</s>';
                            }
                        },
                    },
                    {
                        data: 'id',
                        mRender: function (data, type, full) {
                            let links = '';
                            if (full.visible) {
                                if (!vm.isReadOnly) {
                                    links += `<a href='#${full.id}' data-edit-contact_det='${full.id}'>Edit</a><br/>`;
                                    links += `<a href='#' data-delete-contact_det='${full.id}'>Discard</a><br>`;
                                } else {
                                    links += `<a href='#${full.id}' data-view-contact_det='${full.id}'>View</a><br/>`;
                                }
                            } else if (!vm.isReadOnly) {
                                links += `<a href='#' data-reinstate-contact_det='${full.id}'>Reinstate</a><br>`;
                            }
                            links += `<a href='#' data-history-contact='${full.id}'>History</a><br>`;
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
                    // another option to fix the responsive table overflow css on tab switch
                    setTimeout(function () {
                        vm.adjust_table_width();
                    }, 100);
                },
            },
        };
    },
    watch: {
        isReadOnly: function () {
            this.$refs.contact_detail_datatable.vmDataTable.ajax.reload();
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
            vm.$refs.contact_detail_datatable.vmDataTable.on(
                'click',
                'a[data-edit-contact_det]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-edit-contact_det');
                    vm.editContactDetail(id);
                }
            );
            vm.$refs.contact_detail_datatable.vmDataTable.on(
                'click',
                'a[data-view-contact_det]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-view-contact_det');
                    vm.viewContactDetail(id);
                }
            );
            // External Discard listener
            vm.$refs.contact_detail_datatable.vmDataTable.on(
                'click',
                'a[data-delete-contact_det]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-delete-contact_det');
                    vm.deleteContactDetail(id);
                }
            );
            vm.$refs.contact_detail_datatable.vmDataTable.on(
                'click',
                'a[data-reinstate-contact_det]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-reinstate-contact_det');
                    vm.reinstateContactDetail(id);
                }
            );
            vm.$refs.contact_detail_datatable.vmDataTable.on(
                'childRow.dt',
                function () {
                    helpers.enablePopovers();
                }
            );
            vm.$refs.contact_detail_datatable.vmDataTable.on(
                'click',
                'a[data-history-contact]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-history-contact');
                    vm.historyContactDetail(id);
                }
            );
        },
        refreshFromResponse: function () {
            let vm = this;
            vm.$refs.contact_detail_datatable.vmDataTable.ajax.reload();
        },
        adjust_table_width: function () {
            let vm = this;
            if (vm.$refs.contact_detail_datatable !== undefined) {
                vm.$refs.contact_detail_datatable.vmDataTable.columns
                    .adjust()
                    .responsive.recalc();
            }
        },
        newContactDetail: function () {
            let vm = this;
            this.$refs.contact_detail.contact_detail_id = '';
            //----for adding new contact
            var new_contact_detail = {
                occurrence: vm.occurrence_obj.id,
                contact_name: '',
                role: '',
                contact: '',
                organisation: '',
                notes: '',
            };
            this.$refs.contact_detail.contactObj = new_contact_detail;
            this.$refs.contact_detail.contact_detail_action = 'add';
            this.$refs.contact_detail.isModalOpen = true;
        },
        editContactDetail: function (id) {
            this.$refs.contact_detail.contact_detail_id = id;
            this.$refs.contact_detail.contact_detail_action = 'edit';
            fetch(
                helpers.add_endpoint_json(api_endpoints.contact_detail, id)
            ).then(
                async (response) => {
                    this.$refs.contact_detail.contactObj =
                        await response.json();
                },
                (err) => {
                    console.log(err);
                }
            );
            this.$refs.contact_detail.isModalOpen = true;
        },
        viewContactDetail: function (id) {
            this.$refs.contact_detail.contact_detail_id = id;
            this.$refs.contact_detail.contact_detail_action = 'view';
            fetch(
                helpers.add_endpoint_json(api_endpoints.contact_detail, id)
            ).then(
                async (response) => {
                    this.$refs.contact_detail.contactObj =
                        await response.json();
                },
                (err) => {
                    console.log(err);
                }
            );
            this.$refs.contact_detail.isModalOpen = true;
        },
        deleteContactDetail: function (id) {
            let vm = this;
            swal.fire({
                title: 'Discard Contact',
                text: 'Are you sure you want to discard this Contact?',
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Discard Contact',
                customClass: {
                    confirmButton: 'btn btn-primary',
                    cancelButton: 'btn btn-secondary me-2',
                },
                reverseButtons: true,
            }).then((result) => {
                if (result.isConfirmed) {
                    fetch(
                        helpers.add_endpoint_json(
                            api_endpoints.contact_detail,
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
                                text: 'The Contact has been discarded',
                                icon: 'success',
                                customClass: {
                                    confirmButton: 'btn btn-primary',
                                },
                            }).then(() => {
                                vm.$refs.contact_detail_datatable.vmDataTable.ajax.reload();
                                vm.$emit('refreshOccurrenceReport');
                                if (
                                    vm.occurrence_obj.processing_status ==
                                    'Unlocked'
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
        reinstateContactDetail: function (id) {
            let vm = this;
            fetch(
                helpers.add_endpoint_json(
                    api_endpoints.contact_detail,
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
                        text: 'The Contact has been reinstated',
                        icon: 'success',
                        customClass: {
                            confirmButton: 'btn btn-primary',
                        },
                    }).then(() => {
                        vm.$refs.contact_detail_datatable.vmDataTable.ajax.reload();
                        vm.$emit('refreshOccurrenceReport');
                        if (vm.occurrence_obj.processing_status == 'Unlocked') {
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
        updatedContactDetails() {
            this.$refs.contact_detail_datatable.vmDataTable.ajax.reload();
        },
        historyContactDetail: function (id) {
            this.occContactDetailHistoryId = parseInt(id);
            this.$nextTick(() => {
                this.$refs.occ_contact_detail_history.isModalOpen = true;
            });
        },
    },
};
</script>
