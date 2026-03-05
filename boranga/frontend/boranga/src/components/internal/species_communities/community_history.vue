<template lang="html">
    <div id="communityHistory">
        <modal
            transition="modal fade"
            :title="'Community C' + communityId + ' - History'"
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
                                <div v-if="communityId" class="col-lg-12">
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
                                                'C' + communityId
                                            "
                                            :revision_id="historyId"
                                            :revision_sequence="historySequence"
                                            :primary_model="'Community'"
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
    name: 'CommunityHistory',
    components: {
        modal,
        alert,
        datatable,
        DisplayHistory,
    },
    props: {
        communityId: {
            type: Number,
            required: true,
        },
    },
    data: function () {
        return {
            scientificName: '',
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
                'Community Name',
                'Previous Name',
                'Processing Status',
                'Comment',
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
                    return full.data.data.community;
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
                    if (full.data.community.fields.community_number) {
                        return (
                            full.data.community.fields.community_number +
                            '-' +
                            full.revision_sequence
                        );
                    } else {
                        return (
                            'C' +
                            full.data.community.pk +
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
                data: 'data.data.community.pk',
                orderable: false,
                searchable: false,
                visible: false,
                render: function (row, type, full) {
                    return full.data.community.pk;
                },
                name: 'id',
            };
        },
        column_number: function () {
            return {
                // 2. Number
                data: 'data.data.community.fields.community_number',
                orderable: false,
                searchable: false,
                visible: true,
                render: function (row, type, full) {
                    return full.data.community.fields.community_number;
                },
                name: 'community_number',
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
        column_community_name: function () {
            return {
                data: 'data.communitytaxonomy.fields.community_name',
                defaultContent: '',
                orderable: false,
                searchable: true,
                visible: true,
                name: 'community_name',
            };
        },
        column_previous_name: function () {
            return {
                data: 'data.communitytaxonomy.fields.previous_name',
                defaultContent: '',
                orderable: false,
                searchable: false,
                visible: true,
                name: 'previous_name', //_name',
            };
        },
        column_processing_status: function () {
            return {
                data: 'data.data.community.fields.processing_status',
                defaultContent: '',
                orderable: true,
                searchable: false,
                visible: true,
                render: function (row, type, full) {
                    if (
                        full.data.communitypublishingstatus !== undefined &&
                        full.data.community.fields.processing_status ===
                            'active'
                    ) {
                        let public_status = full.data.communitypublishingstatus
                            .fields.community_public
                            ? 'public'
                            : 'private';
                        return (
                            full.data.community.fields.processing_status +
                            ' - ' +
                            public_status
                        );
                    }
                    return full.data.community.fields.processing_status;
                },
                name: 'processing_status',
            };
        },
        column_comment: function () {
            return {
                data: 'data.data.community.fields.comment',
                defaultContent: '',
                orderable: false,
                searchable: true,
                visible: true,
                render: function (row, type, full) {
                    //return full.data.community.fields.comment;
                    let value = full.data.community.fields.comment;
                    let result = helpers.dtPopover(value, 30, 'hover');
                    return type == 'export' ? value : result;
                },
                name: 'comment',
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
                vm.column_revision_date,
                vm.column_revision_user,
                vm.column_community_name,
                vm.column_previous_name,
                vm.column_processing_status,
                vm.column_comment,
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
                        api_endpoints.lookup_history_community(
                            this.communityId
                        ) + '?format=datatables',
                    dataSrc: 'data',
                },
                buttons: [
                    {
                        extend: 'excel',
                        title: 'Boranga Community History Excel Export',
                        text: '<i class="bi bi-download"></i> Excel',
                        className: 'btn btn-primary me-2 rounded',
                        exportOptions: {
                            orthogonal: 'export',
                        },
                    },
                    {
                        extend: 'csv',
                        title: 'Boranga Community History CSV Export',
                        text: '<i class="bi bi-download"></i> CSV',
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
