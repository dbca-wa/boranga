<template lang="html">
    <div id="speciesHistory">
        <modal
            transition="modal fade"
            :title="'Species S' + speciesId + ' - History'"
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
                                <div v-if="speciesId" class="col-lg-12">
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
                                                'S' + speciesId
                                            "
                                            :revision_id="historyId"
                                            :revision_sequence="historySequence"
                                            :primary_model="'Species'"
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
    name: 'SpeciesHistory',
    components: {
        modal,
        alert,
        datatable,
        DisplayHistory,
    },
    props: {
        speciesId: {
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
                'Scientific Name',
                'Common Name',
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
                    return full.data.data.species;
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
                    if (full.data.species.fields.species_number) {
                        return (
                            full.data.species.fields.species_number +
                            '-' +
                            full.revision_sequence
                        );
                    } else {
                        return (
                            'S' +
                            full.data.species.pk +
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
                data: 'data.data.species.pk',
                orderable: false,
                searchable: false,
                visible: false,
                render: function (row, type, full) {
                    return full.data.species.pk;
                },
                name: 'id',
            };
        },
        column_number: function () {
            return {
                // 2. Number
                data: 'data.data.species.fields.species_number',
                orderable: false,
                searchable: false,
                visible: true,
                render: function (row, type, full) {
                    return full.data.species.fields.species_number;
                },
                name: 'species_number',
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
        column_scientific_name: function () {
            return {
                data: 'data.data.taxonomy.fields.scientific_name',
                defaultContent: '',
                orderable: false,
                searchable: true,
                visible: true,
                render: function (row, type, full) {
                    if (
                        full.data.taxonomy !== undefined &&
                        full.data.taxonomy.fields !== undefined
                    ) {
                        //return full.data.taxonomy.fields.scientific_name;
                        let value = full.data.taxonomy.fields.scientific_name;
                        let result = helpers.dtPopover(value, 30, 'hover');
                        return type == 'export' ? value : result;
                    } else {
                        return '';
                    }
                },
                name: 'scientific_name', //_name',
            };
        },
        column_non_current_name: function () {
            return {
                data: 'data.data.taxonpreviousname.fields.previous_scientific_name',
                defaultContent: '',
                orderable: false,
                searchable: true,
                visible: true,
                render: function (row, type, full) {
                    if (
                        full.data.taxonpreviousname !== undefined &&
                        full.data.taxonpreviousname.fields !== undefined
                    ) {
                        let value =
                            full.data.taxonpreviousname.fields
                                .previous_scientific_name;
                        let result = helpers.dtPopover(value, 30, 'hover');
                        return type == 'export' ? value : result;
                    } else if (
                        full.data.taxonpreviousname !== undefined &&
                        full.data.taxonpreviousname.fields === undefined &&
                        full.data.taxonpreviousname.length > 0
                    ) {
                        let value =
                            full.data.taxonpreviousname[0].fields
                                .previous_scientific_name;
                        let result = helpers.dtPopover(value, 30, 'hover');
                        return type == 'export' ? value : result;
                    } else {
                        return '';
                    }
                },
                name: 'previous_scientific_name', //_name',
            };
        },
        column_common_name: function () {
            return {
                data: 'data.data.taxonvernacular.fields.vernacular_name',
                defaultContent: '',
                orderable: false,
                searchable: true,
                visible: true,
                render: function (row, type, full) {
                    if (full.data.taxonvernacular !== undefined) {
                        //list not dict
                        if (full.data.taxonvernacular.fields === undefined) {
                            var combined_name = '';
                            for (
                                var i = 0;
                                i < full.data.taxonvernacular.length;
                                i++
                            ) {
                                if (i == 0) {
                                    combined_name =
                                        full.data.taxonvernacular[i].fields
                                            .vernacular_name;
                                } else {
                                    combined_name +=
                                        ',' +
                                        full.data.taxonvernacular[i].fields
                                            .vernacular_name;
                                }
                            }
                            //return combined_name;
                            let value = combined_name;
                            let result = helpers.dtPopover(value, 30, 'hover');
                            return type == 'export' ? value : result;
                        }
                        //return full.data.taxonvernacular.fields.vernacular_name;
                        let value =
                            full.data.taxonvernacular.fields.vernacular_name;
                        let result = helpers.dtPopover(value, 30, 'hover');
                        return type == 'export' ? value : result;
                    } else {
                        return '';
                    }
                },
                name: 'vernacular_name', //_name',
            };
        },
        column_processing_status: function () {
            return {
                data: 'data.data.species.fields.processing_status',
                defaultContent: '',
                orderable: true,
                searchable: false,
                visible: true,
                render: function (row, type, full) {
                    if (
                        full.data.speciespublishingstatus !== undefined &&
                        full.data.species.fields.processing_status === 'active'
                    ) {
                        let public_status = full.data.speciespublishingstatus
                            .fields.species_public
                            ? 'public'
                            : 'private';
                        return (
                            full.data.species.fields.processing_status +
                            ' - ' +
                            public_status
                        );
                    }
                    return full.data.species.fields.processing_status;
                },
                name: 'processing_status',
            };
        },
        column_comment: function () {
            return {
                data: 'data.data.species.fields.comment',
                defaultContent: '',
                orderable: false,
                searchable: true,
                visible: true,
                render: function (row, type, full) {
                    //return full.data.species.fields.comment;
                    let value = full.data.species.fields.comment;
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
                vm.column_scientific_name,
                vm.column_common_name,
                vm.column_non_current_name,
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
                        api_endpoints.lookup_history_species(this.speciesId) +
                        '?format=datatables',
                    dataSrc: 'data',
                },
                buttons: [
                    {
                        extend: 'excel',
                        title: 'Boranga Species History Excel Export',
                        text: '<i class="fa-solid fa-download"></i> Excel',
                        className: 'btn btn-primary me-2 rounded',
                        exportOptions: {
                            orthogonal: 'export',
                        },
                    },
                    {
                        extend: 'csv',
                        title: 'Boranga Species History CSV Export',
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
