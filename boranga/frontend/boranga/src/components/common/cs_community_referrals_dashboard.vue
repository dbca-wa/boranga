<template id="cs_communities_referrals_dashboard">
    <div>
        <CollapsibleFilters
            ref="collapsible_filters"
            component_title="Filters"
            class="mb-2"
            :show-warning-icon="filterApplied"
        >
            <div class="row">
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="cs_ref_community_id_lookup"
                            >Community ID:</label
                        >
                        <select
                            id="cs_ref_community_id_lookup"
                            ref="cs_ref_community_id_lookup"
                            name="cs_ref_community_id_lookup"
                            class="form-control"
                        />
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="cs_ref_community_name_lookup"
                            >Community Name:</label
                        >
                        <select
                            id="cs_ref_community_name_lookup"
                            ref="cs_ref_community_name_lookup"
                            name="cs_ref_community_name_lookup"
                            class="form-control"
                        />
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="">Status:</label>
                        <select
                            v-model="filterCSRefCommunityApplicationStatus"
                            class="form-select"
                        >
                            <option value="all">All</option>
                            <option
                                v-for="status in processing_statuses"
                                :value="status.value"
                                :key="status.value"
                            >
                                {{ status.name }}
                            </option>
                        </select>
                    </div>
                </div>
            </div>
        </CollapsibleFilters>
        <div class="row">
            <div class="col-lg-12">
                <datatable
                    :id="datatable_id"
                    ref="cs_communities_ref_datatable"
                    :dt-options="datatable_options"
                    :dt-headers="datatable_headers"
                />
            </div>
        </div>
    </div>
</template>
<script>
import { v4 as uuid } from 'uuid';
import datatable from '@/utils/vue/datatable.vue';
import CollapsibleFilters from '@/components/forms/collapsible_component.vue';

import { api_endpoints, constants, helpers } from '@/utils/hooks';
export default {
    name: 'CSReferralsCommunityTable',
    components: {
        datatable,
        CollapsibleFilters,
    },
    props: {
        group_type_name: {
            type: String,
            required: true,
        },
        group_type_id: {
            type: Number,
            required: true,
            default: 0,
        },
        url: {
            type: String,
            required: true,
        },
        filterCSRefCommunityMigratedId_cache: {
            type: String,
            required: false,
            default: 'filterCSRefCommunityMigratedId',
        },
        filterCSRefCommunityName_cache: {
            type: String,
            required: false,
            default: 'filterCSRefCommunityName',
        },
        filterCSRefCommunityConservationList_cache: {
            type: String,
            required: false,
            default: 'filterCSRefCommunityConservationList',
        },
        filterCSRefCommunityConservationCategory_cache: {
            type: String,
            required: false,
            default: 'filterCSRefCommunityConservationCategory',
        },
        filterCSRefCommunityApplicationStatus_cache: {
            type: String,
            required: false,
            default: 'filterCSRefCommunityApplicationStatus',
        },
    },
    data() {
        return {
            datatable_id: 'cs-communities-ref-datatable-' + uuid(),

            filterCSRefCommunityMigratedId: sessionStorage.getItem(
                this.filterCSRefCommunityMigratedId_cache
            )
                ? sessionStorage.getItem(
                      this.filterCSRefCommunityMigratedId_cache
                  )
                : 'all',

            filterCSRefCommunityName: sessionStorage.getItem(
                this.filterCSRefCommunityName_cache
            )
                ? sessionStorage.getItem(this.filterCSRefCommunityName_cache)
                : 'all',

            filterCSRefCommunityConservationList: sessionStorage.getItem(
                this.filterCSRefCommunityConservationList_cache
            )
                ? sessionStorage.getItem(
                      this.filterCSRefCommunityConservationList_cache
                  )
                : 'all',

            filterCSRefCommunityConservationCategory: sessionStorage.getItem(
                this.filterCSRefCommunityRefConservationCategory_cache
            )
                ? sessionStorage.getItem(
                      this.filterCSRefCommunityRefConservationCategory_cache
                  )
                : 'all',

            filterCSRefCommunityApplicationStatus: sessionStorage.getItem(
                this.filterCSRefCommunityApplicationStatus_cache
            )
                ? sessionStorage.getItem(
                      this.filterCSRefCommunityApplicationStatus_cache
                  )
                : 'all',

            processing_statuses: [
                { value: 'with_referral', name: 'Awaiting' },
                { value: 'completed', name: 'Completed' },
            ],
        };
    },
    computed: {
        filterApplied: function () {
            if (
                this.filterCSRefCommunityMigratedId === 'all' &&
                this.filterCSRefCommunityName === 'all' &&
                this.filterCSRefCommunityConservationList === 'all' &&
                this.filterCSRefCommunityConservationCategory === 'all' &&
                this.filterCSRefCommunityApplicationStatus === 'all'
            ) {
                return false;
            } else {
                return true;
            }
        },
        datatable_headers: function () {
            return [
                'Number',
                'Community',
                'Community Id',
                'Community Name',
                'Status',
                'Action',
            ];
        },
        column_number: function () {
            return {
                data: 'conservation_status_number',
                orderable: true,
                searchable: true,
                visible: true,
                render: function (data, type, full) {
                    let tick = '';
                    if (full.can_be_processed) {
                        tick =
                            " <i class='fa fa-exclamation-circle' style='color:#FFBF00'></i>";
                    } else {
                        tick =
                            " <i class='fa fa-check-circle' style='color:green'></i>";
                    }
                    return full.conservation_status_number + tick;
                },
                name: 'conservation_status__id, conservation_status__conservation_status_number',
            };
        },
        column_community_number: function () {
            return {
                data: 'community_number',
                orderable: true,
                searchable: true,
                visible: true,
                name: 'conservation_status__community__community_number',
            };
        },
        column_community_id: function () {
            return {
                data: 'community_common_id',
                orderable: true,
                searchable: true,
                visible: true,
                render: function (value, type) {
                    let result = helpers.dtPopover(value, 30, 'hover');
                    return type == 'export' ? value : result;
                },
                name: 'conservation_status__community__taxonomy__community_common_id',
            };
        },
        column_community_name: function () {
            return {
                data: 'community_name',
                orderable: true,
                searchable: true,
                visible: true,
                render: function (value, type) {
                    let result = helpers.dtPopover(value, 30, 'hover');
                    return type == 'export' ? value : result;
                },
                name: 'conservation_status__community__taxonomy__community_name',
            };
        },
        column_status: function () {
            return {
                data: 'processing_status',
                orderable: true,
                searchable: true,
                visible: true,
                name: 'processing_status',
            };
        },
        column_action: function () {
            return {
                data: 'conservation_status_id',
                orderable: false,
                searchable: false,
                visible: true,
                render: function (data, type, full) {
                    let links = '';
                    links += full.can_be_processed
                        ? `<a href='/internal/conservation-status/${full.conservation_status_id}/referral/${full.id}'>Process</a><br/>`
                        : `<a href='/internal/conservation-status/${full.conservation_status_id}/referral/${full.id}'>View</a><br/>`;
                    return links;
                },
            };
        },
        column_can_be_processed: function () {
            return {
                data: 'can_be_processed',
                visible: false,
            };
        },
        datatable_options: function () {
            let vm = this;

            let columns = [
                vm.column_number,
                vm.column_community_number,
                vm.column_community_id,
                vm.column_community_name,
                vm.column_status,
                vm.column_action,
                vm.column_can_be_processed,
            ];
            let search = false;
            let buttons = [
                {
                    extend: 'excel',
                    title: 'Boranga CS Communities Proposals Referred to Me Excel Export',
                    text: '<i class="fa-solid fa-download"></i> Excel',
                    className: 'btn btn-primary me-2 rounded',
                    exportOptions: {
                        columns: ':not(.no-export)',
                        orthogonal: 'export',
                    },
                },
                {
                    extend: 'csv',
                    title: 'Boranga CS Communities Proposals Referred to Me CSV Export',
                    text: '<i class="fa-solid fa-download"></i> CSV',
                    className: 'btn btn-primary rounded',
                    exportOptions: {
                        columns: ':not(.no-export)',
                        orthogonal: 'export',
                    },
                },
            ];

            return {
                autoWidth: false,
                language: {
                    processing: constants.DATATABLE_PROCESSING_HTML,
                },
                order: [[0, 'desc']],
                lengthMenu: [
                    [10, 25, 50, 100, 100000000],
                    [10, 25, 50, 100, 'All'],
                ],
                responsive: true,
                serverSide: true,
                searching: search,
                //  to show the "workflow Status","Action" columns always in the last position
                columnDefs: [
                    { responsivePriority: 1, targets: 0 },
                    {
                        responsivePriority: 3,
                        targets: -1,
                        className: 'no-export',
                    },
                    { responsivePriority: 2, targets: -2 },
                ],
                ajax: {
                    url: this.url,
                    dataSrc: 'data',
                    method: 'post',
                    headers: {
                        'X-CSRFToken': helpers.getCookie('csrftoken'),
                    },
                    // adding extra GET params for Custom filtering
                    data: function (d) {
                        d.filter_community_common_id =
                            vm.filterCSRefCommunityMigratedId;
                        d.filter_community_name = vm.filterCSRefCommunityName;
                        d.filter_conservation_list =
                            vm.filterCSRefCommunityConservationList;
                        d.filter_conservation_category =
                            vm.filterCSRefCommunityConservationCategory;
                        d.filter_group_type = vm.group_type_name;
                        d.filter_application_status =
                            vm.filterCSRefCommunityApplicationStatus;
                        d.is_internal = vm.is_internal;
                    },
                },
                dom:
                    "<'d-flex align-items-center'<'me-auto'l>fB>" +
                    "<'row'<'col-sm-12'tr>>" +
                    "<'d-flex align-items-center'<'me-auto'i>p>",
                buttons: buttons,
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
        filterCSRefCommunityMigratedId: function () {
            let vm = this;
            vm.$refs.cs_communities_ref_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterCSRefCommunityMigratedId_cache,
                vm.filterCSRefCommunityMigratedId
            );
        },
        filterCSRefCommunityName: function () {
            let vm = this;
            vm.$refs.cs_communities_ref_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterCSRefCommunityName_cache,
                vm.filterCSRefCommunityName
            );
        },
        filterCSRefCommunityConservationList: function () {
            let vm = this;
            vm.$refs.cs_communities_ref_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterCSRefCommunityConservationList_cache,
                vm.filterCSRefCommunityConservationList
            );
        },
        filterCSRefCommunityConservationCategory: function () {
            let vm = this;
            vm.$refs.cs_communities_ref_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterCSRefCommunityConservationCategory_cache,
                vm.filterCSRefCommunityConservationCategory
            );
        },
        filterCSRefCommunityApplicationStatus: function () {
            let vm = this;
            vm.$refs.cs_communities_ref_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterCSRefCommunityApplicationStatus_cache,
                vm.filterCSRefCommunityApplicationStatus
            );
        },
    },
    mounted: function () {
        let vm = this;
        $('a[data-toggle="collapse"]').on('click', function () {
            var chev = $(this).children()[0];
            window.setTimeout(function () {
                $(chev).toggleClass(
                    'glyphicon-chevron-down glyphicon-chevron-up'
                );
            }, 100);
        });
        this.$nextTick(() => {
            vm.initialiseCommunityNameLookup();
            vm.initialiseCommunityIdLookup();
            vm.addEventListeners();
            var newOption = null;
            if (
                sessionStorage.getItem('filterCSRefCommunityName') != 'all' &&
                sessionStorage.getItem('filterCSRefCommunityName') != null
            ) {
                newOption = new Option(
                    sessionStorage.getItem('filterCSRefCommunityNameText'),
                    vm.filterCSRefCommunityName,
                    false,
                    true
                );
                $('#cs_ref_community_name_lookup').append(newOption);
            }
            if (
                sessionStorage.getItem('filterCSRefCommunityMigratedId') !=
                    'all' &&
                sessionStorage.getItem('filterCSRefCommunityMigratedId') != null
            ) {
                newOption = new Option(
                    sessionStorage.getItem(
                        'filterCSRefCommunityMigratedIdText'
                    ),
                    vm.filterCSRefCommunityMigratedId,
                    false,
                    true
                );
                $('#cs_ref_community_id_lookup').append(newOption);
            }
        });
    },
    methods: {
        initialiseCommunityNameLookup: function () {
            let vm = this;
            $(vm.$refs.cs_ref_community_name_lookup)
                .select2({
                    minimumInputLength: 2,
                    theme: 'bootstrap-5',
                    allowClear: true,
                    placeholder: 'Select Community Name',
                    ajax: {
                        url: api_endpoints.community_name_lookup,
                        dataType: 'json',
                        data: function (params) {
                            var query = {
                                term: params.term,
                                type: 'public',
                                cs_referral: true,
                            };
                            return query;
                        },
                    },
                })
                .on('select2:select', function (e) {
                    let data = e.params.data.id;
                    vm.filterCSRefCommunityName = data;
                    sessionStorage.setItem(
                        'filterCSRefCommunityNameText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.filterCSRefCommunityName = 'all';
                    sessionStorage.setItem('filterCSRefCommunityNameText', '');
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-cs_ref_community_name_lookup-results"]'
                    );
                    searchField[0].focus();
                });
        },
        initialiseCommunityIdLookup: function () {
            let vm = this;
            $(vm.$refs.cs_ref_community_id_lookup)
                .select2({
                    minimumInputLength: 1,
                    theme: 'bootstrap-5',
                    allowClear: true,
                    placeholder: 'Select Community ID',
                    ajax: {
                        url: api_endpoints.community_id_lookup,
                        dataType: 'json',
                        data: function (params) {
                            var query = {
                                term: params.term,
                                type: 'public',
                                cs_referral: true,
                            };
                            return query;
                        },
                    },
                })
                .on('select2:select', function (e) {
                    let data = e.params.data.id;
                    vm.filterCSRefCommunityMigratedId = data;
                    sessionStorage.setItem(
                        'filterCSRefCommunityMigratedIdText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.filterCSRefCommunityMigratedId = 'all';
                    sessionStorage.setItem(
                        'filterCSRefCommunityMigratedIdText',
                        ''
                    );
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-cs_ref_community_id_lookup-results"]'
                    );
                    searchField[0].focus();
                });
        },
        addEventListeners: function () {
            let vm = this;
            vm.$refs.cs_communities_ref_datatable.vmDataTable.on(
                'childRow.dt',
                function () {
                    helpers.enablePopovers();
                }
            );
        },
    },
};
</script>
