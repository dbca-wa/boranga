<template id="cs_external_referrals_dashboard">
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
                        <label for="cs_ref_scientific_name_lookup"
                            >Scientific Name:</label
                        >
                        <select
                            id="cs_ref_scientific_name_lookup"
                            ref="cs_ref_scientific_name_lookup"
                            name="cs_ref_scientific_name_lookup"
                            class="form-control"
                        />
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="cs_referrals_community_name_lookup"
                            >Community Name:</label
                        >
                        <select
                            id="cs_referrals_community_name_lookup"
                            ref="cs_referrals_community_name_lookup"
                            name="cs_referrals_community_name_lookup"
                            class="form-control"
                        />
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="">Status:</label>
                        <select
                            v-model="filterCSRefFloraApplicationStatus"
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
                    ref="flora_cs_ref_datatable"
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
    name: 'CSExternalReferralsTable',
    components: {
        datatable,
        CollapsibleFilters,
    },
    props: {
        url: {
            type: String,
            required: true,
        },
        filterCSRefFloraScientificName_cache: {
            type: String,
            required: false,
            default: 'filterCSRefFloraScientificName',
        },
        filterCSRefFloraCommunityName_cache: {
            type: String,
            required: false,
            default: 'filterCSRefFloraCommunityName',
        },
        filterCSRefFloraApplicationStatus_cache: {
            type: String,
            required: false,
            default: 'filterCSRefFloraApplicationStatus',
        },
    },
    data() {
        return {
            datatable_id: 'cs_external_ref-datatable-' + uuid(),

            filterCSRefFloraScientificName: sessionStorage.getItem(
                this.filterCSRefFloraScientificName_cache
            )
                ? sessionStorage.getItem(
                      this.filterCSRefFloraScientificName_cache
                  )
                : 'all',

            filterCSRefFloraCommunityName: sessionStorage.getItem(
                this.filterCSRefFloraCommunityName_cache
            )
                ? sessionStorage.getItem(
                      this.filterCSRefFloraCommunityName_cache
                  )
                : 'all',

            filterCSRefFloraApplicationStatus: sessionStorage.getItem(
                this.filterCSRefFloraApplicationStatus_cache
            )
                ? sessionStorage.getItem(
                      this.filterCSRefFloraApplicationStatus_cache
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
                this.filterCSRefFloraScientificName === 'all' &&
                this.filterCSRefFloraCommunityName === 'all' &&
                this.filterCSRefFloraApplicationStatus === 'all'
            ) {
                return false;
            } else {
                return true;
            }
        },
        datatable_headers: function () {
            return [
                'Number',
                'Type',
                'Scientific Name',
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
                            "<i class='fa fa-exclamation-circle ms-2' style='color:#FFBF00'></i>";
                    } else {
                        tick =
                            "<i class='fa fa-check-circle ms-2' style='color:green'></i>";
                    }
                    return full.conservation_status_number + tick;
                },
                name: 'conservation_status__id, conservation_status__conservation_status_number',
            };
        },
        column_group_type: function () {
            return {
                data: 'group_type',
                orderable: true,
                searchable: true,
                visible: true,
                render: function (data, type, full) {
                    return `<span class="text-capitalize">${full.group_type}</span>`;
                },
            };
        },
        column_scientific_name: function () {
            return {
                data: 'scientific_name',
                orderable: true,
                searchable: true,
                visible: true,
                render: function (value, type) {
                    let result = helpers.dtPopover(value, 30, 'hover');
                    return type == 'export' ? value : result;
                },
                name: 'conservation_status__species__taxonomy__scientific_name',
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
                data: 'id',
                orderable: false,
                searchable: false,
                visible: true,
                render: function (data, type, full) {
                    let links = '';
                    links += full.can_be_processed
                        ? `<a href='/external/conservation-status/${full.conservation_status_id}/referral/${full.id}'>Process</a><br/>`
                        : `<a href='/external/conservation-status/${full.conservation_status_id}/referral/${full.id}'>View</a><br/>`;
                    return links;
                },
            };
        },
        datatable_options: function () {
            let vm = this;

            let columns = [
                vm.column_number,
                vm.column_group_type,
                vm.column_scientific_name,
                vm.column_community_name,
                vm.column_status,
                vm.column_action,
            ];
            let buttons = [
                {
                    extend: 'excel',
                    title: 'Boranga Conservation Status Proposals Referred to Me Excel Export',
                    text: '<i class="fa-solid fa-download"></i> Excel',
                    className: 'btn btn-primary me-2 rounded',
                    exportOptions: {
                        columns: ':not(.no-export)',
                        orthogonal: 'export',
                    },
                },
                {
                    extend: 'csv',
                    title: 'Boranga Conservation Status Proposals Referred to Me CSV Export',
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
                searching: true,
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
                    headers: {
                        'X-CSRFToken': helpers.getCookie('csrftoken'),
                    },
                    data: function (d) {
                        d.filter_scientific_name =
                            vm.filterCSRefFloraScientificName;
                        d.filter_community_name =
                            vm.filterCSRefFloraCommunityName;
                        d.filter_application_status =
                            vm.filterCSRefFloraApplicationStatus;
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
        filterCSRefFloraScientificName: function () {
            let vm = this;
            vm.$refs.flora_cs_ref_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterCSRefFloraScientificName_cache,
                vm.filterCSRefFloraScientificName
            );
        },
        filterCSRefFloraCommunityName: function () {
            let vm = this;
            vm.$refs.flora_cs_ref_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterCSRefFloraCommunityName_cache,
                vm.filterCSRefFloraCommunityName
            );
        },
        filterCSRefFloraApplicationStatus: function () {
            let vm = this;
            vm.$refs.flora_cs_ref_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterCSRefFloraApplicationStatus_cache,
                vm.filterCSRefFloraApplicationStatus
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
            vm.initialiseScientificNameLookup();
            vm.initialiseCommunityNameLookup();
            vm.addEventListeners();
            var newOption = null;
            if (
                sessionStorage.getItem('filterCSRefFloraScientificName') !=
                    'all' &&
                sessionStorage.getItem('filterCSRefFloraScientificName') != null
            ) {
                newOption = new Option(
                    sessionStorage.getItem(
                        'filterCSRefFloraScientificNameText'
                    ),
                    vm.filterCSRefFloraScientificName,
                    false,
                    true
                );
                $('#cs_ref_scientific_name_lookup').append(newOption);
            }
            if (
                sessionStorage.getItem('filterCSRefFloraCommunityName') !=
                    'all' &&
                sessionStorage.getItem('filterCSRefFloraCommunityName') != null
            ) {
                // contructor new Option(text, value, defaultSelected, selected)
                newOption = new Option(
                    sessionStorage.getItem('filterCSRefFloraCommunityNameText'),
                    vm.filterCSRefFloraCommunityName,
                    false,
                    true
                );
                $('#cs_referrals_community_name_lookup').append(newOption);
            }
        });
    },
    methods: {
        initialiseScientificNameLookup: function () {
            let vm = this;
            $(vm.$refs.cs_ref_scientific_name_lookup)
                .select2({
                    minimumInputLength: 2,
                    theme: 'bootstrap-5',
                    allowClear: true,
                    placeholder: 'Select Scientific Name',
                    ajax: {
                        url: api_endpoints.scientific_name_lookup,
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
                    vm.filterCSRefFloraScientificName = data;
                    sessionStorage.setItem(
                        'filterCSRefFloraScientificNameText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.filterCSRefFloraScientificName = 'all';
                    sessionStorage.setItem(
                        'filterCSRefFloraScientificNameText',
                        ''
                    );
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-cs_ref_scientific_name_lookup-results"]'
                    );
                    // move focus to select2 field
                    searchField[0].focus();
                });
        },
        initialiseCommunityNameLookup: function () {
            let vm = this;
            $(vm.$refs.cs_referrals_community_name_lookup)
                .select2({
                    minimumInputLength: 2,
                    theme: 'bootstrap-5',
                    allowClear: true,
                    placeholder: 'Select Community Name',
                    ajax: {
                        url: api_endpoints.communities_lookup,
                        dataType: 'json',
                        data: function (params) {
                            var query = {
                                term: params.term,
                                type: 'public',
                            };
                            return query;
                        },
                    },
                })
                .on('select2:select', function (e) {
                    let data = e.params.data.id;
                    vm.filterCSRefFloraCommunityName = data;
                    sessionStorage.setItem(
                        'filterCSRefFloraCommunityNameText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.filterCSRefFloraCommunityName = 'all';
                    sessionStorage.setItem(
                        'filterCSRefFloraCommunityNameText',
                        ''
                    );
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-cs_referrals_community_name_lookup-results"]'
                    );
                    // move focus to select2 field
                    searchField[0].focus();
                });
        },
        addEventListeners: function () {
            let vm = this;
            vm.$refs.flora_cs_ref_datatable.vmDataTable.on(
                'childRow.dt',
                function () {
                    helpers.enablePopovers();
                }
            );
        },
    },
};
</script>
