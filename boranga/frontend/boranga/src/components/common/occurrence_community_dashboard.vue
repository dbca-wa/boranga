<template id="communities_occ_dashboard">
    <div>
        <CollapsibleFilters
            ref="collapsible_filters"
            component_title="Filters"
            class="mb-2"
            :show-warning-icon="filterApplied"
        >
            <div class="row">
                <div class="col-md-4">
                    <div
                        id="occurrence_name_lookup_form_group_id"
                        class="form-group"
                    >
                        <label for="occurrence_name_lookup"
                            >Name of Occurrence:</label
                        >
                        <select
                            id="occurrence_name_lookup"
                            ref="occurrence_name_lookup"
                            name="occurrence_name_lookup"
                            class="form-control"
                        />
                    </div>
                </div>
                <div class="col-md-4">
                    <div id="select_occ_community_name" class="form-group">
                        <label for="occ_community_name_lookup"
                            >Community Name:</label
                        >
                        <select
                            id="occ_community_name_lookup"
                            ref="occ_community_name_lookup"
                            name="occ_community_name_lookup"
                            class="form-control"
                        />
                    </div>
                </div>
                <div class="col-md-4">
                    <div id="select_status" class="form-group">
                        <label for="occ_status_lookup">Status:</label>
                        <select
                            v-model="filterOCCCommunityStatus"
                            class="form-select"
                        >
                            <option value="all">All</option>
                            <option
                                v-for="status in proposal_status"
                                :value="status.value"
                                :key="status.value"
                            >
                                {{ status.name }}
                            </option>
                        </select>
                    </div>
                </div>
            </div>
            <div class="row">
                <div class="col-md-4">
                    <div class="form-group">
                        <label for="occ_community_id_lookup"
                            >Community ID:</label
                        >
                        <select
                            id="occ_community_id_lookup"
                            ref="occ_community_id_lookup"
                            name="occ_community_id_lookup"
                            class="form-control"
                        />
                    </div>
                </div>
                <div class="col-md-4">
                    <div class="form-group">
                        <label for="submitter-category">Locked:</label>
                        <select
                            id="submitter-category"
                            v-model="filterOCCCommunityLocked"
                            class="form-select"
                        >
                            <option value="all">All</option>
                            <option value="true">Yes</option>
                            <option value="false">No</option>
                        </select>
                    </div>
                </div>
            </div>
        </CollapsibleFilters>

        <div v-if="show_add_button" class="col-md-12">
            <div class="text-end">
                <button
                    type="button"
                    class="btn btn-primary mb-2"
                    @click.prevent="createCommunityOccurrence"
                >
                    <i class="fa-solid fa-circle-plus"></i> Add Community
                    Occurrence
                </button>
            </div>
        </div>

        <div class="row">
            <div class="col-lg-12">
                <datatable
                    :id="datatable_id"
                    ref="community_occ_datatable"
                    :dt-options="datatable_options"
                    :dt-headers="datatable_headers"
                />
            </div>
            <div v-if="occurrenceHistoryId">
                <OccurrenceReportHistory
                    ref="occurrence_history"
                    :key="occurrenceHistoryId"
                    :occurrence-id="occurrenceHistoryId"
                />
            </div>
        </div>
    </div>
</template>
<script>
import { v4 as uuid } from 'uuid';
import datatable from '@/utils/vue/datatable.vue';
import CollapsibleFilters from '@/components/forms/collapsible_component.vue';
import OccurrenceReportHistory from '../internal/occurrence/community_occurrence_history.vue';

import { api_endpoints, constants, helpers } from '@/utils/hooks';
export default {
    name: 'OccurrenceReportCommunityTable',
    components: {
        datatable,
        CollapsibleFilters,
        OccurrenceReportHistory,
    },
    props: {
        level: {
            type: String,
            required: true,
            validator: function (val) {
                let options = ['internal', 'referral', 'external'];
                return options.indexOf(val) != -1 ? true : false;
            },
        },
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
        profile: {
            type: Object,
            default: null,
        },
        filterOCCCommunityMigratedId_cache: {
            type: String,
            required: false,
            default: 'filterOCCCommunityMigratedId',
        },
        filterOCCCommunityOccurrenceName_cache: {
            type: String,
            required: false,
            default: 'filterOCCCommunityOccurrenceName',
        },
        filterOCCCommunityName_cache: {
            type: String,
            required: false,
            default: 'filterOCCCommunityName',
        },
        filterOCCCommunityStatus_cache: {
            type: String,
            required: false,
            default: 'filterOCCCommunityStatus',
        },
        filterOCCCommunityLocked_cache: {
            type: String,
            required: false,
            default: 'filterOCCCommunityLocked',
        },
        filterOCCFromCommunityDueDate_cache: {
            type: String,
            required: false,
            default: 'filterOCCFromCommunityDueDate',
        },
        filterOCCToCommunityDueDate_cache: {
            type: String,
            required: false,
            default: 'filterOCCToCommunityDueDate',
        },
    },
    data() {
        return {
            uuid: 0,
            occurrenceHistoryId: null,
            datatable_id: 'occurrence-community-datatable-' + uuid(),

            // selected values for filtering
            filterOCCCommunityMigratedId: sessionStorage.getItem(
                this.filterOCCCommunityMigratedId_cache
            )
                ? sessionStorage.getItem(
                      this.filterOCCCommunityMigratedId_cache
                  )
                : 'all',
            filterOCCCommunityOccurrenceName: sessionStorage.getItem(
                this.filterOCCCommunityOccurrenceName_cache
            )
                ? sessionStorage.getItem(
                      this.filterOCCCommunityOccurrenceName_cache
                  )
                : 'all',

            filterOCCCommunityName: sessionStorage.getItem(
                this.filterOCCCommunityName_cache
            )
                ? sessionStorage.getItem(this.filterOCCCommunityName_cache)
                : 'all',

            filterOCCCommunityStatus: sessionStorage.getItem(
                this.filterOCCCommunityStatus_cache
            )
                ? sessionStorage.getItem(this.filterOCCCommunityStatus_cache)
                : 'all',

            filterOCCCommunityLocked: sessionStorage.getItem(
                this.filterOCCCommunityLocked_cache
            )
                ? sessionStorage.getItem(this.filterOCCCommunityLocked_cache)
                : 'all',

            filterOCCFromCommunityDueDate: sessionStorage.getItem(
                this.filterOCCFromCommunityDueDate_cache
            )
                ? sessionStorage.getItem(
                      this.filterOCCFromCommunityDueDate_cache
                  )
                : '',
            filterOCCToCommunityDueDate: sessionStorage.getItem(
                this.filterOCCToCommunityDueDate_cache
            )
                ? sessionStorage.getItem(this.filterOCCToCommunityDueDate_cache)
                : '',

            filterListsCommunity: {},
            occurrence_list: [],
            community_name_list: [],
            status_list: [],
            submissions_from_list: [],
            submissions_to_list: [],

            // filtering options
            // external_status refers to CUSTOMER_STATUS_CHOICES
            // internal_status referes to PROCESSING_STATUS_CHOICES
            internal_status: [
                { value: 'draft', name: 'Draft' },
                { value: 'discarded', name: 'Discarded' },
                { value: 'active', name: 'Active' },
                { value: 'historical', name: 'Historical' },
            ],

            proposal_status: [],
        };
    },
    computed: {
        show_add_button: function () {
            return (
                this.profile?.user &&
                this.profile.user.groups.includes(
                    constants.GROUPS.OCCURRENCE_APPROVERS
                )
            );
        },
        filterApplied: function () {
            if (
                this.filterOCCCommunityMigratedId === 'all' &&
                this.filterOCCCommunityOccurrenceName === 'all' &&
                this.filterOCCCommunityName === 'all' &&
                this.filterOCCCommunityStatus === 'all' &&
                this.filterOCCFromCommunityDueDate === '' &&
                this.filterOCCToCommunityDueDate === '' &&
                this.filterOCCCommunityLocked === 'all'
            ) {
                return false;
            } else {
                return true;
            }
        },
        is_external: function () {
            return this.level == 'external';
        },
        is_internal: function () {
            return this.level == 'internal';
        },
        is_referral: function () {
            return this.level == 'referral';
        },
        addCommunityOCCVisibility: function () {
            let visibility = false;
            if (this.is_internal) {
                visibility = true;
            }
            return visibility;
        },
        datatable_headers: function () {
            return [
                'ID',
                'Number',
                'Name of Occurrence',
                'Community Name',
                'Wild Status',
                'Number of Reports',
                'Community ID',
                'Migrated From ID',
                'Review Due',
                'Status',
                'Action',
            ];
        },
        column_id: function () {
            return {
                data: 'id',
                orderable: true,
                searchable: false,
                visible: false,
                render: function (data, type, full) {
                    return full.id;
                },
                name: 'id',
            };
        },
        column_number: function () {
            return {
                data: 'occurrence_number',
                orderable: true,
                searchable: true,
                visible: true,
                name: 'occurrence_number',
            };
        },
        column_occurrence_name: function () {
            return {
                data: 'occurrence_name',
                orderable: true,
                searchable: true,
                visible: true,
                name: 'occurrence_name',
                render: function (data, type, full) {
                    if (full.occurrence_name) {
                        let value = full.occurrence_name;
                        let result = helpers.dtPopover(value, 30, 'hover');
                        return type == 'export' ? value : result;
                    }
                    return '';
                },
            };
        },
        column_community_common_id: function () {
            return {
                data: 'community_common_id',
                orderable: true,
                searchable: true,
                visible: true,
                render: function (data, type, full) {
                    if (full.community_common_id) {
                        let value = full.community_common_id;
                        let result = helpers.dtPopover(value, 30, 'hover');
                        return type == 'export' ? value : result;
                    }
                    return '';
                },
                name: 'community__taxonomy__community_common_id',
            };
        },
        column_community_name: function () {
            return {
                data: 'community_name',
                orderable: true,
                searchable: true,
                visible: true,
                render: function (data, type, full) {
                    if (full.community_name) {
                        let value = full.community_name;
                        let result = helpers.dtPopover(value, 30, 'hover');
                        return type == 'export' ? value : result;
                    }
                    return '';
                },
                name: 'community__taxonomy__community_name',
            };
        },
        column_number_of_reports: function () {
            return {
                data: 'number_of_reports',
                orderable: true,
                searchable: false,
                visible: true,
            };
        },
        column_migrated_from_id: function () {
            return {
                data: 'migrated_from_id',
                orderable: false,
                searchable: true,
            };
        },
        column_wild_status: function () {
            return {
                data: 'wild_status',
                orderable: false,
                searchable: false,
                visible: true,
            };
        },
        column_review_due_date: function () {
            return {
                data: 'review_due_date',
                orderable: true,
                searchable: true,
                visible: true,
                name: 'review_due_date',
            };
        },
        column_status: function () {
            return {
                data: 'processing_status_display',
                orderable: true,
                searchable: true,
                visible: true,
                name: 'processing_status',
                render: function (data, type, full) {
                    let html = full.processing_status_display;
                    if (!full.show_locked_indicator) {
                        return html;
                    }
                    if (full.locked) {
                        html +=
                            '<i class="bi bi-lock-fill ms-2 text-warning"></i>';
                    } else {
                        html +=
                            '<i class="bi bi-unlock-fill ms-2 text-secondary"></i>';
                    }
                    return html;
                },
            };
        },
        column_action: function () {
            let vm = this;
            return {
                data: 'id',
                orderable: false,
                searchable: false,
                visible: true,
                render: function (data, type, full) {
                    let links = '';
                    if (vm.is_internal) {
                        if (full.can_user_edit) {
                            if (full.processing_status == 'discarded') {
                                links += `<a href='#' data-reinstate-occ-proposal='${full.id}'>Reinstate</a><br/>`;
                                return links;
                            } else {
                                links += `<a href='/internal/occurrence/${full.id}?group_type_name=${vm.group_type_name}&action=edit'>Edit</a><br/>`;
                                if (full.processing_status == 'draft') {
                                    links += `<a href='#' data-discard-occ-proposal='${full.id}'>Discard</a><br/>`;
                                }
                            }
                        } else {
                            links += `<a href='/internal/occurrence/${full.id}?group_type_name=${vm.group_type_name}&action=view'>View</a><br/>`;
                        }
                        links += `<a href='#' data-history-occurrence='${full.id}'>History</a><br>`;
                    }
                    return links;
                },
            };
        },
        datatable_options: function () {
            let vm = this;
            let columns = [];
            let search = null;
            let buttons = [
                {
                    extend: 'excel',
                    title: 'Boranga OCC Communities Excel Export',
                    text: '<i class="fa-solid fa-download"></i> Excel',
                    className: 'btn btn-primary me-2 rounded',
                    exportOptions: {
                        columns: ':not(.no-export)',
                        orthogonal: 'export',
                    },
                },
                {
                    extend: 'csv',
                    title: 'Boranga OCC Communities CSV Export',
                    text: '<i class="fa-solid fa-download"></i> CSV',
                    className: 'btn btn-primary rounded',
                    exportOptions: {
                        columns: ':not(.no-export)',
                        orthogonal: 'export',
                    },
                },
            ];
            if (vm.is_internal) {
                columns = [
                    vm.column_id,
                    vm.column_number,
                    vm.column_occurrence_name,
                    vm.column_community_name,
                    vm.column_wild_status,
                    vm.column_number_of_reports,
                    vm.column_community_common_id,
                    vm.column_migrated_from_id,
                    vm.column_review_due_date,
                    vm.column_status,
                    vm.column_action,
                ];
                search = true;
            }

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
                    { responsivePriority: 1, targets: 1 },
                    {
                        responsivePriority: 3,
                        targets: -1,
                        className: 'no-export',
                    },
                    { responsivePriority: 2, targets: -2 },
                    { responsivePriority: 4, targets: 5 },
                ],
                ajax: {
                    url: this.url,
                    dataSrc: 'data',

                    // adding extra GET params for Custom filtering
                    data: function (d) {
                        d.filter_group_type = vm.group_type_name;
                        d.filter_community_common_id =
                            vm.filterOCCCommunityMigratedId;
                        d.filter_occurrence_name =
                            vm.filterOCCCommunityOccurrenceName;
                        d.filter_community_name = vm.filterOCCCommunityName;
                        d.filter_status = vm.filterOCCCommunityStatus;
                        d.filter_from_due_date =
                            vm.filterOCCFromCommunityDueDate;
                        d.filter_to_due_date = vm.filterOCCToCommunityDueDate;
                        d.filter_locked = vm.filterOCCCommunityLocked;
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
        filterOCCCommunityMigratedId: function () {
            let vm = this;
            vm.$refs.community_occ_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterOCCCommunityMigratedId_cache,
                vm.filterOCCCommunityMigratedId
            );
        },
        filterOCCCommunityOccurrenceName: function () {
            let vm = this;
            vm.$refs.community_occ_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterOCCCommunityOccurrenceName_cache,
                vm.filterOCCCommunityOccurrenceName
            );
        },
        filterOCCCommunityName: function () {
            let vm = this;
            vm.$refs.community_occ_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterOCCCommunityName_cache,
                vm.filterOCCCommunityName
            );
        },
        filterOCCCommunityStatus: function () {
            let vm = this;
            vm.$refs.community_occ_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterOCCCommunityStatus_cache,
                vm.filterOCCCommunityStatus
            );
        },
        filterOCCCommunityLocked: function () {
            let vm = this;
            vm.$refs.community_occ_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterOCCCommunityLocked_cache,
                vm.filterOCCCommunityLocked
            );
        },
        filterOCCFromCommunityDueDate: function () {
            let vm = this;
            vm.$refs.community_occ_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterOCCFromCommunityDueDate_cache,
                vm.filterOCCFromCommunityDueDate
            );
        },
        filterOCCToCommunityDueDate: function () {
            let vm = this;
            vm.$refs.community_occ_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterOCCToCommunityDueDate_cache,
                vm.filterOCCToCommunityDueDate
            );
        },
    },
    mounted: function () {
        this.fetchFilterLists();
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
            vm.initialiseOccurrenceNameLookup();
            vm.initialiseCommunityNameLookup();
            vm.initialiseCommunityIdLookup();
            vm.addEventListeners();
            var newOption = null;
            // -- to set the select2 field with the session value if exists onload()
            if (
                sessionStorage.getItem('filterOCCCommunityOccurrenceName') !=
                    'all' &&
                sessionStorage.getItem('filterOCCCommunityOccurrenceName') !=
                    null
            ) {
                // contructor new Option(text, value, defaultSelected, selected)
                newOption = new Option(
                    sessionStorage.getItem(
                        'filterOCCCommunityOccurrenceNameText'
                    ),
                    vm.filterOCCCommunityOccurrenceName,
                    false,
                    true
                );
                $('#occ_occurrence_lookup').append(newOption);
                //$('#scientific_name_lookup').append(newOption).trigger('change');
            }
            if (
                sessionStorage.getItem('filterOCCCommunityName') != 'all' &&
                sessionStorage.getItem('filterOCCCommunityName') != null
            ) {
                // contructor new Option(text, value, defaultSelected, selected)
                newOption = new Option(
                    sessionStorage.getItem('filterOCCCommunityNameText'),
                    vm.filterOCCCommunityName,
                    false,
                    true
                );
                $('#occ_community_name_lookup').append(newOption);
            }
            if (
                sessionStorage.getItem('filterOCCCommunityMigratedId') !=
                    'all' &&
                sessionStorage.getItem('filterOCCCommunityMigratedId') != null
            ) {
                // contructor new Option(text, value, defaultSelected, selected)
                newOption = new Option(
                    sessionStorage.getItem('filterOCCCommunityMigratedIdText'),
                    vm.filterOCCCommunityMigratedId,
                    false,
                    true
                );
                $('#occ_community_id_lookup').append(newOption);
            }
        });
    },
    methods: {
        historyDocument: function (id) {
            this.occurrenceHistoryId = parseInt(id);
            this.uuid++;
            this.$nextTick(() => {
                this.$refs.occurrence_history.isModalOpen = true;
            });
        },
        initialiseOccurrenceNameLookup: function () {
            let vm = this;
            $(vm.$refs.occurrence_name_lookup)
                .select2({
                    minimumInputLength: 2,
                    dropdownParent: $('#occurrence_name_lookup_form_group_id'),
                    theme: 'bootstrap-5',
                    allowClear: true,
                    placeholder: 'Select Occurrence Name',
                    ajax: {
                        url: api_endpoints.occurrence_name_lookup,
                        dataType: 'json',
                        data: function (params) {
                            var query = {
                                term: params.term,
                                type: 'public',
                                group_type_id: vm.group_type_id,
                                active_only: false,
                            };
                            return query;
                        },
                    },
                })
                .on('select2:select', function (e) {
                    let data = e.params.data.text;
                    vm.filterOCCCommunityOccurrenceName = data;
                    sessionStorage.setItem(
                        'filterOCCCommunityOccurrenceNameText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.filterOCCCommunityOccurrenceName = 'all';
                    sessionStorage.setItem(
                        'filterOCCCommunityOccurrenceNameText',
                        ''
                    );
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-occurrence_name_lookup-results"]'
                    );
                    searchField[0].focus();
                });
        },
        initialiseCommunityNameLookup: function () {
            let vm = this;
            $(vm.$refs.occ_community_name_lookup)
                .select2({
                    minimumInputLength: 2,
                    dropdownParent: $('#select_occ_community_name'),
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
                            };
                            return query;
                        },
                    },
                })
                .on('select2:select', function (e) {
                    let data = e.params.data.id;
                    vm.filterOCCCommunityName = data;
                    sessionStorage.setItem(
                        'filterOCCCommunityNameText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.filterOCCCommunityName = 'all';
                    sessionStorage.setItem('filterOCCCommunityNameText', '');
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-occ_community_name_lookup-results"]'
                    );
                    searchField[0].focus();
                });
        },
        initialiseCommunityIdLookup: function () {
            let vm = this;
            $(vm.$refs.occ_community_id_lookup)
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
                            };
                            return query;
                        },
                    },
                })
                .on('select2:select', function (e) {
                    let data = e.params.data.id;
                    vm.filterOCCCommunityMigratedId = data;
                    sessionStorage.setItem(
                        'filterOCCCommunityMigratedIdText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.filterOCCCommunityMigratedId = 'all';
                    sessionStorage.setItem(
                        'filterOCCCommunityMigratedIdText',
                        ''
                    );
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-occ_community_id_lookup-results"]'
                    );
                    // move focus to select2 field
                    searchField[0].focus();
                });
        },
        fetchFilterLists: function () {
            let vm = this;
            //large FilterList of Community Values object
            fetch(
                api_endpoints.community_filter_dict +
                    '?group_type_name=' +
                    vm.group_type_name
            ).then(
                async (response) => {
                    vm.filterListsCommunity = await response.json();
                    vm.occurrence_list =
                        vm.filterListsCommunity.occurrence_list;
                    vm.community_name_list =
                        vm.filterListsCommunity.community_name_list;
                    vm.status_list = vm.filterListsCommunity.status_list;
                    vm.submissions_from_list =
                        vm.filterListsCommunity.submissions_from_list;
                    vm.submissions_to_list =
                        vm.filterListsCommunity.submissions_to_list;
                    vm.proposal_status = vm.internal_status
                        .slice()
                        .sort((a, b) => {
                            return a.name.trim().localeCompare(b.name.trim());
                        });
                },
                (error) => {
                    console.log(error);
                }
            );
        },
        createCommunityOccurrence: async function () {
            swal.fire({
                title: `Add ${this.group_type_name} Occurrence`,
                text: 'Are you sure you want to add a new community occurrence?',
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Add Occurrence',
                reverseButtons: true,
                customClass: {
                    confirmButton: 'btn btn-primary',
                    cancelButton: 'btn btn-secondary',
                },
            }).then(async (swalresult) => {
                if (swalresult.isConfirmed) {
                    let newCommunityOCCId = null;
                    try {
                        const createUrl = api_endpoints.occurrence;
                        let payload = new Object();
                        payload.group_type_id = this.group_type_id;
                        payload.internal_application = true;
                        let response = await fetch(createUrl, {
                            method: 'POST',
                            headers: {
                                'Content-Type': 'application/json',
                            },
                            body: JSON.stringify(payload),
                        });
                        const data = await response.json();
                        if (data) {
                            newCommunityOCCId = data.id;
                        }
                    } catch (err) {
                        console.log(err);
                        if (this.is_internal) {
                            return err;
                        }
                    }
                    this.$router.push({
                        name: 'internal-occurrence-detail',
                        params: { occurrence_id: newCommunityOCCId },
                    });
                }
            });
        },
        discardOCC: function (occurrence_id) {
            let vm = this;
            swal.fire({
                title: 'Discard Occurrence',
                text: 'Are you sure you want to discard this occurrence?',
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Discard Occurrence',
                customClass: {
                    confirmButton: 'btn btn-primary',
                    cancelButton: 'btn btn-secondary',
                },
                reverseButtons: true,
            }).then(
                (swalresult) => {
                    if (swalresult.isConfirmed) {
                        fetch(
                            api_endpoints.discard_occ_proposal(occurrence_id),
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
                                    text: 'The occurrence has been discarded',
                                    icon: 'success',
                                    customClass: {
                                        confirmButton: 'btn btn-primary',
                                    },
                                });
                                vm.$refs.community_occ_datatable.vmDataTable.ajax.reload(
                                    helpers.enablePopovers,
                                    true
                                );
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
        reinstateOCC: function (occurrence_id) {
            let vm = this;
            swal.fire({
                title: 'Reinstate Occurrence',
                text: 'Are you sure you want to reinstate this occurrence?',
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Reinstate Occurrence',
                customClass: {
                    confirmButton: 'btn btn-primary',
                    cancelButton: 'btn btn-secondary',
                },
                reverseButtons: true,
            }).then(
                (swalresult) => {
                    if (swalresult.isConfirmed) {
                        fetch(
                            api_endpoints.reinstate_occ_proposal(occurrence_id),
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
                                    text: 'Your report has been reinstated',
                                    icon: 'success',
                                    customClass: {
                                        confirmButton: 'btn btn-primary',
                                    },
                                });
                                vm.$refs.community_occ_datatable.vmDataTable.ajax.reload(
                                    helpers.enablePopovers,
                                    true
                                );
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
        addEventListeners: function () {
            let vm = this;
            // internal Discard listener
            vm.$refs.community_occ_datatable.vmDataTable.on(
                'click',
                'a[data-discard-occ-proposal]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-discard-occ-proposal');
                    vm.discardOCC(id);
                }
            );
            vm.$refs.community_occ_datatable.vmDataTable.on(
                'click',
                'a[data-reinstate-occ-proposal]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-reinstate-occ-proposal');
                    vm.reinstateOCC(id);
                }
            );
            vm.$refs.community_occ_datatable.vmDataTable.on(
                'click',
                'a[data-history-occurrence]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-history-occurrence');
                    vm.historyDocument(id);
                }
            );
            vm.$refs.community_occ_datatable.vmDataTable.on(
                'childRow.dt',
                function () {
                    helpers.enablePopovers();
                }
            );
        },
    },
};
</script>
<style scoped>
.dt-buttons {
    float: right;
}
</style>
