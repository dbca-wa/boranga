<template id="species_flora_ocr_referrals_dashboard">
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
                        id="ocr_referrals_select_occurrence"
                        class="form-group"
                    >
                        <label for="ocr_referrals_occurrence_lookup"
                            >Occurrence:</label
                        >
                        <select
                            id="ocr_referrals_occurrence_lookup"
                            ref="ocr_referrals_occurrence_lookup"
                            name="ocr_referrals_occurrence_lookup"
                            class="form-control"
                        />
                    </div>
                </div>
                <div v-if="group_type_name == 'community'" class="col-md-4">
                    <div
                        id="select_ocr_referrals_community_name"
                        class="form-group"
                    >
                        <label for="ocr_referrals_community_name_lookup"
                            >Community Name:</label
                        >
                        <select
                            id="ocr_referrals_community_name_lookup"
                            ref="ocr_referrals_community_name_lookup"
                            name="ocr_referrals_community_name_lookup"
                            class="form-control"
                        />
                    </div>
                </div>
                <div v-else class="col-md-4">
                    <div
                        id="ocr_referrals_select_scientific_name_by_groupname"
                        class="form-group"
                    >
                        <label
                            for="ocr_referrals_scientific_name_lookup_by_groupname"
                            >Scientific Name:</label
                        >
                        <select
                            id="ocr_referrals_scientific_name_lookup_by_groupname"
                            ref="ocr_referrals_scientific_name_lookup_by_groupname"
                            name="ocr_referrals_scientific_name_lookup_by_groupname"
                            class="form-control"
                        />
                    </div>
                </div>
                <div class="col-md-4">
                    <div id="select_status" class="form-group">
                        <label for="">Status:</label>
                        <select
                            v-model="filterOCRReferralsStatus"
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
                    ref="flora_ocr_referrals_datatable"
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
    name: 'OccurrenceReportFloraTable',
    components: {
        datatable,
        CollapsibleFilters,
    },
    props: {
        level: {
            type: String,
            required: true,
            validator: function (val) {
                let options = ['internal', 'external'];
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
        filterOCRReferralsOccurrence_cache: {
            type: String,
            required: false,
            default: 'filterOCRReferralsOccurrence',
        },
        filterOCRReferralsScientificName_cache: {
            type: String,
            required: false,
            default: 'filterOCRReferralsScientificName',
        },
        filterOCRReferralsName_cache: {
            type: String,
            required: false,
            default: 'filterOCRReferralsName',
        },
        filterOCRReferralsStatus_cache: {
            type: String,
            required: false,
            default: 'filterOCRReferralsStatus',
        },
    },
    data() {
        return {
            uuid: 0,
            occurrenceReportHistoryId: null,
            datatable_id: 'species_flora_ocr_referrals_datatable_' + uuid(),
            filterOCRReferralsOccurrence: sessionStorage.getItem(
                this.filterOCRReferralsOccurrence_cache
            )
                ? sessionStorage.getItem(
                      this.filterOCRReferralsOccurrence_cache
                  )
                : 'all',
            filterOCRReferralsScientificName: sessionStorage.getItem(
                this.filterOCRReferralsScientificName_cache
            )
                ? sessionStorage.getItem(
                      this.filterOCRReferralsScientificName_cache
                  )
                : 'all',
            filterOCRReferralsName: sessionStorage.getItem(
                this.filterOCRReferralsName_cache
            )
                ? sessionStorage.getItem(this.filterOCRReferralsName_cache)
                : 'all',
            filterOCRReferralsStatus: sessionStorage.getItem(
                this.filterOCRReferralsStatus_cache
            )
                ? sessionStorage.getItem(this.filterOCRReferralsStatus_cache)
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
                this.filterOCRReferralsOccurrence === 'all' &&
                this.filterOCRReferralsScientificName === 'all' &&
                this.filterOCRReferralsName === 'all' &&
                this.filterOCRReferralsStatus === 'all'
            ) {
                return false;
            } else {
                return true;
            }
        },
        datatable_headers: function () {
            let headers = [
                'ID',
                'Number',
                'Occurrence',
                'Scientific Name',
                'Submission date/time',
                'Submitter',
                'Status',
                'Action',
            ];
            if (this.group_type_name == 'community') {
                headers.splice(3, 1, 'Community Name');
            }
            return headers;
        },
        column_id: function () {
            return {
                data: 'id',
                orderable: true,
                searchable: false,
                visible: false,
            };
        },
        column_number: function () {
            return {
                data: 'occurrence_report_number',
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
                    return full.occurrence_report_number + tick;
                },
                name: 'occurrence_report__occurrence_report_number',
            };
        },
        column_occurrence: function () {
            return {
                data: 'occurrence_name',
                orderable: true,
                searchable: true,
                visible: true,
                render: function (data, type, full) {
                    if (full.occurrence_name) {
                        return full.occurrence_name;
                    }
                    return 'NOT SET';
                },
                name: 'occurrence_report__occurrence__occurrence_number',
            };
        },
        column_scientific_name: function () {
            return {
                data: 'scientific_name',
                orderable: true,
                searchable: true,
                visible: true,
                name: 'occurrence_report__species__taxonomy__scientific_name',
            };
        },
        column_community_name: function () {
            return {
                data: 'community_name',
                orderable: true,
                searchable: true,
                visible: true,
                name: 'occurrence_report__community__taxonomy__community_name',
            };
        },
        column_submission_date_time: function () {
            return {
                data: 'reported_date',
                orderable: true,
                searchable: true,
                visible: true,
                name: 'occurrence_report__reported_date',
            };
        },
        column_submitter: function () {
            return {
                data: 'submitter',
                orderable: false,
                searchable: false,
                visible: true,
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
                        ? `<a href='/internal/occurrence-report/${full.occurrence_report_id}/referral/${full.id}'>Process</a><br/>`
                        : `<a href='/internal/occurrence-report/${full.occurrence_report_id}/referral/${full.id}'>View</a><br/>`;
                    return links;
                },
            };
        },
        datatable_options: function () {
            let vm = this;
            let search = null;
            let buttons = [
                {
                    extend: 'excel',
                    title: 'Boranga Occurrence Reports Referred to Me Excel Export',
                    text: '<i class="fa-solid fa-download"></i> Excel',
                    className: 'btn btn-primary me-2 rounded',
                    exportOptions: {
                        columns: ':not(.no-export)',
                        orthogonal: 'export',
                    },
                },
                {
                    extend: 'csv',
                    title: 'Boranga Occurrence Reports Referred to Me CSV Export',
                    text: '<i class="fa-solid fa-download"></i> CSV',
                    className: 'btn btn-primary rounded',
                    exportOptions: {
                        columns: ':not(.no-export)',
                        orthogonal: 'export',
                    },
                },
            ];

            let columns = [
                vm.column_id,
                vm.column_number,
                vm.column_occurrence,
                vm.column_scientific_name,
                vm.column_submission_date_time,
                vm.column_submitter,
                vm.column_status,
                vm.column_action,
            ];

            if (this.group_type_name == 'community') {
                columns.splice(3, 1, vm.column_community_name);
            }

            search = true;

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
                    {
                        responsivePriority: 1,
                        targets: 0,
                        className: 'no-export',
                    },
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
                    data: function (d) {
                        d.filter_group_type = vm.group_type_name;
                        d.filter_occurrence = vm.filterOCRReferralsOccurrence;
                        d.filter_scientific_name =
                            vm.filterOCRReferralsScientificName;
                        d.filter_community_name = vm.filterOCRReferralsName;
                        d.filter_status = vm.filterOCRReferralsStatus;
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
        filterOCRReferralsOccurrence: function () {
            let vm = this;
            vm.$refs.flora_ocr_referrals_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            );
            sessionStorage.setItem(
                vm.filterOCRReferralsOccurrence_cache,
                vm.filterOCRReferralsOccurrence
            );
        },
        filterOCRReferralsScientificName: function () {
            let vm = this;
            vm.$refs.flora_ocr_referrals_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            );
            sessionStorage.setItem(
                vm.filterOCRReferralsScientificName_cache,
                vm.filterOCRReferralsScientificName
            );
        },
        filterOCRReferralsName: function () {
            let vm = this;
            vm.$refs.flora_ocr_referrals_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            );
            sessionStorage.setItem(
                vm.filterOCRReferralsName_cache,
                vm.filterOCRReferralsName
            );
        },
        filterOCRReferralsStatus: function () {
            let vm = this;
            vm.$refs.flora_ocr_referrals_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            );
            sessionStorage.setItem(
                vm.filterOCRReferralsStatus_cache,
                vm.filterOCRReferralsStatus
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
            vm.initialiseOccurrenceLookup();
            if (vm.group_type_name == 'community') {
                vm.initialiseCommunityNameLookup();
            } else {
                vm.initialiseScientificNameLookup();
            }
            vm.addEventListeners();
            var newOption = null;
            if (
                sessionStorage.getItem('filterOCRReferralsOccurrence') !=
                    'all' &&
                sessionStorage.getItem('filterOCRReferralsOccurrence') != null
            ) {
                newOption = new Option(
                    sessionStorage.getItem('filterOCRReferralsOccurrenceText'),
                    vm.filterOCRReferralsOccurrence,
                    false,
                    true
                );
                $('#ocr_referrals_occurrence_lookup').append(newOption);
            }
            if (
                sessionStorage.getItem('filterOCRReferralsScientificName') !=
                    'all' &&
                sessionStorage.getItem('filterOCRReferralsScientificName') !=
                    null
            ) {
                newOption = new Option(
                    sessionStorage.getItem(
                        'filterOCRReferralsScientificNameText'
                    ),
                    vm.filterOCRReferralsScientificName,
                    false,
                    true
                );
                $('#ocr_referrals_scientific_name_lookup').append(newOption);
            }
            if (
                sessionStorage.getItem('filterOCRReferralsName') != 'all' &&
                sessionStorage.getItem('filterOCRReferralsName') != null
            ) {
                newOption = new Option(
                    sessionStorage.getItem('filterOCRReferralsNameText'),
                    vm.filterOCRReferralsName,
                    false,
                    true
                );
                $('#ocr_referrals_community_name_lookup').append(newOption);
            }
        });
    },
    methods: {
        historyDocument: function (id) {
            this.occurrenceReportHistoryId = parseInt(id);
            this.uuid++;
            this.$nextTick(() => {
                this.$refs.occurrence_report_history.isModalOpen = true;
            });
        },
        initialiseOccurrenceLookup: function () {
            let vm = this;
            $(vm.$refs.ocr_referrals_occurrence_lookup)
                .select2({
                    minimumInputLength: 2,
                    dropdownParent: $('#ocr_referrals_select_occurrence'),
                    theme: 'bootstrap-5',
                    allowClear: true,
                    placeholder: 'Select Occurrence',
                    ajax: {
                        url: api_endpoints.occurrence_lookup,
                        dataType: 'json',
                        data: function (params) {
                            var query = {
                                term: params.term,
                                type: 'public',
                                group_type_id: vm.group_type_id,
                            };
                            return query;
                        },
                    },
                })
                .on('select2:select', function (e) {
                    let data = e.params.data.id;
                    vm.filterOCRReferralsOccurrence = data;
                    sessionStorage.setItem(
                        'filterOCRReferralsOccurrenceText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.filterOCRReferralsOccurrence = 'all';
                    sessionStorage.setItem(
                        'filterOCRReferralsOccurrenceText',
                        ''
                    );
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-ocr_referrals_occurrence_lookup-results"]'
                    );
                    searchField[0].focus();
                });
        },
        initialiseScientificNameLookup: function () {
            let vm = this;
            $(vm.$refs.ocr_referrals_scientific_name_lookup_by_groupname)
                .select2({
                    minimumInputLength: 2,
                    dropdownParent: $(
                        '#ocr_referrals_select_scientific_name_by_groupname'
                    ),
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
                                group_type_id: vm.group_type_id,
                            };
                            return query;
                        },
                    },
                })
                .on('select2:select', function (e) {
                    let data = e.params.data.id;
                    vm.filterOCRReferralsScientificName = data;
                    sessionStorage.setItem(
                        'filterOCRReferralsScientificNameText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.filterOCRReferralsScientificName = 'all';
                    sessionStorage.setItem(
                        'filterOCRReferralsScientificNameText',
                        ''
                    );
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-ocr_referrals_scientific_name_lookup_by_groupname-results"]'
                    );
                    searchField[0].focus();
                });
        },
        initialiseCommunityNameLookup: function () {
            let vm = this;
            $(vm.$refs.ocr_referrals_community_name_lookup)
                .select2({
                    minimumInputLength: 2,
                    dropdownParent: $('#select_ocr_referrals_community_name'),
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
                    vm.filterOCRReferralsName = data;
                    sessionStorage.setItem(
                        'filterOCRReferralsNameText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.filterOCRReferralsName = 'all';
                    sessionStorage.setItem('filterOCRReferralsNameText', '');
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-ocr_referrals_community_name_lookup-results"]'
                    );
                    searchField[0].focus();
                });
        },
        addEventListeners: function () {
            let vm = this;
            vm.$refs.flora_ocr_referrals_datatable.vmDataTable.on(
                'childRow.dt',
                function () {
                    helpers.enablePopovers();
                }
            );
        },
    },
};
</script>
