<template id="external_conservation_status_datatable">
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
                        <label for="">Type:</label>
                        <select v-model="filterCSGroupType" class="form-select">
                            <option value="all">All</option>
                            <option
                                v-for="option in group_types"
                                :value="option.name"
                                :key="option.name"
                            >
                                {{ option.display }}
                            </option>
                        </select>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="cs_scientific_name_lookup"
                            >Scientific Name:</label
                        >
                        <select
                            id="cs_scientific_name_lookup"
                            ref="cs_scientific_name_lookup"
                            name="cs_scientific_name_lookup"
                            class="form-control"
                        />
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="cs_community_name_lookup"
                            >Community Name:</label
                        >
                        <select
                            id="cs_community_name_lookup"
                            ref="cs_community_name_lookup"
                            name="cs_community_name_lookup"
                            class="form-control"
                        />
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="">Status:</label>
                        <select
                            v-model="filterCSApplicationStatus"
                            class="form-select"
                        >
                            <option value="all">All</option>
                            <option
                                v-for="status in customer_statuses"
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
        <div v-if="addCSVisibility" class="col-md-12 dropdown">
            <div class="text-end">
                <button
                    id="cs_proposal_type"
                    class="btn btn-primary dropdown-toggle mb-2"
                    type="button"
                    data-bs-toggle="dropdown"
                    aria-expanded="false"
                >
                    Propose Conservation Status
                </button>
                <ul class="dropdown-menu" aria-labelledby="cs_proposal_type">
                    <li v-for="group in group_types" :key="group.id">
                        <a
                            class="dropdown-item"
                            role="button"
                            @click.prevent="
                                createConservationStatus(
                                    group.id,
                                    group.display
                                )
                            "
                            >{{ group.display }}
                        </a>
                    </li>
                </ul>
            </div>
        </div>
        <div class="row">
            <div class="col-lg-12">
                <datatable
                    :id="datatable_id"
                    ref="conservation_status_datatable"
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

import { constants, api_endpoints, helpers } from '@/utils/hooks';
export default {
    name: 'ExternalConservationStatusDatatable',
    components: {
        datatable,
        CollapsibleFilters,
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
        url: {
            type: String,
            required: true,
        },
        profile: {
            type: Object,
            required: false,
            default: function () {
                return null;
            },
        },
        filterCSGroupType_cache: {
            type: String,
            required: false,
            default: 'filterCSGroupType',
        },
        filterCSScientificName_cache: {
            type: String,
            required: false,
            default: 'filterCSScientificName',
        },
        filterCSExCommunityName_cache: {
            type: String,
            required: false,
            default: 'filterCSExCommunityName',
        },
        filterCSConservationList_cache: {
            type: String,
            required: false,
            default: 'filterCSConservationList',
        },
        filterCSConservationCategory_cache: {
            type: String,
            required: false,
            default: 'filterCSConservationCategory',
        },
        filterCSApplicationStatus_cache: {
            type: String,
            required: false,
            default: 'filterCSApplicationStatus',
        },
    },
    data() {
        return {
            datatable_id: 'conservation_status-datatable-' + uuid(),

            // selected values for filtering
            filterCSGroupType: sessionStorage.getItem(
                this.filterCSGroupType_cache
            )
                ? sessionStorage.getItem(this.filterCSGroupType_cache)
                : 'all',

            filterCSScientificName: sessionStorage.getItem(
                this.filterCSScientificName_cache
            )
                ? sessionStorage.getItem(this.filterCSScientificName_cache)
                : 'all',

            filterCSExCommunityName: sessionStorage.getItem(
                this.filterCSExCommunityName_cache
            )
                ? sessionStorage.getItem(this.filterCSExCommunityName_cache)
                : 'all',

            filterCSConservationList: sessionStorage.getItem(
                this.filterCSConservationList_cache
            )
                ? sessionStorage.getItem(this.filterCSConservationList_cache)
                : 'all',

            filterCSConservationCategory: sessionStorage.getItem(
                this.filterCSConservationCategory_cache
            )
                ? sessionStorage.getItem(
                      this.filterCSConservationCategory_cache
                  )
                : 'all',

            filterCSApplicationStatus: sessionStorage.getItem(
                this.filterCSApplicationStatus_cache
            )
                ? sessionStorage.getItem(this.filterCSApplicationStatus_cache)
                : 'all',

            group_types: [],

            customer_statuses: [
                { value: 'draft', name: 'Draft' },
                { value: 'with_assessor', name: 'Under Review' },
                { value: 'ready_for_agenda', name: 'In Meeting' },
                { value: 'approved', name: 'Approved' },
                { value: 'closed', name: 'Closed' },
                { value: 'delisted', name: 'Delisted' },
                { value: 'declined', name: 'Declined' },
            ],
        };
    },
    computed: {
        filterApplied: function () {
            if (
                this.filterCSGroupType === 'all' &&
                this.filterCSScientificName === 'all' &&
                this.filterCSExCommunityName === 'all' &&
                this.filterCSConservationList === 'all' &&
                this.filterCSConservationCategory === 'all' &&
                this.filterCSApplicationStatus === 'all'
            ) {
                return false;
            } else {
                return true;
            }
        },
        addCSVisibility: function () {
            let visibility;
            visibility = true;
            return visibility;
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
                data: 'conservation_status_number',
                orderable: true,
                searchable: true,
                visible: true,
                render: function (data, type, full) {
                    return full.conservation_status_number;
                },
                name: 'id',
            };
        },
        column_type: function () {
            return {
                data: 'group_type',
                orderable: true,
                searchable: false,
                visible: true,
                render: function (data, type, full) {
                    return full.group_type;
                },
                name: 'species__group_type__name',
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
                name: 'species_taxonomy__scientific_name',
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
                name: 'community__taxonomy__community_name',
            };
        },
        column_status: function () {
            return {
                data: 'customer_status',
                orderable: true,
                searchable: false,
                visible: true,
                render: function (data, type, full) {
                    if (full.customer_status) {
                        return full.customer_status;
                    }
                    return '';
                },
                name: 'customer_status',
            };
        },
        column_action: function () {
            return {
                // 10. Action
                data: 'id',
                orderable: false,
                searchable: false,
                visible: true,
                render: function (data, type, full) {
                    let links = '';
                    if (full.can_user_edit) {
                        links += `<a href='/external/conservation-status/${full.id}'>Continue</a><br/>`;
                        links += `<a href='#${full.id}' data-discard-cs-proposal='${full.id}'>Discard</a><br/>`;
                    } else if (full.can_user_view) {
                        links += `<a href='/external/conservation-status/${full.id}'>View</a>`;
                    } else if (full.processing_status == 'discarded') {
                        links += `<a <a href='#${full.id}' data-reinstate-cs-proposal='${full.id}'>Reinstate</a>`;
                    }

                    return links;
                },
            };
        },
        datatable_options: function () {
            let vm = this;

            let columns;
            let buttons = [
                {
                    extend: 'excel',
                    title: 'Boranga Conservation Status Excel Export',
                    text: '<i class="bi bi-download"></i> Excel',
                    className: 'btn btn-primary me-2 rounded',
                    exportOptions: {
                        columns: ':not(.no-export)',
                        orthogonal: 'export',
                    },
                },
                {
                    extend: 'csv',
                    title: 'Boranga Conservation Status CSV Export',
                    text: '<i class="bi bi-download"></i> CSV',
                    className: 'btn btn-primary rounded',
                    exportOptions: {
                        columns: ':not(.no-export)',
                        orthogonal: 'export',
                    },
                },
            ];
            columns = [
                vm.column_number,
                vm.column_type,
                vm.column_scientific_name,
                vm.column_community_name,
                vm.column_status,
                vm.column_action,
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
                    data: function (d) {
                        d.filter_group_type = vm.filterCSGroupType;
                        d.filter_scientific_name = vm.filterCSScientificName;
                        d.filter_community_name = vm.filterCSExCommunityName;
                        d.filter_conservation_list =
                            vm.filterCSConservationList;
                        d.filter_conservation_category =
                            vm.filterCSConservationCategory;
                        d.filter_application_status =
                            vm.filterCSApplicationStatus;
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
        filterCSGroupType: function () {
            let vm = this;
            vm.$refs.conservation_status_datatable.vmDataTable.ajax.reload(); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterCSGroupType_cache,
                vm.filterCSGroupType
            );
        },
        filterCSScientificName: function () {
            let vm = this;
            vm.$refs.conservation_status_datatable.vmDataTable.ajax.reload(); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterCSScientificName_cache,
                vm.filterCSScientificName
            );
        },
        filterCSExCommunityName: function () {
            let vm = this;
            vm.$refs.conservation_status_datatable.vmDataTable.ajax.reload(); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterCSExCommunityName_cache,
                vm.filterCSExCommunityName
            );
        },
        filterCSConservationList: function () {
            let vm = this;
            vm.$refs.conservation_status_datatable.vmDataTable.ajax.reload(); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterCSConservationList_cache,
                vm.filterCSConservationList
            );
        },
        filterCSConservationCategory: function () {
            let vm = this;
            vm.$refs.conservation_status_datatable.vmDataTable.ajax.reload(); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterCSConservationCategory_cache,
                vm.filterCSConservationCategory
            );
        },
        filterCSApplicationStatus: function () {
            let vm = this;
            vm.$refs.conservation_status_datatable.vmDataTable.ajax.reload(); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterCSApplicationStatus_cache,
                vm.filterCSApplicationStatus
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
            vm.initialiseScientificNameLookup();
            vm.initialiseCommunityNameLookup();
            vm.addEventListeners();
            var newOption;
            // -- to set the select2 field with the session value if exists onload()
            if (
                sessionStorage.getItem('filterCSScientificName') != 'all' &&
                sessionStorage.getItem('filterCSScientificName') != null
            ) {
                // contructor new Option(text, value, defaultSelected, selected)
                newOption = new Option(
                    sessionStorage.getItem('filterCSScientificNameText'),
                    vm.filterCSScientificName,
                    false,
                    true
                );
                $('#cs_scientific_name_lookup').append(newOption);
            }
            if (
                sessionStorage.getItem('filterCSExCommunityName') != 'all' &&
                sessionStorage.getItem('filterCSExCommunityName') != null
            ) {
                // contructor new Option(text, value, defaultSelected, selected)
                newOption = new Option(
                    sessionStorage.getItem('filterCSExCommunityNameText'),
                    vm.filterCSExCommunityName,
                    false,
                    true
                );
                $('#cs_community_name_lookup').append(newOption);
            }
        });
    },
    methods: {
        initialiseScientificNameLookup: function () {
            let vm = this;
            $(vm.$refs.cs_scientific_name_lookup)
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
                            };
                            return query;
                        },
                    },
                })
                .on('select2:select', function (e) {
                    let data = e.params.data.id;
                    vm.filterCSScientificName = data;
                    sessionStorage.setItem(
                        'filterCSScientificNameText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.filterCSScientificName = 'all';
                    sessionStorage.setItem('filterCSScientificNameText', '');
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-cs_scientific_name_lookup-results"]'
                    );
                    // move focus to select2 field
                    searchField[0].focus();
                });
        },
        initialiseCommunityNameLookup: function () {
            let vm = this;
            $(vm.$refs.cs_community_name_lookup)
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
                    vm.filterCSExCommunityName = data;
                    sessionStorage.setItem(
                        'filterCSExCommunityNameText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.filterCSExCommunityName = 'all';
                    sessionStorage.setItem('filterCSExCommunityNameText', '');
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-cs_community_name_lookup-results"]'
                    );
                    // move focus to select2 field
                    searchField[0].focus();
                });
        },
        fetchFilterLists: function () {
            let vm = this;
            fetch(api_endpoints.group_types_dict).then(
                async (response) => {
                    vm.group_types = await response.json();
                },
                (error) => {
                    console.log(error);
                }
            );
        },
        createConservationStatus: async function (group_type, group_type_name) {
            swal.fire({
                title: `Propose New ${group_type_name} Conservation Status`,
                text: `Are you sure you want to propose a new ${group_type_name} Conservation Status?`,
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: `Propose ${group_type_name} Conservation Status`,
                reverseButtons: true,
                customClass: {
                    confirmButton: 'btn btn-primary',
                },
            }).then(async (swalresult) => {
                if (swalresult.isConfirmed) {
                    let newCSId = null;
                    try {
                        const createUrl =
                            api_endpoints.conservation_status + '/';
                        let payload = new Object();
                        payload.application_type_id = group_type;
                        let response = await fetch(createUrl, {
                            method: 'POST',
                            headers: {
                                'Content-Type': 'application/json',
                            },
                            body: JSON.stringify(payload),
                        });
                        const data = await response.json();
                        if (data) {
                            newCSId = data.id;
                        }
                    } catch (err) {
                        console.log(err);
                    }
                    this.$router.push({
                        name: 'draft_cs_proposal',
                        params: { conservation_status_id: newCSId },
                    });
                }
            });
        },
        discardCSProposal: function (conservation_status_id) {
            let vm = this;
            swal.fire({
                title: 'Discard Proposal',
                text: 'Are you sure you want to discard this proposal?',
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Discard Proposal',
                reverseButtons: true,
                buttonsStyling: false,
                customClass: {
                    confirmButton: 'btn btn-primary',
                    cancelButton: 'btn btn-secondary me-2',
                },
            }).then((result) => {
                if (result.isConfirmed) {
                    fetch(
                        api_endpoints.discard_cs_proposal(
                            conservation_status_id
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
                                text: 'Your proposal has been discarded',
                                icon: 'success',
                                customClass: {
                                    confirmButton: 'btn btn-primary',
                                },
                            });
                            vm.$refs.conservation_status_datatable.vmDataTable.ajax.reload();
                        },
                        (error) => {
                            console.log(error);
                        }
                    );
                }
            });
        },
        reinstateCSProposal: function (conservation_status_id) {
            let vm = this;
            swal.fire({
                title: 'Reinstate Proposal',
                text: 'Are you sure you want to reinstate this proposal?',
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Reinstate Proposal',
                reverseButtons: true,
                buttonsStyling: false,
                customClass: {
                    confirmButton: 'btn btn-primary',
                    cancelButton: 'btn btn-secondary me-2',
                },
            }).then((result) => {
                if (result.isConfirmed) {
                    fetch(
                        api_endpoints.reinstate_cs_proposal(
                            conservation_status_id
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
                                text: 'Your proposal has been reinstated. You may continue to work on it now.',
                                icon: 'success',
                                customClass: {
                                    confirmButton: 'btn btn-primary',
                                },
                            });
                            vm.$refs.conservation_status_datatable.vmDataTable.ajax.reload();
                        },
                        (error) => {
                            console.log(error);
                        }
                    );
                }
            });
        },

        addEventListeners: function () {
            let vm = this;
            // External Discard listener
            vm.$refs.conservation_status_datatable.vmDataTable.on(
                'click',
                'a[data-discard-cs-proposal]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-discard-cs-proposal');
                    vm.discardCSProposal(id);
                }
            );
            vm.$refs.conservation_status_datatable.vmDataTable.on(
                'click',
                'a[data-reinstate-cs-proposal]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-reinstate-cs-proposal');
                    vm.reinstateCSProposal(id);
                }
            );
            vm.$refs.conservation_status_datatable.vmDataTable.on(
                'childRow.dt',
                function () {
                    helpers.enablePopovers();
                }
            );
        },
        check_assessor: function (proposal) {
            let vm = this;
            if (proposal.assigned_officer) {
                {
                    if (proposal.assigned_officer == vm.profile.user.full_name)
                        return true;
                    else return false;
                }
            } else {
                var assessor = proposal.allowed_assessors.filter(
                    function (elem) {
                        return (elem.id = vm.profile.user.id);
                    }
                );

                if (assessor.length > 0) return true;
                else return false;
            }
        },
    },
};
</script>
<style scoped>
.dt-buttons {
    float: right;
}

.collapse-icon {
    cursor: pointer;
}

.collapse-icon::before {
    top: 5px;
    left: 4px;
    height: 14px;
    width: 14px;
    border-radius: 14px;
    line-height: 14px;
    border: 2px solid white;
    line-height: 14px;
    content: '-';
    color: white;
    background-color: #d33333;
    display: inline-block;
    box-shadow: 0px 0px 3px #444;
    box-sizing: content-box;
    text-align: center;
    text-indent: 0 !important;
    font-family:
        'Courier New',
        Courier monospace;
    margin: 5px;
}

.expand-icon {
    cursor: pointer;
}

.expand-icon::before {
    top: 5px;
    left: 4px;
    height: 14px;
    width: 14px;
    border-radius: 14px;
    line-height: 14px;
    border: 2px solid white;
    line-height: 14px;
    content: '+';
    color: white;
    background-color: #337ab7;
    display: inline-block;
    box-shadow: 0px 0px 3px #444;
    box-sizing: content-box;
    text-align: center;
    text-indent: 0 !important;
    font-family:
        'Courier New',
        Courier monospace;
    margin: 5px;
}
</style>
