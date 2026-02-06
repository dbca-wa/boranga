<template id="external_occurrence_report_datatable">
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
                        <select
                            v-model="filterOCRGroupType"
                            class="form-select"
                        >
                            <option value="all">All</option>
                            <option
                                v-for="option in group_types"
                                :value="option.name"
                                :key="option.id"
                            >
                                {{ option.display }}
                            </option>
                        </select>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="ocr_scientific_name_lookup"
                            >Scientific Name:</label
                        >
                        <select
                            id="ocr_scientific_name_lookup"
                            ref="ocr_scientific_name_lookup"
                            name="ocr_scientific_name_lookup"
                            class="form-control"
                        />
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="ocr_community_name_lookup"
                            >Community Name:</label
                        >
                        <select
                            id="ocr_community_name_lookup"
                            ref="ocr_community_name_lookup"
                            name="ocr_community_name_lookup"
                            class="form-control"
                        />
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="">Status:</label>
                        <select
                            v-model="filterOCRApplicationStatus"
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
        </CollapsibleFilters>
        <div v-if="addOCRVisibility" class="col-md-12 dropdown">
            <div class="text-end">
                <button
                    id="ocr_type"
                    class="btn btn-primary dropdown-toggle mb-2"
                    type="button"
                    data-bs-toggle="dropdown"
                    aria-expanded="false"
                >
                    Report Occurrence
                </button>
                <ul class="dropdown-menu" aria-labelledby="ocr_type">
                    <li v-for="group in group_types" :key="group.id">
                        <a
                            class="dropdown-item"
                            role="button"
                            @click.prevent="
                                createOccurrenceReport(group.id, group.display)
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
                    ref="occurrence_report_datatable"
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
    name: 'ExternalOccurrenceReportDatatable',
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
        filterOCRGroupType_cache: {
            type: String,
            required: false,
            default: 'filterOCRGroupType',
        },
        filterOCRScientificName_cache: {
            type: String,
            required: false,
            default: 'filterOCRScientificName',
        },
        filterOCRExCommunityName_cache: {
            type: String,
            required: false,
            default: 'filterOCRExCommunityName',
        },
        filterOCRApplicationStatus_cache: {
            type: String,
            required: false,
            default: 'filterOCRApplicationStatus',
        },
    },
    data() {
        return {
            datatable_id: 'ocuurrence-report-datatable-' + uuid(),

            // selected values for filtering
            filterOCRGroupType: sessionStorage.getItem(
                this.filterOCRGroupType_cache
            )
                ? sessionStorage.getItem(this.filterOCRGroupType_cache)
                : 'all',

            filterOCRScientificName: sessionStorage.getItem(
                this.filterOCRScientificName_cache
            )
                ? sessionStorage.getItem(this.filterOCRScientificName_cache)
                : 'all',

            filterOCRExCommunityName: sessionStorage.getItem(
                this.filterOCRExCommunityName_cache
            )
                ? sessionStorage.getItem(this.filterOCRExCommunityName_cache)
                : 'all',

            filterOCRApplicationStatus: sessionStorage.getItem(
                this.filterOCRApplicationStatus_cache
            )
                ? sessionStorage.getItem(this.filterOCRApplicationStatus_cache)
                : 'all',

            //Filter list for scientific name and common name
            group_types: [],

            // filtering options
            external_status: [
                { value: 'draft', name: 'Draft' },
                { value: 'discarded', name: 'Discarded' },
                { value: 'with_assessor', name: 'Under Review' },
                { value: 'approved', name: 'Approved' },
                { value: 'declined', name: 'Declined' },
                { value: 'closed', name: 'Closed' },
            ],
            proposal_status: [],
        };
    },
    computed: {
        filterApplied: function () {
            if (
                this.filterOCRGroupType === 'all' &&
                this.filterOCRScientificName === 'all' &&
                this.filterOCRExCommunityName === 'all' &&
                this.filterOCRApplicationStatus === 'all'
            ) {
                return false;
            } else {
                return true;
            }
        },
        addOCRVisibility: function () {
            let visibility = false;
            visibility = true;
            return visibility;
        },
        datatable_headers: function () {
            return [
                'ID',
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
                name: 'id',
            };
        },
        column_number: function () {
            return {
                data: 'occurrence_report_number',
                orderable: true,
                searchable: true,
                visible: true,
                name: 'id',
            };
        },
        column_type: function () {
            return {
                data: 'group_type',
                orderable: true,
                searchable: true,
                visible: true,
                name: 'group_type__name',
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
                name: 'species__taxonomy__scientific_name',
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
                searchable: true,
                visible: true,
                name: 'customer_status',
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
                    if (full.can_user_edit) {
                        if (full.processing_status == 'discarded') {
                            links += `<a href='#${full.id}' data-reinstate-ocr-proposal='${full.id}'>Reinstate</a><br/>`;
                        } else {
                            links += `<a href='/external/occurrence-report/${full.id}'>Continue</a><br/>`;
                            links += `<a href='#${full.id}' data-discard-ocr-proposal='${full.id}'>Discard</a><br/>`;
                        }
                    } else if (full.can_user_view) {
                        links += `<a href='/external/occurrence-report/${full.id}'>View</a><br />`;
                    }
                    links += `<a href='#${full.id}' data-copy-ocr-proposal='${full.id}'>Copy</a>`;
                    return links;
                },
            };
        },
        datatable_options: function () {
            let vm = this;
            let columns = [];
            let buttons = [
                {
                    extend: 'excel',
                    title: 'Boranga Occurrence Report Excel Export',
                    text: '<i class="fa-solid fa-download"></i> Excel',
                    className: 'btn btn-primary me-2 rounded',
                    exportOptions: {
                        columns: ':not(.no-export)',
                        orthogonal: 'export',
                    },
                },
                {
                    extend: 'csv',
                    title: 'Boranga Occurrence Report CSV Export',
                    text: '<i class="fa-solid fa-download"></i> CSV',
                    className: 'btn btn-primary rounded',
                    exportOptions: {
                        columns: ':not(.no-export)',
                        orthogonal: 'export',
                    },
                },
            ];
            columns = [
                vm.column_id,
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
                    { responsivePriority: 1, targets: 1 },
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
                        d.filter_group_type = vm.filterOCRGroupType;
                        d.filter_scientific_name = vm.filterOCRScientificName;
                        d.filter_community_name = vm.filterOCRExCommunityName;
                        d.filter_application_status =
                            vm.filterOCRApplicationStatus;
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
        filterOCRGroupType: function () {
            let vm = this;
            vm.$refs.occurrence_report_datatable.vmDataTable.ajax.reload(); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterOCRGroupType_cache,
                vm.filterOCRGroupType
            );
        },
        filterOCRScientificName: function () {
            let vm = this;
            vm.$refs.occurrence_report_datatable.vmDataTable.ajax.reload(); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterOCRScientificName_cache,
                vm.filterOCRScientificName
            );
        },
        filterOCRExCommunityName: function () {
            let vm = this;
            vm.$refs.occurrence_report_datatable.vmDataTable.ajax.reload(); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterOCRExCommunityName_cache,
                vm.filterOCRExCommunityName
            );
        },
        filterOCRApplicationStatus: function () {
            let vm = this;
            vm.$refs.occurrence_report_datatable.vmDataTable.ajax.reload(); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterOCRApplicationStatus_cache,
                vm.filterOCRApplicationStatus
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
            var newOption = null;
            if (
                sessionStorage.getItem('filterOCRScientificName') != 'all' &&
                sessionStorage.getItem('filterOCRScientificName') != null
            ) {
                newOption = new Option(
                    sessionStorage.getItem('filterOCRScientificNameText'),
                    vm.filterOCRScientificName,
                    false,
                    true
                );
                $('#ocr_scientific_name_lookup').append(newOption);
            }
            if (
                sessionStorage.getItem('filterOCRExCommunityName') != 'all' &&
                sessionStorage.getItem('filterOCRExCommunityName') != null
            ) {
                newOption = new Option(
                    sessionStorage.getItem('filterOCRExCommunityNameText'),
                    vm.filterOCRExCommunityName,
                    false,
                    true
                );
                $('#ocr_community_name_lookup').append(newOption);
            }
        });
    },
    methods: {
        initialiseScientificNameLookup: function () {
            let vm = this;
            $(vm.$refs.ocr_scientific_name_lookup)
                .select2({
                    minimumInputLength: 2,
                    theme: 'bootstrap-5',
                    allowClear: true,
                    placeholder: 'Select Scientific Name',
                    ajax: {
                        url: api_endpoints.species_lookup,
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
                    vm.filterOCRScientificName = data;
                    sessionStorage.setItem(
                        'filterOCRScientificNameText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.filterOCRScientificName = 'all';
                    sessionStorage.setItem('filterOCRScientificNameText', '');
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-ocr_scientific_name_lookup-results"]'
                    );
                    // move focus to select2 field
                    searchField[0].focus();
                });
        },
        initialiseCommunityNameLookup: function () {
            let vm = this;
            $(vm.$refs.ocr_community_name_lookup)
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
                    vm.filterOCRExCommunityName = data;
                    sessionStorage.setItem(
                        'filterOCRExCommunityNameText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.filterOCRExCommunityName = 'all';
                    sessionStorage.setItem('filterOCRExCommunityNameText', '');
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-ocr_community_name_lookup-results"]'
                    );
                    // move focus to select2 field
                    searchField[0].focus();
                });
        },
        fetchFilterLists: function () {
            let vm = this;
            vm.proposal_status = vm.external_status;
            fetch(api_endpoints.group_types_dict).then(
                async (response) => {
                    vm.group_types = await response.json();
                },
                (error) => {
                    console.log(error);
                }
            );
        },
        createOccurrenceReport: async function (group_type, group_type_name) {
            swal.fire({
                title: `Add ${group_type_name} Occurrence Report`,
                text: `Are you sure you want to add a new ${group_type_name} Occurrence Report?`,
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: `Add ${group_type_name} Occurrence Report`,
                reverseButtons: true,
                customClass: {
                    confirmButton: 'btn btn-primary',
                },
            }).then(async (swalresult) => {
                if (swalresult.isConfirmed) {
                    let newOCRId = null;
                    try {
                        const createUrl = api_endpoints.occurrence_report + '/';
                        let payload = new Object();
                        payload.group_type_id = group_type;
                        let response = await fetch(createUrl, {
                            method: 'POST',
                            headers: {
                                'Content-Type': 'application/json',
                            },
                            body: JSON.stringify(payload),
                        });
                        const data = await response.json();
                        if (data) {
                            newOCRId = data;
                        }
                    } catch (err) {
                        console.log(err);
                    }
                    this.$router.push({
                        name: 'draft_ocr_proposal',
                        params: { occurrence_report_id: newOCRId.id },
                    });
                }
            });
        },
        discardOCR: function (occurrence_report_id) {
            let vm = this;
            swal.fire({
                title: 'Discard Occurrence Report',
                text: 'Are you sure you want to discard this occurrence report?',
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Discard Occurrence Report',
                customClass: {
                    confirmButton: 'btn btn-primary',
                    cancelButton: 'btn btn-secondary',
                },
                reverseButtons: true,
            }).then((swalresult) => {
                if (swalresult.isConfirmed) {
                    fetch(
                        api_endpoints.discard_ocr_proposal(
                            occurrence_report_id
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
                                title: 'Occurrence Report Discarded',
                                text: 'The occurrence report has been discarded',
                                icon: 'success',
                                customClass: {
                                    confirmButton: 'btn btn-primary',
                                },
                            });
                            vm.$refs.occurrence_report_datatable.vmDataTable.ajax.reload();
                        },
                        (error) => {
                            console.log(error);
                        }
                    );
                }
            });
        },
        reinstateOCRProposal: function (occurrence_report_id) {
            let vm = this;
            swal.fire({
                title: 'Reinstate Report',
                text: 'Are you sure you want to reinstate this report?',
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Reinstate Report',
                customClass: {
                    confirmButton: 'btn btn-primary',
                    cancelButton: 'btn btn-secondary',
                },
                reverseButtons: true,
            }).then(
                (swalresult) => {
                    if (swalresult.isConfirmed) {
                        fetch(
                            api_endpoints.reinstate_ocr_proposal(
                                occurrence_report_id
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
                                    text: 'Your report has been reinstated',
                                    icon: 'success',
                                    customClass: {
                                        confirmButton: 'btn btn-primary',
                                    },
                                });
                                vm.$refs.occurrence_report_datatable.vmDataTable.ajax.reload(
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
        copyOCRProposal: function (occurrence_report_id) {
            let vm = this;
            swal.fire({
                title: 'Copy Occurrence Report',
                text: `Are you sure you want to make a copy of occurrence report ${constants.MODELS.OCCURRENCE_REPORT.MODEL_PREFIX}${occurrence_report_id}?`,
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Copy Occurrence Report',
                customClass: {
                    confirmButton: 'btn btn-primary',
                    cancelButton: 'btn btn-secondary',
                },
                reverseButtons: true,
            }).then((swalresult) => {
                if (swalresult.isConfirmed) {
                    fetch(
                        helpers.add_endpoint_json(
                            api_endpoints.occurrence_report,
                            occurrence_report_id + '/copy'
                        ),
                        {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                        }
                    ).then(
                        async (response) => {
                            const ocr_copy = await response.json();
                            swal.fire({
                                title: 'Copied',
                                text: `The occurrence report has been copied to ${ocr_copy.occurrence_report_number}. When you click OK, the new occurrence report will open in a new window.`,
                                icon: 'success',
                                customClass: {
                                    confirmButton: 'btn btn-primary',
                                },
                                didClose: () => {
                                    vm.$refs.occurrence_report_datatable.vmDataTable.ajax.reload();
                                    const routeData = this.$router.resolve({
                                        name: 'draft_ocr_proposal',
                                        params: {
                                            occurrence_report_id: ocr_copy.id,
                                        },
                                        query: { action: 'edit' },
                                    });
                                    window.open(routeData.href, '_blank');
                                },
                            });
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
            vm.$refs.occurrence_report_datatable.vmDataTable.on(
                'click',
                'a[data-discard-ocr-proposal]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-discard-ocr-proposal');
                    vm.discardOCR(id);
                }
            );
            vm.$refs.occurrence_report_datatable.vmDataTable.on(
                'click',
                'a[data-reinstate-ocr-proposal]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-reinstate-ocr-proposal');
                    vm.reinstateOCRProposal(id);
                }
            );
            vm.$refs.occurrence_report_datatable.vmDataTable.on(
                'click',
                'a[data-copy-ocr-proposal]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-copy-ocr-proposal');
                    vm.copyOCRProposal(id);
                }
            );
            vm.$refs.occurrence_report_datatable.vmDataTable.on(
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
