<template id="species_fauna_cs_referrals_dashboard">
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
                        <label for="cs_ref_common_name_lookup"
                            >Common Name:</label
                        >
                        <select
                            id="cs_ref_common_name_lookup"
                            ref="cs_ref_common_name_lookup"
                            name="cs_ref_common_name_lookup"
                            class="form-control"
                        />
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="cs_ref_phylo_group_lookup"
                            >Informal Group:</label
                        >
                        <select
                            id="cs_ref_phylo_group_lookup"
                            ref="cs_ref_phylo_group_lookup"
                            name="cs_ref_phylo_group_lookup"
                            class="form-control"
                        />
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="cs_ref_family_lookup">Family:</label>
                        <select
                            id="cs_ref_family_lookup"
                            ref="cs_ref_family_lookup"
                            name="cs_ref_family_lookup"
                            class="form-control"
                        />
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="cs_ref_genera_lookup">Genus:</label>
                        <select
                            id="cs_ref_genera_lookup"
                            ref="cs_ref_genera_lookup"
                            name="cs_ref_genera_lookup"
                            class="form-control"
                        />
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="">Status:</label>
                        <select
                            v-model="filterCSRefFaunaApplicationStatus"
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
                    ref="fauna_cs_ref_datatable"
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
    name: 'CSReferralsFaunaTable',
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
        filterCSRefFaunaScientificName_cache: {
            type: String,
            required: false,
            default: 'filterCSRefFaunaScientificName',
        },
        filterCSRefFaunaCommonName_cache: {
            type: String,
            required: false,
            default: 'filterCSRefFaunaCommonName',
        },
        filterCSRefFaunaPhylogeneticGroup_cache: {
            type: String,
            required: false,
            default: 'filterCSRefFaunaPhylogeneticGroup',
        },
        filterCSRefFaunaFamily_cache: {
            type: String,
            required: false,
            default: 'filterCSRefFaunaFamily',
        },
        filterCSRefFaunaGenus_cache: {
            type: String,
            required: false,
            default: 'filterCSRefFaunaGenus',
        },
        filterCSRefFaunaApplicationStatus_cache: {
            type: String,
            required: false,
            default: 'filterCSRefFaunaApplicationStatus',
        },
    },
    data() {
        return {
            datatable_id: 'species_fauna_cs_ref-datatable-' + uuid(),

            filterCSRefFaunaScientificName: sessionStorage.getItem(
                this.filterCSRefFaunaScientificName_cache
            )
                ? sessionStorage.getItem(
                      this.filterCSRefFaunaScientificName_cache
                  )
                : 'all',

            filterCSRefFaunaCommonName: sessionStorage.getItem(
                this.filterCSRefFaunaCommonName_cache
            )
                ? sessionStorage.getItem(this.filterCSRefFaunaCommonName_cache)
                : 'all',

            filterCSRefFaunaPhylogeneticGroup: sessionStorage.getItem(
                this.filterCSRefFaunaPhylogeneticGroup_cache
            )
                ? sessionStorage.getItem(
                      this.filterCSRefFaunaPhylogeneticGroup_cache
                  )
                : 'all',

            filterCSRefFaunaFamily: sessionStorage.getItem(
                this.filterCSRefFaunaFamily_cache
            )
                ? sessionStorage.getItem(this.filterCSRefFaunaFamily_cache)
                : 'all',

            filterCSRefFaunaGenus: sessionStorage.getItem(
                this.filterCSRefFaunaGenus_cache
            )
                ? sessionStorage.getItem(this.filterCSRefFaunaGenus_cache)
                : 'all',

            filterCSRefFaunaApplicationStatus: sessionStorage.getItem(
                this.filterCSRefFaunaApplicationStatus_cache
            )
                ? sessionStorage.getItem(
                      this.filterCSRefFaunaApplicationStatus_cache
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
                this.filterCSRefFaunaScientificName === 'all' &&
                this.filterCSRefFaunaCommonName === 'all' &&
                this.filterCSRefFaunaPhylogeneticGroup === 'all' &&
                this.filterCSRefFaunaFamily === 'all' &&
                this.filterCSRefFaunaGenus === 'all' &&
                this.filterCSRefFaunaApplicationStatus === 'all'
            ) {
                return false;
            } else {
                return true;
            }
        },
        datatable_headers: function () {
            return [
                'Number',
                'Species',
                'Scientific Name',
                'Common Name',
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
        column_species_number: function () {
            return {
                data: 'species_number',
                orderable: true,
                searchable: true,
                visible: true,
                name: 'conservation_status__species__species_number',
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
        column_common_name: function () {
            return {
                data: 'common_name',
                orderable: true,
                searchable: true,
                visible: true,
                render: function (value, type) {
                    let result = helpers.dtPopover(value, 30, 'hover');
                    return type == 'export' ? value : result;
                },
                name: 'conservation_status__species__taxonomy__vernaculars__vernacular_name',
            };
        },
        column_status: function () {
            return {
                // 9. Workflow Status
                data: 'processing_status',
                orderable: true,
                searchable: true,
                visible: true,
                name: 'conservation_status__processing_status',
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
                    links += full.can_user_process
                        ? `<a href='/internal/conservation-status/${full.conservation_status}/referral/${full.id}'>Process</a><br/>`
                        : `<a href='/internal/conservation-status/${full.conservation_status}/referral/${full.id}'>View</a><br/>`;
                    return links;
                },
            };
        },
        column_conservation_status: function () {
            return {
                data: 'conservation_status',
                visible: false,
            };
        },
        column_can_be_processed: function () {
            return {
                data: 'can_be_processed',
                visible: false,
            };
        },
        column_can_user_process: function () {
            return {
                data: 'can_user_process',
                visible: false,
            };
        },
        datatable_options: function () {
            let vm = this;

            let columns = [
                vm.column_number,
                vm.column_species_number,
                vm.column_scientific_name,
                vm.column_common_name,
                vm.column_status,
                vm.column_action,
            ];
            let buttons = [
                {
                    extend: 'excel',
                    title: 'Boranga CS Fauna Proposals Referred to Me Excel Export',
                    text: '<i class="fa-solid fa-download"></i> Excel',
                    className: 'btn btn-primary me-2 rounded',
                    exportOptions: {
                        columns: ':not(.no-export)',
                        orthogonal: 'export',
                    },
                },
                {
                    extend: 'csv',
                    title: 'Boranga CS Fauna Proposals Referred to Me CSV Export',
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
                    method: 'post',
                    headers: {
                        'X-CSRFToken': helpers.getCookie('csrftoken'),
                    },
                    // adding extra GET params for Custom filtering
                    data: function (d) {
                        d.filter_group_type = vm.group_type_name;
                        d.filter_scientific_name =
                            vm.filterCSRefFaunaScientificName;
                        d.filter_common_name = vm.filterCSRefFaunaCommonName;
                        d.filter_informal_group =
                            vm.filterCSRefFaunaPhylogeneticGroup;
                        d.filter_family = vm.filterCSRefFaunaFamily;
                        d.filter_genus = vm.filterCSRefFaunaGenus;
                        d.filter_application_status =
                            vm.filterCSRefFaunaApplicationStatus;
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
        filterCSRefFaunaScientificName: function () {
            let vm = this;
            vm.$refs.fauna_cs_ref_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                false
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterCSRefFaunaScientificName_cache,
                vm.filterCSRefFaunaScientificName
            );
        },
        filterCSRefFaunaCommonName: function () {
            let vm = this;
            vm.$refs.fauna_cs_ref_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                false
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterCSRefFaunaCommonName_cache,
                vm.filterCSRefFaunaCommonName
            );
        },
        filterCSRefFaunaPhylogeneticGroup: function () {
            let vm = this;
            vm.$refs.fauna_cs_ref_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                false
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterCSRefFaunaPhylogeneticGroup_cache,
                vm.filterCSRefFaunaPhylogeneticGroup
            );
        },
        filterCSRefFaunaFamily: function () {
            let vm = this;
            vm.$refs.fauna_cs_ref_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                false
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterCSRefFaunaFamily_cache,
                vm.filterCSRefFaunaFamily
            );
        },
        filterCSRefFaunaGenus: function () {
            let vm = this;
            vm.$refs.fauna_cs_ref_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                false
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterCSRefFaunaGenus_cache,
                vm.filterCSRefFaunaGenus
            );
        },
        filterCSRefFaunaApplicationStatus: function () {
            let vm = this;
            vm.$refs.fauna_cs_ref_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                false
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterCSRefFaunaApplicationStatus_cache,
                vm.filterCSRefFaunaApplicationStatus
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
            vm.initialiseCommonNameLookup();
            vm.initialisePhyloGroupLookup();
            vm.initialiseFamilyLookup();
            vm.initialiseGeneraLookup();
            vm.addEventListeners();
            var newOption = null;
            if (
                sessionStorage.getItem('filterCSRefFaunaScientificName') !=
                    'all' &&
                sessionStorage.getItem('filterCSRefFaunaScientificName') != null
            ) {
                newOption = new Option(
                    sessionStorage.getItem(
                        'filterCSRefFaunaScientificNameText'
                    ),
                    vm.filterCSRefFaunaScientificName,
                    false,
                    true
                );
                $('#cs_ref_scientific_name_lookup').append(newOption);
            }
            if (
                sessionStorage.getItem('filterCSRefFaunaCommonName') != 'all' &&
                sessionStorage.getItem('filterCSRefFaunaCommonName') != null
            ) {
                newOption = new Option(
                    sessionStorage.getItem('filterCSRefFaunaCommonNameText'),
                    vm.filterCSRefFaunaCommonName,
                    false,
                    true
                );
                $('#cs_ref_common_name_lookup').append(newOption);
            }
            if (
                sessionStorage.getItem('filterCSRefFaunaPhylogeneticGroup') !=
                    'all' &&
                sessionStorage.getItem('filterCSRefFaunaPhylogeneticGroup') !=
                    null
            ) {
                newOption = new Option(
                    sessionStorage.getItem(
                        'filterCSRefFaunaPhylogeneticGroupText'
                    ),
                    vm.filterCSRefFaunaPhylogeneticGroup,
                    false,
                    true
                );
                $('#cs_ref_phylo_group_lookup').append(newOption);
            }
            if (
                sessionStorage.getItem('filterCSRefFaunaFamily') != 'all' &&
                sessionStorage.getItem('filterCSRefFaunaFamily') != null
            ) {
                newOption = new Option(
                    sessionStorage.getItem('filterCSRefFaunaFamilyText'),
                    vm.filterCSRefFaunaFamily,
                    false,
                    true
                );
                $('#cs_ref_family_lookup').append(newOption);
            }
            if (
                sessionStorage.getItem('filterCSRefFaunaGenus') != 'all' &&
                sessionStorage.getItem('filterCSRefFaunaGenus') != null
            ) {
                newOption = new Option(
                    sessionStorage.getItem('filterCSRefFaunaGenusText'),
                    vm.filterCSRefFaunaGenus,
                    false,
                    true
                );
                $('#cs_ref_genera_lookup').append(newOption);
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
                                group_type_id: vm.group_type_id,
                                cs_referral: true,
                            };
                            return query;
                        },
                    },
                })
                .on('select2:select', function (e) {
                    let data = e.params.data.id;
                    vm.filterCSRefFaunaScientificName = data;
                    sessionStorage.setItem(
                        'filterCSRefFaunaScientificNameText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.filterCSRefFaunaScientificName = 'all';
                    sessionStorage.setItem(
                        'filterCSRefFaunaScientificNameText',
                        ''
                    );
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-cs_ref_scientific_name_lookup-results"]'
                    );
                    searchField[0].focus();
                });
        },
        initialiseCommonNameLookup: function () {
            let vm = this;
            $(vm.$refs.cs_ref_common_name_lookup)
                .select2({
                    minimumInputLength: 2,
                    theme: 'bootstrap-5',
                    allowClear: true,
                    placeholder: 'Select Common Name',
                    ajax: {
                        url: api_endpoints.common_name_lookup,
                        dataType: 'json',
                        data: function (params) {
                            var query = {
                                term: params.term,
                                type: 'public',
                                group_type_id: vm.group_type_id,
                                cs_referral: true,
                            };
                            return query;
                        },
                    },
                })
                .on('select2:select', function (e) {
                    let data = e.params.data.id;
                    vm.filterCSRefFaunaCommonName = data;
                    sessionStorage.setItem(
                        'filterCSRefFaunaCommonNameText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.filterCSRefFaunaCommonName = 'all';
                    sessionStorage.setItem(
                        'filterCSRefFaunaCommonNameText',
                        ''
                    );
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-cs_ref_common_name_lookup-results"]'
                    );
                    searchField[0].focus();
                });
        },
        initialisePhyloGroupLookup: function () {
            let vm = this;
            $(vm.$refs.cs_ref_phylo_group_lookup)
                .select2({
                    minimumInputLength: 2,
                    theme: 'bootstrap-5',
                    allowClear: true,
                    placeholder: 'Select Informal Group',
                    ajax: {
                        url: api_endpoints.phylo_group_lookup,
                        dataType: 'json',
                        data: function (params) {
                            var query = {
                                term: params.term,
                                type: 'public',
                                group_type_id: vm.group_type_id,
                                cs_referral: true,
                            };
                            return query;
                        },
                    },
                })
                .on('select2:select', function (e) {
                    let data = e.params.data.id;
                    vm.filterCSRefFaunaPhylogeneticGroup = data;
                    sessionStorage.setItem(
                        'filterCSRefFaunaPhylogeneticGroupText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.filterCSRefFaunaPhylogeneticGroup = 'all';
                    sessionStorage.setItem(
                        'filterCSRefFaunaPhylogeneticGroupText',
                        ''
                    );
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-cs_ref_phylo_group_lookup-results"]'
                    );
                    searchField[0].focus();
                });
        },
        initialiseFamilyLookup: function () {
            let vm = this;
            $(vm.$refs.cs_ref_family_lookup)
                .select2({
                    minimumInputLength: 2,
                    theme: 'bootstrap-5',
                    allowClear: true,
                    placeholder: 'Select Family',
                    ajax: {
                        url: api_endpoints.family_lookup,
                        dataType: 'json',
                        data: function (params) {
                            var query = {
                                term: params.term,
                                type: 'public',
                                group_type_id: vm.group_type_id,
                                cs_referral: true,
                            };
                            return query;
                        },
                    },
                })
                .on('select2:select', function (e) {
                    let data = e.params.data.id;
                    vm.filterCSRefFaunaFamily = data;
                    sessionStorage.setItem(
                        'filterCSRefFaunaFamilyText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.filterCSRefFaunaFamily = 'all';
                    sessionStorage.setItem('filterCSRefFaunaFamilyText', '');
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-cs_ref_family_lookup-results"]'
                    );
                    searchField[0].focus();
                });
        },
        initialiseGeneraLookup: function () {
            let vm = this;
            $(vm.$refs.cs_ref_genera_lookup)
                .select2({
                    minimumInputLength: 2,
                    theme: 'bootstrap-5',
                    allowClear: true,
                    placeholder: 'Select Genus',
                    ajax: {
                        url: api_endpoints.genera_lookup,
                        dataType: 'json',
                        data: function (params) {
                            var query = {
                                term: params.term,
                                type: 'public',
                                group_type_id: vm.group_type_id,
                                cs_referral: true,
                            };
                            return query;
                        },
                    },
                })
                .on('select2:select', function (e) {
                    let data = e.params.data.id;
                    vm.filterCSRefFaunaGenus = data;
                    sessionStorage.setItem(
                        'filterCSRefFaunaGenusText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.filterCSRefFaunaGenus = 'all';
                    sessionStorage.setItem('filterCSRefFaunaGenusText', '');
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-cs_ref_genera_lookup-results"]'
                    );
                    searchField[0].focus();
                });
        },
        addEventListeners: function () {
            let vm = this;
            vm.$refs.fauna_cs_ref_datatable.vmDataTable.on(
                'childRow.dt',
                function () {
                    helpers.enablePopovers();
                }
            );
        },
    },
};
</script>
