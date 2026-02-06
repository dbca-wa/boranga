<template id="species_flora_cs_referrals_dashboard">
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
    name: 'CSReferralsFloraTable',
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
        filterCSRefFloraScientificName_cache: {
            type: String,
            required: false,
            default: 'filterCSRefFloraScientificName',
        },
        filterCSRefFloraCommonName_cache: {
            type: String,
            required: false,
            default: 'filterCSRefFloraCommonName',
        },
        filterCSRefFloraPhylogeneticGroup_cache: {
            type: String,
            required: false,
            default: 'filterCSRefFloraPhylogeneticGroup',
        },
        filterCSRefFloraFamily_cache: {
            type: String,
            required: false,
            default: 'filterCSRefFloraFamily',
        },
        filterCSRefFloraGenus_cache: {
            type: String,
            required: false,
            default: 'filterCSRefFloraGenus',
        },
        filterCSRefFloraConservationList_cache: {
            type: String,
            required: false,
            default: 'filterCSRefFloraConservationList',
        },
        filterCSRefFloraConservationCategory_cache: {
            type: String,
            required: false,
            default: 'filterCSRefFloraConservationCategory',
        },
        filterCSRefFloraApplicationStatus_cache: {
            type: String,
            required: false,
            default: 'filterCSRefFloraApplicationStatus',
        },
    },
    data() {
        return {
            datatable_id: 'species_flora_cs_ref-datatable-' + uuid(),

            filterCSRefFloraScientificName: sessionStorage.getItem(
                this.filterCSRefFloraScientificName_cache
            )
                ? sessionStorage.getItem(
                      this.filterCSRefFloraScientificName_cache
                  )
                : 'all',

            filterCSRefFloraCommonName: sessionStorage.getItem(
                this.filterCSRefFloraCommonName_cache
            )
                ? sessionStorage.getItem(this.filterCSRefFloraCommonName_cache)
                : 'all',

            filterCSRefFloraPhylogeneticGroup: sessionStorage.getItem(
                this.filterCSRefFloraPhylogeneticGroup_cache
            )
                ? sessionStorage.getItem(
                      this.filterCSRefFloraPhylogeneticGroup_cache
                  )
                : 'all',

            filterCSRefFloraFamily: sessionStorage.getItem(
                this.filterCSRefFloraFamily_cache
            )
                ? sessionStorage.getItem(this.filterCSRefFloraFamily_cache)
                : 'all',

            filterCSRefFloraGenus: sessionStorage.getItem(
                this.filterCSRefFloraGenus_cache
            )
                ? sessionStorage.getItem(this.filterCSRefFloraGenus_cache)
                : 'all',

            filterCSRefFloraConservationList: sessionStorage.getItem(
                this.filterCSRefFloraConservationList_cache
            )
                ? sessionStorage.getItem(
                      this.filterCSRefFloraConservationList_cache
                  )
                : 'all',

            filterCSRefFloraConservationCategory: sessionStorage.getItem(
                this.filterCSRefFloraConservationCategory_cache
            )
                ? sessionStorage.getItem(
                      this.filterCSRefFloraConservationCategory_cache
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
                this.filterCSRefFloraCommonName === 'all' &&
                this.filterCSRefFloraPhylogeneticGroup === 'all' &&
                this.filterCSRefFloraFamily === 'all' &&
                this.filterCSRefFloraGenus === 'all' &&
                this.filterCSRefFloraConservationList === 'all' &&
                this.filterCSRefFloraConservationCategory === 'all' &&
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
                vm.column_species_number,
                vm.column_scientific_name,
                vm.column_common_name,
                vm.column_status,
                vm.column_action,
                vm.column_can_be_processed,
            ];
            let buttons = [
                {
                    extend: 'excel',
                    title: 'Boranga CS Flora Proposals Referred to Me Excel Export',
                    text: '<i class="fa-solid fa-download"></i> Excel',
                    className: 'btn btn-primary me-2 rounded',
                    exportOptions: {
                        columns: ':not(.no-export)',
                        orthogonal: 'export',
                    },
                },
                {
                    extend: 'csv',
                    title: 'Boranga CS Flora Proposals Referred to Me CSV Export',
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
                    data: function (d) {
                        d.filter_group_type = vm.group_type_name;
                        d.filter_scientific_name =
                            vm.filterCSRefFloraScientificName;
                        d.filter_common_name = vm.filterCSRefFloraCommonName;
                        d.filter_informal_group =
                            vm.filterCSRefFloraPhylogeneticGroup;
                        d.filter_family = vm.filterCSRefFloraFamily;
                        d.filter_genus = vm.filterCSRefFloraGenus;
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
        filterCSRefFloraCommonName: function () {
            let vm = this;
            vm.$refs.flora_cs_ref_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterCSRefFloraCommonName_cache,
                vm.filterCSRefFloraCommonName
            );
        },
        filterCSRefFloraPhylogeneticGroup: function () {
            let vm = this;
            vm.$refs.flora_cs_ref_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterCSRefFloraPhylogeneticGroup_cache,
                vm.filterCSRefFloraPhylogeneticGroup
            );
        },
        filterCSRefFloraFamily: function () {
            let vm = this;
            vm.$refs.flora_cs_ref_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterCSRefFloraFamily_cache,
                vm.filterCSRefFloraFamily
            );
        },
        filterCSRefFloraGenus: function () {
            let vm = this;
            vm.$refs.flora_cs_ref_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterCSRefFloraGenus_cache,
                vm.filterCSRefFloraGenus
            );
        },
        filterCSRefFloraConservationList: function () {
            let vm = this;
            vm.$refs.flora_cs_ref_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterCSRefFloraConservationList_cache,
                vm.filterCSRefFloraConservationList
            );
        },
        filterCSRefFloraConservationCategory: function () {
            let vm = this;
            vm.$refs.flora_cs_ref_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterCSRefFloraConservationCategory_cache,
                vm.filterCSRefFloraConservationCategory
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
            vm.initialiseCommonNameLookup();
            vm.initialisePhyloGroupLookup();
            vm.initialiseFamilyLookup();
            vm.initialiseGeneraLookup();
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
                sessionStorage.getItem('filterCSRefFloraCommonName') != 'all' &&
                sessionStorage.getItem('filterCSRefFloraCommonName') != null
            ) {
                newOption = new Option(
                    sessionStorage.getItem('filterCSRefFloraCommonNameText'),
                    vm.filterCSRefFloraCommonName,
                    false,
                    true
                );
                $('#cs_ref_common_name_lookup').append(newOption);
            }
            if (
                sessionStorage.getItem('filterCSRefFloraPhylogeneticGroup') !=
                    'all' &&
                sessionStorage.getItem('filterCSRefFloraPhylogeneticGroup') !=
                    null
            ) {
                newOption = new Option(
                    sessionStorage.getItem(
                        'filterCSRefFloraPhylogeneticGroupText'
                    ),
                    vm.filterCSRefFloraPhylogeneticGroup,
                    false,
                    true
                );
                $('#cs_ref_phylo_group_lookup').append(newOption);
            }
            if (
                sessionStorage.getItem('filterCSRefFloraFamily') != 'all' &&
                sessionStorage.getItem('filterCSRefFloraFamily') != null
            ) {
                newOption = new Option(
                    sessionStorage.getItem('filterCSRefFloraFamilyText'),
                    vm.filterCSRefFloraFamily,
                    false,
                    true
                );
                $('#cs_ref_family_lookup').append(newOption);
            }
            if (
                sessionStorage.getItem('filterCSRefFloraGenus') != 'all' &&
                sessionStorage.getItem('filterCSRefFloraGenus') != null
            ) {
                newOption = new Option(
                    sessionStorage.getItem('filterCSRefFloraGenusText'),
                    vm.filterCSRefFloraGenus,
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
                    vm.filterCSRefFloraCommonName = data;
                    sessionStorage.setItem(
                        'filterCSRefFloraCommonNameText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.filterCSRefFloraCommonName = 'all';
                    sessionStorage.setItem(
                        'filterCSRefFloraCommonNameText',
                        ''
                    );
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-cs_ref_common_name_lookup-results"]'
                    );
                    // move focus to select2 field
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
                    vm.filterCSRefFloraPhylogeneticGroup = data;
                    sessionStorage.setItem(
                        'filterCSRefFloraPhylogeneticGroupText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.filterCSRefFloraPhylogeneticGroup = 'all';
                    sessionStorage.setItem(
                        'filterCSRefFloraPhylogeneticGroupText',
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
                    vm.filterCSRefFloraFamily = data;
                    sessionStorage.setItem(
                        'filterCSRefFloraFamilyText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.filterCSRefFloraFamily = 'all';
                    sessionStorage.setItem('filterCSRefFloraFamilyText', '');
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
                    vm.filterCSRefFloraGenus = data;
                    sessionStorage.setItem(
                        'filterCSRefFloraGenusText',
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm.filterCSRefFloraGenus = 'all';
                    sessionStorage.setItem('filterCSRefFloraGenusText', '');
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
