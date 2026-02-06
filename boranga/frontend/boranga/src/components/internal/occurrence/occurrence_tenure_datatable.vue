<template>
    <div id="occurrence_tenure_datatable_template">
        <CollapsibleFilters
            ref="collapsible_filters"
            component_title="Filters"
            :collapsed="!filterApplied"
            :show-warning-icon="filterApplied"
        >
            <div class="row">
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="occurrence_tenure_feature_id_lookup"
                            >Feature ID:</label
                        >
                        <select
                            id="occurrence_tenure_feature_id_lookup"
                            ref="occurrence_tenure_feature_id_lookup"
                            name="occurrence_tenure_feature_id_lookup"
                            class="form-control"
                        />
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label
                            for="occurrence_tenure_status_lookup"
                            class="text-nowrap"
                            >Status:</label
                        >
                        <select
                            ref="occurrence_tenure_status_lookup"
                            v-model="filterStatus"
                            class="form-select"
                        >
                            <option value="all">All</option>
                            <option
                                v-for="status in tenure_statuses"
                                :key="status.value"
                                :value="status.value"
                            >
                                {{ status.name }}
                            </option>
                        </select>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="occurrence_tenure_vesting_lookup"
                            >Vesting:</label
                        >
                        <select
                            id="occurrence_tenure_vesting_lookup"
                            ref="occurrence_tenure_vesting_lookup"
                            name="occurrence_tenure_vesting_lookup"
                            class="form-control"
                        />
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="occurrence_tenure_purpose_lookup"
                            >Purpose:</label
                        >
                        <select
                            id="occurrence_tenure_purpose_lookup"
                            ref="occurrence_tenure_purpose_lookup"
                            name="occurrence_tenure_purpose_lookup"
                            class="form-control"
                        />
                    </div>
                </div>
            </div>
        </CollapsibleFilters>
        <datatable
            :id="datatable_id"
            ref="occurrence_tenure_datatable"
            :dt-options="options"
            :dt-headers="headers"
        />
        <OccurrenceTenureModal
            ref="occurrence_tenure_modal"
            title="Tenure Area"
            :occurrence-id="occurrenceId"
            :url="occ_tenure_url"
            :change-warning="''"
            :always-read-only="['status', 'tenure_area_id', 'owner_name']"
            @refresh-from-response="updatedTenureArea"
        >
        </OccurrenceTenureModal>
        <div v-if="occTenureHistoryId">
            <OCCTenureHistory
                ref="occ_tenure_history"
                :key="occTenureHistoryId"
                :tenure-id="occTenureHistoryId"
            />
        </div>
    </div>
</template>

<script>
import { api_endpoints, constants, helpers } from '@/utils/hooks';
import datatable from '@/utils/vue/datatable.vue';
import { v4 as uuid } from 'uuid';
import CollapsibleFilters from '@/components/forms/collapsible_component.vue';
import OccurrenceTenureModal from '@/components/internal/occurrence/occurrence_tenure_modal.vue';
import OCCTenureHistory from '../../internal/occurrence/occ_tenure_history.vue';

export default {
    name: 'OccurrenceTenureDatatable',
    components: {
        datatable,
        CollapsibleFilters,
        OccurrenceTenureModal,
        OCCTenureHistory,
    },
    emit: ['highlight-on-map', 'edit-tenure-details'],
    props: {
        occurrenceId: {
            type: Number,
            required: false,
            default: null,
        },
        hrefContainerId: {
            type: String,
            required: false,
            default: '',
        },
        filterStatusCache: {
            type: String,
            required: false,
            default: 'occurrence_tenure_status_filter_cache',
        },
        filterFeatureIdCache: {
            type: String,
            required: false,
            default: 'occurrence_tenure_feature_id_filter_cache',
        },
        filterVestingCache: {
            type: String,
            required: false,
            default: 'occurrence_tenure_vesting_filter_cache',
        },
        filterPurposeCache: {
            type: String,
            required: false,
            default: 'occurrence_tenure_purpose_filter_cache',
        },
    },
    data: function () {
        return {
            uuid: uuid(),
            occTenureHistoryId: null,
            datatable_id: 'occurrence-tenure-datatable-' + uuid(),
            occ_tenure_paginated_url:
                api_endpoints.occurrence_tenure_paginated_internal,
            occ_tenure_url: api_endpoints.occurrence_tenure,
            headers: [
                'Feature ID',
                'Status',
                'Vesting',
                'Purpose',
                'Signif. to OCC',
                'Comments',
                'Owner/Manager',
                'Updated',
                'Action',
            ],
            tenure_statuses: [
                { value: 'current', name: 'Current' },
                { value: 'historical', name: 'Historical' },
            ],
            filterStatus: sessionStorage.getItem(this.filterStatusCache)
                ? sessionStorage.getItem(this.filterStatusCache)
                : 'current',
            filterFeatureId: sessionStorage.getItem(this.filterFeatureIdCache)
                ? sessionStorage.getItem(this.filterFeatureIdCache)
                : 'all',
            filterVesting: sessionStorage.getItem(this.filterVestingCache)
                ? sessionStorage.getItem(this.filterVestingCache)
                : 'all',
            filterPurpose: sessionStorage.getItem(this.filterPurposeCache)
                ? sessionStorage.getItem(this.filterPurposeCache)
                : 'all',
        };
    },
    computed: {
        filterApplied: function () {
            const allFiltersAreAll =
                this.filterStatus === 'current' &&
                this.filterFeatureId === 'all' &&
                this.filterVesting === 'all' &&
                this.filterPurpose === 'all';

            return !allFiltersAreAll;
        },
        column_featureid: function () {
            return {
                data: 'tenure_area_id',
                orderable: true,
                searchable: true,
                visible: true,
                render: function (data, type, row) {
                    return row.featureid;
                },
            };
        },
        column_tenure_area_id: function () {
            return {
                data: 'tenure_area_id',
                orderable: true,
                searchable: true,
                visible: true,
            };
        },
        column_owner_name: function () {
            return {
                data: 'owner_name',
                orderable: true,
                searchable: true,
                visible: true,
                render: function (data, type, row) {
                    let result = helpers.dtPopover(row.owner_name, 30, 'hover');
                    let links = type == 'export' ? row.owner_name : result;
                    return links;
                },
            };
        },
        column_vesting: function () {
            return {
                data: 'vesting',
                name: 'vesting.code, vesting.label',
                orderable: true,
                searchable: true,
                visible: true,
            };
        },
        column_purpose: function () {
            return {
                data: 'purpose',
                name: 'purpose.code, purpose.label',
                orderable: true,
                searchable: true,
                visible: true,
            };
        },
        column_comments: function () {
            return {
                data: 'comments',
                orderable: true,
                searchable: true,
                visible: true,
                render: function (data, type, row) {
                    let result = helpers.dtPopover(row.comments, 30, 'hover');
                    let links = type == 'export' ? row.comments : result;
                    return links;
                },
            };
        },
        column_significant_to_occurrence: function () {
            return {
                data: 'significant_to_occurrence',
                orderable: true,
                searchable: true,
                visible: true,
            };
        },
        column_status: function () {
            return {
                data: 'status',
                orderable: true,
                searchable: true,
                visible: true,
                render: function (data, type, row) {
                    return row.status_display;
                },
            };
        },
        column_datetime_updated: function () {
            return {
                data: 'datetime_updated',
                orderable: true,
                searchable: true,
                visible: true,
            };
        },
        column_action: function () {
            const vm = this;
            return {
                data: 'id',
                orderable: false,
                searchable: false,
                visible: true,

                render: function (data, type, row) {
                    const coordinates = row.tenure_area_point_on_surface
                        ? JSON.stringify(
                              row.tenure_area_point_on_surface.coordinates
                          )
                        : '';
                    let html = `<a href="#${vm.hrefContainerId}" data-highlight-on-map-coordinates="${coordinates}">Highlight on Map</a>`;
                    html += `<br><a href="#" data-edit-tenure-details="${data}">Edit Tenure Details</a>`;
                    html += `<br><a href='#' data-history-tenure='${data}'>History</a><br>`;
                    return html;
                },
            };
        },
        options: function () {
            let columns = [
                this.column_featureid,
                this.column_status,
                this.column_vesting,
                this.column_purpose,
                this.column_significant_to_occurrence,
                this.column_comments,
                this.column_owner_name,
                this.column_datetime_updated,
                this.column_action,
            ];
            let url = this.occ_tenure_paginated_url;
            if (this.occurrenceId) {
                url = `${this.occ_tenure_paginated_url}&occurrence_id=${this.occurrenceId}`;
            }
            return {
                autoWidth: false,
                language: {
                    processing: constants.DATATABLE_PROCESSING_HTML,
                },
                responsive: true,
                searching: true,
                ordering: true,
                order: [
                    [1, 'asc'],
                    [7, 'desc'],
                    [0, 'desc'],
                ],
                columnDefs: [
                    { responsivePriority: 1, targets: 0 },
                    { responsivePriority: 2, targets: -1 },
                ],
                fixedColumns: {
                    start: 1,
                    end: 1,
                },
                paging: true,
                ajax: {
                    url: url,
                    dataSrc: 'data',
                    data: (d) => {
                        d.filter_status = this.filterStatus;
                        d.tenure_area_id = this.filterFeatureId;
                        d.vesting = this.filterVesting;
                        d.purpose = this.filterPurpose;
                    },
                },
                dom:
                    "<'d-flex align-items-center'<'me-auto'l>fB>" +
                    "<'row'<'col-sm-12'tr>>" +
                    "<'d-flex align-items-center'<'me-auto'i>p>",
                buttons: [
                    {
                        extend: 'excel',
                        title: 'Boranga OCC Tenure Excel Export',
                        text: '<i class="fa-solid fa-download"></i> Excel',
                        className: 'btn btn-primary me-2 rounded',
                        exportOptions: {
                            orthogonal: 'export',
                        },
                    },
                    {
                        extend: 'csv',
                        title: 'Boranga OCC Tenure CSV Export',
                        text: '<i class="fa-solid fa-download"></i> CSV',
                        className: 'btn btn-primary rounded',
                        exportOptions: {
                            orthogonal: 'export',
                        },
                    },
                ],
                columns: columns,
                processing: true,
                serverSide: true,

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
        filterStatus: function () {
            this.$refs.occurrence_tenure_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            );
            sessionStorage.setItem(this.filterStatusCache, this.filterStatus);
        },
        filterFeatureId: function () {
            this.$refs.occurrence_tenure_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            );
            sessionStorage.setItem(
                this.filterFeatureIdCache,
                this.filterFeatureId
            );
        },
        filterVesting: function () {
            this.$refs.occurrence_tenure_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            );
            sessionStorage.setItem(this.filterVestingCache, this.filterVesting);
        },
        filterPurpose: function () {
            this.$refs.occurrence_tenure_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            );
            sessionStorage.setItem(this.filterPurposeCache, this.filterPurpose);
        },
        hrefContainerId: function () {
            // This is to ensure that the hrefContainerId is updated in the datatable
            this.$refs.occurrence_tenure_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            );
        },
    },
    mounted: function () {
        this.$nextTick(() => {
            // Make this a loop
            const filterLookupParameters = [
                {
                    ref: 'occurrence_tenure_feature_id_lookup',
                    vModelDataProperty: 'filterFeatureId',
                    placeholder: 'Select a feature ID',
                },
                {
                    ref: 'occurrence_tenure_vesting_lookup',
                    vModelDataProperty: 'filterVesting',
                    placeholder: 'Select a vesting',
                },
                {
                    ref: 'occurrence_tenure_purpose_lookup',
                    vModelDataProperty: 'filterPurpose',
                    placeholder: 'Select a purpose',
                },
            ];
            filterLookupParameters.forEach((parameters) => {
                this.initialiseFilterLookup(...Object.values(parameters));
            });

            this.addEventListeners();
        });
    },
    methods: {
        historyTenure: function (id) {
            this.occTenureHistoryId = parseInt(id);
            this.uuid++;
            this.$nextTick(() => {
                this.$refs.occ_tenure_history.isModalOpen = true;
            });
        },
        addEventListeners: function () {
            const vm = this;
            this.$refs.occurrence_tenure_datatable.vmDataTable.on(
                'click',
                'a[data-highlight-on-map-coordinates]',
                function (e) {
                    let coordinates = $(this).attr(
                        'data-highlight-on-map-coordinates'
                    );
                    coordinates = coordinates || null;
                    if (!coordinates) {
                        e.preventDefault();
                    }
                    vm.highlightOnMap(coordinates);
                }
            );
            this.$refs.occurrence_tenure_datatable.vmDataTable.on(
                'click',
                'a[data-edit-tenure-details]',
                function (e) {
                    e.preventDefault();
                    const id = $(this).attr('data-edit-tenure-details');
                    vm.editTenureDetails(id);
                }
            );
            vm.$refs.occurrence_tenure_datatable.vmDataTable.on(
                'click',
                'a[data-history-tenure]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-history-tenure');
                    vm.historyTenure(id);
                }
            );
            vm.$refs.occurrence_tenure_datatable.vmDataTable.on(
                'childRow.dt',
                function () {
                    helpers.enablePopovers();
                }
            );
        },
        highlightOnMap: function (coordinates = null) {
            this.$emit('highlight-on-map', JSON.parse(coordinates));
        },
        editTenureDetails: function (id) {
            let vm = this;
            this.$refs.occurrence_tenure_modal.object_id = id;
            this.$refs.occurrence_tenure_modal.modal_action = 'edit';

            const url = api_endpoints.occurrence_tenure + id;
            fetch(url)
                .then((response) => response.json())
                .then((data) => {
                    vm.$refs.occurrence_tenure_modal.tenureObj = data;
                    vm.$refs.occurrence_tenure_modal.isModalOpen = true;
                })
                .catch((error) => {
                    console.error('Error:', error);
                });
            this.$emit('edit-tenure-details', id);
        },
        /**
         * Initialises the select2 dropdown for this filter lookup
         * @param {String} ref The ref of the select html element
         * @param {String} vModelDataProperty The selected value will be stored in this property or v-model
         * @param {String=} placeholder A placeholder text for the select2 dropdown
         * @param {String=} apiEndpoint The api endpoint to fetch the data from, defaults to `ref`
         */
        initialiseFilterLookup: function (
            ref,
            vModelDataProperty,
            placeholder = 'Select a value',
            apiEndpoint = null
        ) {
            const vm = this;
            if (!this.$refs[ref]) {
                console.error(
                    `The ref ${ref} does not exist in the component.`
                );
                return;
            }
            if (vm[vModelDataProperty] === undefined) {
                console.error(
                    `The property ${vModelDataProperty} does not exist in the component.`
                );
                return;
            }
            if (!apiEndpoint) {
                apiEndpoint = ref;
            }

            const sessionStorageText =
                this.sessionStorageText(vModelDataProperty);

            $(this.$refs[ref])
                .select2({
                    minimumInputLength: 2,
                    theme: 'bootstrap-5',
                    allowClear: true,
                    placeholder: placeholder,
                    ajax: {
                        url: api_endpoints[apiEndpoint],
                        dataType: 'json',
                        data: function (params) {
                            var query = {
                                term: params.term,
                                type: 'public',
                                occurrence_id: vm.occurrenceId,
                            };
                            return query;
                        },
                    },
                })
                .on('select2:select', function (e) {
                    let data = e.params.data.id;
                    vm[vModelDataProperty] = data;
                    sessionStorage.setItem(
                        sessionStorageText,
                        e.params.data.text
                    );
                })
                .on('select2:unselect', function () {
                    vm[vModelDataProperty] = 'all';
                    sessionStorage.setItem(sessionStorageText, '');
                })
                .on('select2:open', function () {
                    const searchField = $(
                        `[aria-controls="select2-${ref}-results"]`
                    );
                    searchField[0].focus();
                });

            // Add the stored selected value to the select2 dropdown if it exists
            const sessionStorageItem = sessionStorage.getItem(
                this.sessionStorageText(vModelDataProperty)
            );
            if (!['all', null].includes(sessionStorageItem)) {
                const newOption = new Option(
                    sessionStorageItem,
                    this[vModelDataProperty],
                    false,
                    true
                );
                this.$refs[ref].append(newOption);
            }
        },
        sessionStorageText: function (key) {
            return `${key}Text`;
        },
        updatedTenureArea: function (data) {
            console.log('New occurrence tenure area data', data);
            this.$refs.occurrence_tenure_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            );
        },
        adjust_table_width: function () {
            if (this.$refs.occurrence_tenure_datatable !== undefined) {
                this.$refs.occurrence_tenure_datatable.vmDataTable.columns
                    .adjust()
                    .responsive.recalc();
            }
        },
    },
};
</script>
