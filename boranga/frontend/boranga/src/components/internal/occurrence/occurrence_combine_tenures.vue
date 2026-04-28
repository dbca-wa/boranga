<template lang="html">
    <div id="occurrenceCombineTenures">
        <datatable
            :id="panelBody"
            ref="tenures_datatable"
            :dt-options="tenures_options"
            :dt-headers="tenures_headers"
        />
    </div>
</template>

<script>
import { v4 as uuid } from 'uuid';
import datatable from '@vue-utils/datatable.vue';
import { constants, helpers } from '@/utils/hooks';

export default {
    name: 'OccurrenceCombineTenures',
    components: {
        datatable,
    },
    props: {
        selectedTenures: {
            type: Array,
            required: true,
        },
        combineTenureIds: {
            type: Array,
            required: true,
        },
        mainOccurrenceId: {
            type: Number,
            required: true,
        },
    },
    data: function () {
        let vm = this;
        return {
            panelBody: 'tenure-combine-select-' + uuid(),
            checkedTenureNames: [],
            tenures_headers: [
                'Occurrence',
                'CAD PIN',
                'Status',
                'Vesting',
                'Purpose',
                'Signif. to OCC',
                'Comments',
                'Owner/Manager',
                'Updated',
                'Action',
            ],
            tenures_options: {
                autowidth: true,
                language: {
                    processing: constants.DATATABLE_PROCESSING_HTML,
                },
                paging: true,
                responsive: true,
                columnDefs: [
                    { responsivePriority: 1, targets: 0 },
                    { responsivePriority: 2, targets: -1 },
                ],
                data: vm.selectedTenures,
                order: [],
                buttons: [],
                searching: true,
                dom:
                    "<'d-flex align-items-center'<'me-auto'l>fB>" +
                    "<'row'<'col-sm-12'tr>>" +
                    "<'d-flex align-items-center'<'me-auto'i>p>",
                columns: [
                    {
                        data: 'occurrence_number',
                        orderable: true,
                        searchable: true,
                    },
                    {
                        data: 'cad_pin',
                        orderable: true,
                        searchable: true,
                    },
                    {
                        data: 'status_display',
                    },
                    {
                        data: 'vesting',
                    },
                    {
                        data: 'purpose',
                    },
                    {
                        data: 'significant_to_occurrence',
                    },
                    {
                        data: 'comments',
                        mRender: function (data, type, full) {
                            let value = full.comments;
                            let result = helpers.dtPopover(value, 30, 'hover');
                            return type == 'export' ? value : result;
                        },
                    },
                    {
                        data: 'owner_name',
                        mRender: function (data, type, full) {
                            let value = full.related_occurrence_report_numbers;
                            let result = helpers.dtPopover(value, 30, 'hover');
                            return result;
                        },
                    },
                    {
                        data: 'datetime_updated',
                        orderable: true,
                        searchable: true,
                    },
                    {
                        data: 'id',
                        mRender: function (data, type, full) {
                            if (vm.combineTenureIds.includes(full.id)) {
                                if (full.occurrence_id == vm.mainOccurrenceId) {
                                    return `<input id='${full.id}' data-tenure-checkbox='${full.id}' tenure-feature-id='${full.cad_pin || full.featureid}' tenure-status='${full.status_display}' type='checkbox' checked disabled/>`;
                                } else {
                                    return `<input id='${full.id}' data-tenure-checkbox='${full.id}' tenure-feature-id='${full.cad_pin || full.featureid}' tenure-status='${full.status_display}' type='checkbox' checked/>`;
                                }
                            } else {
                                if (
                                    vm.checkedTenureNames.includes(
                                        full.cad_pin || full.featureid
                                    )
                                ) {
                                    return `<input id='${full.id}' data-tenure-checkbox='${full.id}' tenure-feature-id='${full.cad_pin || full.featureid}' tenure-status='${full.status_display}' type='checkbox' disabled/>`;
                                } else {
                                    return `<input id='${full.id}' data-tenure-checkbox='${full.id}' tenure-feature-id='${full.cad_pin || full.featureid}' tenure-status='${full.status_display}' type='checkbox'/>`;
                                }
                            }
                        },
                    },
                ],
            },
            drawCallback: function () {
                setTimeout(function () {
                    vm.adjust_table_width();
                }, 100);
            },
            initComplete: function () {
                // another option to fix the responsive table overflow css on tab switch
                setTimeout(function () {
                    vm.adjust_table_width();
                }, 100);
            },
        };
    },
    created: function () {
        let vm = this;
        vm.getSelectedTenureNames();
    },
    mounted: function () {
        let vm = this;
        this.$nextTick(() => {
            vm.addEventListeners();
        });
    },
    methods: {
        getSelectedTenureNames: function () {
            let vm = this;
            let names = [];
            vm.selectedTenures.forEach((tenure) => {
                if (
                    vm.combineTenureIds.includes(tenure.id) &&
                    !names.includes(tenure.cad_pin || tenure.featureid)
                ) {
                    names.push(tenure.cad_pin || tenure.featureid);
                }
            });
            vm.checkedTenureNames = names;
        },
        adjust_table_width: function () {
            if (this.$refs.tenures_datatable !== undefined) {
                this.$refs.tenures_datatable.vmDataTable.columns
                    .adjust()
                    .responsive.recalc();
            }
            helpers.enablePopovers();
        },
        removeTenure: function (id) {
            let vm = this;
            vm.combineTenureIds.splice(vm.combineTenureIds.indexOf(id), 1);
            vm.getSelectedTenureNames();
        },
        addTenure: function (id) {
            let vm = this;
            vm.combineTenureIds.push(id);
            vm.getSelectedTenureNames();
        },
        addEventListeners: function () {
            let vm = this;

            vm.$refs.tenures_datatable.vmDataTable.on(
                'change',
                'input[data-tenure-checkbox]',
                function (e) {
                    e.preventDefault();
                    var id = parseInt($(this).attr('data-tenure-checkbox'));
                    if ($(this).prop('checked')) {
                        vm.addTenure(id);
                        vm.selectedTenures.forEach((tenure) => {
                            let checkbox =
                                vm.$refs.tenures_datatable.vmDataTable.$(
                                    '#' + tenure.id
                                );
                            if (
                                id != checkbox.attr('data-tenure-checkbox') &&
                                checkbox.attr('tenure-feature-id') ==
                                    $(this).attr('tenure-feature-id') &&
                                checkbox.attr('tenure-status') == 'Current' &&
                                $(this).attr('tenure-status') == 'Current'
                            ) {
                                checkbox.prop('disabled', true);
                            }
                        });
                    } else {
                        vm.removeTenure(id);
                        vm.selectedTenures.forEach((tenure) => {
                            let checkbox =
                                vm.$refs.tenures_datatable.vmDataTable.$(
                                    '#' + tenure.id
                                );
                            if (
                                id != checkbox.attr('data-tenure-checkbox') &&
                                checkbox.attr('tenure-feature-id') ==
                                    $(this).attr('tenure-feature-id') &&
                                checkbox.attr('tenure-status') == 'Current' &&
                                $(this).attr('tenure-status') == 'Current'
                            ) {
                                checkbox.prop('disabled', false);
                            }
                        });
                    }
                }
            );
            vm.$refs.tenures_datatable.vmDataTable.on('draw', function () {
                helpers.enablePopovers();
            });
            vm.$refs.tenures_datatable.vmDataTable.on(
                'childRow.dt',
                function () {
                    helpers.enablePopovers();
                }
            );
        },
    },
};
</script>
