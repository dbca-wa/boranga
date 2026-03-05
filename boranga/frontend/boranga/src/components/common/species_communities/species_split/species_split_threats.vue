<template lang="html">
    <div id="species_split_threats">
        <FormSection :form-collapse="false" label="Threats" :Index="threatBody">
            <form class="form-horizontal" action="index.html" method="post">
                <div class="col-sm-12 form-check form-check-inline">
                    <input
                        :id="'threat_select_all' + species_community.id"
                        class="form-check-input"
                        type="radio"
                        name="threatSelect"
                        value="selectAll"
                        checked
                        @click="selectThreatOption($event)"
                    />
                    <label class="form-check-label"
                        >Copy all threats to Species
                        <template v-if="species_community.taxonomy_details">{{
                            species_community.taxonomy_details.scientific_name
                        }}</template></label
                    >
                </div>
                <div class="col-sm-12 form-check form-check-inline mb-3">
                    <input
                        :id="'threat_select_individual' + species_community.id"
                        class="form-check-input"
                        type="radio"
                        name="threatSelect"
                        value="individual"
                        @click="selectThreatOption($event)"
                    />
                    <label class="form-check-label">Decide per threat</label>
                </div>
                <div>
                    <datatable
                        :id="panelBody"
                        ref="threats_datatable"
                        :dt-options="threats_options"
                        :dt-headers="threats_headers"
                    />
                </div>
            </form>
        </FormSection>
    </div>
</template>
<script>
import { v4 as uuid } from 'uuid';
import datatable from '@vue-utils/datatable.vue';
import FormSection from '@/components/forms/section_toggle.vue';
import { constants, api_endpoints, helpers } from '@/utils/hooks';

export default {
    name: 'SpeciesSplitThreats',
    components: {
        FormSection,
        datatable,
    },
    props: {
        species_community: {
            type: Object,
            required: true,
        },
        species_original: {
            type: Object,
            required: true,
        },
    },
    data: function () {
        let vm = this;
        return {
            uuid: 0,
            threatBody: 'threatBody' + uuid(),
            panelBody: 'species-split-threats-' + uuid(),
            values: null,
            // to store all the documents of original on first load.
            original_species_threats: [],
            threat_url: api_endpoints.threat,
            threats_headers: [
                'Number',
                'Category',
                'Threat Source',
                'Date Observed',
                'Threat Agent',
                'Comments',
                'Current Impact?',
                'Potential Impact?',
                'Action',
            ],
            threats_options: {
                autowidth: false,
                language: {
                    processing: constants.DATATABLE_PROCESSING_HTML,
                },
                responsive: true,
                searching: true,
                //  to show the "workflow Status","Action" columns always in the last position
                columnDefs: [
                    { responsivePriority: 1, targets: 0 },
                    { responsivePriority: 2, targets: -1 },
                ],
                ajax: {
                    url: helpers.add_endpoint_json(
                        api_endpoints.species,
                        vm.species_original.id + '/threats'
                    ),
                    dataSrc: '',
                },
                order: [],
                dom:
                    "<'d-flex align-items-center'<'me-auto'l>fB>" +
                    "<'row'<'col-sm-12'tr>>" +
                    "<'d-flex align-items-center'<'me-auto'i>p>",
                buttons: [
                    {
                        extend: 'excel',
                        title: 'Boranga Species Split Threats Excel Export',
                        text: '<i class="bi bi-download"></i> Excel',
                        className: 'btn btn-primary me-2 rounded',
                        exportOptions: {
                            orthogonal: 'export',
                        },
                    },
                    {
                        extend: 'csv',
                        title: 'Boranga Species Split Threats CSV Export',
                        text: '<i class="bi bi-download"></i> CSV',
                        className: 'btn btn-primary rounded',
                        exportOptions: {
                            orthogonal: 'export',
                        },
                    },
                ],
                columns: [
                    {
                        data: 'threat_number',
                        orderable: true,
                        searchable: true,
                        mRender: function (data, type, full) {
                            if (full.visible) {
                                return full.threat_number;
                            } else {
                                return '<s>' + full.threat_number + '</s>';
                            }
                        },
                    },
                    {
                        data: 'threat_category',
                        orderable: true,
                        searchable: true,
                        mRender: function (data, type, full) {
                            if (full.visible) {
                                return full.threat_category;
                            } else {
                                return '<s>' + full.threat_category + '</s>';
                            }
                        },
                    },
                    {
                        data: 'source',
                        orderable: true,
                        searchable: true,
                        mRender: function (data, type, full) {
                            if (full.visible) {
                                return full.source;
                            } else {
                                return '<s>' + full.source + '</s>';
                            }
                        },
                    },
                    {
                        data: 'date_observed',
                        mRender: function (data, type, full) {
                            if (full.visible) {
                                return data != '' && data != null
                                    ? moment(data).format('DD/MM/YYYY')
                                    : '';
                            } else {
                                return data != '' && data != null
                                    ? '<s>' + moment(data).format('DD/MM/YYYY')
                                    : '' + '</s>';
                            }
                        },
                    },
                    {
                        data: 'threat_agent',
                        orderable: true,
                        searchable: true,
                        mRender: function (data, type, full) {
                            if (full.visible) {
                                return full.threat_agent;
                            } else {
                                return '<s>' + full.threat_agent + '</s>';
                            }
                        },
                    },
                    {
                        data: 'comment',
                        orderable: true,
                        searchable: true,
                        render: function (value, type, full) {
                            let result = helpers.dtPopover(value, 30, 'hover');
                            if (full.visible) {
                                return type == 'export' ? value : result;
                            } else {
                                return type == 'export'
                                    ? '<s>' + value + '</s>'
                                    : '<s>' + result + '</s>';
                            }
                        },
                    },
                    {
                        data: 'current_impact_name',
                        orderable: true,
                        searchable: true,
                        mRender: function (data, type, full) {
                            if (full.visible) {
                                return full.current_impact_name;
                            } else {
                                return (
                                    '<s>' + full.current_impact_name + '</s>'
                                );
                            }
                        },
                    },
                    {
                        data: 'potential_impact_name',
                        orderable: true,
                        searchable: true,
                        mRender: function (data, type, full) {
                            if (full.visible) {
                                return full.potential_impact_name;
                            } else {
                                return (
                                    '<s>' + full.potential_impact_name + '</s>'
                                );
                            }
                        },
                    },
                    {
                        data: 'id',
                        mRender: function (data, type, full) {
                            // Store the original threat IDs for use with radio options
                            if (
                                !vm.original_species_threats.includes(full.id)
                            ) {
                                vm.original_species_threats.push(full.id);
                            }

                            let isChecked =
                                vm.species_community.threat_ids_to_copy.includes(
                                    full.id
                                );
                            let isDisabled =
                                vm.$parent.threat_selection === 'selectAll';

                            return `<input class='form-check-input' type="checkbox" id="threat_chkbox-${vm.species_community.id}-${full.id}" data-add-threat="${full.id}"${isChecked ? ' checked' : ''}${isDisabled ? ' disabled' : ''}>`;
                        },
                    },
                ],
                processing: true,
                drawCallback: function () {
                    helpers.enablePopovers();
                },
                initComplete: function (settings, json) {
                    helpers.enablePopovers();
                    // another option to fix the responsive table overflow css on tab switch
                    setTimeout(function () {
                        vm.adjust_table_width();
                    }, 100);
                    // Ensure all checkboxes are checked if copy_all_documents is true
                    if (vm.species_community.copy_all_documents && json) {
                        // json is the array of document objects
                        vm.species_community.threat_ids_to_copy = json.map(
                            (doc) => doc.id
                        );
                        // Redraw to update checkboxes
                        vm.$refs.threats_datatable.vmDataTable
                            .rows()
                            .invalidate()
                            .draw(false);
                    }
                },
            },
        };
    },
    mounted: function () {
        let vm = this;
        this.$nextTick(() => {
            vm.addEventListeners();

            if (vm.$parent.threat_selection != null) {
                if (vm.$parent.threat_selection === 'selectAll') {
                    document.getElementById(
                        'threat_select_all' + vm.species_community.id
                    ).checked = true;
                } else {
                    document.getElementById(
                        'threat_select_individual' + vm.species_community.id
                    ).checked = true;
                }
            }

            // Adjust table width after paging
            if (vm.$refs.threats_datatable) {
                let dt = vm.$refs.threats_datatable.vmDataTable;
                dt.on('page.dt draw.dt responsive-resize.dt', function () {
                    vm.adjust_table_width();
                });
            }
        });
    },
    methods: {
        selectThreatOption(e) {
            let vm = this;
            let selected_option = e.target.value;
            if (vm.$parent.threat_selection === selected_option) {
                return;
            }
            vm.$parent.threat_selection = selected_option;

            // Always clear the array first
            vm.species_community.threat_ids_to_copy.splice(
                0,
                vm.species_community.threat_ids_to_copy.length
            );

            // For both options, fill with all IDs (so all are checked by default)
            vm.original_species_threats.forEach((id) => {
                vm.species_community.threat_ids_to_copy.push(Number(id));
            });

            if (selected_option == 'selectAll') {
                vm.species_community.copy_all_threats = true;
            } else {
                vm.species_community.copy_all_threats = false;
            }
            this.$refs.threats_datatable.vmDataTable.ajax.reload();
        },
        addEventListeners: function () {
            let vm = this;
            vm.$refs.threats_datatable.vmDataTable.on(
                'change',
                'input[data-add-threat]',
                function () {
                    let id = parseInt($(this).attr('data-add-threat'));
                    if (this.checked) {
                        if (
                            !vm.species_community.threat_ids_to_copy.includes(
                                id
                            )
                        ) {
                            vm.species_community.threat_ids_to_copy.push(id);
                            vm.species_community.threat_ids_to_copy =
                                vm.species_community.threat_ids_to_copy.slice();
                        }
                    } else {
                        let threat_arr =
                            vm.species_community.threat_ids_to_copy;
                        var index = threat_arr.indexOf(id);
                        if (index !== -1) {
                            vm.species_community.threat_ids_to_copy.splice(
                                index,
                                1
                            );
                            vm.species_community.threat_ids_to_copy =
                                vm.species_community.threat_ids_to_copy.slice();
                        }
                    }
                }
            );
            vm.$refs.threats_datatable.vmDataTable.on(
                'childRow.dt',
                function () {
                    helpers.enablePopovers();
                }
            );
        },
        adjust_table_width: function () {
            if (this.$refs.threats_datatable !== undefined) {
                this.$refs.threats_datatable.vmDataTable.columns
                    .adjust()
                    .responsive.recalc();
            }
        },
    },
};
</script>
