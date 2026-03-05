<template lang="html">
    <div id="species-combine-threats">
        <FormSection
            :form-collapse="false"
            label="Threats"
            :Index="threatFormSectionIndex"
        >
            <form class="form-horizontal" method="post">
                <div class="col-sm-12 form-check form-check-inline">
                    <input
                        :id="'threat_select_all' + combine_species.id"
                        class="form-check-input"
                        type="radio"
                        :name="'threatSelect-' + combine_species.id"
                        value="selectAll"
                        :checked="currentThreatSelection === 'selectAll'"
                        @change="selectThreatOption"
                    />
                    <label class="form-check-label">
                        Copy all threats from Species
                        {{ combine_species.species_number }}
                    </label>
                </div>
                <div class="col-sm-12 form-check form-check-inline mb-3">
                    <input
                        :id="'threat_select_individual' + combine_species.id"
                        class="form-check-input"
                        type="radio"
                        :name="'threatSelect-' + combine_species.id"
                        value="individual"
                        :checked="currentThreatSelection === 'individual'"
                        @change="selectThreatOption"
                    />
                    <label class="form-check-label">Decide per threat</label>
                </div>
                <div>
                    <datatable
                        :id="threatsDatatableId"
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
    name: 'SpeciesCombineThreats',
    components: { FormSection, datatable },
    props: {
        resulting_species_community: { type: Object, required: true },
        combine_species: { type: Object, required: true },
    },
    data() {
        const vm = this;
        return {
            threatFormSectionIndex: 'threat-form-section-index-' + uuid(),
            threatsDatatableId: 'threats-datatable-id-' + uuid(),
            // All threat ids for this species (fetched once)
            combine_species_threat_ids: [],
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
                language: { processing: constants.DATATABLE_PROCESSING_HTML },
                responsive: true,
                searching: true,
                columnDefs: [
                    { responsivePriority: 1, targets: 0 },
                    { responsivePriority: 2, targets: -1 },
                ],
                ajax: {
                    url: helpers.add_endpoint_json(
                        api_endpoints.species,
                        vm.combine_species.id + '/threats'
                    ),
                    dataSrc: '',
                },
                order: [[0, 'desc']],
                dom:
                    "<'d-flex align-items-center'<'me-auto'l>fB>" +
                    "<'row'<'col-sm-12'tr>>" +
                    "<'d-flex align-items-center'<'me-auto'i>p>",
                buttons: [
                    {
                        extend: 'excel',
                        title: 'Boranga Species Combine Threats Excel Export',
                        text: '<i class="bi bi-download"></i> Excel',
                        className: 'btn btn-primary me-2 rounded',
                        exportOptions: { orthogonal: 'export' },
                    },
                    {
                        extend: 'csv',
                        title: 'Boranga Species Combine Threats CSV Export',
                        text: '<i class="bi bi-download"></i> CSV',
                        className: 'btn btn-primary rounded',
                        exportOptions: { orthogonal: 'export' },
                    },
                ],
                columns: [
                    {
                        data: 'threat_number',
                        orderable: true,
                        searchable: true,
                        mRender(data, type, full) {
                            return full.visible
                                ? full.threat_number
                                : '<s>' + full.threat_number + '</s>';
                        },
                    },
                    {
                        data: 'threat_category',
                        orderable: true,
                        searchable: true,
                        mRender(data, type, full) {
                            return full.visible
                                ? full.threat_category
                                : '<s>' + full.threat_category + '</s>';
                        },
                    },
                    {
                        data: 'source',
                        orderable: true,
                        searchable: true,
                        mRender(data, type, full) {
                            return full.visible
                                ? full.source
                                : '<s>' + full.source + '</s>';
                        },
                    },
                    {
                        data: 'date_observed',
                        mRender(data, type, full) {
                            if (!data) return '';
                            const formatted = moment(data).format('DD/MM/YYYY');
                            return full.visible
                                ? formatted
                                : '<s>' + formatted + '</s>';
                        },
                    },
                    {
                        data: 'threat_agent',
                        orderable: true,
                        searchable: true,
                        mRender(data, type, full) {
                            return full.visible
                                ? full.threat_agent
                                : '<s>' + full.threat_agent + '</s>';
                        },
                    },
                    {
                        data: 'comment',
                        orderable: true,
                        searchable: true,
                        render(value, type, full) {
                            const result = helpers.dtPopover(
                                value,
                                30,
                                'hover'
                            );
                            if (full.visible) {
                                return type === 'export' ? value : result;
                            }
                            return type === 'export'
                                ? value
                                : '<s>' + result + '</s>';
                        },
                    },
                    {
                        data: 'current_impact_name',
                        orderable: true,
                        searchable: true,
                        mRender(data, type, full) {
                            return full.visible
                                ? full.current_impact_name
                                : '<s>' + full.current_impact_name + '</s>';
                        },
                    },
                    {
                        data: 'potential_impact_name',
                        orderable: true,
                        searchable: true,
                        mRender(data, type, full) {
                            return full.visible
                                ? full.potential_impact_name
                                : '<s>' + full.potential_impact_name + '</s>';
                        },
                    },
                    {
                        data: 'id',
                        mRender(data, type, full) {
                            const entry = vm.getThreatSelectionEntry();
                            const isSelectAll = entry.mode === 'all';
                            const isChecked =
                                isSelectAll ||
                                (entry.mode === 'individual' &&
                                    entry.ids.includes(full.id));
                            const isDisabled = isSelectAll;
                            return `<input class='form-check-input' type="checkbox" data-add-threat="${full.id}"${
                                isChecked ? ' checked' : ''
                            }${isDisabled ? ' disabled' : ''}>`;
                        },
                    },
                ],
                processing: true,
                drawCallback() {
                    helpers.enablePopovers();
                },
                initComplete: (settings, json) => {
                    helpers.enablePopovers();
                    setTimeout(() => this.adjust_table_width(), 100);
                    if (json && this.combine_species_threat_ids.length === 0) {
                        this.combine_species_threat_ids = json.map((d) => d.id);
                    }
                },
            },
        };
    },
    mounted() {
        const entry = this.getThreatSelectionEntry();
        if (!entry.mode) entry.mode = 'all';
        this.addEventListeners();
    },
    beforeUnmount() {
        const dt =
            this.$refs.threats_datatable &&
            this.$refs.threats_datatable.vmDataTable;
        if (dt) {
            dt.off('change', 'input[data-add-threat]');
            dt.off('childRow.dt');
        }
    },
    computed: {
        selectionEntry() {
            return this.getThreatSelectionEntry();
        },
        isThreatSelectAll() {
            return this.selectionEntry.mode === 'all';
        },
        currentThreatSelection() {
            return this.isThreatSelectAll ? 'selectAll' : 'individual';
        },
    },
    methods: {
        getThreatSelectionEntry() {
            const map =
                (this.resulting_species_community.selection &&
                    this.resulting_species_community.selection.threats) ||
                (this.resulting_species_community.selection = {
                    documents: {},
                    threats: {},
                }).threats;
            if (!map[this.combine_species.id]) {
                map[this.combine_species.id] = { mode: 'all', ids: [] };
            }
            return map[this.combine_species.id];
        },
        selectThreatOption(e) {
            const selected = e.target.value;
            const entry = this.getThreatSelectionEntry();
            if (selected === 'selectAll' && entry.mode !== 'all') {
                entry.mode = 'all';
                entry.ids = [];
            } else if (
                selected === 'individual' &&
                entry.mode !== 'individual'
            ) {
                entry.mode = 'individual';
                if (
                    entry.ids.length === 0 &&
                    this.combine_species_threat_ids.length
                ) {
                    entry.ids = this.combine_species_threat_ids.slice();
                }
            }
            this.$nextTick(() =>
                this.$refs.threats_datatable.vmDataTable.ajax.reload(
                    null,
                    false
                )
            );
        },
        addEventListeners() {
            const dt =
                this.$refs.threats_datatable &&
                this.$refs.threats_datatable.vmDataTable;
            if (!dt) return;
            dt.on('change', 'input[data-add-threat]', (evt) => {
                if (this.isThreatSelectAll) return;
                const id = parseInt(
                    evt.currentTarget.getAttribute('data-add-threat')
                );
                if (Number.isNaN(id)) return;
                const entry = this.getThreatSelectionEntry();
                const list = entry.ids;
                const idx = list.indexOf(id);
                if (evt.currentTarget.checked) {
                    if (idx === -1) list.push(id);
                } else if (idx > -1) list.splice(idx, 1);
                entry.ids = list.slice();
            });
            dt.on('childRow.dt', () => helpers.enablePopovers());
        },
        adjust_table_width() {
            this.$nextTick(() => {
                if (this.$refs.threats_datatable) {
                    this.$refs.threats_datatable.vmDataTable.columns
                        .adjust()
                        .responsive.recalc();
                }
            });
        },
    },
};
</script>
