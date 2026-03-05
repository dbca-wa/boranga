<template lang="html">
    <div id="species_combine_documents">
        <FormSection
            :form-collapse="false"
            label="Documents"
            :Index="documentBody"
        >
            <form class="form-horizontal" method="post">
                <div class="col-sm-12 form-check form-check-inline">
                    <input
                        :id="'doc_select_all' + combine_species.id"
                        class="form-check-input"
                        type="radio"
                        :name="'documentSelect-' + combine_species.id"
                        value="selectAll"
                        :checked="currentSelection === 'selectAll'"
                        @change="selectDocumentOption"
                    />
                    <label class="form-check-label">
                        Copy all documents from Species
                        {{ combine_species.species_number }}
                    </label>
                </div>
                <div class="col-sm-12 form-check form-check-inline mb-3">
                    <input
                        :id="'doc_select_individual' + combine_species.id"
                        class="form-check-input"
                        type="radio"
                        :name="'documentSelect-' + combine_species.id"
                        value="individual"
                        :checked="currentSelection === 'individual'"
                        @change="selectDocumentOption"
                    />
                    <label class="form-check-label">Decide per document</label>
                </div>
                <div>
                    <datatable
                        :id="panelBody"
                        ref="documents_datatable"
                        :dt-options="documents_options"
                        :dt-headers="documents_headers"
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
    name: 'SpeciesCombineDocuments',
    components: { FormSection, datatable },
    props: {
        resulting_species_community: { type: Object, required: true },
        combine_species: { type: Object, required: true },
    },
    data() {
        const vm = this;
        return {
            documentBody: 'documentBody' + uuid(),
            panelBody: 'species-combine-documents-' + uuid(),
            // All doc ids for this species (fetched once)
            combine_species_document_ids: [],
            documents_headers: [
                'Number',
                'Category',
                'Sub Category',
                'Document',
                'Description',
                'Date/Time',
                'Action',
            ],
            documents_options: {
                autowidth: true,
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
                        vm.combine_species.id + '/documents'
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
                        title: 'Boranga Species Combine Documents Excel Export',
                        text: '<i class="bi bi-download"></i> Excel',
                        className: 'btn btn-primary me-2 rounded',
                        exportOptions: { orthogonal: 'export' },
                    },
                    {
                        extend: 'csv',
                        title: 'Boranga Species Combine Documents CSV Export',
                        text: '<i class="bi bi-download"></i> CSV',
                        className: 'btn btn-primary rounded',
                        exportOptions: { orthogonal: 'export' },
                    },
                ],
                columns: [
                    {
                        data: 'document_number',
                        orderable: true,
                        searchable: true,
                        mRender(data, type, full) {
                            return full.active
                                ? full.document_number
                                : '<s>' + full.document_number + '</s>';
                        },
                    },
                    {
                        data: 'document_category_name',
                        orderable: true,
                        searchable: true,
                        mRender(data, type, full) {
                            return full.active
                                ? full.document_category_name
                                : '<s>' + full.document_category_name + '</s>';
                        },
                    },
                    {
                        data: 'document_sub_category_name',
                        orderable: true,
                        searchable: true,
                        mRender(data, type, full) {
                            return full.active
                                ? full.document_sub_category_name
                                : '<s>' +
                                      full.document_sub_category_name +
                                      '</s>';
                        },
                    },
                    {
                        data: 'name',
                        orderable: true,
                        searchable: true,
                        mRender(data, type, full) {
                            const value = full.name;
                            if (full.active) {
                                const result = helpers.dtPopoverSplit(
                                    value,
                                    30,
                                    'hover'
                                );
                                return (
                                    '<span><a href="' +
                                    full._file +
                                    '" target="_blank">' +
                                    result.text +
                                    '</a> ' +
                                    result.link +
                                    '</span>'
                                );
                            }
                            const result = helpers.dtPopover(
                                value,
                                30,
                                'hover'
                            );
                            return type === 'export'
                                ? value
                                : '<s>' + result + '</s>';
                        },
                    },
                    {
                        data: 'description',
                        orderable: true,
                        searchable: true,
                        render(value, type, full) {
                            const result = helpers.dtPopover(
                                value,
                                30,
                                'hover'
                            );
                            if (full.active) {
                                return type === 'export' ? value : result;
                            }
                            return type === 'export'
                                ? value
                                : '<s>' + result + '</s>';
                        },
                    },
                    {
                        data: 'uploaded_date',
                        mRender(data, type, full) {
                            if (!data) return '';
                            const formatted =
                                moment(data).format('DD/MM/YYYY HH:mm');
                            return full.active
                                ? formatted
                                : '<s>' + formatted + '</s>';
                        },
                    },
                    {
                        data: 'id',
                        mRender(data, type, full) {
                            const entry = vm.getDocSelectionEntry();
                            const isSelectAll = entry.mode === 'all';
                            const isChecked =
                                isSelectAll ||
                                (entry.mode === 'individual' &&
                                    entry.ids.includes(full.id));
                            const isDisabled = isSelectAll;
                            return `<input class='form-check-input' type="checkbox" data-add-document="${full.id}"${
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
                    if (
                        json &&
                        this.combine_species_document_ids.length === 0
                    ) {
                        this.combine_species_document_ids = json.map(
                            (d) => d.id
                        );
                    }
                },
            },
        };
    },
    mounted() {
        const entry = this.getDocSelectionEntry();
        if (!entry.mode) entry.mode = 'all';
        this.addEventListeners();
    },
    beforeUnmount() {
        const dt =
            this.$refs.documents_datatable &&
            this.$refs.documents_datatable.vmDataTable;
        if (dt) {
            dt.off('change', 'input[data-add-document]');
            dt.off('childRow.dt');
        }
    },
    computed: {
        selectionEntry() {
            return this.getDocSelectionEntry();
        },
        isSelectAll() {
            return this.selectionEntry.mode === 'all';
        },
        currentSelection() {
            return this.isSelectAll ? 'selectAll' : 'individual';
        },
    },
    methods: {
        getDocSelectionEntry() {
            const map =
                (this.resulting_species_community.selection &&
                    this.resulting_species_community.selection.documents) ||
                (this.resulting_species_community.selection = {
                    documents: {},
                    threats: {},
                }).documents;
            if (!map[this.combine_species.id]) {
                map[this.combine_species.id] = { mode: 'all', ids: [] };
            }
            return map[this.combine_species.id];
        },
        selectDocumentOption(e) {
            const selected = e.target.value; // 'selectAll' or 'individual'
            const entry = this.getDocSelectionEntry();
            if (selected === 'selectAll' && entry.mode !== 'all') {
                entry.mode = 'all';
                entry.ids = []; // not needed when all
            } else if (
                selected === 'individual' &&
                entry.mode !== 'individual'
            ) {
                // seed with all current ids so user can uncheck (or leave empty if you prefer)
                entry.mode = 'individual';
                if (
                    entry.ids.length === 0 &&
                    this.combine_species_document_ids.length
                ) {
                    entry.ids = this.combine_species_document_ids.slice();
                }
            }
            this.$nextTick(() =>
                this.$refs.documents_datatable.vmDataTable.ajax.reload(
                    null,
                    false
                )
            );
        },
        addEventListeners() {
            const dt =
                this.$refs.documents_datatable &&
                this.$refs.documents_datatable.vmDataTable;
            if (!dt) return;
            dt.on('change', 'input[data-add-document]', (evt) => {
                if (this.isSelectAll) return;
                const id = parseInt(
                    evt.currentTarget.getAttribute('data-add-document')
                );
                if (Number.isNaN(id)) return;
                const entry = this.getDocSelectionEntry();
                const list = entry.ids;
                const idx = list.indexOf(id);
                if (evt.currentTarget.checked) {
                    if (idx === -1) list.push(id);
                } else if (idx > -1) {
                    list.splice(idx, 1);
                }
                entry.ids = list.slice();
            });
            dt.on('childRow.dt', () => helpers.enablePopovers());
        },
        adjust_table_width() {
            this.$nextTick(() => {
                if (this.$refs.documents_datatable) {
                    this.$refs.documents_datatable.vmDataTable.columns
                        .adjust()
                        .responsive.recalc();
                }
            });
        },
    },
};
</script>
