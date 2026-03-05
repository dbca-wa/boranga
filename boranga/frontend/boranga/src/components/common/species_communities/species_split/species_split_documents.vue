<template lang="html">
    <div id="species_split_documents">
        <FormSection
            :formCollapse="false"
            label="Documents"
            :Index="documentBody"
        >
            <form class="form-horizontal" action="index.html" method="post">
                <div class="col-sm-12 form-check form-check-inline">
                    <input
                        class="form-check-input"
                        type="radio"
                        :id="'doc_select_all' + species_community.id"
                        name="documentSelect"
                        value="selectAll"
                        checked
                        @click="selectDocumentOption($event)"
                    />
                    <label class="form-check-label"
                        >Copy all documents to Species
                        <template v-if="species_community.taxonomy_details">{{
                            species_community.taxonomy_details.scientific_name
                        }}</template>
                    </label>
                </div>
                <div class="col-sm-12 form-check form-check-inline mb-3">
                    <input
                        class="form-check-input"
                        type="radio"
                        :id="'doc_select_individual' + species_community.id"
                        name="documentSelect"
                        value="individual"
                        @click="selectDocumentOption($event)"
                    />
                    <label class="form-check-label">Decide per document</label>
                </div>
                <div>
                    <datatable
                        ref="documents_datatable"
                        :id="panelBody"
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
    name: 'SpeciesSplitDocuments',
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
            documentBody: 'documentBody' + uuid(),
            panelBody: 'species-split-documents-' + uuid(),
            original_document_ids: [],
            species_document_url: api_endpoints.species_documents,
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
                        vm.species_original.id + '/documents'
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
                        title: 'Boranga Species Split Documents Excel Export',
                        text: '<i class="bi bi-download"></i> Excel',
                        className: 'btn btn-primary me-2 rounded',
                        exportOptions: {
                            orthogonal: 'export',
                        },
                    },
                    {
                        extend: 'csv',
                        title: 'Boranga Species Split Documents CSV Export',
                        text: '<i class="bi bi-download"></i> CSV',
                        className: 'btn btn-primary rounded',
                        exportOptions: {
                            orthogonal: 'export',
                        },
                    },
                ],
                columns: [
                    {
                        data: 'document_number',
                        orderable: true,
                        searchable: true,
                        mRender: function (data, type, full) {
                            if (full.active) {
                                return full.document_number;
                            } else {
                                return '<s>' + full.document_number + '</s>';
                            }
                        },
                    },
                    {
                        data: 'document_category_name',
                        orderable: true,
                        searchable: true,
                        mRender: function (data, type, full) {
                            if (full.active) {
                                return full.document_category_name;
                            } else {
                                return (
                                    '<s>' + full.document_category_name + '</s>'
                                );
                            }
                        },
                    },
                    {
                        data: 'document_sub_category_name',
                        orderable: true,
                        searchable: true,
                        mRender: function (data, type, full) {
                            if (full.active) {
                                return full.document_sub_category_name;
                            } else {
                                return (
                                    '<s>' +
                                    full.document_sub_category_name +
                                    '</s>'
                                );
                            }
                        },
                    },
                    {
                        data: 'name',
                        orderable: true,
                        searchable: true,
                        mRender: function (data, type, full) {
                            let links = '';
                            if (full.active) {
                                let value = full.name;
                                let result = helpers.dtPopoverSplit(
                                    value,
                                    30,
                                    'hover'
                                );
                                links +=
                                    '<span><a href="' +
                                    full._file +
                                    '" target="_blank">' +
                                    result.text +
                                    '</a> ' +
                                    result.link +
                                    '</span>';
                            } else {
                                let value = full.name;
                                let result = helpers.dtPopover(
                                    value,
                                    30,
                                    'hover'
                                );
                                links +=
                                    type == 'export'
                                        ? value
                                        : '<s>' + result + '</s>';
                            }
                            return links;
                        },
                    },
                    {
                        data: 'description',
                        orderable: true,
                        searchable: true,
                        render: function (value, type, full) {
                            let result = helpers.dtPopover(value, 30, 'hover');
                            if (full.active) {
                                return type == 'export' ? value : result;
                            } else {
                                return type == 'export'
                                    ? value
                                    : '<s>' + result + '</s>';
                            }
                        },
                    },
                    {
                        data: 'uploaded_date',
                        mRender: function (data, type, full) {
                            if (full.active) {
                                return data != '' && data != null
                                    ? moment(data).format('DD/MM/YYYY HH:mm')
                                    : '';
                            } else {
                                return data != '' && data != null
                                    ? '<s>' +
                                          moment(data).format(
                                              'DD/MM/YYYY HH:mm'
                                          ) +
                                          '</s>'
                                    : '';
                            }
                        },
                    },
                    {
                        data: 'id',
                        mRender: function (data, type, full) {
                            if (!vm.original_document_ids.includes(full.id)) {
                                vm.original_document_ids.push(full.id);
                            }

                            let isChecked =
                                vm.species_community.document_ids_to_copy.includes(
                                    full.id
                                );
                            let isDisabled =
                                vm.$parent.document_selection === 'selectAll';

                            return `<input class='form-check-input' type="checkbox" id="document_chkbox-${vm.species_community.id}-${full.id}" data-add-document="${full.id}"${isChecked ? ' checked' : ''}${isDisabled ? ' disabled' : ''}>`;
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
                        vm.species_community.document_ids_to_copy = json.map(
                            (doc) => doc.id
                        );
                        // Redraw to update checkboxes
                        vm.$refs.documents_datatable.vmDataTable
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
            if (vm.$parent.document_selection != null) {
                if (vm.$parent.document_selection === 'selectAll') {
                    document.getElementById(
                        'doc_select_all' + vm.species_community.id
                    ).checked = true;
                } else {
                    document.getElementById(
                        'doc_select_individual' + vm.species_community.id
                    ).checked = true;
                }
            }

            // Adjust table width after paging
            if (vm.$refs.documents_datatable) {
                let dt = vm.$refs.documents_datatable.vmDataTable;
                dt.on('page.dt draw.dt responsive-resize.dt', function () {
                    vm.adjust_table_width();
                });
            }
        });
    },
    methods: {
        selectDocumentOption(e) {
            let vm = this;
            let selected_option = e.target.value;
            if (vm.$parent.document_selection === selected_option) {
                return;
            }
            vm.$parent.document_selection = selected_option;

            // Always clear the array first
            vm.species_community.document_ids_to_copy.splice(
                0,
                vm.species_community.document_ids_to_copy.length
            );

            // For both options, fill with all IDs
            vm.original_document_ids.forEach((id) => {
                vm.species_community.document_ids_to_copy.push(Number(id));
            });

            if (selected_option == 'selectAll') {
                vm.species_community.copy_all_documents = true;
            } else {
                vm.species_community.copy_all_documents = false;
            }
            this.$refs.documents_datatable.vmDataTable.ajax.reload();
        },
        addEventListeners: function () {
            let vm = this;
            vm.$refs.documents_datatable.vmDataTable.on(
                'change',
                'input[data-add-document]',
                function () {
                    let id = parseInt($(this).attr('data-add-document'));
                    if (this.checked) {
                        if (
                            !vm.species_community.document_ids_to_copy.includes(
                                id
                            )
                        ) {
                            vm.species_community.document_ids_to_copy.push(id);
                            vm.species_community.document_ids_to_copy =
                                vm.species_community.document_ids_to_copy.slice();
                        }
                    } else {
                        let doc_arr = vm.species_community.document_ids_to_copy;
                        id = parseInt(id);
                        var index = doc_arr.indexOf(id);
                        if (index !== -1) {
                            vm.species_community.document_ids_to_copy.splice(
                                index,
                                1
                            );
                            vm.species_community.document_ids_to_copy =
                                vm.species_community.document_ids_to_copy.slice();
                        }
                    }
                }
            );
            vm.$refs.documents_datatable.vmDataTable.on(
                'childRow.dt',
                function () {
                    helpers.enablePopovers();
                }
            );
        },
        adjust_table_width: function () {
            if (this.$refs.documents_datatable !== undefined) {
                this.$refs.documents_datatable.vmDataTable.columns
                    .adjust()
                    .responsive.recalc();
            }
        },
    },
};
</script>
