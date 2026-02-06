<template lang="html">
    <div :id="'related_ocr' + section_type">
        <FormSection
            :form-collapse="false"
            :label="label"
            :Index="'related_ocr' + section_type"
            @toggle-collapse="toggleCollapse"
        >
            <div>
                <datatable
                    :id="datatable_id"
                    ref="related_ocr_datatable"
                    :dt-options="datatable_options"
                    :dt-headers="datatable_headers"
                />
            </div>
        </FormSection>

        <div v-if="sectionOCRId">
            <SectionModal
                ref="section_modal"
                :key="sectionOCRId"
                :section-type-display="sectionTypeFormatted"
                :section-type="section_type"
                :ocr-number="sectionOCRId"
                :section-obj="sectionObj"
            />
        </div>
    </div>
</template>

<script>
import { v4 as uuid } from 'uuid';
import datatable from '@/utils/vue/datatable.vue';
import FormSection from '@/components/forms/section_toggle.vue';
import SectionModal from '@/components/common/occurrence/ocr_section_modal.vue';
import { constants, api_endpoints, helpers } from '@/utils/hooks';

export default {
    name: 'TableRelatedItems',
    components: {
        datatable,
        FormSection,
        SectionModal,
    },
    props: {
        occurrence_obj: {
            type: Object,
            required: true,
        },
        label: {
            type: String,
            required: false,
            default: 'Select Related Occurrence Report Form to Copy Data',
        },
        section_type: {
            type: String,
            required: false,
            default: '',
        },
        allowCopySectionData: {
            type: Boolean,
            default: true,
        },
        isReadOnly: {
            type: Boolean,
            default: false,
        },
        hrefContainerId: {
            type: String,
            required: false,
            default: '',
        },
        targetMapLayerNameForCopy: {
            type: String,
            required: false,
            default: 'query_layer',
        },
        targetMapLayerNameForShowHide: {
            type: String,
            required: false,
            default: 'query_layer',
        },
    },
    emits: [
        'copyUpdate',
        'highlight-on-map',
        'copy-to-map-layer',
        'toggle-show-on-map',
    ],
    data() {
        return {
            uuid: uuid(),
            datatable_id: 'datatable-related-ocr-' + uuid(),
            sectionOCRId: null,
            sectionTypeFormatted: null,
            sectionObj: null,
        };
    },
    computed: {
        column_number: function () {
            return {
                data: 'occurrence_report_number',
                orderable: true,
                searchable: true,
                visible: true,
            };
        },
        column_status: function () {
            return {
                data: 'processing_status_display',
                orderable: true,
                searchable: true,
                visible: true,
            };
        },
        column_observation_date: function () {
            return {
                data: 'observation_date',
                orderable: true,
                searchable: true,
                visible: true,
            };
        },
        column_location_accuracy: function () {
            return {
                data: 'location_accuracy',
                orderable: true,
                searchable: true,
                visible: true,
            };
        },
        column_identification_certainty: function () {
            return {
                data: 'identification_certainty',
                orderable: true,
                searchable: true,
                visible: true,
            };
        },
        column_site: function () {
            return {
                data: 'site',
                orderable: true,
                searchable: true,
                visible: true,
            };
        },
        column_submitter: function () {
            return {
                data: 'submitter',
                orderable: false,
                searchable: false,
                visible: true,
            };
        },
        column_action: function () {
            return {
                //name: 'action',
                data: 'id',
                orderable: false,
                searchable: false,
                visible: true,
                render: function (row, type, full) {
                    let links = '';
                    links += `<a href='/internal/occurrence-report/${full.id}' target="_blank">View</a><br>`;
                    return links;
                },
            };
        },
        copied_to_occurrence: function () {
            return {
                data: 'copied_to_occurrence',
                orderable: true,
                searchable: false,
                visible: true,
                render: function (data, type, row) {
                    return data.includes(row.occurrence) ? 'Yes' : 'No';
                },
            };
        },
        column_copy_action: function () {
            const vm = this;
            return {
                //name: 'action',
                data: 'id',
                orderable: false,
                searchable: false,
                visible: true,
                render: function (row, type, full) {
                    let links = '';
                    if (vm.section_type == 'location') {
                        if (full.geometry_show_on_map === false) {
                            links += `<a href="#javascript:void(0)" data-toggle-show-on-map='${full.id}'>Show on Map</a><br>`;
                        } else {
                            links += `<a href="#${vm.hrefContainerId}" data-highlight-on-map='${full.id}'>Highlight on Map</a><br>`;
                            links += `<a href="javascript:void(0)" data-toggle-show-on-map='${full.id}'>Hide on Map</a><br>`;
                            links += `<a href="#${vm.hrefContainerId}" data-copy-to-occ-layer='${full.id}'>Copy to OCC Layer</a><br>`;
                        }
                    }
                    links += `<a href='#' data-view-section='${full.id}'>View Section</a><br>`;
                    // Check the current value of isReadOnly and allowCopySectionData at render time
                    if (!vm.isReadOnly && vm.allowCopySectionData) {
                        links += `<a href='#' data-replace-section='${full.id}'>Copy Section Data</a><br>`;
                    }
                    return links;
                },
            };
        },
        datatable_options: function () {
            let vm = this;

            // Always use column_copy_action for sections - it will check isReadOnly internally
            let action = vm.column_action;
            if (vm.section_type !== '') {
                action = vm.column_copy_action;
            }

            let columns = [
                vm.column_number,
                vm.column_submitter,
                vm.column_observation_date,
                vm.column_location_accuracy,
                vm.column_identification_certainty,
                vm.column_site,
                vm.copied_to_occurrence,
                action,
            ];
            return {
                autoWidth: true,
                language: {
                    processing: constants.DATATABLE_PROCESSING_HTML,
                },
                responsive: true,
                searching: true,
                ordering: true,
                order: [[0, 'desc']],
                columnDefs: [
                    { responsivePriority: 1, targets: 0 },
                    { responsivePriority: 2, targets: -1 },
                ],
                ajax: {
                    url:
                        '/api/occurrence/' +
                        this.occurrence_obj.id +
                        '/get_related_occurrence_reports/',
                    dataSrc: '',
                    // adding extra GET params for Custom filtering
                    data: function (d) {
                        d.related_filter_type = vm.filterRelatedType;
                    },
                },
                dom:
                    "<'d-flex align-items-center'<'me-auto'l>fB>" +
                    "<'row'<'col-sm-12'tr>>" +
                    "<'d-flex align-items-center'<'me-auto'i>p>",
                buttons: [],
                columns: columns,
                processing: true,
                drawCallback: function () {
                    setTimeout(function () {
                        vm.adjust_table_width();
                    }, 100);
                },
                initComplete: function () {
                    setTimeout(function () {
                        vm.adjust_table_width();
                    }, 100);
                },
            };
        },
        datatable_headers: function () {
            return [
                //'id',
                'Number',
                'Submitter',
                //'Status',
                'Observation Date',
                'Location Accuracy',
                'Identification Certainty',
                'Site',
                'Copied to OCC',
                'Action',
            ];
        },
    },
    mounted: function () {
        let vm = this;
        this.$nextTick(() => {
            vm.addEventListeners();
        });
    },
    watch: {
        hrefContainerId: function () {
            // This is to ensure that the hrefContainerId is updated in the datatable
            this.$refs.related_ocr_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            );
        },
        'occurrence_obj.can_user_edit': function (newVal, oldVal) {
            // When can_user_edit changes (e.g., after unlock), reload the table
            // The render function will check the current isReadOnly value
            if (
                newVal !== oldVal &&
                this.$refs.related_ocr_datatable?.vmDataTable
            ) {
                // Reload the table data - this will re-execute the render functions
                // with the current isReadOnly value
                this.$refs.related_ocr_datatable.vmDataTable.ajax.reload(
                    null,
                    false
                );
            }
        },
    },
    methods: {
        toggleCollapse: function () {
            //console.log("toggle");
            this.adjust_table_width();
        },
        viewSection: function (id) {
            let vm = this;
            //get ocr object with id
            fetch(
                helpers.add_endpoint_json(api_endpoints.occurrence_report, id)
            ).then(
                async (response) => {
                    let ocrObj = await response.json();

                    vm.sectionObj = ocrObj[vm.section_type];
                    vm.sectionOCRId = id;
                    vm.sectionTypeFormatted = vm.section_type
                        .split('_')
                        .map((s) => s.charAt(0).toUpperCase() + s.substring(1))
                        .join(' ');

                    this.$nextTick(() => {
                        this.$refs.section_modal.isModalOpen = true;
                    });
                },
                (err) => {
                    console.log(err);
                }
            );
        },
        copySection: function (id, merge) {
            let vm = this;
            swal.fire({
                title: 'Copy Section',
                text: `Are you sure you want to ${
                    merge
                        ? 'merge this section data with the Occurrence?'
                        : 'replace the data in this section of the Occurrence?'
                } `,
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Copy Section',
                cancelButtonText: 'Cancel',
                reverseButtons: true,
                customClass: {
                    confirmButton: 'btn btn-primary',
                    cancelButton: 'btn btn-secondary',
                },
            }).then(async (swalresult) => {
                if (swalresult.isConfirmed) {
                    vm.errors = false;

                    let formData = new FormData();

                    let data = {
                        occurrence_report_id: id,
                        section: vm.section_type,
                        merge: merge,
                    };
                    formData.append('data', JSON.stringify(data));

                    fetch(
                        helpers.add_endpoint_json(
                            api_endpoints.occurrence,
                            vm.occurrence_obj.id + '/copy_ocr_section'
                        ),
                        {
                            method: 'POST',
                            body: formData,
                        }
                    ).then(
                        async (response) => {
                            const data = await response.json();
                            vm.$refs.related_ocr_datatable.vmDataTable.ajax.reload();
                            vm.$emit('copyUpdate', data, vm.section_type);
                        },
                        (error) => {
                            vm.errors = true;
                            vm.errorString = helpers.apiVueResourceError(error);
                        }
                    );
                }
            });
        },
        adjust_table_width: function () {
            if (this.$refs.related_ocr_datatable) {
                this.$refs.related_ocr_datatable.vmDataTable.columns
                    .adjust()
                    .responsive.recalc();
            }
        },
        reloadDatatable: function () {
            if (this.$refs.related_ocr_datatable) {
                this.$refs.related_ocr_datatable.vmDataTable.ajax.reload();
            }
        },
        addEventListeners: function () {
            let vm = this;
            if (!vm.$refs.related_ocr_datatable?.vmDataTable) {
                return;
            }
            vm.$refs.related_ocr_datatable.vmDataTable.on(
                'click',
                'a[data-view-section]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-view-section');
                    vm.viewSection(id);
                }
            );
            vm.$refs.related_ocr_datatable.vmDataTable.on(
                'click',
                'a[data-merge-section]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-merge-section');
                    vm.copySection(id, true);
                }
            );
            vm.$refs.related_ocr_datatable.vmDataTable.on(
                'click',
                'a[data-replace-section]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-replace-section');
                    vm.copySection(id, false);
                }
            );
            vm.$refs.related_ocr_datatable.vmDataTable.on(
                'click',
                'a[data-highlight-on-map]',
                function (e) {
                    let id = $(this).attr('data-highlight-on-map');
                    id = id || null;
                    if (!id) {
                        e.preventDefault();
                    }
                    vm.highlightOnMap(id);
                }
            );
            vm.$refs.related_ocr_datatable.vmDataTable.on(
                'click',
                'a[data-copy-to-occ-layer]',
                function (e) {
                    let id = $(this).attr('data-copy-to-occ-layer');
                    id = id || null;
                    if (!id) {
                        e.preventDefault();
                    }
                    vm.copyToMapLayer(id);
                }
            );
            vm.$refs.related_ocr_datatable.vmDataTable.on(
                'click',
                'a[data-toggle-show-on-map]',
                function (e) {
                    let id = $(this).attr('data-toggle-show-on-map');
                    id = id || null;
                    if (!id) {
                        e.preventDefault();
                    }
                    vm.toggleShowOnMap(id);
                }
            );
        },
        highlightOnMap: function (id = null) {
            this.$emit('highlight-on-map', id);
        },
        copyToMapLayer: function (id = null) {
            this.$emit('copy-to-map-layer', id, this.targetMapLayerNameForCopy);
        },
        toggleShowOnMap: function (id = null) {
            this.$emit(
                'toggle-show-on-map',
                id,
                this.targetMapLayerNameForShowHide
            );
        },
    },
};
</script>
