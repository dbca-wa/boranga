<template lang="html">
    <div :id="'related_species'">
        <div class="row mb-3">
            <label :for="scientific_name_lookup" class="col-sm-3 control-label"
                >NOMOS Lookup:</label
            >
            <div :id="select_scientific_name" class="col-sm-6">
                <select
                    :id="scientific_name_lookup"
                    :ref="scientific_name_lookup"
                    :disabled="isReadOnly"
                    :name="scientific_name_lookup"
                    class="form-control"
                />
            </div>

            <div class="col-sm-3">
                <button
                    :disabled="
                        isReadOnly || adding_species || !selected_taxonomy_id
                    "
                    type="button"
                    class="btn btn-primary mb-2"
                    @click.prevent="addRelatedSpecies"
                >
                    <i class="bi bi-plus-circle"></i>
                    Add Related Species
                </button>
            </div>
        </div>

        <div class="row mb-4 border-bottom pb-4">
            <label for="" class="col-sm-3 col-form-label">Common Name:</label>
            <div :id="select_common_name" class="col-sm-6">
                <template v-if="!selected_common_name">
                    <select
                        :id="select_common_name"
                        :ref="select_common_name"
                        :disabled="isReadOnly"
                        :name="select_common_name"
                        class="form-control"
                    />
                </template>
                <template v-else>
                    <template
                        v-for="commonName in selected_common_name"
                        :key="commonName"
                    >
                        <h5 class="d-inline">
                            <span class="badge bg-primary me-2">{{
                                commonName
                            }}</span>
                        </h5></template
                    >
                </template>
            </div>
        </div>

        <div class="row">
            <datatable
                :id="datatable_id"
                ref="related_species_datatable"
                :dt-options="datatable_options"
                :dt-headers="datatable_headers"
            />
        </div>
        <RelatedSpeciesModal
            ref="related-species-modal"
            :associated-species-taxonomy-id="
                selected_associated_species_taxonomy_id
            "
            :is-read-only="isReadOnly"
        ></RelatedSpeciesModal>
    </div>
</template>

<script>
import { v4 as uuid } from 'uuid';
import datatable from '@/utils/vue/datatable.vue';
import RelatedSpeciesModal from '@/components/common/occurrence/related_species_modal.vue';
import { constants, api_endpoints, helpers } from '@/utils/hooks';

export default {
    name: 'TableRelatedSpecies',
    components: {
        datatable,
        RelatedSpeciesModal,
    },
    props: {
        occurrence_report_obj: {
            type: Object,
            required: true,
        },
        isReadOnly: {
            type: Boolean,
            default: false,
        },
    },
    data() {
        let vm = this;
        return {
            scientific_name_lookup:
                'scientific_name_lookup' + vm.occurrence_report_obj.id,
            select_scientific_name:
                'select_scientific_name' + vm.occurrence_report_obj.id,
            selected_taxonomy_id: null,
            select_common_name: 'select_common_name' + uuid(),
            selected_common_name: null,
            selected_associated_species_taxonomy_id: null,
            adding_species: false,
            uuid: 0,
            datatable_id: uuid(),
        };
    },
    computed: {
        datatable_headers: function () {
            return [
                'Scientific Name',
                'Common Name(s)',
                'Conservation Status',
                'Species Role',
                'Is Current',
                'Kingdom',
                'Comments',
                'Action',
            ];
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
            };
        },
        column_common_name: function () {
            return {
                data: 'common_name',
                orderable: true,
                searchable: true,
                visible: true,
                render: function (value, type) {
                    let result = '';
                    // Split the value by comma and then render each common name as a bs5 badge
                    if (value && value.length > 0) {
                        value.forEach((name) => {
                            result += `<span class="badge bg-primary me-2">${name.trim()}</span>`;
                        });
                    } else {
                        result = '';
                    }
                    return type == 'export' ? value : result;
                },
            };
        },
        column_conservation_status: function () {
            return {
                data: 'conservation_status',
                orderable: true,
                searchable: true,
                visible: true,
            };
        },
        column_species_role: function () {
            return {
                data: 'species_role',
                orderable: true,
                searchable: true,
                visible: true,
            };
        },
        column_is_current: function () {
            return {
                data: 'is_current',
                orderable: true,
                searchable: false,
                visible: true,
                render: function (value) {
                    return value ? 'Yes' : 'No';
                },
            };
        },
        column_kingdom: function () {
            return {
                data: 'kingdom_name',
                orderable: true,
                searchable: true,
                visible: true,
                render: function (value, type) {
                    let result = helpers.dtPopover(value, 30, 'hover');
                    return type == 'export' ? value : result;
                },
            };
        },
        column_comments: function () {
            return {
                data: 'comments',
                orderable: true,
                searchable: true,
                visible: true,
            };
        },
        column_action: function () {
            let vm = this;
            return {
                data: 'id',
                orderable: false,
                searchable: false,
                visible: true,
                render: function (row, type, full) {
                    let links = `<a href='#' data-view-related-species='${full.id}' class='p-0 me-2'>View</a>`;
                    if (!vm.isReadOnly) {
                        links += `<a href='#' data-edit-related-species='${full.id}' class='p-0 me-2'>Edit</a>`;
                        links += `<a href='#' data-remove-related-species='${full.id}' class='p-0 me-2'>Remove</a>`;
                    }
                    return links;
                },
            };
        },
        datatable_options: function () {
            let vm = this;
            let columns = [
                vm.column_scientific_name,
                vm.column_common_name,
                vm.column_conservation_status,
                vm.column_species_role,
                vm.column_is_current,
                vm.column_kingdom,
                vm.column_comments,
                vm.column_action,
            ];
            return {
                autoWidth: false,
                language: {
                    processing: constants.DATATABLE_PROCESSING_HTML,
                },
                responsive: true,
                searching: true,
                ordering: true,
                order: [[0, 'desc']],
                columnDefs: [
                    { responsivePriority: 1, targets: 0 },
                    { responsivePriority: 999999, targets: -2 },
                    {
                        responsivePriority: 2,
                        targets: -1,
                    },
                ],
                ajax: {
                    url:
                        '/api/occurrence_report/' +
                        this.occurrence_report_obj.id +
                        '/get_related_species/',
                    dataSrc: '',
                },
                dom: 'lBfrtip',
                buttons: [],
                columns: columns,
                processing: true,
            };
        },
    },
    mounted: function () {
        let vm = this;
        this.$nextTick(() => {
            vm.initialiseScientificNameLookup();
            vm.initialiseCommonNameLookup();
            vm.addEventListeners();
        });
    },
    methods: {
        reload: function () {
            let vm = this;
            vm.$refs.related_species_datatable.vmDataTable.ajax.reload();
        },
        addRelatedSpecies: function () {
            let vm = this;
            if (vm.selected_taxonomy_id && !vm.adding_species) {
                vm.adding_species = true;
                fetch(
                    '/api/occurrence_report/' +
                        this.occurrence_report_obj.id +
                        '/add_related_species/',
                    {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json',
                        },
                        body: JSON.stringify({
                            taxonomy_id: vm.selected_taxonomy_id,
                        }),
                    }
                )
                    .then(
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
                                title: 'Added',
                                text: 'Related Species has been added',
                                icon: 'success',
                                customClass: {
                                    confirmButton: 'btn btn-primary',
                                },
                            });
                            vm.$refs.related_species_datatable.vmDataTable.ajax.reload();
                            vm.selected_taxonomy_id = null;
                            $(vm.$refs[vm.scientific_name_lookup])
                                .val(null)
                                .trigger('change');
                            vm.selected_common_name = null;
                            vm.$nextTick(() => {
                                vm.initialiseCommonNameLookup();
                            });
                            if (
                                vm.occurrence_report_obj.processing_status ==
                                'Unlocked'
                            ) {
                                vm.$router.go();
                            }
                        },
                        (error) => {
                            console.log(error);
                        }
                    )
                    .finally(() => {
                        vm.adding_species = false;
                    });
            }
        },
        viewRelatedSpecies: function (id) {
            this.selected_associated_species_taxonomy_id = Number(id);
            this.$refs['related-species-modal'].isModalOpen = true;
        },
        editRelatedSpecies: function (id) {
            this.selected_associated_species_taxonomy_id = Number(id);
            this.$refs['related-species-modal'].viewMode = false;
            this.$refs['related-species-modal'].isModalOpen = true;
        },
        removeRelatedSpecies: function (id) {
            let vm = this;
            swal.fire({
                title: 'Remove Related Species',
                text: 'Are you sure you want to remove this Related Species?',
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Remove Related Species',
                customClass: {
                    confirmButton: 'btn btn-primary',
                    cancelButton: 'btn btn-secondary',
                },
                reverseButtons: true,
            }).then((result) => {
                if (result.isConfirmed) {
                    fetch(
                        '/api/occurrence_report/' +
                            this.occurrence_report_obj.id +
                            '/remove_related_species/',
                        {
                            method: 'DELETE',
                            headers: {
                                'Content-Type': 'application/json',
                            },
                            body: JSON.stringify({
                                related_species_id: id,
                            }),
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
                                title: 'Removed',
                                text: 'Related Species has been removed',
                                icon: 'success',
                                customClass: {
                                    confirmButton: 'btn btn-primary',
                                },
                            });
                            vm.$refs.related_species_datatable.vmDataTable.ajax.reload();
                            if (
                                vm.occurrence_report_obj.processing_status ==
                                'Unlocked'
                            ) {
                                vm.$router.go();
                            }
                        },
                        (error) => {
                            console.log(error);
                        }
                    );
                }
            });
        },
        updatedRelatedSpecies: function () {
            let vm = this;
            vm.$refs.related_species_datatable.vmDataTable.ajax.reload();
            vm.selected_associated_species_taxonomy_id = null;
            if (vm.occurrence_report_obj.processing_status == 'Unlocked') {
                vm.$router.go();
            }
        },
        initialiseScientificNameLookup: function () {
            let vm = this;
            $(vm.$refs[vm.scientific_name_lookup])
                .select2({
                    minimumInputLength: 2,
                    dropdownParent: $('#' + vm.select_scientific_name),
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
                            };
                            return query;
                        },
                    },
                })
                .on('select2:select', function (e) {
                    let data = e.params.data.id;
                    vm.selected_taxonomy_id = data;
                    vm.selected_common_name = null;
                    vm.$nextTick(() => {
                        vm.initialiseCommonNameLookup();
                    });
                })
                .on('select2:unselect', function () {
                    vm.selected_taxonomy_id = null;
                    vm.selected_common_name = null;
                    vm.$nextTick(() => {
                        vm.initialiseCommonNameLookup();
                    });
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-' +
                            vm.scientific_name_lookup +
                            '-results"]'
                    );
                    // move focus to select2 field
                    searchField[0].focus();
                });
        },
        initialiseCommonNameLookup: function () {
            let vm = this;
            $(vm.$refs[vm.select_common_name])
                .select2({
                    minimumInputLength: 2,
                    dropdownParent: $('#' + vm.select_common_name),
                    theme: 'bootstrap-5',
                    allowClear: true,
                    placeholder: 'Select Common Name',
                    ajax: {
                        url: api_endpoints.common_name_lookup_ocr_select,
                        dataType: 'json',
                        data: function (params) {
                            var query = {
                                term: params.term,
                                type: 'public',
                                only_active: false,
                            };
                            return query;
                        },
                    },
                })
                .on('select2:select', function (e) {
                    vm.selected_taxonomy_id = e.params.data.id;
                    var newOption = new Option(
                        e.params.data.scientific_name,
                        e.params.data.id,
                        false,
                        true
                    );
                    $('#' + vm.scientific_name_lookup)
                        .append(newOption)
                        .trigger('change');
                    $(vm.$refs[vm.select_common_name]).select2('destroy');
                    vm.$nextTick(() => {
                        vm.selected_common_name =
                            e.params.data.common_names_list;
                    });
                })
                .on('select2:unselect', function () {
                    vm.selected_common_name = null;
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-' +
                            vm.select_common_name +
                            '-results"]'
                    );
                    // move focus to select2 field
                    searchField[0].focus();
                });
        },
        addEventListeners: function () {
            let vm = this;
            vm.$refs.related_species_datatable.vmDataTable.on(
                'click',
                'a[data-view-related-species]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-view-related-species');
                    vm.viewRelatedSpecies(id);
                }
            );
            vm.$refs.related_species_datatable.vmDataTable.on(
                'click',
                'a[data-edit-related-species]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-edit-related-species');
                    vm.editRelatedSpecies(id);
                }
            );
            vm.$refs.related_species_datatable.vmDataTable.on(
                'click',
                'a[data-remove-related-species]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-remove-related-species');
                    vm.removeRelatedSpecies(id);
                }
            );
        },
    },
};
</script>
<style></style>
