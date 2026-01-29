<template lang="html">
    <div id="related_items">
        <FormSection
            :form-collapse="false"
            label="Related Items"
            Index="related_items"
        >
            <CollapsibleFilters
                ref="collapsible_filters"
                component_title="Filters"
                class="mb-2"
                :show-warning-icon="filterApplied"
            >
                <div class="row">
                    <div class="col-md-3">
                        <div class="form-group">
                            <label for="">Related Type:</label>
                            <select
                                v-model="filterRelatedType"
                                class="form-select"
                            >
                                <option value="all">All</option>
                                <option
                                    v-for="option in related_type_filter_list"
                                    :value="option[0]"
                                    :key="option[0]"
                                >
                                    {{ option[1] }}
                                </option>
                            </select>
                        </div>
                    </div>
                </div>
            </CollapsibleFilters>
            <div>
                <datatable
                    :id="datatable_id"
                    ref="related_items_datatable"
                    :dt-options="datatable_options"
                    :dt-headers="datatable_headers"
                />
            </div>
        </FormSection>
    </div>
</template>

<script>
import { v4 as uuid } from 'uuid';
import { constants, helpers } from '@/utils/hooks';
import datatable from '@/utils/vue/datatable.vue';
import FormSection from '@/components/forms/section_toggle.vue';
import CollapsibleFilters from '@/components/forms/collapsible_component.vue';
export default {
    name: 'TableRelatedItems',
    components: {
        datatable,
        FormSection,
        CollapsibleFilters,
    },
    props: {
        ajax_url: {
            type: String,
            required: true,
        },
        filter_list_url: {
            type: String,
            required: true,
        },
        display_profile_column: {
            type: Boolean,
            default: true,
        },
    },
    data() {
        return {
            datatable_id: 'related-items-' + uuid(),
            related_type_filter_list: [],
            filterRelatedType: 'all',
        };
    },
    computed: {
        column_lodgement_number: function () {
            return {
                data: 'identifier',
                orderable: false,
                searchable: true,
                visible: true,
            };
        },
        column_profile: function () {
            return {
                data: 'related_sc_id',
                orderable: true,
                searchable: true,
                visible: true,
                name: 'related_sc_id',
            };
        },
        column_type: function () {
            return {
                data: 'model_name',
                orderable: false,
                searchable: false,
                visible: true,
            };
        },
        column_description: function () {
            return {
                data: 'descriptor',
                orderable: false,
                searchable: true,
                visible: true,
                render: function (value, type) {
                    let result = helpers.dtPopover(value, 30, 'hover');
                    return type == 'export' ? value : result;
                },
            };
        },
        column_status: function () {
            return {
                data: 'status',
                orderable: false,
                searchable: false,
                visible: true,
            };
        },
        column_action: function () {
            return {
                data: 'action_url',
                orderable: false,
                searchable: false,
                visible: true,
            };
        },
        datatable_options: function () {
            let vm = this;
            let columns = [
                vm.column_lodgement_number,
                vm.column_type,
                vm.column_description,
                vm.column_status,
                vm.column_action,
            ];
            if (vm.display_profile_column) {
                columns.splice(1, 0, vm.column_profile);
            }
            return {
                autoWidth: false,
                language: {
                    processing: constants.DATATABLE_PROCESSING_HTML,
                },
                responsive: true,
                searching: true,
                ordering: true,
                order: [[0, 'desc']],
                serverSide: true,
                ajax: {
                    url: vm.ajax_url,
                    dataSrc: 'data',
                    data: function (d) {
                        d.related_filter_type = vm.filterRelatedType;
                    },
                },
                dom: 'lBfrtip',
                buttons: [],
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
        datatable_headers: function () {
            if (this.display_profile_column) {
                return [
                    'Number',
                    'Profile',
                    'Type',
                    'Description',
                    'Status',
                    'Action',
                ];
            }
            return ['Number', 'Type', 'Description', 'Status', 'Action'];
        },
        filterApplied: function () {
            if (this.filterRelatedType === 'all') {
                return false;
            } else {
                return true;
            }
        },
    },
    watch: {
        filterRelatedType: function () {
            let vm = this;
            vm.$refs.related_items_datatable.vmDataTable.ajax.reload();
        },
    },
    mounted: function () {
        let vm = this;
        vm.fetchFilterList();
        vm.$nextTick(() => {
            vm.$refs.related_items_datatable.vmDataTable.on(
                'childRow.dt',
                function () {
                    helpers.enablePopovers();
                }
            );
        });
    },
    methods: {
        fetchFilterList: function () {
            let vm = this;
            //large FilterList of Species Values object
            fetch(vm.filter_list_url).then(
                async (response) => {
                    vm.related_type_filter_list = await response.json();
                },
                (error) => {
                    console.log(error);
                }
            );
        },
        adjust_table_width: function () {
            if (this.$refs.related_items_datatable !== undefined) {
                this.$refs.related_items_datatable.vmDataTable.columns
                    .adjust()
                    .responsive.recalc();
            }
        },
    },
};
</script>
