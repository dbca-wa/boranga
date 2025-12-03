<template lang="html">
    <div id="section_details">
        <modal
            transition="modal fade"
            :title="'OCC ' + occNumber + ' ' + sectionTypeDisplay"
            large
            :data-loss-warning-on-cancel="false"
            @ok="ok()"
            @cancel="close()"
        >
            <div>
                <FormSection :form-collapse="false" :label="sectionTypeDisplay">
                    <div class="card-body card-collapse">
                        <div v-if="isLoading" class="text-center py-4">
                            <div
                                class="spinner-border text-primary"
                                role="status"
                            >
                                <span class="visually-hidden">Loading...</span>
                            </div>
                        </div>
                        <div v-else>
                            <div v-if="hasData">
                                <div
                                    v-for="(value, index) in displayData"
                                    :key="index"
                                >
                                    <div v-if="value && index != 'id'">
                                        <div v-if="typeof value == 'object'">
                                            <div
                                                v-for="(
                                                    o_value, o_index
                                                ) in value"
                                                :key="o_index"
                                                class="row mb-3"
                                            >
                                                <label
                                                    class="col-sm-6 control-label"
                                                >
                                                    {{ formatLabel(index) }}.{{
                                                        formatLabel(o_index)
                                                    }}:
                                                </label>
                                                <div class="col-sm-6">
                                                    <input
                                                        :disabled="true"
                                                        type="text"
                                                        class="form-control"
                                                        :value="o_value"
                                                    />
                                                </div>
                                            </div>
                                        </div>
                                        <div v-else>
                                            <div class="row mb-3">
                                                <label
                                                    class="col-sm-6 control-label"
                                                >
                                                    {{ formatLabel(index) }}:
                                                </label>
                                                <div class="col-sm-6">
                                                    <textarea
                                                        :disabled="true"
                                                        type="text"
                                                        class="form-control"
                                                        v-bind:value="value"
                                                    ></textarea>
                                                </div>
                                            </div>
                                        </div>
                                    </div>
                                </div>
                            </div>
                            <div v-else class="text-center py-5">
                                <p class="text-secondary fs-5">
                                    No data from this ORF is available for this
                                    section
                                </p>
                            </div>
                        </div>
                    </div>
                </FormSection>
            </div>
            <template #footer>
                <div>
                    <button
                        type="button"
                        class="btn btn-secondary me-2"
                        @click="close"
                    >
                        Close
                    </button>
                </div>
            </template>
        </modal>
    </div>
</template>

<script>
import modal from '@vue-utils/bootstrap-modal.vue';
import FormSection from '@/components/forms/section_toggle.vue';
import { api_endpoints } from '@/utils/hooks';
export default {
    name: 'SectionModal',
    components: {
        modal,
        FormSection,
    },
    props: {
        sectionObj: {
            type: Object,
            required: true,
        },
        sectionType: {
            type: String,
            required: true,
        },
        sectionTypeDisplay: {
            type: String,
            required: true,
        },
        occNumber: {
            type: String,
            required: true,
        },
    },
    data() {
        return {
            isModalOpen: false,
            form: null,
            sectionObjExpanded: {},
            isLoading: false,
        };
    },
    computed: {
        hasData() {
            // Use sectionObjExpanded if it has been populated from API, otherwise use sectionObj
            const dataToCheck =
                Object.keys(this.sectionObjExpanded).length > 0
                    ? this.sectionObjExpanded
                    : this.sectionObj;
            const displayableKeys = Object.keys(dataToCheck).filter((key) => {
                const value = dataToCheck[key];
                // Exclude id, null, undefined, empty strings
                if (key === 'id' || value == null || value === '') {
                    return false;
                }
                // Exclude empty arrays
                if (Array.isArray(value) && value.length === 0) {
                    return false;
                }
                // Exclude empty objects
                if (
                    typeof value === 'object' &&
                    !Array.isArray(value) &&
                    Object.keys(value).length === 0
                ) {
                    return false;
                }
                return true;
            });
            return displayableKeys.length > 0;
        },
        displayData() {
            // Use sectionObjExpanded if it has been populated from API, otherwise use sectionObj
            return Object.keys(this.sectionObjExpanded).length > 0
                ? this.sectionObjExpanded
                : this.sectionObj;
        },
    },
    watch: {
        sectionObj() {
            let vm = this;
            vm.sectionObjExpanded = {};
            this.$nextTick(() => {
                vm.fetchSectionData();
            });
        },
    },
    mounted() {
        let vm = this;
        this.$nextTick(() => {
            vm.fetchSectionData();
        });
    },
    methods: {
        close() {
            this.isModalOpen = false;
            this.errors = false;
            $('.has-error').removeClass('has-error');
        },
        formatLabel(label) {
            // Replace underscores with spaces and convert to title case
            const str = String(label);
            return str
                .replace(/_/g, ' ')
                .split(' ')
                .map(
                    (word) =>
                        word.charAt(0).toUpperCase() +
                        word.slice(1).toLowerCase()
                )
                .join(' ');
        },
        fetchSectionData() {
            let vm = this;
            if (vm.sectionObj.occurrence_id !== undefined) {
                vm.isLoading = true;
                fetch(
                    api_endpoints.lookup_occ_section_values(
                        vm.sectionType,
                        vm.sectionObj.occurrence_id
                    )
                ).then(
                    async (response) => {
                        vm.sectionObjExpanded = await response.json();
                        vm.isLoading = false;
                    },
                    (error) => {
                        console.log(error);
                        vm.isLoading = false;
                    }
                );
            }
        },
    },
};
</script>
