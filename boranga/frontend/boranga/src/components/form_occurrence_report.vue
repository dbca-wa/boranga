<template lang="html">
    <div>
        <div :id="occurrenceReportBody" class="">
            <OCRProfile
                id="ocrProfile"
                ref="ocr_profile"
                :is_external="is_external"
                :referral="referral"
                :show_observer_contact_information="
                    show_observer_contact_information
                "
                :show_comments="show_comments"
                :occurrence_report_obj="occurrence_report_obj"
                @refresh-occurrence-report="refreshOccurrenceReport()"
                @save-occurrence-report="saveOccurrenceReport()"
            >
            </OCRProfile>
            <SubmitterInformation
                v-if="occurrence_report_obj.submitter_information"
                id="submitter_information"
                :key="reloadcount"
                ref="submitter_information"
                :show_submitter_contact_details="show_submitter_contact_details"
                :submitter_information="
                    occurrence_report_obj.submitter_information
                "
                :show_organisation_section="false"
                :disabled="
                    !(
                        occurrence_report_obj.can_user_edit &&
                        occurrence_report_obj.is_submitter
                    ) || referral
                "
            />
        </div>
        <div class="col-md-12">
            <ul id="pills-tab" class="nav nav-pills" role="tablist">
                <li class="nav-item">
                    <a
                        id="pills-location-tab"
                        class="nav-link active"
                        data-bs-toggle="pill"
                        data-target-id="ocr_location"
                        :href="'#' + locationBody"
                        role="tab"
                        :aria-controls="locationBody"
                        aria-selected="true"
                    >
                        Location<template v-if="tabDirtyMap['ocr_location']"
                            ><i
                                class="bi bi-exclamation-circle-fill text-warning ms-2"
                                title="This tab has unsaved changes"
                            ></i
                        ></template>
                    </a>
                </li>
                <li class="nav-item">
                    <a
                        id="pills-observation-tab"
                        class="nav-link"
                        data-bs-toggle="pill"
                        data-target-id="ocr_observation"
                        :href="'#' + observationBody"
                        role="tab"
                        aria-selected="false"
                    >
                        Observation<template
                            v-if="tabDirtyMap['ocr_observation']"
                            ><i
                                class="bi bi-exclamation-circle-fill text-warning ms-2"
                                title="This tab has unsaved changes"
                            ></i
                        ></template>
                    </a>
                </li>
                <li class="nav-item">
                    <a
                        id="pills-habitat-tab"
                        class="nav-link"
                        data-bs-toggle="pill"
                        data-target-id="ocr_habitat"
                        :href="'#' + habitatBody"
                        role="tab"
                        aria-selected="false"
                    >
                        Habitat<template v-if="tabDirtyMap['ocr_habitat']"
                            ><i
                                class="bi bi-exclamation-circle-fill text-warning ms-2"
                                title="This tab has unsaved changes"
                            ></i
                        ></template>
                    </a>
                </li>
                <li class="nav-item">
                    <a
                        id="pills-documents-tab"
                        class="nav-link"
                        data-bs-toggle="pill"
                        data-target-id="ocr_documents"
                        :href="'#' + documentBody"
                        role="tab"
                        aria-selected="false"
                    >
                        Documents
                    </a>
                </li>
                <li class="nav-item">
                    <a
                        id="pills-threats-tab"
                        class="nav-link"
                        data-bs-toggle="pill"
                        data-target-id="ocr_threats"
                        :href="'#' + threatBody"
                        role="tab"
                        aria-selected="false"
                    >
                        Threats
                    </a>
                </li>
            </ul>
            <div id="pills-tabContent" class="tab-content">
                <div
                    :id="locationBody"
                    class="tab-pane fade show active"
                    role="tabpanel"
                    aria-labelledby="pills-location-tab"
                >
                    <OCRLocation
                        id="ocr_location"
                        :key="reloadcount"
                        ref="ocr_location"
                        :is_external="is_external"
                        :is_internal="is_internal"
                        :can-edit-status="canEditStatus"
                        :occurrence_report_obj="occurrence_report_obj"
                        :referral="referral"
                        @refresh-from-response="refreshFromResponse"
                        @dirty="onTabDirtyChange('ocr_location', $event)"
                    >
                    </OCRLocation>
                </div>
                <div
                    :id="observationBody"
                    class="tab-pane fade"
                    role="tabpanel"
                    aria-labelledby="pills-observation-tab"
                >
                    <OCRObservation
                        id="ocr_observation"
                        :key="reloadcount"
                        ref="ocr_observation"
                        :is_internal="is_internal"
                        :is_external="is_external"
                        :occurrence_report_obj="occurrence_report_obj"
                        @dirty="onTabDirtyChange('ocr_observation', $event)"
                    >
                    </OCRObservation>
                </div>
                <div
                    :id="habitatBody"
                    class="tab-pane fade"
                    role="tabpanel"
                    aria-labelledby="pills-habitat-tab"
                >
                    <OCRHabitat
                        id="ocr_habitat"
                        :key="reloadcount"
                        ref="ocr_habitat"
                        :is_internal="is_internal"
                        :is_external="is_external"
                        :occurrence_report_obj="occurrence_report_obj"
                        @dirty="onTabDirtyChange('ocr_habitat', $event)"
                    >
                    </OCRHabitat>
                </div>
                <div
                    :id="documentBody"
                    class="tab-pane fade"
                    role="tabpanel"
                    aria-labelledby="pills-documents-tab"
                >
                    <OCRDocuments
                        id="ocr_documents"
                        :key="reloadcount"
                        ref="ocr_documents"
                        :is_internal="is_internal"
                        :is_external="is_external"
                        :occurrence_report_obj="occurrence_report_obj"
                    >
                    </OCRDocuments>
                </div>
                <div
                    :id="threatBody"
                    class="tab-pane fade"
                    role="tabpanel"
                    aria-labelledby="pills-threats-tab"
                >
                    <OCRThreats
                        id="ocr_threats"
                        :key="reloadcount"
                        ref="ocr_threats"
                        :is_internal="is_internal"
                        :is_external="is_external"
                        :occurrence_report_obj="occurrence_report_obj"
                    >
                    </OCRThreats>
                </div>
            </div>
        </div>
    </div>
</template>

<script>
import { v4 as uuid } from 'uuid';
import OCRLocation from '@/components/common/occurrence/ocr_location.vue';
import OCRHabitat from '@/components/common/occurrence/ocr_habitat.vue';
import OCRObservation from '@/components/common/occurrence/ocr_observation.vue';
import SubmitterInformation from '@/components/common/submitter_information.vue';
import OCRDocuments from '@/components/common/occurrence/ocr_documents.vue';
import OCRThreats from '@/components/common/occurrence/ocr_threats.vue';
import OCRProfile from '@/components/common/occurrence/occurrence_report_profile.vue';

export default {
    components: {
        OCRLocation,
        OCRHabitat,
        OCRObservation,
        OCRDocuments,
        OCRThreats,
        OCRProfile,
        SubmitterInformation,
    },
    props: {
        occurrence_report_obj: {
            type: Object,
            required: true,
        },
        referral: {
            type: Object,
            required: false,
        },
        is_external: {
            type: Boolean,
            default: false,
        },
        is_internal: {
            type: Boolean,
            default: false,
        },
        canEditStatus: {
            type: Boolean,
            default: true,
        },
        show_observer_contact_information: {
            type: Boolean,
            default: true,
        },
        show_comments: {
            type: Boolean,
            default: true,
        },
    },
    emits: [
        'refreshFromResponse',
        'refreshOccurrenceReport',
        'saveOccurrenceReport',
        'dirty',
    ],
    data: function () {
        return {
            values: null,
            reloadcount: 0,
            threatsKey: 0,
            occurrenceReportBody: 'occurrenceReportBody' + uuid(),
            locationBody: 'locationBody' + uuid(),
            habitatBody: 'habitatBody' + uuid(),
            observationBody: 'observationBody' + uuid(),
            threatBody: 'threatBody' + uuid(),
            documentBody: 'documentBody' + uuid(),
            relatedItemBody: 'relatedItemBody' + uuid(),
            tabDirtyMap: {
                ocr_location: false,
                ocr_habitat: false,
                ocr_observation: false,
            },
            allowTabChange: false,
        };
    },
    watch: {
        tabDirtyMap: {
            handler: function () {
                this.$emit('dirty', this.isAnyTabDirty());
            },
            deep: true,
        },
        // When the parent replaces occurrence_report_obj (e.g. after
        // refreshOccurrenceReport), the nested objects are fresh from the
        // API.  Reset every section's dirty snapshot so stale baselines
        // don't cause false "unsaved changes" warnings.
        occurrence_report_obj: function () {
            this.$nextTick(() => {
                this.resetDirtyState();
            });
        },
    },
    computed: {
        show_submitter_contact_details: function () {
            return 'OccurrenceReportReferral' != this.$parent.$options.name;
        },
        related_items_ajax_url: function () {
            return (
                '/api/occurrence_report/' +
                this.occurrence_report_obj.id +
                '/get_related_items/'
            );
        },
        related_items_filter_list_url: function () {
            return '/api/occurrence_report/filter_list.json';
        },
    },
    mounted: function () {
        document.querySelectorAll('a[data-bs-toggle="pill"]').forEach((el) => {
            el.addEventListener('shown.bs.tab', () => {
                if (el.id == 'pills-threats-tab') {
                    this.$refs.ocr_threats.adjust_table_width();
                } else if (el.id == 'pills-documents-tab') {
                    this.$refs.ocr_documents.adjust_table_width();
                } else if (el.id == 'pills-habitat-tab') {
                    this.$refs.ocr_habitat.$refs.related_species.$refs.related_species_datatable.vmDataTable.columns
                        .adjust()
                        .responsive.recalc();
                }
            });
        });
        document.querySelectorAll('[data-bs-toggle="pill"]').forEach((el) => {
            el.addEventListener('show.bs.tab', this.beforeBsTabShown);
        });
    },
    beforeUnmount() {
        document.querySelectorAll('[data-bs-toggle="pill"]').forEach((el) => {
            el.removeEventListener('show.bs.tab', this.beforeBsTabShown);
        });
    },
    methods: {
        resetDirtyState: function () {
            this.tabDirtyMap = {
                ocr_location: false,
                ocr_habitat: false,
                ocr_observation: false,
            };
            for (const key in this.tabDirtyMap) {
                if (this.$refs[key].resetDirtyState) {
                    this.$refs[key].resetDirtyState();
                }
            }
        },
        onTabDirtyChange(tabKey, isDirty) {
            this.tabDirtyMap[tabKey] = isDirty;
        },
        isAnyTabDirty() {
            return Object.values(this.tabDirtyMap).some(Boolean);
        },
        beforeBsTabShown(event) {
            let vm = this;
            // If the tab being navigated away from is not dirty, allow the tab change
            if (
                !vm.tabDirtyMap[
                    event.relatedTarget.getAttribute('data-target-id')
                ]
            ) {
                return;
            }
            // Flag required to prevent infinite loop when switching tabs
            if (vm.allowTabChange) {
                vm.allowTabChange = false;
                return;
            }
            event.preventDefault();
            swal.fire({
                title: 'Unsaved Changes',
                text: 'You have unsaved changes. Are you sure you want to switch tabs?',
                icon: 'question',
                showCancelButton: true,
                reverseButtons: true,
                confirmButtonText: 'Switch Tabs',
                customClass: {
                    confirmButton: 'btn btn-primary',
                    cancelButton: 'btn btn-secondary',
                },
            }).then((result) => {
                if (result.isConfirmed) {
                    vm.allowTabChange = true;
                    bootstrap.Tab.getOrCreateInstance(event.target).show();
                }
            });
        },
        refreshFromResponse: function () {
            this.$emit('refreshFromResponse');
        },
        refreshOccurrenceReport: function () {
            this.$emit('refreshOccurrenceReport');
        },
        saveOccurrenceReport: function () {
            this.$emit('saveOccurrenceReport');
        },
    },
};
</script>

<style lang="css" scoped>
.section {
    text-transform: capitalize;
}

.list-group {
    margin-bottom: 0;
}

.fixed-top {
    position: fixed;
    top: 56px;
}

.nav-item {
    margin-bottom: 2px;
}

.nav-item > li > a {
    background-color: yellow !important;
    color: #fff;
}

.nav-item > li.active > a,
.nav-item > li.active > a:hover,
.nav-item > li.active > a:focus {
    color: white;
    background-color: blue;
    border: 1px solid #888888;
}

.admin > div {
    display: inline-block;
    vertical-align: top;
    margin-right: 1em;
}

.nav-pills .nav-link {
    border-bottom-left-radius: 0;
    border-bottom-right-radius: 0;
    border-top-left-radius: 0.5em;
    border-top-right-radius: 0.5em;
    margin-right: 0.25em;
}

.nav-pills .nav-link {
    background: lightgray;
}

.nav-pills .nav-link.active {
    background: gray;
}
</style>
