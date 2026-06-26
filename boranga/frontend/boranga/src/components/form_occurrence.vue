<template lang="html">
    <div>
        <div :id="occurrenceBody" class="">
            <CommunityOccurrence
                v-if="isCommunity"
                id="communityOccurrence"
                ref="community_occurrence"
                :occurrence_obj="occurrence_obj"
            >
            </CommunityOccurrence>
            <SpeciesOccurrence
                v-else
                id="speciesOccurrence"
                ref="species_occurrence"
                :occurrence_obj="occurrence_obj"
            >
            </SpeciesOccurrence>
        </div>
        <div class="col-md-12">
            <ul id="pills-tab" class="nav nav-pills" role="tablist">
                <li class="nav-item">
                    <a
                        id="pills-location-tab"
                        class="nav-link active"
                        data-bs-toggle="pill"
                        data-target-id="occ_location"
                        :href="'#' + locationBody"
                        role="tab"
                        :aria-controls="locationBody"
                        aria-selected="true"
                    >
                        Location<template v-if="tabDirtyMap['occ_location']"
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
                        data-target-id="occ_observation"
                        :href="'#' + observationBody"
                        role="tab"
                        aria-selected="false"
                    >
                        Observation<template
                            v-if="tabDirtyMap['occ_observation']"
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
                        data-target-id="occ_habitat"
                        :href="'#' + habitatBody"
                        role="tab"
                        aria-selected="false"
                    >
                        Habitat<template v-if="tabDirtyMap['occ_habitat']"
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
                        :href="'#' + threatBody"
                        role="tab"
                        aria-selected="false"
                    >
                        Threats
                    </a>
                </li>
                <li class="nav-item">
                    <a
                        id="pills-related-items-tab"
                        class="nav-link"
                        data-bs-toggle="pill"
                        :href="'#' + relatedItemBody"
                        role="tab"
                        :aria-controls="relatedItemBody"
                        aria-selected="false"
                    >
                        Related Items
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
                    <OCCLocations
                        id="occ_location"
                        :key="reloadcount"
                        ref="occ_location"
                        :is-external="is_external"
                        :is-internal="is_internal"
                        :profile="profile"
                        :can-edit-status="canEditStatus"
                        :occurrence_obj="occurrence_obj"
                        :referral="referral"
                        @dirty="onTabDirtyChange('occ_location', $event)"
                    >
                    </OCCLocations>
                </div>
                <div
                    :id="observationBody"
                    class="tab-pane fade"
                    role="tabpanel"
                    aria-labelledby="pills-observation-tab"
                >
                    <OCCObservation
                        id="occ_observation"
                        :key="reloadcount"
                        ref="occ_observation"
                        :is_internal="is_internal"
                        :occurrence_obj="occurrence_obj"
                        @dirty="onTabDirtyChange('occ_observation', $event)"
                    >
                    </OCCObservation>
                </div>
                <div
                    :id="habitatBody"
                    class="tab-pane fade"
                    role="tabpanel"
                    aria-labelledby="pills-habitat-tab"
                >
                    <OCCHabitat
                        id="occ_habitat"
                        :key="reloadcount"
                        ref="occ_habitat"
                        :is_internal="is_internal"
                        :occurrence_obj="occurrence_obj"
                        @dirty="onTabDirtyChange('occ_habitat', $event)"
                    >
                    </OCCHabitat>
                </div>
                <div
                    :id="documentBody"
                    class="tab-pane fade"
                    role="tabpanel"
                    aria-labelledby="pills-documents-tab"
                >
                    <OCCDocuments
                        id="occ_documents"
                        :key="reloadcount"
                        ref="occ_documents"
                        :is_internal="is_internal"
                        :is_external="is_external"
                        :occurrence_obj="occurrence_obj"
                    >
                    </OCCDocuments>
                </div>
                <div
                    :id="threatBody"
                    class="tab-pane fade"
                    role="tabpanel"
                    aria-labelledby="pills-threats-tab"
                >
                    <OCCThreats
                        id="occ_threats"
                        :key="reloadcount"
                        ref="occ_threats"
                        :is_internal="is_internal"
                        :occurrence_obj="occurrence_obj"
                    >
                    </OCCThreats>
                </div>
                <div
                    :id="reportBody"
                    class="tab-pane fade"
                    role="tabpanel"
                    aria-labelledby="pills-report-tab"
                ></div>
                <div
                    :id="sitesBody"
                    class="tab-pane fade"
                    role="tabpanel"
                    aria-labelledby="pills-sites-tab"
                ></div>
                <div
                    :id="relatedItemBody"
                    class="tab-pane fade"
                    role="tabpanel"
                    aria-labelledby="pills-sites-tab"
                >
                    <RelatedItems
                        id="occurrenceRelatedItems"
                        :key="reloadcount"
                        ref="occurrence_related_items"
                        :ajax_url="related_items_ajax_url"
                        :filter_list_url="related_items_filter_list_url"
                    >
                    </RelatedItems>
                </div>
            </div>
        </div>
    </div>
</template>

<script>
import { v4 as uuid } from 'uuid';
import OCCLocations from '@/components/common/occurrence/occ_locations.vue';
import OCCHabitat from '@/components/common/occurrence/occ_habitat.vue';
import OCCObservation from '@/components/common/occurrence/occ_observation.vue';
import RelatedItems from '@/components/common/table_related_items.vue';
import OCCDocuments from '@/components/common/occurrence/occ_documents.vue';
import OCCThreats from '@/components/common/occurrence/occ_threats.vue';
import SpeciesOccurrence from '@/components/common/occurrence/species_occurrence.vue';
import CommunityOccurrence from '@/components/common/occurrence/community_occurrence.vue';

export default {
    components: {
        OCCLocations,
        OCCHabitat,
        OCCObservation,
        OCCDocuments,
        OCCThreats,
        SpeciesOccurrence,
        CommunityOccurrence,
        RelatedItems,
    },
    props: {
        occurrence_obj: {
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
        profile: {
            type: Object,
            required: false,
        },
        canEditStatus: {
            type: Boolean,
            default: true,
        },
    },
    emits: ['dirty'],
    data: function () {
        return {
            values: null,
            reloadcount: 0,
            locationBody: 'locationBody' + uuid(),
            habitatBody: 'habitatBody' + uuid(),
            observationBody: 'observationBody' + uuid(),
            threatBody: 'threatBody' + uuid(),
            documentBody: 'documentBody' + uuid(),
            relatedItemBody: 'relatedItemBody' + uuid(),
            reportBody: 'reportBody' + uuid(),
            sitesBody: 'sitesBody' + uuid(),
            occurrenceBody: 'occurrenceBody' + uuid(),
            tabDirtyMap: {
                occ_location: false,
                occ_habitat: false,
                occ_observation: false,
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
    },
    computed: {
        isCommunity: function () {
            return this.occurrence_obj.group_type == 'community';
        },
        related_items_ajax_url: function () {
            return (
                '/api/occurrence/' +
                this.occurrence_obj.id +
                '/get_related_items/'
            );
        },
        related_items_filter_list_url: function () {
            return '/api/occurrence/filter_list.json';
        },
    },
    mounted: function () {
        document.querySelectorAll('a[data-bs-toggle="pill"]').forEach((el) => {
            el.addEventListener('shown.bs.tab', () => {
                if (el.id == 'pills-threats-tab') {
                    this.$refs.occ_threats.adjust_table_width();
                } else if (el.id == 'pills-documents-tab') {
                    this.$refs.occ_documents.adjust_table_width();
                } else if (el.id == 'pills-related-items-tab') {
                    this.$refs.occurrence_related_items.adjust_table_width();
                } else if (el.id == 'pills-habitat-tab') {
                    this.$refs.occ_habitat.$refs.related_species.$refs.related_species_datatable.vmDataTable.columns
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
                occ_location: false,
                occ_habitat: false,
                occ_observation: false,
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
