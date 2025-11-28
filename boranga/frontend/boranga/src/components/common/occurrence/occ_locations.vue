<template lang="html">
    <div id="occLocations">
        <FormSection
            :form-collapse="false"
            label="Location"
            Index="occurrence_location"
            :subtitle="isDirty ? 'Unsaved Changes' : ''"
            :subtitle-class="isDirty ? 'text-warning ms-auto' : ''"
            :show-subtitle-icon="true"
        >
            <div class="row mb-3">
                <div class="col">
                    <span class="text-danger">*</span>
                    <span class="text-muted ps-1"
                        >You must indicate the location for your
                        occurrence</span
                    >
                </div>
            </div>
            <div class="row mb-3">
                <MapComponent
                    ref="component_map"
                    :key="componentMapKey"
                    class="me-3"
                    :context="occurrence_obj"
                    :is_external="false"
                    :point-features-supported="true"
                    :point-limit="
                        occurrence_obj.group_type === 'fauna' ? 1 : null
                    "
                    :polygon-features-supported="true"
                    :drawable="!isReadOnly"
                    :editable="!isReadOnly"
                    :file-upload-disabled="isReadOnly"
                    level="internal"
                    style-by="model"
                    :map-info-text="
                        isInternal
                            ? ''
                            : 'Some text to explain the map and its use.'
                    "
                    :selectable="true"
                    :coordinate-reference-systems="coordinateReferenceSystems"
                    :spatial-operations-allowed="['__all__']"
                    :tile-layer-api-url="tileLayerApiUrl"
                    :validate-feature-before-save="true"
                    :query-layer-definition="{
                        name: occurrenceLayerName,
                        title: 'Occurrence',
                        default: true,
                        processed: true,
                        can_edit: true,
                        can_buffer: true,
                        api_url: occApiUrl,
                        ids: [occurrence_obj.id],
                        geometry_name: 'occ_geometry',
                        identifier_name: 'occurrence_number',
                        z_index: 3,
                        property_display_map: occPropertyDisplayMap,
                        property_overwrite: {
                            area_sqm: featureAreaMeter,
                            area_sqhm: (feature) =>
                                featureAreaMeter(feature) / 10000,
                            color: '#6273f5', // light blue
                            stroke: '#192163', // dark blue
                        },
                    }"
                    :additional-layers-definitions="[
                        {
                            name: queryLayerName,
                            title: 'Occurrence Reports',
                            default: false,
                            can_edit: false,
                            can_buffer: false,
                            can_hide_geometries: true,
                            api_url: ocrApiUrl,
                            ids: occurrenceReportIds,
                            geometry_name: 'ocr_geometry',
                            identifier_name: 'occurrence_report_number',
                            z_index: 2,
                            collapse: true,
                            property_display_map: ocrPropertyDisplayMap,
                        },
                        {
                            name: bufferLayerName,
                            title: 'Buffer Geometries',
                            default: false,
                            processed: false,
                            can_edit: false,
                            can_buffer: false,
                            handler: bufferGeometryHandler, // Buffer geometries are a property of occurrence geometry. This handler returns the buffer geometries from the occurrence geometries.
                            geometry_name: 'geometry',
                            identifier_name: 'label',
                            z_index: 1,
                            property_display_map: bufferPropertyDisplayMap,
                            property_overwrite: {
                                area_sqm: featureAreaMeter,
                                area_sqhm: (feature) =>
                                    featureAreaMeter(feature) / 10000,
                                // color: '#ebeb49', // yellowish
                                // stroke: '#db8223', // orange
                            },
                        },
                        {
                            name: 'site_layer',
                            title: 'Site Geometries',
                            default: false,
                            processed: false,
                            can_edit: true,
                            can_buffer: false,
                            api_url: siteApiUrl,
                            query_param_key: 'occurrence_id',
                            identifier_name: 'site_number',
                            z_index: 4,
                            ids: [occurrence_obj.id],
                            property_display_map: sitePropertyDisplayMap,
                            property_overwrite: {
                                label: 'Site',
                                // color: '#FF0000',
                            },
                        },
                    ]"
                    @features-loaded="mapFeaturesLoaded"
                    @crs-select-search="searchForCRS"
                    @toggle-show-hide="toggleShowOnMapLayer"
                    @validate-feature="validateFeature"
                    @prepare-new-site-at-coordinates="
                        prepareNewSiteAtCoordinates
                    "
                    @dirty="mapIsDirty = $event"
                ></MapComponent>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Region:</label>
                <div class="col-sm-9">
                    <select
                        v-model="occurrence_obj.location.region_id"
                        :disabled="isReadOnly"
                        class="form-select"
                        @change="filterDistrict($event)"
                    >
                        <option
                            v-for="option in region_list"
                            :key="option.id"
                            :value="option.id"
                        >
                            {{ option.name }}
                        </option>
                    </select>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">District:</label>
                <div class="col-sm-9">
                    <template v-if="!isReadOnly">
                        <template
                            v-if="
                                filtered_district_list &&
                                filtered_district_list.length > 0 &&
                                occurrence_obj.location.district_id &&
                                !filtered_district_list
                                    .map((d) => d.id)
                                    .includes(
                                        occurrence_obj.location.district_id
                                    )
                            "
                        >
                            <input
                                v-if="occurrence_obj.location.district_id"
                                type="text"
                                class="form-control mb-3"
                                :value="
                                    occurrence_obj.location.district +
                                    ' (Now Archived)'
                                "
                                disabled
                            />
                            <div class="mb-3 text-muted">
                                Change district to:
                            </div>
                        </template>
                        <select
                            v-model="occurrence_obj.location.district_id"
                            class="form-select"
                        >
                            <option
                                v-for="option in filtered_district_list"
                                :key="option.id"
                                :value="option.id"
                            >
                                {{ option.name }}
                            </option>
                        </select>
                    </template>
                    <template v-else>
                        <input
                            v-model="occurrence_obj.location.district"
                            class="form-control"
                            type="text"
                            :disabled="isReadOnly"
                        />
                    </template>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label"
                    >Location Description:</label
                >
                <div class="col-sm-9">
                    <textarea
                        id="loc_description"
                        v-model="occurrence_obj.location.location_description"
                        :disabled="isReadOnly"
                        class="form-control"
                        rows="2"
                        placeholder=""
                    />
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Locality:</label>
                <div class="col-sm-9">
                    <textarea
                        id="locality"
                        v-model="occurrence_obj.location.locality"
                        :disabled="isReadOnly"
                        class="form-control"
                        rows="1"
                        placeholder=""
                    />
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label"
                    >Boundary Description:</label
                >
                <div class="col-sm-9">
                    <textarea
                        id="boundary_descr"
                        v-model="occurrence_obj.location.boundary_description"
                        :disabled="isReadOnly"
                        class="form-control"
                        rows="2"
                        placeholder=""
                    />
                </div>
            </div>

            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label"
                    >Map Data Type</label
                >
                <div class="col-sm-6">
                    <label class="me-2">Boundary</label
                    ><input
                        disabled
                        type="radio"
                        :checked="occurrence_obj.location.has_boundary"
                        class="form-check-input me-2"
                    />
                    <label class="me-2">Point/s</label
                    ><input
                        disabled
                        type="radio"
                        :checked="occurrence_obj.location.has_points"
                        class="form-check-input me-2"
                    />
                </div>
            </div>

            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label"
                    >Coordinate Source:</label
                >
                <div class="col-sm-9">
                    <template v-if="!isReadOnly">
                        <template
                            v-if="
                                coordinate_source_list &&
                                coordinate_source_list.length > 0 &&
                                occurrence_obj.location &&
                                !coordinate_source_list
                                    .map((d) => d.id)
                                    .includes(
                                        occurrence_obj.location
                                            .coordinate_source_id
                                    )
                            "
                        >
                            <input
                                v-if="occurrence_obj.location.coordinate_source"
                                type="text"
                                class="form-control mb-3"
                                :value="
                                    occurrence_obj.location.coordinate_source +
                                    ' (Now Archived)'
                                "
                                disabled
                            />
                            <div class="mb-3 text-muted">
                                Change coordinate source to:
                            </div>
                        </template>
                        <select
                            v-model="
                                occurrence_obj.location.coordinate_source_id
                            "
                            class="form-select"
                        >
                            <option
                                v-for="option in coordinate_source_list"
                                :key="option.id"
                                :value="option.id"
                            >
                                {{ option.name }}
                            </option>
                        </select>
                    </template>
                    <template v-else>
                        <input
                            v-model="occurrence_obj.location.coordinate_source"
                            class="form-control"
                            type="text"
                            :disabled="isReadOnly"
                        />
                    </template>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label fw-bold"
                    >Location Accuracy:
                    <span class="text-danger">*</span></label
                >
                <div class="col-sm-9">
                    <template v-if="!isReadOnly">
                        <template
                            v-if="
                                location_accuracy_list &&
                                location_accuracy_list.length > 0 &&
                                occurrence_obj.location &&
                                !location_accuracy_list
                                    .map((d) => d.id)
                                    .includes(
                                        occurrence_obj.location
                                            .location_accuracy_id
                                    )
                            "
                        >
                            <input
                                v-if="occurrence_obj.location.location_accuracy"
                                type="text"
                                class="form-control mb-3"
                                :value="
                                    occurrence_obj.location.location_accuracy +
                                    ' (Now Archived)'
                                "
                                disabled
                            />
                            <div class="mb-3 text-muted">
                                Change coordinate source to:
                            </div>
                        </template>
                        <select
                            v-model="
                                occurrence_obj.location.location_accuracy_id
                            "
                            class="form-select"
                        >
                            <option
                                v-for="option in location_accuracy_list"
                                :key="option.id"
                                :value="option.id"
                            >
                                {{ option.name }}
                            </option>
                        </select>
                    </template>
                    <template v-else>
                        <input
                            v-model="occurrence_obj.location.location_accuracy"
                            class="form-control"
                            type="text"
                            :disabled="isReadOnly"
                        />
                    </template>
                </div>
            </div>
            <div class="row mb-3">
                <div class="col-sm-12">
                    <span
                        v-if="occurrence_obj.location.copied_ocr"
                        class="float-end"
                        ><b
                            >Sourced from
                            {{ occurrence_obj.location.copied_ocr }}</b
                        ></span
                    >
                </div>
            </div>
            <div class="row mb-3">
                <div class="col-sm-12">
                    <button
                        v-if="!updatingLocationDetails"
                        class="btn btn-sm float-end"
                        :class="{
                            'btn-primary': isDirty,
                            'btn-light': !isDirty,
                            border: !isDirty,
                        }"
                        :disabled="isReadOnly || !isDirty"
                        @click.prevent="updateLocationDetails()"
                    >
                        <template v-if="isDirty"
                            >Save Section<i
                                class="bi bi-exclamation-circle-fill text-warning ps-2"
                            ></i
                        ></template>
                        <template v-else
                            >Saved<i
                                class="bi bi-check-circle-fill text-success ps-2"
                            ></i
                        ></template>
                    </button>
                    <button v-else disabled class="float-end btn btn-primary">
                        Saving
                        <span
                            class="spinner-border spinner-border-sm"
                            role="status"
                            aria-hidden="true"
                        ></span>
                        <span class="visually-hidden">Loading...</span>
                    </button>
                </div>
            </div>
            <FormSection
                :form-collapse="false"
                label="Occurrence Sites"
                Index="occurrence_sites_datatable"
            >
                <div>
                    <OccurrenceSiteDatatable
                        v-if="occurrence_obj"
                        ref="occurrence_sites_datatable"
                        :key="datatableOCCSiteKey"
                        :occurrence_obj="occurrence_obj"
                        @updated-sites="updatedSites"
                    >
                    </OccurrenceSiteDatatable>
                </div>
            </FormSection>
            <!-- Occurrence Tenure Datatable -->
            <FormSection
                :form-collapse="false"
                label="Occurrence Tenures"
                Index="occurrence_tenure_datatable"
            >
                <div>
                    <OccurrenceTenureDatatable
                        v-if="occurrence_obj"
                        ref="occurrence_tenure_datatable"
                        :key="datatableOCCTenureKey"
                        :occurrence-id="occurrence_obj.id"
                        :href-container-id="getMapContainerId"
                        @highlight-on-map="highlightPointOnMap"
                        @edit-tenure-details="editTenureDetails"
                    >
                    </OccurrenceTenureDatatable>
                </div>
            </FormSection>
            <RelatedReports
                v-if="occurrence_obj"
                ref="related_reports_datatable"
                :key="datatableRelatedOCRKey"
                :is-read-only="isReadOnly"
                :occurrence_obj="occurrence_obj"
                :section_type="'location'"
                :href-container-id="getMapContainerId"
                :target-map-layer-name-for-copy="occurrenceLayerName"
                :target-map-layer-name-for-show-hide="queryLayerName"
                @copy-update="copyUpdate"
                @highlight-on-map="highlightOCROnMapLayer"
                @copy-to-map-layer="copyToMapLayer"
                @toggle-show-on-map="toggleShowOCROnMapLayer"
            />
        </FormSection>
    </div>
</template>

<script>
import { v4 as uuid } from 'uuid';
import FormSection from '@/components/forms/section_toggle.vue';
import { api_endpoints, helpers } from '@/utils/hooks';
import MapComponent from '../component_map.vue';
import OccurrenceSiteDatatable from '@/components/internal/occurrence/occurrence_site_datatable.vue';
import OccurrenceTenureDatatable from '@/components/internal/occurrence/occurrence_tenure_datatable.vue';
import RelatedReports from '@/components/common/occurrence/occ_related_ocr_table.vue';
import {
    intersects,
    intersectedArea,
} from '@/components/common/map_functions.js';
export default {
    name: 'OCClocations',
    components: {
        MapComponent,
        FormSection,
        OccurrenceSiteDatatable,
        OccurrenceTenureDatatable,
        RelatedReports,
    },
    props: {
        occurrence_obj: {
            type: Object,
            required: true,
        },
        isExternal: {
            type: Boolean,
            default: false,
        },
        isInternal: {
            type: Boolean,
            default: true,
        },
        canEditStatus: {
            type: Boolean,
            default: true,
        },
        referral: {
            type: Object,
            required: false,
            default: () => null,
        },
    },
    emits: ['dirty'],
    data() {
        return {
            uuid_component_map: uuid(),
            uuid_datatable_ocr: uuid(),
            uuid_datatable_occ_site: uuid(),
            uuid_datatable_occ_tenure: uuid(),
            crs: [],
            region_list: [],
            district_list: [],
            filtered_district_list: [],
            updatingLocationDetails: false,
            listOfValuesDict: {},
            datum_list: [],
            coordinate_source_list: [],
            location_accuracy_list: [],
            mapReady: false,
            mapContainerId: false,
            queryLayerName: 'query_layer',
            occurrenceLayerName: 'occurrence_layer',
            bufferLayerName: 'buffer_layer',
            plausibilityGeometryFeatures: [],
            originalLocation: null,
            mapIsDirty: false,
            bufferLayerPending: false,
        };
    },
    computed: {
        componentMapKey: function () {
            return `component-map-${this.uuid_component_map}`;
        },
        datatableRelatedOCRKey: function () {
            return `datatable-ocr-${this.uuid_datatable_ocr}`;
        },
        datatableOCCSiteKey: function () {
            return `datatable-occ-site-${this.uuid_datatable_occ_site}`;
        },
        datatableOCCTenureKey: function () {
            return `datatable-occ-tenure-${this.uuid_datatable_occ_tenure}`;
        },
        coordinateReferenceSystems: function () {
            return this.crs;
        },
        occurrenceReportIds: function () {
            let ocrIds = this.occurrence_obj.occurrence_reports.map(
                (report) => report.id
            );
            // If no ocr ids for this occ, return [-1], because [] pulls _all_ ocrs
            ocrIds = ocrIds.length ? ocrIds : [-1];
            return ocrIds;
        },
        tileLayerApiUrl: function () {
            return api_endpoints.tile_layer;
        },
        ocrApiUrl: function () {
            return api_endpoints.occurrence_report + '/list_for_map/';
        },
        occApiUrl: function () {
            return api_endpoints.occurrence + 'list_for_map/';
        },
        siteApiUrl: function () {
            return '/api/occurrence_sites/list_for_map/';
        },
        ocrPropertyDisplayMap: function () {
            return {
                id: 'ID',
                // name: 'Name',
                label: 'Label', // Occurrence Report
                geometry_source: 'Geometry Source',
                occurrence_report_number: 'Identification Number',
                processing_status_display: 'Processing Status',
                lodgement_date_display: 'Lodgement Date',
                locked: 'Locked',
                drawn_by: 'Drawn By', // fullname
                last_updated_by: 'Updated By', // fullname
                updated_date: 'Last updated',
            };
        },
        occPropertyDisplayMap: function () {
            return {
                id: 'ID',
                // name: 'Name',
                label: 'Label', // Occurrence
                occurrence_number: 'Identification Number', // OCC1
                geometry_source: 'Geometry Source',
                processing_status_display: 'Processing Status',
                area_sqm: 'Area [m²]',
                area_sqhm: 'Area [ha]',
                drawn_by: 'Drawn By', // fullname
                last_updated_by: 'Updated By', // fullname
                updated_date: 'Last updated',
            };
        },
        bufferPropertyDisplayMap: function () {
            return {
                id: 'ID',
                // name: 'Name',
                label: 'Label', // OCC1 [Buffer]
                geometry_source: 'Geometry Source',
                // processing_status: 'Processing Status',
                // lodgement_date_display: 'Lodgement Date',
                area_sqm: 'Area [m²]',
                area_sqhm: 'Area [ha]',
                buffer_radius: 'Buffer Radius [m]',
                updated_date: 'Last updated',
            };
        },
        sitePropertyDisplayMap: function () {
            return {
                id: 'ID', // 8
                label: 'Label', // Site
                site_number: 'Identification Number', // ST1
                site_name: 'Site Name', // My Site
                drawn_by: 'Drawn By', // fullname
                last_updated_by: 'Updated By', // fullname
                updated_date: 'Last updated',
            };
        },
        bufferGeometriesApiUrl: function () {
            return api_endpoints.occurrence + 'buffer_geometries/';
        },
        isReadOnly: function () {
            return !this.occurrence_obj.can_user_edit;
        },
        csrf_token: function () {
            return helpers.getCookie('csrftoken');
        },
        getMapContainerId: function () {
            if (!this.mapReady) {
                return '';
            }
            return this.mapContainerId;
        },
        locationIsDirty: function () {
            if (this.originalLocation === null) {
                return false;
            }
            return (
                JSON.stringify(this.occurrence_obj.location) !=
                this.originalLocation
            );
        },
        isDirty: function () {
            return this.mapIsDirty || this.locationIsDirty;
        },
    },
    watch: {
        isDirty: function (newValue) {
            if (newValue) {
                this.$emit('dirty', true);
            } else {
                this.$emit('dirty', false);
            }
        },
    },
    created: async function () {
        let vm = this;

        fetch(
            helpers.add_endpoint_join(
                api_endpoints.occurrence,
                `/location-list-of-values/?id=${vm.occurrence_obj.id}`
            )
        )
            .then((response) => {
                if (!response.ok) {
                    throw new Error('Network response was not ok');
                }
                return response.json();
            })
            .then((data) => {
                vm.listOfValuesDict = Object.assign({}, data);
                vm.datum_list = vm.listOfValuesDict.datum_list;
                vm.coordinate_source_list =
                    vm.listOfValuesDict.coordinate_source_list;
                vm.coordinate_source_list.splice(0, 0, {
                    id: null,
                    name: null,
                });
                vm.location_accuracy_list =
                    vm.listOfValuesDict.location_accuracy_list;
                vm.location_accuracy_list.splice(0, 0, {
                    id: null,
                    name: null,
                });
            })
            .catch((error) => {
                console.error('Error fetching location values list:', error);
            });

        const response = await fetch('/api/region_district_filter_dict/');
        vm.filterRegionDistrict = await response.json();
        vm.region_list = vm.filterRegionDistrict.region_list;
        vm.district_list = vm.filterRegionDistrict.district_list;
        vm.region_list.splice(0, 0, {
            id: null,
            name: null,
        });
        this.filterDistrict();

        fetch(
            helpers.add_endpoint_join(
                api_endpoints.occurrence,
                `/available-crs-for-occurrence/?id=${this.occurrence_obj.id}`
            )
        )
            .then((response) => {
                if (!response.ok) {
                    throw new Error('Network response was not ok');
                }
                return response.json();
            })
            .then((data) => {
                this.crs = data.crs;
            })
            .catch((error) => {
                console.error(
                    'Error fetching available orf crs values list:',
                    error
                );
            });

        fetch(
            helpers.add_endpoint_join(
                api_endpoints.occurrence,
                `/plausibility-geometry/?id=${this.occurrence_obj.id}`
            )
        )
            .then((response) => {
                if (!response.ok) {
                    throw new Error('Network response was not ok');
                }
                return response.json();
            })
            .then((data) => {
                const features = [];
                if (data.features) {
                    for (let featureData of data.features) {
                        let feature =
                            vm.$refs.component_map.featureFromDict(featureData);
                        features.push(feature);
                    }
                }
                this.plausibilityGeometryFeatures = Object.assign([], features);
            })
            .catch((error) => {
                console.error(
                    'Error fetching available plausibility-geometry:',
                    error
                );
            });

        // Make sure the datatables have access to the map container id to have the page scroll to the map anchor
        this.refreshDatatables();
    },
    mounted: function () {
        let vm = this;
        // Capture original state after Vue has fully processed the component
        vm.$nextTick(() => {
            vm.resetDirtyState();
        });
    },
    methods: {
        resetDirtyState: function () {
            this.originalLocation = JSON.stringify(
                this.occurrence_obj.location
            );
            this.$refs.component_map.takeSnapshot();
        },
        filterDistrict: function (event) {
            this.$nextTick(() => {
                if (event) {
                    this.occurrence_obj.location.district_id = null; //-----to remove the previous selection
                }
                this.filtered_district_list = [];
                this.filtered_district_list = [
                    {
                        id: null,
                        name: '',
                        region_id: null,
                        archive_date: null,
                    },
                ];
                //---filter districts as per region selected
                for (let choice of this.district_list) {
                    if (
                        choice.region_id ===
                            this.occurrence_obj.location.region_id &&
                        !choice.archive_date &&
                        !new Date(choice.archive_date) <= new Date()
                    ) {
                        this.filtered_district_list.push(choice);
                    }
                }
            });
        },
        copyUpdate: function (object, section) {
            let vm = this;
            vm.occurrence_obj[section] = object[section];
        },
        updateLocationDetails: function () {
            let vm = this;
            vm.updatingLocationDetails = true;

            let payload = { location: vm.occurrence_obj.location };
            // species_id added in updateLocationDetails as its not part of Location model but needs to be updated on button click
            payload.species_id = vm.occurrence_obj.species_id;
            // community_id added in updateLocationDetails as its not part of Location model but needs to be updated on button click
            payload.community_id = vm.occurrence_obj.community_id;

            // When in Entering Conditions status ApplicationForm might not be there
            // adding occ_geometry from the map_component to payload
            if (vm.$refs.component_map) {
                // Get the occ geometry with opacity fields set
                const occ_geometry = vm.OccGeometryFromMap();
                payload.occ_geometry = JSON.stringify(occ_geometry);
                vm.$refs.component_map.setLoadingMap(true);
            }

            payload.site_geometry =
                vm.$refs.component_map.getJSONFeatures('site_layer');

            fetch(
                helpers.add_endpoint_json(
                    api_endpoints.occurrence,
                    vm.occurrence_obj.id + '/update_location_details'
                ),
                {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify(payload),
                }
            ).then(
                async (response) => {
                    vm.updatingLocationDetails = false;
                    vm.occurrence_obj.location = await response.json();
                    vm.originalLocation = JSON.stringify(
                        vm.occurrence_obj.location
                    );
                    swal.fire({
                        title: 'Saved',
                        text: 'Location details have been saved',
                        icon: 'success',
                        showConfirmButton: false,
                        timer: 1200,
                    }).then(() => {
                        if (vm.occurrence_obj.processing_status == 'Unlocked') {
                            vm.$router.go();
                        }
                    });
                    vm.$refs.component_map.forceToRefreshMap();
                    vm.refreshDatatables();
                    vm.$nextTick(() => {
                        vm.$refs.component_map.takeSnapshot();
                        vm.mapIsDirty = false;
                    });
                },
                (error) => {
                    var text = helpers.apiVueResourceError(error);
                    swal.fire({
                        title: 'Error',
                        text:
                            'Location details cannot be saved because of the following error: ' +
                            text,
                        icon: 'error',
                        customClass: {
                            confirmButton: 'btn btn-primary',
                        },
                    });
                    vm.updatingLocationDetails = false;
                    vm.$refs.component_map.setLoadingMap(false);
                }
            );
        },
        incrementComponentMapKey: function () {
            this.uuid_component_map = uuid();
            this.$nextTick(() => {
                this.mapContainerId = this.$refs.component_map.map_container_id;
            });
        },
        refreshDatatables: function () {
            // Rather than recreating these components by incrementing their key
            // we can just reload the datatables to ensure they have fresh data.
            this.refreshDatatableRelatedOCR();
            // The data for this datatable has to be reloaded as sites may have
            // been modified or deleted when updating map geometries.
            this.$refs.occurrence_sites_datatable.updatedSites();
            // The data for this datatable has to be reloaded as it's likely
            // the tenures would change based on modified map geometries.
            this.$refs.occurrence_tenure_datatable.updatedTenureArea();
        },
        refreshDatatableRelatedOCR: function () {
            this.$refs.related_reports_datatable.reloadDatatable();
        },
        updatedSites: function () {
            this.incrementComponentMapKey();
        },
        searchForCRS: function (search, loading) {
            const vm = this;
            if (search.length < 2) {
                loading(false);
                return;
            }

            loading(true);
            fetch(
                helpers.add_endpoint_join(
                    api_endpoints.occurrence_report,
                    `/epsg-code-datums/?search=${search}`
                )
            )
                .then(async (response) => {
                    if (!response.ok) {
                        const text = await response.json();
                        throw new Error(text);
                    } else {
                        return response.json();
                    }
                })
                .then((data) => {
                    console.log('New search data return:', data);
                    // Append to existing list of crs rather than overwrite and potentially lose prior search results which might create issues when setting a pre-selected value
                    const crs_ids = vm.crs.map((crs) => crs.id);
                    data.forEach((crs) => {
                        if (!crs_ids.includes(crs.id)) {
                            vm.crs.push(crs);
                        }
                    });
                })
                .catch((error) => {
                    console.log(error);
                    swal.fire({
                        title: 'Search',
                        text: error,
                        icon: 'error',
                    });
                })
                .finally(() => {
                    loading(false);
                });
        },
        mapFeaturesLoaded: function (event) {
            this.mapContainerId = this.$refs.component_map.map_container_id;
            this.mapReady = true;

            // Prevent infinite recursion: don't reload buffer geometries when the buffer layer itself was just loaded
            // The issue occurs because:
            // 1. Occurrence geometries load -> triggers 'features-loaded' event
            // 2. This handler loads buffer geometries from the occurrence features
            // 3. Loading buffer geometries -> triggers another 'features-loaded' event
            // 4. Without this check, step 2 repeats infinitely
            // Solution: Check if the event was triggered by the buffer layer and skip reloading if so
            const layerName = event?.details?.layerName;
            if (layerName === this.bufferLayerName) {
                return;
            }

            if (this.bufferLayerPending) {
                const bufferPayload = this.buildBufferLayerPayload();
                if (bufferPayload.length) {
                    this.$refs.component_map.loadMapFeatures(
                        bufferPayload,
                        this.bufferLayerName
                    );
                    this.bufferLayerPending = false;
                }
            }
        },
        getMapFeatureById: function (id, layer_name) {
            if (!layer_name) {
                layer_name = this.queryLayerName;
            }
            const map = this.$refs.component_map;
            const layer = map.getLayerByName(layer_name);
            return map.getFeatureById(layer, id);
        },
        getMapFeaturesByOccurrenceReportId: function (
            occurrenceReportId,
            layer_name
        ) {
            if (!layer_name) {
                layer_name = this.queryLayerName;
            }
            const map = this.$refs.component_map;
            const layer = map.getLayerByName(layer_name);
            return map.getFeaturesByOccurrenceReportId(
                layer,
                occurrenceReportId
            );
        },
        highlightPointOnMap: function (coordinates) {
            if (!coordinates) {
                console.warn('No coordinates found');
                return;
            }
            this.$refs.component_map.highlightPointOnTenureLayer(coordinates);
        },
        highlightOCROnMapLayer: function (id) {
            const feature = this.getMapFeatureById(id);
            this.$refs.component_map.centerOnFeature(
                feature,
                this.$refs.component_map.mapDefaultZoom
            );
        },
        editTenureDetails: function (id) {
            console.log(id);
        },
        copyToMapLayer: function (id, target_layer) {
            console.log('Copy to map layer:', id, target_layer);
            const map = this.$refs.component_map;
            const feature = this.getMapFeatureById(id);
            map.copyFeatureToLayer(feature, map.getLayerByName(target_layer));
        },
        /**
         * Toggle the show on map property of a feature.
         * @param {Object|Number} feature - The feature object or the feature id.
         * @param {String} layer_name - The layer name where the feature is located.
         */
        toggleShowOnMapLayer: function (feature, layer_name) {
            if (!isNaN(Number(feature))) {
                const id = feature;
                feature = this.getMapFeatureById(id, layer_name);
            }
            const show_on_map = feature.getProperties().show_on_map;
            feature.set('show_on_map', !show_on_map);
            this.updateShowHide(feature, layer_name).then(() => {
                this.refreshDatatableRelatedOCR();
                this.$refs.component_map.forceToRefreshMap();
            });
        },
        toggleShowOCROnMapLayer: function (occurrenceReportId, layer_name) {
            const features = this.getMapFeaturesByOccurrenceReportId(
                occurrenceReportId,
                layer_name
            );
            if (!features || features.length === 0) {
                console.warn(
                    `No features found for occurrence report with id ${occurrenceReportId} on layer ${layer_name}.`
                );
                return;
            }
            let show_on_map = features[0].getProperties().show_on_map;
            for (let feature of features) {
                feature.set(
                    'show_on_map',
                    !feature.getProperties().show_on_map
                );
            }
            show_on_map = !show_on_map; // Toggle the show_on_map property
            this.updateShowHideOCRGeometries(
                occurrenceReportId,
                show_on_map
            ).then(() => {
                this.refreshDatatableRelatedOCR();
                this.$refs.component_map.forceToRefreshMap();
                let shownOrHidden = show_on_map ? 'shown' : 'hidden';
                swal.fire({
                    title: `Geometries ${shownOrHidden}`,
                    text: `Occurrence Report ORF${occurrenceReportId} geometries are now ${shownOrHidden}.`,
                    icon: 'success',
                    timer: 1500,
                    showConfirmButton: false,
                });
            });
        },
        bufferGeometryHandler: function () {
            const payload = this.buildBufferLayerPayload();
            if (!payload.length) {
                this.bufferLayerPending = true;
            }
            return payload;
        },
        buildBufferLayerPayload: function () {
            const mapComponent = this.$refs.component_map;
            if (!mapComponent || !mapComponent.map) {
                console.warn(
                    'Map component is not ready when deriving buffer geometries.'
                );
                return [];
            }

            const occurrenceLayer = mapComponent.getLayerByName(
                this.occurrenceLayerName
            );
            if (!occurrenceLayer) {
                console.warn(
                    'Occurrence layer is unavailable when deriving buffer geometries.'
                );
                return [];
            }

            const source =
                typeof occurrenceLayer.getSource === 'function'
                    ? occurrenceLayer.getSource()
                    : null;
            if (!source) {
                console.warn(
                    'Occurrence layer source is unavailable when deriving buffer geometries.'
                );
                return [];
            }

            const bufferFeatures = source
                .getFeatures()
                .map((feature) => feature.getProperties().buffer_geometry)
                .filter(
                    (bufferGeometry) =>
                        bufferGeometry && bufferGeometry.geometry
                );

            if (!bufferFeatures.length) {
                return [];
            }

            return [
                {
                    geometry: {
                        type: 'FeatureCollection',
                        features: bufferFeatures,
                    },
                },
            ];
        },
        updateShowHide: function (feature, layerName) {
            if (!layerName) {
                console.warn(`No layer name provided for update show/hide.`);
                return;
            }

            let apiEndpoint = null;
            if (layerName == this.queryLayerName) {
                apiEndpoint = helpers.add_endpoint_join(
                    api_endpoints.occurrence_report,
                    `/${feature.getProperties().name}/update_show_on_map/`
                );
            }

            if (!apiEndpoint) {
                console.warn(
                    `No API endpoint found for update show/hide for layer ${layerName}.`
                );
                return;
            }

            const showOnMap = feature.getProperties().show_on_map;
            const modelId = feature.getProperties().model_id;
            const payload = {
                method: 'PATCH',
                headers: {
                    'Content-Type': 'application/json',
                    'X-CSRFToken': this.csrf_token,
                },
                body: JSON.stringify({
                    show_on_map: showOnMap,
                    model_id: modelId,
                }),
            };

            console.log(
                `Updating show on map for ${
                    feature.getProperties().label
                }: ${showOnMap}.`
            );
            return fetch(apiEndpoint, payload)
                .then(async (response) => {
                    if (!response.ok) {
                        return await response.json().then((json) => {
                            throw new Error(json);
                        });
                    }
                    return response.json();
                })
                .then((data) => {
                    return data;
                })
                .catch((error) => {
                    swal.fire({
                        title: 'Error',
                        text:
                            'Cannot change geometry visibility because of the following error: ' +
                            error,
                        icon: 'error',
                        customClass: {
                            confirmButton: 'btn btn-primary',
                        },
                    });
                });
        },
        updateShowHideOCRGeometries: function (
            occurrenceReportId,
            show_on_map
        ) {
            if (!occurrenceReportId) {
                console.warn(
                    `No occurrenceReportId provided for update show/hide.`
                );
                return;
            }

            const apiEndpoint = helpers.add_endpoint_join(
                api_endpoints.occurrence_report,
                `/${occurrenceReportId}/update_show_on_map/`
            );

            const payload = {
                method: 'PATCH',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    show_on_map: show_on_map,
                }),
            };

            return fetch(apiEndpoint, payload)
                .then(async (response) => {
                    if (!response.ok) {
                        return await response.json().then((json) => {
                            throw new Error(json);
                        });
                    }
                    return response.json();
                })
                .then((data) => {
                    return data;
                })
                .catch((error) => {
                    swal.fire({
                        title: 'Error',
                        text:
                            'Cannot change geometry visibility because of the following error: ' +
                            error,
                        icon: 'error',
                        customClass: {
                            confirmButton: 'btn btn-primary',
                        },
                    });
                });
        },
        featureAreaMeter: function (feature) {
            if (feature) {
                return this.$refs.component_map.featureArea(feature);
            }
            return 0;
        },
        OccGeometryFromMap: function () {
            const occ_geometry = JSON.parse(
                this.$refs.component_map.getJSONFeatures()
            );
            const buffer_opacity = this.$refs.component_map
                .getLayerByName(this.bufferLayerName)
                .getProperties().opacity;
            // Set buffer opacity to the features
            occ_geometry.features.forEach((f) => {
                f.properties.buffer_opacity = buffer_opacity;
            });

            return occ_geometry;
        },
        validateFeature: function (feature) {
            // Validate the feature
            if (this.plausibilityGeometryFeatures.length > 0) {
                console.log('Validating feature:', feature);
                this.$refs.component_map.setLoadingMap(true);
                this.plausibilityCheckFeature(feature).then(async (areas) => {
                    let warning = false;
                    let error = false;

                    // eslint-disable-next-line no-unused-vars
                    for (let [key, entry] of Object.entries(areas)) {
                        const ratio_effective_area =
                            entry.ratio_effective_area || 1.0;
                        const entities =
                            (entry.area * ratio_effective_area) /
                            entry.average_area;
                        if (
                            entry.error_value &&
                            entities >= entry.error_value
                        ) {
                            console.error(
                                `Feature potentially intersects with up to ${Math.ceil(
                                    entities
                                )} entities with an error value threshold of ${
                                    entry.error_value
                                } entities with an average area of ${
                                    entry.average_area
                                } m²`
                            );
                            error = true;
                            break;
                        } else if (
                            entry.warning_value &&
                            entities >= entry.warning_value
                        ) {
                            console.warn(
                                `Feature potentially intersects with up to ${Math.ceil(
                                    entities
                                )} entities with a warning value threshold of ${
                                    entry.warning_value
                                } entities with an average area of ${
                                    entry.average_area
                                } m²`
                            );
                            warning = true;
                        } else {
                            console.log(
                                `Feature potentially intersects with up ${Math.ceil(
                                    entities
                                )} entities with an average area of ${
                                    entry.average_area
                                } m²`
                            );
                        }
                    }

                    if (warning || error) {
                        swal.fire({
                            title: warning ? 'Warning' : 'Error',
                            text: warning
                                ? 'The feature potentially intersects with too many tenure areas. Do you want to continue?'
                                : 'The feature intersects with too many tenure areas. Please adjust the feature.',
                            icon: warning ? 'warning' : 'error',
                            showCancelButton: true,
                            showConfirmButton: warning ? true : false,
                            confirmButtonText: 'Yes',
                            cancelButtonText: warning ? 'No' : 'Close',
                            customClass: {
                                confirmButton: 'btn btn-primary',
                                cancelButton: 'btn btn-secondary',
                            },
                        }).then((result) => {
                            if (result.isConfirmed) {
                                this.$refs.component_map.finishDrawing();
                            } else {
                                this.$refs.component_map.setLoadingMap(false);
                            }
                        });
                    } else {
                        this.$refs.component_map.finishDrawing();
                    }
                });
            } else {
                console.log(
                    'No plausibility geometry features found for validation.'
                );
                this.$refs.component_map.finishDrawing();
            }
        },
        /**
         * Intersects the input feature with the plausibility geometry features and returns a dictionary of intersected areas.
         * @param feature - The feature to check for plausibility
         * @returns {Promise<{Object}>} - A dictionary of intersected areas in the form of {area: value, average_area: value, warning_value: value, error_value: value}
         */
        plausibilityCheckFeature: async function (feature) {
            // Store all plausibility features that intersect with the input feature
            const plausibilityFeatures = {};
            // Could have more than one plausibility feature defined, so check each one
            const featuresIntersects = this.plausibilityGeometryFeatures.filter(
                (f) => {
                    // A list of plausibility features that intersect with the input feature at this iteration step
                    const plausibilityFeature = intersects(feature, f);
                    if (plausibilityFeature.length > 0) {
                        const props = f.getProperties();
                        plausibilityFeature.map((pf) => {
                            // Set the stats properties of the plausibility feature to the intersected plausibility feature
                            pf.set('average_area', props.average_area);
                            pf.set('warning_value', props.warning_value);
                            pf.set('error_value', props.error_value);
                            pf.set(
                                'ratio_effective_area',
                                props.ratio_effective_area
                            );
                        });
                        if (!plausibilityFeatures[f.ol_uid]) {
                            plausibilityFeatures[f.ol_uid] = [];
                        }
                        plausibilityFeatures[f.ol_uid].push(
                            ...plausibilityFeature
                        );
                        return true;
                    }
                }
            );
            console.log('Features intersects', featuresIntersects);

            const areas = {};
            if (featuresIntersects.length > 0) {
                for (let [key, value] of Object.entries(plausibilityFeatures)) {
                    const props = value[0].getProperties();
                    areas[key] = {
                        average_area: props.average_area,
                        warning_value: props.warning_value,
                        error_value: props.error_value,
                        ratio_effective_area: props.ratio_effective_area,
                    };
                    areas[key]['area'] = value.reduce(
                        (accumulator, plausibilityFeature) =>
                            accumulator +
                            intersectedArea(feature, plausibilityFeature),
                        0
                    );
                }
                console.log('Total intersected areas', areas);
            }

            return areas;
        },
        prepareNewSiteAtCoordinates: function (coordinates) {
            this.$refs.occurrence_sites_datatable.prepareSiteFromCoordinates(
                coordinates
            );
        },
    },
};
</script>

<style lang="css" scoped></style>
