<template>
    <div>
        <div class="justify-content-end align-items-center mb-2">
            <div v-if="mapInfoText.length > 0" class="row">
                <div class="col-md-6">
                    <alert type="info"
                        ><strong>
                            <!-- eslint-disable-next-line vue/no-v-html -->
                            <p><span v-html="mapInfoText"></span></p></strong
                    ></alert>
                </div>
                <div class="col-md-6">
                    <div class="row" style="margin: auto">
                        <alert
                            v-if="hasErrorMessage"
                            class="mb-1 ml-1"
                            type="danger"
                            icon="exclamation-triangle-fill"
                        >
                            <span> {{ errorMessage }} </span>
                        </alert>
                    </div>
                    <div class="row" style="margin: auto">
                        <alert
                            v-if="hasModifiedFeatures"
                            class="mb-0 ml-1"
                            type="warning"
                            icon="exclamation-triangle-fill"
                        >
                            <span>
                                Adding or modifying a feature will cause any
                                existing geospatial data to be re-evaluated on
                                save and possibly be changed.
                            </span>
                        </alert>
                    </div>
                </div>
            </div>
        </div>
        <div
            :id="map_container_id"
            class="d-flex justify-content-center"
            style="position: relative"
        >
            <div :id="elem_id" class="map">
                <div
                    v-show="map"
                    class="basemap-button"
                    title="Toggle background map"
                >
                    <SvgIcon
                        id="basemap_sat"
                        name="basemap_sat"
                        @click="setBaseLayer('sat')"
                    />
                    <SvgIcon
                        id="basemap_street"
                        name="basemap_street"
                        @click="setBaseLayer('street')"
                    />
                </div>
                <div v-show="map" class="optional-layers-wrapper">
                    <div style="position: relative">
                        <div
                            class="optional-layers-button-wrapper"
                            :title="`There are ${optionalLayers.length} optional layers available}`"
                        >
                            <!-- :class="
                                    optionalLayers.length ? '' : 'disabled'
                                " -->
                            <div
                                class="optional-layers-button btn"
                                :class="isEditingALayer ? 'btn-danger' : ''"
                                @mouseover="hover = true"
                            >
                                <SvgIcon name="layers" />
                            </div>
                        </div>
                        <transition>
                            <div
                                v-show="hover"
                                class="layer_options layer_menu"
                                @mouseleave="hover = false"
                            >
                                <div id="layer-control"></div>
                            </div>
                            <!-- <div
                                v-show="hover"
                                class="layer_options layer_menu"
                                @mouseleave="hover = false"
                            >
                                <div
                                    v-for="layer in optionalLayers"
                                    :key="layer.ol_uid"
                                >
                                    <div
                                        class="row"
                                        :title="layer.values_.abstract"
                                    >
                                        <input
                                            :id="layer.ol_uid"
                                            type="checkbox"
                                            :checked="layer.values_.visible"
                                            class="layer_option col-md-1"
                                            @change="
                                                changeLayerVisibility(layer)
                                            "
                                        />
                                        <label
                                            :for="layer.ol_uid"
                                            class="layer_option col-md-6"
                                            >{{ layer.get('title') }}</label
                                        >
                                        <RangeSlider
                                            class="col-md-5"
                                            @value-changed="
                                                valueChanged($event, layer)
                                            "
                                        />
                                    </div>
                                </div>
                            </div> -->
                        </transition>
                    </div>
                    <!-- <div style="position: relative"> -->
                    <!-- <transition> -->
                    <!-- <div
                        class="optional-layers-button-wrapper"
                        :title="`There are ${optionalLayers.length} geometries available`"
                    >
                        <div
                            class="optional-layers-button btn"
                            :class="featureCount ? '' : 'disabled'"
                            @click="toggleElementVisibility('submenu-geometries-list')"
                            @mouseover="hover = true"
                        >
                            <img src="../../assets/geo-location.svg" />
                        </div>
                    </div> -->
                    <!-- </transition> -->
                    <div
                        v-if="featureCount"
                        id="submenu-geometries-list"
                        class="map-menu-submenu moved-menu-vertical"
                    >
                        <form class="layer_options form-horizontal">
                            <div
                                v-for="(
                                    features, name
                                ) in mapFeaturesGroupedAndSorted"
                                :key="`${name}-${features.length}`"
                            >
                                <a
                                    class="btn btn-secondary mb-1 w-100"
                                    role="button"
                                    data-bs-toggle="collapse"
                                    :data-bs-target="`#geometry-list-collapsible-${name}`"
                                    :href="`#geometry-list-collapsible-${name}`"
                                    aria-expanded="true"
                                    :aria-controls="`geometry-list-collapsible-${name}`"
                                >
                                    <!-- Filter for not false to also catch undefined as trueish (not all geoms have show_on_map) -->
                                    <small
                                        v-if="
                                            getLayerDefinitionByName(name)
                                                .can_hide_geometries === true
                                        "
                                        >{{ layerNameTitles[name] }} ({{
                                            filterFeaturesShowOnMap(
                                                features,
                                                false,
                                                (not = true)
                                            ).length
                                        }})
                                        <span
                                            v-if="
                                                filterFeaturesShowOnMap(
                                                    features,
                                                    false
                                                ).length
                                            "
                                        >
                                            ({{
                                                filterFeaturesShowOnMap(
                                                    features,
                                                    false
                                                ).length
                                            }}
                                            hidden)
                                        </span>
                                    </small>
                                    <small v-else
                                        >{{ layerNameTitles[name] }} ({{
                                            features.length
                                        }})</small
                                    >
                                </a>
                                <div
                                    :id="`geometry-list-collapsible-${name}`"
                                    class="collapse max-vh-50"
                                    :class="[
                                        getLayerDefinitionByName(name).collapse
                                            ? ''
                                            : 'show',
                                        features.length >= 4
                                            ? 'overflow-auto'
                                            : '',
                                    ]"
                                >
                                    <div
                                        v-for="feature in features"
                                        :key="
                                            feature.ol_uid +
                                            feature.getProperties()
                                                .original_geometry.properties
                                                .srid +
                                            feature.getProperties()
                                                .original_geometry.properties
                                                .latitude +
                                            feature.getProperties()
                                                .original_geometry.properties
                                                .longitude +
                                            feature.getProperties().show_on_map
                                        "
                                        :set="
                                            displayProperties =
                                                featureGetDisplayProperties(
                                                    feature,
                                                    getLayerByName(name)
                                                )
                                        "
                                        class="input-group input-group-sm mb-1 text-nowrap"
                                    >
                                        <!-- Select geometry-checkbox -->
                                        <div class="input-group-text">
                                            <input
                                                :id="`feature-${feature.ol_uid}-checkbox`"
                                                type="checkbox"
                                                data-bs-toggle="tooltip"
                                                data-bs-placement="top"
                                                data-bs-title="Select feature"
                                                :disabled="
                                                    getLayerDefinitionByName(
                                                        name
                                                    ).can_hide_geometries ===
                                                        true &&
                                                    feature.getProperties()
                                                        .show_on_map == false
                                                "
                                                :title="`${
                                                    selectedFeatureIds.includes(
                                                        feature.getProperties()
                                                            .id
                                                    )
                                                        ? 'Deselect'
                                                        : 'Select'
                                                } ${
                                                    feature.getProperties()
                                                        .label
                                                }`"
                                                :checked="
                                                    selectedFeatureIds.includes(
                                                        feature.getProperties()
                                                            .id
                                                    )
                                                "
                                                class="form-check-input"
                                                @change="selectFeature(feature)"
                                            />
                                        </div>
                                        <button
                                            v-if="
                                                getLayerDefinitionByName(name)
                                                    .can_hide_geometries ===
                                                    true &&
                                                feature.getProperties()
                                                    .show_on_map === false
                                            "
                                            type="button"
                                            class="btn btn-secondary me-1"
                                            data-bs-toggle="tooltip"
                                            data-bs-placement="top"
                                            data-bs-title="Zoom to feature"
                                            :title="`Toggle ${
                                                feature.getProperties()
                                                    .identifier_value
                                            } map visibility`"
                                            @click="
                                                toggleFeatureShowOnMap(
                                                    feature,
                                                    name
                                                )
                                            "
                                        >
                                            <SvgIcon
                                                class="svg-object"
                                                name="hidden"
                                            />
                                        </button>
                                        <!-- Zoom to-button and geometry type-icon -->
                                        <button
                                            v-else
                                            type="button"
                                            class="btn btn-secondary me-1"
                                            data-bs-toggle="tooltip"
                                            data-bs-placement="top"
                                            data-bs-title="Zoom to feature"
                                            :title="`Zoom to ${
                                                feature.getProperties()
                                                    .identifier_value
                                            }`"
                                            @mouseenter="
                                                toggleHidden($event.target)
                                            "
                                            @mouseleave="
                                                toggleHidden($event.target)
                                            "
                                            @click="
                                                centerOnFeature(
                                                    feature,
                                                    mapDefaultZoom
                                                )
                                            "
                                        >
                                            <SvgIcon
                                                v-if="
                                                    isMultiPointFeature(feature)
                                                "
                                                class="svg-object"
                                                name="draw-points"
                                            />
                                            <svg
                                                v-else-if="
                                                    isPointLikeFeature(feature)
                                                "
                                                class="svg-object"
                                                width="24"
                                                height="24"
                                                xmlns="http://www.w3.org/2000/svg"
                                            >
                                                <!-- A circle -->
                                                <circle
                                                    cx="12"
                                                    cy="12"
                                                    r="5"
                                                    :fill="
                                                        feature.getProperties()
                                                            .color
                                                    "
                                                    :stroke="
                                                        selectedFeatureIds.includes(
                                                            feature.getProperties()
                                                                .id
                                                        )
                                                            ? 'red'
                                                            : feature.getProperties()
                                                                  .stroke
                                                    "
                                                    stroke-width="2"
                                                />
                                            </svg>
                                            <svg
                                                v-else-if="
                                                    isMultiPolygonFeature(
                                                        feature
                                                    )
                                                "
                                                class="svg-object"
                                                width="24"
                                                height="24"
                                                xmlns="http://www.w3.org/2000/svg"
                                            >
                                                <!-- Two intersecting rectangles -->
                                                <path
                                                    d="M2 2 16 2 16 16 2 16 z"
                                                    :fill="
                                                        feature.getProperties()
                                                            .color
                                                    "
                                                    :stroke="
                                                        selectedFeatureIds.includes(
                                                            feature.getProperties()
                                                                .id
                                                        )
                                                            ? 'red'
                                                            : feature.getProperties()
                                                                  .stroke
                                                    "
                                                    stroke-width="2"
                                                />
                                                <path
                                                    d="M9 9 22 9 22 22 9 22 z"
                                                    :fill="
                                                        feature.getProperties()
                                                            .color
                                                    "
                                                    :stroke="
                                                        selectedFeatureIds.includes(
                                                            feature.getProperties()
                                                                .id
                                                        )
                                                            ? 'red'
                                                            : feature.getProperties()
                                                                  .stroke
                                                    "
                                                    stroke-width="2"
                                                />
                                            </svg>
                                            <svg
                                                v-else
                                                class="svg-object"
                                                width="24"
                                                height="24"
                                                xmlns="http://www.w3.org/2000/svg"
                                            >
                                                <!-- A rectangle -->
                                                <polygon
                                                    points="2,2 22,2 22,22 2,22"
                                                    :fill="
                                                        feature.getProperties()
                                                            .color
                                                    "
                                                    :stroke="
                                                        selectedFeatureIds.includes(
                                                            feature.getProperties()
                                                                .id
                                                        )
                                                            ? 'red'
                                                            : feature.getProperties()
                                                                  .stroke
                                                    "
                                                    stroke-width="2"
                                                />
                                            </svg>
                                            <SvgIcon
                                                class="svg-object"
                                                name="map-zoom"
                                                :hidden="true"
                                            />
                                        </button>
                                        <!-- Latitude -->
                                        <div
                                            class="form-floating flex-grow-1 input-group-text"
                                        >
                                            <input
                                                v-if="
                                                    isPointLikeFeature(feature)
                                                "
                                                :id="`feature-${feature.ol_uid}-latitude-input`"
                                                :ref="`feature-${feature.ol_uid}-latitude-input`"
                                                class="form-control min-width-90"
                                                :value="
                                                    userCoordinates(feature)[1]
                                                "
                                                placeholder="Latitude"
                                                type="number"
                                                :min="
                                                    isOriginalGeometryCrsProjected(
                                                        feature
                                                    )
                                                        ? -10e6
                                                        : gisExtentArray[1]
                                                "
                                                :max="
                                                    isOriginalGeometryCrsProjected(
                                                        feature
                                                    )
                                                        ? 0.0
                                                        : gisExtentArray[3]
                                                "
                                                :step="
                                                    isOriginalGeometryCrsProjected(
                                                        feature
                                                    )
                                                        ? 1.0
                                                        : 0.0001
                                                "
                                                data-bs-toggle="tooltip"
                                                data-bs-placement="top"
                                                data-bs-title="Enter the latitude value"
                                                :disabled="
                                                    getLayerDefinitionByName(
                                                        name
                                                    ).can_edit == false
                                                "
                                                @change="
                                                    updateUserInputGeoData(
                                                        feature
                                                    )
                                                "
                                            />
                                            <label
                                                v-if="
                                                    isPointLikeFeature(feature)
                                                "
                                                :for="`feature-${feature.ol_uid}-longitude-input`"
                                                ><span
                                                    v-if="
                                                        isOriginalGeometryCrsProjected(
                                                            feature
                                                        )
                                                    "
                                                    >Northing</span
                                                ><span v-else
                                                    >Latitude</span
                                                ></label
                                            >
                                        </div>
                                        <!-- Longitude -->
                                        <div
                                            class="form-floating flex-grow-1 input-group-text"
                                        >
                                            <input
                                                v-if="
                                                    isPointLikeFeature(feature)
                                                "
                                                :id="`feature-${feature.ol_uid}-longitude-input`"
                                                :ref="`feature-${feature.ol_uid}-longitude-input`"
                                                class="form-control min-width-90 me-1"
                                                :value="
                                                    userCoordinates(feature)[0]
                                                "
                                                placeholder="Longitude"
                                                type="number"
                                                :min="
                                                    isOriginalGeometryCrsProjected(
                                                        feature
                                                    )
                                                        ? 0.0
                                                        : gisExtentArray[0]
                                                "
                                                :max="
                                                    isOriginalGeometryCrsProjected(
                                                        feature
                                                    )
                                                        ? 40e6
                                                        : gisExtentArray[2]
                                                "
                                                :step="
                                                    isOriginalGeometryCrsProjected(
                                                        feature
                                                    )
                                                        ? 1.0
                                                        : 0.0001
                                                "
                                                data-bs-toggle="tooltip"
                                                data-bs-placement="top"
                                                data-bs-title="Enter the longitude value"
                                                :disabled="
                                                    getLayerDefinitionByName(
                                                        name
                                                    ).can_edit == false
                                                "
                                                @change="
                                                    updateUserInputGeoData(
                                                        feature
                                                    )
                                                "
                                            />
                                            <label
                                                v-if="
                                                    isPointLikeFeature(feature)
                                                "
                                                :for="`feature-${feature.ol_uid}-longitude-input`"
                                                ><span
                                                    v-if="
                                                        isOriginalGeometryCrsProjected(
                                                            feature
                                                        )
                                                    "
                                                    >Easting</span
                                                ><span v-else
                                                    >Longitude</span
                                                ></label
                                            >
                                        </div>
                                        <!-- Buffer Radius -->
                                        <div
                                            v-if="
                                                getLayerDefinitionByName(name)
                                                    ?.can_buffer
                                            "
                                            class="form-floating flex-grow-1 input-group-text"
                                        >
                                            <input
                                                :id="`feature-${feature.ol_uid}-buffer-radius-input`"
                                                :ref="`feature-${feature.ol_uid}-buffer-radius-input`"
                                                class="form-control min-width-90"
                                                :value="bufferRadius(feature)"
                                                placeholder="Buffer Radius"
                                                type="number"
                                                data-bs-toggle="tooltip"
                                                data-bs-placement="top"
                                                data-bs-title="Enter a buffer radius value"
                                                :disabled="
                                                    getLayerDefinitionByName(
                                                        name
                                                    ).can_edit == false
                                                "
                                                @change="
                                                    updateUserInputBufferRadius(
                                                        feature,
                                                        $event.target
                                                            .valueAsNumber
                                                    )
                                                "
                                            />
                                            <label
                                                :for="`feature-${feature.ol_uid}-buffer-radius-input`"
                                                >Buffer Radius [m]</label
                                            >
                                        </div>
                                        <!-- CRS Dropdown -->
                                        <div
                                            class="input-group-text form-floating flex-grow-1 min-width-210 justify-content-end"
                                        >
                                            <SelectFilter
                                                :id="`feature-${feature.ol_uid}-crs-select`"
                                                :ref="`feature-${feature.ol_uid}-crs-select`"
                                                :title="`Feature ${
                                                    feature.getProperties().id
                                                }`"
                                                :show-title="false"
                                                placeholder="Coordinate Reference System"
                                                :options="
                                                    coordinateReferenceSystemsForSelectFilter
                                                "
                                                :pre-selected-filter-item="
                                                    feature.getProperties()
                                                        .original_geometry
                                                        .properties.srid ||
                                                    mapSrid
                                                "
                                                classes="min-width-210"
                                                :disabled="
                                                    getLayerDefinitionByName(
                                                        name
                                                    ).can_edit == false
                                                "
                                                @option:selected="
                                                    (selected) => {
                                                        updateUserInputGeoData(
                                                            feature,
                                                            selected.value
                                                        );
                                                    }
                                                "
                                                @search="
                                                    (...args) =>
                                                        $emit(
                                                            'crs-select-search',
                                                            ...args
                                                        )
                                                "
                                            />
                                        </div>
                                    </div>
                                </div>
                            </div>

                            <a
                                class="btn btn-secondary mb-1 w-100"
                                role="button"
                                data-bs-toggle="collapse"
                                :data-bs-target="`#geometry-list-collapsible-edit`"
                                :href="`#geometry-list-collapsible-edit`"
                                aria-expanded="true"
                                :aria-controls="`geometry-list-collapsible-edit`"
                            >
                                <small>Edit</small>
                            </a>
                            <div
                                :id="`geometry-list-collapsible-edit`"
                                class="collapse overflow-auto max-vh-50"
                            >
                                <!-- A new-point Button -->
                                <div
                                    class="input-group-text justify-content-end"
                                >
                                    <button
                                        type="button"
                                        class="btn btn-primary btn-sm"
                                        :class="
                                            pointFeaturesSupported
                                                ? ''
                                                : 'disabled'
                                        "
                                        data-bs-toggle="tooltip"
                                        data-bs-placement="top"
                                        data-bs-title="Add a new point"
                                        title="Add a new point"
                                        @click="addNewPoint(-31.0, 116.0)"
                                    >
                                        <i class="fa-solid fa-circle-plus"></i>
                                    </button>
                                </div>
                                <!-- A copy-selected Button -->
                                <div
                                    class="input-group-text justify-content-end"
                                    title="Select geometries to copy"
                                >
                                    <button
                                        type="button"
                                        class="btn btn-primary btn-sm"
                                        :class="
                                            selectedFeatureIds.length
                                                ? ''
                                                : 'disabled'
                                        "
                                        data-bs-toggle="tooltip"
                                        data-bs-placement="top"
                                        :data-bs-title="`Copy selected to ${
                                            layerNameTitles[
                                                getDefaultQueryLayerName()
                                            ]
                                        } layer`"
                                        :title="`Copy selected to ${
                                            layerNameTitles[
                                                getDefaultQueryLayerName()
                                            ]
                                        } layer`"
                                        @click="
                                            copySelectedToLayer(
                                                getDefaultQueryLayerName()
                                            )
                                        "
                                    >
                                        <i class="fa-solid fa-copy"></i>
                                    </button>
                                </div>
                            </div>
                        </form>
                        <!-- </transition> -->
                    </div>
                    <div
                        class="optional-layers-button-wrapper"
                        :title="
                            featureCount
                                ? `Open table for ${featureCount} geometries`
                                : 'No geometries available'
                        "
                    >
                        <div
                            class="optional-layers-button btn"
                            :class="featureCount ? '' : 'disabled'"
                            @click="
                                toggleElementVisibility(
                                    'submenu-geometries-list',
                                    $event.target
                                )
                            "
                        >
                            <SvgIcon name="geo-location" />
                        </div>
                    </div>
                    <!-- </div> -->
                    <!-- Toggle measure tool between active and not active -->
                    <div class="optional-layers-button-wrapper">
                        <div
                            :title="
                                mode == 'measure'
                                    ? 'Deactivate measure tool'
                                    : 'Measure distances on the map'
                            "
                            class="btn"
                            :class="[
                                mode == 'measure'
                                    ? 'optional-layers-button-active'
                                    : 'optional-layers-button',
                            ]"
                            @click="callSetMode('measure')"
                        >
                            <SvgIcon class="svg-object" name="ruler" />
                        </div>
                    </div>

                    <div
                        id="submenu-draw"
                        class="map-menu-submenu moved-menu-vertical"
                    >
                        <div class="scaled-button">
                            <div
                                class="submenu-button-wrapper"
                                :title="
                                    polygonFeaturesSupported
                                        ? ''
                                        : 'The map does not support polygon features'
                                "
                            >
                                <div
                                    :title="
                                        mode == 'draw' && subMode == 'Polygon'
                                            ? 'Deactivate draw tool'
                                            : 'Draw a new polygon feature'
                                    "
                                    class="btn optional-layers-button"
                                    :class="[
                                        mode == 'draw' && subMode == 'Polygon'
                                            ? 'optional-layers-button-active'
                                            : 'optional-layers-button',
                                        polygonFeaturesSupported
                                            ? ''
                                            : 'disabled',
                                    ]"
                                    @click="callSetMode('draw', 'Polygon')"
                                >
                                    <SvgIcon
                                        class="svg-object"
                                        name="draw-polygon"
                                    />
                                </div>
                            </div>
                            <div
                                class="submenu-button-wrapper"
                                :title="
                                    pointFeaturesSupported
                                        ? ''
                                        : 'The map does not support point features'
                                "
                            >
                                <div
                                    :title="
                                        mode == 'draw' && subMode == 'Point'
                                            ? 'Deactivate draw tool'
                                            : 'Draw new point features or edit selected points or polygon vertices'
                                    "
                                    class="btn optional-layers-button"
                                    :class="[
                                        mode == 'draw' && subMode == 'Point'
                                            ? 'optional-layers-button-active'
                                            : 'optional-layers-button',
                                        pointFeaturesSupported
                                            ? ''
                                            : 'disabled',
                                    ]"
                                    @click="callSetMode('draw', 'Point')"
                                >
                                    <SvgIcon
                                        class="svg-object"
                                        name="draw-points"
                                    />
                                </div>
                            </div>
                        </div>
                    </div>

                    <div
                        v-if="drawable"
                        class="optional-layers-button-wrapper"
                        title="To draw features first toggle on layer editing"
                    >
                        <div
                            :title="
                                mode == 'draw'
                                    ? 'Drawing mode active'
                                    : 'Select a drawing mode'
                            "
                            class="btn optional-layers-button"
                            @click="
                                mode == 'draw'
                                    ? toggleElementVisibility('submenu-draw')
                                    : toggleElementVisibility(
                                          'submenu-draw',
                                          $event.target
                                      )
                            "
                        >
                            <SvgIcon class="svg-object" name="pen-icon" />
                        </div>
                    </div>
                    <div
                        v-if="editable"
                        class="optional-layers-button-wrapper"
                        :title="
                            isEditingALayer
                                ? 'No geometries to transform'
                                : 'To transform features first toggle on layer editing'
                        "
                    >
                        <div
                            :title="
                                mode == 'transform'
                                    ? 'Deactivate transform tool'
                                    : 'Transform an existing feature'
                            "
                            class="btn"
                            :class="[
                                mode == 'transform'
                                    ? 'optional-layers-button-active'
                                    : 'optional-layers-button',
                                drawable && featureCount && isEditingALayer
                                    ? ''
                                    : 'disabled',
                            ]"
                            @click="callSetMode('transform')"
                        >
                            <SvgIcon
                                class="svg-object"
                                name="transform-polygon"
                            />
                        </div>
                    </div>
                    <div
                        class="optional-layers-button-wrapper"
                        title="Zoom map to layer(s)"
                    >
                        <div
                            title="Zoom map to layer(s)"
                            class="optional-layers-button btn"
                            :class="featureCount ? '' : 'disabled'"
                            @click="displayAllFeatures()"
                        >
                            <SvgIcon name="map-zoom-to-layer" />
                        </div>
                    </div>
                    <div
                        class="optional-layers-button-wrapper"
                        title="Select features to download as GeoJSON"
                    >
                        <div
                            class="optional-layers-button btn"
                            :class="selectedFeatureIds.length ? '' : 'disabled'"
                            :title="`Download selected features as GeoJSON`"
                            @click="geoJsonButtonClicked"
                        >
                            <SvgIcon name="download" />
                            <span
                                v-if="selectedFeatureIds.length"
                                id="selectedFeatureCountWarning"
                                class="badge badge-warning"
                                >{{ selectedFeatureIds.length }}</span
                            >
                        </div>
                    </div>

                    <div
                        id="submenu-spatial-operations"
                        class="map-menu-submenu"
                    >
                        <form class="layer_options form-horizontal">
                            <div
                                class="input-group input-group-sm mb-1 text-nowrap"
                            >
                                <!-- Spatial Operations Dropdown -->
                                <div
                                    class="input-group-text form-floating flex-grow-1"
                                >
                                    <SelectFilter
                                        id="features-spatial-operation-select"
                                        ref="features-spatial-operation-select"
                                        :disabled="processingFeatures"
                                        :title="`Run a ${selectedSpatialOperation} spatial operation on ${
                                            selectedFeatureIds.length
                                        } selected feature${
                                            selectedFeatureIds.length > 1
                                                ? 's'
                                                : ''
                                        }`"
                                        :show-title="false"
                                        placeholder="Spatial Operation"
                                        :options="
                                            spatialOperationsAvailable.map(
                                                (op) => {
                                                    return {
                                                        id: op.id,
                                                        name: op.name,
                                                    };
                                                }
                                            )
                                        "
                                        :pre-selected-filter-item="
                                            selectedSpatialOperation
                                        "
                                        classes="min-width-150"
                                        @option:selected="
                                            (selected) => {
                                                selectedSpatialOperation =
                                                    selected.value;
                                            }
                                        "
                                        @option:deselected="
                                            () => {
                                                selectedSpatialOperation = null;
                                            }
                                        "
                                    />
                                </div>
                                <!-- Related input field like buffer distance -->
                                <div
                                    v-if="
                                        spatialOperationsAvailable.find(
                                            (op) =>
                                                op.id ==
                                                selectedSpatialOperation
                                        )?.number_params > 0
                                    "
                                    class="form-floating flex-grow-1 input-group-text"
                                >
                                    <input
                                        id="spatial-operation-parameter-input"
                                        ref="spatial-operation-parameter-input"
                                        class="form-control min-width-90 me-1"
                                        :disabled="processingFeatures"
                                        :value="spatialOperationParameters[0]"
                                        :placeholder="parameterInputLabel"
                                        type="number"
                                        min="0"
                                        :step="unitDependentStep"
                                        data-bs-toggle="tooltip"
                                        data-bs-placement="top"
                                        data-bs-title="Enter a value"
                                        @change="
                                            spatialOperationParameters[0] =
                                                $event.target.value
                                        "
                                    />
                                    <label
                                        for="spatial-operation-parameter-input"
                                        >{{ parameterInputLabel }}</label
                                    >
                                </div>

                                <!-- Unit Dropdown -->
                                <div
                                    v-if="
                                        spatialOperationsAvailable.find(
                                            (op) =>
                                                op.id ==
                                                selectedSpatialOperation
                                        )?.number_params > 0
                                    "
                                    class="input-group-text form-floating flex-grow-1"
                                >
                                    <SelectFilter
                                        id="features-unit-select"
                                        ref="features-unit-select"
                                        :disabled="processingFeatures"
                                        :title="`Run a ${selectedSpatialOperation} spatial operation on ${
                                            selectedFeatureIds.length
                                        } selected feature${
                                            selectedFeatureIds.length > 1
                                                ? 's'
                                                : ''
                                        }`"
                                        :show-title="false"
                                        placeholder="Select a unit"
                                        :options="
                                            spatialUnitsAvailable.map((op) => {
                                                return {
                                                    id: op.id,
                                                    name: op.name,
                                                };
                                            })
                                        "
                                        :pre-selected-filter-item="
                                            selectedSpatialUnit
                                        "
                                        classes="min-width-150"
                                        @option:selected="
                                            (selected) => {
                                                selectedSpatialUnit =
                                                    selected.value;
                                            }
                                        "
                                        @option:deselected="
                                            () => {
                                                selectedSpatialUnit = null;
                                            }
                                        "
                                    />
                                </div>
                                <!-- Button to run the operation -->
                                <div
                                    v-if="selectedSpatialOperation"
                                    class="form-floating flex-grow-1 input-group-text"
                                >
                                    <div class="scaled-button">
                                        <div
                                            class="submenu-button-wrapper"
                                            :title="
                                                selectedFeatureIds.length
                                                    ? 'Process selected features'
                                                    : 'Select feature(s) to process'
                                            "
                                        >
                                            <div
                                                :title="`Process ${
                                                    selectedFeatureIds.length
                                                } selected feature${
                                                    selectedFeatureIds.length >
                                                    1
                                                        ? 's'
                                                        : ''
                                                }`"
                                                class="btn optional-layers-button"
                                                :class="[
                                                    selectedFeatureIds.length ==
                                                        0 || processingFeatures
                                                        ? 'disabled'
                                                        : 'btn-warning',
                                                    navbarButtonsDisabled
                                                        ? 'disabled'
                                                        : '',
                                                ]"
                                                @click="
                                                    processFeatures(
                                                        selectedSpatialOperation,
                                                        spatialOperationParameters,
                                                        selectedSpatialUnit
                                                    )
                                                "
                                            >
                                                <SvgIcon
                                                    :name="
                                                        spatialOperationsAvailable.find(
                                                            (op) =>
                                                                op.id ==
                                                                selectedSpatialOperation
                                                        ).icon
                                                    "
                                                />
                                            </div>
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </form>
                    </div>

                    <!-- Spatially process Features -->
                    <div
                        v-if="
                            defaultProcessedGeometryLayerName &&
                            spatialOperationsAvailable.length
                        "
                        class="optional-layers-button-wrapper"
                        :title="
                            featureCount
                                ? 'Select feature(s) to process'
                                : 'No features to process'
                        "
                    >
                        <div
                            class="optional-layers-button btn"
                            title="Select a method to process selected features"
                            @click="
                                toggleElementVisibility(
                                    'submenu-spatial-operations',
                                    $event.target
                                )
                            "
                        >
                            <SvgIcon name="spatial-processing" />
                            <span
                                v-if="selectedFeatureIds.length"
                                id="selectedFeatureCountWarning"
                                class="badge badge-warning"
                                >{{ selectedFeatureIds.length }}</span
                            >
                        </div>
                    </div>

                    <!-- Delete features -->
                    <div
                        v-if="editable"
                        class="optional-layers-button-wrapper"
                        :title="
                            featureCount
                                ? 'Select feature(s) to delete'
                                : 'No features to delete'
                        "
                    >
                        <div
                            class="optional-layers-button btn"
                            :class="[
                                selectedFeatureIds.length == 0
                                    ? 'disabled'
                                    : 'btn-danger',
                                navbarButtonsDisabled ? 'disabled' : '',
                            ]"
                            :title="`Delete ${
                                selectedFeatureIds.length
                            } selected feature${
                                selectedFeatureIds.length > 1 ? 's' : ''
                            }`"
                            @click="removeModelFeatures()"
                        >
                            <SvgIcon name="trash-bin" />
                            <span
                                v-if="selectedFeatureIds.length"
                                id="selectedFeatureCountDanger"
                                class="badge badge-warning"
                                >{{ selectedFeatureIds.length }}</span
                            >
                        </div>
                    </div>
                    <div
                        v-if="canUndoAction || canUndoDrawnVertex"
                        class="optional-layers-button-wrapper"
                        title="Undo last action"
                    >
                        <div
                            class="optional-layers-button btn"
                            :class="[
                                (mode !== 'draw' && hasUndo) ||
                                (mode === 'draw' && canUndoDrawnVertex)
                                    ? ''
                                    : 'disabled',
                                navbarButtonsDisabled ? 'disabled' : '',
                            ]"
                            :title="
                                'Undo ' +
                                (canUndoDrawnVertex
                                    ? 'last point'
                                    : undoRedoStackTopInteractionName('undo'))
                            "
                            @click="undo()"
                        >
                            <SvgIcon name="map-undo" />
                        </div>
                    </div>
                    <div
                        v-if="canRedoAction || canRedoDrawnVertex"
                        class="optional-layers-button-wrapper"
                        title="Redo last action"
                    >
                        <div
                            class="optional-layers-button btn"
                            :class="[
                                (mode !== 'draw' && hasRedo) ||
                                (mode === 'draw' && canRedoDrawnVertex)
                                    ? ''
                                    : 'disabled',
                                navbarButtonsDisabled ? 'disabled' : '',
                            ]"
                            :title="
                                'Redo ' +
                                (canRedoDrawnVertex
                                    ? 'last point'
                                    : undoRedoStackTopInteractionName('redo'))
                            "
                            @click="redo()"
                        >
                            <SvgIcon name="map-redo" />
                        </div>
                    </div>
                    <div
                        v-if="optionalLayersActive"
                        class="optional-layers-button-wrapper"
                    >
                        <div
                            :title="
                                mode == 'info'
                                    ? 'Deactivate info tool'
                                    : 'Click a data set feature for more information'
                            "
                            class="btn"
                            :class="[
                                mode == 'info'
                                    ? 'optional-layers-button-active'
                                    : 'optional-layers-button',
                            ]"
                            @click="callSetMode('info')"
                        >
                            <SvgIcon name="info-query" />
                        </div>
                    </div>
                </div>

                <!-- Flexbox container for aligning the toasts -->
                <div
                    aria-live="polite"
                    aria-atomic="true"
                    class="toast-container d-flex justify-content-center align-items-center w-100 h-100 top-0 start-0"
                >
                    <div
                        id="featureToast"
                        :class="[
                            cursorInLeftHalfOfMap
                                ? 'positionToRight'
                                : 'positionToLeft',
                            cursorInBottomHalfOfMap
                                ? 'positionToTop'
                                : 'positionToBottom',
                        ]"
                        class="toast"
                        aria-live="assertive"
                        aria-atomic="true"
                        style="z-index: 9999"
                        @mouseenter="showToastCloseButton = true"
                        @mouseleave="showToastCloseButton = false"
                    >
                        <template v-if="selectedModel && mode === 'layer'">
                            <div class="toast-header">
                                <img src="" class="rounded me-2" alt="" />
                                <strong class="me-auto">
                                    {{ selectedModel.Label
                                    }}<span
                                        v-if="
                                            selectedModel[
                                                'Identification Number'
                                            ]
                                        "
                                        >:</span
                                    >
                                    {{ selectedModel['Identification Number'] }}
                                </strong>
                                <button
                                    v-show="showToastCloseButton"
                                    type="button"
                                    class="btn-close"
                                    data-bs-dismiss="toast"
                                    aria-label="Close"
                                ></button>
                            </div>
                            <div class="toast-body">
                                <table class="table table-sm">
                                    <tbody
                                        v-for="property in Object.keys(
                                            selectedModel
                                        )"
                                        :key="`${property} - ${selectedModel[property]}`"
                                    >
                                        <tr
                                            v-if="
                                                !['Label', 'label'].includes(
                                                    property
                                                )
                                            "
                                        >
                                            <th scope="row">{{ property }}</th>
                                            <td>
                                                {{ selectedModel[property] }}
                                            </td>
                                        </tr>
                                    </tbody>
                                </table>
                            </div>
                        </template>
                    </div>
                </div>

                <div
                    v-if="isDirty"
                    id="unsaved-changes-toast"
                    class="toast align-items-center d-inline-block text-bg-warning border-0 w-auto show position-absolute bottom-0 start-50 translate-middle-x mb-2 ms-2"
                    style="z-index: 100"
                    role="alert"
                    aria-live="assertive"
                    aria-atomic="true"
                >
                    <div class="d-flex">
                        <div
                            class="toast-body d-flex align-items-center text-warning m-0 p-0 mx-2"
                        >
                            <i
                                class="bi bi-exclamation-circle-fill text-warning me-2"
                            ></i>
                            Unsaved changes
                        </div>
                    </div>
                </div>

                <!-- Overlay popup bubble when clicking a DBCA layer feature -->
                <div
                    v-show="map"
                    id="popup"
                    ref="popup"
                    class="ol-popup ol-selectable overlay-feature-popup"
                >
                    <template v-if="overlayFeatureInfo">
                        <div class="toast-header">
                            <strong class="me-auto">{{
                                overlayFeatureInfo.featureId
                            }}</strong>
                            <button
                                type="button"
                                role="button"
                                class="btn btn-sm text-nowrap ol-popup-closer"
                                aria-label="Close Overlay"
                                @click="overlay(undefined, undefined)"
                            >
                                <span style="font-size: smaller"
                                    ><i
                                        class="fa-fw fa-regular fa-window-close"
                                    ></i>
                                    Close</span
                                >
                            </button>
                        </div>
                        <div id="popup-content toast-body">
                            <div
                                class="table-responsive overflow-scroll"
                                style="height: 250px; width: 350px"
                            >
                                <table
                                    style="width: 100%; z-index: 9999"
                                    class="table table-sm"
                                >
                                    <thead class="table-light">
                                        <tr>
                                            <th scope="col">Property</th>
                                            <th scope="col">Value</th>
                                        </tr>
                                    </thead>
                                    <tbody
                                        v-for="[
                                            property,
                                            value,
                                        ] in Object.entries(overlayFeatureInfo)"
                                        :key="property + value"
                                    >
                                        <tr v-if="property != 'geometry'">
                                            <td scope="row" class="py-0">
                                                <small class="fw-bold">{{
                                                    property
                                                }}</small>
                                            </td>
                                            <td class="py-0">
                                                <small>
                                                    {{
                                                        overlayFeatureInfo[
                                                            property
                                                        ]
                                                            ? overlayFeatureInfo[
                                                                  property
                                                              ]
                                                            : 'No value'
                                                    }}
                                                </small>
                                            </td>
                                        </tr>
                                    </tbody>
                                </table>
                            </div>
                        </div>
                    </template>
                </div>
                <div v-show="loadingMap" id="map-spinner" class="text-primary">
                    <div
                        class="spinner-border text-primary"
                        style="width: 3rem; height: 3rem"
                        role="status"
                    >
                        <span class="visually-hidden">Loading...</span>
                    </div>
                </div>
                <!-- <BootstrapSpinner
                    v-if="
                        redirectingToModelDetails ||
                        queryingGeoserver ||
                        fetchingProposals ||
                        loadingMap
                    "
                    id="map-spinner"
                    class="text-primary"
                /> -->
            </div>
            <div id="coords"></div>
            <!-- <BootstrapSpinner v-if="!proposals" class="text-primary" /> -->
        </div>
        <div v-if="debug" class="row">
            <div class="col-sm-6">Undo Stack:</div>
            <div class="col-sm-6">Redo Stack:</div>
            <div class="col-sm-6">
                <div v-for="(item, idx) in undoStack" :key="idx">
                    <div>{{ idx }} {{ item.type }}</div>
                </div>
            </div>
            <div class="col-sm-6">
                <div v-for="(item, idx) in redoStack" :key="idx">
                    <div>{{ idx }} {{ item.type }}</div>
                </div>
            </div>
        </div>
        <!-- If no context provided, e.g. no proposal or cp, don't allow for shapefile upload -->
        <div v-if="context" class="row shapefile-row">
            <div v-if="!fileUploadDisabled" class="col-sm-6 border p-2">
                <div class="row mb-2">
                    <div class="col">
                        <label for="shapefile_document" class="fw-bold"
                            >Upload Shapefile or archive(s) containing
                            shapefiles
                            <span>({{ archiveTypesAllowed.join(', ') }})</span>
                        </label>
                    </div>
                    <div class="col">
                        <FileField
                            id="shapefile_document_document"
                            ref="shapefile_document"
                            :readonly="fileUploadDisabled"
                            name="shapefile_document"
                            :multiple="true"
                            :is-repeatable="true"
                            :document-action-url="shapefileDocumentUrl"
                            :replace_button_by_text="true"
                            :file-types="
                                shapefileTypesAllowed
                                    .concat(archiveTypesAllowed)
                                    .join(', ')
                            "
                            :text_string="`Attach Files ${shapefileTypesAllowed.join(
                                ', '
                            )} or ${archiveTypesAllowed.join(', ')}`"
                            @update-parent="shapeFilesUpdated"
                        />
                    </div>
                </div>
                <div
                    v-if="
                        !uploadedFileTypes.includes('.zip') &&
                        !uploadedFileTypes.includes('.prj')
                    "
                    class="row"
                >
                    <div class="col">
                        <alert
                            >If you do not upload a .prj file, we will use
                            <a
                                href="https://en.wikipedia.org/wiki/World_Geodetic_System#WGS84"
                                target="_blank"
                                >WGS 84</a
                            >
                            / 'EPSG:4326'
                        </alert>
                    </div>
                </div>
                <div v-if="hasUploadedShapefiles" class="row">
                    <div class="col">
                        <button
                            v-if="isValidating"
                            type="button"
                            disabled
                            class="btn btn-primary"
                        >
                            <div class="row">
                                <div
                                    class="col-sm-12 text-nowrap text-truncate"
                                >
                                    <span
                                        class="spinner-border spinner-border-sm"
                                        role="status"
                                        aria-hidden="true"
                                    ></span>
                                    <span class="visually-hidden"
                                        >Loading...</span
                                    >
                                </div>
                            </div>
                        </button>
                        <button
                            v-else
                            type="button"
                            class="btn btn-primary"
                            :disabled="
                                !uploadedFilesComplete || fileUploadDisabled
                            "
                            :class="uploadedFilesComplete ? '' : 'disabled'"
                            @click="validate_map_docs"
                        >
                            <div class="row">
                                <div
                                    class="col-sm-12 text-nowrap text-truncate"
                                >
                                    Process Files
                                </div>
                            </div>
                        </button>
                    </div>
                </div>
            </div>
            <div class="col-sm-6">
                <div id="legend_title"></div>
                <div id="legend">
                    <img src="" />
                </div>
            </div>
        </div>
    </div>
</template>

<script>
import { v4 as uuid } from 'uuid';
import { api_endpoints, helpers } from '@/utils/hooks';

import { toRaw, isProxy } from 'vue';
import 'ol/ol.css';
import alert from '@vue-utils/alert.vue';
import Map from 'ol/Map';
import View from 'ol/View';
import TileLayer from 'ol/layer/Tile';
import { Draw, Select, Snap } from 'ol/interaction';
import ModifyFeature from 'ol-ext/interaction/ModifyFeature';
import UndoRedo from 'ol-ext/interaction/UndoRedo';
import Transform from 'ol-ext/interaction/Transform';
import LayerSwitcher from 'ol-ext/control/LayerSwitcher';
import Feature from 'ol/Feature';
import VectorLayer from 'ol/layer/Vector';
import VectorSource from 'ol/source/Vector';
import LayerGroup from 'ol/layer/Group';
import Collection from 'ol/Collection';
import { Circle as CircleStyle, Fill, Stroke, Style, Icon } from 'ol/style';
import { FullScreen as FullScreenControl } from 'ol/control';
import { LineString, Point, MultiPoint, Polygon, MultiPolygon } from 'ol/geom';
import { fromExtent } from 'ol/geom/Polygon';
import { getArea } from 'ol/sphere.js';
import { transform as transformCoordinates } from 'ol/proj';
import { createStringXY } from 'ol/coordinate.js';
import { defaults as defaultControls } from 'ol/control/defaults.js';
import GeoJSON from 'ol/format/GeoJSON';
import Overlay from 'ol/Overlay.js';
import DragAndDrop from 'ol/interaction/DragAndDrop.js';
import DragBox from 'ol/interaction/DragBox.js';
import ScaleLine from 'ol/control/ScaleLine.js';
import MousePosition from 'ol/control/MousePosition.js';
import { platformModifierKeyOnly } from 'ol/events/condition.js';
import MeasureStyles, { formatLength } from '@/components/common/measure.js';
import FileField from '@/components/forms/filefield_immediate.vue';
import {
    fetchGISExtent,
    fetchTileLayers,
    fetchProposals,
    set_mode,
    layerAtEventPixel,
    queryLayerAtPoint,
} from '@/components/common/map_functions.js';
import shp, { combine, parseShp, parseDbf } from 'shpjs';
import SelectFilter from '@/components/common/SelectFilter.vue';
import assets from '@/utils/assets.js';
import SvgIcon from '@/utils/vue/svg-icon.vue';

export default {
    name: 'MapComponent',
    components: {
        FileField,
        alert,
        SelectFilter,
        SvgIcon,
    },
    props: {
        level: {
            type: String,
            required: true,
            validator: function (val) {
                let options = ['internal', 'referral', 'external'];
                return options.indexOf(val) != -1 ? true : false;
            },
        },
        /**
         * The context of the map. This is used to determine which layers to show on the map.
         * The context should be a model object, e.g. a Proposal, Application, etc.
         * Used to allocate ids, labels, etc to new features
         */
        context: {
            type: Object,
            required: false,
            default: null,
        },
        /**
         * A geojson feature collection of features (possibly related to the context) to display on the map.
         */
        featureCollection: {
            type: Object,
            required: false,
            default() {
                return { features: [], type: 'FeatureCollection' };
            },
            validator: function (val) {
                return val.type == 'FeatureCollection' ? true : false;
            },
        },
        /**
         * General color definitions for the features to be used when styling by `assessor`
         * @values unknown, draw, applicant, assessor
         */
        featureColors: {
            type: Object,
            required: false,
            default: () => {
                return {
                    unknown: '#9999', // greyish
                    draw: '#00FFFF', // cyan
                    applicant: '#00FF0077',
                    assessor: '#0000FF77',
                };
            },
            validator: function (val) {
                let options = ['unknown', 'draw', 'applicant', 'assessor'];
                Object.keys(val).forEach((key) => {
                    if (!options.includes(key.toLowerCase())) {
                        console.error('Invalid feature color key: ' + key);
                        return false;
                    }
                    // Invalid color values will evaluate to an empty string
                    let test = new Option().style;
                    test.color = val[key];
                    if (test.color === '') {
                        console.error(
                            `Invalid ${key} color value: ${val[key]}`
                        );
                        return false;
                    }
                });
                return true;
            },
        },
        /**
         * Point color definitions for the features to be used when styling by `assessor`
         * @values unknown, draw, applicant, assessor
         */
        pointFeatureColors: {
            type: Object,
            required: false,
            default: () => {
                return {
                    unknown: '#9999', // greyish
                    draw: '#FFFFAA', // cyan
                    applicant: '#00FF00',
                    assessor: '#0000FF',
                };
            },
            validator: function (val) {
                let options = ['unknown', 'draw', 'applicant', 'assessor'];
                Object.keys(val).forEach((key) => {
                    if (!options.includes(key.toLowerCase())) {
                        console.error('Invalid feature color key: ' + key);
                        return false;
                    }
                    // Invalid color values will evaluate to an empty string
                    let test = new Option().style;
                    test.color = val[key];
                    if (test.color === '') {
                        console.error(
                            `Invalid ${key} color value: ${val[key]}`
                        );
                        return false;
                    }
                });
                return true;
            },
        },
        /**
         * A dictionary of query parameters to pass to the WFS geoserver
         * The parent component needs to add the `cql_filter` parameter to filter the features by a spatial opration
         */
        owsQuery: {
            type: Object,
            required: false,
            default: () => {
                return {
                    // typeName
                    'kaartdijin-boodja-private:CPT_CADASTRE_SCDB': {
                        version: '2.0.0', // WFS version
                        srsName: 'EPSG:4326',
                        propertyName: 'SHAPE', // Default to query for feature geometries only
                        geometry: 'SHAPE', // Geometry name (not `the_geom`)
                        propertyNameForInfoTitle: null,
                    },
                    'kaartdijin-boodja-public:CPT_DBCA_REGIONS': {
                        version: '2.0.0', // WFS version
                        srsName: 'EPSG:4326',
                        propertyName: 'SHAPE', // Default to query for feature geometries only
                        geometry: 'SHAPE', // Geometry name (not `the_geom`)
                        propertyNameForInfoTitle: 'DRG_REGION_NAME',
                    },
                    'kaartdijin-boodja-public:CPT_DBCA_DISTRICTS': {
                        version: '2.0.0', // WFS version
                        srsName: 'EPSG:4326',
                        propertyName: 'SHAPE', // Default to query for feature geometries only
                        geometry: 'SHAPE', // Geometry name (not `the_geom`)
                        propertyNameForInfoTitle: 'DDT_DISTRICT_NAME',
                    },
                };
            },
        },
        /**
         * Whether the map supports drawing of polygons
         */
        polygonFeaturesSupported: {
            type: Boolean,
            required: false,
            default: true,
        },
        /**
         * Whether the map supports drawing of points
         */
        pointFeaturesSupported: {
            type: Boolean,
            required: false,
            default: true,
        },
        /**
         * Number of points that can be drawn on the map
         */
        pointLimit: {
            type: Number,
            required: false,
        },
        /**
         * Whether to enable drawing of new features
         */
        drawable: {
            type: Boolean,
            required: false,
            default: false,
        },
        /**
         * Whether to enable editing of existing features (e.g. select for deletion or transformation)
         */
        editable: {
            type: Boolean,
            required: false,
            default: false,
        },
        /**
         * A text that explains what is expected on the map
         */
        mapInfoText: {
            type: String,
            required: false,
            default: '',
        },
        /**
         * Whether to refresh the map when the component is mounted
         * Defaults to false, as it might be advisable to load and refresh the map only when it is needed
         * E.g. when the map tab is selected
         */
        refreshMapOnMounted: {
            type: Boolean,
            required: false,
            default: false,
        },
        zoomToFeaturesOnLoad: {
            type: Boolean,
            required: false,
            default: true,
        },
        /**
         * Tolerance for considering the pointer close enough to a segment or vertex for editing
         * See: https://openlayers.org/en/latest/apidoc/module-ol_interaction_Modify-Modify.html
         */
        pixelTolerance: {
            type: Number,
            required: false,
            default: 5,
        },
        /**
         * Consider features within some distance of a provided pixel
         * See: https://openlayers.org/en/latest/examples/hit-tolerance.html
         */
        hitTolerance: {
            type: Number,
            required: false,
            default: 4,
        },
        /**
         * The maximum length of the undo/redo stacks
         */
        undoStackMaxLength: {
            type: Number,
            required: false,
            default: 0, // 0 means no limit
        },
        navbarButtonsDisabled: {
            type: Boolean,
            required: false,
            default: false,
        },
        /**
         * Whether upload of shapefiles, drag&drop etc is diabled
         */
        fileUploadDisabled: {
            type: Boolean,
            required: false,
            default: false,
        },
        coordinateReferenceSystems: {
            type: Array,
            required: false,
            default: () => {
                return [{ id: 4326, value: 'EPSG:4326 - WGS 84' }];
            },
        },
        mapSrid: {
            type: Number,
            required: false,
            default: 4326,
        },
        spatialOperationsAllowed: {
            type: Array,
            required: false,
            default: function () {
                return ['__all__'];
            },
        },
        tileLayerApiUrl: {
            type: String,
            required: false,
            default: null,
        },
        proposalApiUrl: {
            type: String,
            required: false,
            default: null,
        },
        /**
         * The default layer definition for the query layer
         * This layer is always available and it will be used to add new features to
         * even if no default-layer is specified.
         * On the contrary, specifying no processed-layer disables the processing of features
         */
        queryLayerDefinition: {
            type: Object,
            required: false,
            default: function () {
                return {
                    name: 'query_layer', // The name of the layer by which is determined to add proposalId's proposals to
                    title: 'Query Layer',
                    default: true, // The default layer where in most cases features are added to
                    processed: true, // The layer where processed geometries are added to
                    can_edit: true,
                    can_buffer: true, // Whether features may be used to create buffer geometries
                    can_hide_geometries: false, // Whether geometries may selectively be shown or hidden on the map
                    api_url: null, // The API endpoint to fetch features from
                    ids: [], //Ids of proposals to be fetched by the map component and displayed on the map.
                    //  Negative values fetch no proposals
                    //  Positive values fetch proposals with those ids
                    //  Empty list `[]` fetches all proposals
                    handler: null, // A callback function to invoke on fetched features
                    geometry_name: 'geometry', // The name of the geometry field in the model. If not provided, the object itself is treated as the geometry
                    identifier_name: null, // The name of the identifier field in the model
                    z_index: 0, // The z-index of the layer, can be used to order layers
                    collapse: false, // Whether the layer is collapsed by default
                    property_display_map: {}, // A dictionary to map property names to display names, e.g. for a popup or export to a geodata file
                    property_overwrite: null, // A dictionary to overwrite the displayed property values, keys can take a (callback) function as well, e.g. to dynamically calculate the area of a shape
                };
            },
        },
        additionalLayersDefinitions: {
            type: Array,
            required: false,
            default: function () {
                return [];
            },
        },
        /**
         * Whether to validate the feature before saving it
         * If set to true, the `validate-feature` event is emitted before saving the feature
         * and the parent component needs to call finishDrawing on the map component
         */
        validateFeatureBeforeSave: {
            type: Boolean,
            required: false,
            default: false,
        },
    },
    emits: [
        'validate-feature',
        'refreshFromResponse',
        'features-loaded',
        'toggle-show-hide',
        'crs-select-search',
        'prepare-new-site-at-coordinates',
        'dirty',
    ],
    data() {
        return {
            elem_id: uuid(),
            map_container_id: uuid(),
            map: null,
            mapDefaultZoom: 13.335, // ~1:5e4
            gisExtentArray: null,
            tileLayerMapbox: null,
            tileLayerSat: null,
            selectedBaseLayer: null,
            optionalLayers: [],
            hover: false,
            mode: 'normal',
            subMode: null,
            drawForMeasure: null,
            drawPolygonsForModel: null, // Polygon Draw interaction
            drawPointsForModel: null, // Points Draw interaction
            newFeatureId: 0,
            measurementLayer: null,
            style: MeasureStyles.defaultStyle,
            segmentStyle: MeasureStyles.segmentStyle,
            labelStyle: MeasureStyles.labelStyle,
            segmentStyles: null,
            content_element: null,
            featureToast: null,
            selectedFeature: null,
            selectedModel: null,
            redirectingToModelDetails: false,
            queryingGeoserver: false,
            loadingMap: false,
            fetchingProposals: false,
            processingFeatures: false,
            proposals: [],
            layerSources: {},
            vectorLayers: {},
            defaultQueryLayerName: null, // The layer where e.g. dropped geometries are added to
            defaultProcessedGeometryLayerName: null, // The layer to which processed geometries are added to
            editableFeatureCollection: new Collection([], { unique: true }),
            selectedFeatureCollection: new Collection([], { unique: true }),
            zIndex: 0, // Incrementing Z-index for overlays
            lastPoint: null,
            sketchCoordinates: [[]],
            defaultColor: '#eeeeee',
            clickSelectStroke: new Stroke({
                color: 'rgba(255, 0, 0, 0.7)',
                width: 2,
            }),
            hoverFill: new Fill({
                color: 'rgba(255, 255, 255, 0.5)',
            }),
            hoverStrokePolygon: new Stroke({
                color: 'rgba(255, 255, 255, 0.5)',
                width: 1,
            }),
            hoverStrokePoint: new Stroke({
                color: 'rgba(255, 255, 255, 0.5)',
                width: 2,
            }),
            set_mode: set_mode,
            isValidating: false,
            errorMessage: null,
            overlayFeatureInfo: {},
            deletedFeatures: [], // keep track of deleted features
            undoredo: null,
            undoredo_forSketch: null, // Undo/redo stack for the sketch layer
            unOrRedoing_sketchPoint: false, // Whether currently undoing or redoing a sketch point
            modifiedFeaturesStack: [], // A stack of only those undoable actions that modified a feature
            drawing: false, // Whether the map is in draw (pencil icon) mode
            transforming: false, // Whether the map is in transform (resize, scale, rotate) mode
            numShapefiles: 0,
            uploadedFileTypes: [], // The currently uploaded types
            archiveTypesAllowed: ['.zip'], // The allowed archive types
            shapefileTypesAllowed: ['.shp', '.dbf', '.prj', '.shx', '.cpg'], // The allowed shapefile types
            shapefileTypesRequired: ['.shp', '.dbf', '.shx'], // The required shapefile types
            userInputGeometryStack: [],
            selectedSpatialOperation: null,
            selectedSpatialUnit: 'm',
            spatialOperationParameters: [1.0],
            fetchTileLayers: fetchTileLayers,
            fetchProposals: fetchProposals,
            cursorInLeftHalfOfMap: true,
            cursorInBottomHalfOfMap: true,
            showToastCloseButton: false,
            editableLayersArray: [],
            activeEditLayer: null,
            optionalLayersActive: false,
            mapMarkerPath: assets.pathToSvg('map-marker'), // The map marker icon path
            mapMarker: null,
            geojsonSnapshot: null, // A snapshot of the geojson feature collection
            isDirty: false, // Whether the map has unsaved changes
        };
    },
    computed: {
        shapefileDocumentUrl: function () {
            let endpoint = '';
            let obj_id = 0;
            if (this.context?.model_name == 'occurrencereport') {
                endpoint = api_endpoints.occurrence_report;
                obj_id = this.context.id;
            } else if (this.context?.model_name == 'occurrence') {
                endpoint = api_endpoints.occurrence;
                obj_id = this.context.id;
            } else {
                console.warn('shapefileDocumentUrl: invalid context');
                return ''; // Should not reach here.
            }
            let url = helpers.add_endpoint_join(
                endpoint,
                '/' + obj_id + '/process_shapefile_document/'
            );
            return url;
        },
        canUndoAction: function () {
            // The ol-ext undo/redo module states it is still experimental, might want to disable undo/redo at all
            return true;
        },
        canRedoAction: function () {
            // The ol-ext undo/redo module states it is still experimental, might want to disable undo/redo at all
            return true;
        },
        canUndoDrawnVertex: function () {
            this.sketchCoordinates || this.unOrRedoing_sketchPoint; // Mentioned here to force update of the computed property
            return (
                this.mode == 'draw' &&
                !this.unOrRedoing_sketchPoint &&
                this.drawPolygonsForModel &&
                this.drawPolygonsForModel.getActive() &&
                this.undoredo_forSketch.getStack('undo').length > 0
            );
        },
        canRedoDrawnVertex: function () {
            this.sketchCoordinates || this.unOrRedoing_sketchPoint; // Mentioned here to force update of the computed property
            return (
                this.mode == 'draw' &&
                !this.unOrRedoing_sketchPoint &&
                this.drawPolygonsForModel &&
                this.drawPolygonsForModel.getActive() &&
                this.undoredo_forSketch.getStack('redo').length > 0
            );
        },
        featureCount: function () {
            this.defaultQueryLayerName;
            this.defaultProcessedGeometryLayerName;
            const features = [];

            for (let layerName in this.layerSources) {
                features.push(...this.layerSources[layerName].getFeatures());
            }

            return features.length;
        },
        hasErrorMessage: function () {
            let vm = this;
            return vm.errorMessage !== null;
        },
        hasModifiedFeatures: function () {
            let vm = this;
            return vm.modifiedFeaturesStack.length > 0;
        },
        debug: function () {
            if (this.$route.query.debug) {
                return this.$route.query.debug === 'true';
            }
            return false;
        },
        /**
         * Returns the stack of undoable actions
         */
        undoStack: function () {
            let vm = this;
            if (!vm.undoredo) {
                return [];
            } else {
                return vm.undoredo.getStack('undo');
            }
        },
        /**
         * Returns the stack of undoable actions
         */
        redoStack: function () {
            let vm = this;
            if (!vm.undoredo) {
                return [];
            } else {
                return vm.undoredo.getStack('redo');
            }
        },
        hasUndo: function () {
            let vm = this;
            if (!vm.undoredo) {
                return false;
            } else {
                vm.modifiedFeaturesStack; // Mentioned here to force update of the computed property
                let stack = vm.undoredo.getStack('undo');
                return stack.length > 0;
            }
        },
        hasRedo: function () {
            let vm = this;
            if (!vm.undoredo) {
                return false;
            } else {
                vm.modifiedFeaturesStack; // Mentioned here to force update of the computed property
                let stack = vm.undoredo.getStack('redo');
                return stack.length > 0;
            }
        },
        hasUploadedShapefiles: function () {
            return this.numShapefiles;
        },
        csrf_token: function () {
            return helpers.getCookie('csrftoken');
        },
        /**
         * Returns whether the uploaded files are either only shapefiles or only archives
         */
        uploadedFilesComplete: function () {
            // The uploaded files contain archives
            const containsArchiveTypes = this.archiveTypesAllowed.some((type) =>
                this.uploadedFileTypes.includes(type)
            );
            // Every uploaded file is an archive
            const archiveTypesComplete = this.uploadedFileTypes.every((type) =>
                this.archiveTypesAllowed.includes(type)
            );
            // The uploaded files contain shapefiles
            const containsShapefileTypes = this.shapefileTypesAllowed.some(
                (type) => this.uploadedFileTypes.includes(type)
            );
            // Every required shapefile type is uploaded
            const shapefileTypesComplete = this.shapefileTypesRequired.every(
                (type) => this.uploadedFileTypes.includes(type)
            );

            // Either every uploaded file is an archive or
            // every required shapefile type is uploaded but not both
            return (
                (archiveTypesComplete && !containsShapefileTypes) ||
                (shapefileTypesComplete && !containsArchiveTypes)
            );
        },
        coordinateReferenceSystemsForSelectFilter: function () {
            return this.coordinateReferenceSystems.map((crs) => {
                return {
                    id: crs.id,
                    name: crs.name,
                };
            });
        },
        spatialOperationsAvailable: function () {
            let spatialOperations = [
                {
                    id: 'buffer_geometries',
                    name: 'Buffer',
                    icon: 'buffer-geometries',
                    number_params: 1,
                },
                {
                    id: 'convex_hull',
                    name: 'Convex Hull',
                    icon: 'convex-hull',
                    number_params: 0,
                },
                {
                    id: 'intersect_geometries',
                    name: 'Intersection',
                    icon: 'intersect-geometries',
                    number_params: 0,
                },
                {
                    id: 'union_geometries',
                    name: 'Union',
                    icon: 'union-geometries',
                    number_params: 0,
                },
                {
                    id: 'voronoi',
                    name: 'Voronoi',
                    icon: 'voronoi',
                    number_params: 0,
                },
                {
                    id: 'centroid',
                    name: 'Centroid',
                    icon: 'centroid',
                    number_params: 0,
                },
                // Spatial statistics functions below
                {
                    id: 'mean_center',
                    name: 'Mean Center',
                    icon: 'mean-center.svg',
                    number_params: 0,
                },
                {
                    id: 'standard_distance',
                    name: 'Standard Distance',
                    icon: 'standard-distance.svg',
                    number_params: 0,
                },
            ];

            if (this.spatialOperationsAllowed.includes('__all__')) {
                return spatialOperations;
            }
            spatialOperations = spatialOperations.filter((operation) => {
                return this.spatialOperationsAllowed.includes(operation.id);
            });

            return spatialOperations;
        },
        spatialUnitsAvailable: function () {
            const units = [
                {
                    id: 'm',
                    name: 'Metres',
                },
                {
                    id: 'deg',
                    name: 'Degree',
                },
            ];
            return units;
        },
        layerNameTitles: function () {
            return Object.fromEntries(
                this.vectorLayerDefinitions().map((def) => [
                    def.name,
                    def.title,
                ])
            );
        },
        /**
         * Returns the features in the modelQuerySource grouped by their source layer title and sorted by their id
         */
        mapFeaturesGroupedAndSorted: function () {
            const sortedFeatures = {};
            this.vectorLayerDefinitions().forEach((def) => {
                sortedFeatures[def.name] = [];
            });
            this.featureCount;

            for (let source in this.layerSources) {
                const features = this.layerSources[source]
                    .getFeatures()
                    .toSorted(function (a, b) {
                        return a.getProperties().id - b.getProperties().id;
                    });
                // const key = layerNameTitles[source];
                if (!Object.keys(sortedFeatures).includes(source)) {
                    sortedFeatures[source] = [];
                }
                sortedFeatures[source].push(...features);
            }
            return sortedFeatures;
        },
        parameterInputLabel: function () {
            let label = 'Parameter';
            if (this.selectedSpatialOperation == 'buffer') {
                const unit = this.selectedSpatialUnit
                    ? this.selectedSpatialUnit
                    : 'N/A';
                label = `Buffer Distance (${unit})`;
            }
            return label;
        },
        unitDependentStep: function () {
            return this.selectedSpatialUnit === 'deg' ? 0.0001 : 1;
        },
        selectedFeatureIds: function () {
            return this.selectedFeatureCollection.getArray().map((feature) => {
                return feature.getProperties().id;
            });
        },
        activeEditLayerName: function () {
            if (!this.activeEditLayer) {
                return null;
            }
            return this.activeEditLayer.getProperties().name;
        },
        isEditingALayer: function () {
            return Boolean(this.activeEditLayer);
        },
        vectorLayersArray: function () {
            return Object.values(this.vectorLayers);
        },
        canAddMorePoints: function () {
            if (!this.pointFeaturesSupported) {
                return false;
            }
            if (this.pointLimit && this.pointLimit > 0) {
                // Only count Point and MultiPoint features in the editableFeatureCollection
                const pointCount = this.editableFeatureCollection
                    .getArray()
                    .filter((feature) => {
                        const type = feature.getGeometry().getType();
                        return type === 'Point' || type === 'MultiPoint';
                    }).length;
                return pointCount < this.pointLimit;
            }
            return true; // No limit on points
        },
    },
    watch: {
        selectedFeatureIds: function () {
            if (this.selectedFeatureIds.length == 0) {
                this.errorMessageProperty(null);
            }
        },
        optionalLayers: {
            handler: function (newVal) {
                if (newVal.length == 0) {
                    return false;
                }
                let visibleLayers = newVal.filter(
                    (layer) => layer.values_.visible === true
                );
                this.optionalLayersActive = visibleLayers.length > 0;
            },
            deep: true,
        },
    },
    created: function () {
        let initialisers = [
            // Query Layer
            this.fetchProposals(
                this,
                this.queryLayerDefinition.api_url,
                this.queryLayerDefinition.ids,
                this.queryLayerDefinition.query_param_key
            ),
            // Tile Layers
            fetchTileLayers(this, this.tileLayerApiUrl),
            // Fetch the GIS extent for the map
            fetchGISExtent(api_endpoints.gis_extent),
        ];
        // Addional Layers
        const additionalInitialisers = [];
        for (let layerDef of this.additionalLayersDefinitions) {
            additionalInitialisers.push(
                this.fetchProposals(
                    this,
                    layerDef.api_url,
                    layerDef.ids,
                    layerDef.query_param_key
                )
            );
        }
        this.mapInitialisationPromise = Promise.all([
            ...initialisers,
            ...additionalInitialisers,
        ]);

        this.selectedSpatialOperation =
            this.spatialOperationsAvailable.length == 0
                ? null
                : this.spatialOperationsAvailable[0].id;

        console.log('Map created');
    },
    mounted: function () {
        let vm = this;
        vm.setLoadingMap(true);

        // Import the map-marker svg
        assets.asset(this.mapMarkerPath, 'url').then((result) => {
            vm.mapMarker = result.default;
        });

        this.mapInitialisationPromise.then((initialised) => {
            const proposals = vm.initialiseProposals(initialised.shift()); // pop first element
            const baseLayers = vm.initialiseBaseLayers(initialised.shift());
            vm.gisExtentArray = initialised.shift(); // pop the GIS extent tuple
            vm.createMap(baseLayers);
            vm.map.updateSize(); // Ensure map knows its size
            vm.addTileLayers();
            vm.initialiseMap();
            // Query Layer
            vm.loadMapFeatures(proposals, vm.queryLayerDefinition.name);
            for (let i = 0; i < initialised.length; i++) {
                const layerDef = vm.additionalLayersDefinitions[i];
                let features = vm.initialiseProposals(initialised[i], layerDef);

                if (layerDef.handler) {
                    features = layerDef.handler(features);
                }
                vm.loadMapFeatures(features, layerDef.name);
            }

            console.log('Done fetching map initilisation data');
            vm.setLoadingMap(false);
            if (vm.zoomToFeaturesOnLoad) {
                vm.displayAllFeatures();
            }
            vm.takeSnapshot();
        });

        this.$nextTick(() => {
            var toastEl = document.getElementById('featureToast');
            $('#map-spinner').css('position', 'absolute'); // Position spinner in center of map
            $('#map-spinner').css('top', '50%');
            $('#map-spinner').css('left', '50%');
            $('#map-spinner').css('zIndex', 9999);
            vm.featureToast = new bootstrap.Toast(toastEl, { autohide: false });
            if (vm.refreshMapOnMounted) {
                vm.forceToRefreshMap();
            } else {
                console.log('Done initializing map (no refresh)');
            }
        });
    },
    beforeUnmount() {
        // Clean up listeners if needed
        this.editableFeatureCollection.un('add', this.onFeatureChanged);
        this.editableFeatureCollection.un('remove', this.onFeatureChanged);
        this.editableFeatureCollection.forEach((feature) => {
            feature.un('change', this.onFeatureChanged);
        });
    },
    methods: {
        takeSnapshot() {
            const format = new GeoJSON();
            const features = this.editableFeatureCollection.getArray();
            this.geojsonSnapshot = format.writeFeaturesObject(features);
            this.isDirty = false;
            this.$emit('dirty', this.isDirty);
        },
        onFeatureChanged() {
            const format = new GeoJSON();
            const features = this.editableFeatureCollection.getArray();
            const currentGeoJSON = format.writeFeaturesObject(features);
            // Compare current to snapshot
            this.isDirty =
                JSON.stringify(currentGeoJSON) !==
                JSON.stringify(this.geojsonSnapshot);
            this.$emit('dirty', this.isDirty);
        },
        // Call this after saving to reset the dirty state
        markClean() {
            this.takeSnapshot();
        },
        callSetMode: function (mode, subMode = null) {
            this.set_mode(this, mode, subMode);
        },
        setLoadingMap(loading = false) {
            console.log('set loading-map', loading);
            this.loadingMap = loading;
        },
        /**
         * Returns the euclidean distance between two pixel coordinates
         * @param {Array} p1 a pixel coordinate pair in the form [x1, y1]
         * @param {Array} p2 a pixel coordinate pair in the form [x2, y2]
         */
        pixelDistance(p1, p2) {
            return Math.sqrt(
                Math.pow(p1[0] - p2[0], 2) + Math.pow(p1[1] - p2[1], 2)
            );
        },
        valueChanged: function (value, tileLayer) {
            tileLayer.setOpacity(value / 100);
        },
        download_content: function (content, fileName, contentType) {
            var a = document.createElement('a');
            var file = new Blob([content], { type: contentType });
            a.href = URL.createObjectURL(file);
            a.download = fileName;
            a.click();
        },
        geoJsonButtonClicked: function () {
            const selectedFeatures = this.selectedFeatureCollection.getArray();
            if (selectedFeatures.length == 0) {
                this.errorMessageProperty('Must select features to download');
                console.error('Must select features to download');
                return;
            }

            const format = new GeoJSON();
            const features = [];
            selectedFeatures.forEach((f) => {
                const feature = f.clone();
                feature.unset('model');
                features.push(feature);
            });
            const geojson = format.writeFeatures(features);

            this.download_content(
                geojson,
                'boranga_layers.geojson',
                'text/plain'
            );
        },
        displayAllFeatures: function (features) {
            let vm = this;
            if (vm.map) {
                if (
                    vm.layerSources[this.defaultQueryLayerName].getFeatures()
                        .length > 0
                ) {
                    let view = vm.map.getView();

                    let ext;
                    if (features) {
                        ext = vm.getFeaturesExtent(features);
                    } else {
                        ext =
                            vm.layerSources[
                                vm.defaultQueryLayerName
                            ].getExtent();
                    }

                    let centre = [
                        (ext[0] + ext[2]) / 2.0,
                        (ext[1] + ext[3]) / 2.0,
                    ];
                    let resolution = view.getResolutionForExtent(ext);
                    let z = view.getZoomForResolution(resolution) - 1;
                    view.animate({ zoom: z, center: centre });
                }
            }
        },
        onFeaturesLoaded: function (event) {
            let vm = this;
            if (event.details.loaded == true) {
                // Pass the event to parent components so they can identify which layer triggered the event
                // This is needed to prevent infinite recursion in handlers that reload derived layers
                vm.$emit('features-loaded', event);

                vm.initialiseUndoRedos();

                vm.dragbox = new DragBox({
                    condition: platformModifierKeyOnly,
                });
                vm.dragbox.on('boxend', function () {
                    const extent = vm.dragbox.getGeometry().getExtent();

                    const layers = vm.getLayersWithFeatures();
                    layers.forEach((layer) => {
                        layer
                            .getSource()
                            .forEachFeatureIntersectingExtent(
                                extent,
                                function (feature) {
                                    vm.selectFeature(feature);
                                }
                            );
                    });
                });

                vm.map.addInteraction(vm.undoredo);
                vm.map.addInteraction(vm.undoredo_forSketch);
                vm.map.addInteraction(vm.dragbox);
                vm.takeSnapshot(); // Take a snapshot of the current state of the map
            }
        },
        getFeaturesExtent: function (features) {
            const [E, S, W, N] = [[], [], [], []];
            for (let feature of features) {
                let extent = feature.getGeometry().getExtent();
                E.push(extent[0]);
                S.push(extent[1]);
                W.push(extent[2]);
                N.push(extent[3]);
            }
            return [
                Math.min(...E),
                Math.min(...S),
                Math.max(...W),
                Math.max(...N),
            ];
        },
        /**
         * Centers the map based on its size on a feature's geometry extent.
         * The map size is pixel dimensions of the box to fit the extent into.
         * @param {Feature} feature The feature to center on
         * @param {number=} maxZoom The maximum zoom level to fit the feature and that the map zooms to, defaults to no limit
         */
        centerOnFeature: function (feature, maxZoom) {
            let ext = feature.getGeometry().getExtent();
            if (!maxZoom) {
                const extPol = fromExtent(ext);
                extPol.scale(1.5);
                ext = new Feature(extPol).getGeometry().getExtent();
            }
            this.map.getView().fit(ext, {
                duration: 1000,
                size: this.map.getSize(),
                maxZoom: maxZoom,
            });
        },
        /**
         * Toggles the visibility of a feature on the map by changing the show_on_map property of the geometry
         * @param {Object} feature A feature
         */
        toggleFeatureShowOnMap: function (
            feature,
            layerName,
            toggleKey = 'show_on_map'
        ) {
            const properties = feature.getProperties();
            if (!Object.hasOwn(properties, toggleKey)) {
                console.log(`Feature does not have a ${toggleKey} property`);
            }
            this.$emit('toggle-show-hide', feature, layerName);
        },
        setBaseLayer: function (selected_layer_name) {
            let vm = this;
            if (selected_layer_name == 'sat') {
                vm.tileLayerMapbox.setVisible(false);
                vm.tileLayerSat.setVisible(true);
                vm.selectedBaseLayer = vm.tileLayerSat;
                $('#basemap_sat').hide();
                $('#basemap_street').show();
            } else {
                vm.tileLayerMapbox.setVisible(true);
                vm.tileLayerSat.setVisible(false);
                vm.selectedBaseLayer = vm.tileLayerMapbox;
                $('#basemap_street').hide();
                $('#basemap_sat').show();
            }
        },
        clearMeasurementLayer: function () {
            let vm = this;
            let features = vm.measurementLayer.getSource().getFeatures();
            features.forEach((feature) => {
                vm.measurementLayer.getSource().removeFeature(feature);
            });
        },
        forceToRefreshMap(timeout = 700) {
            let vm = this;
            setTimeout(function () {
                console.log('Refreshing map');
                vm.map.updateSize();
                // Unset loading map spinner here
                vm.setLoadingMap(false);
            }, timeout);
        },
        addJoint: function (point, styles) {
            let s = this.createStyle('#3399cc', '#3399cc', 'Point', 2);
            s.setGeometry(point);
            styles.push(s);

            return styles;
        },
        /**
         * Returns a color for a feature based on the styleBy property
         * and either the feature or model object
         * @param {dict} featureData A feature object
         * @param {Proxy} model A model object
         */
        styleByColor: function (featureData, model, geometry_source = null) {
            let vm = this;
            if (!geometry_source) {
                geometry_source =
                    featureData.properties?.geometry_source?.toLowerCase() ||
                    'draw';
            }

            const type = vm.getFeatureType(featureData);
            const featureColors = ['Point', 'MultiPoint'].includes(type)
                ? vm.pointFeatureColors
                : vm.featureColors;

            if (model.color) {
                return model.color;
            } else if (featureColors[geometry_source]) {
                return featureColors[geometry_source];
            } else {
                return featureColors['unknown'] || vm.defaultColor;
            }
        },
        /**
         * Returns a style based on feature type. Defaults to a polygon style
         * @param {string|object} fill The fill color or a Fill object
         * @param {string=|object=} stroke The stroke color or a Stroke object
         * @param {string=} type The feature type
         * @param {number=} radius The radius of the point circle, defaults to 7
         * @param {number=} width The stroke width, defaults to 2
         * @param {string=} svg The path to an svg icon for a map marker to use instead of a circle for a point feature
         */
        createStyle: function (
            fill,
            stroke = null,
            type = null,
            radius = 7,
            width = 2,
            svg = null,
            transparency = 0
        ) {
            let vm = this;
            if (!fill) {
                fill = vm.defaultColor;
            }
            if (!stroke) {
                stroke = vm.defaultColor;
            }

            if (!(fill instanceof Fill)) {
                if (!vm.isColor(fill)) {
                    // Check for fill being a color string first
                    fill = vm.defaultColor;
                }
                fill = new Fill({ color: fill });
            }
            if (!(stroke instanceof Stroke)) {
                if (!vm.isColor(stroke)) {
                    // Check for stroke being a color string first
                    stroke = vm.defaultColor;
                }
                stroke = new Stroke({
                    color: stroke,
                    width: width,
                });
            }

            if (['MultiPolygon', 'Polygon', null].includes(type)) {
                return new Style({
                    stroke: stroke,
                    fill: fill,
                });
            } else if (type === 'LineString') {
                return new Style({
                    stroke: stroke,
                });
            } else if (['MultiPoint', 'Point'].includes(type)) {
                if (svg) {
                    return new Style({
                        image: new Icon({
                            anchor: [0.5, 1.0],
                            src: svg,
                            opacity: 1 - transparency,
                            color: fill.getColor(),
                        }),
                    });
                } else {
                    return new Style({
                        image: new CircleStyle({
                            radius: radius,
                            stroke: stroke,
                            fill: fill,
                        }),
                    });
                }
            } else {
                console.error('Unknown feature type: ' + type);
            }
        },
        styleFunctionForMeasurement: function (feature) {
            let vm = this;
            let for_layer = feature.get('for_layer', false);

            const styles = [];
            styles.push(vm.style); // This style is for the feature itself
            styles.push(vm.segmentStyle);

            ///////
            // From here, adding labels and tiny circles at the end points of the linestring
            ///////
            const geometry = feature.getGeometry();
            if (geometry.getType() === 'LineString') {
                let segment_count = 0;
                geometry.forEachSegment(function (a, b) {
                    const segment = new LineString([a, b]);
                    const label = formatLength(segment);
                    const segmentPoint = new Point(
                        segment.getCoordinateAt(0.5)
                    );

                    // Add a style for this segment
                    let segment_style = vm.segmentStyle.clone(); // Because there could be multilpe segments, we should copy the style per segment
                    segment_style.setGeometry(segmentPoint);
                    segment_style.getText().setText(label);
                    styles.push(segment_style);

                    if (segment_count == 0) {
                        // Add a tiny circle to the very first coordinate of the linestring
                        let p = new Point(a);
                        vm.addJoint(p, styles);
                    }
                    // Add tiny circles to the end of the linestring
                    let p = new Point(b);
                    vm.addJoint(p, styles);

                    segment_count++;
                });
            }

            if (!for_layer) {
                // We don't need the last label when draw on the layer.
                let label_on_mouse = formatLength(geometry); // Total length of the linestring
                let point = new Point(geometry.getLastCoordinate());
                vm.labelStyle.setGeometry(point);
                vm.labelStyle.getText().setText(label_on_mouse);
                styles.push(vm.labelStyle);
            }

            return styles;
        },
        setStyleForUnAndSelectedFeatures: function (style) {
            let vm = this;
            if (style === undefined) {
                if (this.mode == 'draw') {
                    style = vm.modifySelectStyle;
                } else {
                    style = vm.basicSelectStyle;
                }
            }
            const layers = vm.getLayersWithFeatures();
            const selectedFeatures = vm.selectedFeatures();
            const editableSelectedFeatures = vm.editableSelectedFeatures();
            layers.forEach((layer) => {
                const features = layer.getSource().getFeatures();
                features.forEach((feature) => {
                    if (this.isFeatureEligibleToHide(feature)) {
                        // Ignore hidden features
                        return;
                    }
                    if (editableSelectedFeatures.includes(feature)) {
                        feature.setStyle(style);
                    } else if (selectedFeatures.includes(feature)) {
                        feature.setStyle(vm.basicSelectStyle);
                    } else {
                        feature.setStyle(undefined);
                    }
                });
            });
        },
        initialiseMap: function () {
            let vm = this;

            // Full screen toggle
            let fullScreenControl = new FullScreenControl();
            vm.map.addControl(fullScreenControl);

            vm.initialiseMeasurementLayer();
            // Init vector layers
            vm.initialiseQueryLayer();
            vm.initialiseProcessingLayer();
            vm.defaultQueryLayerName = vm.getDefaultQueryLayerName();
            vm.defaultProcessedGeometryLayerName =
                vm.getDefaultProcessedGeometryLayerName();

            vm.initialiseDrawLayer();

            vm.initialiseLayerSwitcher([
                vm.measurementLayer,
                ...vm.vectorLayersArray,
            ]);

            vm.initialiseLayerEvents();

            // update map extent when new features added
            vm.map.on('rendercomplete', vm.displayAllFeatures());
            vm.map.on('features-loaded', vm.onFeaturesLoaded);

            vm.initialisePointerMoveEvent();
            vm.snap = vm.initialiseSnap(vm.vectorLayersArray);
            vm.dragAndDrop = new DragAndDrop({
                projection: `EPSG:${vm.mapSrid}`,
                formatConstructors: [GeoJSON],
            });
            vm.dragAndDrop.on('addfeatures', function (event) {
                let features = event.features;
                let source = vm.layerSources[vm.defaultQueryLayerName];
                for (let i = 0, ii = features.length; i < ii; i++) {
                    let feature = features[i];
                    let color = vm.styleByColor(feature, vm.context, 'draw');

                    vm.setFeaturePropertiesFromContext(feature, vm.context, {
                        color: color,
                    });

                    source.addFeature(feature);
                    vm.userInputGeometryStackAdd(feature);
                }
            });

            let extent_interactions = [vm.snap, vm.dragAndDrop];
            if (vm.editable) {
                // Only add these interactions if polygons are editable
                vm.select = vm.initialiseSelectFeatureEvent();
                vm.modify = vm.initialiseModifyFeatureEvent();
                vm.transform = vm.initialiseTransform();
                extent_interactions.push(vm.select, vm.modify, vm.transform);
            }

            vm.map.getInteractions().extend(extent_interactions);

            vm.modifySetActive(false);

            vm.initialiseSingleClickEvent();
            vm.initialiseDoubleClickEvent();

            // Custom drag and drop
            vm.map.getViewport().addEventListener('dragover', function (evt) {
                evt.preventDefault();
            });
            vm.map.getViewport().addEventListener('drop', function (evt) {
                // Prevent default behavior (Prevent file from being opened)
                evt.preventDefault();
                vm.processDatatransferEvent(evt);
            });
            // vm.map.getViewport().addEventListener('dragenter', function (evt) {
            //     // Prevent default behavior (Prevent file from being opened)
            //     evt.preventDefault();
            // });

            vm.callSetMode('layer');
            vm.setBaseLayer('street');
        },
        initialiseMeasurementLayer: function () {
            let vm = this;

            // Measure tool
            let draw_source = new VectorSource({ wrapX: false });
            vm.drawForMeasure = new Draw({
                source: draw_source,
                type: 'LineString',
                style: vm.styleFunctionForMeasurement,
            });
            // Set a custom listener to the Measure tool
            vm.drawForMeasure.set('escKey', '');
            vm.drawForMeasure.on('change:escKey', function () {});
            vm.drawForMeasure.on('drawstart', function () {
                // Set measuring to true on mode change (fn `set_mode`), not drawstart
            });
            vm.drawForMeasure.on('drawend', function () {
                // Set measuring to false on mode change
            });

            // Create a layer to retain the measurement
            vm.measurementLayer = new VectorLayer({
                title: 'Measurements',
                source: draw_source,
                style: function (feature, resolution) {
                    feature.set('for_layer', true);
                    return vm.styleFunctionForMeasurement(feature, resolution);
                },
            });
            vm.map.addInteraction(vm.drawForMeasure);
            vm.map.addLayer(vm.measurementLayer);
        },
        initialiseFeatureQueryLayer: function (title, name) {
            const modelQuerySource = new VectorSource({});
            const polygonStyle = this.createStyle(null, null, 'Polygon');
            this.layerSources[name] = modelQuerySource;
            const layerDef = this.getLayerDefinitionByName(name);
            let zIndex = layerDef.z_index;

            this.vectorLayers[name] = new VectorLayer({
                title: title,
                name: name,
                source: modelQuerySource,
                can_edit: layerDef.can_edit,
                editing: false,
                style: (feature) => {
                    const color = feature.get('color') || this.defaultColor;
                    let style = polygonStyle;
                    if (this.isPolygonLikeFeature(feature)) {
                        style.getFill().setColor(color);
                    } else if (this.isPointLikeFeature(feature)) {
                        const rgba = this.colorHexToRgbaValues(color);
                        style = this.createStyle(
                            color,
                            null,
                            'Point',
                            null,
                            null,
                            this.mapMarker,
                            rgba[3]
                        );
                    }
                    return style;
                },
            });
            // Add the layer
            this.map.addLayer(this.vectorLayers[name]);
            // Set zIndex to some layers to be rendered over the other layers
            if (!zIndex) {
                zIndex = this.zIndex;
                this.zIndex += 1;
            }
            console.log('Setting zIndex', zIndex, 'for layer', name);
            this.vectorLayers[name].setZIndex(zIndex);
            // this.vectorLayers[name].setOpacity(0.5);
            this.zIndex += 1;
        },
        initialiseQueryLayer: function () {
            this.initialiseFeatureQueryLayer(
                this.queryLayerDefinition.title,
                this.queryLayerDefinition.name
                // this.queryLayerDefinition.can_edit
            );
        },
        initialiseProcessingLayer: function () {
            for (let def of this.additionalLayersDefinitions) {
                this.initialiseFeatureQueryLayer(
                    def.title,
                    def.name
                    // def.can_edit
                );
            }
        },
        initialiseDrawLayer: function () {
            let vm = this;
            if (!vm.drawable) {
                return;
            }

            vm.drawPolygonsForModel = new Draw({
                source: vm.layerSources[vm.defaultQueryLayerName],
                type: 'Polygon',
                geometryFunction: function (coordinates, geometry) {
                    if (geometry) {
                        if (coordinates[0].length) {
                            // Add a closing coordinate to match the first
                            geometry.setCoordinates(
                                [coordinates[0].concat([coordinates[0][0]])],
                                this.geometryLayout_
                            );
                        } else {
                            geometry.setCoordinates([], this.geometryLayout_);
                        }
                    } else {
                        geometry = new Polygon(
                            coordinates,
                            this.geometryLayout_
                        );
                    }

                    if (vm.unOrRedoing_sketchPoint) {
                        // Don't run below undo stack logic while executing an undo/redo of sketch points
                        return geometry;
                    }

                    // Current feature id list for undo stack
                    let before = [...vm.sketchCoordinates];
                    // Ignore the last coordinate that is the movable cursor point
                    let drawnVertexCoords = coordinates[0].toSpliced(-1);
                    if (before.length != drawnVertexCoords.length) {
                        // Sort out back-to-back duplicate coordinates
                        let sketchCoordinates = drawnVertexCoords
                            .slice()
                            .reduce((acc, cur) => {
                                let prev = acc.slice(-1)[0] || [];
                                if (prev[0] !== cur[0] && prev[1] !== cur[1]) {
                                    acc.push(cur);
                                }
                                return acc;
                            }, []);

                        // Return from calculation if the new sketch coordinates are the same as the previous
                        if (
                            before.length === sketchCoordinates.length &&
                            before
                                .flat(1)
                                .every(
                                    (coord, index) =>
                                        coord ===
                                        sketchCoordinates.flat(1)[index]
                                )
                        ) {
                            return geometry;
                        }
                        // Set new sketch coordinates
                        vm.sketchCoordinates = sketchCoordinates;

                        // Add to undo stack
                        vm.undoredo_forSketch.push('add polygon point', {
                            before: before,
                            after: vm.sketchCoordinates,
                        });
                    }

                    return geometry;
                },
                condition: function (evt) {
                    if (evt.originalEvent.buttons === 1) {
                        // Only allow drawing when the left mouse button is pressed
                        return true;
                    } else if (evt.originalEvent.buttons === 2) {
                        // If the right mouse button is pressed, undo the last point
                        if (vm.canUndoDrawnVertex) {
                            vm.undoredo_forSketch.undo();
                        } else {
                            vm.callSetMode('layer');
                        }
                    } else {
                        return false;
                    }
                },
                finishCondition: function () {
                    if (vm.lastPoint) {
                        if (vm.validateFeatureBeforeSave) {
                            const coordinates = vm.sketchCoordinates.slice();
                            coordinates.push(coordinates[0]);
                            const feature = new Feature({
                                id: -1,
                                geometry: new Polygon([coordinates]),
                                label: 'validation',
                                color: vm.defaultColor,
                                geometry_source: 'validation',
                            });
                            vm.emitValidateFeature(feature);
                        } else {
                            console.log('Skipping feature validation');
                            vm.finishDrawing();
                        }
                    }
                    return false;
                },
            });

            vm.drawPointsForModel = new Draw({
                source: vm.layerSources[vm.defaultQueryLayerName],
                type: 'Point',
                condition: function () {
                    if (!vm.canAddMorePoints) {
                        if (vm.mode === 'draw' && vm.subMode === 'Point') {
                            swal.fire({
                                title: 'Point Limit Reached',
                                text: `You cannot add more than ${vm.pointLimit} points.`,
                                icon: 'warning',
                                customClass: {
                                    confirmButton: 'btn btn-primary',
                                },
                            });
                        }
                        return false;
                    }
                    return true;
                },
            });

            vm.drawPolygonsForModel.set('escKey', '');
            vm.drawPolygonsForModel.on('change:escKey', function () {
                console.log('ESC key pressed');
            });
            vm.drawPolygonsForModel.on('drawstart', function () {
                vm.errorMessage = null;
                vm.lastPoint = null;
            });
            vm.drawPolygonsForModel.on('click', function (evt) {
                console.log('Draw: click event', evt);
            });
            vm.drawPolygonsForModel.on('drawend', function (evt) {
                vm.onDrawEnd(evt.feature);
            });
            vm.drawPointsForModel.on('drawend', function (evt) {
                vm.onDrawEnd(evt.feature);
                vm.userInputGeometryStackAdd(evt.feature);
            });

            vm.map.addInteraction(vm.drawPolygonsForModel);
            vm.map.addInteraction(vm.drawPointsForModel);
        },
        initialiseProposals: function (proposals, layerDef = null) {
            this.proposals = proposals;
            this.assignProposalFeatureColors(proposals, layerDef);

            return proposals;
        },
        initialiseBaseLayers: function (tileLayers) {
            this.tileLayerMapbox = new TileLayer({
                title: 'Mapbox Streets',
                type: 'base',
                visible: true,
                // source: new OSM(),
            });
            this.tileLayerSat = new TileLayer({
                title: 'Satellite Map',
                type: 'base',
                visible: false,
                // source: new OSM(),
            });

            for (let tileLayer of tileLayers) {
                if (tileLayer.get('is_streets_background')) {
                    this.tileLayerMapbox = tileLayer;
                } else if (tileLayer.get('is_satellite_background')) {
                    this.tileLayerSat = tileLayer;
                } else {
                    this.optionalLayers.push(tileLayer);
                }
            }

            const baseLayers = new LayerGroup({
                title: 'Background Maps',
                layers: [this.tileLayerMapbox, this.tileLayerSat],
            });
            console.log('baseLayers', baseLayers);
            // Hack
            if (!baseLayers.getSource) {
                baseLayers.getSource = () => {
                    return this.selectedBaseLayer;
                };
            }

            return baseLayers;
        },
        initialiseLayerSwitcher: function (layers) {
            const targetElement = $('#layer-control').get(0);

            // Remove any existing control before creating a new one to prevent duplicate lists
            if (this.layerSwitcher) {
                if (this.map) {
                    this.map.removeControl(this.layerSwitcher);
                }
                if (targetElement) {
                    targetElement.innerHTML = '';
                }

                if (this.editableFeatureCollection) {
                    this.editableFeatureCollection.un(
                        'add',
                        this.onFeatureChanged
                    );
                    this.editableFeatureCollection.un(
                        'remove',
                        this.onFeatureChanged
                    );
                    this.editableFeatureCollection.forEach((feature) => {
                        feature.un('change', this.onFeatureChanged);
                    });
                }

                this.layerSwitcher = null;
            }

            if (targetElement) {
                targetElement.innerHTML = '';
            }

            // Add layer switcher control inside the map
            const props = {
                target: targetElement,
                show_progress: true,
                extent: true,
                trash: false,
                displayInLayerSwitcher: function (l) {
                    return l.get('displayInLayerSwitcher');
                },
                editableLayers: function () {
                    return this.editableLayers();
                },
            };

            if (layers) {
                layers.forEach((layer) => {
                    layer.set('displayInLayerSwitcher', true);
                });
                // const layerGroup = new LayerGroup({
                //     title: 'Layers',
                //     layers: layers,
                // });
                // props['layerGroup'] = layerGroup;
            }

            this.layerSwitcher = new LayerSwitcher(props);

            if (this.editable || this.drawable) {
                // Add a new button to the list if the component allows for editing or drawing
                this.layerSwitcher.on('drawlist', (e) => {
                    const layer = e.layer;
                    const divWrapper = $('<div>');

                    divWrapper.addClass([
                        'optional-layers-button-wrapper',
                        'optional-layers-button-fit',
                    ]);
                    divWrapper.attr('title', 'Layer Editing: Off');
                    divWrapper.attr(
                        'id',
                        `layer-editing-button-${layer.ol_uid}`
                    );

                    const divDraw = $('<div>');
                    divDraw.addClass(['btn', 'optional-layers-button']);

                    const img = $('<img>');
                    img.addClass('svg-object');

                    // Import the pen-icon svg
                    assets
                        .asset(assets.pathToSvg('pen-icon'), 'url')
                        .then((result) => {
                            return result.default;
                        })
                        .then((svg) => {
                            img.attr('src', svg);
                            divDraw.append(img);
                            divWrapper.append(divDraw);
                        });

                    if (layer.get('can_edit')) {
                        divWrapper.click((e) => {
                            const target = e.originalEvent.currentTarget;
                            const toggle_editing = !layer.get('toggle_editing');
                            // Clear the collection of editable features
                            this.editableFeatureCollection.clear();

                            if (toggle_editing) {
                                // Get the other editable layers
                                const otherEditableLayers =
                                    this.editableLayers().filter(
                                        (l) => l.ol_uid != layer.ol_uid
                                    );
                                // Turn off editing for all other layers if toggling on for this layer
                                otherEditableLayers.forEach((l) => {
                                    const b = $(
                                        `#layer-editing-button-${l.ol_uid}`
                                    );
                                    this.layerToggleEditing(l, b, false);
                                });

                                // Populate the collection of editable features
                                layer
                                    .getSource()
                                    .getFeatures()
                                    .forEach((f) => {
                                        this.editableFeatureCollection.push(f);
                                    });

                                // A bit clunky but this makes it so when switching editing modes the feature selection styles get properly set
                                const mode = this.mode;
                                const subMode = this.subMode;
                                this.callSetMode('layer');
                                this.callSetMode(mode, subMode);
                            } else {
                                this.callSetMode('layer');
                            }

                            // Toggle on this layer's editing
                            this.layerToggleEditing(
                                layer,
                                target,
                                toggle_editing
                            );
                        });

                        divWrapper.appendTo(
                            $('> .ol-layerswitcher-buttons', e.li)
                        );
                    }
                    // Listen for add/remove on the collection
                    this.editableFeatureCollection.on(
                        'add',
                        this.onFeatureChanged
                    );
                    this.editableFeatureCollection.on(
                        'remove',
                        this.onFeatureChanged
                    );

                    // Listen for changes to features (geometry edits, etc.)
                    this.editableFeatureCollection.forEach((feature) => {
                        feature.on('change', this.onFeatureChanged);
                    });
                });
            }

            this.layerSwitcher.on('layer:opacity', (e) => {
                const layer = e.layer;
                const opacity = layer.getProperties().opacity;
                layer
                    .getSource()
                    .getFeatures()
                    .forEach((feature) => {
                        feature.set('opacity', opacity);
                    });
            });

            // Add a button to show/hide the layers
            const button = $(
                '<button id="global-layer-visibilty-toggle" class="btn btn-primary btn-sm mb-2"></button>'
            )
                .text('Show/Hide All Layers')
                .click((e) => {
                    e.preventDefault();
                    e.stopPropagation();
                    const a = this.map
                        .getLayers()
                        .getArray()
                        .filter((l) =>
                            this.layerSwitcher.displayInLayerSwitcher(l)
                        );

                    const b = !a[0].getVisible();
                    if (b) button.removeClass('show');
                    else button.addClass('show');
                    for (let i = 0; i < a.length; i++) {
                        a[i].setVisible(b);
                    }
                });
            this.layerSwitcher.setHeader(
                $(
                    '<div class="d-flex justify-content-center border-bottom mb-3">'
                )
                    .append(button)
                    .get(0)
            );

            this.map.addControl(this.layerSwitcher);

            if (targetElement) {
                // Ensure the menu only contains the most recent LayerSwitcher instance
                const layerSwitcherContainers = Array.from(
                    targetElement.children || []
                ).filter((element) =>
                    element.classList?.contains('ol-layerswitcher')
                );
                if (layerSwitcherContainers.length > 1) {
                    layerSwitcherContainers
                        .slice(0, -1)
                        .forEach((element) => element.remove());
                }
            }
        },
        initialiseLayerEvents: function () {
            const editableLayers = this.editableLayers();
            editableLayers.forEach((layer) => {
                layer.getSource().on('addfeature', (evt) => {
                    this.editableFeatureCollection.push(evt.feature);
                });
                layer.getSource().on('removefeature', (evt) => {
                    this.editableFeatureCollection.remove(evt.feature);
                });
            });
        },
        initialiseUndoRedos: function () {
            let vm = this;
            // Add undo/redo AFTER proposal geometries have been added to the map
            vm.undoredo = new UndoRedo({
                layers: vm.vectorLayersArray,
            });
            vm.undoredo.clear();

            // Somehow passing the parameter has no effect, so we set it here
            vm.undoredo.setMaxLength(vm.undoStackMaxLength);
            // Define a custom undo/redo for selected features
            vm.undoredo.define(
                'select feature',
                function (s) {
                    // Undo fn: set to the previous feature collection and styles
                    console.log(
                        'undo selected',
                        s.before.getArray(),
                        s.after.getArray()
                    );
                    // Set the collection of selected features to the before value
                    vm.setSelectedFeatureCollection(s.before);
                    vm.setStyleForUnAndSelectedFeatures();
                },
                function (s) {
                    // Redo fn: reset the feature collection and styles
                    console.log(
                        'redo selected',
                        s.before.getArray(),
                        s.after.getArray()
                    );
                    // Set the collection of selected features to the after value
                    vm.setSelectedFeatureCollection(s.after);
                    vm.setStyleForUnAndSelectedFeatures();
                }
            );

            vm.undoredo.define(
                'update user input geodata',
                function (s) {
                    // Undo fn
                    // The last entry the user has edited in the list of geometries
                    let last = vm.userInputGeometryStack.slice(-1)[0];
                    // Set the stack to the state before the last edit
                    vm.userInputGeometryStack = s.before;
                    // The user input geometry before the last edit, which is the state we want the feature to revert to
                    const original_geometry = vm.userInputGeometryStackLast(
                        last.ol_uid
                    );

                    vm.unOrRedoFeatureUserInputGeoData(
                        last.ol_uid,
                        original_geometry
                    );
                },
                function (s) {
                    // Redo fn
                    vm.userInputGeometryStack = s.after;
                    // The last entry the user has edited in the list of geometries
                    const last = vm.userInputGeometryStack.slice(-1)[0];
                    // The user input geometry before the last edit, which is the state we want the feature to revert to
                    const original_geometry = vm.userInputGeometryStackLast(
                        last.ol_uid
                    );
                    vm.unOrRedoFeatureUserInputGeoData(
                        last.ol_uid,
                        original_geometry
                    );
                }
            );

            // Setup a dedicated undo/redo for sketch points on the draw layer
            vm.undoredo_forSketch = new UndoRedo({
                layers: vm.vectorLayersArray,
            });
            vm.undoredo_forSketch.clear();

            vm.undoredo_forSketch.setMaxLength(vm.undoStackMaxLength);
            vm.undoredo_forSketch.define(
                'add polygon point',
                function (s) {
                    // Undo fn: set to the previous sketch coordinates
                    vm.unOrRedoing_sketchPoint = true;
                    vm.sketchCoordinates = s.before;
                    vm.undoLeaseLicensePoint();
                    vm.unOrRedoing_sketchPoint = false;
                },
                function (s) {
                    // Redo fn: reset the sketch coordinates
                    vm.unOrRedoing_sketchPoint = true;
                    vm.sketchCoordinates = s.after;
                    vm.redoLeaseLicensePoint();
                    vm.unOrRedoing_sketchPoint = false;
                }
            );

            for (let eventName of ['stack:add', 'stack:remove']) {
                vm.undoredo.addEventListener(eventName, function () {
                    let undo_stack = vm.undoredo.getStack('undo');

                    let stack = undo_stack.filter((item) => {
                        // Filter out the actions that modify an existing or add a feature
                        return (
                            (['addfeature'].includes(item.type) &&
                                item.feature.getProperties().geometry_source ===
                                    'New') ||
                            ['translate', 'rotate', 'scale'].includes(item.name)
                        );
                    });

                    vm.modifiedFeaturesStack = Object.assign(stack, {});
                });
            }
        },
        initialiseSnap: function (snapLayers) {
            const snapCollection = new Collection([], {
                unique: true,
            });

            snapLayers.forEach(function (layer) {
                layer.getSource().on('addfeature', function (evt) {
                    snapCollection.push(evt.feature);
                });
                layer.getSource().on('removefeature', function (evt) {
                    snapCollection.remove(evt.feature);
                });
            });

            const snap = new Snap({
                features: snapCollection,
            });

            // eslint-disable-next-line no-unused-vars
            snap.on('change', function (event) {
                console.log('Snap change event', this.target);
            });

            return snap;
        },
        createMap: function (baseLayers) {
            let container =
                this.$refs.popup || document.getElementById('popup');
            let overlay = new Overlay({
                element: container,
                autoPan: {
                    margin: 110,
                },
            });
            const mousePositionControl = new MousePosition({
                coordinateFormat: createStringXY(4),
                projection: 'EPSG:4326',
                className: 'custom-mouse-position',
                placeholder: 'Mouse Coordinates',
            });
            this.map = new Map({
                controls: defaultControls({
                    rotate: false,
                }).extend([
                    new ScaleLine({
                        bar: true,
                        text: true,
                        minWidth: 140,
                    }),
                    mousePositionControl,
                ]),
                layers: [baseLayers],
                overlays: [overlay],
                target: this.elem_id,
                view: new View({
                    center: [115.95, -31.95],
                    zoom: 7,
                    projection: `EPSG:${this.mapSrid}`,
                    enableRotation: false,
                }),
            });
        },
        initialisePointerMoveEvent: function () {
            let vm = this;

            const hoverStylePolygon = vm.createStyle(
                vm.hoverFill,
                vm.hoverStrokePolygon,
                'Polygon'
            );

            // Cache the hover fill so we don't have to create a new one every time
            // Also prevent overwriting property `hoverFill` color
            let _hoverFill = null;
            function hoverSelect(feature) {
                const color = feature.get('color') || vm.defaultColor;
                let hoverStyle = hoverStylePolygon;
                if (vm.isPointLikeFeature(feature)) {
                    const rgba = vm.colorHexToRgbaValues(color);
                    hoverStyle = vm.createStyle(
                        vm.hoverFill,
                        vm.hoverStrokePoint,
                        'Point',
                        null,
                        null,
                        vm.mapMarker,
                        rgba[3]
                    );
                }
                _hoverFill = new Fill({ color: color });

                // If the feature is already selected, use the select stroke when hovering
                if (
                    vm.selectedFeatureIds.includes(feature.getProperties().id)
                ) {
                    hoverStyle.setFill(_hoverFill);
                    hoverStyle.setStroke(vm.clickSelectStroke);
                } else {
                    if (vm.isPolygonLikeFeature(feature)) {
                        hoverStyle.setFill(vm.hoverFill);
                        hoverStyle.setStroke(vm.hoverStrokePolygon);
                    } else if (vm.isPointLikeFeature(feature)) {
                        const image = hoverStyle.getImage();
                        // Marker icons don't have a fill or stroke property
                        if (Object.hasOwn(image, 'setFill')) {
                            hoverStyle.getImage().setFill(vm.hoverFill);
                        }
                        if (Object.hasOwn(image, 'setStroke')) {
                            hoverStyle
                                .getImage()
                                .setStroke(vm.hoverStrokePoint);
                        }
                    }
                }
                return hoverStyle;
            }

            let selected = null;
            let lastPointerMove = 0;
            vm.map.on('pointermove', function (evt) {
                const now = Date.now();
                if (now - lastPointerMove < 50) return; // 20 FPS max
                lastPointerMove = now;
                function isSelectedFeature(selected) {
                    if (!selected) {
                        return false;
                    }
                    return vm.selectedFeatureIds.includes(
                        selected.getProperties().id
                    );
                }
                if (selected !== null) {
                    if (isSelectedFeature(selected)) {
                        // Don't alter style of click-selected features
                        console.log('ignoring hover on selected feature');
                        if (vm.drawing) {
                            // Enable modify polygon the hovered polygon is selected and drawing mode is active
                            vm.modifySetActive(true);
                            vm.transformSetActive(false);
                        } else {
                            // Disable modify polygon when drawing mode is not active
                            vm.modifySetActive(false);
                        }
                    } else {
                        selected.setStyle(undefined);
                        if (!(vm.measuring || vm.drawing)) {
                            // Don't highlight features when measuring or drawing
                            const type = selected.getGeometry().getType();
                            const rgba = vm.colorHexToRgbaValues(
                                selected.values_.color
                            );
                            selected.setStyle(
                                vm.createStyle(
                                    selected.getProperties().color,
                                    selected.getProperties().stroke,
                                    type,
                                    null,
                                    null,
                                    vm.mapMarker,
                                    rgba[3]
                                )
                            );
                        }
                    }
                    selected = null;
                }
                vm.map.forEachFeatureAtPixel(
                    evt.pixel,
                    function (feature, layer) {
                        selected = feature;
                        const properties = vm.featureGetDisplayProperties(
                            selected,
                            layer
                        );
                        vm.selectedModel = structuredClone(properties);
                        if (!isSelectedFeature(selected)) {
                            selected.setStyle(hoverSelect);
                        }

                        return true;
                    },
                    {
                        layerFilter: function (layer) {
                            return Object.keys(vm.vectorLayers).includes(
                                layer.get('name')
                            );
                        },
                    }
                );

                // Change to info cursor if hovering over an optional layer
                let layer_at_pixel = layerAtEventPixel(vm, evt);
                // Compare layer names at pixel with optional layer names and set `hit` property accordingly
                let optional_layer_names = vm.optionalLayers.map((layer) => {
                    return layer.get('name');
                });
                let hit = layer_at_pixel.some(
                    (lyr) => optional_layer_names.indexOf(lyr.get('name')) >= 0
                );

                vm.map.getTargetElement().style.cursor = hit
                    ? 'help'
                    : 'default';

                const width = vm.map.getViewport().clientWidth;
                const height = vm.map.getViewport().clientHeight;
                vm.cursorInLeftHalfOfMap = evt.pixel[0] < width / 2;
                vm.cursorInBottomHalfOfMap = evt.pixel[1] > height / 2;

                if (selected) {
                    vm.featureToast.show();
                } else {
                    vm.featureToast.hide();
                }
            });
        },
        initialiseSingleClickEvent: function () {
            let vm = this;
            vm.map.on('singleclick', function (evt) {
                if (vm.drawing || vm.measuring) {
                    vm.lastPoint = new Feature(new Point(evt.coordinate));
                    return;
                }

                let feature = vm.map.forEachFeatureAtPixel(
                    evt.pixel,
                    function (feature) {
                        return feature;
                    }
                );
                if (feature) {
                    vm.selectFeature(feature);
                }
            });
        },
        copyMouseCoordinatesToClipboard: function () {
            // Get the coordinates from the mouse position control
            const mousePosElem = document.querySelector(
                '.custom-mouse-position'
            );
            const coords = mousePosElem ? mousePosElem.textContent.trim() : '';
            if (coords) {
                navigator.clipboard.writeText(coords);
                let text = `Coordinates "${coords}" copied to clipboard.`;
                if (this.context?.model_name == 'occurrence') {
                    text +=
                        '<br/><br/>Would you like to prepare a new site at these coordinates?';
                    swal.fire({
                        icon: 'success',
                        title: 'Copied!',
                        html: text,
                        showCancelButton: true,
                        showConfirmButton: true,
                        reverseButtons: true,
                        customClass: {
                            confirmButton: 'btn btn-primary',
                            cancelButton: 'btn btn-secondary',
                        },
                        confirmButtonText: 'Yes, prepare site',
                        cancelButtonText: 'No, thanks',
                    }).then((result) => {
                        if (result.isConfirmed) {
                            this.$emit(
                                'prepare-new-site-at-coordinates',
                                coords
                                    .split(',')
                                    .map((c) => parseFloat(c.trim()))
                            );
                        }
                    });
                    return;
                }
                swal.fire({
                    icon: 'success',
                    title: 'Copied!',
                    text: text,
                    timer: 1200,
                    showCancelButton: false,
                    showConfirmButton: false,
                });
            }
        },
        initialiseDoubleClickEvent: function () {
            let vm = this;
            vm.map.on('dblclick', function (evt) {
                vm.redirectingToModelDetails = true;
                evt.stopPropagation();

                let feature = vm.map.forEachFeatureAtPixel(
                    evt.pixel,
                    function (feature) {
                        return feature;
                    }
                );
                if (feature) {
                    let model = feature.getProperties().model;
                    if (!model) {
                        vm.redirectingToModelDetails = false;
                        return;
                    }

                    const isOccurrenceContext =
                        vm.context?.model_name === 'occurrence';
                    const isOccurrenceFeature =
                        isOccurrenceContext &&
                        (model?.model_name === 'occurrence' ||
                            (model?.details_url &&
                                vm.context?.details_url &&
                                model.details_url === vm.context.details_url) ||
                            (model?.id &&
                                vm.context?.id &&
                                model.id === vm.context.id));

                    if (isOccurrenceFeature) {
                        vm.redirectingToModelDetails = false;
                        // Keep the prepare-site flow active without opening a duplicate details page
                        vm.copyMouseCoordinatesToClipboard();
                        return;
                    }

                    let model_path = model.details_url;
                    // Remove trailing slash from urls
                    let pathnames = [
                        window.location.pathname,
                        model.details_url,
                    ].filter((path) => ![undefined, null, ''].includes(path));

                    for (let i = 0; i < pathnames.length; i++) {
                        let path_name = pathnames[i];
                        if (path_name[path_name.length - 1] === '/') {
                            path_name = path_name.slice(0, -1);
                        }
                        pathnames[i] = path_name;
                    }
                    // array remove duplicates
                    pathnames = [...new Set(pathnames)];

                    if (pathnames.length === 1) {
                        console.log('already on model details page');
                        vm.redirectingToModelDetails = false;
                        vm.copyMouseCoordinatesToClipboard();
                    } else {
                        window.open(model_path, '_blank'); // Open in new tab
                        vm.redirectingToModelDetails = false;
                    }
                } else {
                    vm.redirectingToModelDetails = false;
                    vm.copyMouseCoordinatesToClipboard();
                }
            });
        },
        initialiseSelectFeatureEvent: function () {
            let vm = this;
            if (!vm.editable) {
                return null;
            }
            // A basic style for selected polygons
            vm.basicSelectStyle = function (feature) {
                var color = feature.get('color') || vm.defaultColor;
                const type = feature.getGeometry().getType();
                return [vm.createStyle(color, vm.clickSelectStroke, type)];
            };
            // Basic style plus extra circles for vertices to help with modifying
            // See: https://github.com/openlayers/openlayers/issues/3165#issuecomment-71432465
            vm.modifySelectStyle = function (feature) {
                var image = new CircleStyle({
                    radius: 5,
                    fill: null,
                    stroke: new Stroke({ color: 'orange', width: 2 }),
                });
                var color = feature.get('color') || vm.defaultColor;
                return [
                    new Style({
                        image: image,
                        geometry: function (feature) {
                            const type = feature.getGeometry().getType();
                            if (type === 'Point') {
                                return feature.getGeometry();
                            }
                            const coordinates = feature
                                .getGeometry()
                                .getCoordinates()[0];
                            return new MultiPoint(coordinates);
                        },
                    }),
                    vm.createStyle(color, vm.clickSelectStroke, 'Polygon'),
                ];
            };
            // select interaction working on "singleclick"
            const selectSingleClick = new Select({
                style: vm.basicSelectStyle,
                layers: vm.vectorLayersArray,
                wrapX: false,
                condition: function () {
                    // Prevent the interaction's standard select event
                    return false;
                },
            });
            selectSingleClick.on('select', (evt) => {
                if (vm.transforming) {
                    console.log('ignoring select event while transforming');
                    return;
                }
                $.each(evt.selected, function (idx, feature) {
                    console.log(
                        `Selected feature ${feature.getProperties().id}`,
                        toRaw(feature)
                    );
                    // Current features
                    let beforeFeatures = [
                        ...vm.selectedFeatureCollection.getArray(),
                    ];
                    feature.setStyle(vm.basicSelectStyle);
                    // Add to the collection for the purpose of controlling which features can be modified (ModifyFeature)
                    vm.selectedFeatureCollection.push(feature);
                    // Add to undo stack
                    vm.undoredo.push('select feature', {
                        before: new Collection(beforeFeatures, {
                            unique: true,
                        }),
                        after: new Collection(
                            [...vm.selectedFeatureCollection.getArray()],
                            { unique: true }
                        ),
                    });
                });

                $.each(evt.deselected, function (idx, feature) {
                    console.log(
                        `Unselected feature ${feature.getProperties().id}`
                    );
                    // Current features
                    let beforeFeatures = [
                        ...vm.selectedFeatureCollection.getArray(),
                    ];
                    feature.setStyle(undefined);
                    // Remove from the collection for the purpose of controlling which features can be modified (ModifyFeature)
                    vm.selectedFeatureCollection.remove(feature);
                    // Add to undo stack
                    vm.undoredo.push('select feature', {
                        before: new Collection(beforeFeatures, {
                            unique: true,
                        }),
                        after: new Collection(
                            [...vm.selectedFeatureCollection.getArray()],
                            { unique: true }
                        ),
                    });
                });
            });
            // When the map mode changes between draw and anything else, update the style of the selected features
            selectSingleClick.addEventListener('map:modeChanged', (evt) => {
                console.log('map mode changed', evt);
                if (
                    evt.details.new_mode === 'draw' &&
                    evt.details.new_subMode === 'Point'
                ) {
                    vm.setStyleForUnAndSelectedFeatures(vm.modifySelectStyle);
                } else {
                    vm.setStyleForUnAndSelectedFeatures();
                }
            });

            return selectSingleClick;
        },
        initialiseModifyFeatureEvent: function () {
            let vm = this;
            const modify = new ModifyFeature({
                features: vm.selectedFeatureCollection,
                pixelTolerance: vm.pixelTolerance,
                deleteCondition: function (evt) {
                    if (
                        evt.type !== 'pointerdown' ||
                        evt.originalEvent.button !== 2 // Remove vertex on right click
                    ) {
                        return false;
                    }
                    evt.stopPropagation();

                    let f = vm.map.getFeaturesAtPixel(evt.pixel, {
                        hitTolerance: vm.hitTolerance,
                    });
                    if (!f) {
                        return false;
                    }

                    // Get the selected features that are also editable
                    const features = vm.editableSelectedFeatures();

                    features.forEach((feature) => {
                        let coords = feature.getGeometry().getCoordinates();

                        for (let j = 0; j < coords.length; j++) {
                            let coord = coords[j];
                            if (coord.length <= 4) {
                                // Needs three vertices to form a polygon, four because the first and last are the same
                                return false;
                            }

                            for (let k = 0; k < coord.length; k++) {
                                let pxl1 = evt.pixel; // clicked pixel coordinates
                                let pxl2 = vm.map.getPixelFromCoordinate(
                                    coord[k]
                                ); // calculated pixel coordinates

                                // Distance between pixel1 and pixel2
                                let distance = vm.pixelDistance(pxl1, pxl2);
                                if (distance <= vm.pixelTolerance) {
                                    let selectedCoord = coord[k];
                                    console.log('delete coord', selectedCoord);
                                    coord.splice(k, 1);
                                    if (selectedCoord == null) {
                                        return;
                                    }
                                    feature
                                        .getGeometry()
                                        .setCoordinates([coord]);
                                    vm.emitValidateFeature(feature);
                                }
                            }
                        }
                    });
                },
                // A filter that takes a feature and return true if it can be modified
                filter: function (feature) {
                    return vm.editableSelectedFeatures().includes(feature);
                },
                insertVertexCondition: function (evt) {
                    evt.stopPropagation();

                    const f = vm.map.getFeaturesAtPixel(evt.pixel, {
                        hitTolerance: vm.hitTolerance,
                    });
                    if (!f) {
                        return false;
                    }

                    const features = vm
                        .editableSelectedFeatures()
                        .filter((feature) => {
                            return f.includes(feature);
                        });

                    if (features.length > 0) {
                        return true;
                    }
                    return false;
                },
            });

            modify.addEventListener('modifyend', function (evt) {
                console.log('Modify end', evt.features);
                const feature = evt.features[0];
                const original_srid =
                    feature.getProperties().original_geometry.properties.srid;
                let coordinates = feature.getGeometry().getCoordinates();
                if (original_srid != vm.mapSrid) {
                    // Transform the coordinates from the map crs to the user input crs
                    coordinates = transformCoordinates(
                        coordinates,
                        `EPSG:${vm.mapSrid}`,
                        `EPSG:${original_srid}`
                    );
                }

                vm.userCoordinates(feature, coordinates);
                vm.emitValidateFeature(feature);
            });

            return modify;
        },
        initialiseTransform: function () {
            let vm = this;

            const transform = new Transform({
                features: toRaw(vm.editableFeatureCollection),
                hitTolerance: vm.hitTolerance,
            });

            const transformEndCallback = function (evt) {
                evt.features.forEach((feature) => {
                    vm.emitValidateFeature(feature);
                    const original_srid =
                        feature.getProperties().original_geometry.properties
                            .srid;
                    let coordinates = feature.getGeometry().getCoordinates();
                    if (original_srid != vm.mapSrid) {
                        // Transform the coordinates from the map crs to the user input crs
                        coordinates = transformCoordinates(
                            coordinates,
                            `EPSG:${vm.mapSrid}`,
                            `EPSG:${original_srid}`
                        );
                    }
                    vm.userCoordinates(feature, coordinates);
                });
            };

            for (const eventName of ['translateend', 'rotateend', 'scaleend']) {
                transform.addEventListener(eventName, transformEndCallback);
            }

            return transform;
        },
        undoLeaseLicensePoint: function () {
            let vm = this;
            if (vm.lastPoint) {
                vm.layerSources[vm.defaultQueryLayerName].removeFeature(
                    vm.lastPoint
                );
                vm.lastPoint = null;
                vm.sketchCoordinates = [[]];
                vm.selectedFeatureId = null;
            } else {
                vm.drawPolygonsForModel.removeLastPoint();
            }
        },
        redoLeaseLicensePoint: function () {
            let vm = this;

            const sketchLineGeom =
                vm.drawPolygonsForModel.sketchLine_?.getGeometry();
            let coordinates = vm.drawPolygonsForModel.sketchCoords_[0];
            // Redo finish coordinate
            let finishCoordinate = vm.sketchCoordinates.slice(-1);

            if (sketchLineGeom !== undefined) {
                // No geometry, only the first sketch coordinates point present
                sketchLineGeom.setCoordinates([coordinates]);
            }

            if (finishCoordinate) {
                vm.drawPolygonsForModel.appendCoordinates(finishCoordinate);
            }

            vm.drawPolygonsForModel.updateSketchFeatures_();
        },
        /**
         * Perform a spatial operation on the selected features
         * @param {String} operation The operation to perform on the selected features
         * @param {Array=} parameters The parameters to pass to the operation
         */
        processFeatures: async function (
            operation,
            parameters = [],
            unit = null
        ) {
            this.processingFeatures = true;

            if (!unit) {
                // Note: Find a better way to determine the unit if not set
                console.warn('Unit not set, defaulting to degrees');
                unit = 'deg';
            }
            const selectedFeatures = this.selectedFeatures();
            if (selectedFeatures.length === 0) {
                console.warn('No features selected');
                return;
            }
            console.log('Buffering features', selectedFeatures);
            const format = new GeoJSON();
            const features = [];

            for (let feature of selectedFeatures) {
                let geomStr = format.writeGeometry(feature.getGeometry());
                features.push(JSON.parse(geomStr));
            }

            const featureCollection = {
                type: 'FeatureCollection',
                features: features.map((geom) => {
                    return {
                        type: 'Feature',
                        geometry: geom,
                    };
                }),
            };

            let success = false;
            let errorStr = '';
            let endpoint = '';
            if (this.context?.model_name == 'occurrencereport') {
                endpoint = api_endpoints.occurrence_report;
            } else if (this.context?.model_name == 'occurrence') {
                endpoint = api_endpoints.occurrence;
            } else {
                console.error('processFeatures: invalid context');
                return ''; // Should not reach here.
            }
            const processedGeometry = await fetch(
                helpers.add_endpoint_join(
                    endpoint,
                    `/spatially-process-geometries/?geometry=${JSON.stringify(
                        featureCollection
                    )}&operation=${operation}&parameters=${parameters.join(
                        ','
                    )}&unit=${unit}`
                )
            )
                .then(async (response) => {
                    if (!response.ok) {
                        return await response.json().then((json) => {
                            throw new Error(json);
                        });
                    }
                    return response.json();
                })
                .then((data) => {
                    const empty = data.features.some((feature) => {
                        return feature.geometry.coordinates.length == 0;
                    });
                    if (empty) {
                        throw new Error('Operation returned an empty geometry');
                    }
                    success = true;
                    return data;
                })
                .catch((error) => {
                    errorStr = error;
                    console.error('Error processing geometry:', error);
                })
                .finally(() => {
                    swal.fire({
                        title: success
                            ? 'Processing Successful'
                            : 'Processing Failed',
                        icon: success ? 'success' : 'error',
                        text: success ? '' : JSON.stringify(errorStr),
                        timer: success ? 1200 : 0,
                        showConfirmButton: !success,
                        timerProgressBar: success,
                    }).then(() => {
                        if (success) {
                            for (let feature of processedGeometry.features) {
                                feature.properties.color =
                                    this.getRandomColor();
                                feature.properties.geometry_source =
                                    this.spatialOperationsAvailable.filter(
                                        (op) => op.id === operation
                                    )[0].name;
                            }

                            if (!this.defaultProcessedGeometryLayerName) {
                                return;
                            }
                            const features = this.addFeatureCollectionToMap(
                                processedGeometry,
                                this.layerSources[
                                    this.defaultProcessedGeometryLayerName
                                ]
                            );
                            this.displayAllFeatures(features);
                        }
                        this.processingFeatures = false;
                    });
                });

            return processedGeometry;
        },
        removeModelFeatures: function () {
            console.log('Removing model features');
            let vm = this;
            let cannot_delete_features = [];
            console.log('selectedFeatureIds', vm.selectedFeatureIds);
            console.log('Selected features:', vm.selectedFeatures());
            const features = vm.selectedFeatures().filter((feature) => {
                if (
                    [undefined, false].includes(
                        feature.getProperties().locked
                    ) ||
                    vm.debug // Allow deletion of locked features if debug mode is enabled
                ) {
                    return feature;
                } else {
                    console.warn(
                        `Cannot delete feature. ${
                            feature.getProperties().id
                        } is locked`
                    );
                    cannot_delete_features.push(feature.getProperties().id);
                }
            });
            console.log('Features to delete:', features);

            console.log('Cannot delete features:', cannot_delete_features);
            if (cannot_delete_features.length > 0) {
                vm.errorMessageProperty(null);
                vm.errorMessageProperty(
                    `Cannot delete feature(s) ${cannot_delete_features.join(
                        ', '
                    )} anymore.`
                );
            }

            console.log('Removing feature from layer');

            const layersWithFeatures = vm.getLayersWithFeatures();
            layersWithFeatures.map((layer) => {
                const source = layer.getSource();
                features.map((feature) => {
                    if (source.hasFeature(feature)) {
                        vm.deletedFeaturesProperty(feature);
                        source.removeFeature(feature);
                    }
                });
            });

            // A list of selected features ol uids
            const featuresOlUids = features.map((feature) => {
                return Number(feature.ol_uid);
            }, []);
            // Remove selected features from `selectedFeatureCollection`
            [...vm.selectedFeatureCollection.getArray()].forEach((feature) => {
                if (
                    feature &&
                    featuresOlUids.includes(Number(feature.ol_uid))
                ) {
                    console.log(`Removing feature ${feature.ol_uid}`);
                    vm.selectedFeatureCollection.remove(feature);
                }
            });
        },
        /**
         * Adds a GeoJSON feature collection to the map
         * @param {Object} featureCollection A GeoJSON feature collection object to add to the map
         * @param {Object=} layerSource The layer source to add the features to, defaults to the query layer source
         */
        addFeatureCollectionToMap: function (featureCollection, layerSource) {
            let vm = this;
            if (!layerSource) {
                layerSource = vm.layerSources[vm.defaultQueryLayerName];
            }
            if (featureCollection == null) {
                featureCollection = vm.featureCollection;
            }
            const features = [];
            console.log('Adding features to map:', featureCollection);

            for (let featureData of featureCollection['features']) {
                let feature = vm.featureFromDict(
                    featureData,
                    featureData.model
                );

                features.push(feature);
                layerSource.addFeature(feature);
            }

            return features;
        },
        /**
         * Assigns colors to proposals
         * @param {Array} proposals An array of proposals to assign colors to
         */
        assignProposalFeatureColors: function (proposals, layerDef = null) {
            let vm = this;
            if (!layerDef) {
                layerDef = {};
            }
            if (Array.isArray(proposals)) {
                // Assign a random color to each proposal
                proposals.forEach(function (proposal) {
                    const properyOverwrite = this.property_overwrite || {};
                    const color = vm.colorPickOrRandom(
                        properyOverwrite.color,
                        proposal.color
                    );
                    const stroke = vm.colorPickOrRandom(
                        properyOverwrite.stroke,
                        proposal.stroke
                    );
                    proposal.color = color;
                    proposal.stroke = stroke;
                }, layerDef);
            } else {
                // Assume that "proposals" is a feature collection
                if (!proposals.features) {
                    console.error(
                        'Proposals must be an array or a feature collection. Skipping...'
                    );
                    return;
                }
                proposals.features.forEach(function (feature) {
                    if (!feature.properties) {
                        console.warn(
                            'Feature has no properties. Creating...',
                            feature
                        );
                        feature.properties = {};
                    }
                    const properyOverwrite = this.property_overwrite || {};
                    const color = vm.colorPickOrRandom(
                        properyOverwrite.color,
                        feature.properties.color
                    );
                    const stroke = vm.colorPickOrRandom(
                        properyOverwrite.stroke,
                        feature.properties.stroke
                    );
                    feature.properties.color = color;
                    feature.properties.stroke = stroke;
                }, layerDef);
            }
        },
        /**
         * Loads a list of proposals as new map features
         * @param {Object} proposals The proposals to load as new map features
         * @param {String=} toSource The layer source to load the features to
         */
        loadMapFeatures: function (proposals, toSource = null) {
            const vm = this;
            const source =
                vm.layerSources[toSource] ||
                vm.layerSources[vm.defaultQueryLayerName];
            // If no geometry name is provided, assume the geometry is the proposals object itself
            const geometry_name =
                vm.getLayerDefinitionByName(toSource)?.geometry_name || null;

            console.log(`Loading features to source ${toSource}`, proposals);
            let opacities = [];
            // Remove all features from the layer
            source.clear();
            if (geometry_name) {
                proposals.forEach(function (proposal) {
                    const geometry = proposal[geometry_name];
                    if (!geometry) {
                        console.warn(
                            `Proposal ${proposal.id} has no geometry named ${geometry_name}. Skipping...`
                        );
                        return;
                    }
                    const propertyOverwrite =
                        this.getLayerDefinitionByName(toSource)
                            .property_overwrite || {};

                    const properties = Object.fromEntries(
                        Object.entries(propertyOverwrite).map(([k, v]) => [
                            k,
                            typeof v === 'function' ? v() : v,
                        ])
                    );
                    // Get the name of the identifier from the layer def, if exists
                    const identifierName =
                        this.getLayerDefinitionByName(toSource)
                            .identifier_name || null;
                    // Add the identifier to the properties
                    properties['identifier_name'] = identifierName;
                    properties['can_hide_geometries'] =
                        this.getLayerDefinitionByName(toSource)
                            .can_hide_geometries === true;
                    for (const [key, value] of Object.entries(proposal)) {
                        this.addFeatureDisplayPropertyValue(
                            key,
                            value,
                            null, // feature
                            properties,
                            null, // propertyMap
                            propertyOverwrite
                        );
                    }
                    opacities = vm.addGeometryToMapSource(
                        geometry,
                        properties,
                        source
                    );
                }, vm);
            } else {
                const propertyOverwrite =
                    vm.getLayerDefinitionByName(toSource).property_overwrite ||
                    {};
                const identifierName =
                    this.getLayerDefinitionByName(toSource).identifier_name ||
                    null;
                propertyOverwrite['identifier_name'] = identifierName;
                // The following line can lead to circular references
                propertyOverwrite['layer_definition'] =
                    this.getLayerDefinitionByName(toSource).name;
                opacities = vm.addGeometryToMapSource(
                    proposals,
                    propertyOverwrite,
                    source
                );
            }

            // Set the opacity of the associated layer
            if (opacities.length > 0) {
                const layer = this.vectorLayers[toSource];
                if (layer) {
                    layer.setOpacity(opacities[0]);
                }
            }
            // Include the layer name in the event details so parent components can identify
            // which layer triggered the event (used to prevent infinite recursion in handlers)
            vm.map.dispatchEvent({
                type: 'features-loaded',
                details: {
                    loaded: true,
                    layerName: toSource,
                },
            });
        },
        /**
         * Adds a geometry object to a layer source
         * @param {Object} geometry A geometry object, e.g. a dictionary
         * @param {Object} modelObj A model object, e.g. a proposal
         * @param {Object} source A layer source object, e.g. a VectorSource
         */
        addGeometryToMapSource: function (geometry, modelObj, source) {
            const vm = this;
            if (!geometry.features) {
                console.warn(
                    `modelObj ${modelObj.id} geometry has no features. Skipping...`
                );
                return;
            }
            const opacities = [];
            geometry.features.forEach(function (featureData) {
                if (!featureData) {
                    console.warn(
                        `No data for this geometry feature: ${featureData}. Skipping...`
                    );
                    return;
                }
                if (!featureData.geometry) {
                    console.warn(
                        `Feature ${featureData.id} has no geometry. Skipping...`
                    );
                    return;
                }
                let feature = vm.featureFromDict(featureData, modelObj);
                if (source.getFeatureById(feature.getId())) {
                    console.warn(
                        `Feature ${feature.getId()} already exists in the source. Skipping...`
                    );
                    return;
                }
                const opacity = feature.getProperties().opacity;
                if (
                    typeof opacity === 'number' &&
                    opacity >= 0 &&
                    opacity <= 1
                ) {
                    opacities.push(feature.getProperties().opacity);
                }
                source.addFeature(feature);
            });

            return opacities;
        },
        addTileLayers: function () {
            let vm = this;
            for (let tileLayer of this.optionalLayers) {
                console.log('Adding optional layer', tileLayer);
                vm.map.addLayer(tileLayer);
                tileLayer.on('change:visible', function (e) {
                    if (e.oldValue == false) {
                        $('#legend')
                            .find('img')
                            .attr('src', this.values_.legend_url);
                        $('#legend_title').text(this.values_.title);
                    } else if (e.oldValue == true) {
                        $('#legend_title').text('');
                        $('#legend').find('img').attr('src', '');
                        // Hide any overlays when the optional layer is turned off
                        vm.overlay(undefined, undefined);
                    } else {
                        console.error(
                            'Cannot assess tile layer visibility change.'
                        );
                    }
                });
            }
            // Lets ol display a popup with clicked feature properties
            vm.map.on('singleclick', function (evt) {
                if (vm.mode !== 'info') {
                    return;
                }
                let coordinate = evt.coordinate;
                layerAtEventPixel(vm, evt).forEach((lyr) => {
                    console.log('Clicked on tile layer', lyr);
                    queryLayerAtPoint(vm, lyr, coordinate);
                });
            });
        },
        onDrawEnd: function (feature) {
            let vm = this;
            console.log('drawend', feature.values_.geometry.flatCoordinates);

            vm.setFeaturePropertiesFromContext(feature);
            if (vm.isPolygonLikeFeature(feature)) {
                feature.set('area_sqm', vm.featureArea(feature));
            }
            console.log('newFeatureId = ' + vm.newFeatureId);
            vm.lastPoint = feature;
            vm.sketchCoordinates = [[]];
        },
        /**
         * Sets the properties of a feature from a context model object
         * @param {Feature} feature The feature object
         * @param {Proxy} context The model object
         * @param {Object=} properties Additional properties to set oder overwrite
         * @param {Style=} style The style object
         * @returns {Feature} The feature object with updated properties
         */
        setFeaturePropertiesFromContext: function (
            feature,
            context,
            properties = {},
            style = null
        ) {
            if (!context) {
                context = this.context || {};
            }
            // Add original_geometry for list of geometries and modification of geom parameters
            const coords = feature.getGeometry().getCoordinates();
            const original_geometry = properties.original_geometry || {
                coordinates: coords,
                properties: { srid: this.mapSrid },
            };

            const color =
                context.color ||
                properties.color ||
                this.featureColors['draw'] ||
                this.featureColors['unknown'] ||
                this.defaultColor;
            const stroke =
                properties.stroke || context.stroke || this.defaultColor;

            const label = properties.label || context.label || 'Draw';

            // Apply the passed in properties to the feature, but overwrite where necessary (nullish coalescing operator ??=)
            const featureProperties = structuredClone(toRaw(properties));
            featureProperties['can_hide_geometries'] =
                context['can_hide_geometries'] || false;
            // Make sure all features have an id under the same key
            featureProperties['identifier_value'] =
                context[context.identifier_name] ||
                properties[context.identifier_name] ||
                label;
            featureProperties['id'] ??= this.newFeatureId;
            featureProperties['model'] ??= context;
            featureProperties['geometry_source'] ??=
                properties.geometry_source === null ? null : 'New';
            featureProperties['name'] ??= context.id || -1;
            featureProperties['label'] ??= label;
            featureProperties['color'] ??= color;
            featureProperties['stroke'] ??= stroke;
            featureProperties['srid'] ??= this.mapSrid;
            featureProperties['original_geometry'] ??= original_geometry;

            feature.setProperties(featureProperties);

            // eslint-disable-next-line no-unused-vars
            const featureStyleFunction = (feature, resolution) => {
                if (!style) {
                    return this.createFeatureStyle(feature);
                }
                return style;
            };
            feature.setStyle(featureStyleFunction);
            this.newFeatureId++;

            return feature;
        },
        /**
         * Creates a styled feature object from a feature dictionary
         * @param {dict} featureData A feature dictionary
         * @param {Proxy} model A model object
         */
        featureFromDict: function (featureData, model) {
            let vm = this;
            if (model == null) {
                model = vm.context || {};
            }

            if (!featureData.properties['color']) {
                featureData.properties['color'] = vm.styleByColor(
                    featureData,
                    model
                );
            }

            const type = vm.getFeatureType(featureData);
            // let style = vm.createStyle(color, vm.defaultColor, type);
            let geometry;
            if (type === 'Polygon') {
                geometry = new Polygon(featureData.geometry.coordinates);
            } else if (type === 'MultiPolygon') {
                geometry = new MultiPolygon(featureData.geometry.coordinates);
            } else if (['MultiPoint', 'Point'].includes(type)) {
                if (type === 'Point') {
                    geometry = new Point(featureData.geometry.coordinates);
                } else if (type === 'MultiPoint') {
                    geometry = new MultiPoint(featureData.geometry.coordinates);
                }
            } else if (type === 'LineString') {
                alert('LineString not yet supported');
            } else if (type === 'GeometryCollection') {
                alert(
                    'GeometryCollection not yet supported. Consider using MultiPolygon or similar.'
                );
            } else {
                console.error(`Unsupported geometry type ${type}`);
            }

            let feature = new Feature({
                geometry: geometry,
            });

            // to remove the ocr_geometry as it shows up when the geometry is downloaded
            let propertyModel = model;
            delete propertyModel.ocr_geometry;

            vm.setFeaturePropertiesFromContext(
                feature,
                propertyModel,
                featureData.properties
            );

            if (featureData.id) {
                // Id of the model object (https://datatracker.ietf.org/doc/html/rfc7946#section-3.2)
                feature.setId(featureData.id);
            }
            this.userInputGeometryStackAdd(feature);

            console.log(feature);

            return feature;
        },
        getProposalById: function (id) {
            return this.proposals.find((proposal) => proposal.id === id);
        },
        getRandomColor: function () {
            let letters = '0123456789ABCDEF';
            let color = '#';
            for (let i = 0; i < 6; i++) {
                color += letters[Math.floor(Math.random() * 16)];
            }
            return color;
        },
        getRandomRGBAColor: function () {
            var o = Math.round,
                r = Math.random,
                s = 255;
            return [o(r() * s), o(r() * s), o(r() * s), 0.5];
        },
        /**
         * Returns a stringified GeoJSON representation of the features in the requested layer
         * @param {String=} layer_name The name of the layer to query as defined in a layer-definition prop,
         * defaults to the default query layer
         */
        getJSONFeatures: function (layer_name) {
            if (!layer_name) {
                layer_name = this.defaultQueryLayerName;
            }
            const format = new GeoJSON();
            if (!this.layerSources[layer_name]) {
                // Just adding this to cover the case of the user quickly
                // pressing save and exit button before the map is fully loaded
                // and the layer sources are not yet available
                console.error(
                    `Layer source ${layer_name} not found. Cannot get features.`
                );
                return;
            }
            const layerFeatures = this.layerSources[layer_name].getFeatures();

            const features = [];
            layerFeatures.forEach(function (f) {
                // Keep the feature-id of an existing feature
                const featureId = f.getId();
                const feature = f.clone();
                feature.unset('model');
                feature.setId(featureId);
                features.push(feature);
            });

            return format.writeFeatures(features);
        },
        /**
         * Returns a dictionary of query parameters for a given layer
         * @param {String} layerStr The dictionary key containing the layer information
         */
        queryParamsDict: function (layerStr) {
            let vm = this;

            if (!vm.owsQuery) {
                console.error('OWS query defintion not found');
                return {};
            }

            if (!(layerStr in vm.owsQuery)) {
                console.error(`Layer ${layerStr} not found in OWS query`);
                return {};
            }
            if (!vm.owsQuery[layerStr].typeName) {
                console.error(`Layer ${layerStr} needs a typeName`);
                return {};
            }

            return {
                service: vm.owsQuery.service || 'WFS',
                version: vm.owsQuery.version || '1.0.0',
                request: vm.owsQuery[layerStr].request || 'GetFeature',
                typeName: vm.owsQuery[layerStr].typeName,
                maxFeatures: vm.owsQuery[layerStr].maxFeatures || '5000',
                srsName: vm.owsQuery[layerStr].srsName || `EPSG:${vm.mapSrid}`,
                outputFormat:
                    vm.owsQuery[layerStr].outputFormat || 'application/json',
                propertyName:
                    vm.owsQuery[layerStr].propertyName || 'wkb_geometry',
            };
        },
        emitValidateFeature: function (feature) {
            console.log('Emitting validate feature', feature);
            this.$emit('validate-feature', feature);
        },
        finishDrawing: function () {
            this.queryingGeoserver = false;
            this.errorMessage = null;
            // eslint-disable-next-line no-unused-vars
            const feature = this.drawPolygonsForModel.finishDrawing();
            this.setLoadingMap(false);
            if (this.mode == 'draw' && this.selectedFeatureIds.length == 0) {
                this.callSetMode('layer');
            }
        },
        /**
         * Returns the current error message or sets it to the provided message.
         * @param {String} message The new error message
         */
        errorMessageProperty: function (message) {
            let vm = this;
            if (message === undefined) {
                return vm.errorMessage;
            }

            vm.queryingGeoserver = false;

            // Only overwrite the current message if the new message is null (removes the message)
            // Or the current message is null (no message is set)
            if (message === null || vm.errorMessage === null) {
                vm.errorMessage = message;
            }
        },
        /**
         * Sets or unsets overlay feature information bubble
         * @param {Array} coordinate clicked coordinate pair
         * @param {Dict} feature clicked feature properties or undefined
         */
        overlay: function (coordinate, feature) {
            let vm = this;
            let overlay = toRaw(vm.map.overlays_.array_[0]);
            if (feature === undefined) {
                vm.overlayFeatureInfo = {};
            } else {
                vm.overlayFeatureInfo = feature.getProperties();
                vm.overlayFeatureInfo['clickedCoordinate'] = coordinate;
                vm.overlayFeatureInfo['featureId'] = feature.getId();
            }
            if (overlay) {
                console.log('setting overlay position to ', coordinate);
                overlay.setPosition(coordinate);
            }

            return overlay;
        },
        deletedFeaturesProperty: function (feature) {
            let vm = this;
            if (feature === undefined) {
                return vm.deletedFeatures;
            } else {
                vm.deletedFeatures.push(feature);
            }
        },
        validate_map_docs: function () {
            var vm = this;
            var formData = new FormData();
            vm.errorString = '';
            const options = {
                method: 'POST',
                body: formData,
            };
            vm.isValidating = true;
            let endpoint = '';
            if (this.context?.model_name == 'occurrencereport') {
                endpoint = api_endpoints.occurrence_report;
            } else if (this.context?.model_name == 'occurrence') {
                endpoint = api_endpoints.occurrence;
            } else {
                console.error('validate_map_docs: invalid context');
                return ''; // Should not reach here.
            }
            fetch(
                helpers.add_endpoint_join(
                    endpoint,
                    `/${vm.context.id}/validate_map_files/`
                ),
                options
            )
                .then(async (response) => {
                    if (!response.ok) {
                        const data = await response.json();
                        throw new Error(JSON.stringify(data));
                    } else {
                        return response.json();
                    }
                })
                .then((data) => {
                    vm.$emit('refreshFromResponse', data);
                    // Once the shapefile is converted to a proposal geometry the files are deleted
                    // so calling this will remove the file list from the front end
                    vm.$refs.shapefile_document.get_documents();
                    vm.$nextTick(() => {
                        vm.loadMapFeatures(
                            [data],
                            this.queryLayerDefinition.name
                        );
                        vm.displayAllFeatures();
                        swal.fire({
                            title: 'Success',
                            text: 'The shapefile has been processed successfully.',
                            icon: 'success',
                            customClass: {
                                confirmButton: 'btn btn-primary',
                            },
                        });
                    });
                })
                .catch((error) => {
                    console.log(error);
                    vm.errorString = helpers.apiVueResourceError(error);
                    swal.fire({
                        title: 'Validation Error',
                        text: error,
                        icon: 'error',
                    });
                })
                .finally(() => {
                    vm.isValidating = false;
                });
        },
        getLayersWithFeatures: function () {
            const layers = [];
            if (!this.map) {
                return layers;
            }

            const mapLayers = this.map.getLayers();
            mapLayers.getArray().map((layer) => {
                // Not all types of layer have a getSource method
                try {
                    layer.getSource().getFeatures();
                    layers.push(layer);
                } catch {
                    //
                }
            }, layers);

            return layers;
        },
        /**
         * Returns all features across all layers
         */
        getMapFeatures: function () {
            const features = [];
            this.getLayersWithFeatures().map((layer) => {
                features.push(...layer.getSource().getFeatures());
            });

            return features;
        },
        /**
         * Returns the editable features
         */
        editableFeatures: function () {
            let vm = this;
            const features = vm.getMapFeatures();
            const editableFeaturesIds = vm.editableFeatureCollection
                .getArray()
                .map((feature) => {
                    return feature.getProperties().id;
                });

            return features.filter((feature) => {
                return editableFeaturesIds.includes(feature.getProperties().id);
            });
        },
        /**
         * Returns the selected features
         */
        selectedFeatures: function () {
            let vm = this;
            const features = vm.getMapFeatures();
            console.log('features:', features);
            return features.filter((feature) => {
                return vm.selectedFeatureIds.includes(
                    feature.getProperties().id
                );
            });
        },
        /**
         * Returns features that are both selectable and editable
         */
        editableSelectedFeatures: function () {
            const selectedFeatures = this.selectedFeatures();
            const editableFeatures = this.editableFeatures();
            return selectedFeatures.filter((feature) => {
                return editableFeatures.includes(feature);
            });
        },
        setSelectedFeatureCollection: function (collection) {
            this.selectedFeatureCollection.clear();
            collection.forEach((feature) => {
                this.selectedFeatureCollection.push(feature);
            });
        },
        /**
         * Sets interactions for modify to active or inactive
         * @param {boolean} active
         */
        modifySetActive(active) {
            let vm = this;
            if (vm.editable) {
                vm.modify.setActive(active);
            }
            vm.snap.setActive(active);
        },
        /**
         * Sets interactions for modify to active or inactive
         */
        transformSetActive(active) {
            let vm = this;
            if (!vm.editable) {
                return;
            }
            vm.select.setActive(!active);
            vm.transform.setActive(active);
        },
        /**
         * Undoes the last map interaction
         */
        undo: function () {
            let vm = this;
            // Need to do double check here
            if (vm.canUndoDrawnVertex) {
                vm.undoredo_forSketch.undo();
            } else if (vm.canUndoAction) {
                vm.undoredo.undo();
                // Find the last feature in the redo stack and validate it (the last feature doesn't necessarily need to be the last item in the stack, as the last item could e.g. be a 'blockend' object)
                let item = vm.undoredo._redoStack
                    .getArray()
                    .toReversed() // .reverse() mutates in-place, .toReversed() doesn't
                    .find((item) => {
                        if (item.feature) {
                            return item;
                        }
                    });
                if (item && item.feature) {
                    vm.finishDrawing();
                }
            } else {
                // Nothing
            }
        },
        /**
         * Redoes the last map interaction
         */
        redo: function () {
            let vm = this;
            if (vm.canRedoDrawnVertex) {
                vm.undoredo_forSketch.redo();
            } else if (vm.canRedoAction) {
                vm.undoredo.redo();
                // Find the last feature in the undo stack and validate it
                let item = vm.undoredo._undoStack
                    .getArray()
                    .toReversed() // .reverse() mutates in-place, .toReversed() doesn't
                    .find((item) => {
                        if (item.feature) {
                            return item;
                        }
                    });
                if (item && item.feature) {
                    vm.finishDrawing();
                }
            } else {
                // Nothing
            }
        },
        /**
         * Returns a description for the top action in the undo or redo stack
         * @param {String} stack_name The name of the stack to get the top action from
         */
        undoRedoStackTopInteractionName: function (stack_name = 'undo') {
            let vm = this;
            if (!vm.undoredo) {
                return;
            }
            let stack = vm.undoredo.getStack(stack_name);

            if (stack && stack.length > 0) {
                return (
                    (stack.slice(-1)[0] || []).name ||
                    (stack.slice(-1)[0] || []).type
                );
            }
            return 'last action';
        },
        /**
         * Toggles a given element's visibility (like a submenu) on or off
         * and highlights the respective menu button that toggles the submenu
         * @param {String} elementId The id of the element to toggle visibility on or off
         * @param {Object} button  The html element that was clicked to toggle the element
         */
        toggleElementVisibility: function (elementId, button) {
            const element = document.getElementById(elementId);
            // Must no necessarily be the button, can be an icon it
            const btn = $(button).closest('.btn');

            if (element.style.display !== 'block') {
                element.style.display = 'block';
                btn.addClass('optional-layers-button-active');
            } else {
                element.style.display = 'none';
                if (btn.hasClass('optional-layers-button-active')) {
                    btn.removeClass('optional-layers-button-active');
                }
            }
        },
        shapeFilesUpdated: function () {
            let numShapefiles =
                this.$refs.shapefile_document?.numDocuments || 0;
            this.numShapefiles = numShapefiles;
            this.uploadedFileTypes =
                this.$refs.shapefile_document.documents.map((doc) => {
                    return doc.name.slice(doc.name.lastIndexOf('.'));
                }, []);
        },
        featureArea: function (feature, projection = null) {
            const geometry = feature.getGeometry();
            if (!geometry) {
                console.error('Feature has no geometry');
                return null;
            }
            if (!projection) {
                projection = `EPSG:${this.mapSrid}`;
            }
            return Math.round(
                getArea(geometry, {
                    projection: projection,
                })
            );
        },
        getShpExtensionIdxFromDict: function (dict, ext) {
            return ext in dict ? dict[ext] : null;
        },
        processDatatransferEvent: function (evt) {
            let vm = this;
            // Array to store dropped shapefiles
            const shapeFiles = [];
            if (evt.dataTransfer.items) {
                // Use DataTransferItemList interface to access the file(s)
                // eslint-disable-next-line no-unused-vars
                [...evt.dataTransfer.items].forEach(async (item, i) => {
                    // If dropped items aren't files, reject them
                    if (item.kind === 'file') {
                        const file = item.getAsFile();
                        const fileType = file.name.slice(-4);

                        if (vm.shapefileTypesAllowed.includes(fileType)) {
                            // Non-compressed list of files
                            shapeFiles.push(file);
                        } else if (vm.archiveTypesAllowed.includes(fileType)) {
                            // Compressed archive
                            await file.arrayBuffer().then(async (buffer) => {
                                await shp(buffer)
                                    .then((geojson) => {
                                        console.log(geojson);
                                        vm.addFeatureCollectionToMap(geojson);
                                    })
                                    .catch((error) => {
                                        swal.fire({
                                            title: 'Error',
                                            text: error,
                                            icon: 'error',
                                        });
                                    });
                            });
                        } else {
                            // Nothing
                        }
                    }
                });

                if (!shapeFiles.length) {
                    return;
                }

                // Map of shapefile extensions to their index in the shapeFiles/shapeBuffers array
                vm.shpExtensionIdxMap = {};
                let shapeBuffers = shapeFiles.map((shp, idx) => {
                    console.log(this.shpExtensionIdxMap);
                    this.shpExtensionIdxMap[shp.name.slice(-4)] = idx;
                    return shp.arrayBuffer();
                });

                Promise.all(shapeBuffers).then(async (buffers) => {
                    const dict = vm.shpExtensionIdxMap;
                    const shpIdx = vm.getShpExtensionIdxFromDict(dict, '.shp');
                    const prjIdx = vm.getShpExtensionIdxFromDict(dict, '.prj');
                    const dbfIdx = vm.getShpExtensionIdxFromDict(dict, '.dbf');
                    // Note: why doesn't shape2json use index files?
                    // eslint-disable-next-line no-unused-vars
                    const shxIdx = vm.getShpExtensionIdxFromDict(dict, '.shx');
                    // Char-set / encoding description file
                    const cpgIdx = vm.getShpExtensionIdxFromDict(dict, '.cpg');

                    if (!buffers[shpIdx] || !buffers[dbfIdx]) {
                        console.error('No .shp or .dbf file provided');
                        return;
                    }

                    // Using default WGS84 specification if no .prj file is provided
                    let prjStr =
                        '+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs';
                    if (buffers[prjIdx]) {
                        prjStr = new TextDecoder().decode(
                            new Uint8Array(buffers[prjIdx])
                        );
                    }
                    console.log('Using proj4 string:', prjStr);

                    const geojson = combine([
                        parseShp(buffers[shpIdx], prjStr),
                        parseDbf(buffers[dbfIdx], buffers[cpgIdx]),
                    ]);

                    vm.addFeatureCollectionToMap(geojson);
                    console.log('Done loading features');
                    delete vm.shpExtensionIdxMap;
                });
            } else {
                // Use DataTransfer interface to access the file(s)
                [...evt.dataTransfer.files].forEach((file, i) => {
                    console.log(
                        `Implement for files: file[${i}].name = ${file.name}`
                    );
                });
            }
        },
        selectFeature: function (feature) {
            if (this.isFeatureEligibleToHide(feature)) {
                // Prevent selecting hidden features
                return;
            }
            this.map.getInteractions().forEach((interaction) => {
                if (interaction instanceof Select) {
                    let selected = [];
                    let deselected = [];
                    let feature_id = feature.get('id');
                    if (this.selectedFeatureIds.includes(feature_id)) {
                        // already selected, so deselect
                        deselected.push(feature);
                    } else {
                        // not selected, so select
                        // Priya commented the below to avoid the duplication count of 2 on delete button
                        selected.push(feature);
                    }
                    interaction.dispatchEvent({
                        type: 'select',
                        selected: selected,
                        deselected: deselected,
                    });
                }
            });
        },
        createFeatureStyle: function (feature) {
            if (this.isFeatureEligibleToHide(feature)) {
                return new Style({});
            }
            const color = feature.getProperties().color;
            const stroke = feature.getProperties().stroke;
            const type = feature.getGeometry().getType();

            let style = this.createStyle(color, stroke, type, null, 1);
            let rgba = color;
            if (!Array.isArray(color)) {
                rgba = this.colorHexToRgbaValues(color);
            }
            if (['MultiPoint', 'Point'].includes(type)) {
                style = this.createStyle(
                    color,
                    stroke,
                    type,
                    null,
                    null,
                    this.mapMarker,
                    rgba[3]
                );
            }

            return style;
        },
        colorStrToStyle: function (colorStr) {
            let s = new Option().style;
            s.color = colorStr;
            return s;
        },
        isColor: function (colorStr) {
            return this.colorStrToStyle(colorStr).color !== '';
        },
        colorHexToRgbaValues: function (color) {
            let _, rgb, r, g, b, a;

            // Get the rgb hex and alpha values with considering shortcut color hex values
            // eslint-disable-next-line no-unused-vars
            [_, rgb, a] = new RegExp(
                '^[#]?((?:[A-Fa-f0-9]{3}){1,2})([A-Fa-f0-9]{1,2})?$'
            ).exec(color);
            // Convert 3-digit hex shortcuts to 6-digit hex
            rgb =
                rgb.length == 3
                    ? rgb[0] + rgb[0] + rgb[1] + rgb[1] + rgb[2] + rgb[2]
                    : rgb;
            a = a || '00';
            a = a.length == 1 ? a + a : a;

            // Convert hex to r, g, b, a float values
            const rgba = `0x${rgb}${a}`;
            r = (rgba >> 24) & 255;
            g = (rgba >> 16) & 255;
            b = (rgba >> 8) & 255;
            a = (rgba & 255) / 255;

            return [r, g, b, a];
        },
        /**
         * Returns whether an array is one-dimensional
         * @param {Array} array An array
         */
        isOneDimensionalArray: function (array) {
            return array.every((entry) => !Array.isArray(entry));
        },
        toggleHidden: function (target) {
            const hidden = $(target).find(
                'img.svg-icon.hidden, svg.svg-object.hidden'
            );
            const notHidden = $(target)
                .find('img.svg-icon, svg.svg-object')
                .not('.hidden');
            hidden.removeClass('hidden');
            notHidden.addClass('hidden');
        },
        /**
         * Returns the user input coordinates from the list of feature geometries
         * If coordinates is provided, also updates the feature's coordinates.
         * If srid is provided, also updates the feature's srid.
         * @param {Object} feature A feature
         * @param {Array=} coordinates A coordinate pair array
         * @param {Number=} srid The SRID of the coordinates
         */
        featureInputCoordinates: function (feature, coordinates, srid) {
            if (coordinates) {
                this.$refs[
                    `feature-${feature.ol_uid}-latitude-input`
                ][0].value = coordinates[1];
                this.$refs[
                    `feature-${feature.ol_uid}-longitude-input`
                ][0].value = coordinates[0];
            }
            if (srid) {
                this.$refs[`feature-${feature.ol_uid}-crs-select`][0].value =
                    srid;
            }

            const inputLat =
                this.$refs[`feature-${feature.ol_uid}-latitude-input`][0].value;
            const inputLon =
                this.$refs[`feature-${feature.ol_uid}-longitude-input`][0]
                    .value;

            return [Number(inputLon), Number(inputLat)];
        },
        /**
         * Set the coordinates of a feature. Currently only supporting Point and MultiPoint features.
         * @param {object} feature A feature object
         * @param {array} coordinates A coordinate pair array
         */
        setCoordinates: function (feature, coordinates) {
            const isMulti = ['MultiPoint'].includes(
                feature.getGeometry().getType()
            );
            if (isMulti && this.isOneDimensionalArray(coordinates)) {
                feature.getGeometry().setCoordinates([coordinates]);
            } else {
                feature.getGeometry().setCoordinates(coordinates);
            }
        },
        cloneFeature: function (feature, coordinates = null) {
            const clone = feature.clone();
            if (coordinates) {
                this.setCoordinates(clone, coordinates);
            }
            return clone;
        },
        transformFeature: async function (feature, srid_from, srid_to) {
            const format = new GeoJSON();
            const geomStr = format.writeGeometry(feature.getGeometry());
            let endpoint = '';
            if (this.context?.model_name == 'occurrencereport') {
                endpoint = api_endpoints.occurrence_report;
            } else if (this.context?.model_name == 'occurrence') {
                endpoint = api_endpoints.occurrence;
            } else {
                console.error('transformFeature: invalid context');
                return ''; // Should not reach here.
            }
            const transformed = await fetch(
                helpers.add_endpoint_join(
                    endpoint,
                    `/transform-geometry/?geometry=${geomStr}&from=${srid_from}&to=${srid_to}`
                )
            )
                .then((response) => {
                    if (!response.ok) {
                        throw new Error('Network response was not ok');
                    }
                    return response.json();
                })
                .then((data) => {
                    return data;
                })
                .catch((error) => {
                    console.error(
                        'Error coordinate transforming geometry:',
                        error
                    );
                });
            return transformed;
        },
        updateUserInputBufferRadius: function (feature, radius) {
            feature.set('buffer_radius', radius);
        },
        /**
         * Updates the user input coordinates and srid that are stored on the feature as original_geometry
         * @param {Object} feature A feature
         * @param {Number} srid The SRID of the feature
         */
        updateUserInputGeoData: function (feature, srid) {
            if (!srid) {
                srid = feature.getProperties().srid;
            }
            this.transformToMapCrs(feature, srid).then((coordinates) => {
                // Update the user input coordinates and srid
                this.userCoordinates(feature, coordinates, srid);
            });
        },
        transformToMapCrs: async function (feature, srid) {
            const newSrid = Number(srid);
            if (!newSrid) {
                console.warn(`Invalid SRID. ${srid} is not a number.`);
                return;
            }

            if (!this.isPointLikeFeature(feature)) {
                this.errorMessageProperty(
                    `Feature type ${feature
                        .getGeometry()
                        .getType()} is not yet supported for transformation.`
                );
                return;
            }

            // Point coordinates
            const inputCoordinates = this.featureInputCoordinates(feature);

            // Store the new srid in the feature for backend transformation
            feature.set('srid', newSrid);
            if (newSrid === this.mapSrid) {
                console.log('No need to transform');
                this.setCoordinates(feature, inputCoordinates);
                return inputCoordinates;
            }

            const selectComponent =
                this.$refs[`feature-${feature.ol_uid}-crs-select`][0].$refs
                    .vueSelectFilter;
            selectComponent.toggleLoading(true);

            const transformFeature = this.cloneFeature(
                feature,
                inputCoordinates
            );

            const transformed = await this.transformFeature(
                transformFeature,
                newSrid,
                this.mapSrid
            );

            console.log('coordinates after', transformed);
            if (!transformed) {
                console.error('No transformed coordinates');
                selectComponent.toggleLoading(false);
                return inputCoordinates;
            }
            this.setCoordinates(feature, transformed.coordinates);
            selectComponent.toggleLoading(false);

            return inputCoordinates;
        },
        isMultiPointFeature: function (feature) {
            return feature.getGeometry().getType() === 'MultiPoint';
        },
        isPointLikeFeature: function (feature) {
            return ['Point', 'MultiPoint'].includes(
                feature.getGeometry().getType()
            );
        },
        isMultiPolygonFeature: function (feature) {
            return feature.getGeometry().getType() === 'MultiPolygon';
        },
        isPolygonLikeFeature: function (feature) {
            return ['Polygon', 'MultiPolygon'].includes(
                feature.getGeometry().getType()
            );
        },
        isOriginalGeometryCrsProjected: function (feature) {
            return feature.getProperties().original_geometry.properties
                ?.crs_projected;
        },
        userInputGeometryStackAdd: function (feature) {
            const original_geometry = feature.getProperties().original_geometry;
            const clone = structuredClone(original_geometry);
            clone['ol_uid'] = feature.ol_uid;
            this.userInputGeometryStack.push(clone);
        },
        userInputGeometryStackLast: function (ol_uid) {
            let last;
            if (ol_uid) {
                last = this.userInputGeometryStack
                    .slice()
                    .toReversed() // .reverse() mutates in-place, .toReversed() doesn't
                    .find((item) => {
                        if (item.ol_uid == ol_uid) {
                            return true;
                        }
                    });
            } else {
                last = this.userInputGeometryStack.slice(-1)[0];
            }
            // We only need the dict keys we are interested in
            return (({ coordinates, properties, type }) => ({
                coordinates,
                properties,
                type,
            }))(last);
        },
        /**
         * Returns the coordinates from the feature.
         * If coordinates is provided, also updates the feature's coordinates.
         * @param {object} feature A feature object
         * @param {array=} coordinates A coordinate pair array
         * @param {number=} srid The SRID of the coordinates
         */
        userCoordinates: function (feature, coordinates, srid) {
            const geometry = feature.getProperties().original_geometry;
            let coordsChanged = false;
            let sridChanged = false;
            if (coordinates) {
                // Whether coords have been changed between user input and input to this function
                coordsChanged = !geometry.coordinates.every(
                    (e, i) => e == coordinates[i]
                );
                geometry.coordinates = coordinates;
            }

            if (srid) {
                // Whether srid has been changed between user input and input to this function
                sridChanged = !(
                    parseInt(geometry.properties.srid) === parseInt(srid)
                );
                geometry.properties.srid = srid;
            }

            if (coordsChanged || sridChanged) {
                // Update the undo-redo stack for user input geodata
                const clone = structuredClone(toRaw(geometry));
                clone['ol_uid'] = feature.ol_uid;
                const before = [...this.userInputGeometryStack];
                this.userInputGeometryStack.push(clone);

                this.undoredo.push('update user input geodata', {
                    before: before,
                    after: structuredClone(toRaw(this.userInputGeometryStack)),
                });
            }

            if (['MultiPoint'].includes(geometry.type)) {
                return geometry.coordinates[0];
            }
            return geometry.coordinates;
        },
        bufferRadius: function (feature) {
            return feature.getProperties().buffer_radius;
        },
        unOrRedoFeatureUserInputGeoData: function (ol_uid, original_geometry) {
            // Find the respective feature on the map by ol_uid
            this.layerSources[this.defaultQueryLayerName]
                .getFeatures()
                .forEach((feature) => {
                    if (feature.ol_uid == ol_uid) {
                        // Revert to the last user input geometry on the feature instance
                        const clone = structuredClone(original_geometry);
                        Object.assign(
                            feature.getProperties().original_geometry,
                            clone
                        );
                        // Revert to the last user input geometry for that feature in the list of geometries
                        this.featureInputCoordinates(
                            feature,
                            original_geometry.coordinates,
                            original_geometry.properties.srid
                        );
                        // Finally perform a transformation of the feature on the map
                        this.transformToMapCrs(
                            feature,
                            original_geometry.properties.srid
                        );
                    }
                });
        },
        addNewPoint: function (lat, lon) {
            console.log(lat, lon);
            const feature = new Feature({
                geometry: new Point([lon, lat]),
            });
            const color = this.styleByColor(feature, this.context, 'draw');
            this.setFeaturePropertiesFromContext(feature, this.context, {
                color: color,
            });

            this.layerSources[this.defaultQueryLayerName].addFeature(feature);
            this.userInputGeometryStackAdd(feature);

            // this.bootstrapTooltipTrigger();
        },
        bootstrapTooltipTrigger: function () {
            var tooltipTriggerList = [].slice.call(
                document.querySelectorAll('[data-bs-toggle="tooltip"]')
            );
            // eslint-disable-next-line no-unused-vars
            var tooltipList = tooltipTriggerList.map(
                function (tooltipTriggerEl) {
                    return new bootstrap.Tooltip(tooltipTriggerEl);
                }
            );
        },
        /**
         * Returns the type of a feature
         * @param {Object} feature A feature object or a feature dictionary
         */
        getFeatureType: function (feature) {
            const type =
                'getGeometry' in feature
                    ? feature.getGeometry().getType()
                    : feature.geometry
                      ? feature.geometry.type
                      : feature.type
                        ? feature.type
                        : null;
            if (!type) {
                console.error('Unknown feature type: ' + feature);
            }

            return type;
        },
        editableLayers: function () {
            const layers = this.getLayersWithFeatures();
            this.editableLayersArray = layers.filter((layer) => {
                return layer.get('can_edit');
            });
            return this.editableLayersArray;
        },
        vectorLayerDefinitions: function () {
            return this.additionalLayersDefinitions.concat(
                this.queryLayerDefinition
            );
        },
        getDefaultQueryLayerName: function () {
            const defaultLayerDef = this.vectorLayerDefinitions().filter(
                (layer) => {
                    return layer.default == true;
                }
            );

            if (defaultLayerDef.length > 1) {
                throw new Error('Cannot have more than one default layer.');
            } else {
                return defaultLayerDef.length > 0
                    ? (this.defaultQueryLayerName = defaultLayerDef[0].name)
                    : (this.defaultQueryLayerName =
                          this.queryLayerDefinition.name);
            }
        },
        getDefaultProcessedGeometryLayerName: function () {
            const processedLayerDef = this.vectorLayerDefinitions().filter(
                (layer) => {
                    return layer.processed == true;
                }
            );
            if (processedLayerDef.length > 1) {
                throw new Error('Cannot have more than one processed layer.');
            } else {
                return processedLayerDef.length > 0
                    ? (this.defaultProcessedGeometryLayerName =
                          processedLayerDef[0].name)
                    : (this.defaultProcessedGeometryLayerName = null);
            }
        },
        /**
         * Toggles editing for a layer on or off and updates the respective layerSwitcher button
         * @param {Object} layer A layer object
         * @param {Object} toggleButton A jQuery HTML button from the layerSwitcher
         * @param {Boolean} editing Whether to toggle editing for this button/layer on or off
         */
        layerToggleEditing: function (layer, toggleButton, editing) {
            layer.set('toggle_editing', editing);
            const btn = $(toggleButton).find('.btn');
            const img = $(toggleButton).find('img');
            if (editing) {
                img.addClass('svg-green');
                btn.addClass('btn-success');
                btn.addClass('btn-danger');
                $(toggleButton).attr('title', 'Layer Editing: On');
                layer.set('editing', true);
                this.activeEditLayer = layer;
            } else {
                btn.removeClass('btn-danger');
                btn.removeClass('btn-success');
                img.removeClass('svg-green');
                $(toggleButton).attr('title', 'Layer Editing: Off');
                layer.set('editing', false);
                this.activeEditLayer = null;
            }
        },
        /**
         * Queries the tenure layer at point coordinates and pans/zooms to coordinates
         * @param {Array} coordinates Point coordinates
         */
        highlightPointOnTenureLayer: function (coordinates) {
            if (!coordinates) {
                return;
            }
            const tenureLayer = this.optionalLayers.filter((layer) => {
                return layer.get('is_tenure_intersects_query_layer') == true;
            })[0];
            if (tenureLayer) {
                queryLayerAtPoint(this, tenureLayer, coordinates);
            }
            const feature = new Feature({
                geometry: new Point(coordinates),
            });
            this.centerOnFeature(feature, this.mapDefaultZoom);
        },
        getLayerDefinitionByName: function (layer_name) {
            return this.vectorLayerDefinitions().find((layer_def) => {
                return layer_def.name == layer_name;
            });
        },
        /**
         * Returns a layer by its name
         * @param {String} layer_name The 'name' property of the layer. Corresponds to the 'name' property in the layer definition prop
         */
        getLayerByName: function (layer_name) {
            if (!this.map) {
                return null;
            }
            return this.map
                .getLayers()
                .getArray()
                .find((layer) => {
                    return layer.get('name') == layer_name;
                });
        },
        /**
         * Returns a layer's feature by its id
         * @param {Object} layer A layer
         * @param {String|Number} feature_id The id of a feature on the layer
         */
        getFeatureById: function (layer, feature_id) {
            let featureId = Number(feature_id);
            if (isNaN(featureId)) {
                console.error(`Feature ID ${feature_id} is not a number`);
                return;
            }
            return layer
                .getSource()
                .getFeatures()
                .find((feature) => {
                    // The features name property is the model instance pk
                    return feature.getProperties().name == featureId;
                });
        },
        getFeaturesByOccurrenceReportId: function (
            layer,
            occurrence_report_id
        ) {
            return layer
                .getSource()
                .getFeatures()
                .filter((feature) => {
                    return feature.getProperties().name == occurrence_report_id;
                });
        },
        copyFeatureToLayer: function (feature, layer) {
            const copy = feature.clone();
            copy.unset('id');
            copy.unset('name');
            copy.unset('label');
            copy.unset('color');
            copy.unset('stroke');
            copy.unset('geometry_source');
            copy.set('locked', false);

            const props = feature.getProperties();
            const color = this.styleByColor(copy, this.context, 'draw');
            this.setFeaturePropertiesFromContext(copy, this.context, {
                color: color,
                created_from_object: {
                    model_class: props.model_class,
                    model_id: props.model_id,
                },
            });

            layer.getSource().addFeature(copy);
            return copy;
        },
        copySelectedToLayer: function (layer_name) {
            console.log('Copying selected features to layer:', layer_name);
            const targetLayer = this.getLayerByName(layer_name);
            this.selectedFeatureCollection.getArray().map((feature) => {
                this.copyFeatureToLayer(feature, targetLayer);
            });
        },
        /**
         * Adds a key-value pair to a feature's properties display-object
         * @param {String} key A key to add to the properties object
         * @param {String} value A value to add to the properties' key
         * @param {Object} properties A dictionary of property key-value pairs
         * @param {Object=} propertyMap A dictionary to map property names to display names
         * @param {Object=} propertyOverwrite A dictionary to overwrite property values
         */
        addFeatureDisplayPropertyValue: function (
            key,
            value,
            feature,
            properties,
            propertyMap = null,
            propertyOverwrite = null
        ) {
            if (isProxy(value)) {
                return properties;
            }
            if (propertyOverwrite && key in propertyOverwrite) {
                if (typeof propertyOverwrite[key] === 'function') {
                    value = propertyOverwrite[key](feature);
                } else {
                    value = propertyOverwrite[key];
                }
            }
            if (propertyMap) {
                if (key in propertyMap) {
                    properties[propertyMap[key]] = value;
                }
            } else {
                properties[key] = value;
            }
            return properties;
        },
        /**
         * Compiles a dictionary of feature and model properties for a feature
         * Ignore properties that are proxies
         * @param {Object} selected A feature object
         * @param {Object} layer A layer object
         */
        featureGetDisplayProperties: function (selected, layer) {
            const properties = {};
            const layerName = layer.getProperties().name;
            const layerDef = this.getLayerDefinitionByName(layerName);
            const propertyMap = layerDef.property_display_map || null;
            const propertyOverwrite = layerDef.property_overwrite || null;

            for (const [key, value] of Object.entries(
                selected.getProperties()
            )) {
                this.addFeatureDisplayPropertyValue(
                    key,
                    value,
                    selected,
                    properties,
                    propertyMap,
                    propertyOverwrite
                );
            }

            if (!selected.getProperties().model) {
                console.error('No model found for feature');
            } else {
                for (const [key, value] of Object.entries(
                    selected.getProperties().model
                )) {
                    this.addFeatureDisplayPropertyValue(
                        key,
                        value,
                        selected,
                        properties,
                        propertyMap,
                        propertyOverwrite
                    );
                }
            }

            return properties;
        },
        filterFeaturesShowOnMap: function (features, what, not = false) {
            if (not) {
                return features.filter(
                    (f) => f.getProperties().show_on_map !== what
                );
            }
            return features.filter(
                (f) => f.getProperties().show_on_map === what
            );
        },
        isFeatureEligibleToHide: function (feature) {
            if (!feature.getProperties().can_hide_geometries) {
                return false;
            }
            return feature.getProperties().show_on_map === false;
        },
        colorPickOrRandom: function (...colors) {
            const color = colors.find((color) => {
                if (this.isColor(color)) {
                    return color;
                }
            });
            if (color) {
                return color;
            }
            return this.getRandomColor();
        },
    },
};
</script>
<style>
@import '../../../../../static/boranga/css/map.css';
@import 'ol-ext/dist/ol-ext.css';

.custom-mouse-position {
    position: absolute;
    top: 3px;
    right: 45px;
    background-color: rgba(255, 255, 255, 0.8);
    padding-top: 2px;
    padding-bottom: 2px;
    z-index: 1000;
    margin-top: 6px;
    border-radius: 5px;
    font-size: 0.8em;
    padding-left: 10px;
    padding-right: 10px;
}

.custom-mouse-position:empty {
    display: none;
}
</style>
<style scoped>
#featureToast {
    position: absolute;
}
.positionToLeft {
    left: 60px;
}
.positionToRight {
    right: 20px;
}
.positionToTop {
    top: 20px;
}
.positionToBottom {
    bottom: 20px;
}

.badge {
    position: absolute;
    z-index: 100;
    padding-left: 9px;
    padding-right: 9px;
    -webkit-border-radius: 9px;
    -moz-border-radius: 9px;
    border-radius: 9px;
}

.label-warning[href],
.badge-warning[href] {
    background-color: #c67605;
}

#selectedFeatureCountDanger {
    font-size: 12px;
    color: #fff;
    padding: 0 5px;
    vertical-align: top;
    margin-left: -10px;
    background: #ff0000;
}
#selectedFeatureCountWarning {
    font-size: 12px;
    color: #fff;
    padding: 0 5px;
    vertical-align: top;
    margin-left: -10px;
    background: #ffa500;
}

.map-spinner {
    position: relative;
}

.shapefile-row {
    /* Works to make the Upload shapefile section fit neatly with the map
    haven't investigated why it's needed. */
    margin-left: 0px;
}

.force-parent-lh {
    line-height: inherit !important;
}

#submenu-draw {
    display: none;
}
#submenu-spatial-operations {
    display: none;
}
#submenu-geometries-list {
    display: none;
}
.hidden {
    display: none;
}
.input-group-append + small {
    width: 100%;
}

.min-width-60 {
    min-width: 60px !important;
}
.min-width-90 {
    min-width: 90px !important;
}
.min-width-120 {
    min-width: 120px !important;
}
.min-width-150 {
    min-width: 150px !important;
}
.min-width-180 {
    min-width: 180px !important;
}
.min-width-210 {
    min-width: 210px !important;
}
.svg-green {
    filter: invert(42%) sepia(93%) saturate(1352%) hue-rotate(87deg)
        brightness(119%) contrast(119%);
}

.ol-popup {
    position: absolute;
    background-color: white;
    box-shadow: 0 1px 4px rgba(0, 0, 0, 0.2);
    padding: 15px;
    border-radius: 10px;
    border: 1px solid #cccccc;
    bottom: 12px;
    left: -50px;
    min-width: 280px;
}
.ol-popup:after,
.ol-popup:before {
    top: 100%;
    border: solid transparent;
    content: '';
    height: 0;
    width: 0;
    position: absolute;
    cursor: pointer;
    /* pointer-events: none; */
}
.ol-popup:after {
    border-top-color: white;
    border-width: 10px;
    left: 48px;
    margin-left: -10px;
}
.ol-popup:before {
    border-top-color: #cccccc;
    border-width: 11px;
    left: 48px;
    margin-left: -11px;
}
/* .ol-popup-closer {
    text-decoration: none;
    position: absolute;
    top: 2px;
    right: 8px;
} */
/* .ol-popup-closer:after {
    content: '✖';
} */
.ol-textarea {
    width: fit-content;
    height: 100%;
    resize: none;
    text-wrap: nowrap;
    overflow-x: auto;
    scrollbar-width: thin;
    font-size: 1rem;
    line-height: 1;
}
.max-vh-50 {
    max-height: 50vh;
}
</style>
