import TileWMS from 'ol/source/TileWMS';
import WMSCapabilities from 'ol/format/WMSCapabilities';
import WMTS, { optionsFromCapabilities } from 'ol/source/WMTS.js';
import WMTSTileGrid from 'ol/tilegrid/WMTS.js';
import WMTSCapabilities from 'ol/format/WMTSCapabilities.js';
import { get as getProjection } from 'ol/proj.js';
import { getTopLeft, getWidth } from 'ol/extent.js';
import TileLayer from 'ol/layer/Tile';
import GeoJSON from 'ol/format/GeoJSON';
import Feature from 'ol/Feature';
import { Polygon } from 'ol/geom';
import { Style, Fill, Stroke } from 'ol/style';
import { utils } from '@/utils/hooks';
import { booleanIntersects } from '@turf/boolean-intersects';
import { booleanWithin } from '@turf/boolean-within';
import { polygon, multiPolygon, featureCollection } from '@turf/helpers';
import { area } from '@turf/area';
import { intersect } from '@turf/intersect';
import { helpers } from '@/utils/hooks.js';

// Layer to use as map base layer
export var baselayer_name = 'mapbox-streets';
// export var baselayer_name = 'mapbox-dark'

// WMTS connection alert (displayed once per layer)
const _wmtsAlerted = new Set();
function _alertWMTSConnection(layerDef, err) {
    const name =
        (layerDef.display_title && layerDef.display_title.trim()) ||
        layerDef.layer_name ||
        'WMTS layer';
    const key = `${layerDef.geoserver_url}|${layerDef.layer_name}`;
    if (_wmtsAlerted.has(key)) return;
    _wmtsAlerted.add(key);

    const status = helpers.statusFromError(err);
    const footerHtml = status
        ? `Response Code: ${helpers.escapeHtml(status)}.`
        : undefined;

    swal.fire({
        icon: 'error',
        title: `Unable to load map layer: ${name}`,
        html: `<p>Could not connect to the WMTS service.</p><p>This will prevent this page from functioning correctly.</p><p>Please contact support.</p>`,
        footer: footerHtml,
        confirmButtonText: 'OK',
        customClass: { confirmButton: 'btn btn-primary' },
    });
}

/**
 * Returns layers at map event pixel coordinates
 * @param {Proxy} map_component A map component instance
 * @param {Event} evt An Event object
 * @returns an array of layers
 */
export function layerAtEventPixel(map_component, evt) {
    const layer_at_pixel = [];
    const layers = [];
    map_component.map.getLayers().forEach((layer) => {
        let lyrs;
        if (typeof layer.getLayers === 'function') {
            lyrs = layer.getLayersArray();
        } else {
            lyrs = [layer];
        }
        // layers = layers.concat(lyrs);
        lyrs.forEach((lyr) => {
            const isBackgroundLayer =
                lyr.get('is_satellite_background') ||
                lyr.get('is_streets_background');
            if (lyr.getVisible() && !isBackgroundLayer) {
                layers.push(lyr);
            }
        });
    }, layers);

    layers.forEach((layer) => {
        if (!map_component.informing) {
            return;
        }
        let pixel = map_component.map.getEventPixel(evt.originalEvent);

        let data = layer.getData(pixel);
        // Return if no data or the alpha channel in RGBA is zero (transparent)
        if (!data /*|| data[3] == 0*/) {
            return;
        }
        layer_at_pixel.push(layer);
    });

    return layer_at_pixel;
}

/**
 * Fetches the GIS extent from an API endpoint and returns it as an array of floats.
 * @param {string} gisExtentApiUrl The url to the end point that returns the GIS extent
 * @returns
 */
export async function fetchGISExtent(gisExtentApiUrl) {
    if (!gisExtentApiUrl) {
        console.warn('No GIS extent API URL provided');
        return null;
    }
    let gisExtent = null;
    await fetch(gisExtentApiUrl)
        .then(async (response) => {
            if (!response.ok) {
                throw new Error('Network response was not ok');
            }
            return response.json();
        })
        .then((extent) => {
            console.log('GIS extent', extent);
            gisExtent = extent;
        })
        .catch((error) => {
            console.error('Error fetching GIS extent:', error);
        });
    if (gisExtent === null) {
        console.warn('GIS extent is null');
        return null;
    }
    if (!Array.isArray(gisExtent) || gisExtent.length !== 4) {
        console.error('GIS extent is not an array of four numbers', gisExtent);
        return null;
    }
    // Convert the extent to a tuple of numbers
    const extentArray = gisExtent.map((num) => parseFloat(num));
    if (extentArray.some((num) => isNaN(num))) {
        console.error('GIS extent contains non-numeric values', extentArray);
        return null;
    }
    return extentArray;
}

/**
 * Queries the WMS server for its capabilities and adds optional layers to a map
 * @param {Proxy} map_component A map component instance
 * @param {string} tileLayerApiUrl The url to the tile layer API
 */
export async function fetchTileLayers(map_component, tileLayerApiUrl) {
    // let parser = new WMSCapabilities();
    if (!tileLayerApiUrl) {
        console.error('No tile layer API url provided');
        return [];
    }
    let tileLayers = [];

    await fetch(tileLayerApiUrl)
        .then(async (response) => {
            if (!response.ok) {
                throw new Error('Network response was not ok');
            }
            return response.json();
        })
        .then(async (layers) => {
            console.log('tilelayer', layers);
            tileLayers = await _helper.tileLayerFromLayerDefinitions(layers);
        })
        .catch((error) => {
            console.error('Error fetching tilelayer:', error);
        });
    return tileLayers;
}

/**
 * Returns proposal objects by ID from an API endpoint
 * @param {Object} map_component The map component instance
 * @param {String} proposalApiUrl The URL to the proposal API
 * @param {Array} proposalIds An array of proposal IDs to fetch
 * @returns Proposal objects
 */
export async function fetchProposals(
    map_component,
    proposalApiUrl,
    proposalIds,
    queryParamKey = 'proposal_ids'
) {
    if (!proposalApiUrl) {
        console.warn('No proposal API URL provided');
        return [];
    }
    if (!proposalIds) {
        console.error('No proposal IDs provided');
        return [];
    }
    map_component.fetchingProposals = true;
    let url = proposalApiUrl;
    // Characters to concatenate pseudo url elements
    let chars = ['&', '&', '?'];
    let proposals = [];

    if (proposalIds.length > 0) {
        url += `${chars.pop()}${queryParamKey}=` + proposalIds.toString();
    }
    await fetch(url)
        .then(async (response) => {
            const data = await response.json();
            if (!response.ok) {
                const error = (data && data.message) || response.statusText;
                console.log(error);
                return Promise.reject(error);
            }
            proposals = data;
        })
        .catch((error) => {
            console.error('There was an error!', error);
        })
        .finally(() => {
            map_component.fetchingProposals = false;
        });

    return proposals;
}

export async function queryLayerAtPoint(map_component, layer, coordinate) {
    map_component.queryingGeoserver = true;
    let c = [coordinate[0], coordinate[1]];
    if (layer.getProperties().invert_xy) {
        c = [coordinate[1], coordinate[0]];
    }
    let point = `POINT (${c.join(' ')})`;

    let query_str = _helper.geoserverQuery.bind(this)(
        point,
        map_component,
        layer
    );

    if (query_str === undefined) {
        console.warn('A query string could not be created.');
        return;
    }

    _helper.validateFeatureQuery(query_str).then(async (features) => {
        if (features.length === 0) {
            console.warn('No features found at this location.');
            map_component.overlay(undefined);
        } else {
            console.log('Feature', features);
            map_component.overlay(coordinate, features[0]);
        }
        map_component.errorMessageProperty(null);
    });
}

/**
 * Sets the mode of interaction of the map.
 * @param {string} mode The mode to set the map to (layer, draw, measure)
 * @param {string=} subMode The submode to set the map to (e.g. draw: 'Polygon', 'Point')
 */
export function set_mode(map_component, mode, subMode = null) {
    if (!this.map.getTargetElement()) {
        console.warn(
            'Map not initialized in set_mode function. Returning false.'
        );
        return false;
    }
    // Toggle map mode on/off when the new mode is the old one
    if (this.mode == mode) {
        if (this.subMode == subMode) {
            this.mode = 'layer';
            this.subMode = null;
        } else {
            // If only the submode is different, set the new submode but keep the mode
            this.subMode = subMode;
        }
    } else {
        this.mode = mode;
        this.subMode = subMode;
    }

    this.drawing = false;
    this.measuring = false;
    this.informing = false;
    this.transforming = false;
    map_component.errorMessageProperty(null);
    this.overlay(undefined);
    this.map.getTargetElement().style.cursor = 'default';
    this.transformSetActive(false);

    if (this.mode === 'layer') {
        this.clearMeasurementLayer();
        _helper.toggle_draw_or_measure.bind(this)(false, false);
    } else if (this.mode === 'draw') {
        this.clearMeasurementLayer();
        this.sketchCoordinates = [[]];
        _helper.toggle_draw_or_measure.bind(this)(false, true);
        this.undoredo_forSketch.clear(); // Clear the sketch coordinates undo/redo stack
        this.drawing = true;
    } else if (this.mode === 'transform') {
        this.clearMeasurementLayer();
        this.transformSetActive(true);
        _helper.toggle_draw_or_measure.bind(this)(false, false);
        this.transforming = true;
    } else if (this.mode === 'measure') {
        _helper.toggle_draw_or_measure.bind(this)(true, false);
        this.measuring = true;
    } else if (this.mode === 'info') {
        _helper.toggle_draw_or_measure.bind(this)(false, false);
        this.informing = true;
    } else {
        console.error(`Cannot set mode ${mode}`);
        return false;
    }
    if (this.select) {
        // Call back to the map so selected features can adept their style to the new mode
        this.select.dispatchEvent({
            type: 'map:modeChanged',
            details: {
                new_mode: this.mode,
                new_subMode: this.subMode,
            },
        });
    }

    return true;
}

/**
 * Defines polygon feature styling depending on whether the geometry source
 * is a Registration of Interest or a Competitive Process
 * @param {object} feature A single geometry feature (e.g. a drawn polygon)
 */
export function polygon_style(feature) {
    let strokecolor = '#3498DB';
    let fillcolor = '#85C1E9';
    if (feature.get('source') === 'registration_of_interest') {
        // yellowish
        strokecolor = [244, 208, 63, 1.0];
        fillcolor = [241, 196, 15, 0.7];
    } else if (feature.get('source') === 'competitive_process') {
        // orangish
        strokecolor = [240, 178, 122, 1.0];
        fillcolor = [230, 126, 34, 0.7];
    }
    return new Style({
        fill: new Fill({
            color: fillcolor,
        }),
        stroke: new Stroke({
            color: strokecolor,
            width: 1,
        }),
    });
}

/**
 * Validate feature callback function. Calls `finnishDrawing` on the map component
 * when the feature is valid. A feature is condidered valid when it intersects with
 * the DBCS legislated land-water layer.
 * @param {string} feature The feature
 * @param {Proxy} component_map The map component
 */
export function validateFeature(feature, component_map) {
    let vm = this;
    let feature_wkt = undefined;
    console.log('Validate feature', feature);
    if (feature === undefined) {
        // Get the WKT representation of the currently drawn polygon sketch
        feature_wkt = _helper.featureToWKT.bind(vm)();
    } else {
        // Get the WKT representation of the provided feature
        feature_wkt = _helper.featureToWKT(feature);
    }

    if (component_map === undefined) {
        component_map = vm.$refs.component_map;
    }

    let query = _helper.geoserverQuery(feature_wkt, component_map);

    _helper.validateFeatureQuery(query).then(async (features) => {
        if (features.length === 0) {
            console.warn('New feature is not valid');
            component_map.errorMessageProperty(
                'The polygon you have drawn does not intersect with any DBCA lands or water.'
            );
        } else {
            console.log('New feature is valid', features);
            component_map.finishDrawing();
        }
    });
}
/**
 * Compares two features to determine whether they intersect. If one of the feature
 * is a polygon with holes, the function will return a valid feature if the other feature
 * is within the outer polygon and not completely within any of the holes.
 * The feature that likely contains the holes should be the second argument.
 * @param {Object} feature1 A feature to compare
 * @param {Object} feature2 A feature to compare
 * @returns The intersected features if the two features intersect, otherwise an empty list
 */
export function intersects(feature1, feature2) {
    // Two polygons to compare
    let poly1;
    let poly2;
    const geom1 = feature1.getGeometry();
    const geom2 = feature2.getGeometry();

    const coordinates1 = geom1.getCoordinates();
    if (geom1.getType() == 'Polygon') {
        poly1 = polygon(coordinates1);
    } else if (geom1.getType() == 'MultiPolygon') {
        poly1 = multiPolygon(coordinates1);
    } else {
        console.error('Feature 1 is not a polygon or multipolygon');
        return []; // Return an empty feature list
    }

    const coordinates2 = geom2.getCoordinates();
    if (geom2.getType() == 'Polygon') {
        poly2 = polygon(coordinates2);
        if (booleanIntersects(poly1, poly2)) {
            return [
                new Feature({
                    geometry: new Polygon(coordinates2),
                }),
            ];
        }
        return []; // Return an empty feature list
    } else if (geom2.getType() == 'MultiPolygon') {
        // let intersectFeature = new Feature({});
        const intersectFeatures = [];
        for (let i = 0; i < coordinates2.length; i++) {
            poly2 = _helper.polygonFromCoordinate(coordinates2[i], poly1);
            if (poly2 === null) {
                // We might have to consider edge cases of 'islands' within holes, for instance:
                // ----------------
                // |    hole      |
                // |  +--------+  |
                // |  | island |  |
                // |  |        |  |
                // |  +--------+  |
                // |              |
                // ----------------
                // This is likely to be a rare case, so we can ignore it for now
                return intersectFeatures;
            }
            if (booleanIntersects(poly1, poly2)) {
                const intersectFeature = new Feature({
                    geometry: new Polygon(poly2.geometry.coordinates),
                });
                intersectFeatures.push(intersectFeature);
                console.log('Feature 1 intersects with', intersectFeature);
            }
        }
        return intersectFeatures;
    } else {
        console.error('Feature 2 is not a polygon or multipolygon');
        return []; // Return an empty feature list
    }
}

/**
 * Determines the area of the intersection between two features.
 * The feature that likely contains the holes should be the second argument.
 * @param {Object} feature1 A feature to intersect
 * @param {Object} feature2 A feature to intersect
 */
export function intersectedArea(feature1, feature2) {
    const coords1 = feature1.getGeometry().getCoordinates();
    const coords2 = feature2.getGeometry().getCoordinates();
    const outer = coords2[0];
    const inner = coords2.slice(1, coords2.length);

    featureCollection([polygon([outer]), polygon(coords1)]);
    const outerIntersection = intersect(
        featureCollection([polygon([outer]), polygon(coords1)])
    );
    const innerIntersections = [];

    const outerIntersectionArea = area(outerIntersection);

    if (inner.length > 0) {
        inner.forEach((hole) => {
            innerIntersections.push(
                intersect(
                    featureCollection([polygon([hole]), polygon(coords1)])
                )
            );
        });
    }

    // Calculate the area of the intersection
    const innerIntersectionArea = innerIntersections.reduce(
        (accumulator, feature) => accumulator + area(feature),
        0
    );

    const intersectionArea = outerIntersectionArea - innerIntersectionArea;
    console.log(
        `Feature Area: ${outerIntersectionArea}, Intersection Area: ${intersectionArea}`
    );

    return intersectionArea;
}

export let owsQuery = {
    version: '1.0.0',
    landwater: {
        typeName: 'public:dbca_legislated_lands_and_waters',
        srsName: 'EPSG:4326',
        propertyName:
            'objectid,wkb_geometry,category,leg_act,leg_identifier,leg_name,leg_tenure,leg_vesting,shape_area,leg_poly_area',
        geometry: 'wkb_geometry',
    },
};

/**
 * Module with map related helper functions
 */
const _helper = {
    /**
     * Toggles measure and polygon layer active or inactive
     * @param {boolean} drawForMeasure Whether to set the measure layer active or inactive
     * @param {boolean} drawForModel Whether to set the model's polygon layer active or inactive
     */
    toggle_draw_or_measure: function (drawForMeasure, drawForModel) {
        /**
         * Sets the active state of the Draw layers
         * @param {boolean} pointsActive Set the active state of the points layer
         * @param {boolean} polygonsActive Set the active state of the polygons layer
         */
        var drawForModelSetActive = function (pointsActive, polygonsActive) {
            if (this.drawPointsForModel) {
                this.drawPointsForModel.setActive(pointsActive);
            }
            if (this.drawPolygonsForModel) {
                this.drawPolygonsForModel.setActive(polygonsActive);
            }
        }.bind(this);

        if (this.drawForMeasure) {
            this.drawForMeasure.setActive(drawForMeasure);
        }
        if (drawForModel && this.subMode === 'Polygon') {
            // Set points drawing layer inactive and polygons drawing layer active
            drawForModelSetActive(false, true);
        } else if (drawForModel && this.subMode === 'Point') {
            // Set points drawing layer active and polygons drawing layer inactive
            drawForModelSetActive(true, false);
        } else {
            // Set both points and polygons drawing layers inactive
            drawForModelSetActive(false, false);
        }
    },
    /**
     * Returns a Well-known-text (WKT) representation of a feature
     * @param {Feature} feature A feature to validate
     */
    featureToWKT: function (feature) {
        let vm = this;

        if (feature === undefined) {
            // If no feature is provided, create a feature from the current sketch
            let coordinates = vm.$refs.component_map.sketchCoordinates.slice();
            coordinates.push(coordinates[0]);
            feature = new Feature({
                id: -1,
                geometry: new Polygon([coordinates]),
                label: 'validation',
                color: vm.defaultColor,
                geometry_source: 'validation',
            });
        }

        // Prepare a WFS feature intersection request
        let flatCoordinates = feature.values_.geometry.flatCoordinates;

        // Transform list of flat coordinates into a list of coordinate pairs,
        // e.g. ['x1 y1', 'x2 y2', 'x3 y3']
        let flatCoordinateStringPairs = flatCoordinates
            .map((coord, index) =>
                index % 2 == 0
                    ? [flatCoordinates[index], flatCoordinates[index + 1]].join(
                          ' '
                      )
                    : ''
            )
            .filter((item) => item != '');

        // Create a Well-Known-Text polygon string from the coordinate pairs
        return `POLYGON ((${flatCoordinateStringPairs.join(', ')}))`;
    },
    /**
     * Builds a query string for the geoserver based on the provided WKT
     * @param {str} wkt A geometry in Well-known-text (WKT) format
     * @param {Object} map_component The map component
     * @returns A query string for the geoserver
     */
    geoserverQuery: function (wkt, map_component, layer) {
        let vm = this;
        if (wkt === undefined) {
            console.warn('No WKT provided');
            return;
        }
        if (map_component === undefined) {
            map_component = vm.$refs.component_map;
        }
        // The geoserver url
        const sourceUrl = layer.getSource().getUrls()[0];
        const owsUrl = `${sourceUrl}?`;
        const layerName = layer.getProperties().name;

        // Create a params dict for the WFS request to the land-water layer
        if (!Object.hasOwn(map_component.owsQuery, layerName)) {
            console.error(
                `No owsQuery parameters found for layer ${layerName}`
            );
            return;
        }
        const queryDef = map_component.owsQuery[layerName];
        const paramsDict = {
            service: 'WFS',
            request: 'GetFeature',
            typeName: `${layer.get('name')}`,
            maxFeatures: '5000',
            srsName: `${queryDef.srsName}`,
            // propertyName: `${kb.propertyName}`,
            geometry: `${queryDef.geometry}`,
            outputFormat: 'application/json',
        };
        // let paramsDict = map_component.queryParamsDict('landwater');
        // let geometry_name = map_component.owsQuery.kb.geometry;
        paramsDict['CQL_FILTER'] = `INTERSECTS(${queryDef.geometry},${wkt})`;

        // Turn params dict into a param query string
        let params = new URLSearchParams(paramsDict).toString();
        let query = `${owsUrl}${params}`;

        return query;
    },
    /**
     * Validates an openlayers feature against a geoserver `url`.
     * @param {Feature} feature A feature to validate
     * @returns {Promise} A promise that resolves to a list of intersected features
     */
    validateFeatureQuery: async function (query) {
        let vm = this;

        let features = [];
        // Query the WFS
        vm.queryingGeoserver = true;
        let urls = [query];

        let requests = urls.map((url) =>
            utils.fetchUrl(url).then((response) => response)
        );
        await Promise.all(requests)
            .then((data) => {
                features = new GeoJSON().readFeatures(data[0]);
            })
            .catch((error) => {
                console.log(error.message);
                vm.errorMessage = error.message;
            });

        return features;
    },
    tileLayerFromLayerDefinitions: async function (layers) {
        const tileLayers = [];

        for (let j in layers) {
            const layer = layers[j];
            const isBackgroundLayer =
                layer.is_satellite_background || layer.is_streets_background;
            const layerParams = {
                name: layer.layer_name,
                // abstract: layer.Abstract.trim(),
                title: layer.display_title.trim(),
                visible: layer.visible,
                // extent: layer.BoundingBox[0].extent,
                displayInLayerSwitcher: !isBackgroundLayer,
                is_satellite_background: layer.is_satellite_background,
                is_streets_background: layer.is_streets_background,
                invert_xy: layer.invert_xy,
                minZoom: layer.min_zoom,
                maxZoom: layer.max_zoom,
                is_tenure_intersects_query_layer:
                    layer.is_tenure_intersects_query_layer,
            };
            const isCapabilitiesUrl = layer.is_capabilities_url;
            const layerService = layer.service.toLowerCase();

            if (layerService === 'wmts') {
                // eslint-disable-next-line no-unused-vars
                const layerExtent = [
                    112.06055, -35.35322, 129.77051, -12.55456,
                ];
                const matrixSet = layer.matrix_set;
                const tilePixelSize = layer.tile_pixel_size;

                let wmtsLayerOptions;

                if (isCapabilitiesUrl) {
                    // WMTS from GetCapabilities
                    const parser = new WMTSCapabilities();
                    wmtsLayerOptions = await fetch(layer.geoserver_url)
                        .then(async function (response) {
                            if (!response.ok) {
                                return await response.text().then((json) => {
                                    throw new Error(json);
                                });
                            }
                            return response.text();
                        })
                        .then(function (text) {
                            const result = parser.read(text);

                            if (jQuery.isEmptyObject(result.Contents)) {
                                throw new Error(
                                    'result.Contents is an empty object'
                                );
                            }

                            const options = optionsFromCapabilities(result, {
                                layer: layer.layer_name,
                                matrixSet: matrixSet,
                                format: 'image/png',
                                tileSize: tilePixelSize,
                            });
                            return options;
                        })
                        .catch(function (error) {
                            console.error(
                                'Error fetching WMTS capabilities:',
                                error
                            );
                            _alertWMTSConnection(layer, error);
                        });
                } else {
                    // WMTS from layer definition directly
                    const projectionCode =
                        matrixSet === 'gda94' ? 'EPSG:4326' : 'EPSG:3857';
                    const projection = getProjection(projectionCode);
                    const projectionExtent = projection.getExtent();

                    const resolutions = new Array(19);
                    const width = getWidth(projectionExtent); // 360
                    const size = width / tilePixelSize;
                    const matrixIds = new Array(19);
                    for (let z = 0; z <= 18; ++z) {
                        // generate resolutions and matrixIds arrays for this WMTS
                        resolutions[z] = size / Math.pow(2, z);
                        matrixIds[z] = `${matrixSet}:${z}`;
                    }

                    wmtsLayerOptions = {
                        url: layer.geoserver_url,
                        format: 'image/png',
                        projection: projectionCode,
                        wrapX: true,
                        tileGrid: new WMTSTileGrid({
                            // extent: layerExtent,
                            origin: getTopLeft(projectionExtent),
                            resolutions: resolutions,
                            matrixIds: matrixIds,
                            tileSize: tilePixelSize,
                        }),
                        layer: layer.layer_name,
                        matrixSet: matrixSet,
                    };
                }

                if (!wmtsLayerOptions) {
                    console.error(
                        'Error creating WMTS layer options for',
                        layer
                    );
                    continue;
                }
                layerParams['source'] = new WMTS(wmtsLayerOptions);
            } else if (['wms'].includes(layerService)) {
                let wmsLayerOptions;
                if (isCapabilitiesUrl) {
                    // WMS from GetCapabilities
                    let parser = new WMSCapabilities();
                    wmsLayerOptions = await fetch(layer.geoserver_url)
                        .then(async function (response) {
                            if (!response.ok) {
                                return await response.text().then((json) => {
                                    throw new Error(json);
                                });
                            }
                            return response.text();
                        })
                        .then(function (text) {
                            const result = parser.read(text);
                            const options = result.Capability.Layer.Layer.find(
                                (l) => l.Name === layer.layer_name
                            );
                            if (!options) {
                                throw new Error(
                                    `Layer ${layer.layer_name} not found in WMS capabilities`
                                );
                            }

                            return {
                                url: result.Capability.Request.GetMap.DCPType[0]
                                    .HTTP.Get.OnlineResource,
                                params: {
                                    FORMAT: 'image/png',
                                    VERSION: '1.1.1',
                                    tiled: true,
                                    STYLES: '',
                                    LAYERS: `${layer.layer_name}`,
                                },
                            };
                        })
                        .catch(function (error) {
                            console.error(
                                'Error fetching WMS capabilities:',
                                error
                            );
                        });
                } else {
                    // WMS from layer definition directly
                    wmsLayerOptions = {
                        url: layer.geoserver_url,
                        params: {
                            FORMAT: 'image/png',
                            VERSION: '1.1.1',
                            tiled: true,
                            STYLES: '',
                            LAYERS: `${layer.layer_name}`,
                        },
                    };

                    // Data for an image tiles can only be retrieved if the source's crossOrigin property is set (https://openlayers.org/en/latest/apidoc/module-ol_layer_Tile-TileLayer.html#getData)
                    // E.g. info tool doesn't work without crossOrigin: 'anonymous', getting
                    // "Failed to execute 'getImageData' on 'CanvasRenderingContext2D': The canvas has been tainted by cross-origin data" at canvas/Layer.js::getImageData
                    if (!layer.geoserver_url.startsWith('/geoproxy/')) {
                        wmsLayerOptions['crossOrigin'] = 'anonymous';
                    }
                }

                if (!wmsLayerOptions) {
                    console.error(
                        'Error creating WMS layer options for',
                        layer
                    );
                    continue;
                }
                layerParams['source'] = new TileWMS(wmsLayerOptions);
            } else {
                console.error('Unknown layer service type', layerService);
                continue;
            }

            const tileLayer = new TileLayer(layerParams);

            let legend_url = null;
            if (layer.Name == baselayer_name) {
                //
            } else {
                if (typeof layer.Style != 'undefined') {
                    legend_url = layer.Style[0].LegendURL[0].OnlineResource;
                }
            }

            // Set additional attributes to the layer
            tileLayer.set('columns', []); // []
            tileLayer.set('display_all_columns', true); // true
            tileLayer.set('legend_url', legend_url);

            tileLayers.push(tileLayer);
        }
        return tileLayers;
    },
    /**
     * Returns a polygon object from a list of coordinates
     * @param {Array} coordinate A list of coordinates
     * @param {Object} otherPolygon A polygon object to test coordinate against when there are holes
     * @returns A list of polygon objects that each contain an intersecting polygon at the first index
     * and zero or more intersecting holes at the following indices
     */
    polygonFromCoordinate: function (coordinate, otherPolygon) {
        let numCoords = 0;
        let validCoords = 0;
        coordinate.forEach((coord) => {
            numCoords += 1;
            validCoords += coord.every((coord) => coord.length == 2) ? 1 : 0;
        });
        if (numCoords != validCoords) {
            console.error('Feature 2 contains invalid coordinates');
            return false;
        }
        if (numCoords == 1) {
            return polygon(coordinate);
        } else {
            // A polygon with holes, e.g. [[outer], [hole1], [hole2], ...]
            if (
                otherPolygon &&
                coordinate
                    .slice(1, coordinate.length)
                    .some((hole) =>
                        booleanWithin(otherPolygon, polygon([hole]))
                    )
            ) {
                // The feature is within a hole, which means it does not intersect
                console.log('Feature is within a hole');
                return null;
            }
            // Find the intersecting holes
            const holes = coordinate
                .slice(1, coordinate.length)
                .filter((hole) => {
                    return booleanIntersects(otherPolygon, polygon([hole]));
                });
            if (holes.length == 0) {
                // No intersecting holes
                return polygon([coordinate[0]]);
            }
            console.log('Feature intersects with holes', holes);
            // The outer polygon is the first element in the list
            const outer = coordinate[0];
            return polygon([outer, ...holes]);
        }
    },
};
