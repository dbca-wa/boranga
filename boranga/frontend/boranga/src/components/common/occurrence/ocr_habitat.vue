<template lang="html">
    <div id="habitat">
        <FormSection
            :form-collapse="false"
            label="Habitat Composition"
            :Index="habitatCompositionBody"
            :subtitle="habitatCompositionIsDirty ? 'Unsaved Changes' : ''"
            :subtitle-class="
                habitatCompositionIsDirty ? 'text-warning ms-auto' : ''
            "
            :show-subtitle-icon="true"
        >
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Land Form:</label>
                <div class="col-sm-9">
                    <select
                        ref="land_form_select"
                        v-model="
                            occurrence_report_obj.habitat_composition.land_form
                        "
                        :disabled="isReadOnly"
                        style="width: 100%"
                        class="form-select input-sm"
                    >
                        <option
                            v-for="option in land_form_list"
                            :key="option.id"
                            :value="option.id"
                            :disabled="option.disabled"
                        >
                            {{ option.name }}
                        </option>
                    </select>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Rock Type:</label>
                <div class="col-sm-9">
                    <template v-if="!isReadOnly">
                        <template
                            v-if="
                                rock_type_list &&
                                rock_type_list.length > 0 &&
                                occurrence_report_obj.habitat_composition
                                    .rock_type_id &&
                                !rock_type_list
                                    .map((d) => d.id)
                                    .includes(
                                        occurrence_report_obj
                                            .habitat_composition.rock_type_id
                                    )
                            "
                        >
                            <input
                                v-if="
                                    occurrence_report_obj.habitat_composition
                                        .rock_type
                                "
                                type="text"
                                class="form-control mb-3"
                                :value="
                                    occurrence_report_obj.habitat_composition
                                        .rock_type + ' (Now Archived)'
                                "
                                disabled
                            />
                            <div class="mb-3 text-muted">
                                Change rock type to:
                            </div>
                        </template>
                        <select
                            v-model="
                                occurrence_report_obj.habitat_composition
                                    .rock_type_id
                            "
                            class="form-select"
                        >
                            <option
                                v-for="rock_type in rock_type_list"
                                :key="rock_type.id"
                                :value="rock_type.id"
                            >
                                {{ rock_type.name }}
                            </option>
                        </select>
                    </template>
                    <template v-else>
                        <input
                            v-model="
                                occurrence_report_obj.habitat_composition
                                    .rock_type
                            "
                            class="form-control"
                            type="text"
                            :disabled="isReadOnly"
                        />
                    </template>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label"
                    >Loose Rock (%):</label
                >
                <div class="col-sm-6">
                    <input
                        id="loose_rock_per"
                        v-model="
                            occurrence_report_obj.habitat_composition
                                .loose_rock_percent
                        "
                        :disabled="isReadOnly"
                        type="number"
                        class="form-control"
                        placeholder=""
                        min="0"
                        max="100"
                    />
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Soil Type:</label>
                <div class="col-sm-9">
                    <select
                        ref="soil_type_select"
                        v-model="
                            occurrence_report_obj.habitat_composition.soil_type
                        "
                        :disabled="isReadOnly"
                        style="width: 100%"
                        class="form-select input-sm"
                    >
                        <option
                            v-for="option in soil_type_list"
                            :key="option.id"
                            :value="option.id"
                            :disabled="option.disabled"
                        >
                            {{ option.name }}
                        </option>
                    </select>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label"
                    >Soil Colour:</label
                >
                <div class="col-sm-9">
                    <template v-if="!isReadOnly">
                        <template
                            v-if="
                                soil_colour_list &&
                                soil_colour_list.length > 0 &&
                                occurrence_report_obj.habitat_composition
                                    .soil_colour_id &&
                                !soil_colour_list
                                    .map((d) => d.id)
                                    .includes(
                                        occurrence_report_obj
                                            .habitat_composition.soil_colour_id
                                    )
                            "
                        >
                            <input
                                v-if="
                                    occurrence_report_obj.habitat_composition
                                        .soil_colour
                                "
                                colour="text"
                                class="form-control mb-3"
                                :value="
                                    occurrence_report_obj.habitat_composition
                                        .soil_colour + ' (Now Archived)'
                                "
                                disabled
                            />
                            <div class="mb-3 text-muted">
                                Change soil colour to:
                            </div>
                        </template>
                        <select
                            v-model="
                                occurrence_report_obj.habitat_composition
                                    .soil_colour_id
                            "
                            class="form-select"
                        >
                            <option
                                v-for="soil_colour in soil_colour_list"
                                :key="soil_colour.id"
                                :value="soil_colour.id"
                            >
                                {{ soil_colour.name }}
                            </option>
                        </select>
                    </template>
                    <template v-else>
                        <input
                            v-model="
                                occurrence_report_obj.habitat_composition
                                    .soil_colour
                            "
                            class="form-control"
                            colour="text"
                            :disabled="isReadOnly"
                        />
                    </template>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label"
                    >Soil Condition:</label
                >
                <div class="col-sm-9">
                    <template v-if="!isReadOnly">
                        <template
                            v-if="
                                soil_condition_list &&
                                soil_condition_list.length > 0 &&
                                occurrence_report_obj.habitat_composition
                                    .soil_condition_id &&
                                !soil_condition_list
                                    .map((d) => d.id)
                                    .includes(
                                        occurrence_report_obj
                                            .habitat_composition
                                            .soil_condition_id
                                    )
                            "
                        >
                            <input
                                v-if="
                                    occurrence_report_obj.habitat_composition
                                        .soil_condition
                                "
                                type="text"
                                class="form-control mb-3"
                                :value="
                                    occurrence_report_obj.habitat_composition
                                        .soil_condition + ' (Now Archived)'
                                "
                                disabled
                            />
                            <div class="mb-3 text-muted">
                                Change soil condition to:
                            </div>
                        </template>
                        <select
                            v-model="
                                occurrence_report_obj.habitat_composition
                                    .soil_condition_id
                            "
                            class="form-select"
                        >
                            <option
                                v-for="soil_condition in soil_condition_list"
                                :key="soil_condition.id"
                                :value="soil_condition.id"
                            >
                                {{ soil_condition.name }}
                            </option>
                        </select>
                    </template>
                    <template v-else>
                        <input
                            v-model="
                                occurrence_report_obj.habitat_composition
                                    .soil_condition
                            "
                            class="form-control"
                            type="text"
                            :disabled="isReadOnly"
                        />
                    </template>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Drainage:</label>
                <div class="col-sm-9">
                    <template v-if="!isReadOnly">
                        <template
                            v-if="
                                drainage_list &&
                                drainage_list.length > 0 &&
                                occurrence_report_obj.habitat_composition
                                    .drainage_id &&
                                !drainage_list
                                    .map((d) => d.id)
                                    .includes(
                                        occurrence_report_obj
                                            .habitat_composition.drainage_id
                                    )
                            "
                        >
                            <input
                                v-if="
                                    occurrence_report_obj.habitat_composition
                                        .drainage
                                "
                                type="text"
                                class="form-control mb-3"
                                :value="
                                    occurrence_report_obj.habitat_composition
                                        .drainage + ' (Now Archived)'
                                "
                                disabled
                            />
                            <div class="mb-3 text-muted">
                                Change drainage to:
                            </div>
                        </template>
                        <select
                            v-model="
                                occurrence_report_obj.habitat_composition
                                    .drainage_id
                            "
                            class="form-select"
                        >
                            <option
                                v-for="drainage in drainage_list"
                                :key="drainage.id"
                                :value="drainage.id"
                            >
                                {{ drainage.name }}
                            </option>
                        </select>
                    </template>
                    <template v-else>
                        <input
                            v-model="
                                occurrence_report_obj.habitat_composition
                                    .drainage
                            "
                            class="form-control"
                            type="text"
                            :disabled="isReadOnly"
                        />
                    </template>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label"
                    >Water Quality:</label
                >
                <div class="col-sm-9">
                    <textarea
                        v-model="
                            occurrence_report_obj.habitat_composition
                                .water_quality
                        "
                        :disabled="isReadOnly"
                        type="text"
                        class="form-control"
                        placeholder=""
                    />
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label"
                    >Habitat Notes:</label
                >
                <div class="col-sm-9">
                    <textarea
                        v-model="
                            occurrence_report_obj.habitat_composition
                                .habitat_notes
                        "
                        :disabled="isReadOnly"
                        type="text"
                        class="form-control"
                        placeholder=""
                    />
                </div>
            </div>
            <div class="row mb-3">
                <div class="col-sm-12">
                    <button
                        v-if="!updatingHabitatCompositionDetails"
                        class="btn btn-sm float-end"
                        :class="{
                            'btn-primary': habitatCompositionIsDirty,
                            'btn-light': !habitatCompositionIsDirty,
                            border: !habitatCompositionIsDirty,
                        }"
                        :disabled="isReadOnly || !habitatCompositionIsDirty"
                        @click.prevent="updateHabitatCompositionDetails()"
                    >
                        <template v-if="habitatCompositionIsDirty"
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
        </FormSection>
        <FormSection
            :form-collapse="false"
            label="Habitat Condition"
            :Index="habitatConditionBody"
            :subtitle="habitatConditionIsDirty ? 'Unsaved Changes' : ''"
            :subtitle-class="
                habitatConditionIsDirty ? 'text-warning ms-auto' : ''
            "
            :show-subtitle-icon="true"
        >
            <label for="" class="col-lg-3 control-label fs-5 fw-bold mb-3"
                >Keighery Scale</label
            >
            <div class="row mb-2">
                <label for="" class="col-sm-3 control-label">Pristine:</label>
                <div class="col-sm-2">
                    <div class="input-group">
                        <input
                            id="pristine"
                            v-model="
                                occurrence_report_obj.habitat_condition.pristine
                            "
                            :disabled="isReadOnly"
                            type="number"
                            class="form-control"
                            placeholder=""
                            min="0"
                            max="100"
                            step="0.01"
                            @change="
                                checkKeigheryScaleTotal($event, 'pristine')
                            "
                        />
                        <span class="input-group-text">%</span>
                    </div>
                </div>
            </div>
            <div class="row mb-2">
                <label for="" class="col-sm-3 control-label">Excellent:</label>
                <div class="col-sm-2">
                    <div class="input-group">
                        <input
                            id="excellent"
                            v-model="
                                occurrence_report_obj.habitat_condition
                                    .excellent
                            "
                            :disabled="isReadOnly"
                            type="number"
                            class="form-control"
                            placeholder=""
                            min="0"
                            max="100"
                            step="0.01"
                            @change="
                                checkKeigheryScaleTotal($event, 'excellent')
                            "
                        />
                        <span class="input-group-text">%</span>
                    </div>
                </div>
            </div>
            <div class="row mb-2">
                <label for="" class="col-sm-3 control-label">Very Good:</label>
                <div class="col-sm-2">
                    <div class="input-group">
                        <input
                            id="very_good"
                            v-model="
                                occurrence_report_obj.habitat_condition
                                    .very_good
                            "
                            :disabled="isReadOnly"
                            type="number"
                            class="form-control"
                            placeholder=""
                            min="0"
                            max="100"
                            step="0.01"
                            @change="
                                checkKeigheryScaleTotal($event, 'very_good')
                            "
                        />
                        <span class="input-group-text">%</span>
                    </div>
                </div>
            </div>
            <div class="row mb-2">
                <label for="" class="col-sm-3 control-label">Good:</label>
                <div class="col-sm-2">
                    <div class="input-group">
                        <input
                            id="good"
                            v-model="
                                occurrence_report_obj.habitat_condition.good
                            "
                            :disabled="isReadOnly"
                            type="number"
                            class="form-control"
                            placeholder=""
                            min="0"
                            max="100"
                            step="0.01"
                            @change="checkKeigheryScaleTotal($event, 'good')"
                        />
                        <span class="input-group-text">%</span>
                    </div>
                </div>
            </div>
            <div class="row mb-2">
                <label for="" class="col-sm-3 control-label">Degraded:</label>
                <div class="col-sm-2">
                    <div class="input-group">
                        <input
                            id="degraded"
                            v-model="
                                occurrence_report_obj.habitat_condition.degraded
                            "
                            :disabled="isReadOnly"
                            type="number"
                            class="form-control"
                            placeholder=""
                            min="0"
                            max="100"
                            step="0.01"
                            @change="
                                checkKeigheryScaleTotal($event, 'degraded')
                            "
                        />
                        <span class="input-group-text">%</span>
                    </div>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label"
                    >Completely Degraded:</label
                >
                <div class="col-sm-2">
                    <div class="input-group">
                        <input
                            id="completely_degraded"
                            v-model="
                                occurrence_report_obj.habitat_condition
                                    .completely_degraded
                            "
                            :disabled="isReadOnly"
                            type="number"
                            class="form-control"
                            placeholder=""
                            min="0"
                            max="100"
                            step="0.01"
                            @change="
                                checkKeigheryScaleTotal(
                                    $event,
                                    'completely_degraded'
                                )
                            "
                        />
                        <span class="input-group-text">%</span>
                    </div>
                </div>
            </div>
            <div class="row pt-3 border-top">
                <label for="" class="col-sm-3 control-label">Total</label>
                <div class="col-sm-2">
                    <div class="input-group">
                        <input
                            id="keighery-scale-total"
                            v-model="keigheryScaleTotal"
                            readonly
                            type="number"
                            class="form-control"
                            placeholder=""
                            :class="
                                keigheryScaleTotal == 100.0
                                    ? 'border-success border-2'
                                    : 'border-danger border-2'
                            "
                        />
                        <span class="input-group-text">%</span>
                    </div>
                </div>
            </div>
            <div
                v-if="occurrence_report_obj.group_type == 'community'"
                class="row mt-3 mb-3"
            >
                <label for="" class="col-sm-3 control-label"
                    >Observation Date:
                </label>
                <div class="col-sm-9">
                    <input
                        v-model="
                            occurrence_report_obj.habitat_condition.obs_date
                        "
                        :disabled="true"
                        type="date"
                        class="form-control"
                        name="obs_date"
                    />
                </div>
            </div>
            <div class="row mb-3">
                <div class="col-sm-12">
                    <button
                        v-if="!updatingHabitatConditionDetails"
                        class="btn btn-sm float-end"
                        :class="{
                            'btn-primary': habitatConditionIsDirty,
                            'btn-light': !habitatConditionIsDirty,
                            border: !habitatConditionIsDirty,
                        }"
                        :disabled="isReadOnly || !habitatConditionIsDirty"
                        @click.prevent="updateHabitatConditionDetails()"
                    >
                        <template v-if="habitatConditionIsDirty"
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
        </FormSection>
        <FormSection
            :form-collapse="false"
            label="Vegetation Structure"
            :Index="vegetationStructureBody"
            :subtitle="vegetationStructureIsDirty ? 'Unsaved Changes' : ''"
            :subtitle-class="
                vegetationStructureIsDirty ? 'text-warning ms-auto' : ''
            "
            :show-subtitle-icon="true"
        >
            <div class="row mb-3">
                <label for="" class="col-sm-6 control-label"
                    >Vegetation Structure - Layer 4 (Upper Layer):</label
                >
                <div class="col-sm-9">
                    <textarea
                        id="vegetation_structure_text_4"
                        v-model="
                            occurrence_report_obj.vegetation_structure
                                .vegetation_structure_layer_four
                        "
                        :disabled="isReadOnly"
                        type="text"
                        row="2"
                        class="form-control"
                        placeholder=""
                    />
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-6 control-label"
                    >Vegetation Structure - Layer 3 (Mid Layer):</label
                >
                <div class="col-sm-9">
                    <textarea
                        id="vegetation_structure_text_3"
                        v-model="
                            occurrence_report_obj.vegetation_structure
                                .vegetation_structure_layer_three
                        "
                        :disabled="isReadOnly"
                        type="text"
                        row="2"
                        class="form-control"
                        placeholder=""
                    />
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-6 control-label"
                    >Vegetation Structure - Layer 2 (Lower Layer):</label
                >
                <div class="col-sm-9">
                    <textarea
                        id="vegetation_structure_text_2"
                        v-model="
                            occurrence_report_obj.vegetation_structure
                                .vegetation_structure_layer_two
                        "
                        :disabled="isReadOnly"
                        type="text"
                        row="2"
                        class="form-control"
                        placeholder=""
                    />
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-6 control-label"
                    >Vegetation Structure - Layer 1 (Ground Layer):</label
                >
                <div class="col-sm-9">
                    <textarea
                        id="vegetation_structure_text_1"
                        v-model="
                            occurrence_report_obj.vegetation_structure
                                .vegetation_structure_layer_one
                        "
                        :disabled="isReadOnly"
                        type="text"
                        row="2"
                        class="form-control"
                        placeholder=""
                    />
                </div>
            </div>
            <div class="row mb-3">
                <div class="col-sm-12">
                    <button
                        v-if="!updatingVegetationStructure"
                        class="btn btn-sm float-end"
                        :class="{
                            'btn-primary': vegetationStructureIsDirty,
                            'btn-light': !vegetationStructureIsDirty,
                            border: !vegetationStructureIsDirty,
                        }"
                        :disabled="isReadOnly || !vegetationStructureIsDirty"
                        @click.prevent="updateVegetationStructure()"
                    >
                        <template v-if="vegetationStructureIsDirty"
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
        </FormSection>
        <FormSection
            :form-collapse="false"
            label="Fire History"
            :Index="fireHistoryBody"
            :subtitle="fireHistoryIsDirty ? 'Unsaved Changes' : ''"
            :subtitle-class="fireHistoryIsDirty ? 'text-warning ms-auto' : ''"
            :show-subtitle-icon="true"
        >
            <label for="" class="col-lg-3 control-label fs-5 fw-bold"
                >Last Fire History</label
            >
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label"
                    >Last Fire Estimate:</label
                >
                <div class="col-sm-9">
                    <input
                        ref="last_fire_date"
                        v-model="
                            occurrence_report_obj.fire_history
                                .last_fire_estimate
                        "
                        :disabled="isReadOnly"
                        type="month"
                        class="form-control"
                        name="last_fire_date"
                        :max="new Date().toISOString().slice(0, 7)"
                        @blur="checkDate()"
                    />
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Intensity:</label>
                <div class="col-sm-9">
                    <template v-if="!isReadOnly">
                        <template
                            v-if="
                                intensity_list &&
                                intensity_list.length > 0 &&
                                occurrence_report_obj.fire_history
                                    .intensity_id &&
                                !intensity_list
                                    .map((d) => d.id)
                                    .includes(
                                        occurrence_report_obj.fire_history
                                            .intensity_id
                                    )
                            "
                        >
                            <input
                                v-if="
                                    occurrence_report_obj.fire_history.intensity
                                "
                                type="text"
                                class="form-control mb-3"
                                :value="
                                    occurrence_report_obj.fire_history
                                        .intensity + ' (Now Archived)'
                                "
                                disabled
                            />
                            <div class="mb-3 text-muted">
                                Change intensity to:
                            </div>
                        </template>
                        <select
                            v-model="
                                occurrence_report_obj.fire_history.intensity_id
                            "
                            class="form-select"
                        >
                            <option
                                v-for="intensity in intensity_list"
                                :key="intensity.id"
                                :value="intensity.id"
                            >
                                {{ intensity.name }}
                            </option>
                        </select>
                    </template>
                    <template v-else>
                        <input
                            v-model="
                                occurrence_report_obj.fire_history.intensity
                            "
                            class="form-control"
                            type="text"
                            :disabled="isReadOnly"
                        />
                    </template>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Comments:</label>
                <div class="col-sm-9">
                    <textarea
                        id="fire_history_comment"
                        v-model="occurrence_report_obj.fire_history.comment"
                        :disabled="isReadOnly"
                        type="text"
                        row="2"
                        class="form-control"
                        placeholder=""
                    />
                </div>
            </div>
            <div class="row mb-3">
                <div class="col-sm-12">
                    <button
                        v-if="!updatingFireHistoryDetails"
                        class="btn btn-sm float-end"
                        :class="{
                            'btn-primary': fireHistoryIsDirty,
                            'btn-light': !fireHistoryIsDirty,
                            border: !fireHistoryIsDirty,
                        }"
                        :disabled="isReadOnly || !fireHistoryIsDirty"
                        @click.prevent="updateFireHistoryDetails()"
                    >
                        <template v-if="fireHistoryIsDirty"
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
        </FormSection>
        <FormSection
            :form-collapse="false"
            label="Associated Species"
            :Index="associatedSpeciesBody"
            :subtitle="associatedSpeciesIsDirty ? 'Unsaved Changes' : ''"
            :subtitle-class="
                associatedSpeciesIsDirty ? 'text-warning ms-auto' : ''
            "
            :show-subtitle-icon="true"
        >
            <div class="row mb-3 border-bottom pb-3">
                <label for="" class="col-sm-3 control-label"
                    >Species List Relates To:</label
                >
                <div class="col-sm-9">
                    <template v-if="!isReadOnly">
                        <template
                            v-if="
                                speciesListRelatesToOptions &&
                                speciesListRelatesToOptions.length > 0 &&
                                occurrence_report_obj.associated_species
                                    .species_list_relates_to_id &&
                                !speciesListRelatesToOptions
                                    .map((d) => d.id)
                                    .includes(
                                        occurrence_report_obj.associated_species
                                            .species_list_relates_to_id
                                    )
                            "
                        >
                            <input
                                v-if="
                                    occurrence_report_obj.associated_species
                                        .species_list_relates_to
                                "
                                type="text"
                                class="form-control mb-3"
                                :value="
                                    occurrence_report_obj.associated_species
                                        .species_list_relates_to +
                                    ' (Now Archived)'
                                "
                                disabled
                            />
                            <div class="mb-3 text-muted">
                                Change what the species list relates to:
                            </div>
                        </template>
                        <select
                            v-model="
                                occurrence_report_obj.associated_species
                                    .species_list_relates_to_id
                            "
                            class="form-select"
                        >
                            <option :value="null">
                                Select what the species list relates to
                            </option>
                            <option
                                v-for="speciesListRelatesTo in speciesListRelatesToOptions"
                                :key="speciesListRelatesTo.id"
                                :value="speciesListRelatesTo.id"
                            >
                                {{ speciesListRelatesTo.name }}
                            </option>
                        </select>
                    </template>
                    <template v-else>
                        <input
                            v-model="
                                occurrence_report_obj.associated_species
                                    .species_list_relates_to
                            "
                            class="form-control"
                            type="text"
                            :disabled="isReadOnly"
                        />
                    </template>
                </div>
            </div>

            <RelatedSpecies
                ref="related_species"
                :is-read-only="isReadOnly"
                :occurrence_report_obj="occurrence_report_obj"
                class="mb-3"
            />
            <div class="row border-top mb-3 pt-3">
                <div class="col-sm-3">
                    <label for="related_species" class="control-label"
                        >Comment</label
                    >
                </div>
                <div class="col-sm-9">
                    <textarea
                        id="related_species"
                        v-model="
                            occurrence_report_obj.associated_species.comment
                        "
                        :disabled="isReadOnly"
                        type="text"
                        row="2"
                        class="form-control"
                        placeholder=""
                    />
                </div>
            </div>
            <div class="row mb-3">
                <div class="col-sm-12">
                    <button
                        v-if="!updatingAssociatedSpeciesDetails"
                        class="btn btn-sm float-end"
                        :class="{
                            'btn-primary': associatedSpeciesIsDirty,
                            'btn-light': !associatedSpeciesIsDirty,
                            border: !associatedSpeciesIsDirty,
                        }"
                        :disabled="isReadOnly || !associatedSpeciesIsDirty"
                        @click.prevent="updateAssociatedSpeciesDetails()"
                    >
                        <template v-if="associatedSpeciesIsDirty"
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
        </FormSection>
    </div>
</template>

<script>
import { v4 as uuid } from 'uuid';
import FormSection from '@/components/forms/section_toggle.vue';
import RelatedSpecies from '@/components/common/occurrence/ocr_related_species_table.vue';
import { api_endpoints, helpers } from '@/utils/hooks';
export default {
    name: 'OCRHabitat',
    components: {
        FormSection,
        RelatedSpecies,
    },
    props: {
        occurrence_report_obj: {
            type: Object,
            required: true,
        },
        // this prop is only send from split species form to make the original species readonly
        is_readonly: {
            type: Boolean,
            default: false,
        },
        is_external: {
            type: Boolean,
            default: false,
        },
    },
    data: function () {
        let vm = this;
        return {
            habitatCompositionBody: 'habitatCompositionBody' + uuid(),
            habitatConditionBody: 'habitatConditionBody' + uuid(),
            vegetationStructureBody: 'vegetationStructureBody' + uuid(),
            fireHistoryBody: 'fireHistoryBody' + uuid(),
            associatedSpeciesBody: 'associatedSpeciesBody' + uuid(),
            originalHabitatComposition: null,
            originalHabitatCondition: null,
            originalVegetationStructure: null,
            originalFireHistory: null,
            originalAssociatedSpecies: null,
            //---to show fields related to Fauna
            isFauna:
                vm.occurrence_report_obj.group_type === 'fauna' ? true : false,
            //----list of values dictionary
            listOfValuesDict: {},
            //scientific_name_list: [],
            rock_type_list: [],
            soil_type_list: [],
            soil_colour_list: [],
            soil_condition_list: [],
            land_form_list: [],
            drainage_list: [],
            intensity_list: [],
            speciesListRelatesToOptions: [],
            updatingHabitatCompositionDetails: false,
            updatingHabitatConditionDetails: false,
            updatingVegetationStructure: false,
            updatingFireHistoryDetails: false,
            updatingAssociatedSpeciesDetails: false,
            previousKeigheryValue: null,
        };
    },
    computed: {
        isReadOnly: function () {
            //override for split reports
            if (this.is_readonly) {
                return this.is_readonly;
            }
            return this.occurrence_report_obj.readonly;
        },
        keigheryScaleTotal: function () {
            return (
                Number(this.occurrence_report_obj.habitat_condition.pristine) +
                Number(this.occurrence_report_obj.habitat_condition.excellent) +
                Number(this.occurrence_report_obj.habitat_condition.very_good) +
                Number(this.occurrence_report_obj.habitat_condition.good) +
                Number(this.occurrence_report_obj.habitat_condition.degraded) +
                Number(
                    this.occurrence_report_obj.habitat_condition
                        .completely_degraded
                )
            ).toFixed(2);
        },
        habitatCompositionIsDirty: function () {
            if (this.originalHabitatComposition === null) {
                return false;
            }
            return (
                JSON.stringify(
                    this.occurrence_report_obj.habitat_composition
                ) != this.originalHabitatComposition
            );
        },
        habitatConditionIsDirty: function () {
            if (this.originalHabitatCondition === null) {
                return false;
            }
            return (
                JSON.stringify(this.occurrence_report_obj.habitat_condition) !=
                this.originalHabitatCondition
            );
        },
        vegetationStructureIsDirty: function () {
            if (this.originalVegetationStructure === null) {
                return false;
            }
            return (
                JSON.stringify(
                    this.occurrence_report_obj.vegetation_structure
                ) != this.originalVegetationStructure
            );
        },
        fireHistoryIsDirty: function () {
            if (this.originalFireHistory === null) {
                return false;
            }
            return (
                JSON.stringify(this.occurrence_report_obj.fire_history) !=
                this.originalFireHistory
            );
        },
        associatedSpeciesIsDirty: function () {
            if (this.originalAssociatedSpecies === null) {
                return false;
            }
            return (
                JSON.stringify(this.occurrence_report_obj.associated_species) !=
                this.originalAssociatedSpecies
            );
        },
        isDirty: function () {
            return (
                this.habitatCompositionIsDirty ||
                this.habitatConditionIsDirty ||
                this.vegetationStructureIsDirty ||
                this.fireHistoryIsDirty ||
                this.associatedSpeciesIsDirty
            );
        },
    },
    emits: ['dirty'],
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
        //------fetch list of values
        const response = await fetch(
            '/api/occurrence_report/list_of_values.json'
        );
        vm.listOfValuesDict = await response.json();
        vm.land_form_list = vm.listOfValuesDict.land_form_list;
        vm.land_form_list.splice(0, 0, {
            id: '',
            name: '',
        });
        vm.rock_type_list = vm.listOfValuesDict.rock_type_list;
        vm.rock_type_list.splice(0, 0, {
            id: null,
            name: null,
        });
        vm.soil_type_list = vm.listOfValuesDict.soil_type_list;
        vm.soil_type_list.splice(0, 0, {
            id: null,
            name: null,
        });
        vm.soil_colour_list = vm.listOfValuesDict.soil_colour_list;
        vm.soil_colour_list.splice(0, 0, {
            id: null,
            name: null,
        });
        vm.soil_condition_list = vm.listOfValuesDict.soil_condition_list;
        vm.soil_condition_list.splice(0, 0, {
            id: null,
            name: null,
        });
        vm.drainage_list = vm.listOfValuesDict.drainage_list;
        vm.drainage_list.splice(0, 0, {
            id: null,
            name: null,
        });
        vm.intensity_list = vm.listOfValuesDict.intensity_list;
        vm.intensity_list.splice(0, 0, {
            id: null,
            name: null,
        });
        vm.speciesListRelatesToOptions =
            vm.listOfValuesDict.species_list_relates_to_list;

        vm.$nextTick(() => {
            vm.resetDirtyState();
        });
    },
    mounted: function () {
        let vm = this;
        vm.eventListeners();
        vm.initialiseLandFormSelect();
        vm.initialiseSoilTypeSelect();

        // Capture original state after Vue has fully processed the component
        // This prevents false "unsaved changes" warnings on new records
        vm.$nextTick(() => {
            vm.resetDirtyState();
        });
    },
    methods: {
        resetDirtyState: function () {
            this.originalHabitatComposition = JSON.stringify(
                this.occurrence_report_obj.habitat_composition
            );
            this.originalHabitatCondition = JSON.stringify(
                this.occurrence_report_obj.habitat_condition
            );
            this.originalVegetationStructure = JSON.stringify(
                this.occurrence_report_obj.vegetation_structure
            );
            this.originalFireHistory = JSON.stringify(
                this.occurrence_report_obj.fire_history
            );
            this.originalAssociatedSpecies = JSON.stringify(
                this.occurrence_report_obj.associated_species
            );
        },
        eventListeners: function () {
            let vm = this;
            $(vm.$refs.flowering_period_select)
                .select2({
                    theme: 'bootstrap-5',
                    allowClear: true,
                    placeholder: 'Select Flowering Period',
                    multiple: true,
                })
                .on('select2:select', function (e) {
                    var selected = $(e.currentTarget);
                    vm.occurrence_report_obj.conservation_attributes.flowering_period =
                        selected.val();
                })
                .on('select2:unselect', function (e) {
                    var selected = $(e.currentTarget);
                    vm.occurrence_report_obj.conservation_attributes.flowering_period =
                        selected.val();
                });
        },
        checkDate: function () {
            if (
                this.occurrence_report_obj.fire_history.last_fire_estimate ===
                ''
            ) {
                this.occurrence_report_obj.fire_history.last_fire_estimate =
                    null;
                return;
            }
            if (
                isNaN(
                    new Date(
                        this.occurrence_report_obj.fire_history
                            .last_fire_estimate
                    )
                )
            ) {
                return;
            }
            if (
                isNaN(
                    new Date(
                        this.occurrence_report_obj.fire_history
                            .last_fire_estimate + '-01'
                    )
                )
            ) {
                return;
            }
            if (
                new Date(
                    this.occurrence_report_obj.fire_history.last_fire_estimate +
                        '-01'
                ) > new Date()
            ) {
                this.occurrence_report_obj.fire_history.last_fire_estimate =
                    new Date().toISOString().slice(0, 7);
                this.$nextTick(() => {
                    this.$refs.last_fire_date.focus();
                });
                swal.fire({
                    title: 'Error',
                    text: 'Last fire estimate cannot be in the future',
                    icon: 'error',
                    customClass: {
                        confirmButton: 'btn btn-primary',
                    },
                });
            }
        },
        relatedSpeciesTextChanged: function (new_text) {
            this.occurrence_report_obj.associated_species.related_species =
                new_text;
        },
        initialiseLandFormSelect: function () {
            let vm = this;
            // Initialise select2 for proposed Conservation Criteria
            $(vm.$refs.land_form_select)
                .select2({
                    theme: 'bootstrap-5',
                    allowClear: true,
                    multiple: true,
                    placeholder: 'Select Land Form',
                })
                .on('select2:select', function (e) {
                    var selected = $(e.currentTarget);
                    vm.occurrence_report_obj.habitat_composition.land_form =
                        selected.val();
                })
                .on('select2:unselect', function (e) {
                    var selected = $(e.currentTarget);
                    vm.occurrence_report_obj.habitat_composition.land_form =
                        selected.val();
                });
        },
        initialiseSoilTypeSelect: function () {
            let vm = this;
            $(vm.$refs.soil_type_select)
                .select2({
                    theme: 'bootstrap-5',
                    allowClear: true,
                    multiple: true,
                    placeholder: 'Select Soil Type(s)',
                })
                .on('select2:select', function (e) {
                    var selected = $(e.currentTarget);
                    vm.occurrence_report_obj.habitat_composition.soil_type =
                        selected.val();
                })
                .on('select2:unselect', function (e) {
                    var selected = $(e.currentTarget);
                    vm.occurrence_report_obj.habitat_composition.soil_type =
                        selected.val();
                });
        },
        updateHabitatCompositionDetails: function () {
            let vm = this;
            vm.updatingHabitatCompositionDetails = true;
            fetch(
                helpers.add_endpoint_json(
                    api_endpoints.occurrence_report,
                    vm.occurrence_report_obj.id +
                        '/update_habitat_composition_details'
                ),
                {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify(
                        vm.occurrence_report_obj.habitat_composition
                    ),
                }
            ).then(async (response) => {
                let data = await response.json();
                if (!response.ok) {
                    swal.fire({
                        title: 'Error',
                        text:
                            'Habitat Composition details cannot be saved because of the following error: ' +
                            JSON.stringify(data),
                        icon: 'error',
                        customClass: {
                            confirmButton: 'btn btn-primary',
                        },
                    });
                    vm.updatingHabitatCompositionDetails = false;
                    return;
                }
                vm.updatingHabitatCompositionDetails = false;
                vm.occurrence_report_obj.habitat_composition = data;
                vm.originalHabitatComposition = JSON.stringify(
                    vm.occurrence_report_obj.habitat_composition
                );
                swal.fire({
                    title: 'Saved',
                    text: 'Habitat Composition details have been saved',
                    icon: 'success',
                    showConfirmButton: false,
                    timer: 1200,
                }).then(() => {
                    if (
                        vm.occurrence_report_obj.processing_status == 'Unlocked'
                    ) {
                        vm.$router.go();
                    }
                });
            });
        },
        updateHabitatConditionDetails: function () {
            let vm = this;
            if (!vm.validateKeigheryScaleTotal()) {
                return;
            }
            vm.updatingHabitatConditionDetails = true;
            fetch(
                helpers.add_endpoint_json(
                    api_endpoints.occurrence_report,
                    vm.occurrence_report_obj.id +
                        '/update_habitat_condition_details'
                ),
                {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify(
                        vm.occurrence_report_obj.habitat_condition
                    ),
                }
            ).then(async (response) => {
                let data = await response.json();
                if (!response.ok) {
                    swal.fire({
                        title: 'Error',
                        text:
                            'Habitat Condition details cannot be saved because of the following error: ' +
                            JSON.stringify(data),
                        icon: 'error',
                        customClass: {
                            confirmButton: 'btn btn-primary',
                        },
                    }).then(() => {
                        if (
                            vm.occurrence_report_obj.processing_status ==
                            'Unlocked'
                        ) {
                            vm.$router.go();
                        }
                    });
                    vm.updatingHabitatConditionDetails = false;
                }
                vm.updatingHabitatConditionDetails = false;
                vm.occurrence_report_obj.habitat_condition = data;
                vm.originalHabitatCondition = JSON.stringify(
                    vm.occurrence_report_obj.habitat_condition
                );
                swal.fire({
                    title: 'Saved',
                    text: 'Habitat Condition details have been saved',
                    icon: 'success',
                    showConfirmButton: false,
                    timer: 1200,
                }).then(() => {
                    if (
                        vm.occurrence_report_obj.processing_status == 'Unlocked'
                    ) {
                        vm.$router.go();
                    }
                });
            });
        },
        updateVegetationStructure: function () {
            let vm = this;
            vm.updatingVegetationStructure = true;
            fetch(
                helpers.add_endpoint_json(
                    api_endpoints.occurrence_report,
                    vm.occurrence_report_obj.id + '/update_vegetation_structure'
                ),
                {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify(
                        vm.occurrence_report_obj.vegetation_structure
                    ),
                }
            ).then(
                async (response) => {
                    vm.updatingVegetationStructure = false;
                    vm.occurrence_report_obj.vegetation_structure =
                        await response.json();
                    vm.originalVegetationStructure = JSON.stringify(
                        vm.occurrence_report_obj.vegetation_structure
                    );
                    swal.fire({
                        title: 'Saved',
                        text: 'Vegetation Structure details have been saved',
                        icon: 'success',
                        showConfirmButton: false,
                        timer: 1200,
                    }).then(() => {
                        if (
                            vm.occurrence_report_obj.processing_status ==
                            'Unlocked'
                        ) {
                            vm.$router.go();
                        }
                    });
                },
                (error) => {
                    var text = helpers.apiVueResourceError(error);
                    swal.fire({
                        title: 'Error',
                        text:
                            'Vegetation Structure details cannot be saved because of the following error: ' +
                            text,
                        icon: 'error',
                        customClass: {
                            confirmButton: 'btn btn-primary',
                        },
                    });
                    vm.updatingVegetationStructure = false;
                }
            );
        },
        updateFireHistoryDetails: function () {
            let vm = this;
            vm.updatingFireHistoryDetails = true;
            fetch(
                helpers.add_endpoint_json(
                    api_endpoints.occurrence_report,
                    vm.occurrence_report_obj.id + '/update_fire_history_details'
                ),
                {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify(vm.occurrence_report_obj.fire_history),
                }
            ).then(
                async (response) => {
                    vm.updatingFireHistoryDetails = false;
                    vm.occurrence_report_obj.fire_history =
                        await response.json();
                    vm.originalFireHistory = JSON.stringify(
                        vm.occurrence_report_obj.fire_history
                    );
                    swal.fire({
                        title: 'Saved',
                        text: 'Fire History details have been saved',
                        icon: 'success',
                        showConfirmButton: false,
                        timer: 1200,
                    }).then(() => {
                        if (
                            vm.occurrence_report_obj.processing_status ==
                            'Unlocked'
                        ) {
                            vm.$router.go();
                        }
                    });
                },
                (error) => {
                    var text = helpers.apiVueResourceError(error);
                    swal.fire({
                        title: 'Error',
                        text:
                            'Fire History details cannot be saved because of the following error: ' +
                            text,
                        icon: 'error',
                        customClass: {
                            confirmButton: 'btn btn-primary',
                        },
                    });
                    vm.updatingFireHistoryDetails = false;
                }
            );
        },
        updateAssociatedSpeciesDetails: function () {
            let vm = this;
            vm.updatingAssociatedSpeciesDetails = true;
            fetch(
                helpers.add_endpoint_json(
                    api_endpoints.occurrence_report,
                    vm.occurrence_report_obj.id +
                        '/update_associated_species_details'
                ),
                {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify(
                        vm.occurrence_report_obj.associated_species
                    ),
                }
            ).then(
                async (response) => {
                    vm.updatingAssociatedSpeciesDetails = false;
                    vm.occurrence_report_obj.associated_species =
                        await response.json();
                    vm.originalAssociatedSpecies = JSON.stringify(
                        vm.occurrence_report_obj.associated_species
                    );
                    swal.fire({
                        title: 'Saved',
                        text: 'Associated Species details have been saved',
                        icon: 'success',
                        showConfirmButton: false,
                        timer: 1200,
                    }).then(() => {
                        if (
                            vm.occurrence_report_obj.processing_status ==
                            'Unlocked'
                        ) {
                            vm.$router.go();
                        }
                    });
                },
                (error) => {
                    var text = helpers.apiVueResourceError(error);
                    swal.fire({
                        title: 'Error',
                        text:
                            'Associated Species cannot be saved because of the following error: ' +
                            text,
                        icon: 'error',
                        customClass: {
                            confirmButton: 'btn btn-primary',
                        },
                    });
                    vm.updatingAssociatedSpeciesDetails = false;
                }
            );
        },
        checkKeigheryScaleTotal: function (event, key) {
            if (this.keigheryScaleTotal > 100.0) {
                let difference = (100.0 - this.keigheryScaleTotal).toFixed(2);
                let targetValue = event.target.value;
                let newValue = (
                    Number(targetValue) + Number(difference)
                ).toFixed(2);
                if (newValue < 0) {
                    newValue = Number(0.0).toFixed(2);
                }
                this.occurrence_report_obj.habitat_condition[key] = newValue;
            }
        },
        validateKeigheryScaleTotal: function () {
            let vm = this;
            if (
                vm.keigheryScaleTotal != (100.0).toFixed(2) &&
                vm.keigheryScaleTotal != (0.0).toFixed(2)
            ) {
                swal.fire({
                    title: 'Keighery Scale Total Error',
                    text:
                        'Keighery Scale total should be either 0% or 100%. Currently the total is ' +
                        vm.keigheryScaleTotal +
                        '%.',
                    icon: 'error',
                    customClass: {
                        confirmButton: 'btn btn-primary',
                    },
                });
                return false;
            }
            return true;
        },
    },
};
</script>

<style lang="css" scoped>
/*ul, li {
        zoom:1;
        display: inline;
    }*/
fieldset.scheduler-border {
    border: 1px groove #ddd !important;
    padding: 0 1.4em 1.4em 1.4em !important;
    margin: 0 0 1.5em 0 !important;
    -webkit-box-shadow: 0px 0px 0px 0px #000;
    box-shadow: 0px 0px 0px 0px #000;
}

legend.scheduler-border {
    width: inherit;
    /* Or auto */
    padding: 0 10px;
    /* To give a bit of padding on the left and right */
    border-bottom: none;
}

input[type='text'],
select {
    width: 100%;
    padding: 0.375rem 2.25rem 0.375rem 0.75rem;
}

input[type='number'] {
    width: 50%;
}

input.ocr_number {
    width: 20%;
}
</style>
