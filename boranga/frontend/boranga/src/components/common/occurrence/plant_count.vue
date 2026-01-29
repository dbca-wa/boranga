<template lang="html">
    <div id="plant_count">
        <div class="row mb-3">
            <label for="" class="col-sm-3 control-label"
                >Plant Count Method:</label
            >
            <div class="col-sm-9">
                <template v-if="!isReadOnly">
                    <template
                        v-if="
                            plant_count_method_list &&
                            plant_count_method_list.length > 0 &&
                            plant_count.plant_count_method_id &&
                            !plant_count_method_list
                                .map((d) => d.id)
                                .includes(plant_count.plant_count_method_id)
                        "
                    >
                        <input
                            v-if="plant_count.plant_count_method"
                            type="text"
                            class="form-control mb-3"
                            :value="
                                plant_count.plant_count_method +
                                ' (Now Archived)'
                            "
                            disabled
                        />
                        <div class="mb-3 text-muted">
                            Change plant count method to:
                        </div>
                    </template>
                    <select
                        v-model="plant_count.plant_count_method_id"
                        class="form-select"
                    >
                        <option
                            v-for="plant_count_method in plant_count_method_list"
                            :key="plant_count_method.id"
                            :value="plant_count_method.id"
                        >
                            {{ plant_count_method.name }}
                        </option>
                    </select>
                </template>
                <template v-else>
                    <input
                        v-model="plant_count.plant_count_method"
                        class="form-control"
                        type="text"
                        :disabled="isReadOnly"
                    />
                </template>
            </div>
        </div>
        <div class="row mb-3">
            <label for="" class="col-sm-3 control-label"
                >Plant Count Accuracy:</label
            >
            <div class="col-sm-9">
                <template v-if="!isReadOnly">
                    <template
                        v-if="
                            plant_count_accuracy_list &&
                            plant_count_accuracy_list.length > 0 &&
                            plant_count.plant_count_accuracy_id &&
                            !plant_count_accuracy_list
                                .map((d) => d.id)
                                .includes(plant_count.plant_count_accuracy_id)
                        "
                    >
                        <input
                            v-if="plant_count.plant_count_accuracy"
                            type="text"
                            class="form-control mb-3"
                            :value="
                                plant_count.plant_count_accuracy +
                                ' (Now Archived)'
                            "
                            disabled
                        />
                        <div class="mb-3 text-muted">
                            Change plant count accuracy to:
                        </div>
                    </template>
                    <select
                        v-model="plant_count.plant_count_accuracy_id"
                        class="form-select"
                    >
                        <option
                            v-for="plant_count_accuracy in plant_count_accuracy_list"
                            :key="plant_count_accuracy.id"
                            :value="plant_count_accuracy.id"
                        >
                            {{ plant_count_accuracy.name }}
                        </option>
                    </select>
                </template>
                <template v-else>
                    <input
                        v-model="plant_count.plant_count_accuracy"
                        class="form-control"
                        type="text"
                        :disabled="isReadOnly"
                    />
                </template>
            </div>
        </div>
        <div class="row mb-3">
            <label for="" class="col-sm-3 control-label"
                >Counted Subject:</label
            >
            <div class="col-sm-9">
                <template v-if="!isReadOnly">
                    <template
                        v-if="
                            counted_subject_list &&
                            counted_subject_list.length > 0 &&
                            plant_count.counted_subject_id &&
                            !counted_subject_list
                                .map((d) => d.id)
                                .includes(plant_count.counted_subject_id)
                        "
                    >
                        <input
                            v-if="plant_count.counted_subject"
                            type="text"
                            class="form-control mb-3"
                            :value="
                                plant_count.counted_subject + ' (Now Archived)'
                            "
                            disabled
                        />
                        <div class="mb-3 text-muted">
                            Change counted subject to:
                        </div>
                    </template>
                    <select
                        v-model="plant_count.counted_subject_id"
                        class="form-select"
                    >
                        <option
                            v-for="counted_subject in counted_subject_list"
                            :key="counted_subject.id"
                            :value="counted_subject.id"
                        >
                            {{ counted_subject.name }}
                        </option>
                    </select>
                </template>
                <template v-else>
                    <input
                        v-model="plant_count.counted_subject"
                        class="form-control"
                        type="text"
                        :disabled="isReadOnly"
                    />
                </template>
            </div>
        </div>
        <div class="row mb-3">
            <label for="" class="col-sm-3 control-label"
                >Observation Date:
            </label>
            <div class="col-sm-9">
                <input
                    v-model="plant_count.obs_date"
                    :disabled="true"
                    type="date"
                    class="form-control"
                    name="plant_obs_date"
                />
            </div>
        </div>
        <div class="row mb-3">
            <label for="" class="col-sm-3 control-label"
                >Estimated Population Area:</label
            >
            <div class="col-sm-6">
                <div class="input-group">
                    <input
                        id="est_population_area"
                        v-model="plant_count.estimated_population_area"
                        :disabled="isReadOnly"
                        type="number"
                        class="form-control ocr_number"
                        placeholder=""
                        min="0.00"
                        max="9999999999.99"
                        step="0.01"
                    />
                    <span class="input-group-text">m<sup>2</sup></span>
                </div>
            </div>
        </div>
        <div class="row mb-3">
            <label for="" class="col-sm-3 control-label"
                >Condition of Plants:</label
            >
            <div class="col-sm-9">
                <template v-if="!isReadOnly">
                    <template
                        v-if="
                            plant_condition_list &&
                            plant_condition_list.length > 0 &&
                            plant_count.plant_condition_id &&
                            !plant_condition_list
                                .map((d) => d.id)
                                .includes(plant_count.plant_condition_id)
                        "
                    >
                        <input
                            v-if="plant_count.plant_condition"
                            type="text"
                            class="form-control mb-3"
                            :value="
                                plant_count.plant_condition + ' (Now Archived)'
                            "
                            disabled
                        />
                        <div class="mb-3 text-muted">
                            Change plant count accuracy to:
                        </div>
                    </template>
                    <select
                        v-model="plant_count.plant_condition_id"
                        class="form-select"
                    >
                        <option
                            v-for="plant_condition in plant_condition_list"
                            :key="plant_condition.id"
                            :value="plant_condition.id"
                        >
                            {{ plant_condition.name }}
                        </option>
                    </select>
                </template>
                <template v-else>
                    <input
                        v-model="plant_count.plant_condition"
                        class="form-control"
                        type="text"
                        :disabled="isReadOnly"
                    />
                </template>
            </div>
        </div>
        <label for="" class="col-lg-3 control-label fs-5 fw-bold mb-3"
            >Plant Count Type Used</label
        >
        <div class="row mb-3">
            <div class="col">
                <div class="form-check form-check-inline">
                    <input
                        class="form-check-input"
                        type="radio"
                        name="count-status"
                        id="count-status-not-counted"
                        value="not_counted"
                        v-model="plant_count.count_status"
                    />
                    <label class="form-check-label" for="inlineRadio1"
                        >Not Counted</label
                    >
                </div>
                <div class="form-check form-check-inline">
                    <input
                        class="form-check-input"
                        type="radio"
                        name="count-status"
                        id="count-status-simple-count"
                        value="simple_count"
                        v-model="plant_count.count_status"
                    />
                    <label class="form-check-label" for="inlineRadio2"
                        >Simple Count</label
                    >
                </div>
                <div class="form-check form-check-inline">
                    <input
                        class="form-check-input"
                        type="radio"
                        name="count-status"
                        id="count-status-detailed-count"
                        value="detailed_count"
                        v-model="plant_count.count_status"
                    />
                    <label class="form-check-label" for="inlineRadio3"
                        >Detailed Count</label
                    >
                </div>
            </div>
        </div>
        <div
            id="plant-count-detailed"
            v-if="plant_count.count_status == 'detailed_count'"
        >
            <label for="" class="col-lg-3 control-label fs-5 fw-bold mb-3"
                >Plant Count - Detailed</label
            >
            <div class="row mb-3">
                <div class="col-sm-3"></div>
                <div class="col-sm-2 fw-bold">Mature</div>
                <div class="col-sm-2 fw-bold">Juvenile</div>
                <div class="col-sm-2 fw-bold">Seedling</div>
            </div>
            <div class="row mb-3">
                <div class="col-sm-3 fw-bold">Alive</div>
                <div class="col-sm-2">
                    <input
                        id="alive_mature"
                        v-model="plant_count.detailed_alive_mature"
                        :disabled="isReadOnly"
                        type="number"
                        class="form-control plant_count"
                        placeholder=""
                        min="0"
                    />
                </div>
                <div class="col-sm-2">
                    <input
                        id="plant_alive_juvenile"
                        v-model="plant_count.detailed_alive_juvenile"
                        :disabled="isReadOnly"
                        type="number"
                        class="form-control plant_count"
                        placeholder=""
                        min="0"
                    />
                </div>
                <div class="col-sm-2">
                    <input
                        id="alive_seedling"
                        v-model="plant_count.detailed_alive_seedling"
                        :disabled="isReadOnly"
                        type="number"
                        class="form-control plant_count"
                        placeholder=""
                        min="0"
                    />
                </div>
            </div>
            <div class="row mb-3">
                <div class="col-sm-3 fw-bold">Dead</div>
                <div class="col-sm-2">
                    <input
                        id="dead_mature"
                        v-model="plant_count.detailed_dead_mature"
                        :disabled="isReadOnly"
                        type="number"
                        class="form-control plant_count"
                        placeholder=""
                        min="0"
                    />
                </div>
                <div class="col-sm-2">
                    <input
                        id="plant_dead_juvenile"
                        v-model="plant_count.detailed_dead_juvenile"
                        :disabled="isReadOnly"
                        type="number"
                        class="form-control plant_count"
                        placeholder=""
                        min="0"
                    />
                </div>
                <div class="col-sm-2">
                    <input
                        id="dead_seedling"
                        v-model="plant_count.detailed_dead_seedling"
                        :disabled="isReadOnly"
                        type="number"
                        class="form-control plant_count"
                        placeholder=""
                        min="0"
                    />
                </div>
            </div>
        </div>
        <div
            id="plant-count-simple"
            v-if="plant_count.count_status == 'simple_count'"
        >
            <label for="" class="col-lg-3 control-label fs-5 fw-bold mb-3"
                >Plant Count - Simple</label
            >
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label"
                    >Number alive:</label
                >
                <div class="col-sm-6">
                    <input
                        id="simple_alive"
                        v-model="plant_count.simple_alive"
                        :disabled="isReadOnly"
                        type="number"
                        class="form-control ocr_number"
                        placeholder=""
                        min="0"
                    />
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label"
                    >Number dead:</label
                >
                <div class="col-sm-6">
                    <input
                        id="simple_dead"
                        v-model="plant_count.simple_dead"
                        :disabled="isReadOnly"
                        type="number"
                        class="form-control ocr_number"
                        placeholder=""
                        min="0"
                    />
                </div>
            </div>
        </div>
        <label for="" class="col-lg-3 control-label fs-5 fw-bold mb-3"
            >Quadrats</label
        >
        <div class="row mb-3">
            <label class="col-sm-3 control-label">Quadrats Present?</label>
            <div class="col">
                <div class="form-check form-check-inline">
                    <label for="quadratsPresentYes">Yes</label>
                    <input
                        id="quadratsPresentYes"
                        v-model="plant_count.quadrats_present"
                        :disabled="isReadOnly"
                        class="form-check-input"
                        type="radio"
                        value="true"
                    />&nbsp;
                </div>
                <div class="form-check form-check-inline">
                    <label for="quadratsPresentNo">No</label>
                    <input
                        id="quadratsPresentNo"
                        v-model="plant_count.quadrats_present"
                        :disabled="isReadOnly"
                        class="form-check-input"
                        type="radio"
                        value="false"
                    />&nbsp;
                </div>
            </div>
        </div>
        <div class="row mb-3">
            <label class="col-sm-3 control-label"
                >Quadrats Data attached?</label
            >
            <div class="col">
                <div class="form-check form-check-inline">
                    <label for="quadratsDataYes">Yes</label>
                    <input
                        id="quadratsDataYes"
                        v-model="plant_count.quadrats_data_attached"
                        :disabled="isReadOnly"
                        class="form-check-input"
                        type="radio"
                        value="true"
                    />&nbsp;
                </div>
                <div class="form-check form-check-inline">
                    <label for="quadratsDataNo">No</label>
                    <input
                        id="quadratsDataNo"
                        v-model="plant_count.quadrats_data_attached"
                        :disabled="isReadOnly"
                        class="form-check-input"
                        type="radio"
                        value="false"
                    />&nbsp;
                </div>
            </div>
        </div>
        <div class="row mb-3">
            <label for="" class="col-sm-3 control-label"
                >Number of quadrats surveyed:</label
            >
            <div class="col-sm-6">
                <input
                    id="quadrats_surveyed"
                    v-model="plant_count.quadrats_surveyed"
                    :disabled="isReadOnly"
                    type="number"
                    class="form-control ocr_number"
                    placeholder=""
                    min="0"
                />
            </div>
        </div>
        <div class="row mb-3">
            <label for="" class="col-sm-3 control-label"
                >Individual quadrat area:</label
            >
            <div class="col-sm-4">
                <div class="input-group w-75">
                    <input
                        id="ind_quadrat_area"
                        v-model="plant_count.individual_quadrat_area"
                        :disabled="isReadOnly"
                        type="number"
                        class="form-control"
                        placeholder=""
                        min="0.00"
                        step="0.01"
                        max="9999999999.99"
                        @change.prevent="
                            if ($event.target.value > 9999999999.99) {
                                plant_count.individual_quadrat_area = 9999999999.99;
                            } else if ($event.target.value < 0) {
                                plant_count.individual_quadrat_area = 0;
                            }
                        "
                    />
                    <span class="input-group-text">m<sup>2</sup></span>
                </div>
            </div>
        </div>
        <div class="row mb-3">
            <label for="" class="col-sm-3 control-label"
                >Total quadrat area:</label
            >
            <div class="col-sm-4">
                <div class="input-group w-75">
                    <input
                        id="tot_quadrat_area"
                        v-model="plant_count.total_quadrat_area"
                        :disabled="isReadOnly"
                        type="number"
                        class="form-control ocr_number"
                        placeholder=""
                        min="0.00"
                        step="0.01"
                        max="9999999999.99"
                        @change.prevent="
                            if ($event.target.value > 9999999999.99) {
                                plant_count.total_quadrat_area = 9999999999.99;
                            } else if ($event.target.value < 0) {
                                plant_count.total_quadrat_area = 0;
                            }
                        "
                    />
                    <span class="input-group-text">m<sup>2</sup></span>
                </div>
            </div>
        </div>
        <div class="row mb-3">
            <label for="" class="col-sm-3 control-label"
                >Flowering plants:</label
            >
            <div class="col-sm-4">
                <div class="input-group w-50">
                    <input
                        id="flow_plant_per"
                        v-model="plant_count.flowering_plants_per"
                        :disabled="isReadOnly"
                        type="number"
                        class="form-control ocr_number"
                        placeholder=""
                        min="0.00"
                        max="100.00"
                        step="0.01"
                        @change.prevent="
                            if ($event.target.value > 100) {
                                plant_count.flowering_plants_per = 100;
                            } else if ($event.target.value < 0) {
                                plant_count.flowering_plants_per = 0;
                            }
                        "
                    />
                    <span class="input-group-text">%</span>
                </div>
            </div>
        </div>
        <label for="" class="col-lg-3 control-label fs-5 fw-bold"></label>
        <div class="row mb-3">
            <label class="col-sm-3 control-label"
                >Clonal Reproduction present?</label
            >
            <div class="col">
                <div class="form-check form-check-inline">
                    <label for="reprodPresentYes">Yes</label>
                    <input
                        id="reprodPresentYes"
                        v-model="plant_count.clonal_reproduction_present"
                        :disabled="isReadOnly"
                        class="form-check-input"
                        type="radio"
                        value="true"
                    />&nbsp;
                </div>
                <div class="form-check form-check-inline">
                    <label for="reprodPresentNo">No</label>
                    <input
                        id="reprodPresentNo"
                        v-model="plant_count.clonal_reproduction_present"
                        :disabled="isReadOnly"
                        class="form-check-input"
                        type="radio"
                        value="false"
                    />&nbsp;
                </div>
            </div>
        </div>
        <div class="row mb-3">
            <label class="col-sm-3 control-label"
                >Vegetative State present?</label
            >
            <div class="col">
                <div class="form-check form-check-inline">
                    <label for="vegStatePresentYes">Yes</label>
                    <input
                        id="vegStatePresentYes"
                        v-model="plant_count.vegetative_state_present"
                        :disabled="isReadOnly"
                        class="form-check-input"
                        type="radio"
                        value="true"
                    />&nbsp;
                </div>
                <div class="form-check form-check-inline">
                    <label for="vegStatePresentNo">No</label>
                    <input
                        id="vegStatePresentNo"
                        v-model="plant_count.vegetative_state_present"
                        :disabled="isReadOnly"
                        class="form-check-input"
                        type="radio"
                        value="false"
                    />&nbsp;
                </div>
            </div>
        </div>
        <div class="row mb-3">
            <label class="col-sm-3 control-label">Flower Buds present?</label>
            <div class="col">
                <div class="form-check form-check-inline">
                    <label for="flowerBudsPresentYes">Yes</label>
                    <input
                        id="flowerBudsPresentYes"
                        v-model="plant_count.flower_bud_present"
                        :disabled="isReadOnly"
                        class="form-check-input"
                        type="radio"
                        value="true"
                    />&nbsp;
                </div>
                <div class="form-check form-check-inline">
                    <label for="flowerBudsPresentNo">No</label>
                    <input
                        id="flowerBudsPresentNo"
                        v-model="plant_count.flower_bud_present"
                        :disabled="isReadOnly"
                        class="form-check-input"
                        type="radio"
                        value="false"
                    />&nbsp;
                </div>
            </div>
        </div>
        <div class="row mb-3">
            <label class="col-sm-3 control-label">Flowers present?</label>
            <div class="col">
                <div class="form-check form-check-inline">
                    <label for="flowerPresentYes">Yes</label>
                    <input
                        id="flowerPresentYes"
                        v-model="plant_count.flower_present"
                        :disabled="isReadOnly"
                        class="form-check-input"
                        type="radio"
                        value="true"
                    />&nbsp;
                </div>
                <div class="form-check form-check-inline">
                    <label for="flowerPresentNo">No</label>
                    <input
                        id="flowerPresentNo"
                        v-model="plant_count.flower_present"
                        :disabled="isReadOnly"
                        class="form-check-input"
                        type="radio"
                        value="false"
                    />&nbsp;
                </div>
            </div>
        </div>
        <div class="row mb-3">
            <label class="col-sm-3 control-label"
                >Immature Fruit present?</label
            >
            <div class="col">
                <div class="form-check form-check-inline">
                    <label for="immFruitPresentYes">Yes</label>
                    <input
                        id="immFruitPresentYes"
                        v-model="plant_count.immature_fruit_present"
                        :disabled="isReadOnly"
                        class="form-check-input"
                        type="radio"
                        value="true"
                    />&nbsp;
                </div>
                <div class="form-check form-check-inline">
                    <label for="immFruitPresentNo">No</label>
                    <input
                        id="immFruitPresentNo"
                        v-model="plant_count.immature_fruit_present"
                        :disabled="isReadOnly"
                        class="form-check-input"
                        type="radio"
                        value="false"
                    />&nbsp;
                </div>
            </div>
        </div>
        <div class="row mb-3">
            <label class="col-sm-3 control-label">Ripe Fruit present?</label>
            <div class="col">
                <div class="form-check form-check-inline">
                    <label for="ripeFruitPresentYes">Yes</label>
                    <input
                        id="ripeFruitPresentYes"
                        v-model="plant_count.ripe_fruit_present"
                        :disabled="isReadOnly"
                        class="form-check-input"
                        type="radio"
                        value="true"
                    />&nbsp;
                </div>
                <div class="form-check form-check-inline">
                    <label for="ripeFruitPresentNo">No</label>
                    <input
                        id="ripeFruitPresentNo"
                        v-model="plant_count.ripe_fruit_present"
                        :disabled="isReadOnly"
                        class="form-check-input"
                        type="radio"
                        value="false"
                    />&nbsp;
                </div>
            </div>
        </div>
        <div class="row mb-3">
            <label class="col-sm-3 control-label"
                >Dehisced Fruit present?</label
            >
            <div class="col">
                <div class="form-check form-check-inline">
                    <label for="dehFruitPresentYes">Yes</label>
                    <input
                        id="dehFruitPresentYes"
                        v-model="plant_count.dehisced_fruit_present"
                        :disabled="isReadOnly"
                        class="form-check-input"
                        type="radio"
                        value="true"
                    />&nbsp;
                </div>
                <div class="form-check form-check-inline">
                    <label for="dehFruitPresentNo">No</label>
                    <input
                        id="dehFruitPresentNo"
                        v-model="plant_count.dehisced_fruit_present"
                        :disabled="isReadOnly"
                        class="form-check-input"
                        type="radio"
                        value="false"
                    />&nbsp;
                </div>
            </div>
        </div>
        <div class="row mb-3">
            <label for="" class="col-sm-3 control-label"
                >Pollinator Observations:</label
            >
            <div class="col-sm-9">
                <textarea
                    id="pollinator_obs"
                    v-model="plant_count.pollinator_observation"
                    :disabled="isReadOnly"
                    type="text"
                    row="2"
                    class="form-control"
                    placeholder=""
                />
            </div>
        </div>
        <div class="row mb-3">
            <label for="" class="col-sm-3 control-label"
                >Plant Count Comments:</label
            >
            <div class="col-sm-9">
                <textarea
                    id="plant_count_comment"
                    v-model="plant_count.comment"
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
                <span v-if="plant_count.copied_ocr" class="float-end"
                    ><b>Sourced from {{ plant_count.copied_ocr }}</b></span
                >
            </div>
        </div>
        <div class="row mb-3">
            <div class="col-sm-12">
                <button
                    v-if="!updatingPlantCountDetails"
                    class="btn btn-sm float-end"
                    :class="{
                        'btn-primary': plantCountIsDirty,
                        'btn-light': !plantCountIsDirty,
                        border: !plantCountIsDirty,
                    }"
                    :disabled="isReadOnly || !plantCountIsDirty"
                    @click.prevent="updatePlantCountDetails()"
                >
                    <template v-if="plantCountIsDirty"
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
    </div>
</template>

<script>
import { api_endpoints, helpers } from '@/utils/hooks';
export default {
    name: 'PlantCount',
    props: {
        plant_count: {
            type: Object,
            required: true,
        },
        is_report: {
            type: Boolean,
            required: true,
        },
        occurrence_id: {
            type: Number,
            required: true,
        },
        processing_status: {
            type: String,
            required: false,
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
        isReadOnly: {
            type: Boolean,
            default: false,
        },
    },
    data: function () {
        return {
            //----list of values dictionary
            listOfPlantValuesDict: {},
            plant_count_method_list: [],
            plant_count_accuracy_list: [],
            plant_condition_list: [],
            counted_subject_list: [],
            updatingPlantCountDetails: false,
            originalPlantCount: JSON.stringify(this.plant_count), // to check if the plant count details are changed
        };
    },
    emits: ['update-plant-count', 'dirty'],
    computed: {
        plantCountIsDirty: function () {
            let vm = this;
            return JSON.stringify(vm.plant_count) !== vm.originalPlantCount;
        },
        isDirty: function () {
            return this.plantCountIsDirty;
        },
    },
    watch: {
        isDirty: function (newValue) {
            let vm = this;
            if (newValue) {
                vm.$emit('dirty', true);
            } else {
                vm.$emit('dirty', false);
            }
        },
    },
    created: async function () {
        let vm = this;
        const response = await fetch(
            `/api/occurrence/plant_count_list_of_values.json`
        );
        vm.listOfPlantValuesDict = await response.json();
        vm.plant_count_method_list =
            vm.listOfPlantValuesDict.plant_count_method_list;
        vm.plant_count_method_list.splice(0, 0, {
            id: null,
            name: null,
        });
        vm.plant_count_accuracy_list =
            vm.listOfPlantValuesDict.plant_count_accuracy_list;
        vm.plant_count_accuracy_list.splice(0, 0, {
            id: null,
            name: null,
        });
        vm.plant_condition_list = vm.listOfPlantValuesDict.plant_condition_list;
        vm.plant_condition_list.splice(0, 0, {
            id: null,
            name: null,
        });
        vm.counted_subject_list = vm.listOfPlantValuesDict.counted_subject_list;
        vm.counted_subject_list.splice(0, 0, {
            id: null,
            name: null,
        });
        vm.$nextTick(() => {
            vm.resetDirtyState();
        });
    },
    methods: {
        resetDirtyState: function () {
            this.originalPlantCount = JSON.stringify(this.plant_count);
        },
        updatePlantCountDetails: function () {
            let vm = this;
            vm.updatingPlantCountDetails = true;
            let endpoint = api_endpoints.occurrence;
            if (vm.is_report) {
                endpoint = api_endpoints.occurrence_report;
            }
            fetch(
                helpers.add_endpoint_json(
                    endpoint,
                    vm.occurrence_id + '/update_plant_count_details'
                ),
                {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify(vm.plant_count),
                }
            )
                .then(
                    async (response) => {
                        const data = await response.json();
                        if (!response.ok) {
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
                        vm.$emit('update-plant-count', data);
                        vm.originalPlantCount = JSON.stringify(data);

                        swal.fire({
                            title: 'Saved',
                            text: 'Plant Count details have been saved',
                            icon: 'success',
                            showConfirmButton: false,
                            timer: 1200,
                        }).then(() => {
                            if (vm.processing_status == 'Unlocked') {
                                vm.$router.go();
                            }
                        });
                    },
                    (error) => {
                        var text = helpers.apiVueResourceError(error);
                        swal.fire({
                            title: 'Error',
                            text:
                                'Plant Count details cannot be saved because of the following error: ' +
                                text,
                            icon: 'error',
                            customClass: {
                                confirmButton: 'btn btn-primary',
                            },
                        });
                    }
                )
                .finally(() => {
                    vm.updatingPlantCountDetails = false;
                });
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

input.plant_count {
    width: 63%;
}

input.ocr_number {
    width: 20%;
}
</style>
