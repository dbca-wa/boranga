<template lang="html">
    <div id="animal_observation">
        <form @change="calculateTotalNumberSeen">
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label"
                    >Primary Detection Method:</label
                >
                <div class="col-sm-9">
                    <select
                        ref="primary_detection_select"
                        v-model="animal_observation.primary_detection_method"
                        :disabled="isReadOnly"
                        class="form-select input-sm"
                    >
                        <option
                            v-for="option in primary_detection_method_list"
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
                    >Secondary Signs:</label
                >
                <div class="col-sm-9">
                    <select
                        ref="secondary_sign_select"
                        v-model="animal_observation.secondary_sign"
                        :disabled="isReadOnly"
                        class="form-select input-sm"
                        multiple
                    >
                        <option
                            v-for="secondary_sign in secondary_sign_list"
                            :key="secondary_sign.id"
                            :value="secondary_sign.id"
                            :disabled="secondary_sign.disabled"
                        >
                            {{ secondary_sign.name }}
                            <template v-if="secondary_sign.disabled">
                                (Archived)
                            </template>
                        </option>
                    </select>
                    <div
                        v-if="archivedSecondarySignNames.length"
                        class="mt-2 small text-muted"
                    >
                        Archived selections:
                        {{ archivedSecondarySignNames.join(', ') }}
                    </div>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label"
                    >Animal Behaviour:</label
                >
                <div class="col-sm-9">
                    <template v-if="!isReadOnly">
                        <template
                            v-if="
                                animal_behaviour_list &&
                                animal_behaviour_list.length > 0 &&
                                animal_observation.animal_behaviour &&
                                !animal_behaviour_list
                                    .map((d) => d.id)
                                    .includes(
                                        animal_observation.animal_behaviour
                                    )
                            "
                        >
                            <input
                                v-if="animal_observation.animal_behaviour_name"
                                type="text"
                                class="form-control mb-3"
                                :value="
                                    animal_observation.animal_behaviour_name +
                                    ' (Now Archived)'
                                "
                                disabled
                            />
                            <div class="mb-3 text-muted">
                                Change animal observation to:
                            </div>
                        </template>
                        <select
                            v-model="animal_observation.animal_behaviour"
                            class="form-select"
                        >
                            <option
                                v-for="animal_behaviour in animal_behaviour_list"
                                :key="animal_behaviour.id"
                                :value="animal_behaviour.id"
                            >
                                {{ animal_behaviour.name }}
                            </option>
                        </select>
                    </template>
                    <template v-else>
                        <input
                            v-model="animal_observation.animal_behaviour_name"
                            class="form-control"
                            type="text"
                            :disabled="isReadOnly"
                        />
                    </template>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label"
                    >Reproductive State:</label
                >
                <div class="col-sm-9">
                    <select
                        ref="reproductive_state_select"
                        v-model="animal_observation.reproductive_state"
                        :disabled="isReadOnly"
                        class="form-select input-sm"
                    >
                        <option
                            v-for="option in reproductive_state_list"
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
                    >Animal Health:</label
                >
                <div class="col-sm-9">
                    <template v-if="!isReadOnly">
                        <template
                            v-if="
                                animal_health_list &&
                                animal_health_list.length > 0 &&
                                animal_observation.animal_health &&
                                !animal_health_list
                                    .map((d) => d.id)
                                    .includes(animal_observation.animal_health)
                            "
                        >
                            <input
                                v-if="animal_observation.animal_health_name"
                                type="text"
                                class="form-control mb-3"
                                :value="
                                    animal_observation.animal_health_name +
                                    ' (Now Archived)'
                                "
                                disabled
                            />
                            <div class="mb-3 text-muted">
                                Change animal health to:
                            </div>
                        </template>
                        <select
                            v-model="animal_observation.animal_health"
                            class="form-select"
                        >
                            <option
                                v-for="animal_health in animal_health_list"
                                :key="animal_health.id"
                                :value="animal_health.id"
                            >
                                {{ animal_health.name }}
                            </option>
                        </select>
                    </template>
                    <template v-else>
                        <input
                            v-model="animal_observation.animal_health_name"
                            class="form-control"
                            type="text"
                            :disabled="isReadOnly"
                        />
                    </template>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label"
                    >Cause of Death/Injury:</label
                >
                <div class="col-sm-9">
                    <template v-if="!isReadOnly">
                        <template
                            v-if="
                                death_injury_reason_list &&
                                death_injury_reason_list.length > 0 &&
                                animal_observation.death_injury_reason &&
                                !death_injury_reason_list
                                    .map((d) => d.id)
                                    .includes(
                                        animal_observation.death_injury_reason
                                    )
                            "
                        >
                            <input
                                v-if="
                                    animal_observation.death_injury_reason_name
                                "
                                type="text"
                                class="form-control mb-3"
                                :value="
                                    animal_observation.death_injury_reason_name +
                                    ' (Now Archived)'
                                "
                                disabled
                            />
                            <div class="mb-3 text-muted">
                                Change death reason to:
                            </div>
                        </template>
                        <select
                            v-model="animal_observation.death_injury_reason"
                            class="form-select"
                        >
                            <option
                                v-for="death_injury_reason in death_injury_reason_list"
                                :key="death_injury_reason.id"
                                :value="death_injury_reason.id"
                            >
                                {{ death_injury_reason.name }}
                            </option>
                        </select>
                    </template>
                    <template v-else>
                        <input
                            v-model="
                                animal_observation.death_injury_reason_name
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
                    >Distinctive Features:</label
                >
                <div class="col-sm-9">
                    <textarea
                        id="distinct_features"
                        v-model="animal_observation.distinctive_feature"
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
                    >Actions Taken:</label
                >
                <div class="col-sm-9">
                    <textarea
                        id="distinct_features"
                        v-model="animal_observation.action_taken"
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
                    >Actions Required:</label
                >
                <div class="col-sm-9">
                    <textarea
                        id="distinct_features"
                        v-model="animal_observation.action_required"
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
                    >Animal Observation Comments:</label
                >
                <div class="col-sm-9">
                    <textarea
                        id="distinct_features"
                        v-model="
                            animal_observation.animal_observation_detail_comment
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
                <label for="" class="col-sm-3 control-label"
                    >Observation Date:
                </label>
                <div class="col-sm-9">
                    <input
                        v-model="animal_observation.obs_date"
                        :disabled="true"
                        type="date"
                        class="form-control"
                        name="animal_obs_date"
                    />
                </div>
            </div>
            <label for="" class="col-lg-3 control-label fs-5 fw-bold mb-3"
                >Animal Count Method</label
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
                            v-model="animal_observation.count_status"
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
                            v-model="animal_observation.count_status"
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
                            v-model="animal_observation.count_status"
                        />
                        <label class="form-check-label" for="inlineRadio3"
                            >Detailed Count</label
                        >
                    </div>
                </div>
            </div>

            <div
                id="animal-count-detailed"
                v-if="animal_observation.count_status == 'detailed_count'"
            >
                <label for="" class="col-lg control-label fs-5 fw-bold"
                    >Animal Count - Detailed:</label
                >
                <div class="row mb-3">
                    <div class="col-sm-4"></div>
                    <div class="col-sm-2 fw-bold">Adult</div>
                    <div class="col-sm-2 fw-bold">Juvenile</div>
                    <div class="col-sm-2 fw-bold">Age Unknown</div>
                </div>
                <div class="row mb-3">
                    <div class="col-sm-4 fw-bold">Alive</div>
                    <div class="col-sm-2">
                        Male
                        <input
                            id="alive_adult_male"
                            v-model="animal_observation.alive_adult_male"
                            :disabled="isReadOnly"
                            type="number"
                            class="form-control animal-count-input"
                            placeholder=""
                            min="0"
                            step="1"
                        />
                    </div>
                    <div class="col-sm-2">
                        Male
                        <input
                            id="alive_juvenile_male"
                            v-model="animal_observation.alive_juvenile_male"
                            :disabled="isReadOnly"
                            type="number"
                            class="form-control animal-count-input"
                            placeholder=""
                            min="0"
                            step="1"
                        />
                    </div>
                    <div class="col-sm-2">
                        Male
                        <input
                            id="alive_unsure_male"
                            v-model="animal_observation.alive_unsure_male"
                            :disabled="isReadOnly"
                            type="number"
                            class="form-control animal-count-input"
                            placeholder=""
                            min="0"
                            step="1"
                        />
                    </div>
                </div>
                <div class="row mb-3">
                    <div class="col-sm-4 fw-bold"></div>
                    <div class="col-sm-2">
                        Female
                        <input
                            id="alive_adult_female"
                            v-model="animal_observation.alive_adult_female"
                            :disabled="isReadOnly"
                            type="number"
                            class="form-control animal-count-input"
                            placeholder=""
                            min="0"
                            step="1"
                        />
                    </div>
                    <div class="col-sm-2">
                        Female
                        <input
                            id="alive_juvenile_female"
                            v-model="animal_observation.alive_juvenile_female"
                            :disabled="isReadOnly"
                            type="number"
                            class="form-control animal-count-input"
                            placeholder=""
                            min="0"
                            step="1"
                        />
                    </div>
                    <div class="col-sm-2">
                        Female
                        <input
                            id="alive_unsure_female"
                            v-model="animal_observation.alive_unsure_female"
                            :disabled="isReadOnly"
                            type="number"
                            class="form-control animal-count-input"
                            placeholder=""
                            min="0"
                            step="1"
                        />
                    </div>
                </div>
                <div class="row mb-3">
                    <div class="col-sm-4 fw-bold"></div>
                    <div class="col-sm-2">
                        Sex Unknown
                        <input
                            id="alive_adult_unknown"
                            v-model="animal_observation.alive_adult_unknown"
                            :disabled="isReadOnly"
                            type="number"
                            class="form-control animal-count-input"
                            placeholder=""
                            min="0"
                            step="1"
                        />
                    </div>
                    <div class="col-sm-2">
                        Sex Unknown
                        <input
                            id="alive_juvenile_unknown"
                            v-model="animal_observation.alive_juvenile_unknown"
                            :disabled="isReadOnly"
                            type="number"
                            class="form-control animal-count-input"
                            placeholder=""
                            min="0"
                            step="1"
                        />
                    </div>
                    <div class="col-sm-2">
                        Sex Unknown
                        <input
                            id="alive_unsure_unknown"
                            v-model="animal_observation.alive_unsure_unknown"
                            :disabled="isReadOnly"
                            type="number"
                            class="form-control animal-count-input"
                            placeholder=""
                            min="0"
                            step="1"
                        />
                    </div>
                </div>

                <div class="row mb-3">
                    <div class="col-sm-4"></div>
                    <div class="col-sm-2 fw-bold">Adult</div>
                    <div class="col-sm-2 fw-bold">Juvenile</div>
                    <div class="col-sm-2 fw-bold">Age Unknown</div>
                </div>
                <div class="row mb-3">
                    <div class="col-sm-4 fw-bold">Dead</div>
                    <div class="col-sm-2">
                        Male
                        <input
                            id="dead_adult_male"
                            v-model="animal_observation.dead_adult_male"
                            :disabled="isReadOnly"
                            type="number"
                            class="form-control animal-count-input"
                            placeholder=""
                            min="0"
                            step="1"
                        />
                    </div>
                    <div class="col-sm-2">
                        Male
                        <input
                            id="dead_juvenile_male"
                            v-model="animal_observation.dead_juvenile_male"
                            :disabled="isReadOnly"
                            type="number"
                            class="form-control animal-count-input"
                            placeholder=""
                            min="0"
                            step="1"
                        />
                    </div>
                    <div class="col-sm-2">
                        Male
                        <input
                            id="dead_unsure_male"
                            v-model="animal_observation.dead_unsure_male"
                            :disabled="isReadOnly"
                            type="number"
                            class="form-control animal-count-input"
                            placeholder=""
                            min="0"
                            step="1"
                        />
                    </div>
                </div>
                <div class="row mb-3">
                    <div class="col-sm-4 fw-bold"></div>
                    <div class="col-sm-2">
                        Female
                        <input
                            id="dead_adult_female"
                            v-model="animal_observation.dead_adult_female"
                            :disabled="isReadOnly"
                            type="number"
                            class="form-control animal-count-input"
                            placeholder=""
                            min="0"
                            step="1"
                        />
                    </div>
                    <div class="col-sm-2">
                        Female
                        <input
                            id="dead_juvenile_female"
                            v-model="animal_observation.dead_juvenile_female"
                            :disabled="isReadOnly"
                            type="number"
                            class="form-control animal-count-input"
                            placeholder=""
                            min="0"
                            step="1"
                        />
                    </div>
                    <div class="col-sm-2">
                        Female
                        <input
                            id="dead_unsure_female"
                            v-model="animal_observation.dead_unsure_female"
                            :disabled="isReadOnly"
                            type="number"
                            class="form-control animal-count-input"
                            placeholder=""
                            min="0"
                            step="1"
                        />
                    </div>
                </div>
                <div class="row mb-4">
                    <div class="col-sm-4 fw-bold"></div>
                    <div class="col-sm-2">
                        Sex Unknown
                        <input
                            id="dead_adult_unknown"
                            v-model="animal_observation.dead_adult_unknown"
                            :disabled="isReadOnly"
                            type="number"
                            class="form-control animal-count-input"
                            placeholder=""
                            min="0"
                            step="1"
                        />
                    </div>
                    <div class="col-sm-2">
                        Sex Unknown
                        <input
                            id="dead_juvenile_unknown"
                            v-model="animal_observation.dead_juvenile_unknown"
                            :disabled="isReadOnly"
                            type="number"
                            class="form-control animal-count-input"
                            placeholder=""
                            min="0"
                            step="1"
                        />
                    </div>
                    <div class="col-sm-2">
                        Sex Unknown
                        <input
                            id="dead_unsure_unknown"
                            v-model="animal_observation.dead_unsure_unknown"
                            :disabled="isReadOnly"
                            type="number"
                            class="form-control animal-count-input"
                            placeholder=""
                            min="0"
                            step="1"
                        />
                    </div>
                </div>
            </div>
            <div
                id="animal-count-simple"
                v-if="animal_observation.count_status == 'simple_count'"
            >
                <label for="" class="col-lg-3 control-label fs-5 fw-bold mb-3"
                    >Animal Count - Simple</label
                >
                <div class="row mb-3">
                    <label for="" class="col-sm-3 control-label"
                        >Number alive:</label
                    >
                    <div class="col-sm-6">
                        <input
                            id="simple_alive"
                            v-model="animal_observation.simple_alive"
                            :disabled="isReadOnly"
                            type="number"
                            class="form-control animal-count-input"
                            placeholder=""
                            min="0"
                            step="1"
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
                            v-model="animal_observation.simple_dead"
                            :disabled="isReadOnly"
                            type="number"
                            class="form-control animal-count-input"
                            placeholder=""
                            min="0"
                            step="1"
                        />
                    </div>
                </div>
            </div>

            <div
                class="row mb-3"
                v-if="animal_observation.count_status != 'not_counted'"
            >
                <label for="" class="col-sm-4 control-label"
                    >Total Number Seen (alive and dead):</label
                >
                <div class="col-sm-6">
                    <input
                        id="quadrats_surveyed"
                        v-model="total_seen"
                        readonly
                        type="number"
                        class="form-control ocr_number"
                        placeholder=""
                        min="0"
                    />
                </div>
            </div>
            <div class="row mb-3">
                <div class="col-sm-12">
                    <span v-if="animal_observation.copied_ocr" class="float-end"
                        ><b
                            >Sourced from {{ animal_observation.copied_ocr }}</b
                        ></span
                    >
                </div>
            </div>
            <div class="row mb-3">
                <div class="col-sm-12">
                    <button
                        v-if="!updatingAnimalOnservationDetails"
                        class="btn btn-sm float-end"
                        :class="{
                            'btn-primary': animalObservationIsDirty,
                            'btn-light': !animalObservationIsDirty,
                            border: !animalObservationIsDirty,
                        }"
                        :disabled="isReadOnly || !animalObservationIsDirty"
                        @click.prevent="updateAnimalObservationDetails()"
                    >
                        <template v-if="animalObservationIsDirty"
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
        </form>
    </div>
</template>

<script>
import { api_endpoints, helpers } from '@/utils/hooks';
export default {
    props: {
        animal_observation: {
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
    emits: ['update-animal-observation', 'dirty'],
    data: function () {
        return {
            //----list of values dictionary
            primary_detection_method_list: [],
            secondary_sign_list: [],
            animal_behaviour_list: [],
            reproductive_state_list: [],
            death_injury_reason_list: [],
            animal_health_list: [],
            updatingAnimalOnservationDetails: false,
            total_seen: 0,
            listOfAnimalValuesDict: {},
            originalAnimalObservation: JSON.stringify(this.animal_observation),
        };
    },
    computed: {
        animalObservationIsDirty: function () {
            let vm = this;
            return (
                JSON.stringify(vm.animal_observation) !==
                vm.originalAnimalObservation
            );
        },
        isDirty: function () {
            return this.animalObservationIsDirty;
        },
        archivedSecondarySignNames: function () {
            const selected = this.animal_observation.secondary_sign_names || [];
            if (!selected.length) {
                return [];
            }

            const activeIds = new Set(
                (this.secondary_sign_list || [])
                    .filter(
                        (option) =>
                            option &&
                            !option.disabled &&
                            String(option.id).length > 0
                    )
                    .map((option) => String(option.id))
            );

            return selected
                .filter((item) => {
                    if (!item) {
                        return false;
                    }
                    const id = String(item.id);
                    return item.archived || !activeIds.has(id);
                })
                .map((item) => item.name)
                .filter((name) => !!name);
        },
    },
    watch: {
        animal_observation: function () {
            let vm = this;
            vm.normaliseMultiSelectFields();
            $(vm.$refs.primary_detection_select)
                .val(vm.animal_observation.primary_detection_method)
                .trigger('change.select2');
            if (vm.$refs.secondary_sign_select) {
                $(vm.$refs.secondary_sign_select)
                    .val(vm.animal_observation.secondary_sign)
                    .trigger('change.select2');
            }
            $(vm.$refs.reproductive_state_select)
                .val(vm.animal_observation.reproductive_state)
                .trigger('change.select2');
        },
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
            `/api/occurrence/animal_observation_list_of_values.json`
        );
        vm.listOfAnimalValuesDict = await response.json();
        vm.primary_detection_method_list =
            vm.listOfAnimalValuesDict.primary_detection_method_list;
        vm.primary_detection_method_list.splice(0, 0, {
            id: '',
            name: '',
            disabled: false,
        });
        vm.secondary_sign_list = vm.listOfAnimalValuesDict.secondary_sign_list;
        vm.secondary_sign_list.splice(0, 0, {
            id: '',
            name: '',
            disabled: false,
        });
        vm.animal_behaviour_list =
            vm.listOfAnimalValuesDict.animal_behaviour_list;
        vm.animal_behaviour_list.splice(0, 0, {
            id: '',
            name: '',
        });
        vm.reproductive_state_list =
            vm.listOfAnimalValuesDict.reproductive_state_list;
        vm.reproductive_state_list.splice(0, 0, {
            id: '',
            name: '',
        });
        vm.death_injury_reason_list =
            vm.listOfAnimalValuesDict.death_injury_reason_list;
        vm.death_injury_reason_list.splice(0, 0, {
            id: null,
            name: null,
        });
        vm.animal_health_list = vm.listOfAnimalValuesDict.animal_health_list;
        vm.animal_health_list.splice(0, 0, {
            id: null,
            name: null,
        });
        vm.normaliseMultiSelectFields();
    },
    mounted: function () {
        let vm = this;
        vm.initialisePrimaryDetectionSelect();
        vm.initialiseSecondarySignSelect();
        vm.initialiseReprodStateSelect();
        vm.calculateTotalNumberSeen();
    },
    methods: {
        normaliseMultiSelectFields: function () {
            const multiFields = [
                'primary_detection_method',
                'secondary_sign',
                'reproductive_state',
            ];
            multiFields.forEach((field) => {
                let value = this.animal_observation[field];
                if (value === null || value === undefined || value === '') {
                    this.animal_observation[field] = [];
                } else if (!Array.isArray(value)) {
                    this.animal_observation[field] = [value];
                }
            });
        },
        resetDirtyState: function () {
            this.originalAnimalObservation = JSON.stringify(
                this.animal_observation
            );
        },
        calculateTotalNumberSeen: function () {
            let vm = this;
            vm.total_seen = 0;
            $('input.animal-count-input').each(function (i, n) {
                if (!isNaN($(n).val()) && $(n).val() !== '') {
                    vm.total_seen += parseInt($(n).val(), 10);
                }
            });
            return vm.total_seen;
        },
        initialisePrimaryDetectionSelect: function () {
            let vm = this;
            // Initialise select2 for proposed Conservation Criteria
            $(vm.$refs.primary_detection_select)
                .select2({
                    theme: 'bootstrap-5',
                    allowClear: true,
                    multiple: true,
                    placeholder: 'Select Primary Detection Method',
                })
                .on('select2:select', function (e) {
                    var selected = $(e.currentTarget);
                    vm.animal_observation.primary_detection_method =
                        selected.val() || [];
                })
                .on('select2:unselect', function (e) {
                    var selected = $(e.currentTarget);
                    vm.animal_observation.primary_detection_method =
                        selected.val() || [];
                })
                .val(vm.animal_observation.primary_detection_method)
                .trigger('change.select2');
        },
        initialiseSecondarySignSelect: function () {
            let vm = this;
            // Initialise select2 for proposed Conservation Criteria
            $(vm.$refs.secondary_sign_select)
                .select2({
                    theme: 'bootstrap-5',
                    allowClear: true,
                    multiple: true,
                    placeholder: 'Select Secondary Signs',
                })
                .on('select2:select', function (e) {
                    var selected = $(e.currentTarget);
                    vm.animal_observation.secondary_sign = selected.val() || [];
                })
                .on('select2:unselect', function (e) {
                    var selected = $(e.currentTarget);
                    vm.animal_observation.secondary_sign = selected.val() || [];
                })
                .val(vm.animal_observation.secondary_sign)
                .trigger('change.select2');
        },
        initialiseReprodStateSelect: function () {
            let vm = this;
            // Initialise select2 for proposed Conservation Criteria
            $(vm.$refs.reproductive_state_select)
                .select2({
                    theme: 'bootstrap-5',
                    allowClear: true,
                    multiple: true,
                    placeholder: 'Select Reproductive State',
                })
                .on('select2:select', function (e) {
                    var selected = $(e.currentTarget);
                    vm.animal_observation.reproductive_state =
                        selected.val() || [];
                })
                .on('select2:unselect', function (e) {
                    var selected = $(e.currentTarget);
                    vm.animal_observation.reproductive_state =
                        selected.val() || [];
                })
                .val(vm.animal_observation.reproductive_state)
                .trigger('change.select2');
        },
        updateAnimalObservationDetails: function () {
            let vm = this;
            vm.updatingAnimalOnservationDetails = true;
            let endpoint = api_endpoints.occurrence;
            if (vm.is_report) {
                endpoint = api_endpoints.occurrence_report;
            }
            fetch(
                helpers.add_endpoint_json(
                    endpoint,
                    vm.occurrence_id + '/update_animal_observation_details'
                ),
                {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify(vm.animal_observation),
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
                        vm.$emit('update-animal-observation', data);
                        vm.originalAnimalObservation = JSON.stringify(data);
                        swal.fire({
                            title: 'Saved',
                            text: 'Animal Observation details have been saved',
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
                                'Animal Observation details cannot be saved because of the following error: ' +
                                text,
                            icon: 'error',
                            customClass: {
                                confirmButton: 'btn btn-primary',
                            },
                        });
                        vm.updatingAnimalOnservationDetails = false;
                    }
                )
                .finally(() => {
                    vm.updatingAnimalOnservationDetails = false;
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

input.animal_observation {
    width: 63%;
}

input.ocr_number {
    width: 20%;
}
</style>
