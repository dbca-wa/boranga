<template lang="html">
    <div id="community">
        <fieldset id="species-profile-fieldset" @change="saveSpeciesCommunity">
            <FormSection
                :form-collapse="false"
                label="Taxonomy"
                Index="taxonomy"
            >
                <div class="row mb-3">
                    <label
                        for=""
                        class="col-sm-3 control-label"
                        :class="isReadOnly ? '' : 'fw-bold'"
                        >Community Name:
                        <span v-if="!isReadOnly" class="text-danger"
                            >*</span
                        ></label
                    >
                    <div class="col-sm-9">
                        <textarea
                            id="community_name"
                            v-model="
                                species_community.taxonomy_details
                                    .community_name
                            "
                            :disabled="isReadOnly"
                            class="form-control"
                            rows="1"
                            placeholder=""
                        />
                    </div>
                </div>
                <div v-if="species_community.renamed_from" class="row mb-3">
                    <label for="renamed_from" class="col-sm-3 control-label"
                        >Renamed From:</label
                    >
                    <div class="col-sm-7">
                        <textarea
                            id="renamed_from"
                            :disabled="true"
                            class="form-control"
                            rows="1"
                            placeholder=""
                            :value="`${species_community.renamed_from.community_number} - ${species_community.renamed_from.community_name}`"
                        />
                    </div>
                    <div class="col-sm-2">
                        <a
                            class="btn btn-primary"
                            role="button"
                            target="_blank"
                            :href="`/internal/species-communities/${species_community.renamed_from.id}?group_type_name=${species_community.group_type}`"
                        >
                            <i class="bi bi-box-arrow-up-right"></i>
                            {{
                                species_community.renamed_from.community_number
                            }}
                        </a>
                    </div>
                </div>
                <template
                    v-if="
                        species_community.renamed_to &&
                        species_community.renamed_to.length > 0
                    "
                >
                    <div
                        v-for="(renamed, index) in species_community.renamed_to"
                        class="row mb-3"
                        :key="renamed.id"
                    >
                        <label
                            v-if="index === 0"
                            for="renamed_to_0"
                            class="col-sm-3 control-label"
                            >Renamed To:</label
                        >
                        <div v-else class="col-sm-3"></div>
                        <div class="col-sm-7">
                            <textarea
                                :id="`renamed_to_${index}`"
                                :disabled="true"
                                class="form-control"
                                rows="1"
                                placeholder=""
                                :value="`${renamed.community_number} - ${renamed.community_name}`"
                            />
                        </div>
                        <div class="col-sm-2">
                            <a
                                class="btn btn-primary"
                                role="button"
                                target="_blank"
                                :href="`/internal/species-communities/${renamed.id}?group_type_name=${species_community.group_type}`"
                            >
                                <i class="bi bi-box-arrow-up-right"></i>
                                {{ renamed.community_number }}
                            </a>
                        </div>
                    </div>
                </template>
                <div class="row mb-3">
                    <label
                        for=""
                        class="col-sm-3 control-label"
                        :class="isReadOnly ? '' : 'fw-bold'"
                        >Community ID:
                        <span v-if="!isReadOnly" class="text-danger"
                            >*</span
                        ></label
                    >
                    <div class="col-sm-9">
                        <input
                            id="community_common_id"
                            v-model="
                                species_community.taxonomy_details
                                    .community_common_id
                            "
                            :disabled="isReadOnly"
                            type="text"
                            class="form-control"
                            placeholder=""
                        />
                    </div>
                </div>
                <div
                    v-if="
                        showField(
                            species_community.taxonomy_details
                                .community_description
                        )
                    "
                    class="row mb-3"
                >
                    <label for="" class="col-sm-3 control-label"
                        >Community Description:</label
                    >
                    <div class="col-sm-9">
                        <textarea
                            id="community_description"
                            v-model="
                                species_community.taxonomy_details
                                    .community_description
                            "
                            :disabled="isReadOnly"
                            class="form-control"
                            rows="2"
                            placeholder=""
                        />
                    </div>
                </div>
                <div
                    v-if="
                        showField(
                            species_community.taxonomy_details.previous_name
                        )
                    "
                    class="row mb-3"
                >
                    <label for="" class="col-sm-3 control-label"
                        >Previous Name:</label
                    >
                    <div class="col-sm-9">
                        <textarea
                            id="community_previous_name"
                            v-model="
                                species_community.taxonomy_details.previous_name
                            "
                            :disabled="isReadOnly"
                            class="form-control"
                            placeholder=""
                        />
                    </div>
                </div>
                <div
                    v-if="
                        showField(
                            species_community.taxonomy_details.name_authority
                        )
                    "
                    class="row mb-3"
                >
                    <label for="" class="col-sm-3 control-label"
                        >Name Authority:</label
                    >
                    <div class="col-sm-9">
                        <textarea
                            id="name_authority"
                            v-model="
                                species_community.taxonomy_details
                                    .name_authority
                            "
                            :disabled="isReadOnly"
                            rows="1"
                            class="form-control"
                            placeholder=""
                        />
                    </div>
                </div>
                <div
                    v-if="
                        showField(
                            species_community.taxonomy_details.name_comments
                        )
                    "
                    class="row mb-3"
                >
                    <label for="" class="col-sm-3 control-label"
                        >Name Comments:</label
                    >
                    <div class="col-sm-9">
                        <textarea
                            id="community_comment"
                            v-model="
                                species_community.taxonomy_details.name_comments
                            "
                            :disabled="isReadOnly"
                            class="form-control"
                            placeholder=""
                        />
                    </div>
                </div>
                <div v-if="species_community.migrated_from_id" class="row mb-3">
                    <label
                        for="migrated-from-id"
                        class="col-sm-3 col-form-label"
                        >Migrated From ID:</label
                    >
                    <div class="col-sm-9">
                        <input
                            id="migrated-from-id"
                            v-model="species_community.migrated_from_id"
                            :disabled="true"
                            type="text"
                            class="form-control"
                            placeholder=""
                        />
                    </div>
                </div>
            </FormSection>
            <FormSection
                v-if="species_community.distribution"
                :form-collapse="false"
                label="Distribution"
                Index="distribution"
            >
                <div class="row mb-3">
                    <label
                        for=""
                        class="col-sm-3 control-label"
                        :class="isReadOnly ? '' : 'fw-bold'"
                        >Distribution:
                        <span v-if="!isReadOnly" class="text-danger"
                            >*</span
                        ></label
                    >
                    <div class="col-sm-9">
                        <textarea
                            id="distribution"
                            v-model="
                                species_community.distribution.distribution
                            "
                            :disabled="isReadOnly"
                            class="form-control"
                            rows="1"
                            placeholder=""
                        />
                    </div>
                </div>
                <div class="row mb-3">
                    <label
                        for=""
                        class="col-sm-3 control-label"
                        :class="isReadOnly ? '' : 'fw-bold'"
                        >Region:
                        <span v-if="!isReadOnly" class="text-danger"
                            >*</span
                        ></label
                    >
                    <div class="col-sm-9">
                        <select
                            ref="regions_select"
                            v-model="species_community.regions"
                            :disabled="isReadOnly"
                            class="form-select"
                        >
                            <option value="" selected disabled>
                                Select region
                            </option>
                            <option
                                v-for="option in region_list"
                                :key="option.value"
                                :value="option.value"
                            >
                                {{ option.text }}
                            </option>
                        </select>
                    </div>
                </div>
                <div v-if="species_community.regions" class="row mb-3">
                    <label
                        for=""
                        class="col-sm-3 col-form-label"
                        :class="isReadOnly ? '' : 'fw-bold'"
                        >District:
                        <span v-if="!isReadOnly" class="text-danger"
                            >*</span
                        ></label
                    >
                    <div class="col-sm-9">
                        <select
                            ref="districts_select"
                            v-model="species_community.districts"
                            :disabled="isReadOnly"
                            class="form-select"
                        >
                            <option value="" selected disabled>
                                Select district
                            </option>
                            <option
                                v-for="option in district_list"
                                :key="option.value"
                                :value="option.value"
                                :disabled="
                                    option.archive_date &&
                                    new Date(option.archive_date) <= new Date()
                                "
                            >
                                {{ option.text }}
                                <template
                                    v-if="
                                        option.archive_date &&
                                        new Date(option.archive_date) <=
                                            new Date()
                                    "
                                >
                                    (Archived)</template
                                >
                            </option>
                        </select>
                    </div>
                </div>
                <template v-if="show_calculated_distribution_fields">
                    <div class="row mb-3 border-top pt-3">
                        <label for="" class="col-sm-5 col-form-label"
                            >Number of Occurrences:</label
                        >
                        <div class="col-sm-4">
                            <input
                                v-if="species_community.distribution.noo_auto"
                                id="no_of_occurrences"
                                v-model="species_community.occurrence_count"
                                :disabled="isNOOReadOnly"
                                type="number"
                                class="form-control"
                                placeholder=""
                            />
                            <input
                                v-else
                                id="no_of_occurrences"
                                ref="number_of_occurrences"
                                v-model="
                                    species_community.distribution
                                        .number_of_occurrences
                                "
                                :disabled="isNOOReadOnly"
                                type="number"
                                class="form-control"
                                placeholder=""
                            />
                        </div>
                        <div
                            v-if="!isReadOnly"
                            class="col-sm-3 d-flex align-items-center"
                        >
                            <div class="form-check form-check-inline">
                                <input
                                    id="noo_auto"
                                    v-model="
                                        species_community.distribution.noo_auto
                                    "
                                    :disabled="isReadOnly"
                                    type="radio"
                                    :value="true"
                                    class="form-check-input"
                                    @click="switchNOO('true')"
                                />
                                <label class="form-check-label">auto</label>
                            </div>
                            <div class="form-check form-check-inline">
                                <input
                                    id="noo_manual"
                                    v-model="
                                        species_community.distribution.noo_auto
                                    "
                                    :disabled="isReadOnly"
                                    type="radio"
                                    :value="false"
                                    class="form-check-input"
                                    @click="switchNOO('false')"
                                />
                                <label class="form-check-label">manual</label>
                            </div>
                        </div>
                    </div>
                    <div class="row mb-3">
                        <label for="" class="col-sm-5 col-form-label"
                            >Extent of Occurrences:
                            <HelpText
                                v-if="species_community.distribution.eoo_auto"
                                section_id="communities_extent_of_occurrences"
                            />
                        </label>
                        <div class="col-sm-4">
                            <div class="input-group">
                                <input
                                    v-if="
                                        species_community.distribution.eoo_auto
                                    "
                                    id="extent_of_occurrence"
                                    v-model="
                                        species_community.area_occurrence_convex_hull_km2
                                    "
                                    :disabled="isEOOReadOnly"
                                    type="number"
                                    class="form-control"
                                    placeholder=""
                                />
                                <input
                                    v-else
                                    id="extent_of_occurrence"
                                    ref="extent_of_occurrence"
                                    v-model="
                                        species_community.distribution
                                            .extent_of_occurrences
                                    "
                                    :disabled="isEOOReadOnly"
                                    type="number"
                                    min="0.00"
                                    step="0.00001"
                                    max="9999999999.99"
                                    class="form-control"
                                    placeholder=""
                                />
                                <span
                                    id="area_of_occupancy_km2-addon"
                                    class="input-group-text"
                                    >km&#xb2;</span
                                >
                            </div>
                        </div>
                        <div
                            v-if="!isReadOnly"
                            class="col-sm-3 d-flex align-items-center"
                        >
                            <div class="form-check form-check-inline">
                                <input
                                    id="eoo_auto"
                                    v-model="
                                        species_community.distribution.eoo_auto
                                    "
                                    :disabled="isReadOnly"
                                    type="radio"
                                    :value="true"
                                    class="form-check-input"
                                    @click="switchEOO('true')"
                                />
                                <label class="form-check-label">auto</label>
                            </div>
                            <div class="form-check form-check-inline">
                                <input
                                    id="eoo_manual"
                                    v-model="
                                        species_community.distribution.eoo_auto
                                    "
                                    :disabled="isReadOnly"
                                    type="radio"
                                    :value="false"
                                    class="form-check-input"
                                    @click="switchEOO('false')"
                                />
                                <label class="form-check-label">manual</label>
                            </div>
                        </div>
                    </div>
                    <div class="row mb-3">
                        <label for="" class="col-sm-5 col-form-label"
                            >Area of Occupancy:
                            <div
                                id="area-of-occupancy-help-text"
                                class="form-text"
                            >
                                Based on IUCN 10km x 10km grid method
                            </div></label
                        >
                        <div class="col-sm-4">
                            <div class="input-group">
                                <input
                                    id="area_of_occupany"
                                    v-model="
                                        species_community.distribution
                                            .area_of_occupancy
                                    "
                                    :disabled="isReadOnly"
                                    type="number"
                                    class="form-control"
                                    placeholder=""
                                />
                                <span
                                    id="area_of_occupancy_km2-addon"
                                    class="input-group-text"
                                    >km&#xb2;</span
                                >
                            </div>
                        </div>
                    </div>
                    <div class="row mb-3">
                        <label for="" class="col-sm-5 col-form-label"
                            >Actual Area of Occupancy:
                            <HelpText
                                v-if="
                                    species_community.distribution
                                        .aoo_actual_auto
                                "
                                section_id="communities_actual_area_of_occupancy"
                            />
                        </label>
                        <div class="col-sm-4">
                            <div class="input-group">
                                <input
                                    v-if="
                                        species_community.distribution
                                            .aoo_actual_auto
                                    "
                                    id="area_of_occupancy_actual"
                                    v-model="
                                        species_community.area_of_occupancy_km2
                                    "
                                    :disabled="isAOOActualReadOnly"
                                    type="number"
                                    step="any"
                                    class="form-control"
                                    placeholder=""
                                    area-describedby=""
                                />
                                <input
                                    v-else
                                    id="area_of_occupancy_actual"
                                    ref="area_of_occupancy_actual"
                                    v-model="
                                        species_community.distribution
                                            .area_of_occupancy_actual
                                    "
                                    :disabled="isAOOActualReadOnly"
                                    type="number"
                                    step="any"
                                    class="form-control"
                                    placeholder=""
                                />
                                <span
                                    id="area_of_occupancy_km2-addon"
                                    class="input-group-text"
                                    >km&#xb2;</span
                                >
                            </div>
                        </div>
                        <div
                            v-if="!isReadOnly"
                            class="col-sm-3 d-flex align-items-center"
                        >
                            <div class="form-check form-check-inline">
                                <input
                                    id="aoo_actual_auto"
                                    v-model="
                                        species_community.distribution
                                            .aoo_actual_auto
                                    "
                                    :disabled="isReadOnly"
                                    type="radio"
                                    :value="true"
                                    class="form-check-input"
                                    @click="switchAOOActual('true')"
                                />
                                <label class="form-check-label">auto</label>
                            </div>
                            <div class="form-check form-check-inline">
                                <input
                                    id="aoo_actual_manual"
                                    v-model="
                                        species_community.distribution
                                            .aoo_actual_auto
                                    "
                                    :disabled="isReadOnly"
                                    type="radio"
                                    :value="false"
                                    class="form-check-input"
                                    @click="switchAOOActual('false')"
                                />
                                <label class="form-check-label">manual</label>
                            </div>
                        </div>
                    </div>
                </template>
                <div
                    v-if="
                        showField(
                            species_community.distribution
                                .number_of_iucn_locations
                        )
                    "
                    class="row mb-3"
                >
                    <label for="" class="col-sm-5 control-label"
                        >Number of IUCN Locations:</label
                    >
                    <div class="col-sm-4">
                        <input
                            id="no_of_iucn_locations"
                            v-model="
                                species_community.distribution
                                    .number_of_iucn_locations
                            "
                            :disabled="isReadOnly"
                            type="number"
                            class="form-control"
                            placeholder=""
                        />
                    </div>
                </div>
                <div
                    v-if="
                        showField(
                            species_community.distribution
                                .community_original_area
                        )
                    "
                    class="row mb-3"
                >
                    <label for="" class="col-sm-5 control-label"
                        >Community Original Area:</label
                    >
                    <div class="col-sm-4">
                        <div class="input-group">
                            <input
                                id="community_original_area"
                                v-model="
                                    species_community.distribution
                                        .community_original_area
                                "
                                :disabled="isReadOnly"
                                type="number"
                                step="0.01"
                                min="0"
                                max="9999999999.99"
                                class="form-control"
                                placeholder=""
                            />
                            <span
                                id="community_original_area-addon"
                                class="input-group-text"
                                >ha</span
                            >
                        </div>
                    </div>
                </div>
                <div
                    v-if="
                        showField(
                            species_community.distribution
                                .community_original_area_accuracy
                        )
                    "
                    class="row mb-3"
                >
                    <label for="" class="col-sm-5 control-label"
                        >Community Original Area (+/- ha) Accuracy:</label
                    >
                    <div class="col-sm-4">
                        <input
                            id="community_original_area_accuracy"
                            v-model="
                                species_community.distribution
                                    .community_original_area_accuracy
                            "
                            :disabled="isReadOnly"
                            type="number"
                            class="form-control"
                            placeholder=""
                        />
                    </div>
                </div>
                <div
                    v-if="
                        showField(
                            species_community.distribution
                                .community_original_area_reference
                        )
                    "
                    class="row mb-3"
                >
                    <label for="" class="col-sm-5 control-label"
                        >Community Original Area (ha) Reference:</label
                    >
                    <div class="col-sm-4">
                        <input
                            id="community_original_area_reference"
                            v-model="
                                species_community.distribution
                                    .community_original_area_reference
                            "
                            :disabled="isReadOnly"
                            type="text"
                            class="form-control"
                            placeholder=""
                        />
                    </div>
                </div>
            </FormSection>
            <BasicConservationStatus
                :conservation_status="species_community.conservation_status"
                :is_internal="is_internal"
                :is_external="is_external"
                :is-conservation-status-public="conservation_status_public"
            />
            <!-- <FormSection v-if="conservation_attributes_public || is_internal" :formCollapse="false"
            label="Conservation Attributes" Index="conservation_attributes">
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Pollinator Information:</label>
                <div class="col-sm-9">
                    <textarea :disabled="isReadOnly" type="text" class="form-control" id="pollinator_info"
                        placeholder="" v-model="species_community.conservation_attributes.pollinator_information" />
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Minimum Fire Interval:</label>
                <div class="col-sm-3">
                    <div class="form-check form-check-inline">
                        <label for="" class="control-label">Range</label>
                        <input class="form-check-input" type="checkbox" name="minimum_fire_interval_range"
                            id="minimum_fire_interval_range" v-model="minimum_fire_interval_range"
                            :disabled="isReadOnly" @change="handleMinimumFireIntervalRange()" />
                    </div>
                </div>
                <label for="" class="col-sm-6 control-label" style="color: red;">{{ errors.minimum_fire_interval_error
                    }}</label>
            </div>
            <div class="row mb-3" v-if="!minimum_fire_interval_range">
                <label for="" class="col-sm-3 control-label"></label>
                <div class="col-sm-3">
                    <div class="input-group">
                        <input :disabled="isReadOnly" type="number" class="form-control" id="minimum_fire_interval_from"
                            placeholder="" @change="validateMinimumFireIntervalRange()"
                            v-model="species_community.conservation_attributes.minimum_fire_interval_from" />
                        <select :disabled="isReadOnly" class="form-select" @change="validateMinimumFireIntervalRange()"
                            v-model="species_community.conservation_attributes.minimum_fire_interval_choice">
                            <option v-for="option in interval_choice" :value="option.id" v-bind:key="option.id">
                                {{ option.name }}
                            </option>
                        </select>
                    </div>
                </div>
                <div class="col-sm-4">
                    <label for="" class="control-label">{{ minFireIntervalMonthsComputed }}</label>
                </div>
            </div>
            <div class="row mb-3" v-else>
                <div class="col-sm-3"></div>
                <div class="col-sm-6">
                    <div class="input-group">
                        <label for="" class="input-group-text">From</label>
                        <input :disabled="isReadOnly" type="number" class="form-control" id="minimum_fire_interval_from"
                            placeholder="" @change="validateMinimumFireIntervalRange()"
                            v-model="species_community.conservation_attributes.minimum_fire_interval_from" />
                        <label for="" class="input-group-text">to</label>
                        <input :disabled="isReadOnly" type="number" class="form-control" id="minimum_fire_interval_to"
                            placeholder="" @change="validateMinimumFireIntervalRange()"
                            v-model="species_community.conservation_attributes.minimum_fire_interval_to" />
                        <select :disabled="isReadOnly" class="form-select" @change="validateMinimumFireIntervalRange()"
                            v-model="species_community.conservation_attributes.minimum_fire_interval_choice">
                            <option v-for="option in interval_choice" :value="option.id" v-bind:key="option.id">
                                {{ option.name }}
                            </option>
                        </select>
                    </div>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Response to Fire:</label>
                <div class="col-sm-9">
                    <textarea :disabled="isReadOnly" type="text" class="form-control" id="response_to_fire"
                        placeholder="" v-model="species_community.conservation_attributes.response_to_fire" />
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Post Fire Habitat Interactions:</label>
                <div class="col-sm-9">
                    <select :disabled="isReadOnly" class="form-select"
                        v-model="species_community.conservation_attributes.post_fire_habitat_interaction_id">
                        <option v-for="option in post_fire_habitatat_interactions_list" :value="option.id"
                            v-bind:key="option.id">
                            {{ option.name }}
                        </option>
                    </select>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Hydrology:</label>
                <div class="col-sm-9">
                    <textarea :disabled="isReadOnly" type="text" class="form-control" id="hydrology" placeholder=""
                        v-model="species_community.conservation_attributes.hydrology" />
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Ecological Community and Biological Information:
                </label>
                <div class="col-sm-9">
                    <textarea :disabled="isReadOnly" type="text" class="form-control"
                        id="ecological_biological_information" placeholder=""
                        v-model="species_community.conservation_attributes.ecological_and_biological_information" />
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Research Requirements:</label>
                <div class="col-sm-9">
                    <textarea :disabled="isReadOnly" type="text" class="form-control" id="research_requirements"
                        placeholder="" v-model="species_community.conservation_attributes.research_requirements" />
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Response to Dieback:</label>
                <div class="col-sm-9">
                    <textarea :disabled="isReadOnly" type="text" class="form-control" id="response_to_dieback"
                        placeholder="" v-model="species_community.conservation_attributes.response_to_dieback" />
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Other relevant diseases:</label>
                <div class="col-sm-9">
                    <textarea :disabled="isReadOnly" type="text" class="form-control" id="other_relevant_diseases"
                        placeholder="" v-model="species_community.conservation_attributes.other_relevant_diseases" />
                </div>
            </div>
        </FormSection> -->
            <FormSection
                v-if="is_internal"
                :form-collapse="false"
                label="General"
                Index="general"
            >
                <div class="row mb-3">
                    <label for="" class="col-sm-3 control-label"
                        >Department File Numbers:</label
                    >
                    <div class="col-sm-9">
                        <input
                            id="department_file_numbers"
                            v-model="species_community.department_file_numbers"
                            :disabled="isReadOnly"
                            type="text"
                            class="form-control"
                            placeholder=""
                        />
                    </div>
                </div>
                <div class="row mb-3">
                    <label
                        for="last_data_curation_date"
                        class="col-sm-3 control-label"
                        >Last data curation date:
                    </label>
                    <div class="col-sm-9">
                        <input
                            ref="last_data_curation_date"
                            v-model="species_community.last_data_curation_date"
                            :disabled="isReadOnly"
                            type="date"
                            class="form-control"
                            name="last_data_curation_date"
                            min="1990-01-01"
                            :max="new Date().toISOString().split('T')[0]"
                            @blur="checkDate()"
                        />
                    </div>
                </div>
                <div class="row mb-3">
                    <label
                        for="conservation_plan_exists"
                        class="col-sm-3 control-label"
                        >Conservation Plan Exists:
                    </label>
                    <div class="col-sm-9">
                        <label for="conservation_plan_exists" class="me-2"
                            >No</label
                        >
                        <input
                            id="conservation_plan_exists"
                            v-model="species_community.conservation_plan_exists"
                            :disabled="isReadOnly"
                            type="radio"
                            :value="false"
                            class="form-check-input me-2"
                        />
                        <label for="conservation_plan_exists" class="me-2"
                            >Yes</label
                        >
                        <input
                            id="conservation_plan_exists"
                            v-model="species_community.conservation_plan_exists"
                            :disabled="isReadOnly"
                            type="radio"
                            :value="true"
                            class="form-check-input"
                        />
                    </div>
                </div>
                <div
                    v-if="species_community.conservation_plan_exists"
                    class="row mb-3"
                >
                    <label
                        for="conservation_plan_reference"
                        class="col-sm-3 control-label"
                        >Conservation Plan Reference:
                    </label>
                    <div class="col-sm-9">
                        <input
                            ref="conservation_plan_reference"
                            v-model="
                                species_community.conservation_plan_reference
                            "
                            :disabled="isReadOnly"
                            type="text"
                            class="form-control"
                            name="conservation_plan_reference"
                        />
                    </div>
                </div>
                <div class="row mb-3">
                    <label for="" class="col-sm-3 control-label"
                        >Comment:</label
                    >
                    <div class="col-sm-9">
                        <textarea
                            id="comment"
                            v-model="species_community.comment"
                            :disabled="isReadOnly"
                            class="form-control"
                            rows="3"
                            placeholder=""
                        />
                    </div>
                </div>
            </FormSection>
            <FormSection
                v-if="is_internal"
                :form-collapse="false"
                label="Publishing"
                Index="publishing"
            >
                <div class="row mb-3">
                    <label
                        for="distribution_publishing"
                        class="col-sm-3 control-label"
                        >Distribution:
                    </label>
                    <div class="col-sm-9">
                        <label for="distribution_publishing" class="me-2"
                            >Private</label
                        >
                        <input
                            id="distribution_publishing"
                            v-model="
                                species_community.publishing_status
                                    .distribution_public
                            "
                            :disabled="isReadOnly || !isPublic || !isActive"
                            type="radio"
                            :value="false"
                            class="form-check-input me-2"
                        />
                        <label for="distribution_publishing" class="me-2"
                            >Public</label
                        >
                        <input
                            id="distribution_publishing"
                            v-model="
                                species_community.publishing_status
                                    .distribution_public
                            "
                            :disabled="isReadOnly || !isPublic || !isActive"
                            type="radio"
                            :value="true"
                            class="form-check-input"
                        />
                    </div>
                </div>
                <div class="row mb-3">
                    <label
                        for="conservation_status_publishing"
                        class="col-sm-3 control-label"
                        >Conservation Status:
                    </label>
                    <div class="col-sm-9">
                        <label for="conservation_status_publishing" class="me-2"
                            >Private</label
                        >
                        <input
                            id="conservation_status_publishing"
                            v-model="
                                species_community.publishing_status
                                    .conservation_status_public
                            "
                            :disabled="isReadOnly || !isPublic || !isActive"
                            type="radio"
                            :value="false"
                            class="form-check-input me-2"
                        />
                        <label for="conservation_status_publishing" class="me-2"
                            >Public</label
                        >
                        <input
                            id="conservation_status_publishing"
                            v-model="
                                species_community.publishing_status
                                    .conservation_status_public
                            "
                            :disabled="isReadOnly || !isPublic || !isActive"
                            type="radio"
                            :value="true"
                            class="form-check-input"
                        />
                    </div>
                </div>
                <!-- <div class="row mb-3">
                <label for="conservation_attributes_publishing" class="col-sm-3 control-label">Conservation Attributes:
                </label>
                <div class="col-sm-9">
                    <label for="conservation_attributes_publishing" class="me-2">Private</label>
                    <input :disabled="isReadOnly || !isPublic || !isActive" type="radio" :value="false"
                        class="form-check-input me-2" id="conservation_attributes_publishing"
                        v-model="species_community.publishing_status.conservation_attributes_public">
                    <label for="conservation_attributes_publishing" class="me-2">Public</label>
                    <input :disabled="isReadOnly || !isPublic || !isActive" type="radio" :value="true"
                        class="form-check-input" id="conservation_attributes_publishing"
                        v-model="species_community.publishing_status.conservation_attributes_public">
                </div>
            </div> -->
                <div class="row mb-3">
                    <label
                        for="threats_publishing"
                        class="col-sm-3 control-label"
                        >Threats:
                    </label>
                    <div class="col-sm-9">
                        <label for="threats_publishing" class="me-2"
                            >Private</label
                        >
                        <input
                            id="threats_publishing"
                            v-model="
                                species_community.publishing_status
                                    .threats_public
                            "
                            :disabled="isReadOnly || !isPublic || !isActive"
                            type="radio"
                            :value="false"
                            class="form-check-input me-2"
                        />
                        <label for="threats_publishing" class="me-2"
                            >Public</label
                        >
                        <input
                            id="threats_publishing"
                            v-model="
                                species_community.publishing_status
                                    .threats_public
                            "
                            :disabled="isReadOnly || !isPublic || !isActive"
                            type="radio"
                            :value="true"
                            class="form-check-input"
                        />
                    </div>
                </div>
                <div class="row mb-3">
                    <div class="col-sm-12">
                        <button
                            v-if="!updatingPublishing"
                            :disabled="isReadOnly || !isPublic || !isActive"
                            class="btn btn-primary btn-sm float-end"
                            @click.prevent="updatePublishingDetails()"
                        >
                            Update
                        </button>
                        <button
                            v-else
                            disabled
                            class="float-end btn btn-primary"
                        >
                            Updating
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
        </fieldset>
    </div>
</template>

<script>
import { v4 as uuid } from 'uuid';
import FormSection from '@/components/forms/section_toggle.vue';
import BasicConservationStatus from './basic_conservation_status.vue';
import HelpText from '@/components/common/help_text.vue';

import { api_endpoints, helpers } from '@/utils/hooks';
export default {
    name: 'CommunityProfile',
    components: {
        FormSection,
        BasicConservationStatus,
        HelpText,
    },
    props: {
        species_community: {
            type: Object,
            required: true,
        },
        species_community_original: {
            type: Object,
            required: true,
        },
        is_internal: {
            type: Boolean,
            default: false,
        },
        is_external: {
            type: Boolean,
            default: false,
        },
        is_readonly: {
            type: Boolean,
            default: false,
        },
    },
    data: function () {
        return {
            updatingPublishing: false,
            conservationStatusBody: 'conservationStatusBody' + uuid(),
            species_list: [],
            community_profile_dict: {},
            post_fire_habitatat_interactions_list: [],
            region_list: [],
            district_list: [],
            filtered_district_list: [],
            minimum_fire_interval_range: false,
            interval_choice: [
                { id: 1, name: 'year/s' },
                { id: 2, name: 'month/s' },
            ],
            errors: {
                minimum_fire_interval_error: null,
            },
        };
    },
    computed: {
        distribution_public: function () {
            return (
                this.isPublic &&
                this.species_community.publishing_status.distribution_public
            );
        },
        conservation_status_public: function () {
            return (
                this.isPublic &&
                this.species_community.publishing_status
                    .conservation_status_public
            );
        },
        conservation_attributes_public: function () {
            return (
                this.isPublic &&
                this.species_community.publishing_status
                    .conservation_attributes_public
            );
        },
        isReadOnly: function () {
            if (this.is_readonly) {
                return true;
            }
            let action = this.$route.query.action;
            if (action === 'view') {
                return true;
            } else if (
                action === 'edit' &&
                this.species_community &&
                this.species_community.user_edit_mode
            ) {
                return false;
            } else {
                return this.species_community.readonly;
            }
        },
        isActive: function () {
            return this.species_community.processing_status === 'Active'
                ? true
                : false;
        },
        isPublic: function () {
            return this.isActive &&
                this.species_community.publishing_status.community_public
                ? true
                : false;
        },
        isNOOReadOnly: function () {
            let vm = this;
            if (vm.species_community.distribution.noo_auto === true) {
                return true;
            } else {
                return vm.isReadOnly;
            }
        },
        isEOOReadOnly: function () {
            let vm = this;
            if (vm.species_community.distribution.eoo_auto === true) {
                return true;
            } else {
                return vm.isReadOnly;
            }
        },
        isAOOActualReadOnly: function () {
            let vm = this;
            if (vm.species_community.distribution.aoo_actual_auto === true) {
                return true;
            } else {
                return vm.isReadOnly;
            }
        },
        minFireIntervalMonthsComputed: function () {
            const totalMonths = parseInt(
                this.species_community.conservation_attributes
                    .minimum_fire_interval_from
            );
            const intervalChoice =
                this.species_community.conservation_attributes
                    .minimum_fire_interval_choice;

            if (totalMonths > 12 && intervalChoice == 2) {
                const years = Math.floor(totalMonths / 12);
                const months = totalMonths % 12;
                return years + ' year/s ' + months + ' month/s';
            } else {
                return '';
            }
        },
        show_calculated_distribution_fields: function () {
            return (
                this.is_internal ||
                (this.species_community.distribution.noo_auto &&
                    this.species_community.occurrence_count > 0) ||
                (!this.species_community.distribution.noo_auto &&
                    this.species_community.distribution.number_of_occurrences >
                        0)
            );
        },
    },
    watch: {
        'species_community.distribution.number_of_iucn_locations': function (
            newVal
        ) {
            let vm = this;
            if (newVal == '') {
                vm.species_community.distribution.number_of_iucn_locations =
                    null;
            }
        },
        'species_community.distribution.community_original_area_accuracy':
            function (newVal) {
                let vm = this;
                if (newVal == '') {
                    vm.species_community.distribution.community_original_area_accuracy =
                        null;
                }
            },
        'species_community.distribution.community_original_area_reference':
            function (newVal) {
                let vm = this;
                if (newVal == '') {
                    vm.species_community.distribution.community_original_area_reference =
                        null;
                }
            },
    },
    created: async function () {
        let vm = this;
        //----set the distribution field values if auto onload
        if (vm.species_community.distribution) {
            if (vm.species_community.distribution.noo_auto == true) {
                vm.species_community.distribution.number_of_occurrences =
                    vm.species_community.distribution.cal_number_of_occurrences;
            }
            if (vm.species_community.distribution.eoo_auto == true) {
                vm.species_community.distribution.extent_of_occurrences =
                    vm.species_community.distribution.cal_extent_of_occurrences;
            }
            if (vm.species_community.distribution.aoo_actual_auto == true) {
                vm.species_community.distribution.area_of_occupancy_actual =
                    vm.species_community.distribution.cal_area_of_occupancy_actual;
            }
        }
        if (vm.species_community.conservation_attributes) {
            if (
                vm.species_community.conservation_attributes
                    .minimum_fire_interval_to != null &&
                vm.species_community.conservation_attributes
                    .minimum_fire_interval_to != '' &&
                vm.species_community.conservation_attributes
                    .minimum_fire_interval_to != undefined
            ) {
                vm.minimum_fire_interval_range = true;
            }
        }
        //------fetch list of values
        const response = await fetch('/api/community_profile_dict/');
        vm.community_profile_dict = await response.json();
        vm.post_fire_habitatat_interactions_list =
            vm.community_profile_dict.post_fire_habitatat_interactions_list;
        vm.post_fire_habitatat_interactions_list.splice(0, 0, {
            id: null,
            name: null,
        });
        vm.fetchRegions();
    },
    mounted: function () {
        let vm = this;
        vm.initialiseRegionSelect();
        vm.initialiseDistrictSelect();
    },
    methods: {
        showField(fieldValue) {
            if (!this.isReadOnly) {
                return true;
            }
            return this.isReadOnly && fieldValue;
        },
        updatePublishing(data) {
            let vm = this;
            fetch(
                helpers.add_endpoint_json(
                    api_endpoints.community,
                    vm.species_community.id + '/update_publishing_status'
                ),
                {
                    method: 'PATCH',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: data,
                }
            ).then(
                async (response) => {
                    vm.updatingPublishing = false;
                    vm.species_community.publishing_status =
                        await response.json();
                    vm.species_community_original.publishing_status =
                        helpers.copyObject(
                            vm.species_community.publishing_status
                        );
                    swal.fire({
                        title: 'Saved',
                        text: 'Publishing settings have been updated',
                        icon: 'success',
                        showConfirmButton: false,
                        timer: 1200,
                    });
                },
                (error) => {
                    var text = helpers.apiVueResourceError(error);
                    swal.fire({
                        title: 'Error',
                        text:
                            'Publishing settings cannot be updated because of the following error: ' +
                            text,
                        icon: 'error',
                        customClass: {
                            confirmButton: 'btn btn-primary',
                        },
                    });
                    vm.updatingPublishing = false;
                }
            );
        },
        updatePublishingDetails: function () {
            let vm = this;
            vm.updatingPublishing = true;
            //if not already public, we make it public (notify user first)
            //but only if it is active
            if (vm.isPublic && vm.isActive) {
                //send just publishing form data
                let data = JSON.stringify(
                    vm.species_community.publishing_status
                );
                vm.updatePublishing(data);
            } else {
                swal.fire({
                    title: 'Error',
                    text: 'Record not active and cannot be made public',
                    icon: 'error',
                    customClass: {
                        confirmButton: 'btn btn-primary',
                    },
                });
                vm.updatingPublishing = false;
            }
        },
        filterDistrict: function (event) {
            this.$nextTick(() => {
                if (event) {
                    this.species_community.district_id = null; //-----to remove the previous selection
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
                    if (choice.region_id === this.species_community.region_id) {
                        this.filtered_district_list.push(choice);
                    }
                }
            });
        },
        fetchRegions: function () {
            let vm = this;

            fetch(api_endpoints.regions).then(
                async (response) => {
                    vm.api_regions = await response.json();
                    for (var i = 0; i < vm.api_regions.length; i++) {
                        this.region_list.push({
                            text: vm.api_regions[i].name,
                            value: vm.api_regions[i].id,
                            districts: vm.api_regions[i].districts,
                        });
                    }
                    // vm.setProposalData2(this.regions);
                    if (vm.species_community.regions) {
                        vm.chainedSelectDistricts(
                            vm.species_community.regions,
                            'fetch'
                        );
                    }
                },
                (error) => {
                    console.log(error);
                }
            );
        },
        searchList: function (id, search_list) {
            /* Searches for dictionary in list */
            for (var i = 0; i < search_list.length; i++) {
                if (search_list[i].value == id) {
                    return search_list[i];
                }
            }
            return [];
        },
        chainedSelectDistricts: function (regions, action) {
            let vm = this;
            if (action != 'fetch') {
                // Remove any districts that are not in the selected regions
                var selected_districts = vm.species_community.districts;
                for (var i = 0; i < selected_districts.length; i++) {
                    var district = selected_districts[i];
                    var region = vm.searchList(district, vm.region_list);
                    if (region == []) {
                        vm.species_community.districts.splice(i, 1);
                    }
                }
            }
            vm.district_list = [];
            if (regions) {
                for (let r of regions) {
                    var api_districts = this.searchList(
                        r,
                        vm.region_list
                    ).districts;
                    if (api_districts.length > 0) {
                        for (let i = 0; i < api_districts.length; i++) {
                            this.district_list.push({
                                text: api_districts[i].name,
                                value: api_districts[i].id,
                                archive_date: api_districts[i].archive_date,
                            });
                        }
                    }
                }
            }
        },
        initialiseRegionSelect: function () {
            let vm = this;
            $(vm.$refs.regions_select)
                .select2({
                    theme: 'bootstrap-5',
                    allowClear: true,
                    multiple: true,
                    placeholder: 'Select Region',
                })
                .on('select2:select', function (e) {
                    var selected = $(e.currentTarget);
                    vm.species_community.regions = selected.val();
                    vm.chainedSelectDistricts(
                        vm.species_community.regions,
                        'select'
                    );
                    this.$emit('save-species-community');
                })
                .on('select2:unselect', function (e) {
                    var selected = $(e.currentTarget);
                    vm.species_community.regions = selected.val();
                    vm.chainedSelectDistricts(
                        vm.species_community.regions,
                        'deselect'
                    );
                    this.$emit('save-species-community');
                });
        },
        initialiseDistrictSelect: function () {
            let vm = this;
            $(vm.$refs.districts_select)
                .select2({
                    theme: 'bootstrap-5',
                    allowClear: true,
                    multiple: true,
                    placeholder: 'Select District',
                })
                .on('select2:select', function (e) {
                    var selected = $(e.currentTarget);
                    vm.species_community.districts = selected.val();
                    this.$emit('save-species-community');
                })
                .on('select2:unselect', function (e) {
                    var selected = $(e.currentTarget);
                    vm.species_community.districts = selected.val();
                    this.$emit('save-species-community');
                });
        },
        checkDate: function () {
            if (this.species_community.last_data_curation_date === '') {
                this.species_community.last_data_curation_date = null;
                return;
            }
            if (
                this.species_community.last_data_curation_date === null ||
                isNaN(new Date(this.species_community.last_data_curation_date))
            ) {
                return;
            }
            if (
                new Date(this.species_community.last_data_curation_date) >
                new Date()
            ) {
                this.species_community.last_data_curation_date = new Date()
                    .toISOString()
                    .split('T')[0];
                this.$nextTick(() => {
                    this.$refs.last_data_curation_date.focus();
                });
                swal.fire({
                    title: 'Error',
                    text: 'Last data curation date cannot be in the future',
                    icon: 'error',
                    customClass: {
                        confirmButton: 'btn btn-primary',
                    },
                });
            }
            if (
                new Date(this.species_community.last_data_curation_date) <
                new Date('1990-01-01')
            ) {
                this.species_community.last_data_curation_date = new Date(
                    '1990-01-01'
                )
                    .toISOString()
                    .split('T')[0];
                this.$nextTick(() => {
                    this.$refs.last_data_curation_date.focus();
                });
                swal.fire({
                    title: 'Error',
                    text: 'Last data curation date cannot be before 01/01/1990',
                    icon: 'error',
                    customClass: {
                        confirmButton: 'btn btn-primary',
                    },
                });
            }
        },
        handleMinimumFireIntervalRange: function () {
            if (this.minimum_fire_interval_range == false) {
                this.species_community.conservation_attributes.minimum_fire_interval_to =
                    null;
            }
        },
        validateMinimumFireIntervalRange: function () {
            const rangeFrom = parseInt(
                this.species_community.conservation_attributes
                    .minimum_fire_interval_from
            );
            const rangeTo = parseInt(
                this.species_community.conservation_attributes
                    .minimum_fire_interval_to
            );
            const intervalChoice =
                this.species_community.conservation_attributes
                    .minimum_fire_interval_choice;
            if (
                (rangeFrom != null || rangeTo != null) &&
                intervalChoice == null
            ) {
                this.errors.minimum_fire_interval_error =
                    'Please select years/months';
            } else if (rangeFrom >= rangeTo) {
                this.errors.minimum_fire_interval_error =
                    'Please enter a valid range';
            } else {
                this.errors.minimum_fire_interval_error = '';
            }
        },
        switchNOO: function (value) {
            let vm = this;
            var selectedValue = value;
            if (selectedValue === 'true') {
                swal.fire({
                    title: 'Changing from Manual to Auto',
                    text: 'If you choose to revert back to manual in future the manually entered value will still be there for you. It is not deleted.',
                    icon: 'info',
                    confirmButtonText: 'Ok',
                    showCancelButton: false,
                    customClass: {
                        confirmButton: 'btn btn-primary',
                    },
                    reverseButtons: true,
                }).then(
                    (swalresult) => {
                        if (swalresult.isConfirmed) {
                            document.getElementById('noo_auto').checked = true;
                            document.getElementById('noo_manual').checked =
                                false;
                            vm.species_community.distribution.noo_auto = true;
                        } else if (
                            swalresult.dismiss === swal.DismissReason.cancel
                        ) {
                            document.getElementById('noo_manual').checked =
                                true;
                            document.getElementById('noo_auto').checked = false;
                            vm.species_community.distribution.noo_auto = false;
                        }
                    },
                    (error) => {
                        console.error('Error:', error);
                    }
                );
            } else {
                document.getElementById('noo_manual').checked = true;
                document.getElementById('noo_auto').checked = false;
                vm.species_community.distribution.noo_auto = false;
                vm.$nextTick(() => {
                    vm.$refs.number_of_occurrences.focus();
                });
            }
        },
        switchEOO: function (value) {
            let vm = this;
            var selectedValue = value;
            if (selectedValue === 'true') {
                swal.fire({
                    title: 'Changing from Manual to Auto',
                    text: 'If you choose to revert back to manual in future the manually entered value will still be there for you. It is not deleted.',
                    icon: 'info',
                    confirmButtonText: 'Ok',
                    showCancelButton: false,
                    customClass: {
                        confirmButton: 'btn btn-primary',
                    },
                    reverseButtons: true,
                }).then(
                    (swalresult) => {
                        if (swalresult.isConfirmed) {
                            // set EOO field to calculted_EOO vale
                            document.getElementById('eoo_auto').checked = true;
                            document.getElementById('eoo_manual').checked =
                                false;
                            // set eoo to true to fire the change of value so the EOO input box readonly
                            vm.species_community.distribution.eoo_auto = true;
                            vm.$nextTick(() => {
                                vm.enablePopovers();
                            });
                        } else if (
                            swalresult.dismiss === swal.DismissReason.cancel
                        ) {
                            document.getElementById('eoo_manual').checked =
                                true;
                            document.getElementById('eoo_auto').checked = false;
                            // set eoo to false to fire the change of value so the EOO input box will be editable
                            vm.species_community.distribution.eoo_auto = false;
                            //Otherwise revert back to its manual value if swal cancelled
                        }
                    },
                    (error) => {
                        console.error('Error:', error);
                    }
                );
            } else {
                // set EOO value to null if manual selected
                document.getElementById('eoo_manual').checked = true;
                document.getElementById('eoo_auto').checked = false;
                // set eoo to false to fire the change of value so the EOO input box will be editable
                vm.species_community.distribution.eoo_auto = false;
                vm.$nextTick(() => {
                    vm.$refs.extent_of_occurrence.focus();
                });
            }
        },
        switchAOOActual: function (value) {
            let vm = this;
            var selectedValue = value;
            if (selectedValue === 'true') {
                swal.fire({
                    title: 'Changing from Manual to Auto',
                    text: 'If you choose to revert back to manual in future the manually entered value will still be there for you. It is not deleted.',
                    icon: 'info',
                    confirmButtonText: 'Ok',
                    showCancelButton: false,
                    customClass: {
                        confirmButton: 'btn btn-primary',
                    },
                    reverseButtons: true,
                }).then(
                    (swalresult) => {
                        if (swalresult.isConfirmed) {
                            // set AOOActual field to calculted_AOOActual vale
                            document.getElementById('aoo_actual_auto').checked =
                                true;
                            document.getElementById(
                                'aoo_actual_manual'
                            ).checked = false;
                            // set aoo_actual to true to fire the change of value so the AOOActual input box readonly
                            vm.species_community.distribution.aoo_actual_auto = true;
                            vm.$nextTick(() => {
                                vm.enablePopovers();
                            });
                        } else if (
                            swalresult.dismiss === swal.DismissReason.cancel
                        ) {
                            document.getElementById(
                                'aoo_actual_manual'
                            ).checked = true;
                            document.getElementById('aoo_actual_auto').checked =
                                false;
                            // set eoo to false to fire the change of value so the EOO input box will be editable
                            vm.species_community.distribution.aoo_actual_auto = false;
                            //Otherwise revert back to its manual value if swal cancelled
                        }
                    },
                    (error) => {
                        console.error('Error:', error);
                    }
                );
            } else {
                // set AOOActual value to null if manual selected
                document.getElementById('aoo_actual_manual').checked = true;
                document.getElementById('aoo_actual_auto').checked = false;
                // set aoo_actual to false to fire the change of value so the AOOActual input box will be editable
                vm.species_community.distribution.aoo_actual_auto = false;
                vm.$nextTick(() => {
                    vm.$refs.area_of_occupancy_actual.focus();
                });
            }
        },
        saveSpeciesCommunity: function (e) {
            // For the select2 we emit after the select/unselect event as otherwise the value is not yet updated
            if (!e.target.className.includes('select2-')) {
                this.$emit('save-species-community');
            }
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
</style>
