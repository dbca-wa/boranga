<template lang="html">
    <div id="species_combine_profile">
        <FormSection
            :form-collapse="false"
            label="Taxonomy"
            Index="species-combine-taxon-form-section-index"
        >
            <div class="row mb-3">
                <label
                    for="combine-species-display"
                    class="col-sm-3 col-form-label"
                    >Scientific Name:</label
                >
                <div class="col-sm-8">
                    <textarea
                        id="combine-species-display"
                        v-model="species_display"
                        disabled
                        class="form-control"
                        rows="3"
                    />
                </div>
            </div>
            <div class="row mb-3">
                <label for="combine-common-name" class="col-sm-3 col-form-label"
                    >Common Name:</label
                >
                <div class="col-sm-8">
                    <textarea
                        id="combine-common-name"
                        v-model="common_name"
                        :disabled="true"
                        class="form-control"
                        rows="2"
                        placeholder=""
                    />
                </div>
            </div>
            <div class="row mb-3">
                <label
                    for="combine-taxon-name-id"
                    class="col-sm-3 col-form-label"
                    >Taxon Name ID:</label
                >
                <div class="col-sm-8">
                    <input
                        id="combine-taxon-name-id"
                        v-model="taxon_name_id"
                        :disabled="true"
                        type="text"
                        class="form-control"
                        placeholder=""
                    />
                </div>
            </div>
            <div class="row mb-3">
                <label
                    for="combine-previous-name"
                    class="col-sm-3 col-form-label"
                    >Previous Name:</label
                >
                <div class="col-sm-8">
                    <input
                        id="combine-previous-name"
                        v-model="taxon_previous_name"
                        :disabled="true"
                        type="text"
                        class="form-control"
                        placeholder=""
                    />
                </div>
            </div>
            <div class="row mb-3">
                <label
                    for="combine-phylogenetic-group"
                    class="col-sm-3 col-form-label"
                    >Informal Group:</label
                >
                <div class="col-sm-8">
                    <textarea
                        id="combine-phylogenetic-group"
                        v-model="informal_group"
                        :disabled="true"
                        class="form-control"
                        rows="1"
                        placeholder=""
                    />
                </div>
            </div>
            <div class="row mb-3">
                <label for="combine-family" class="col-sm-3 col-form-label"
                    >Family:</label
                >
                <div class="col-sm-8">
                    <textarea
                        id="combine-family"
                        v-model="family"
                        :disabled="true"
                        rows="1"
                        class="form-control"
                        placeholder=""
                    />
                </div>
            </div>
            <div class="row mb-3">
                <label for="combine-genus" class="col-sm-3 col-form-label"
                    >Genus:</label
                >
                <div class="col-sm-8">
                    <textarea
                        id="combine-genus"
                        v-model="genus"
                        :disabled="true"
                        rows="1"
                        class="form-control"
                        placeholder=""
                    />
                </div>
            </div>
            <div class="row mb-3">
                <label
                    for="combine-name-authority"
                    class="col-sm-3 col-form-label"
                    >Name Authority:</label
                >
                <div class="col-sm-8">
                    <textarea
                        id="combine-name-authority"
                        v-model="name_authority"
                        :disabled="true"
                        rows="1"
                        class="form-control"
                        placeholder=""
                    />
                </div>
            </div>
            <div class="row mb-3">
                <label
                    for="combine-name-comments"
                    class="col-sm-3 col-form-label"
                    >Nomos names comments:</label
                >
                <div class="col-sm-8">
                    <textarea
                        id="combine-name-comments"
                        v-model="name_comments"
                        :disabled="true"
                        class="form-control"
                        rows="3"
                        placeholder=""
                    />
                </div>
            </div>
        </FormSection>
        <FormSection
            :form-collapse="false"
            label="Distribution"
            Index="species-combine-distribution-form-section-index"
        >
            <div
                v-for="species in speciesBeingCombined"
                :key="species.id"
                class="row mb-3"
            >
                <label
                    :for="'combine-distribution' + species.id"
                    class="col-sm-3 col-form-label text-secondary"
                    >{{ species.species_number }} Distribution:</label
                >
                <div class="col-sm-8">
                    <textarea
                        :id="'combine-distribution-' + species.id"
                        v-model="species.distribution.distribution"
                        :disabled="true"
                        type="text"
                        class="form-control"
                        placeholder=""
                    />
                </div>
                <div class="col-sm-1">
                    <div class="form-check form-check-inline">
                        <input
                            :id="'combine-distribution-checkbox-' + species.id"
                            class="form-check-input"
                            type="checkbox"
                            :name="'distribution-checkbox-' + species.id"
                            @change="
                                concatenateOrRemoveValue(
                                    'combine-distribution-checkbox-' +
                                        species.id,
                                    'combine-distribution-' + species.id,
                                    'distribution.distribution',
                                    species.distribution.distribution
                                )
                            "
                        />
                    </div>
                </div>
            </div>
            <div class="row mb-3 pb-3 border-bottom pt-3 custom-border-top">
                <label
                    for="combine-distribution"
                    class="col-sm-3 col-form-label"
                    >Distribution:</label
                >
                <div class="col-sm-8">
                    <textarea
                        id="combine-distribution"
                        v-model="species_community.distribution.distribution"
                        class="form-control"
                        rows="1"
                        placeholder=""
                    />
                </div>
            </div>
            <div
                v-for="species in speciesBeingCombined"
                :key="species.id"
                class="row mb-3"
            >
                <label
                    :for="'combine-regions-select-' + species.id"
                    class="col-sm-3 col-form-label text-secondary"
                    >{{ species.species_number }} Region:</label
                >
                <div class="col-sm-8">
                    <span>{{ getselectedRegionNames(species) }}</span>
                </div>
                <div class="col-sm-1">
                    <div class="form-check form-check-inline">
                        <input
                            :id="'combine-regions-select-' + species.id"
                            class="form-check-input"
                            type="checkbox"
                            @change="
                                checkRegionDistrictInput(
                                    'combine-regions-select-' + species.id,
                                    'regions',
                                    species,
                                    'regions_select'
                                )
                            "
                        />
                    </div>
                </div>
            </div>
            <div class="row mb-3 pb-3 border-bottom pt-3 custom-border-top">
                <label
                    for="combine-regions-select"
                    class="col-sm-3 col-form-label"
                    >Region:</label
                >
                <div :id="select_regions" class="col-sm-8">
                    <select
                        id="combine-regions-select"
                        ref="regions_select"
                        v-model="species_community.regions"
                        style="width: 100%"
                        class="form-select input-sm"
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
            <div
                v-for="species in speciesBeingCombined"
                :key="species.id"
                class="row mb-3"
            >
                <label
                    :for="'combine-districts-select-' + species.id"
                    class="col-sm-3 col-form-label text-secondary"
                    >{{ species.species_number }} District:</label
                >
                <div class="col-sm-8">
                    <label
                        :for="'combine-districts-select-' + species.id"
                        class="control-label"
                        >{{ getselectedDistrictNames(species) }}</label
                    >
                </div>
                <div class="col-sm-1">
                    <div class="form-check form-check-inline">
                        <input
                            :id="'combine-districts-select-' + species.id"
                            class="form-check-input"
                            type="checkbox"
                            :disabled="isDistrictCheckboxDisabled(species)"
                            @change="
                                checkRegionDistrictInput(
                                    'combine-districts-select-' + species.id,
                                    'districts',
                                    species,
                                    'districts_select'
                                )
                            "
                        />
                    </div>
                </div>
            </div>
            <div class="row mb-3 pb-3 border-bottom pt-3 custom-border-top">
                <label
                    for="combine-districts-select"
                    class="col-sm-3 col-form-label"
                    >District:</label
                >
                <div :id="select_districts" class="col-sm-8">
                    <select
                        id="combine-districts-select"
                        ref="districts_select"
                        v-model="species_community.districts"
                        class="form-select"
                    >
                        <option value="" selected disabled>
                            Select district
                        </option>
                        <option
                            v-for="option in district_list"
                            :key="option.value"
                            :value="option.value"
                        >
                            {{ option.text }}
                        </option>
                    </select>
                </div>
            </div>
            <div
                v-for="species in speciesBeingCombined"
                :key="species.id"
                class="row mb-3"
            >
                <label
                    :for="'combine-number-of-occurrences-' + species.id"
                    class="col-sm-4 col-form-label text-secondary"
                    >{{ species.species_number }} Number of Occurrences:</label
                >
                <div class="col-sm-4">
                    <input
                        :id="'combine-number-of-occurrences-' + species.id"
                        v-model="species.distribution.number_of_occurrences"
                        :disabled="true"
                        type="number"
                        class="form-control"
                        placeholder=""
                    />
                </div>
                <div class="col-sm-3 d-flex align-items-center">
                    <div class="form-check form-check-inline">
                        <input
                            v-model="species.distribution.noo_auto"
                            type="radio"
                            value="true"
                            :disabled="true"
                            class="form-check-input"
                            :name="'noo_auto_' + species.id"
                        />
                        <label class="form-check-label">auto</label>
                    </div>
                    <div class="form-check form-check-inline">
                        <input
                            v-model="species.distribution.noo_auto"
                            type="radio"
                            value="false"
                            :disabled="true"
                            class="form-check-input"
                            :name="'noo_auto_' + species.id"
                        />
                        <label class="form-check-label">manual</label>
                    </div>
                </div>
                <div class="col-sm-1">
                    <div class="form-check form-check-inline">
                        <input
                            :id="
                                'combine-number-of-occurrences-checkbox-' +
                                species.id
                            "
                            class="form-check-input"
                            type="checkbox"
                            name="combine-number-of-occurrences-checkbox"
                            @change="
                                setOrClearDistributionValue({
                                    fieldPath:
                                        'distribution.number_of_occurrences',
                                    speciesDistribution: species.distribution,
                                    autoField: 'noo_auto',
                                    manualField: 'number_of_occurrences',
                                    event: $event,
                                })
                            "
                        />
                    </div>
                </div>
            </div>
            <div class="row mb-3 pb-3 border-bottom pt-3 custom-border-top">
                <label
                    for="combine-number-of-occurrences"
                    class="col-sm-4 col-form-label"
                    >Number of Occurrences:</label
                >
                <div class="col-sm-4">
                    <input
                        v-if="species_community.distribution.noo_auto"
                        id="combine-number-of-occurrences"
                        ref="number_of_occurrences"
                        v-model="species_community.occurrence_count"
                        :disabled="species_community.distribution.noo_auto"
                        type="number"
                        class="form-control"
                        placeholder=""
                    />
                    <input
                        v-else
                        id="combine-number-of-occurrences"
                        ref="number_of_occurrences"
                        v-model="
                            species_community.distribution.number_of_occurrences
                        "
                        type="number"
                        class="form-control"
                        placeholder=""
                    />
                </div>
                <div class="col-sm-3 d-flex align-items-center">
                    <div class="form-check form-check-inline">
                        <input
                            v-model="species_community.distribution.noo_auto"
                            type="radio"
                            :value="true"
                            class="form-check-input"
                            name="noo_auto"
                        />
                        <label class="form-check-label">auto</label>
                    </div>
                    <div class="form-check form-check-inline">
                        <input
                            v-model="species_community.distribution.noo_auto"
                            type="radio"
                            :value="false"
                            class="form-check-input"
                            name="noo_auto"
                        />
                        <label class="form-check-label">manual</label>
                    </div>
                </div>
            </div>
            <div>
                <div
                    v-for="species in speciesBeingCombined"
                    :key="species.id"
                    class="row mb-3"
                >
                    <label
                        :for="'combine-extent-of-occurrences-' + species.id"
                        class="col-sm-4 col-form-label text-secondary"
                        >{{ species.species_number }} Extent of
                        Occurrences:</label
                    >
                    <div class="col-sm-4">
                        <div class="input-group">
                            <input
                                :id="
                                    'combine-extent-of-occurrences-' +
                                    species.id
                                "
                                v-model="
                                    species.distribution.extent_of_occurrences
                                "
                                :disabled="true"
                                type="number"
                                class="form-control"
                                placeholder=""
                            />
                            <span
                                :id="
                                    'combine-extent-of-occurrences-km2-addon' +
                                    species.id
                                "
                                class="input-group-text"
                                >km&#xb2;</span
                            >
                        </div>
                    </div>
                    <div class="col-sm-3 d-flex align-items-center">
                        <div class="form-check form-check-inline">
                            <input
                                v-model="species.distribution.eoo_auto"
                                type="radio"
                                value="true"
                                :disabled="true"
                                class="form-check-input"
                                :name="'eoo_auto_' + species.id"
                            />
                            <label class="form-check-label">auto</label>
                        </div>
                        <div class="form-check form-check-inline">
                            <input
                                v-model="species.distribution.eoo_auto"
                                type="radio"
                                value="false"
                                :disabled="true"
                                class="form-check-input"
                                :name="'eoo_auto_' + species.id"
                            />
                            <label class="form-check-label">manual</label>
                        </div>
                    </div>
                    <div class="col-sm-1">
                        <div class="form-check form-check-inline">
                            <input
                                :id="
                                    'combine-extent-of-occurrences-checkbox-' +
                                    species.id
                                "
                                class="form-check-input"
                                type="checkbox"
                                name="combine-extent-of-occurrences-checkbox"
                                @change="
                                    setOrClearDistributionValue({
                                        fieldPath:
                                            'distribution.extent_of_occurrences',
                                        speciesDistribution:
                                            species.distribution,
                                        autoField: 'eoo_auto',
                                        manualField: 'extent_of_occurrences',
                                        event: $event,
                                    })
                                "
                            />
                        </div>
                    </div>
                </div>
            </div>
            <div class="row mb-3 pb-3 border-bottom pt-3 custom-border-top">
                <label
                    for="combine-extent-of-occurrences"
                    class="col-sm-4 col-form-label"
                    >Extent of Occurrences:</label
                >
                <div class="col-sm-4">
                    <div class="input-group">
                        <input
                            v-if="species_community.distribution.eoo_auto"
                            id="combine-extent-of-occurrences"
                            v-model="
                                species_community.area_occurrence_convex_hull_km2
                            "
                            :disabled="species_community.distribution.eoo_auto"
                            type="number"
                            class="form-control"
                            placeholder=""
                        />
                        <input
                            v-else
                            id="combine-extent-of-occurrences"
                            ref="extent_of_occurrences"
                            v-model="
                                species_community.distribution
                                    .extent_of_occurrences
                            "
                            type="number"
                            min="0.00"
                            step="0.00001"
                            max="9999999999.99"
                            class="form-control"
                            placeholder=""
                        />
                        <span
                            id="combine-extent-of-occurrences-km2-addon"
                            class="input-group-text"
                            >km&#xb2;</span
                        >
                    </div>
                </div>
                <div class="col-sm-3 d-flex align-items-center">
                    <div class="form-check form-check-inline">
                        <input
                            v-model="species_community.distribution.eoo_auto"
                            type="radio"
                            :value="true"
                            class="form-check-input"
                            name="eoo_auto"
                        />
                        <label class="form-check-label">auto</label>
                    </div>
                    <div class="form-check form-check-inline">
                        <input
                            v-model="species_community.distribution.eoo_auto"
                            type="radio"
                            :value="false"
                            class="form-check-input"
                            name="eoo_auto"
                        />
                        <label class="form-check-label">manual</label>
                    </div>
                </div>
            </div>
            <div>
                <div
                    v-for="species in speciesBeingCombined"
                    :key="species.id"
                    class="row mb-3"
                >
                    <label
                        :for="'combine-area-of-occupancy-' + species.id"
                        class="col-sm-4 col-form-label text-secondary"
                        >{{ species.species_number }} Area of Occupancy:
                    </label>
                    <div class="col-sm-4 d-flex align-items-center">
                        <div class="input-group">
                            <input
                                :id="'combine-area-of-occupancy-' + species.id"
                                v-model="species.distribution.area_of_occupancy"
                                :disabled="true"
                                type="number"
                                class="form-control"
                                placeholder=""
                            /><span
                                :id="
                                    'combine-area-of-occupancy-km2-addon' +
                                    species.id
                                "
                                class="input-group-text"
                                >km&#xb2;</span
                            >
                        </div>
                    </div>
                    <div class="col-sm-3"></div>
                    <div class="col-sm-1">
                        <div class="form-check form-check-inline">
                            <input
                                :id="
                                    'combine-area-of-occupancy-checkbox-' +
                                    species.id
                                "
                                class="form-check-input"
                                type="checkbox"
                                name="combine-area-of-occupancy-checkbox"
                                @change="
                                    setOrClearDistributionValue({
                                        fieldPath:
                                            'distribution.area_of_occupancy',
                                        speciesDistribution:
                                            species.distribution,
                                        autoField: null,
                                        manualField: 'area_of_occupancy',
                                        event: $event,
                                    })
                                "
                            />
                        </div>
                    </div>
                </div>
            </div>
            <div class="row mb-3 pb-3 border-bottom pt-3 custom-border-top">
                <label
                    for="combine-area-of-occupancy"
                    class="col-sm-4 col-form-label"
                    >Area of Occupancy:
                    <div
                        id="combine-area-of-occupancy-help-text"
                        class="form-text"
                    >
                        Based on IUCN 2km x 2km grid method
                    </div></label
                >
                <div class="col-sm-4">
                    <div class="input-group">
                        <input
                            id="combine-area-of-occupancy"
                            ref="area_of_occupancy"
                            v-model="
                                species_community.distribution.area_of_occupancy
                            "
                            type="number"
                            class="form-control"
                            placeholder=""
                        />
                        <span
                            id="combine-area-of-occupancy-km2-addon"
                            class="input-group-text"
                            >km&#xb2;</span
                        >
                    </div>
                </div>
            </div>
            <div>
                <div
                    v-for="species in speciesBeingCombined"
                    :key="species.id"
                    class="row mb-3"
                >
                    <label
                        :for="'combine-actual-area-of-occupancy-' + species.id"
                        class="col-sm-4 col-form-label text-secondary"
                        >{{ species.species_number }} Actual Area of
                        Occupancy:</label
                    >
                    <div class="col-sm-4">
                        <div class="input-group">
                            <input
                                :id="
                                    'combine-actual-area-of-occupancy-' +
                                    species.id
                                "
                                v-model="
                                    species.distribution
                                        .area_of_occupancy_actual
                                "
                                :disabled="true"
                                type="number"
                                class="form-control"
                                placeholder=""
                            /><span
                                :id="
                                    'combine-actual-area-of-occupancy-km2-addon' +
                                    species.id
                                "
                                class="input-group-text"
                                >km&#xb2;</span
                            >
                        </div>
                    </div>
                    <div class="col-sm-3 d-flex align-items-center">
                        <div class="form-check form-check-inline">
                            <input
                                v-model="species.distribution.aoo_actual_auto"
                                type="radio"
                                value="true"
                                :disabled="true"
                                class="form-check-input"
                                :name="'aoo_actual_auto_' + species.id"
                            />
                            <label class="form-check-label">auto</label>
                        </div>
                        <div class="form-check form-check-inline">
                            <input
                                v-model="species.distribution.aoo_actual_auto"
                                type="radio"
                                value="false"
                                :disabled="true"
                                class="form-check-input"
                                :name="'aoo_actual_auto_' + species.id"
                            />
                            <label class="form-check-label">manual</label>
                        </div>
                    </div>
                    <div class="col-sm-1">
                        <div class="form-check form-check-inline">
                            <input
                                :id="
                                    'combine-area-of-occupancy-actual-checkbox-' +
                                    species.id
                                "
                                class="form-check-input"
                                type="checkbox"
                                name="combine-area-of-occupancy-actual-checkbox"
                                @change="
                                    setOrClearDistributionValue({
                                        fieldPath:
                                            'distribution.area_of_occupancy_actual',
                                        speciesDistribution:
                                            species.distribution,
                                        autoField: 'aoo_actual_auto',
                                        manualField: 'area_of_occupancy_actual',
                                        event: $event,
                                    })
                                "
                            />
                        </div>
                    </div>
                </div>
            </div>
            <div class="row mb-3 pb-3 border-bottom pt-3 custom-border-top">
                <label
                    for="combine-actual-area-of-occupancy"
                    class="col-sm-4 col-form-label"
                    >Actual Area of Occupancy:</label
                >
                <div class="col-sm-4">
                    <div class="input-group">
                        <input
                            v-if="
                                species_community.distribution.aoo_actual_auto
                            "
                            id="area_of_occupancy_actual"
                            ref="area_of_occupancy_actual"
                            v-model="species_community.area_of_occupancy_km2"
                            :disabled="
                                species_community.distribution.aoo_actual_auto
                            "
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
                <div class="col-sm-3 d-flex align-items-center">
                    <div class="form-check form-check-inline">
                        <input
                            v-model="
                                species_community.distribution.aoo_actual_auto
                            "
                            type="radio"
                            :value="true"
                            class="form-check-input"
                            name="aoo_actual_auto"
                        />
                        <label class="form-check-label">auto</label>
                    </div>
                    <div class="form-check form-check-inline">
                        <input
                            v-model="
                                species_community.distribution.aoo_actual_auto
                            "
                            type="radio"
                            :value="false"
                            class="form-check-input"
                            name="aoo_actual_auto"
                        />
                        <label class="form-check-label">manual</label>
                    </div>
                </div>
            </div>
            <div>
                <div
                    v-for="species in speciesBeingCombined"
                    :key="species.id"
                    class="row mb-3"
                >
                    <label
                        for="'combine-number-of-iucn-locations-' + species.id"
                        class="col-sm-4 col-form-label text-secondary"
                        >{{ species.species_number }} Number of IUCN
                        Locations:</label
                    >
                    <div class="col-sm-4">
                        <input
                            :id="
                                'combine-number-of-iucn-locations-' + species.id
                            "
                            v-model="
                                species.distribution.number_of_iucn_locations
                            "
                            :disabled="true"
                            type="number"
                            class="form-control"
                            placeholder=""
                        />
                    </div>
                    <div class="col-sm-3"></div>
                    <div class="col-sm-1">
                        <div class="form-check form-check-inline">
                            <input
                                id="no_of_iucn_locations_checkbox"
                                class="form-check-input"
                                type="checkbox"
                                name="no_of_iucn_locations_checkbox"
                                @change="
                                    setOrClearDistributionValue({
                                        fieldPath:
                                            'distribution.number_of_iucn_locations',
                                        speciesDistribution:
                                            species.distribution,
                                        autoField: null,
                                        manualField: 'number_of_iucn_locations',
                                        event: $event,
                                    })
                                "
                            />
                        </div>
                    </div>
                </div>
            </div>
            <div class="row mb-1 pt-3 custom-border-top">
                <label
                    for="combine-number-of-iucn-locations"
                    class="col-sm-4 col-form-label"
                    >Number of IUCN Locations:</label
                >
                <div class="col-sm-4">
                    <input
                        id="combine-number-of-iucn-locations"
                        v-model="
                            species_community.distribution
                                .number_of_iucn_locations
                        "
                        type="number"
                        class="form-control"
                        placeholder=""
                    />
                </div>
            </div>
        </FormSection>
        <FormSection
            :form-collapse="false"
            label="General"
            Index="species-combine-general-form-section-index"
        >
            <div>
                <div
                    v-for="species in speciesBeingCombined"
                    :key="species.id"
                    class="row mb-3"
                >
                    <label
                        for="combine-department-file-numbers"
                        class="col-sm-3 col-form-label text-secondary"
                        >{{ species.species_number }} Department File
                        Numbers:</label
                    >
                    <div class="col-sm-8">
                        <input
                            id="combine-department-file-numbers"
                            v-model="species.department_file_numbers"
                            :disabled="true"
                            type="text"
                            class="form-control"
                            placeholder=""
                        />
                    </div>
                    <div class="col-sm-1">
                        <div class="form-check form-check-inline">
                            <input
                                :id="
                                    'combine-department-file-numbers-checkbox-' +
                                    species.id
                                "
                                class="form-check-input"
                                type="checkbox"
                                name="combine-department-file-numbers-checkbox"
                                @change="
                                    concatenateOrRemoveValue(
                                        'combine-department-file-numbers-checkbox-' +
                                            species.id,
                                        'combine-department-file-numbers-checkbox-' +
                                            species.id,
                                        'department_file_numbers',
                                        species.department_file_numbers
                                    )
                                "
                            />
                        </div>
                    </div>
                </div>
            </div>
            <div class="row mb-3 pb-3 border-bottom pt-3 custom-border-top">
                <label
                    for="combine-department-file-numbers"
                    class="col-sm-3 col-form-label"
                    >Department File Numbers:
                </label>
                <div class="col-sm-8">
                    <input
                        id="combine-department-file-numbers"
                        ref="combine_department_file_numbers"
                        v-model="species_community.department_file_numbers"
                        type="text"
                        class="form-control"
                    />
                </div>
            </div>
            <div
                v-for="species in speciesBeingCombined"
                :key="species.id"
                class="row mb-3"
            >
                <label
                    :for="'combine-last-data-curation-date' + species.id"
                    class="col-sm-3 col-form-label text-secondary"
                    >{{ species.species_number }} Last data curation
                    date:</label
                >
                <div class="col-sm-8">
                    <input
                        :id="'combine-last-data-curation-date' + species.id"
                        v-model="species.last_data_curation_date"
                        type="date"
                        name="combine-last-data-curation-date"
                        class="form-control"
                        :disabled="true"
                    />
                </div>
                <div class="col-sm-1">
                    <div class="form-check form-check-inline">
                        <input
                            :id="
                                'combine-last-data-curation-date-form-check' +
                                species.id
                            "
                            class="form-check-input"
                            type="checkbox"
                            name="combine-last-data-curation-date-checkbox"
                            @change="
                                setOrClearValue(
                                    'last_data_curation_date',
                                    species.last_data_curation_date,
                                    $event.target.checked,
                                    $event
                                )
                            "
                        />
                    </div>
                </div>
            </div>
            <div class="row mb-3 pb-3 border-bottom">
                <label
                    for="combine-last-data-curation-date"
                    class="col-sm-3 col-form-label"
                    >Last data curation date:
                </label>
                <div class="col-sm-8">
                    <input
                        id="combine-last-data-curation-date"
                        ref="last_data_curation_date"
                        v-model="species_community.last_data_curation_date"
                        type="date"
                        class="form-control"
                        name="last_data_curation_date"
                        min="1990-01-01"
                        :max="new Date().toISOString().split('T')[0]"
                        @blur="checkDate()"
                    />
                </div>
            </div>
            <div
                v-for="species in speciesBeingCombined"
                :key="species.id"
                class="row mb-3"
            >
                <label
                    :for="'combine-conservation-plan-exists-no-' + species.id"
                    class="col-sm-3 col-form-label text-secondary"
                    >{{ species.species_number }} Conservation Plan
                    Exists:</label
                >
                <div class="col-sm-8 d-flex align-items-center">
                    <label
                        :for="
                            'combine-conservation-plan-exists-no-' + species.id
                        "
                        class="me-2"
                        >No</label
                    >
                    <input
                        :id="
                            'combine-conservation-plan-exists-no-' + species.id
                        "
                        v-model="species.conservation_plan_exists"
                        :disabled="true"
                        type="radio"
                        :value="false"
                        :name="'combine-conservation-plan-exists-' + species.id"
                        class="form-check-input me-2"
                    />
                    <label
                        :for="
                            'combine-conservation-plan-exists-yes-' + species.id
                        "
                        class="me-2"
                        >Yes</label
                    >
                    <input
                        :id="
                            'combine-conservation-plan-exists-yes-' + species.id
                        "
                        v-model="species.conservation_plan_exists"
                        :disabled="true"
                        type="radio"
                        :value="true"
                        :name="'combine-conservation-plan-exists-' + species.id"
                        class="form-check-input"
                    />
                </div>
                <div class="col-sm-1">
                    <div class="form-check form-check-inline">
                        <input
                            :id="
                                'combine-conservation-plan-exists-checkbox-' +
                                species.id
                            "
                            class="form-check-input"
                            type="checkbox"
                            name="combine-conservation-plan-exists-checkbox"
                            @change="
                                setOrClearValue(
                                    'conservation_plan_exists',
                                    species.conservation_plan_exists,
                                    $event.target.checked,
                                    $event
                                )
                            "
                        />
                    </div>
                </div>
            </div>
            <div class="row mb-3 pb-3 border-bottom pt-3 custom-border-top">
                <label class="col-sm-3 col-form-label"
                    >Conservation Plan Exists:
                </label>
                <div class="col-sm-9 d-flex align-items-center">
                    <label
                        for="combine-conservation-plan-exists-no"
                        class="me-2"
                        >No</label
                    >
                    <input
                        id="combine-conservation-plan-exists-no"
                        v-model="species_community.conservation_plan_exists"
                        type="radio"
                        :value="false"
                        name="conservation_plan_exists"
                        class="form-check-input me-2"
                    />
                    <label
                        for="combine-conservation-plan-exists-yes"
                        class="me-2"
                        >Yes</label
                    >
                    <input
                        id="combine-conservation-plan-exists-yes"
                        v-model="species_community.conservation_plan_exists"
                        type="radio"
                        :value="true"
                        class="form-check-input"
                        name="conservation_plan_exists"
                    />
                </div>
            </div>
            <template v-if="species_community.conservation_plan_exists">
                <div
                    v-for="species in speciesBeingCombined"
                    :key="species.id"
                    class="row mb-3"
                >
                    <label
                        :for="
                            'combine-conservation-plan-reference-' + species.id
                        "
                        class="col-sm-3 col-form-label text-secondary"
                        >{{ species.species_number }} Conservation Plan
                        Reference:</label
                    >
                    <div class="col-sm-8">
                        <input
                            :id="
                                'combine-conservation-plan-reference-' +
                                species.id
                            "
                            v-model="species.conservation_plan_reference"
                            :disabled="true"
                            type="text"
                            class="form-control"
                            placeholder=""
                        />
                    </div>
                    <div class="col-sm-1">
                        <div class="form-check form-check-inline">
                            <input
                                :id="
                                    'combine-conservation-plan-reference-checkbox-' +
                                    species.id
                                "
                                class="form-check-input"
                                type="checkbox"
                                name="combine-conservation-plan-reference-checkbox"
                                @change="
                                    concatenateOrRemoveValue(
                                        'combine-conservation-plan-reference-checkbox-' +
                                            species.id,
                                        'combine-conservation-plan-reference-' +
                                            species.id,
                                        'conservation_plan_reference',
                                        species.conservation_plan_reference
                                    )
                                "
                            />
                        </div>
                    </div>
                </div>
                <div class="row mb-3 pb-3 border-bottom">
                    <label
                        for="combine-conservation-plan-reference"
                        class="col-sm-3 col-form-label"
                        >Conservation Plan Reference:</label
                    >
                    <div class="col-sm-8">
                        <textarea
                            id="combine-conservation-plan-reference"
                            ref="conservation_plan_reference"
                            v-model="
                                species_community.conservation_plan_reference
                            "
                            class="form-control"
                            maxlength="500"
                            placeholder=""
                        />
                    </div>
                </div>
            </template>
            <div>
                <div
                    v-for="species in speciesBeingCombined"
                    :key="species.id"
                    class="row mb-3"
                >
                    <label
                        :for="'combine-comment-' + species.id"
                        class="col-sm-3 col-form-label text-secondary"
                        >{{ species.species_number }} Comment:</label
                    >
                    <div class="col-sm-8">
                        <textarea
                            :id="'combine-comment-' + species.id"
                            v-model="species.comment"
                            :disabled="true"
                            class="form-control"
                            rows="3"
                        />
                    </div>
                    <div class="col-sm-1">
                        <div class="form-check form-check-inline">
                            <input
                                :id="'combine-comment-chk-' + species.id"
                                class="form-check-input"
                                type="checkbox"
                                :name="'combine-comment-chk-' + species.id"
                                @change="
                                    concatenateOrRemoveValue(
                                        'combine-comment-chk-' + species.id,
                                        'combine-comment-' + species.id,
                                        'comment',
                                        species.comment
                                    )
                                "
                            />
                        </div>
                    </div>
                </div>
            </div>
            <div class="row mb-1 pt-3 custom-border-top">
                <label for="combine-comment" class="col-sm-3 col-form-label"
                    >Comment:</label
                >
                <div class="col-sm-8">
                    <textarea
                        id="combine-comment"
                        v-model="species_community.comment"
                        class="form-control"
                        rows="3"
                        placeholder=""
                    />
                </div>
            </div>
            <div></div>
        </FormSection>
    </div>
</template>

<script>
import FormSection from '@/components/forms/section_toggle.vue';
import { api_endpoints, helpers as h } from '@/utils/hooks';

export default {
    name: 'SpeciesCombineProfile',
    components: {
        FormSection,
    },
    props: {
        species_community: {
            type: Object,
            required: true,
        },
        speciesBeingCombined: {
            type: Array,
            required: true,
        },
    },
    data: function () {
        return {
            species_community_copy: null,
            select_regions: 'species-combine-select-regions',
            select_districts: 'species-combine-select-districts',

            region_list: [],
            district_list: [],

            selectedSpecies: null,
            species_display: '',
            common_name: null,
            taxon_name_id: null,
            taxon_previous_name: null,
            informal_group: null,
            family: null,
            genus: null,
            name_authority: null,
            name_comments: null,
        };
    },
    watch: {
        'species_community.distribution.noo_auto': function (newVal) {
            let vm = this;
            if (newVal === true || newVal === 'true') {
                vm.species_community.distribution.number_of_occurrences =
                    vm.species_community.occurrence_count;
            } else {
                vm.$nextTick(() => {
                    vm.$refs.number_of_occurrences.focus();
                });
            }
        },
        'species_community.distribution.eoo_auto': function (newVal) {
            let vm = this;
            if (newVal === true || newVal === 'true') {
                vm.species_community.distribution.extent_of_occurrences =
                    vm.species_community.area_occurrence_convex_hull_km2;
            } else {
                vm.$nextTick(() => {
                    vm.$refs.extent_of_occurrences.focus();
                });
            }
        },
        'species_community.distribution.aoo_actual_auto': function (newVal) {
            let vm = this;
            if (newVal === true || newVal === 'true') {
                vm.species_community.distribution.area_of_occupancy_actual =
                    vm.species_community.area_of_occupancy_km2;
            } else {
                vm.$nextTick(() => {
                    vm.$refs.area_of_occupancy_actual.focus();
                });
            }
        },
        'species_community.taxonomy_id': function () {
            this.loadTaxonomydetails();
            this.species_community_copy = JSON.parse(
                JSON.stringify(this.species_community)
            );
        },
    },
    computed: {
        validDistrictIds() {
            // Get all valid district IDs for the selected regions
            let ids = [];
            this.region_list.forEach((region) => {
                if (
                    this.species_community.regions &&
                    this.species_community.regions.includes(region.value)
                ) {
                    region.districts.forEach((district) => {
                        ids.push(district.id);
                    });
                }
            });
            return ids;
        },
    },
    created: async function () {
        let vm = this;
        vm.fetchRegions();
        vm.species_community_copy = JSON.parse(
            JSON.stringify(vm.species_community)
        );
    },
    mounted: function () {
        let vm = this;
        vm.loadTaxonomydetails();
        vm.initialiseRegionSelect();
        vm.initialiseDistrictSelect();
    },
    methods: {
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
        loadTaxonomydetails: function () {
            let vm = this;
            if (vm.species_community.taxonomy_details != null) {
                var newOption = new Option(
                    vm.species_community.taxonomy_details.scientific_name,
                    vm.species_community.taxonomy_id,
                    false,
                    true
                );
                $('#' + vm.scientific_name_lookup).append(newOption);
                vm.species_display =
                    vm.species_community.taxonomy_details.scientific_name;
                vm.species_display =
                    vm.species_community.taxonomy_details.scientific_name;
                vm.common_name =
                    vm.species_community.taxonomy_details.common_name;
                vm.taxon_name_id =
                    vm.species_community.taxonomy_details.taxon_name_id;
                vm.taxon_previous_name =
                    vm.species_community.taxonomy_details.taxon_previous_name;
                vm.informal_group =
                    vm.species_community.taxonomy_details.informal_group;
                vm.family = vm.species_community.taxonomy_details.family_name;
                vm.genus = vm.species_community.taxonomy_details.genera_name;
                vm.name_authority =
                    vm.species_community.taxonomy_details.name_authority;
                vm.name_comments =
                    vm.species_community.taxonomy_details.name_comments;
            }
        },
        getselectedRegionNames(species) {
            // Filter regions_list to get only the selected regions
            let selected_region = species.regions;
            const selectedRegions = this.region_list.filter((region) =>
                selected_region.includes(region.value)
            );
            // Map the selected regions to their names and join them with commas
            return selectedRegions.map((region) => region.text).join(', ');
        },
        getselectedDistrictNames(species) {
            // Initialize an empty array to store the names of selected districts
            let selectedNames = [];
            // Iterate over each region
            this.region_list.forEach((region) => {
                // Filter the districts of the current region
                region.districts.forEach((district) => {
                    if (species.districts.includes(district.id)) {
                        selectedNames.push(district.name);
                    }
                });
            });
            // Join the names with commas
            return selectedNames.join(', ');
        },
        checkRegionDistrictInput: function (
            chkbox,
            obj_field,
            species,
            select2_ref = ''
        ) {
            // Get the value to add/remove (array for regions/districts)
            const value = species[obj_field];
            // Get the current array from the combined object
            let current = Array.isArray(this.species_community[obj_field])
                ? [...this.species_community[obj_field]]
                : [];

            if ($('#' + chkbox).is(':checked')) {
                // Add values not already present (robust to type)
                value.forEach((v) => {
                    if (!current.map(String).includes(String(v)))
                        current.push(v);
                });
            } else {
                // Remove values, robust to type (string/number) mismatches
                const valueStr = value.map(String);
                current = current.filter((v) => !valueStr.includes(String(v)));
            }

            // Set the updated array
            this.species_community[obj_field] = current;

            // If using select2, update the UI
            if (select2_ref && this.$refs[select2_ref]) {
                $(this.$refs[select2_ref]).val(current).trigger('change');
                if (obj_field === 'regions') {
                    this.chainedSelectDistricts(current, 'select');
                }
            }

            if (obj_field === 'regions') {
                // Get all valid district IDs for the selected regions
                let validDistrictIds = [];
                this.region_list.forEach((region) => {
                    if (current.map(String).includes(String(region.value))) {
                        region.districts.forEach((district) => {
                            validDistrictIds.push(district.id);
                        });
                    }
                });

                // Start with current districts or empty array
                let districtsCurrent = Array.isArray(
                    this.species_community.districts
                )
                    ? [...this.species_community.districts]
                    : [];

                if (
                    $('#' + chkbox).is(':checked') &&
                    Array.isArray(species.districts)
                ) {
                    // Concatenate valid districts from this species
                    species.districts.forEach((d) => {
                        if (
                            validDistrictIds.map(String).includes(String(d)) &&
                            !districtsCurrent.map(String).includes(String(d))
                        ) {
                            districtsCurrent.push(d);
                        }
                    });
                } else if (Array.isArray(species.districts)) {
                    // Remove only the districts from this species, robust to type
                    const speciesDistrictsStr = species.districts.map(String);
                    districtsCurrent = districtsCurrent.filter(
                        (d) => !speciesDistrictsStr.includes(String(d))
                    );
                }

                // Only keep districts that are valid for the selected regions (robust to type)
                const validDistrictIdsStr = validDistrictIds.map(String);
                districtsCurrent = districtsCurrent.filter((d) =>
                    validDistrictIdsStr.includes(String(d))
                );

                this.species_community.districts = districtsCurrent;

                // Update select2 for districts
                this.$nextTick(() => {
                    if (this.$refs.districts_select) {
                        $(this.$refs.districts_select)
                            .val(this.species_community.districts)
                            .trigger('change');
                    }
                });
            }
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
            for (var i = 0; i < search_list.length; i++) {
                if (search_list[i].value == id) {
                    return search_list[i];
                }
            }
            return { districts: [] };
        },
        chainedSelectDistricts: function (regions) {
            let vm = this;
            vm.district_list = [];
            if (regions) {
                for (let r of regions) {
                    var api_districts = this.searchList(
                        r,
                        vm.region_list
                    ).districts;
                    if (api_districts.length > 0) {
                        for (var i = 0; i < api_districts.length; i++) {
                            this.district_list.push({
                                text: api_districts[i].name,
                                value: api_districts[i].id,
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
                    dropdownParent: $('#' + vm.select_regions),
                    placeholder: 'Select Region',
                })
                .on('change', function (e) {
                    // Always assign a new array reference!
                    vm.species_community.regions = [
                        ...($(e.currentTarget).val() || []).map(Number),
                    ];
                    vm.chainedSelectDistricts(
                        vm.species_community.regions,
                        'select'
                    );
                    vm.$forceUpdate(); // Ensure Vue reactivity
                });
        },
        isDistrictCheckboxDisabled(species) {
            // Disable if no regions are selected
            if (
                !this.species_community.regions ||
                this.species_community.regions.length === 0
            ) {
                return true;
            }
            // Use the computed property so Vue tracks reactivity!
            const validDistrictIds = this.validDistrictIds;
            // Enable if at least one district in this species is valid for the selected regions
            return !species.districts.some((d) => validDistrictIds.includes(d));
        },
        initialiseDistrictSelect: function () {
            let vm = this;
            $(vm.$refs.districts_select)
                .select2({
                    theme: 'bootstrap-5',
                    allowClear: true,
                    dropdownParent: $('#' + vm.select_districts),
                    multiple: true,
                    placeholder: 'Select District',
                })
                .on('select2:select', function (e) {
                    var selected = $(e.currentTarget);
                    vm.species_community.districts = selected.val();
                })
                .on('select2:unselect', function (e) {
                    var selected = $(e.currentTarget);
                    vm.species_community.districts = selected.val();
                });
        },
        concatenateOrRemoveValue: function (
            checkboxId,
            inputId,
            fieldPath, // now accepts dot notation, e.g. 'distribution.distribution'
            value
        ) {
            let vm = this;
            let current = h.getNested(vm.species_community, fieldPath);
            if ($('#' + checkboxId).is(':checked')) {
                // If checked, concatenate if not already present
                let arr = current
                    ? current
                          .split(',')
                          .map((v) => v.trim())
                          .filter((v) => v.length > 0)
                    : [];
                if (!arr.includes(value)) {
                    arr.push(value);
                }
                h.setNested(vm.species_community, fieldPath, arr.join(', '));
            } else {
                // If unchecked, remove the value robustly
                let arr = current
                    ? current
                          .split(',')
                          .map((v) => v.trim())
                          .filter((v) => v.length > 0 && v !== value)
                    : [];
                h.setNested(vm.species_community, fieldPath, arr.join(', '));
            }
            // Final cleanup: remove stray commas/whitespace
            let cleaned = h.getNested(vm.species_community, fieldPath);
            if (cleaned) {
                cleaned = cleaned
                    .replace(/(^,)|(,$)/g, '')
                    .replace(/,{2,}/g, ',')
                    .trim();
                if (cleaned === ',') cleaned = '';
                h.setNested(vm.species_community, fieldPath, cleaned);
            }
        },
        setOrClearValue(fieldName, value, checked, event) {
            if (checked) {
                // Uncheck all other checkboxes with the same name
                const name = event.target.name;
                const checkboxes = document.querySelectorAll(
                    `input[type="checkbox"][name="${name}"]`
                );
                checkboxes.forEach((cb) => {
                    if (cb !== event.target) {
                        cb.checked = false;
                    }
                });
                this.species_community[fieldName] = value;
            } else {
                // Restore from copy if available, else empty string
                if (
                    this.species_community_copy &&
                    Object.prototype.hasOwnProperty.call(
                        this.species_community_copy,
                        fieldName
                    )
                ) {
                    // Restore the value
                    this.species_community[fieldName] =
                        this.species_community_copy[fieldName];
                } else {
                    this.species_community[fieldName] = '';
                }
                // If it's a radio group, also restore the radio value
                // Example: conservation_plan_exists
                const radio = document.querySelectorAll(
                    `input[type="radio"][name="${fieldName}"]`
                );
                radio.forEach((r) => {
                    r.checked =
                        r.value == String(this.species_community[fieldName]);
                });
            }
        },
        setOrClearDistributionValue({
            fieldPath, // e.g. 'distribution.number_of_occurrences'
            speciesDistribution, // e.g. species.distribution
            autoField, // e.g. 'noo_auto'
            manualField, // e.g. 'number_of_occurrences'
            event,
        }) {
            const checked = event.target.checked;
            let vm = this;

            if (checked) {
                // Uncheck all other checkboxes with the same name
                const name = event.target.name;
                const checkboxes = document.querySelectorAll(
                    `input[type="checkbox"][name="${name}"]`
                );
                checkboxes.forEach((cb) => {
                    if (cb !== event.target) {
                        cb.checked = false;
                    }
                });

                // 1. Detect the auto/manual state of the source species
                let sourceIsAuto = false;
                if (
                    autoField &&
                    speciesDistribution &&
                    autoField in speciesDistribution
                ) {
                    sourceIsAuto =
                        speciesDistribution[autoField] === true ||
                        speciesDistribution[autoField] === 'true';
                }

                // 2. Set the combined profile's auto/manual flag to match the source species
                if (autoField) {
                    vm.species_community.distribution[autoField] = sourceIsAuto;
                }

                // 3. Copy the correct value based on the new combined profile state
                let valueToCopy;
                if (manualField) {
                    valueToCopy = speciesDistribution[manualField];
                } else {
                    valueToCopy = speciesDistribution;
                }
                h.setNested(vm.species_community, fieldPath, valueToCopy);
            } else {
                // Restore from backup or clear
                let backup = this.species_community_copy;
                let restored = h.getNested(backup, fieldPath);
                h.setNested(
                    vm.species_community,
                    fieldPath,
                    restored !== undefined ? restored : ''
                );
                // Restore the auto/manual flag as well
                if (
                    autoField &&
                    backup &&
                    backup.distribution &&
                    autoField in backup.distribution
                ) {
                    vm.species_community.distribution[autoField] =
                        backup.distribution[autoField];
                }
            }
        },
    },
};
</script>
<style scoped>
.custom-border-top {
    border-top: 1px solid #f2f2f2;
}
</style>
