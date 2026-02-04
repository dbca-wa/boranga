<template lang="html">
    <div id="species">
        <FormSection :form-collapse="false" label="Taxonomy" :Index="taxonBody">
            <div v-if="showRetainOriginalSpeciesCheckbox" class="row mb-3">
                <div for="" class="col-sm-3">Retain Original Species?</div>
                <div class="col-sm-8">
                    <div class="form-check">
                        <input
                            class="form-check-input"
                            type="checkbox"
                            name="retain-original-species"
                            id="retain-original-species"
                            v-model="retainOriginalSpecies"
                            @change="toggleRetainOriginalSpecies"
                        />
                    </div>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label"
                    >Scientific Name:</label
                >
                <div :id="select_scientific_name" class="col-sm-8">
                    <select
                        :id="scientific_name_lookup"
                        :ref="scientific_name_lookup"
                        :name="scientific_name_lookup"
                        class="form-control"
                    />
                </div>
            </div>
            <template v-if="species_community.taxonomy_id">
                <div class="row mb-3">
                    <label for="" class="col-sm-3 control-label"></label>
                    <div class="col-sm-8">
                        <textarea
                            id="species_display"
                            v-model="species_display"
                            disabled
                            class="form-control"
                            rows="3"
                        />
                    </div>
                </div>
                <div class="row mb-3">
                    <label for="" class="col-sm-3 control-label"
                        >Common Name:</label
                    >
                    <div class="col-sm-8">
                        <textarea
                            id="common_name"
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
                        for="split_taxon_name_id"
                        class="col-sm-3 control-label"
                        >Taxon Name ID:</label
                    >
                    <div class="col-sm-8">
                        <input
                            id="split_taxon_name_id"
                            v-model="taxon_name_id"
                            :disabled="true"
                            type="text"
                            class="form-control"
                            placeholder=""
                        />
                    </div>
                </div>
                <div class="row mb-3">
                    <label for="" class="col-sm-3 control-label"
                        >Previous Name:</label
                    >
                    <div class="col-sm-8">
                        <input
                            id="previous_name"
                            v-model="taxon_previous_name"
                            :disabled="true"
                            type="text"
                            class="form-control"
                            placeholder=""
                        />
                    </div>
                </div>
                <div class="row mb-3">
                    <label for="" class="col-sm-3 control-label"
                        >Informal Group:</label
                    >
                    <div class="col-sm-8">
                        <textarea
                            id="informal_group"
                            v-model="informal_group"
                            :disabled="true"
                            class="form-control"
                            rows="1"
                            placeholder=""
                        />
                    </div>
                </div>
                <div class="row mb-3">
                    <label for="" class="col-sm-3 control-label">Family:</label>
                    <div class="col-sm-8">
                        <textarea
                            id="family"
                            v-model="family"
                            :disabled="true"
                            rows="1"
                            class="form-control"
                            placeholder=""
                        />
                    </div>
                </div>
                <div class="row mb-3">
                    <label for="" class="col-sm-3 control-label">Genus:</label>
                    <div class="col-sm-8">
                        <textarea
                            id="genus"
                            v-model="genus"
                            :disabled="true"
                            rows="1"
                            class="form-control"
                            placeholder=""
                        />
                    </div>
                </div>
                <div class="row mb-3">
                    <label for="" class="col-sm-3 control-label"
                        >Name Authority:</label
                    >
                    <div class="col-sm-8">
                        <textarea
                            id="name_authority"
                            v-model="name_authority"
                            :disabled="true"
                            rows="1"
                            class="form-control"
                            placeholder=""
                        />
                    </div>
                </div>
                <div class="row mb-3">
                    <label for="" class="col-sm-3 control-label"
                        >Nomos names comments:</label
                    >
                    <div class="col-sm-8">
                        <textarea
                            id="comment"
                            v-model="name_comments"
                            :disabled="true"
                            class="form-control"
                            rows="3"
                            placeholder=""
                        />
                    </div>
                </div>
            </template>
        </FormSection>
        <FormSection
            :form-collapse="false"
            label="Distribution"
            :Index="distributionBody"
        >
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label"
                    >{{ species_original.species_number }} Distribution:</label
                >
                <div class="col-sm-8">
                    <textarea
                        id="distribution"
                        v-model="species_original.distribution.distribution"
                        :disabled="true"
                        class="form-control"
                        rows="1"
                        placeholder=""
                    />
                </div>
                <div class="col-sm-1">
                    <input
                        :id="'distribution' + split_index"
                        class="form-check-input"
                        type="checkbox"
                        @change="
                            checkDistributionInput(
                                'distribution' + split_index,
                                'distribution'
                            )
                        "
                    />
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label"
                    >Distribution:</label
                >
                <div class="col-sm-8">
                    <textarea
                        id="distribution"
                        v-model="species_community.distribution.distribution"
                        class="form-control"
                        rows="1"
                        placeholder=""
                    />
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label"
                    >{{ species_original.species_number }} Region:</label
                >
                <div :id="select_regions_read_only" class="col-sm-8">
                    <label for="" class="control-label">{{
                        getselectedRegionNames(species_original)
                    }}</label>
                </div>
                <div class="col-sm-1">
                    <input
                        :id="'regions_select_chk' + split_index"
                        class="form-check-input"
                        type="checkbox"
                        @change="
                            checkRegionDistrictInput(
                                'regions_select_chk' + split_index,
                                'regions',
                                'regions_select'
                            )
                        "
                    />
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Region:</label>
                <div :id="select_regions" class="col-sm-8">
                    <select
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
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label"
                    >{{ species_original.species_number }} District:</label
                >
                <div :id="select_districts_read_only" class="col-sm-8">
                    <label for="" class="control-label">{{
                        getselectedDistrictNames(species_original)
                    }}</label>
                </div>
                <div class="col-sm-1">
                    <input
                        :id="'districts_select_chk' + split_index"
                        class="form-check-input"
                        type="checkbox"
                        v-model="useOriginalDistrictsCheckboxChecked"
                        :disabled="isOriginalDistrictCheckboxDisabled"
                        @change="
                            checkRegionDistrictInput(
                                'districts_select_chk' + split_index,
                                'districts',
                                'districts_select'
                            )
                        "
                    />
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">District:</label>
                <div :id="select_districts" class="col-sm-8">
                    <select
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
            <div class="row mb-3">
                <label for="" class="col-sm-4 col-form-label"
                    >{{ species_original.species_number }} Number of
                    Occurrences:</label
                >
                <div class="col-sm-4">
                    <input
                        v-if="species_original.distribution.noo_auto"
                        id="no_of_occurrences"
                        v-model="species_original.occurrence_count"
                        :disabled="true"
                        type="number"
                        class="form-control"
                        placeholder=""
                    />
                    <input
                        v-else
                        id="no_of_occurrences"
                        ref="number_of_occurrences"
                        v-model="
                            species_original.distribution.number_of_occurrences
                        "
                        :disabled="true"
                        type="number"
                        class="form-control"
                        placeholder=""
                    />
                </div>
                <div class="col-sm-3 d-flex align-items-center">
                    <div class="form-check form-check-inline">
                        <input
                            id="noo_auto"
                            v-model="species_original.distribution.noo_auto"
                            :disabled="true"
                            type="radio"
                            :value="true"
                            class="form-check-input"
                        />
                        <label class="form-check-label">auto</label>
                    </div>
                    <div class="form-check form-check-inline">
                        <input
                            id="noo_manual"
                            v-model="species_original.distribution.noo_auto"
                            :disabled="true"
                            type="radio"
                            :value="false"
                            class="form-check-input"
                        />
                        <label class="form-check-label">manual</label>
                    </div>
                </div>
                <div class="col-sm-1">
                    <input
                        :id="'no_of_occurrences' + split_index"
                        class="form-check-input"
                        type="checkbox"
                        @change="
                            checkDistributionInput(
                                'no_of_occurrences' + split_index,
                                'number_of_occurrences',
                                species_original.distribution.noo_auto,
                                'noo_auto'
                            )
                        "
                    />
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-4 col-form-label"
                    >Number of Occurrences:</label
                >
                <div class="col-sm-4">
                    <input
                        v-if="species_community.distribution.noo_auto"
                        id="no_of_occurrences"
                        v-model="species_community.occurrence_count"
                        type="number"
                        class="form-control"
                        :disabled="species_community.distribution.noo_auto"
                        placeholder=""
                    />
                    <input
                        v-else
                        id="no_of_occurrences"
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
                            id="noo_auto"
                            v-model="species_community.distribution.noo_auto"
                            type="radio"
                            :value="true"
                            class="form-check-input"
                        />
                        <label class="form-check-label">auto</label>
                    </div>
                    <div class="form-check form-check-inline">
                        <input
                            id="noo_manual"
                            v-model="species_community.distribution.noo_auto"
                            type="radio"
                            :value="false"
                            class="form-check-input"
                        />
                        <label class="form-check-label">manual</label>
                    </div>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-4 col-form-label"
                    >{{ species_original.species_number }} Extent of
                    Occurrences:
                </label>
                <div class="col-sm-4">
                    <div class="input-group">
                        <input
                            v-if="species_original.distribution.eoo_auto"
                            id="extent_of_occurrence"
                            v-model="
                                species_original.area_occurrence_convex_hull_km2
                            "
                            :disabled="true"
                            type="number"
                            class="form-control"
                            placeholder=""
                        />
                        <input
                            v-else
                            id="extent_of_occurrence"
                            ref="extent_of_occurrence"
                            v-model="
                                species_original.distribution
                                    .extent_of_occurrences
                            "
                            :disabled="true"
                            type="number"
                            min="0.00"
                            step="0.01"
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
                <div class="col-sm-3 d-flex align-items-center">
                    <div class="form-check form-check-inline">
                        <input
                            id="eoo_auto"
                            v-model="species_original.distribution.eoo_auto"
                            :disabled="true"
                            type="radio"
                            :value="true"
                            class="form-check-input"
                        />
                        <label class="form-check-label">auto</label>
                    </div>
                    <div class="form-check form-check-inline">
                        <input
                            id="eoo_manual"
                            v-model="species_original.distribution.eoo_auto"
                            :disabled="true"
                            type="radio"
                            :value="false"
                            class="form-check-input"
                        />
                        <label class="form-check-label">manual</label>
                    </div>
                </div>
                <div class="col-sm-1">
                    <input
                        :id="'eoo_manual' + species_original.id"
                        class="form-check-input"
                        type="checkbox"
                        @change="
                            checkDistributionInput(
                                'eoo_manual' + species_original.id,
                                'extent_of_occurrences',
                                species_original.distribution.eoo_auto,
                                'eoo_auto'
                            )
                        "
                    />
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-4 col-form-label"
                    >Extent of Occurrences:
                </label>
                <div class="col-sm-4">
                    <div class="input-group">
                        <input
                            v-if="species_community.distribution.eoo_auto"
                            id="extent_of_occurrence"
                            v-model="
                                species_community.area_occurrence_convex_hull_km2
                            "
                            type="number"
                            :disabled="species_community.distribution.eoo_auto"
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
                            type="number"
                            min="0.00"
                            step="0.01"
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
                <div class="col-sm-3 d-flex align-items-center">
                    <div class="form-check form-check-inline">
                        <input
                            id="eoo_auto"
                            v-model="species_community.distribution.eoo_auto"
                            type="radio"
                            :value="true"
                            class="form-check-input"
                        />
                        <label class="form-check-label">auto</label>
                    </div>
                    <div class="form-check form-check-inline">
                        <input
                            id="eoo_manual"
                            v-model="species_community.distribution.eoo_auto"
                            type="radio"
                            :value="false"
                            class="form-check-input"
                        />
                        <label class="form-check-label">manual</label>
                    </div>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-4 col-form-label"
                    >{{ species_original.species_number }} Area of
                    Occupancy:</label
                >
                <div class="col-sm-4">
                    <div class="input-group">
                        <input
                            id="area_of_occupany"
                            v-model="
                                species_original.distribution.area_of_occupancy
                            "
                            :disabled="true"
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
                <div class="col-sm-3"></div>
                <div class="col-sm-1">
                    <input
                        :id="'area_of_occupancy' + species_original.id"
                        class="form-check-input"
                        type="checkbox"
                        @change="
                            checkDistributionInput(
                                'area_of_occupancy' + species_original.id,
                                'area_of_occupancy'
                            )
                        "
                    />
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-4 col-form-label"
                    >Area of Occupancy:</label
                >

                <div class="col-sm-4">
                    <div class="input-group">
                        <input
                            id="area_of_occupany"
                            v-model="
                                species_community.distribution.area_of_occupancy
                            "
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
                <label for="" class="col-sm-4 col-form-label"
                    >{{ species_original.species_number }} Actual Area of
                    Occupancy:
                </label>
                <div class="col-sm-4">
                    <div class="input-group">
                        <input
                            v-if="species_original.distribution.aoo_actual_auto"
                            id="area_of_occupancy_actual"
                            v-model="species_original.area_of_occupancy_km2"
                            :disabled="true"
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
                                species_original.distribution
                                    .area_of_occupancy_actual
                            "
                            :disabled="true"
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
                            id="aoo_actual_auto"
                            v-model="
                                species_original.distribution.aoo_actual_auto
                            "
                            :disabled="true"
                            type="radio"
                            :value="true"
                            class="form-check-input"
                        />
                        <label class="form-check-label">auto</label>
                    </div>
                    <div class="form-check form-check-inline">
                        <input
                            id="aoo_actual_manual"
                            v-model="
                                species_original.distribution.aoo_actual_auto
                            "
                            :disabled="true"
                            type="radio"
                            :value="false"
                            class="form-check-input"
                        />
                        <label class="form-check-label">manual</label>
                    </div>
                </div>
                <div class="col-sm-1">
                    <input
                        :id="'aoo_actual_manual' + species_original.id"
                        class="form-check-input"
                        type="checkbox"
                        @change="
                            checkDistributionInput(
                                'aoo_actual_manual' + species_original.id,
                                'area_of_occupancy_actual',
                                species_original.distribution.aoo_actual_auto,
                                'aoo_actual_auto'
                            )
                        "
                    />
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-4 col-form-label"
                    >Actual Area of Occupancy:
                </label>
                <div class="col-sm-4">
                    <div class="input-group">
                        <input
                            v-if="
                                species_community.distribution.aoo_actual_auto
                            "
                            id="area_of_occupancy_actual"
                            v-model="species_community.area_of_occupancy_km2"
                            type="number"
                            :disabled="
                                species_community.distribution.aoo_actual_auto
                            "
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
                            id="aoo_actual_auto"
                            v-model="
                                species_community.distribution.aoo_actual_auto
                            "
                            type="radio"
                            :value="true"
                            class="form-check-input"
                        />
                        <label class="form-check-label">auto</label>
                    </div>
                    <div class="form-check form-check-inline">
                        <input
                            id="aoo_actual_manual"
                            v-model="
                                species_community.distribution.aoo_actual_auto
                            "
                            type="radio"
                            :value="false"
                            class="form-check-input"
                        />
                        <label class="form-check-label">manual</label>
                    </div>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-4 control-label"
                    >{{ species_original.species_number }} Number of IUCN
                    Locations:</label
                >
                <div class="col-sm-4">
                    <input
                        id="no_of_iucn_locations"
                        v-model="
                            species_original.distribution
                                .number_of_iucn_locations
                        "
                        :disabled="true"
                        type="number"
                        class="form-control"
                        placeholder=""
                    />
                </div>
                <div class="col-sm-3"></div>
                <div class="col-sm-1">
                    <input
                        :id="'no_of_iucn_locations' + split_index"
                        class="form-check-input"
                        type="checkbox"
                        @change="
                            checkDistributionInput(
                                'no_of_iucn_locations' + split_index,
                                'number_of_iucn_locations'
                            )
                        "
                    />
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-4 control-label"
                    >Number of IUCN Locations:</label
                >
                <div class="col-sm-4">
                    <input
                        id="no_of_iucn_locations"
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
            <div class="row mb-3">
                <label for="" class="col-sm-4 col-form-label"
                    >{{ species_original.species_number }} Number of IUCN
                    Sub-populations:</label
                >
                <div class="col-sm-4">
                    <input
                        id="number_of_iucn_subpopulations"
                        v-model="
                            species_original.distribution
                                .number_of_iucn_subpopulations
                        "
                        :disabled="true"
                        type="number"
                        class="form-control"
                        placeholder=""
                    />
                </div>
                <div class="col-sm-3"></div>
                <div class="col-sm-1">
                    <input
                        :id="'no_of_iucn_subpopulations' + split_index"
                        class="form-check-input"
                        type="checkbox"
                        @change="
                            checkDistributionInput(
                                'no_of_iucn_subpopulations' + split_index,
                                'number_of_iucn_subpopulations'
                            )
                        "
                    />
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-4 col-form-label"
                    >Number of IUCN Sub-populations:</label
                >
                <div class="col-sm-4">
                    <input
                        id="number_of_iucn_subpopulations"
                        v-model="
                            species_community.distribution
                                .number_of_iucn_subpopulations
                        "
                        type="number"
                        class="form-control"
                        placeholder=""
                    />
                </div>
            </div>
        </FormSection>
        <FormSection
            v-if="!thisSplitSpeciesHasOriginalTaxonomyId"
            :form-collapse="false"
            label="General"
            :Index="generalBody"
        >
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label"
                    >{{ species_original.species_number }} Department File
                    Numbers:</label
                >
                <div class="col-sm-8">
                    <input
                        id="department_file_numbers"
                        v-model="species_original.department_file_numbers"
                        :disabled="true"
                        type="text"
                        class="form-control"
                        placeholder=""
                    />
                </div>
                <div class="col-sm-1">
                    <input
                        :id="'dept_file_chk' + split_index"
                        class="form-check-input"
                        type="checkbox"
                        @change="
                            checkGeneralInput(
                                'dept_file_chk' + split_index,
                                'department_file_numbers'
                            )
                        "
                    />
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label"
                    >Department File Numbers:</label
                >
                <div class="col-sm-8">
                    <input
                        id="department_file_numbers"
                        v-model="species_community.department_file_numbers"
                        type="text"
                        class="form-control"
                        placeholder=""
                    />
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label"
                    >{{ species_original.species_number }} Last data curation
                    date:
                </label>
                <div class="col-sm-8">
                    <input
                        ref="last_data_curation_date"
                        v-model="species_original.last_data_curation_date"
                        :disabled="true"
                        type="date"
                        class="form-control"
                        name="last_data_curation_date"
                        min="1990-01-01"
                        :max="new Date().toISOString().split('T')[0]"
                        @blur="checkDate()"
                    />
                </div>
                <div class="col-sm-1">
                    <input
                        :id="'last_data_curation_date_chk' + split_index"
                        class="form-check-input"
                        type="checkbox"
                        @change="
                            checkGeneralInput(
                                'last_data_curation_date_chk' + split_index,
                                'last_data_curation_date'
                            )
                        "
                    />
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label"
                    >Last data curation date:
                </label>
                <div class="col-sm-8">
                    <input
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
            <div class="row mb-3">
                <label class="col-sm-3 col-form-label"
                    >{{ species_original.species_number }} Conservation Plan
                    Exists:
                </label>
                <div class="col-sm-8">
                    <label
                        for="original_conservation_plan_exists_no"
                        class="me-2"
                        >No</label
                    >
                    <input
                        id="original_conservation_plan_exists_no"
                        v-model="species_original.conservation_plan_exists"
                        :disabled="true"
                        type="radio"
                        :value="false"
                        name="original_conservation_plan_exists"
                        class="form-check-input me-2"
                    />
                    <label
                        for="original_conservation_plan_exists_yes"
                        class="me-2"
                        >Yes</label
                    >
                    <input
                        id="original_conservation_plan_exists_yes"
                        v-model="species_original.conservation_plan_exists"
                        :disabled="true"
                        type="radio"
                        :value="true"
                        name="original_conservation_plan_exists"
                        class="form-check-input"
                    />
                </div>
                <div class="col-sm-1">
                    <input
                        :id="'conservation_plan_exists_chk' + split_index"
                        class="form-check-input"
                        v-model="useOriginalConservationPlanExists"
                        type="checkbox"
                    />
                </div>
            </div>
            <div class="row mb-3">
                <label class="col-sm-3 col-form-label"
                    >Conservation Plan Exists:
                </label>
                <div class="col-sm-9">
                    <label for="conservation_plan_exists_no" class="me-2"
                        >No</label
                    >
                    <input
                        id="conservation_plan_exists_no"
                        v-model="species_community.conservation_plan_exists"
                        type="radio"
                        :value="false"
                        name="conservation_plan_exists"
                        class="form-check-input me-2"
                    />
                    <label for="conservation_plan_exists_yes" class="me-2"
                        >Yes</label
                    >
                    <input
                        id="conservation_plan_exists_yes"
                        v-model="species_community.conservation_plan_exists"
                        type="radio"
                        :value="true"
                        class="form-check-input"
                        name="conservation_plan_exists"
                        @change="focusConservationPlanReference"
                    />
                </div>
            </div>
            <div
                v-if="species_original.conservation_plan_exists"
                class="row mb-3"
            >
                <label
                    for="conservation_plan_reference"
                    class="col-sm-3 col-form-label"
                    >{{ species_original.species_number }} Conservation Plan
                    Reference:
                </label>
                <div class="col-sm-8">
                    <input
                        ref="conservation_plan_reference"
                        v-model="species_original.conservation_plan_reference"
                        :disabled="true"
                        type="text"
                        class="form-control"
                        name="conservation_plan_reference"
                    />
                </div>
                <div class="col-sm-1">
                    <input
                        :id="'conservation_plan_reference_chk' + split_index"
                        class="form-check-input"
                        type="checkbox"
                        @change="
                            checkGeneralInput(
                                'conservation_plan_reference_chk' + split_index,
                                'conservation_plan_reference'
                            )
                        "
                    />
                </div>
            </div>
            <div
                v-if="species_community.conservation_plan_exists"
                class="row mb-3"
            >
                <label
                    for="conservation_plan_reference"
                    class="col-sm-3 col-form-label"
                    >Conservation Plan Reference:
                </label>
                <div class="col-sm-8">
                    <input
                        ref="conservation_plan_reference"
                        v-model="species_community.conservation_plan_reference"
                        type="text"
                        :disabled="!species_community.conservation_plan_exists"
                        class="form-control"
                        name="conservation_plan_reference"
                    />
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label"
                    >{{ species_original.species_number }} Comment:</label
                >
                <div class="col-sm-8">
                    <textarea
                        id="comment"
                        v-model="species_original.comment"
                        :disabled="true"
                        class="form-control"
                        rows="3"
                        placeholder=""
                    />
                </div>
                <div class="col-sm-1">
                    <input
                        :id="'comment_chk' + split_index"
                        class="form-check-input"
                        type="checkbox"
                        @change="
                            checkGeneralInput(
                                'comment_chk' + split_index,
                                'comment'
                            )
                        "
                    />
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Comment:</label>
                <div class="col-sm-8">
                    <textarea
                        id="comment"
                        v-model="species_community.comment"
                        class="form-control"
                        rows="3"
                        placeholder=""
                    />
                </div>
            </div>
        </FormSection>
    </div>
</template>

<script>
import { v4 as uuid } from 'uuid';
import FormSection from '@/components/forms/section_toggle.vue';
import { api_endpoints, helpers } from '@/utils/hooks';
export default {
    name: 'SpeciesSplitProfile',
    components: {
        FormSection,
    },
    props: {
        species_community: {
            type: Object,
            required: true,
        },
        species_original: {
            type: Object,
            required: true,
        },
        splitSpeciesListContainsOriginalTaxonomy: {
            type: Boolean,
            required: true,
        },
        selectedTaxonomies: {
            type: Array,
            required: true,
        },
        split_index: {
            type: Number,
            required: true,
        },
    },
    data: function () {
        let vm = this;
        return {
            scientific_name_lookup:
                'scientific_name_lookup' + vm.species_community.index,
            select_scientific_name:
                'select_scientific_name' + vm.species_community.index,
            select_regions: 'select_regions' + vm.species_community.index,
            select_regions_read_only:
                'select_regions_read_only' + vm.species_community.index,
            select_districts_read_only:
                'select_districts_read_only' + vm.species_community.index,
            select_districts: 'select_districts' + vm.species_community.index,
            taxonBody: 'taxonBody' + uuid(),
            distributionBody: 'distributionBody' + uuid(),
            generalBody: 'generalBody' + uuid(),

            selected_species_community_copy: null,

            retainOriginalSpecies: false,

            region_list: [],
            district_list: [],
            district_list_readonly: [],
            useOriginalDistrictsCheckboxChecked: false,
            useOriginalConservationPlanExists: false,

            // to display the species Taxonomy selected details
            species_display: '',
            common_name: null,
            taxon_name_id: null,
            taxon_previous_name: null,
            informal_group: null,
            family: null,
            genus: null,
            name_authority: null,
            name_comments: null,
            species_id: null,
        };
    },
    computed: {
        isReadOnly: function () {
            let action = this.$route.query.action;
            if (
                action === 'edit' &&
                this.species_community &&
                this.species_community.user_edit_mode
            ) {
                return false;
            } else {
                return this.species_community.readonly;
            }
        },
        showRetainOriginalSpeciesCheckbox: function () {
            return (
                !this.splitSpeciesListContainsOriginalTaxonomy ||
                this.species_community.taxonomy_id ===
                    this.species_original.taxonomy_id
            );
        },
        thisSplitSpeciesHasOriginalTaxonomyId: function () {
            return (
                this.species_community.taxonomy_id ===
                this.species_original.taxonomy_id
            );
        },
        isOriginalDistrictCheckboxDisabled() {
            let validDistrictIds = [];
            this.region_list.forEach((region) => {
                if (
                    this.species_community.regions &&
                    this.species_community.regions.includes(region.value)
                ) {
                    region.districts.forEach((district) => {
                        validDistrictIds.push(district.id);
                    });
                }
            });

            // Disable if no regions are selected
            if (
                !this.species_community.regions ||
                this.species_community.regions.length === 0
            ) {
                return true;
            }
            if (validDistrictIds.length === 0) {
                return false;
            }

            if (!this.useOriginalDistrictsCheckboxChecked) {
                // If unchecked, enable only if original districts are valid
                return !this.species_original.districts.every((d) =>
                    validDistrictIds.includes(d)
                );
            } else {
                // If checked, enable only if revert districts are valid
                const revertDistricts = this.selected_species_community_copy
                    ? this.selected_species_community_copy.districts
                    : this.species_original.districts;
                return !revertDistricts.every((d) =>
                    validDistrictIds.includes(d)
                );
            }
        },
    },
    watch: {
        'species_community.distribution.noo_auto': function (newVal) {
            let vm = this;
            var selectedValue = newVal;
            if (selectedValue === 'true') {
                vm.species_community.distribution.number_of_occurrences =
                    vm.species_community.occurrence_count;
            } else {
                if (vm.species_id && vm.selected_species_community_copy) {
                    vm.species_community.distribution.number_of_occurrences =
                        vm.selected_species_community_copy.distribution.number_of_occurrences;
                } else {
                    vm.species_community.distribution.number_of_occurrences =
                        null;
                }
            }
        },
        'species_community.distribution.eoo_auto': function (newVal) {
            let vm = this;
            var selectedValue = newVal;
            if (selectedValue === 'true') {
                vm.species_community.distribution.extent_of_occurrences =
                    vm.species_community.area_occurrence_convex_hull_km2;
            } else {
                if (vm.species_id && vm.selected_species_community_copy) {
                    vm.species_community.distribution.extent_of_occurrences =
                        vm.selected_species_community_copy.distribution.extent_of_occurrences;
                } else {
                    vm.species_community.distribution.extent_of_occurrences =
                        null;
                }
            }
        },
        'species_community.distribution.aoo_actual_auto': function (newVal) {
            let vm = this;
            var selectedValue = newVal;
            if (selectedValue === 'true') {
                vm.species_community.distribution.area_of_occupancy_actual =
                    vm.species_community.area_of_occupancy_km2;
            } else {
                if (vm.species_id && vm.selected_species_community_copy) {
                    vm.species_community.distribution.area_of_occupancy_actual =
                        vm.selected_species_community_copy.distribution.area_of_occupancy_actual;
                } else {
                    vm.species_community.distribution.area_of_occupancy_actual =
                        null;
                }
            }
        },
        'species_community.regions': function (newRegions) {
            // Get all valid district IDs for the new regions
            let validDistrictIds = [];
            this.region_list.forEach((region) => {
                if (newRegions && newRegions.includes(region.value)) {
                    region.districts.forEach((district) => {
                        validDistrictIds.push(district.id);
                    });
                }
            });
            // If the original districts are not all valid, uncheck the checkbox and revert districts
            if (
                !this.species_original.districts.every((d) =>
                    validDistrictIds.includes(d)
                )
            ) {
                this.useOriginalDistrictsCheckboxChecked = false;
                // Revert districts to previous value
                const revertDistricts = this.selected_species_community_copy
                    ? this.selected_species_community_copy.districts
                    : [];
                this.species_community.districts = revertDistricts;
                // Update select2 widget
                this.$nextTick(() => {
                    if (this.$refs.districts_select) {
                        $(this.$refs.districts_select)
                            .val(this.species_community.districts)
                            .trigger('change');
                    }
                });
            }
        },
        'species_community.conservation_plan_exists': function () {
            // If the value of this field doesn't match that of the original species and the checkbox to
            // copy the value from the original species is checked, reset the checkbox
            if (
                this.useOriginalConservationPlanExists &&
                this.species_community.conservation_plan_exists !==
                    this.species_original.conservation_plan_exists
            ) {
                this.useOriginalConservationPlanExists = false;
            }
        },
        useOriginalConservationPlanExists(newVal) {
            if (newVal) {
                this.species_community.conservation_plan_exists =
                    this.species_original.conservation_plan_exists;
            } else {
                if (this.species_id && this.selected_species_community_copy) {
                    this.species_community.conservation_plan_exists =
                        this.selected_species_community_copy.conservation_plan_exists;
                } else {
                    this.species_community.conservation_plan_exists = false;
                }
            }
        },
        species_id: function (newVal) {
            if (newVal) {
                this.fetchSpeciesData();
            } else {
                this.resetSpeciesData();
            }
        },
    },
    created: async function () {
        let vm = this;
        //----set the distribution field values if auto onload
        if (vm.species_community.distribution.noo_auto == true) {
            vm.species_community.distribution.number_of_occurrences =
                vm.species_community.occurrence_count;
        }
        if (vm.species_community.distribution.eoo_auto == true) {
            vm.species_community.distribution.extent_of_occurrences =
                vm.species_community.area_occurrence_convex_hull_km2;
        }
        if (vm.species_community.distribution.aoo_actual_auto == true) {
            vm.species_community.distribution.area_of_occupancy_actual =
                vm.species_community.area_of_occupancy_km2;
        }
        vm.fetchRegions();
    },
    mounted: function () {
        let vm = this;
        vm.initialiseScientificNameLookup();
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
        resetTaxonomyDetails: function () {
            let vm = this;
            if (vm.species_community.taxonomy_details) {
                vm.species_community.taxonomy_details = null;
            }
            vm.species_community.taxonomy_id = '';
            vm.species_display = '';
            vm.common_name = '';
            vm.taxon_name_id = '';
            vm.taxon_previous_name = '';
            vm.informal_group = '';
            vm.family = '';
            vm.genus = '';
            vm.name_authority = '';
            vm.name_comments = '';
            vm.species_id = null;
        },
        initialiseScientificNameLookup: function () {
            let vm = this;
            $(vm.$refs[vm.scientific_name_lookup])
                .select2({
                    minimumInputLength: 2,
                    dropdownParent: $('#' + vm.select_scientific_name),
                    theme: 'bootstrap-5',
                    allowClear: true,
                    placeholder:
                        'Click to Search for the Scientific Name for this Split Species',
                    ajax: {
                        url: api_endpoints.scientific_name_lookup,
                        dataType: 'json',
                        data: function (params) {
                            var query = {
                                term: params.term,
                                type: 'public',
                                group_type_id:
                                    vm.species_community.group_type_id,
                                species_profile: false,
                                no_profile_draft_and_historical_only: true,
                                exclude_taxonomy_ids: vm.selectedTaxonomies,
                            };
                            return query;
                        },
                    },
                })
                .on('select2:selecting', function (e) {
                    let data = e.params.args.data.id;
                    if (
                        vm.splitSpeciesListContainsOriginalTaxonomy &&
                        vm.species_original.taxonomy_id === data
                    ) {
                        e.preventDefault();
                        swal.fire({
                            title: 'Original Taxonomy Already Selected',
                            text: 'There is already a split species with the same taxonomy as the original species.',
                            icon: 'info',
                            customClass: {
                                confirmButton: 'btn btn-primary',
                            },
                            didClose: () => {
                                vm.resetTaxonomyDetails();
                                vm.$nextTick(() => {
                                    $(
                                        vm.$refs[vm.scientific_name_lookup]
                                    ).select2('open');
                                });
                            },
                        });
                        return false;
                    }
                    if (vm.selectedTaxonomies.includes(data)) {
                        e.preventDefault();
                        swal.fire({
                            title: 'Taxonomy Already Selected',
                            text: 'This taxonomy is already selected in another split species.',
                            icon: 'info',
                            customClass: {
                                confirmButton: 'btn btn-primary',
                            },
                            didClose: () => {
                                vm.resetTaxonomyDetails();
                                vm.$nextTick(() => {
                                    $(
                                        vm.$refs[vm.scientific_name_lookup]
                                    ).select2('open');
                                });
                            },
                        });
                        return false;
                    }
                })
                .on('select2:select', function (e) {
                    let data = e.params.data.id;
                    if (
                        vm.retainOriginalSpecies &&
                        data != vm.species_original.taxonomy_id
                    ) {
                        vm.retainOriginalSpecies = false;
                    }
                    if (
                        !vm.retainOriginalSpecies &&
                        data == vm.species_original.taxonomy_id
                    ) {
                        vm.retainOriginalSpecies = true;
                    }
                    vm.species_community.taxonomy_id = data;
                    vm.species_community.taxonomy_details = e.params.data;
                    vm.species_display = e.params.data.scientific_name;
                    vm.common_name = e.params.data.common_name;
                    vm.taxon_name_id = e.params.data.taxon_name_id;
                    vm.taxon_previous_name = e.params.data.taxon_previous_name;
                    vm.informal_group = e.params.data.informal_group;
                    vm.family = e.params.data.family_name;
                    vm.genus = e.params.data.genera_name;
                    vm.name_authority = e.params.data.name_authority;
                    vm.name_comments = e.params.data.name_comments;
                    if (!vm.retainOriginalSpecies) {
                        vm.species_id = e.params.data.species_id;
                        // If this is a new species (no species_id), initialise selects
                        if (!vm.species_id) {
                            vm.$nextTick(() => {
                                vm.initialiseRegionSelect();
                                vm.chainedSelectDistricts(
                                    vm.species_community.regions,
                                    'init'
                                );
                            });
                        }
                    } else {
                        vm.$nextTick(() => {
                            vm.initialiseRegionSelect();
                            vm.initialiseDistrictSelect();
                        });
                    }
                })
                .on('select2:unselect', function () {
                    vm.resetTaxonomyDetails();
                    vm.species_community.taxonomy_id = '';
                    vm.species_community.taxonomy_details = null;
                    vm.species_id = null;
                    vm.retainOriginalSpecies = false;
                })
                .on('select2:clear', function () {
                    vm.resetTaxonomyDetails();
                    vm.species_community.taxonomy_id = '';
                    vm.species_community.taxonomy_details = null;
                    vm.species_id = null;
                    vm.retainOriginalSpecies = false;
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-' +
                            vm.scientific_name_lookup +
                            '-results"]'
                    );
                    // move focus to select2 field
                    searchField[0].focus();
                });
        },
        checkDistributionInput: function (
            chkbox,
            obj_field,
            auto = false,
            auto_field = ''
        ) {
            // if checkbox is checked copy value from original  species to new species
            if ($('#' + chkbox).is(':checked') == true) {
                if (auto) {
                    this.species_community.distribution[auto_field] =
                        this.species_original.distribution[auto_field];
                    return;
                }
                this.species_community.distribution[obj_field] =
                    this.species_original.distribution[obj_field];
            } else {
                if (this.species_id && this.selected_species_community_copy) {
                    this.species_community.distribution[obj_field] =
                        this.selected_species_community_copy.distribution[
                            obj_field
                        ];
                } else {
                    this.species_community.distribution[obj_field] =
                        this.species_original.distribution[obj_field] || null;
                }
                this.species_community.distribution[auto_field] = this
                    .selected_species_community_copy
                    ? this.selected_species_community_copy.distribution[
                          auto_field
                      ]
                    : false;
            }
        },
        checkGeneralInput: function (chkbox, obj_field, bool_field = null) {
            // if checkbox is checked copy value from original species to new species
            if ($('#' + chkbox).is(':checked') == true) {
                this.species_community[obj_field] =
                    this.species_original[obj_field];
                if (bool_field) {
                    this.species_community[bool_field] =
                        this.species_original[bool_field];
                }
            } else {
                if (this.species_id && this.selected_species_community_copy) {
                    this.species_community[obj_field] = this
                        .selected_species_community_copy
                        ? this.selected_species_community_copy[obj_field]
                        : '';
                    if (bool_field) {
                        this.species_community[bool_field] = this
                            .selected_species_community_copy
                            ? this.selected_species_community_copy[bool_field]
                            : this.species_original[bool_field]
                              ? this.species_original[bool_field]
                              : false;
                    }
                } else {
                    this.species_community[obj_field] = null;
                    if (bool_field) {
                        this.species_community[bool_field] = false;
                    }
                }
            }
        },
        checkGeneralBooleanInput: function (event) {
            if (event.target.checked) {
                this.species_community.conservation_plan_exists =
                    event.target.checked;
            } else {
                if (this.species_id && this.selected_species_community_copy) {
                    this.species_community.conservation_plan_exists =
                        this.selected_species_community_copy.conservation_plan_exists;
                } else {
                    this.species_community.conservation_plan_exists =
                        this.species_original.conservation_plan_exists;
                }
            }
        },
        checkRegionDistrictInput: function (
            chkbox,
            obj_field,
            select2_ref = ''
        ) {
            // if checkbox is checked copy value from original species to new species
            if ($('#' + chkbox).is(':checked') == true) {
                this.species_community[obj_field] =
                    this.species_original[obj_field];
                if (select2_ref != '') {
                    $(this.$refs[select2_ref])
                        .val(this.species_community[obj_field])
                        .trigger('change');
                    this.chainedSelectDistricts(
                        this.species_community.regions,
                        'select'
                    );
                }
                // --- Auto check and populate districts if regions are being copied ---
                if (obj_field === 'regions') {
                    // Get all valid district IDs for the selected regions
                    let validDistrictIds = [];
                    this.region_list.forEach((region) => {
                        if (
                            this.species_community.regions &&
                            this.species_community.regions.includes(
                                region.value
                            )
                        ) {
                            region.districts.forEach((district) => {
                                validDistrictIds.push(district.id);
                            });
                        }
                    });
                    // If original districts are valid for these regions, auto check and populate
                    if (
                        this.species_original.districts.every((d) =>
                            validDistrictIds.includes(d)
                        )
                    ) {
                        this.useOriginalDistrictsCheckboxChecked = true;
                        this.species_community.districts =
                            this.species_original.districts;
                        this.$nextTick(() => {
                            if (this.$refs.districts_select) {
                                $(this.$refs.districts_select)
                                    .val(this.species_community.districts)
                                    .trigger('change');
                            }
                        });
                    }
                }
            } else {
                if (select2_ref != '') {
                    let regionIds = this.selected_species_community_copy
                        ? this.selected_species_community_copy.regions
                        : this.species_original.regions;
                    $(this.$refs[select2_ref]).val(regionIds).trigger('change');
                    this.chainedSelectDistricts(regionIds, 'deselect');
                }
                this.species_community[obj_field] = this
                    .selected_species_community_copy
                    ? this.selected_species_community_copy[obj_field]
                    : [];
                // Uncheck districts checkbox if regions are unchecked
                if (obj_field === 'regions') {
                    this.useOriginalDistrictsCheckboxChecked = false;
                }
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
        focusConservationPlanReference: function () {
            this.$nextTick(() => {
                this.$refs.conservation_plan_reference.focus();
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
                    if (vm.species_community.regions) {
                        vm.chainedSelectDistricts(
                            vm.species_community.regions,
                            'fetch'
                        );
                    }
                    if (vm.species_original.regions) {
                        vm.chainedSelectReadonlyDistricts(
                            vm.species_original.regions,
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
                            vm.district_list.push({
                                text: api_districts[i].name,
                                value: api_districts[i].id,
                            });
                        }
                    }
                }
            }
            // Instead of duplicating Select2 code, just call:
            this.$nextTick(() => {
                this.initialiseDistrictSelect();
            });
        },
        chainedSelectReadonlyDistricts: function (regions) {
            let vm = this;

            vm.district_list_readonly = [];
            if (regions) {
                for (let r of regions) {
                    var api_districts = this.searchList(
                        r,
                        vm.region_list
                    ).districts;
                    if (api_districts.length > 0) {
                        for (var i = 0; i < api_districts.length; i++) {
                            this.district_list_readonly.push({
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
                .on('select2:select', function (e) {
                    var selected = $(e.currentTarget);
                    vm.species_community.regions = selected.val();
                    vm.chainedSelectDistricts(
                        vm.species_community.regions,
                        'select'
                    );
                })
                .on('select2:unselect', function (e) {
                    var selected = $(e.currentTarget);
                    vm.species_community.regions = selected.val();
                    vm.chainedSelectDistricts(
                        vm.species_community.regions,
                        'deselect'
                    );
                });

            // Set initial value after initialization
            $(vm.$refs.regions_select)
                .val(vm.species_community.regions)
                .trigger('change');

            $(vm.$refs.regions_select_read_only).select2({
                theme: 'bootstrap-5',
                allowClear: true,
                multiple: true,
                dropdownParent: $('#' + vm.select_regions_read_only),
                placeholder: 'Select Region',
            });
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

            // Set initial value after initialization
            $(vm.$refs.districts_select)
                .val(vm.species_community.districts)
                .trigger('change');

            $(vm.$refs.districts_select_read_only).select2({
                theme: 'bootstrap-5',
                allowClear: true,
                dropdownParent: $('#' + vm.select_districts_read_only),
                multiple: true,
                placeholder: 'Select District',
            });
        },
        toggleRetainOriginalSpecies: function (event) {
            this.retainOriginalSpecies = event.target.checked;
            if (this.retainOriginalSpecies) {
                // 1. Add the option if it doesn't exist
                if (
                    $(this.$refs[this.scientific_name_lookup]).find(
                        "option[value='" +
                            this.species_original.taxonomy_details.id +
                            "']"
                    ).length === 0
                ) {
                    $(this.$refs[this.scientific_name_lookup]).append(
                        new Option(
                            this.species_original.taxonomy_details.text,
                            this.species_original.taxonomy_details.id,
                            true,
                            true
                        )
                    );
                }

                // Manually call the logic from select2:select
                const data = this.species_original.taxonomy_details;
                this.species_community.taxonomy_details = data;
                this.species_display = data.scientific_name;
                this.common_name = data.common_name;
                this.taxon_name_id = data.taxon_name_id;
                this.taxon_previous_name = data.taxon_previous_name;
                this.informal_group = data.informal_group;
                this.family = data.family_name;
                this.genus = data.genera_name;
                this.name_authority = data.name_authority;
                this.name_comments = data.name_comments;

                this.species_community.taxonomy_id =
                    this.species_original.taxonomy_details.id;
                $(this.$refs[this.scientific_name_lookup])
                    .val(this.species_original.taxonomy_details.id)
                    .trigger('change');
            } else {
                $(this.$refs[this.scientific_name_lookup])
                    .val('')
                    .trigger('change');
                $(this.$refs[this.scientific_name_lookup]).select2('open');
                this.resetTaxonomyDetails();
                this.$nextTick(() => {
                    this.initialiseRegionSelect();
                    this.chainedSelectDistricts(
                        this.species_community.regions,
                        'init'
                    );
                });
            }
        },
        fetchSpeciesData: function () {
            fetch(
                helpers.add_endpoint_json(
                    api_endpoints.species,
                    this.species_id
                )
            )
                .then((response) => response.json())
                .then((data) => {
                    this.selected_species_community_copy = JSON.parse(
                        JSON.stringify(data)
                    );
                    // Set the data for the general and distribution sections and regions/districts
                    this.species_community.department_file_numbers =
                        data.department_file_numbers;
                    this.species_community.last_data_curation_date =
                        data.last_data_curation_date;
                    this.species_community.conservation_plan_exists =
                        data.conservation_plan_exists;
                    this.species_community.conservation_plan_reference =
                        data.conservation_plan_reference;
                    this.species_community.comment = data.comment;
                    this.species_community.distribution = data.distribution;
                    // For each region in the data, populate the regions select2
                    this.species_community.regions = data.regions;
                    this.species_community.districts = data.districts;
                    this.initialiseRegionSelect();
                    this.chainedSelectDistricts(
                        this.species_community.regions,
                        'fetch'
                    );
                })
                .catch((error) => {
                    console.error('Error fetching species data:', error);
                });
        },
        resetSpeciesData: function () {
            this.selected_species_community_copy = null;
            this.species_community.department_file_numbers =
                this.species_original.department_file_numbers;
            this.species_community.last_data_curation_date =
                this.species_original.last_data_curation_date;
            this.species_community.conservation_plan_exists =
                this.species_original.conservation_plan_exists;
            this.species_community.conservation_plan_reference =
                this.species_original.conservation_plan_reference;
            this.species_community.comment = this.species_original.comment;
            this.species_community.distribution =
                this.species_original.distribution;
            this.species_community.regions = this.species_original.regions;
            this.species_community.districts = this.species_original.districts;
        },
    },
};
</script>
