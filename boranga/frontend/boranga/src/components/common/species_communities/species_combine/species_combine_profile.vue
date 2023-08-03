<template lang="html">
    <div id="species_combine_profile">
        <FormSection :formCollapse="false" label="Taxonomy" :Index="taxonBody">
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Scientific Name:</label>
                <div class="col-sm-8" :id="select_scientific_name">
                    <!-- <select :disabled="isReadOnly" class="form-select" 
                        v-model="species_community.taxonomy_id" id="scientific_name" @change="loadTaxonomydetails()">
                        <option v-for="option in taxon_names" :value="option.id" v-bind:key="option.id">
                            {{ option.scientific_name }}                            
                        </option>
                    </select> -->
                    <select :disabled="isReadOnly"
                        :id="scientific_name_lookup"  
                        :name="scientific_name_lookup"  
                        :ref="scientific_name_lookup" 
                        class="form-control" />
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label"></label>
                <div class="col-sm-8">
                    <textarea disabled class="form-control" rows="3" id="species_display" v-model="species_display"/>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Common Name:</label>
                <div class="col-sm-8">
                    <textarea :disabled="true" class="form-control" rows="2" id="common_name" placeholder="" 
                    v-model="common_name"/>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Taxon Name ID:</label>
                <div class="col-sm-8">
                    <input :disabled="true" type="text" class="form-control" id="taxon_name_id" placeholder="" 
                    v-model="taxon_name_id"/>
                    <!-- gives error for the below when new species created as no taxonomy id is present -->
                    <!-- <input :disabled="true" type="text" class="form-control" id="taxon_name_id" placeholder="" 
                    v-model="species_communities.taxonomy_details.taxon_name_id"/> -->
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Previous Name:</label>
                <div class="col-sm-8">
                    <input :disabled="true" type="text" class="form-control" id="previous_name" placeholder="" 
                    v-model="taxon_previous_name"/>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Phylogenetic Group:</label>
                <div class="col-sm-8">
                    <!-- <select :disabled="true" class="form-select" v-model="phylogenetic_group_id" id="phylogenetic_group">
                        <option v-for="option in phylo_group_list" :value="option.id" v-bind:key="option.id">
                            {{ option.name }}                            
                        </option>
                    </select> -->
                    <textarea :disabled="true" class="form-control" rows="1" id="phylogenetic_group" placeholder="" 
                    v-model="phylogenetic_group"/>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Family:</label>
                <div class="col-sm-8">
                    <select :disabled="true" class="form-select" v-model="family_id" id="family">
                        <option v-for="option in family_list" :value="option.id" v-bind:key="option.id">
                            {{ option.name }}                            
                        </option>
                    </select>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Genus:</label>
                <div class="col-sm-8">
                    <select :disabled="true" class="form-select" v-model="genus_id" id="genus">
                        <option v-for="option in genus_list" :value="option.id" v-bind:key="option.id">
                            {{ option.name }}                            
                        </option>
                    </select>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Name Authority:</label>
                <div class="col-sm-8">
                    <input :disabled="true" type="text" class="form-control" id="name_authority" placeholder="" 
                    v-model="name_authority"/>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Name names comments:</label>
                <div class="col-sm-8">
                    <textarea :disabled="true" class="form-control" rows="3" id="comment" placeholder=""
                    v-model="name_comments"/>
                </div>
            </div>
        </FormSection>
        <!-- <FormSection :formCollapse="false" label="Distribution" :Index="distributionBody">
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Region:</label>
                <div class="col-sm-8">
                    <select :disabled="isReadOnly" class="form-select" @change="filterDistrict($event)" v-model="species_community.region_id">
                        <option v-for="option in region_list" :value="option.id" v-bind:key="option.id">
                            {{ option.name }}                            
                        </option>
                    </select>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">District:</label>
                <div class="col-sm-8">
                    <select :disabled="isReadOnly" class="form-select" v-model="species_community.district_id">
                        <option v-for="option in filtered_district_list" :value="option.id" v-bind:key="option.id">
                            {{ option.name }}                            
                        </option>
                    </select>

                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Number of Occurrences:</label>
                <div class="col-sm-6">
                    <input :disabled="isReadOnly" type="number" class="form-control" id="no_of_occurrences" placeholder="" v-model="species_community.distribution.number_of_occurrences"/>
                </div>
                <div class="col-sm-3">    
                    <input :disabled="isReadOnly" type="radio" value="true" 
                            class="noo_auto form-check-input" name="noo_auto" 
                            v-model="species_community.distribution.noo_auto">
                    <label>auto</label>
                    <input :disabled="isReadOnly" type="radio" value="false" 
                            class="noo_auto form-check-input" name="noo_auto" 
                            v-model="species_community.distribution.noo_auto">
                    <label>manual</label>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Extent of Occurrence (km2):</label>
                <div class="col-sm-6">
                    <input :disabled="isReadOnly" type="number" class="form-control" id="extent_of_occurrence" 
                    placeholder="" v-model="species_community.distribution.extent_of_occurrences"/>
                </div>
                <div class="col-sm-3">    
                    <input :disabled="isReadOnly" type="radio" value="true" 
                            class="eoo_auto form-check-input" name="eoo_auto" 
                            v-model="species_community.distribution.eoo_auto">
                    <label>auto</label>
                    <input :disabled="isReadOnly" type="radio" value="false" 
                            class="eoo_auto form-check-input" name="eoo_auto" 
                            v-model="species_community.distribution.eoo_auto">
                    <label>manual</label>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Area of Occupancy (km2):</label>
                <div class="col-sm-6">
                    <input :disabled="isReadOnly" type="number" class="form-control" id="area_of_occupancy_actual" placeholder="" 
                    v-model="species_community.distribution.area_of_occupancy_actual"/>
                </div>
                <div class="col-sm-3">    
                    <input :disabled="isReadOnly" type="radio" value="true" 
                            class="aoo_actual_auto form-check-input" name="aoo_actual_auto" 
                            v-model="species_community.distribution.aoo_actual_auto">
                    <label>auto</label>
                    <input :disabled="isReadOnly" type="radio" value="false" 
                            class="aoo_actual_auto form-check-input" name="aoo_actual_auto" 
                            v-model="species_community.distribution.aoo_actual_auto">
                    <label>manual</label>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Area of Occupancy<br>(2km x 2km):</label>
                <div class="col-sm-6">
                    <input :disabled="isReadOnly" type="number" class="form-control" id="area_of_occupany" placeholder="" 
                    v-model="species_community.distribution.area_of_occupancy"/>
                </div>
                <div class="col-sm-3">    
                    <input :disabled="isReadOnly" type="radio" value="true" 
                            class="aoo_auto form-check-input" name="aoo_auto" v-model="species_community.distribution.aoo_auto">
                    <label>auto</label>
                    <input :disabled="isReadOnly" type="radio" value="false" 
                            class="aoo_auto form-check-input" name="aoo_auto" v-model="species_community.distribution.aoo_auto">
                    <label>manual</label>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Number of IUCN Locations:</label>
                <div class="col-sm-8">
                    <input :disabled="isReadOnly" type="number" class="form-control" id="no_of_iucn_locations" 
                    placeholder="" v-model="species_community.distribution.number_of_iucn_locations"/>
                </div>
            </div>
        </FormSection> -->
        <FormSection :formCollapse="false" label="Conservation Attributes" :Index="conservationBody">
            <div class="row mb-3">
                <div class="row mb-3" v-for="(species, index) in original_species_combine_list">
                    <label for="" class="col-sm-3 control-label">{{ species.species_number }} Habitat/Growth Form:</label>
                    <div class="col-sm-8">
                        <textarea :disabled="true" type="text" class="form-control" placeholder="" 
                        v-model="species.conservation_attributes.habitat_growth_form"/>
                    </div>
                    <div class="col-sm-1">
                        <input class="form-check-input" type="checkbox" name="habitat_frm_chk" :id="'habitat_frm_chk'+species.id" 
                        @change="checkConservationInput('habitat_frm_chk','habitat_frm_chk'+species.id,'habitat_growth_form',species.conservation_attributes.habitat_growth_form)" />
                    </div>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Habitat/Growth Form:</label>
                <div class="col-sm-8">
                    <textarea :disabled="isReadOnly" type="text" class="form-control" 
                    id="habitat_new" placeholder="" v-model="species_community.conservation_attributes.habitat_growth_form" />
                </div>
            </div>
            
            <div v-show="!isFauna">
                <div class="row mb-3" v-for="(species, index) in original_species_combine_list">
                    <label for="" class="col-sm-3 control-label">{{ species.species_number }} Flowering Period:</label>
                    <div class="col-sm-8">
                        <select :disabled="true" class="form-select" 
                            v-model="species.conservation_attributes.flowering_period">
                            <option v-for="option in flowering_period_list" :value="option.id" v-bind:key="option.id">
                                {{ option.name }}                            
                            </option>
                        </select>
                    </div>
                    <div class="col-sm-1">
                        <input class="form-check-input" type="checkbox" name="flowering_prd_chk" :id="'flowering_prd_chk'+species.id" 
                        @change="checkConservationInput('flowering_prd_chk','flowering_prd_chk'+species.id,'flowering_period',species.conservation_attributes.flowering_period)" />
                    </div>
                </div>
            </div>
            <div class="row mb-3" v-show="!isFauna">
                <label for="" class="col-sm-3 control-label">Flowering Period:</label>
                <div class="col-sm-8">
                    <select :disabled="isReadOnly" class="form-select" 
                        v-model="species_community.conservation_attributes.flowering_period">
                        <option v-for="option in flowering_period_list" :value="option.id" v-bind:key="option.id">
                            {{ option.name }}                            
                        </option>
                    </select>
                </div>
            </div>

            <div v-show="!isFauna">
                <div class="row mb-3" v-for="(species, index) in original_species_combine_list">
                    <label for="" class="col-sm-3 control-label">{{ species.species_number }} Fruiting Period:</label>
                    <div class="col-sm-8">
                        <select :disabled="true" class="form-select" 
                            v-model="species.conservation_attributes.fruiting_period">
                            <option v-for="option in fruiting_period_list" :value="option.id" v-bind:key="option.id">
                                {{ option.name }}                            
                            </option>
                        </select>
                    </div>
                    <div class="col-sm-1">
                        <input class="form-check-input" type="checkbox" name="fruiting_prd_chk" :id="'fruiting_prd_chk'+species.id" 
                        @change="checkConservationInput('fruiting_prd_chk','fruiting_prd_chk'+species.id,'fruiting_period',species.conservation_attributes.fruiting_period)" />
                    </div>
                </div>
            </div>
            <div class="row mb-3" v-show="!isFauna">
                <label for="" class="col-sm-3 control-label">Fruiting Period:</label>
                <div class="col-sm-8">
                    <select :disabled="isReadOnly" class="form-select" 
                        v-model="species_community.conservation_attributes.fruiting_period">
                        <option v-for="option in fruiting_period_list" :value="option.id" v-bind:key="option.id">
                            {{ option.name }}                            
                        </option>
                    </select>
                </div>
            </div>

            <div v-show="!isFauna">
                <div class="row mb-3" v-for="(species, index) in original_species_combine_list">
                    <label for="" class="col-sm-3 control-label">{{ species.species_number }} Flora Recruitment Type:</label>
                    <div class="col-sm-8">
                        <select :disabled="true" class="form-select" 
                            v-model="species.conservation_attributes.flora_recruitment_type_id">
                            <option v-for="option in flora_recruitment_type_list" :value="option.id" v-bind:key="option.id">
                                {{ option.name }}                            
                            </option>
                        </select>
                    </div>
                    <div class="col-sm-1">
                        <input class="form-check-input" type="checkbox" name="flora_recruit_type_chk" :id="'flora_recruit_type_chk'+species.id" 
                        @change="checkConservationInput('flora_recruit_type_chk','flora_recruit_type_chk'+species.id,'flora_recruitment_type_id', species.conservation_attributes.flora_recruitment_type_id)" />
                    </div>
                </div>
            </div>
            <div class="row mb-3" v-show="!isFauna">
                <label for="" class="col-sm-3 control-label">Flora Recruitment Type:</label>
                <div class="col-sm-8">
                    <select :disabled="isReadOnly" class="form-select" 
                        v-model="species_community.conservation_attributes.flora_recruitment_type_id">
                        <option v-for="option in flora_recruitment_type_list" :value="option.id" v-bind:key="option.id">
                            {{ option.name }}                            
                        </option>
                    </select>
                </div>
            </div>

            <div v-show="!isFauna">
                <div class="row mb-3" v-for="(species, index) in original_species_combine_list">
                    <label for="" class="col-sm-3 control-label">{{ species.species_number }} Flora Recruitment Notes:</label>
                    <div class="col-sm-8">
                        <textarea :disabled="true" type="text" class="form-control" 
                        id="recruitment_notes" placeholder="" 
                        v-model="species.conservation_attributes.flora_recruitment_notes"/>
                    </div>
                    <div class="col-sm-1">
                        <input class="form-check-input" type="checkbox" name="flora_recruit_notes_chk" :id="'flora_recruit_notes_chk'+species.id" 
                        @change="checkConservationInput('flora_recruit_notes_chk','flora_recruit_notes_chk'+species.id,'flora_recruitment_notes', species.conservation_attributes.flora_recruitment_notes)" />
                    </div>
                </div>
            </div>
            <div class="row mb-3" v-show="!isFauna">
                <label for="" class="col-sm-3 control-label">Flora Recruitment Notes:</label>
                <div class="col-sm-8">
                    <textarea :disabled="isReadOnly" type="text" class="form-control" 
                    id="recruitment_notes" placeholder="" 
                    v-model="species_community.conservation_attributes.flora_recruitment_notes"/>
                </div>
            </div>

            <div v-show="!isFauna">
                <div class="row mb-3" v-for="(species, index) in original_species_combine_list">
                    <label for="" class="col-sm-3 control-label">{{ species.species_number }} Seed Viability and Germination Info:</label>
                    <!-- <div class="col-sm-8">
                        <select :disabled="true" class="form-select" 
                            v-model="species.conservation_attributes.seed_viability_germination_info_id">
                            <option v-for="option in seed_viability_germination_info_list" :value="option.id" v-bind:key="option.id">
                                {{ option.name }}                            
                            </option>
                        </select>
                    </div> -->
                    <div class="col-sm-8">
                        <textarea :disabled="true" type="text" class="form-control" placeholder="" 
                        v-model="species.conservation_attributes.seed_viability_germination_info"/>
                    </div>
                    <div class="col-sm-1">
                        <input class="form-check-input" type="checkbox" name="seed_viability_chk" :id="'seed_viability_chk'+species.id" 
                        @change="checkConservationInput('seed_viability_chk','seed_viability_chk'+species.id, 'seed_viability_germination_info', species.conservation_attributes.seed_viability_germination_info)" />
                    </div>
                </div>
            </div>
            <div class="row mb-3" v-show="!isFauna">
                <label for="" class="col-sm-3 control-label">Seed Viability and Germination Info:</label>
                <div class="col-sm-8">
                    <!-- <select :disabled="isReadOnly" class="form-select" 
                        v-model="species_community.conservation_attributes.seed_viability_germination_info_id">
                        <option v-for="option in seed_viability_germination_info_list" :value="option.id" v-bind:key="option.id">
                            {{ option.name }}                            
                        </option>
                    </select> -->
                    <textarea :disabled="isReadOnly" type="text" class="form-control" placeholder="" 
                        v-model="species_community.conservation_attributes.seed_viability_germination_info"/>
                </div>
            </div>

            <div v-show="!isFauna">
                <div class="row mb-3" v-for="(species, index) in original_species_combine_list">
                    <label for="" class="col-sm-3 control-label">{{ species.species_number }} Root Morphology:</label>
                    <div class="col-sm-8">
                        <select :disabled="true" class="form-select" 
                            v-model="species.conservation_attributes.root_morphology_id">
                            <option v-for="option in root_morphology_list" :value="option.id" v-bind:key="option.id">
                                {{ option.name }}                            
                            </option>
                        </select>
                    </div>
                    <div class="col-sm-1">
                        <input class="form-check-input" type="checkbox" name="root_morphology_chk" :id="'root_morphology_chk'+species.id" 
                        @change="checkConservationInput('root_morphology_chk','root_morphology_chk'+species.id,'root_morphology_id', species.conservation_attributes.root_morphology_id)" />
                    </div>
                </div>
            </div>
            <div class="row mb-3" v-show="!isFauna">
                <label for="" class="col-sm-3 control-label">Root Morphology:</label>
                <div class="col-sm-8">
                    <select :disabled="isReadOnly" class="form-select" 
                        v-model="species_community.conservation_attributes.root_morphology_id">
                        <option v-for="option in root_morphology_list" :value="option.id" v-bind:key="option.id">
                            {{ option.name }}                            
                        </option>
                    </select>
                </div>
            </div>

            <div v-show="!isFauna">
                <div class="row mb-3" v-for="(species, index) in original_species_combine_list">
                    <label for="" class="col-sm-3 control-label">{{ species.species_number }} Pollinator Information:</label>
                    <div class="col-sm-8">
                        <!-- <select :disabled="true" class="form-select" 
                            v-model="species.conservation_attributes.pollinator_information_id">
                            <option v-for="option in pollinator_info_list" :value="option.id" v-bind:key="option.id">
                                {{ option.name }}                            
                            </option>
                        </select> -->
                        <textarea :disabled="true" type="text" class="form-control" placeholder="" 
                        v-model="species.conservation_attributes.pollinator_information"/>
                    </div>
                    <div class="col-sm-1">
                        <input class="form-check-input" type="checkbox" name="pollinator_chk" :id="'pollinator_chk'+species.id" 
                        @change="checkConservationInput('pollinator_chk','pollinator_chk'+species.id,'pollinator_information', species.conservation_attributes.pollinator_information)" />
                    </div>
                </div>
            </div>
            <div class="row mb-3" v-show="!isFauna">
                <label for="" class="col-sm-3 control-label">Pollinator Information:</label>
                <div class="col-sm-8">
                    <!-- <select :disabled="isReadOnly" class="form-select" 
                        v-model="species_community.conservation_attributes.pollinator_information_id">
                        <option v-for="option in pollinator_info_list" :value="option.id" v-bind:key="option.id">
                            {{ option.name }}                            
                        </option>
                    </select> -->
                    <textarea :disabled="isReadOnly" type="text" class="form-control" placeholder="" 
                        v-model="species_community.conservation_attributes.pollinator_information"/>
                </div>
            </div>

            <div v-show="isFauna">
                <div class="row mb-3" v-for="(species, index) in original_species_combine_list">
                    <label for="" class="col-sm-3 control-label">{{ species.species_number }} Breeding Period:</label>
                    <div class="col-sm-8">
                        <select :disabled="true" class="form-select" 
                            v-model="species.conservation_attributes.breeding_period_id">
                            <option v-for="option in breeding_period_list" :value="option.id" v-bind:key="option.id">
                                {{ option.name }}                            
                            </option>
                        </select>
                    </div>
                    <div class="col-sm-1">
                        <input class="form-check-input" type="checkbox" name="breeding_prd_chk" :id="'breeding_prd_chk'+species.id" 
                        @change="checkConservationInput('breeding_prd_chk','breeding_prd_chk'+species.id,'breeding_period_id', species.conservation_attributes.breeding_period_id)" />
                    </div>
                </div>
            </div>
            <div class="row mb-3" v-show="isFauna">
                <label for="" class="col-sm-3 control-label">Breeding Period:</label>
                <div class="col-sm-8">
                    <select :disabled="isReadOnly" class="form-select" 
                        v-model="species_community.conservation_attributes.breeding_period_id">
                        <option v-for="option in breeding_period_list" :value="option.id" v-bind:key="option.id">
                            {{ option.name }}                            
                        </option>
                    </select>
                </div>
            </div>

            <div v-show="isFauna">
                <div class="row mb-3" v-for="(species, index) in original_species_combine_list">
                    <label for="" class="col-sm-3 control-label">{{ species.species_number }} Fauna Breeding:</label>
                    <div class="col-sm-8">
                        <!-- <div v-for="option in fauna_breeding_list">
                            <input :disabled="true" class='form-check-input' type="radio" v-bind:value="option.id" 
                                :id="'breeding_type_'+option.id" 
                                v-model="species.conservation_attributes.fauna_breeding_id">
                            <label :for="'breeding_type_'+option.id">{{ option.name }}</label>
                        </div> -->
                        <textarea :disabled="true" type="text" class="form-control" placeholder="" 
                        v-model="species.conservation_attributes.fauna_breeding"/>
                    </div>
                    <div class="col-sm-1">
                        <input class="form-check-input" type="checkbox" name="breeding_type_chk" :id="'breeding_type_chk'+species.id" 
                        @change="checkConservationInput('breeding_type_chk','breeding_type_chk'+species.id,'fauna_breeding', species.conservation_attributes.fauna_breeding)" />
                    </div>
                </div>
            </div>
            <div class="row mb-3" v-show="isFauna">
                <label for="" class="col-sm-3 control-label">Fauna Breeding:</label>
                <div class="col-sm-8">
                    <!-- <div v-for="option in fauna_breeding_list">
                        <input :disabled="isReadOnly" class='form-check-input' type="radio" v-bind:value="option.id" 
                            :id="'breeding_type_'+option.id" 
                            v-model="species_community.conservation_attributes.fauna_breeding_id">
                        <label :for="'breeding_type_'+option.id">{{ option.name }}</label>
                    </div> -->
                    <textarea :disabled="isReadOnly" type="text" class="form-control" placeholder="" 
                        v-model="species_community.conservation_attributes.fauna_breeding"/>
                </div>
            </div>

            <div v-show="isFauna">
                <div class="row mb-3" v-for="(species, index) in original_species_combine_list">
                    <label for="" class="col-sm-3 control-label">{{ species.species_number }} Fauna Reproductive capacity:</label>
                    <div class="col-sm-8">
                        <input :disabled="true" type="text" class="form-control" 
                        id="fauna_reproductive_capacity" placeholder="" 
                        v-model="species.conservation_attributes.fauna_reproductive_capacity"/>
                    </div>
                    <div class="col-sm-1">
                        <input class="form-check-input" type="checkbox" name="reproductive_cap_chk" :id="'reproductive_cap_chk'+species.id" 
                        @change="checkConservationInput('reproductive_cap_chk','reproductive_cap_chk'+species.id,'fauna_reproductive_capacity', species.conservation_attributes.fauna_reproductive_capacity)" />
                    </div>
                </div>
            </div>
            <div class="row mb-3" v-show="isFauna">
                <label for="" class="col-sm-3 control-label">Fauna Reproductive capacity:</label>
                <div class="col-sm-8">
                    <input :disabled="isReadOnly" type="text" class="form-control" 
                    id="fauna_reproductive_capacity" placeholder="" 
                    v-model="species_community.conservation_attributes.fauna_reproductive_capacity"/>
                </div>
            </div>

            <div>
                <div class="row mb-3" v-for="(species, index) in original_species_combine_list">
                    <label for="" class="col-sm-3 control-label">{{ species.species_number }} Time to Maturity:</label>
                    <div class="col-sm-8">
                        <input :disabled="true" type="number" class="form-control" 
                        id="time_to_maturity" placeholder="" 
                        v-model="species.conservation_attributes.time_to_maturity"/>
                    </div>
                    <div class="col-sm-1">
                        <!-- checkInput(checkbox_name,checkbox_id , v-model object attribute of this field) -->
                        <input class="form-check-input" type="checkbox" name="maturity_time_chk" :id="'maturity_time_chk'+species.id" 
                        @change="checkConservationInput('maturity_time_chk','maturity_time_chk'+species.id,'time_to_maturity', species.conservation_attributes.time_to_maturity)" />
                    </div>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Time to Maturity:</label>
                <div class="col-sm-8">
                    <input :disabled="isReadOnly" type="number" class="form-control" id="time_to_maturity" placeholder="" 
                    v-model="species_community.conservation_attributes.time_to_maturity"/>
                </div>
            </div>

            <div>
                <div class="row mb-3" v-for="(species, index) in original_species_combine_list">
                    <label for="" class="col-sm-3 control-label">{{ species.species_number }} Generation Length:</label>
                    <div class="col-sm-8">
                        <input :disabled="true" type="number" class="form-control" 
                        id="generation_length" placeholder="" 
                        v-model="species.conservation_attributes.generation_length"/>
                    </div>
                    <div class="col-sm-1">
                        <!-- checkInput(checkbox_name,checkbox_id , v-model object attribute of this field) -->
                        <input class="form-check-input" type="checkbox" name="generation_chk" :id="'generation_chk'+species.id" 
                        @change="checkConservationInput('generation_chk','generation_chk'+species.id,'generation_length', species.conservation_attributes.generation_length)" />
                    </div>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Generation Length:</label>
                <div class="col-sm-8">
                    <input :disabled="isReadOnly" type="number" class="form-control" id="generation_length" placeholder="" 
                    v-model="species_community.conservation_attributes.generation_length"/>
                </div>
            </div>

            <div>
                <div class="row mb-3" v-for="(species, index) in original_species_combine_list">
                    <label for="" class="col-sm-3 control-label">{{ species.species_number }} Average Lifespan:</label>
                    <div class="col-sm-8">
                        <input :disabled="true" type="number" class="form-control" 
                        id="average_lifespan" placeholder="" 
                        v-model="species.conservation_attributes.average_lifespan"/>
                    </div>
                    <div class="col-sm-1">
                        <!-- checkInput(checkbox_name,checkbox_id , v-model object attribute of this field) -->
                        <input class="form-check-input" type="checkbox" name="lifespan_chk" :id="'lifespan_chk'+species.id" 
                        @change="checkConservationInput('lifespan_chk','lifespan_chk'+species.id,'average_lifespan', species.conservation_attributes.average_lifespan)" />
                    </div>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Average Lifespan:</label>
                <div class="col-sm-8">
                    <input :disabled="isReadOnly" type="number" class="form-control" 
                    id="average_lifespan" placeholder="" 
                    v-model="species_community.conservation_attributes.average_lifespan"/>
                </div>
            </div>

            <div>
                <div class="row mb-3" v-for="(species, index) in original_species_combine_list">
                    <label for="" class="col-sm-3 control-label">{{ species.species_number }} Minimum Fire Interval:</label>
                    <div class="col-sm-8">
                        <input :disabled="true" type="text" class="form-control" 
                        id="minimum_fire_interval" placeholder="" 
                        v-model="species.conservation_attributes.minimum_fire_interval"/>
                    </div>
                    <div class="col-sm-1">
                        <!-- checkInput(checkbox_name, checkbox_id , v-model object attribute of this field) -->
                        <input class="form-check-input" type="checkbox" name="fire_interval_chk" :id="'fire_interval_chk'+species.id" 
                        @change="checkConservationInput('fire_interval_chk','fire_interval_chk'+species.id,'minimum_fire_interval', species.conservation_attributes.minimum_fire_interval)" />
                    </div>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Minimum Fire Interval:</label>
                <div class="col-sm-8">
                    <input :disabled="isReadOnly" type="text" class="form-control" 
                    id="minimum_fire_interval" placeholder="" 
                    v-model="species_community.conservation_attributes.minimum_fire_interval"/>
                </div>
            </div>

            <div>
                <div class="row mb-3" v-for="(species, index) in original_species_combine_list">
                    <label for="" class="col-sm-3 control-label">{{ species.species_number }} Response to Fire:</label>
                    <div class="col-sm-8">
                        <textarea :disabled="true" type="text" class="form-control" id="response_to_fire" placeholder="" v-model="species.conservation_attributes.response_to_fire"/>
                    </div>
                    <div class="col-sm-1">
                        <!-- checkInput(checkbox_name,checkbox_id , v-model object attribute of this field) -->
                        <input class="form-check-input" type="checkbox" name="fire_resp_chk" :id="'fire_resp_chk'+species.id" 
                        @change="checkConservationInput('fire_resp_chk','fire_resp_chk'+species.id,'response_to_fire', species.conservation_attributes.response_to_fire)" />
                    </div>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Response to Fire:</label>
                <div class="col-sm-8">
                    <textarea :disabled="isReadOnly" type="text" class="form-control" id="response_to_fire" placeholder="" v-model="species_community.conservation_attributes.response_to_fire"/>
                </div>
            </div>

            <div>
                <div class="row mb-3" v-for="(species, index) in original_species_combine_list">
                    <label for="" class="col-sm-3 control-label">{{ species.species_number }} Post Fire Habitat Interactions:</label>
                    <div class="col-sm-8">
                        <select :disabled="true" class="form-select" 
                            v-model="species.conservation_attributes.post_fire_habitat_interaction_id">
                            <option v-for="option in post_fire_habitatat_interactions_list" :value="option.id" v-bind:key="option.id">
                                {{ option.name }}                            
                            </option>
                        </select>
                    </div>
                    <div class="col-sm-1">
                        <!-- checkInput(checkbox_name,checkbox_id , v-model object attribute of this field) -->
                        <input class="form-check-input" type="checkbox" name="fire_habitat_chk" :id="'fire_habitat_chk'+species.id" 
                        @change="checkConservationInput('fire_habitat_chk','fire_habitat_chk'+species.id,'post_fire_habitat_interaction_id', species.conservation_attributes.post_fire_habitat_interaction_id)" />
                    </div>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Post Fire Habitat Interactions:</label>
                <div class="col-sm-8">
                    <select :disabled="isReadOnly" class="form-select" 
                        v-model="species_community.conservation_attributes.post_fire_habitat_interaction_id">
                        <option v-for="option in post_fire_habitatat_interactions_list" :value="option.id" v-bind:key="option.id">
                            {{ option.name }}                            
                        </option>
                    </select>
                </div>
            </div>

            <div>
                <div class="row mb-3" v-for="(species, index) in original_species_combine_list">
                    <label for="" class="col-sm-3 control-label">{{ species.species_number }} Response to Disturbance:</label>
                    <div class="col-sm-8">
                        <textarea :disabled="true" type="text" class="form-control" 
                        id="response_to_disturbance" placeholder="" 
                        v-model="species.conservation_attributes.response_to_disturbance"/>
                    </div>
                    <div class="col-sm-1">
                        <!-- checkInput(checkbox_name,checkbox_id , v-model object attribute of this field) -->
                        <input class="form-check-input" type="checkbox" name="disturbance_resp_chk" :id="'disturbance_resp_chk'+species.id" 
                        @change="checkConservationInput('disturbance_resp_chk','disturbance_resp_chk'+species.id,'response_to_disturbance', species.conservation_attributes.response_to_disturbance)" />
                    </div>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Response to Disturbance:</label>
                <div class="col-sm-8">
                    <textarea :disabled="isReadOnly" type="text" class="form-control" 
                    id="response_to_disturbance" placeholder="" 
                    v-model="species_community.conservation_attributes.response_to_disturbance"/>
                </div>
            </div>

            <div>
                <div class="row mb-3" v-for="(species, index) in original_species_combine_list">
                    <label for="" class="col-sm-3 control-label">{{ species.species_number }} Habitat:</label>
                    <div class="col-sm-8">
                        <textarea :disabled="true" type="text" class="form-control" id="habitat" 
                        placeholder="" v-model="species.conservation_attributes.habitat"/>
                    </div>
                    <div class="col-sm-1">
                        <!-- checkInput(checkbox_name,checkbox_id , v-model object attribute of this field) -->
                        <input class="form-check-input" type="checkbox" name="habitat_chk" :id="'habitat_chk'+species.id" 
                        @change="checkConservationInput('habitat_chk','habitat_chk'+species.id,'habitat', species.conservation_attributes.habitat)" />
                    </div>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Habitat:</label>
                <div class="col-sm-8">
                    <textarea :disabled="isReadOnly" type="text" class="form-control" id="habitat" 
                    placeholder="" v-model="species_community.conservation_attributes.habitat"/>
                </div>
            </div>

            <div>
                <div class="row mb-3" v-for="(species, index) in original_species_combine_list">
                    <label for="" class="col-sm-3 control-label">{{ species.species_number }} Hydrology:</label>
                    <div class="col-sm-8">
                        <textarea :disabled="true" type="text" class="form-control" id="hydrology" 
                        placeholder="" v-model="species.conservation_attributes.hydrology"/>
                    </div>
                    <div class="col-sm-1">
                        <!-- checkInput(checkbox_name,checkbox_id , v-model object attribute of this field) -->
                        <input class="form-check-input" type="checkbox" name="hydrology_chk" :id="'hydrology_chk'+species.id" 
                        @change="checkConservationInput('hydrology_chk','hydrology_chk'+species.id,'hydrology', species.conservation_attributes.hydrology)" />
                    </div>
                </div>
            </div>
            <div class="row mb-3" v-show="!isFauna">
                <label for="" class="col-sm-3 control-label">Hydrology:</label>
                <div class="col-sm-8">
                    <textarea :disabled="isReadOnly" type="text" class="form-control" id="hydrology" 
                    placeholder="" v-model="species_community.conservation_attributes.hydrology"/>
                </div>
            </div>

            <div v-show="isFauna">
                <div class="row mb-3" v-for="(species, index) in original_species_combine_list">
                    <label for="" class="col-sm-3 control-label">{{ species.species_number }} Diet and Food Source:</label>
                    <div class="col-sm-8">
                        <textarea :disabled="true" type="text" class="form-control" id="diet_food_source" 
                        placeholder="" v-model="species.conservation_attributes.diet_and_food_source"/>
                    </div>
                    <div class="col-sm-1">
                        <!-- checkInput(checkbox_name,checkbox_id , v-model object attribute of this field) -->
                        <input class="form-check-input" type="checkbox" name="diet_src_chk" :id="'diet_src_chk'+species.id" 
                        @change="checkConservationInput('diet_src_chk','diet_src_chk'+species.id,'diet_and_food_source', species.conservation_attributes.diet_and_food_source)" />
                    </div>
                </div>
            </div>
            <div class="row mb-3" v-show="isFauna">
                <label for="" class="col-sm-3 control-label">Diet and Food Source:</label>
                <div class="col-sm-8">
                    <textarea :disabled="isReadOnly" type="text" class="form-control" id="diet_food_source" 
                    placeholder="" v-model="species_community.conservation_attributes.diet_and_food_source"/>
                </div>
            </div>

            <div v-show="isFauna">
                <div class="row mb-3" v-for="(species, index) in original_species_combine_list">
                    <label for="" class="col-sm-3 control-label">{{ species.species_number }} Home Range:</label>
                    <div class="col-sm-8">
                        <textarea :disabled="true" type="text" class="form-control" id="home_range" 
                        placeholder="" v-model="species.conservation_attributes.home_range"/>
                    </div>
                    <div class="col-sm-1">
                        <!-- checkInput(checkbox_name,checkbox_id , v-model object attribute of this field) -->
                        <input class="form-check-input" type="checkbox" name="home_rng_chk" :id="'home_rng_chk'+species.id" 
                        @change="checkConservationInput('home_rng_chk','home_rng_chk'+species.id,'home_range', species.conservation_attributes.home_range)" />
                    </div>
                </div>
            </div>
            <div class="row mb-3" v-show="isFauna">
                <label for="" class="col-sm-3 control-label">Home Range:</label>
                <div class="col-sm-8">
                    <textarea :disabled="isReadOnly" type="text" class="form-control" id="home_range" 
                    placeholder="" v-model="species_community.conservation_attributes.home_range"/>
                </div>
            </div>

            <div>
                <div class="row mb-3" v-for="(species, index) in original_species_combine_list">
                    <label for="" class="col-sm-3 control-label">{{ species.species_number }} Research Requirements:</label>
                    <div class="col-sm-8">
                        <textarea :disabled="true" type="text" class="form-control" 
                        id="research_requirements" 
                        placeholder="" v-model="species.conservation_attributes.research_requirements"/>
                    </div>
                    <div class="col-sm-1">
                        <!-- checkInput(checkbox_name,checkbox_id , v-model object attribute of this field) -->
                        <input class="form-check-input" type="checkbox" name="research_req_chk" :id="'research_req_chk'+species.id" 
                        @change="checkConservationInput('research_req_chk','research_req_chk'+species.id,'research_requirements', species.conservation_attributes.research_requirements)" />
                    </div>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Research Requirements:</label>
                <div class="col-sm-8">
                    <textarea :disabled="isReadOnly" type="text" class="form-control" 
                    id="research_requirements" 
                    placeholder="" v-model="species_community.conservation_attributes.research_requirements"/>
                </div>
            </div>

            <div v-show="!isFauna">
                <div class="row mb-3" v-for="(species, index) in original_species_combine_list">
                    <label for="" class="col-sm-3 control-label">{{ species.species_number }} Response to Dieback:</label>
                    <div class="col-sm-8">
                        <textarea :disabled="true" type="text" class="form-control" 
                        id="response_to_dieback" 
                        placeholder="" v-model="species.conservation_attributes.response_to_dieback"/>
                    </div>
                    <div class="col-sm-1">
                        <!-- checkInput(checkbox_name,checkbox_id , v-model object attribute of this field) -->
                        <input class="form-check-input" type="checkbox" name="dieback_resp_chk" :id="'dieback_resp_chk'+species.id" 
                        @change="checkConservationInput('dieback_resp_chk','dieback_resp_chk'+species.id,'response_to_dieback', species.conservation_attributes.response_to_dieback)" />
                    </div>
                </div>
            </div>
            <div class="row mb-3" v-show="!isFauna">
                <label for="" class="col-sm-3 control-label">Response to Dieback:</label>
                <div class="col-sm-8">
                    <textarea :disabled="isReadOnly" type="text" class="form-control" 
                    id="response_to_dieback" 
                    placeholder="" v-model="species_community.conservation_attributes.response_to_dieback"/>
                </div>
            </div>

            <div>
                <div class="row mb-3" v-for="(species, index) in original_species_combine_list">
                    <label for="" class="col-sm-3 control-label">{{ species.species_number }} Other relevant diseases:</label>
                    <div class="col-sm-8">
                        <textarea :disabled="true" type="text" class="form-control" 
                        id="other_relevant_diseases" 
                        placeholder="" v-model="species.conservation_attributes.other_relevant_diseases"/>
                    </div>
                    <div class="col-sm-1">
                        <!-- checkInput(checkbox_name,checkbox_id , v-model object attribute of this field) -->
                        <input class="form-check-input" type="checkbox" name="disease_chk" :id="'disease_chk'+species.id" 
                        @change="checkConservationInput('disease_chk','disease_chk'+species.id,'other_relevant_diseases', species.conservation_attributes.other_relevant_diseases)" />
                    </div>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Other relevant diseases:</label>
                <div class="col-sm-8">
                    <textarea :disabled="isReadOnly" type="text" class="form-control" 
                    id="other_relevant_diseases" 
                    placeholder="" v-model="species_community.conservation_attributes.other_relevant_diseases"/>
                </div>
            </div>

        </FormSection>
        <FormSection :formCollapse="false" label="General" :Index="generalBody">
            <div>
                <div class="row mb-3" v-for="(species, index) in original_species_combine_list">
                    <label for="" class="col-sm-3 control-label">{{ species.species_number }} Department File Numbers:</label>
                    <div class="col-sm-8">
                        <input :disabled="true" type="text" class="form-control" id="department_file_numbers" placeholder="" v-model="species.distribution.department_file_numbers"/>
                    </div>
                    <div class="col-sm-1">
                        <!-- checkInput(checkbox_name,checkbox_id , v-model object attribute of this field) -->
                        <input class="form-check-input" type="checkbox" name="dept_file_chk" :id="'dept_file_chk'+species.id" 
                        @change="checkDistributionInput('dept_file_chk','dept_file_chk'+species.id,'department_file_numbers', species.distribution.department_file_numbers)" />
                    </div>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Department File Numbers:</label>
                <div class="col-sm-8">
                    <input :disabled="isReadOnly" type="text" class="form-control" id="department_file_numbers" placeholder="" v-model="species_community.distribution.department_file_numbers"/>
                </div>
            </div>

            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Last data curration date: </label>
                <div class="col-sm-8">
                    <input :disabled="isReadOnly" type="date" class="form-control" name="last_data_curration_date" 
                    ref="last_data_curration_date" @change="checkDate()" v-model="species_community.last_data_curration_date" />
                </div>
            </div>
            <div>
                <div class="row mb-3" v-for="(species, index) in original_species_combine_list">
                    <label for="" class="col-sm-3 control-label">{{ species.species_number }} Comment:</label>
                    <div class="col-sm-8">
                        <textarea :disabled="true" class="form-control" rows="3" id="comment" placeholder="" v-model="species.comment"/>
                    </div>
                    <div class="col-sm-1">
                        <!-- checkInput(checkbox_name,checkbox_id , v-model object attribute of this field, value) -->
                        <input class="form-check-input" type="checkbox" name="comment_chk" :id="'comment_chk'+species.id" @change="checkCommentInput('comment_chk','comment_chk'+species.id,'comment', species.comment)" />
                    </div>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Comment:</label>
                <div class="col-sm-8">
                    <textarea :disabled="isReadOnly" class="form-control" rows="3" id="comment" placeholder="" v-model="species_community.comment"/>
                </div>
            </div>
        </FormSection>
    </div>
</template>

<script>
import Vue from 'vue' ;
import FormSection from '@/components/forms/section_toggle.vue';
import {
  api_endpoints,
  helpers
}
from '@/utils/hooks'
export default {
        name: 'SpeciesCombineProfile',
        props:{
            species_community:{
                type: Object,
                required:true
            },
            original_species_combine_list:{
                type: Array,
                required:true
            },
        },
        data:function () {
            let vm = this;
            return{
                scientific_name_lookup: 'scientific_name_lookup' + vm.species_community.id,
                select_scientific_name: "select_scientific_name"+ vm.species_community.id,
                taxonBody: 'taxonBody' + vm._uid,
                distributionBody: 'distributionBody' + vm._uid,
                conservationBody: 'conservationBody' + vm._uid,
                generalBody: 'generalBody' + vm._uid,
                //---to show fields related to Fauna
                isFauna: vm.species_community.group_type==="fauna"?true:false,
                //----list of values dictionary
                taxon_names: [],
                species_profile_dict: {},
                //scientific_name_list: [],
                family_list: [],
                genus_list: [],
                phylo_group_list: [],
                region_list: [],
                district_list: [],
                filtered_district_list: [],
                //---conservatiuon attributes field lists
                flowering_period_list: [],
                fruiting_period_list: [],
                flora_recruitment_type_list: [],
                root_morphology_list: [],
                post_fire_habitatat_interactions_list: [],
                breeding_period_list: [],
                // to display the species Taxonomy selected details
                species_display: '',
                common_name: null,
                taxon_name_id: null,
                taxon_previous_name:null,
                phylogenetic_group: null,
                family_id: null,
                genus_id: null,
                name_authority: null,
                name_comments: null,
            }
        },
        components: {
            FormSection,
        },
        computed: {
            isReadOnly: function(){
                let action = this.$route.query.action;
                if(action === "edit" && this.species_community && this.species_community.user_edit_mode){
                    return false;
                }
                else{
                    return this.species_community.readonly;
                }
            },
        },
        watch:{
            "species_community.distribution.noo_auto": function(newVal) {
                let vm=this;
                var selectedValue = newVal;
                    if(selectedValue === "true"){
                        vm.species_community.distribution.number_of_occurrences=vm.species_community.distribution.cal_number_of_occurrences;
                    }
                    else{
                        vm.species_community.distribution.number_of_occurrences=null;
                    }
            },
            "species_community.distribution.eoo_auto": function(newVal) {
                let vm=this;
                var selectedValue = newVal;
                    if(selectedValue === "true"){
                        vm.species_community.distribution.extent_of_occurrences=vm.species_community.distribution.cal_extent_of_occurrences;
                    }
                    else{
                        vm.species_community.distribution.extent_of_occurrences=null;
                    }
            },
            "species_community.distribution.aoo_actual_auto": function(newVal) {
                let vm=this;
                var selectedValue = newVal;
                    if(selectedValue === "true"){
                        vm.species_community.distribution.area_of_occupancy_actual=vm.species_community.distribution.cal_area_of_occupancy_actual;
                    }
                    else{
                        vm.species_community.distribution.area_of_occupancy_actual=null;
                    }
            },
            "species_community.distribution.aoo_auto": function(newVal) {
                let vm=this;
                var selectedValue = newVal;
                    if(selectedValue === "true"){
                        vm.species_community.distribution.area_of_occupancy=vm.species_community.distribution.cal_area_of_occupancy;
                    }
                    else{
                        vm.species_community.distribution.area_of_occupancy=null;
                    }
            },
        },
        methods:{
            filterDistrict: function(event) {
                this.$nextTick(() => {
                    if(event){
                      this.species_community.district_id=null; //-----to remove the previous selection
                    }
                    this.filtered_district_list=[];
                    this.filtered_district_list=[{
                      id:null,
                      name:"",
                      region_id:null,
                    }];
                    //---filter districts as per region selected
                    for(let choice of this.district_list){
                        if(choice.region_id === this.species_community.region_id)
                        {
                          this.filtered_district_list.push(choice);
                        }
                    }
                });
            },
            checkDate: function(){
                let vm=this;
                if(vm.$refs.last_data_curration_date.value){
                    vm.species_community.last_data_curration_date = vm.$refs.last_data_curration_date.value;
                }
                else{
                    vm.species_community.last_data_curration_date=null;
                }
            },
            initialiseScientificNameLookup: function(){
                let vm = this;
                $(vm.$refs[vm.scientific_name_lookup]).select2({
                    minimumInputLength: 2,
                    dropdownParent: $("#"+vm.select_scientific_name),
                    "theme": "bootstrap-5",
                    allowClear: true,
                    placeholder:"Select Scientific Name",
                    ajax: {
                        url: api_endpoints.scientific_name_lookup,
                        dataType: 'json',
                        data: function(params) {
                            var query = {
                                term: params.term,
                                type: 'public',
                                group_type_id: vm.species_community.group_type_id,
                                taxon_details: true,
                                species_id: vm.species_community.id, // to filter species  current/non_current
                            }
                            return query;
                        },
                        // results: function (data, page) { // parse the results into the format expected by Select2.
                        //     // since we are using custom formatting functions we do not need to alter remote JSON data
                        //     return {results: data};
                        // },
                    },
                }).
                on("select2:select", function (e) {
                    var selected = $(e.currentTarget);
                    let data = e.params.data.id;
                    vm.species_community.taxonomy_id = data
                    vm.species_display = e.params.data.scientific_name;
                    vm.common_name = e.params.data.common_name;
                    vm.taxon_name_id = e.params.data.taxon_name_id;
                    vm.taxon_previous_name = e.params.data.taxon_previous_name;
                    vm.phylogenetic_group = e.params.data.phylogenetic_group;
                    vm.family_id = e.params.data.family_fk_id;
                    vm.genus_id = e.params.data.genus_id;
                    vm.name_authority = e.params.data.name_authority;
                    vm.name_comments = e.params.data.name_comments;
                    // vm.filterFloraScientificName = data;
                    // sessionStorage.setItem("filterFloraScientificNameText", e.params.data.text);
                }).
                on("select2:unselect",function (e) {
                    var selected = $(e.currentTarget);
                    vm.species_community.taxonomy_id = ''
                    vm.species_display = '';
                    vm.common_name = '';
                    vm.taxon_name_id = '';
                    vm.taxon_previous_name = '';
                    vm.phylogenetic_group = '';
                    vm.family_id = '';
                    vm.genus_id = '';
                    vm.name_authority = '';
                    vm.name_comments = '';
                }).
                on("select2:open",function (e) {
                    const searchField = $('[aria-controls="select2-'+vm.scientific_name_lookup+'-results"]')
                    // move focus to select2 field
                    searchField[0].focus();
                });
            },
            loadTaxonomydetails: function(){
                let vm=this;
                //console.log(vm.taxon_names);
                if(vm.species_community.taxonomy_details!=null){
                    vm.species_display = vm.species_community.taxonomy_details.scientific_name;
                    vm.common_name = vm.species_community.taxonomy_details.common_name;
                    vm.taxon_name_id = vm.species_community.taxonomy_details.taxon_name_id;
                    vm.taxon_previous_name = vm.species_community.taxonomy_details.taxon_previous_name;
                    vm.phylogenetic_group = vm.species_community.taxonomy_details.phylogenetic_group;
                    vm.family_id = vm.species_community.taxonomy_details.family_fk_id;
                    vm.genus_id = vm.species_community.taxonomy_details.genus_id;
                    vm.name_authority = vm.species_community.taxonomy_details.name_authority;
                    vm.name_comments = vm.species_community.taxonomy_details.name_comments;
                }
            },
            //--------on/off checkbox value to new species--------
            checkHabitatForm: function(){
                if($("#habitat_frm_chk"+this.species_community.id).is(':checked')== true) {
                    this.species_community.conservation_attributes.habitat_growth_form=this.species_original.conservation_attributes.habitat_growth_form;
                }else{
                    this.species_community.conservation_attributes.habitat_growth_form=null;
                }
            },
            checkConservationInput: function(chkbox_name,chkbox_id,obj_field,value){
                // if checkbox is checked copy value from original  species to new species
                if($("#"+chkbox_id).is(':checked')== true){
                    this.species_community.conservation_attributes[obj_field] = value;
                }else{
                    this.species_community.conservation_attributes[obj_field]=null;
                }
                //--- to select only one checkbox at a time in a group
                let chkbox_name_arr=document.getElementsByName(chkbox_name);
                for(var i=0; i<chkbox_name_arr.length; i++)
                {
                    if(chkbox_name_arr[i].id != chkbox_id)
                    {
                        chkbox_name_arr[i].checked = false;
                    }
                }
            },
            checkDistributionInput: function(chkbox_name,chkbox_id,obj_field,value){
                // if checkbox is checked copy value from original  species to new species
                if($("#"+chkbox_id).is(':checked')== true){
                    this.species_community.distribution[obj_field] = value;
                }else{
                    this.species_community.distribution[obj_field]=null;
                }
                //--- to select only one checkbox at a time in a group
                let chkbox_name_arr=document.getElementsByName(chkbox_name);
                for(var i=0; i<chkbox_name_arr.length; i++)
                {
                    if(chkbox_name_arr[i].id != chkbox_id)
                    {
                        chkbox_name_arr[i].checked = false;
                    }
                }
            },
            checkCommentInput: function(chkbox_name,chkbox_id,obj_field,value){
                // if checkbox is checked copy value from original  species to new species
                if($("#"+chkbox_id).is(':checked')== true){
                    this.species_community[obj_field] = value;
                }else{
                    this.species_community[obj_field]=null;
                }
                //--- to select only one checkbox at a time in a group
                let chkbox_name_arr=document.getElementsByName(chkbox_name);
                for(var i=0; i<chkbox_name_arr.length; i++)
                {
                    if(chkbox_name_arr[i].id != chkbox_id)
                    {
                        chkbox_name_arr[i].checked = false;
                    }
                }
            },
            //----------------------------------------------------------------
            eventListeners:function (){
            },
        },
        created: async function() {
            let vm=this;
            //----set the distribution field values if auto onload
            if(vm.species_community.distribution.noo_auto == true){
                vm.species_community.distribution.number_of_occurrences=vm.species_community.distribution.cal_number_of_occurrences;
            }
            if(vm.species_community.distribution.eoo_auto == true){
                vm.species_community.distribution.extent_of_occurrences=vm.species_community.distribution.cal_extent_of_occurrences;
            }
            if(vm.species_community.distribution.aoo_actual_auto == true){
                vm.species_community.distribution.area_of_occupancy_actual=vm.species_community.distribution.cal_area_of_occupancy_actual;
            }
            if(vm.species_community.distribution.aoo_auto == true){
                vm.species_community.distribution.area_of_occupancy=vm.species_community.distribution.cal_area_of_occupancy;
            }
            //--------get api taxon_names depending on flora/fauna
            let taxon_api_url=null;
            if(vm.isFauna){
                taxon_api_url=api_endpoints.taxonomy+'/fauna_taxon_names.json';
            }
            else{
                taxon_api_url=api_endpoints.taxonomy+'/flora_taxon_names.json';
            }
            vm.$http.get(taxon_api_url).then((response) => {
                vm.taxon_names = response.body;
                this.loadTaxonomydetails();
            });
            //------fetch list of values
            const res = await Vue.http.get('/api/species_profile_dict/');
            vm.species_profile_dict = res.body;
            vm.family_list = vm.species_profile_dict.family_list;
            vm.family_list.splice(0,0,
                {
                    id: null,
                    name: null,
                });
            vm.genus_list = vm.species_profile_dict.genus_list;
            vm.genus_list.splice(0,0,
                {
                    id: null,
                    name: null,
                });
            vm.phylo_group_list = vm.species_profile_dict.phylo_group_list;
            vm.phylo_group_list.splice(0,0,
                {
                    id: null,
                    name: null,
                });
            vm.flowering_period_list = vm.species_profile_dict.flowering_period_list;
            vm.flowering_period_list.splice(0,0,
                {
                    id: null,
                    name: null,
                });
            vm.fruiting_period_list = vm.species_profile_dict.fruiting_period_list;
            vm.fruiting_period_list.splice(0,0,
                {
                    id: null,
                    name: null,
                });
            vm.flora_recruitment_type_list = vm.species_profile_dict.flora_recruitment_type_list;
            vm.flora_recruitment_type_list.splice(0,0,
                {
                    id: null,
                    name: null,
                });
            vm.root_morphology_list = vm.species_profile_dict.root_morphology_list;
            vm.root_morphology_list.splice(0,0,
                {
                    id: null,
                    name: null,
                });
            vm.post_fire_habitatat_interactions_list = vm.species_profile_dict.post_fire_habitatat_interactions_list;
            vm.post_fire_habitatat_interactions_list.splice(0,0,
                {
                    id: null,
                    name: null,
                });
            vm.breeding_period_list = vm.species_profile_dict.breeding_period_list;
            vm.breeding_period_list.splice(0,0,
                {
                    id: null,
                    name: null,
                });
            const response = await Vue.http.get('/api/region_district_filter_dict/');
            vm.filterRegionDistrict= response.body;
            vm.region_list= vm.filterRegionDistrict.region_list;
            vm.district_list= vm.filterRegionDistrict.district_list;
            vm.region_list.splice(0,0,
            {
                id: null,
                name: null,
            });
            this.filterDistrict();
        },
        mounted: function(){
            let vm = this;
            //vm.eventListeners();
            vm.initialiseScientificNameLookup();
            vm.loadTaxonomydetails();
        }
    }
</script>

<style lang="css" scoped>
    fieldset.scheduler-border {
    border: 1px groove #ddd !important;
    padding: 0 1.4em 1.4em 1.4em !important;
    margin: 0 0 1.5em 0 !important;
    -webkit-box-shadow:  0px 0px 0px 0px #000;
            box-shadow:  0px 0px 0px 0px #000;
    }
    legend.scheduler-border {
    width:inherit; /* Or auto */
    padding:0 10px; /* To give a bit of padding on the left and right */
    border-bottom:none;
    }
    input[type=text], select {
        width: 100%;
    }
    input[type=number]{
        width: 50%;
    }
</style>

