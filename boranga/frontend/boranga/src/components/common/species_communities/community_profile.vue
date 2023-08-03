<template lang="html">
    <div id="community">
        <FormSection :formCollapse="false" label="Taxonomy" Index="taxonomy">
            <!-- <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Species:</label>
                <div class="col-sm-9">
                    <select style="width:100%;" class="form-select input-sm" :disabled="isReadOnly" multiple ref="species_select" v-model="species_community.species">
                        <option v-for="s in species_list" :value="s.id" :key="s.id">{{s.id}} - {{s.scientific_name}}</option>
                    </select>
                </div>
            </div> -->
            <!-- <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Community Name:</label>
                <div class="col-sm-9" :id="select_community_name">
                    <select :disabled="isReadOnly"
                        :id="community_name_lookup"  
                        :name="community_name_lookup"  
                        :ref="community_name_lookup" 
                        class="form-control" />
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label"></label>
                <div class="col-sm-9">
                    <textarea disabled class="form-control" rows="3" id="community_name_display" 
                    v-model="community_name_display"/>
                </div>
            </div> -->
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Community Name:</label>
                <div class="col-sm-9">
                    <textarea :disabled="isReadOnly" class="form-control" rows="1" id="community_name" placeholder=""
                    v-model="species_community.taxonomy_details.community_name"/>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Community ID:</label>
                <div class="col-sm-9">
                    <input :disabled="isReadOnly" type="text" class="form-control"
                    id="community_migrated_id" placeholder=""
                    v-model="species_community.taxonomy_details.community_migrated_id"/>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Community Description:</label>
                <div class="col-sm-9">
                    <textarea :disabled="isReadOnly" class="form-control" rows="2" id="community_description" placeholder=""
                    v-model="species_community.taxonomy_details.community_description"/>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Previous Name:</label>
                <div class="col-sm-9">
                    <textarea :disabled="isReadOnly" class="form-control" id="community_previous_name" placeholder=""
                    v-model="species_community.taxonomy_details.previous_name"/>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Name Authority:</label>
                <div class="col-sm-9">
                    <input :disabled="isReadOnly" type="text" class="form-control" id="name_authority" placeholder="" 
                    v-model="species_community.taxonomy_details.name_authority"/>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Name Comments:</label>
                <div class="col-sm-9">
                    <textarea :disabled="isReadOnly" class="form-control" id="community_comment" placeholder=""
                    v-model="species_community.taxonomy_details.name_comments"/>
                </div>
            </div>
        </FormSection>
        <FormSection :formCollapse="false" label="Distribution" Index="distribution">
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Region:</label>
                <div class="col-sm-9">
                    <select :disabled="isReadOnly" class="form-select" @change="filterDistrict($event)" v-model="species_community.region_id" placeholder="Select Region">
                        <option v-for="option in region_list" :value="option.id" v-bind:key="option.id">
                            {{ option.name }}                            
                        </option>
                    </select>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">District:</label>
                <div class="col-sm-9">
                    <select :disabled="isReadOnly" class="form-select" v-model="species_community.district_id" placeholder="Select District">
                        <option v-for="option in filtered_district_list" :value="option.id" v-bind:key="option.id">
                            {{ option.name }}                            
                        </option>
                    </select>
                </div>
            </div>
        <!-- </FormSection>
        <FormSection :formCollapse="false" label="Ecological Communities" Index="ecological_communities"> -->
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
                <label for="" class="col-sm-3 control-label">Area of Occupancy(km2):</label>
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
                <label for="" class="col-sm-3 control-label">Number of IUCN Locations:</label>
                <div class="col-sm-9">
                    <input :disabled="isReadOnly" type="number" class="form-control" id="no_of_iucn_locations"
                    placeholder="" v-model="species_community.distribution.number_of_iucn_locations"/>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Number of IUCN Sub-populations:</label>
                <div class="col-sm-9">
                    <input :disabled="isReadOnly" type="number" class="form-control" id="number_of_iucn_subpopulations"
                    placeholder="" v-model="species_community.distribution.number_of_iucn_subpopulations"/>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Community Original Area (ha):</label>
                <div class="col-sm-9">
                    <input :disabled="isReadOnly" type="number" class="form-control" id="community_original_area" placeholder="" v-model="species_community.distribution.community_original_area"/>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Community Original Area (+/- ha) Accuracy:</label>
                <div class="col-sm-9">
                    <input :disabled="isReadOnly" type="number" class="form-control" id="community_original_area_accuracy" placeholder=""
                    v-model="species_community.distribution.community_original_area_accuracy"/>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Community Original Area (ha) Reference:</label>
                <div class="col-sm-9">
                    <input :disabled="isReadOnly" type="text" class="form-control" id="community_original_area_reference" placeholder=""
                    v-model="species_community.distribution.community_original_area_reference"/>
                </div>
            </div>
        </FormSection>
        <FormSection :formCollapse="false" label="Conservation Attributes" Index="conservation_attributes">
            <!-- <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Habitat/Growth Form:</label>
                <div class="col-sm-9">
                    <textarea :disabled="isReadOnly" type="text" class="form-control"
                    id="habitat_growth_form" placeholder="" 
                    v-model="species_community.conservation_attributes.habitat_growth_form"/>
                </div>
            </div> -->
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Pollinator Information:</label>
                <div class="col-sm-9">
                    <!-- <select :disabled="isReadOnly" class="form-select"
                        v-model="species_community.conservation_attributes.pollinator_information_id">
                        <option v-for="option in pollinator_info_list" :value="option.id" v-bind:key="option.id">
                            {{ option.name }}                            
                        </option>
                    </select> -->
                    <textarea :disabled="isReadOnly" type="text" class="form-control"
                    id="pollinator_info" placeholder="" 
                    v-model="species_community.conservation_attributes.pollinator_information"/>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Minimum Fire Interval:</label>
                <div class="col-sm-9">
                    <input :disabled="isReadOnly" type="text" class="form-control"
                    id="minimum_fire_interval" placeholder="" 
                    v-model="species_community.conservation_attributes.minimum_fire_interval"/>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Response to Fire:</label>
                <div class="col-sm-9">
                    <textarea :disabled="isReadOnly" type="text" class="form-control" id="response_to_fire" placeholder="" v-model="species_community.conservation_attributes.response_to_fire"/>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Post Fire Habitat Interactions:</label>
                <div class="col-sm-9">
                    <select :disabled="isReadOnly" class="form-select"
                        v-model="species_community.conservation_attributes.post_fire_habitat_interaction_id">
                        <option v-for="option in post_fire_habitatat_interactions_list" :value="option.id" v-bind:key="option.id">
                            {{ option.name }}                            
                        </option>
                    </select>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Hydrology:</label>
                <div class="col-sm-9">
                    <textarea :disabled="isReadOnly" type="text" class="form-control" id="hydrology" 
                    placeholder="" v-model="species_community.conservation_attributes.hydrology"/>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Ecological Community and Biological Information:
                </label>
                <div class="col-sm-9">
                    <textarea :disabled="isReadOnly" type="text" class="form-control" 
                    id="ecological_biological_information" placeholder="" 
                    v-model="species_community.conservation_attributes.ecological_and_biological_information"/>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Research Requirements:</label>
                <div class="col-sm-9">
                    <textarea :disabled="isReadOnly" type="text" class="form-control" 
                    id="research_requirements" 
                    placeholder="" v-model="species_community.conservation_attributes.research_requirements"/>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Response to Dieback:</label>
                <div class="col-sm-9">
                    <textarea :disabled="isReadOnly" type="text" class="form-control" 
                    id="response_to_dieback" 
                    placeholder="" v-model="species_community.conservation_attributes.response_to_dieback"/>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Other relevant diseases:</label>
                <div class="col-sm-9">
                    <textarea :disabled="isReadOnly" type="text" class="form-control" 
                    id="other_relevant_diseases" 
                    placeholder="" v-model="species_community.conservation_attributes.other_relevant_diseases"/>
                </div>
            </div>
        </FormSection>
        <FormSection :formCollapse="false" label="General" Index="general">
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Department File Numbers:</label>
                <div class="col-sm-9">
                    <input :disabled="isReadOnly" type="text" class="form-control" id="department_file_numbers" placeholder="" v-model="species_community.distribution.department_file_numbers"/>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Last data curration date: </label>
                <div class="col-sm-9">
                     <input :disabled="isReadOnly" type="date" class="form-control" name="last_data_curration_date" 
                    ref="last_data_curration_date" @change="checkDate()" v-model="species_community.last_data_curration_date" />
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Comment:</label>
                <div class="col-sm-9">
                    <textarea :disabled="isReadOnly" class="form-control" rows="3" id="comment" placeholder=""
                    v-model="species_community.comment"/>
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
// require("select2/dist/css/select2.min.css");
// require("select2-bootstrap-5-theme/dist/select2-bootstrap-5-theme.min.css")

export default {
        name: 'Community',
        props:{
            species_community:{
                type: Object,
                required:true
            },
        },
        data:function () {
            let vm = this;
            return{
                // community_name_lookup: 'community_name_lookup' + vm._uid,
                // select_community_name: "select_community_name"+ vm._uid,
                species_list: [],
                // taxon_names: [],
                community_profile_dict: {},
                post_fire_habitatat_interactions_list: [],
                region_list: [],
                district_list: [],
                filtered_district_list: [],
                // community_name_display:'',
                // community_migrated_id: null,
                // community_description: null,
                // previous_name:null,
                // name_authority: null,
                // name_comments: null,
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
            // initialiseCommunityNameLookup: function(){
            //     let vm = this;
            //     $(vm.$refs[vm.community_name_lookup]).select2({
            //         minimumInputLength: 2,
            //         dropdownParent: $("#"+vm.select_community_name),
            //         "theme": "bootstrap-5",
            //         allowClear: true,
            //         placeholder:"Select Community Name",
            //         ajax: {
            //             url: api_endpoints.community_name_lookup,
            //             dataType: 'json',
            //             data: function(params) {
            //                 var query = {
            //                     term: params.term,
            //                     type: 'public',
            //                     taxon_details: true,
            //                 }
            //                 return query;
            //             },
            //             // results: function (data, page) { // parse the results into the format expected by Select2.
            //             //     // since we are using custom formatting functions we do not need to alter remote JSON data
            //             //     return {results: data};
            //             // },
            //         },
            //     }).
            //     on("select2:select", function (e) {
            //         var selected = $(e.currentTarget);
            //         let data = e.params.data.id;
            //         vm.species_community.taxonomy_id = data
            //         vm.community_name_display = e.params.data.community_name;
            //         vm.community_migrated_id = e.params.data.community_migrated_id;
            //         vm.community_description = e.params.data.community_description;
            //         vm.previous_name = e.params.data.previous_name;
            //         vm.name_authority = e.params.data.name_authority;
            //         vm.name_comments = e.params.data.name_comments;
            //         // vm.filterFloraScientificName = data;
            //         // sessionStorage.setItem("filterFloraScientificNameText", e.params.data.text);
            //     }).
            //     on("select2:unselect",function (e) {
            //         var selected = $(e.currentTarget);
            //         vm.species_community.taxonomy_id = ''
            //         vm.community_name_display = '';
            //         vm.community_migrated_id = '';
            //         vm.community_description = '';
            //         vm.previous_name = '';
            //         vm.name_authority = '';
            //         vm.name_comments = '';
            //     }).
            //     on("select2:open",function (e) {
            //         const searchField = $('[aria-controls="select2-'+vm.community_name_lookup+'-results"]')
            //         // move focus to select2 field
            //         searchField[0].focus();
            //     });
            // },
            // loadTaxonomydetails: function(){
            //     let vm=this;
            //     if(vm.species_community.taxonomy_details!=null){
            //         var newOption = new Option(vm.species_community.taxonomy_details.community_name, vm.species_community.taxonomy_id, false, true);
            //         // newOption.setAttribute('data-select2-id', '2');
            //         $('#'+ vm.community_name_lookup).append(newOption);
            //         vm.community_name_display = vm.species_community.taxonomy_details.community_name;
            //         vm.community_migrated_id = vm.species_community.taxonomy_details.community_migrated_id;
            //         vm.community_description = vm.species_community.taxonomy_details.community_description;
            //         vm.previous_name = vm.species_community.taxonomy_details.previous_name;
            //         vm.name_authority = vm.species_community.taxonomy_details.name_authority;
            //         vm.name_comments = vm.species_community.taxonomy_details.name_comments;
            //     }
            // },
            eventListeners:function (){
                let vm=this;

                var date= new Date()
                var today= new Date(date.getFullYear(), date.getMonth(), date.getDate());
            },
        },
        created: async function() {
            let vm = this;
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
            //-----fetch species_list
            // const res = await Vue.http.get('/api/species/species_list.json');
            // vm.species_list= res.body.data;
            //--------get api taxon_names
            // vm.$http.get(api_endpoints.community_taxonomy+'/taxon_names.json').then((response) => {
            //     vm.taxon_names = response.body;
            // });
            //------fetch list of values
            const res_obj = await Vue.http.get('/api/community_profile_dict/');
            vm.community_profile_dict = res_obj.body;
            vm.post_fire_habitatat_interactions_list = vm.community_profile_dict.post_fire_habitatat_interactions_list;
            vm.post_fire_habitatat_interactions_list.splice(0,0,
                {
                    id: null,
                    name: null,
                });
            const response = await Vue.http.get('/api/region_district_filter_dict/');
            vm.filterRegionDistrict= response.body;
            vm.region_list = vm.filterRegionDistrict.region_list;
            vm.district_list= vm.filterRegionDistrict.district_list;
            vm.region_list.splice(0,0,
            {
                id: null,
                name: null,
            });
            this.filterDistrict();
            // this.loadTaxonomydetails();
        },
        mounted: function(){
            let vm = this;
            // vm.initialiseCommunityNameLookup();
            // vm.loadTaxonomydetails();
            // Initialise select2 for Species
            // $(vm.$refs.species_select).select2({
            //     "theme": "bootstrap-5",
            //     allowClear: true,
            //     placeholder:"Select Species",
            //     multiple: true,
            // }).
            // on("select2:select",function (e) {
            //     var selected = $(e.currentTarget);
            //     vm.species_community.species = selected.val();
            // }).
            // on("select2:unselect",function (e) {
            //     var selected = $(e.currentTarget);
            //     vm.species_community.species = selected.val();
            // });
        }
    }
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
        padding: 0.375rem 2.25rem 0.375rem 0.75rem;
    }
</style>

