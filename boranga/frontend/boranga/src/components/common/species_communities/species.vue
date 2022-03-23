<template lang="html">
<!-- <div class="row" id="userInfo">
    <div class="col-sm-12">
        <div class="panel panel-default">
            <div class="panel-heading">
                <h3 class="panel-title">Distribution <small></small>
                <a class="panelClicker" :href="'#'+distributionBody" data-toggle="collapse"  data-parent="#userInfo" expanded="true" :aria-controls="distributionBody">
                <span class="glyphicon glyphicon-chevron-up pull-right "></span>
                </a>
                </h3>
            </div>
            <div class="panel-body collapse in" :id="distributionBody">
                <div class="" >
                    <div class="form-horizontal col-sm-12 borderDecoration">
                        <div class="form-group">
                            <div class="row">
                                <div class="col-sm-3">
                                    <label class="control-label pull-left"  for="Name">Some Species data</label>
                                </div>
                                <div class="col-sm-9" style="margin-bottom: 5px; width:53% !important">
                                    <input type="text" class="form-control" name="Some species data" placeholder="" :disabled="proposal.readonly" v-model="some_species_data">
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>    
        </div>
        <div class="panel panel-default">
            <div class="panel-heading">
                <h3 class="panel-title">Taxonomy<small></small>
                <a class="panelClicker" :href="'#'+taxonomyBody" data-toggle="collapse"  data-parent="#userInfo" expanded="true" :aria-controls="taxonomyBody">
                <span class="glyphicon glyphicon-chevron-up pull-right "></span>
                </a>
                </h3>
            </div>
            <div class="panel-body collapse in" :id="taxonomyBody">
                <div class="" >               
                    <div class="form-horizontal col-sm-12 borderDecoration">
                        <div class="form-group">
                            <div class="row">
                                <div class="col-sm-3">
                                    <label class="control-label pull-left"  for="previous_name">Previous Name:</label>
                                </div>
                                <div class="col-sm-6" style="margin-bottom: 5px; width:53% !important">
                                    <input type="text" class="form-control" id="previous_name" name="previous_name" placeholder="" :disabled="proposal.readonly" v-model="some_species_data">
                                </div>

                                <div class="col-sm-3">
                                    <label class="control-label pull-left"  for="common_name">Common Name:</label>
                                </div>
                                <div class="col-sm-6" style="margin-bottom: 5px; width:53% !important">
                                    <input type="text" class="form-control" id="common_name" name="common_name" placeholder="" :disabled="proposal.readonly" v-model="some_species_data">
                                </div>
                            </div>
                            <div class="row">
                                <div class="col-sm-3">
                                    <label class="control-label pull-left"  for="family">Family:</label>
                                </div>
                                <div class="col-sm-9" style="margin-bottom: 5px; width:53% !important">
                                    <input type="text" class="form-control" id="family" name="family" placeholder="" :disabled="proposal.readonly" v-model="some_species_data">
                                </div>
                            </div>
                            <div class="row">
                                <div class="col-sm-3">
                                    <label class="control-label pull-left"  for="genus">Genus:</label>
                                </div>
                                <div class="col-sm-9" style="margin-bottom: 5px; width:53% !important">
                                    <input type="text" class="form-control" id="genus" name="genus" placeholder="" :disabled="proposal.readonly" v-model="some_species_data">
                                </div>
                            </div>
                            <div class="row">
                                <div class="col-sm-3">
                                    <label class="control-label pull-left"  for="phylogenetic_group">Phylogenetic Group:</label>
                                </div>
                                <div class="col-sm-9" style="margin-bottom: 5px; width:53% !important">
                                    <input type="text" class="form-control" id="phylogenetic_group" name="phylogenetic_group" placeholder="" :disabled="proposal.readonly" v-model="some_species_data">
                                </div>
                            </div>
                            <div class="row">
                                <div class="col-sm-3">
                                    <label class="control-label pull-left"  for="name_authority">Name Authority:</label>
                                </div>
                                <div class="col-sm-9" style="margin-bottom: 5px; width:53% !important">
                                    <input type="text" class="form-control" id="name_authority" name="name_authority" placeholder="" :disabled="proposal.readonly" v-model="some_species_data">
                                </div>
                            </div>
                            <div class="row">
                                <div class="col-sm-3">
                                    <label class="control-label pull-left"  for="community_description">Community Description:</label>
                                </div>
                                <div class="col-sm-9" style="margin-bottom: 5px; width:53% !important">
                                    <input type="textarea" class="form-control" id="community_description" name="community_description" placeholder="" :disabled="proposal.readonly" v-model="some_species_data">
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>    
        </div>
        <div class="panel panel-default">
            <div class="panel-heading">
                <h3 class="panel-title">Conservation Attributes <small></small>
                <a class="panelClicker" :href="'#'+conservationAttributesBody" data-toggle="collapse"  data-parent="#userInfo" expanded="true" :aria-controls="conservationAttributesBody">
                <span class="glyphicon glyphicon-chevron-up pull-right "></span>
                </a>
                </h3>
            </div>
            <div class="panel-body collapse in" :id="conservationAttributesBody">
                <div class="" >
                    <div class="form-horizontal col-sm-12 borderDecoration">
                        <div class="form-group">
                            <div class="row">
                                <div class="col-sm-3">
                                    <label class="control-label pull-left"  for="Name">Some Species data</label>
                                </div>
                                <div class="col-sm-9" style="margin-bottom: 5px; width:53% !important">
                                    <input type="text" class="form-control" name="Some species data" placeholder="" :disabled="proposal.readonly" v-model="some_species_data">
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>    
        </div>
        <div class="panel panel-default">
            <div class="panel-heading">
                <h3 class="panel-title">Conservation Status <small></small>
                <a class="panelClicker" :href="'#'+conservationStatusBody" data-toggle="collapse"  data-parent="#userInfo" expanded="true" :aria-controls="conservationStatusBody">
                <span class="glyphicon glyphicon-chevron-up pull-right "></span>
                </a>
                </h3>
            </div>
            <div class="panel-body collapse in" :id="conservationStatusBody">
                <div class="" >
                    <div class="form-horizontal col-sm-12 borderDecoration">
                        <div class="form-group">
                            <div class="row">
                                <div class="col-sm-3">
                                    <label class="control-label pull-left"  for="Name">Some Species data</label>
                                </div>
                                <div class="col-sm-9" style="margin-bottom: 5px; width:53% !important">
                                    <input type="text" class="form-control" name="Some species data" placeholder="" :disabled="proposal.readonly" v-model="some_species_data">
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>    
        </div>
    </div>

</div> -->
    <div id="species">
        <FormSection label="Distribution" Index="distribution">
            <div class="row form-group">
                <label for="" class="col-sm-3 control-label">Department File Numbers:</label>
                <div class="col-sm-9">
                    <input :disabled="proposal.readonly" type="text" class="form-control" id="department_file_numbers" placeholder="" v-model="some_species_data"/>
                </div>
            </div>
        </FormSection>
        <FormSection label="Taxonomy" Index="taxonomy">
            <div class="row form-group">
                <label for="" class="col-sm-3 control-label">Taxon Id:</label>
                <div class="col-sm-9">
                    <input :disabled="proposal.readonly" type="text" class="form-control" id="taxon_id" placeholder="" v-model="some_species_data"/>
                </div>
            </div>
            <div class="row form-group">
                <label for="" class="col-sm-3 control-label">Previous Name:</label>
                <div class="col-sm-9">
                    <input :disabled="proposal.readonly" type="text" class="form-control" id="previous_name" placeholder="" v-model="some_species_data"/>
                </div>
            </div>
            <div class="row form-group">
                <label for="" class="col-sm-3 control-label">Common Name:</label>
                <div class="col-sm-9">
                    <input :disabled="proposal.readonly" type="text" class="form-control" id="common_name" placeholder="" v-model="some_species_data"/>
                </div>
            </div>
            <div class="row form-group">
                <label for="" class="col-sm-3 control-label">Family:</label>
                <div class="col-sm-9">
                    <input :disabled="proposal.readonly" type="text" class="form-control" id="family" placeholder="" v-model="some_species_data"/>
                </div>
            </div>
            <div class="row form-group">
                <label for="" class="col-sm-3 control-label">Genus:</label>
                <div class="col-sm-9">
                    <input :disabled="proposal.readonly" type="text" class="form-control" id="genus" placeholder="" v-model="some_species_data"/>
                </div>
            </div>
            <div class="row form-group">
                <label for="" class="col-sm-3 control-label">Phylogenetic Group:</label>
                <div class="col-sm-9">
                    <input :disabled="proposal.readonly" type="text" class="form-control" id="phylogenetic_group" placeholder="" v-model="some_species_data"/>
                </div>
            </div>
            <div class="row form-group">
                <label for="" class="col-sm-3 control-label">Name Authority:</label>
                <div class="col-sm-9">
                    <input :disabled="proposal.readonly" type="text" class="form-control" id="name_authority" placeholder="" v-model="some_species_data"/>
                </div>
            </div>
             <div class="row form-group">
                <label for="" class="col-sm-3 control-label">Community Id:</label>
                <div class="col-sm-9">
                    <input :disabled="proposal.readonly" type="text" class="form-control" id="community_id" placeholder="" v-model="some_species_data"/>
                </div>
            </div>
             <div class="row form-group">
                <label for="" class="col-sm-3 control-label">Community Number:</label>
                <div class="col-sm-9">
                    <input :disabled="proposal.readonly" type="text" class="form-control" id="community_number" placeholder="" v-model="some_species_data"/>
                </div>
            </div>
            <div class="row form-group">
                <label for="" class="col-sm-3 control-label">Community Description:</label>
                <div class="col-sm-9">
                    <input :disabled="proposal.readonly" type="textarea" style="width: 70%;" class="form-control" id="community_description" placeholder="" v-model="some_species_data"/>
                </div>
            </div>
        </FormSection>
        <FormSection label="Conservation Attributes" Index="conservation_attributes">
            <div class="row form-group">
                <label for="" class="col-sm-3 control-label">General Management Advice:</label>
                <div class="col-sm-9">
                    <input :disabled="proposal.readonly" type="text" class="form-control" id="general_management_advice" placeholder="" v-model="some_species_data"/>
                </div>
            </div>
        </FormSection>
        <FormSection label="Conservation Status" Index="conservation_status">
            <div class="row form-group">
                
            </div>
        </FormSection>
    </div>
</template>

<script>
import Vue from 'vue' 
import FormSection from '@/components/forms/section_toggle.vue'
import {
  api_endpoints,
  helpers
}
from '@/utils/hooks'
export default {
        props:{
            proposal:{
                type: Object,
                required:true
            }
        },
        data:function () {
            let vm = this;
            return{
                distributionBody: 'distributionBody'+vm._uid,
                taxonomyBody: 'taxonomyBody'+vm._uid,
                conservationAttributesBody: 'conservationAttributesBody'+vm._uid,
                conservationStatusBody: 'conservationStatusBody'+vm._uid,
                values:null,
                accreditation_choices:[],
                accreditation_type:[],
                selected_accreditations:[],
                licence_period_choices:[],
                mooring: [''],
                global_settings:[],
                //mooring:[{'value':''}],
                datepickerOptions:{
                format: 'DD/MM/YYYY',
                showClear:true,
                useCurrent:false,
                keepInvalid:true,
                allowInputToggle:true,
                some_species_data: null,
            },
            }
        },
        components: {
            FormSection,
        },
        computed: {
            deed_poll_url: function(){
                let vm=this;
                if(vm.global_settings){
                    for(var i=0; i<vm.global_settings.length; i++){
                        if(vm.global_settings[i].key=='deed_poll'){
                            return vm.global_settings[i].value;
                        }
                    }
                }
                return '';
            },
            credit_facility_link: function(){
                let vm=this;
                if(vm.global_settings){
                    for(var i=0; i<vm.global_settings.length; i++){
                        if(vm.global_settings[i].key=='credit_facility_link'){
                            return vm.global_settings[i].value;
                        }
                    }
                }
                return '';
            }
        },
        watch:{
            
            accreditation_type: function(){
                this.proposal.other_details.accreditation_type=this.accreditation_type.key;
            },
        },
        methods:{
            handleRadioChange: function(e){
                    if(e.target.value=="true"){
                        console.log(e.target.value);
                        $('#show_docket').removeClass('hidden')
                    }
                    else{
                        $('#show_docket').addClass('hidden')
                    }
                
            },
            handleSelectionChange: function(e){
                    if(e.target.value=="true"){
                        console.log(e.target.value);
                        $('#show_credit_link').removeClass('hidden')
                    }
                    else{
                        $('#show_credit_link').addClass('hidden')
                    }
                
            },
            showDockteNumber: function(){
                let vm=this;
                if(vm.proposal && vm.proposal.other_details.credit_docket_books){
                    var input = this.$refs.docket_books_yes;
                    var e = document.createEvent('HTMLEvents');
                    e.initEvent('change', true, true);
                    var disabledStatus = input.disabled;
                    try {
                        /* Firefox will not fire events for disabled widgets, so (temporarily) enabling them */
                        if(disabledStatus) {
                            input.disabled = false;
                        }
                        input.dispatchEvent(e);
                    } finally {
                        if(disabledStatus) {
                            input.disabled = true;
                        }
                    }
                }
            },
            showCreditFacilityLink: function(){
                let vm=this;
                if(vm.proposal && vm.proposal.other_details.credit_fees){
                    var input = this.$refs.credit_fees_yes;
                    var e = document.createEvent('HTMLEvents');
                    e.initEvent('change', true, true);
                    var disabledStatus = input.disabled;
                    try {
                        /* Firefox will not fire events for disabled widgets, so (temporarily) enabling them */
                        if(disabledStatus) {
                            input.disabled = false;
                        }
                        input.dispatchEvent(e);
                    } finally {
                        if(disabledStatus) {
                            input.disabled = true;
                        }
                    }
                }
            },
            addMooring: function(){
                let vm=this;
                //var new_mooring= helpers.copyObject(vm.mooring)
                var new_mooring= helpers.copyObject(vm.proposal.other_details.mooring)
                new_mooring.push('');
                vm.proposal.other_details.mooring=new_mooring;
                console.log(vm.proposal.other_details.mooring);
            },
            fetchAccreditationChoices: function(){
                let vm = this;
                vm.$http.get('/api/accreditation_choices.json').then((response) => {
                    vm.accreditation_choices = response.body;
                    if(vm.proposal.other_details.accreditation_type
                        ){
                        for(var i=0; i<vm.accreditation_choices.length; i++){
                            if(vm.accreditation_choices[i].key==vm.proposal.other_details.accreditation_type){
                                vm.accreditation_type=vm.accreditation_choices[i]
                            }
                        }
                    }
                    
                },(error) => {
                    console.log(error);
                } );
            },
            fetchLicencePeriodChoices: function(){
                let vm = this;
                vm.$http.get('/api/licence_period_choices.json').then((response) => {
                    vm.licence_period_choices = response.body;
                    
                },(error) => {
                    console.log(error);
                } );
            },
            fetchGlobalSettings: function(){
                let vm = this;
                vm.$http.get('/api/global_settings.json').then((response) => {
                    vm.global_settings = response.body;
                    
                },(error) => {
                    console.log(error);
                } );
            },
            checkProposalAccreditation: function(){
                let vm= this;
                if(vm.proposal && vm.proposal.other_details){
                    for(var i=0; i<vm.proposal.other_details.accreditations.length; i++){
                        vm.proposal.other_details.accreditations[i].is_deleted=false;
                        vm.selected_accreditations.push(vm.proposal.other_details.accreditations[i].accreditation_type);
                    }
                }
            },
            selectAccreditation: function(e, accreditation_type){
                let vm=this;
                if(e.target.checked){
                    var found=false;
                    for(var i=0;i<vm.proposal.other_details.accreditations.length; i++){
                        if(vm.proposal.other_details.accreditations[i].accreditation_type==accreditation_type.key){
                            found=true;
                            vm.proposal.other_details.accreditations[i].is_deleted=false;
                        }
                    }
                    if(found==false){
                    var data={
                        'accreditation_type': accreditation_type.key,
                        'accreditation_expiry':null,
                        'comments':'',
                        'proposal_other_details': vm.proposal.other_details.id,
                        'is_deleted': false,
                        'accreditation_type_value': accreditation_type.value
                    }
                    var acc=helpers.copyObject(vm.proposal.other_details.accreditations);
                    acc.push(data);
                    vm.proposal.other_details.accreditations=acc;
                    }
                }
                else{
                    for(var i=0;i<vm.proposal.other_details.accreditations.length; i++)
                    {

                        if(vm.proposal.other_details.accreditations[i].accreditation_type==accreditation_type.key)
                        {
                            if(vm.proposal.other_details.accreditations[i].id){
                                //console.log('yes')
                                var acc=helpers.copyObject(vm.proposal.other_details.accreditations);
                                acc[i].is_deleted=true;
                                vm.proposal.other_details.accreditations=acc;
                            }
                            else{
                                var acc=helpers.copyObject(vm.proposal.other_details.accreditations);
                                acc.splice(i,1);
                                vm.proposal.other_details.accreditations=acc;
                            }
                        }
                    }
                }
            },
            eventListeners:function (){
                let vm=this;

                var date= new Date()
                var today= new Date(date.getFullYear(), date.getMonth(), date.getDate());

                $(vm.$refs.accreditation_expiry).datetimepicker(vm.datepickerOptions);
                $(vm.$refs.accreditation_expiry).on('dp.change', function(e){
                    if ($(vm.$refs.accreditation_expiry).data('DateTimePicker').date()) {
                        

                        vm.proposal.other_details.accreditation_expiry =  e.date.format('DD/MM/YYYY');
                    }
                    else if ($(vm.$refs.accreditation_expiry).data('date') === "") {
                        vm.proposal.other_details.accreditation_expiry = "";
                    }
                 });
                //Nominated start date listener
                $(vm.$refs.nominated_start_date).datetimepicker(vm.datepickerOptions);
                //Set minimum date on datetimepicker so that nominated
                //start date cannot be selected prior to today
                $(vm.$refs.nominated_start_date).data("DateTimePicker").minDate(today);
                $(vm.$refs.nominated_start_date).on('dp.change', function(e){
                    if ($(vm.$refs.nominated_start_date).data('DateTimePicker').date()) {
                        

                        vm.proposal.other_details.nominated_start_date =  e.date.format('DD/MM/YYYY');
                    }
                    else if ($(vm.$refs.nominated_start_date).data('date') === "") {
                        vm.proposal.other_details.nominated_start_date = "";
                    }
                 });
                //Insurance expiry date listener
                $(vm.$refs.insurance_expiry).datetimepicker(vm.datepickerOptions);
                //Set minimum date on datetimepicker so that
                //insurance expiry date cannot be selected prior to today
                $(vm.$refs.insurance_expiry).data("DateTimePicker").minDate(today);
                $(vm.$refs.insurance_expiry).on('dp.change', function(e){
                    if ($(vm.$refs.insurance_expiry).data('DateTimePicker').date()) {                       

                        vm.proposal.other_details.insurance_expiry =  e.date.format('DD/MM/YYYY');
                    }
                    else if ($(vm.$refs.insurance_expiry).data('date') === "") {
                        vm.proposal.other_details.insurance_expiry = "";
                    }
                 });
                // Intialise select2
                $(vm.$refs.preferred_licence_period).select2({
                    "theme": "bootstrap",
                    allowClear: true,
                    placeholder:"Select preferred licence term"
                }).
                on("select2:select",function (e) {
                    var selected = $(e.currentTarget);
                    vm.proposal.other_details.preferred_licence_period = selected.val();
                    vm.proposal.other_details.preferred_licence_period_id = selected.val();
                }).
                on("select2:unselect",function (e) {
                    var selected = $(e.currentTarget);
                    vm.proposal.other_details.preferred_licence_period = selected.val();
                    vm.proposal.other_details.preferred_licence_period_id = selected.val();
                });
            },
        },
        mounted: function(){
            let vm = this;
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
</style>

