<template id="external_conservation_status_datatable">
    <div>
        <CollapsibleFilters component_title="Filters" ref="collapsible_filters" @created="collapsible_component_mounted" class="mb-2">
            <div class="row">
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="">Type:</label>
                        <select class="form-select" v-model="filterCSGroupType">
                            <option value="all">All</option>
                            <option v-for="option in group_types" :value="option.name">{{option.display}}
                            </option>
                        </select>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="cs_scientific_name_lookup">Scientific Name:</label>
                        <select 
                            id="cs_scientific_name_lookup"  
                            name="cs_scientific_name_lookup"  
                            ref="cs_scientific_name_lookup" 
                            class="form-control" />
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="cs_community_name_lookup">Community Name:</label>
                        <select 
                            id="cs_community_name_lookup"  
                            name="cs_community_name_lookup"  
                            ref="cs_community_name_lookup" 
                            class="form-control" />
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="">Conservation List:</label>
                        <select class="form-select" v-model="filterCSConservationList" 
                        @change="filterConservationCategory($event)">
                            <option value="all">All</option>
                            <option v-for="list in conservation_list_dict" :value="list.id">{{list.code}}</option>
                        </select>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="">Conservation Category:</label>
                        <select class="form-select" v-model="filterCSConservationCategory">
                            <option value="all">All</option>
                            <option v-for="list in filtered_conservation_category_list" :value="list.id">{{list.code}}</option>
                        </select>
                    </div>
                </div>
                <div class="col-md-3">
                <div class="form-group">
                    <label for="">Status:</label>
                    <select class="form-select" v-model="filterCSApplicationStatus">
                        <option value="all">All</option>
                        <option v-for="status in proposal_status" :value="status.value">{{ status.name }}</option>
                    </select>
                </div>
            </div>
            </div>
        </CollapsibleFilters>

        <!-- <div v-if="addCSVisibility" class="col-md-12">
            <div class="text-end">
                <button type="button" class="btn btn-primary mb-2 " @click.prevent="createFloraConservationStatus"><i class="fa-solid fa-circle-plus"></i> Add Conservation Status</button>
            </div>
        </div> -->
        <div v-if="addCSVisibility" class="col-md-12 dropdown">
            <div class="text-end">
                <button class="btn btn-primary dropdown-toggle mb-2" type="button" id="cs_proposal_type" data-bs-toggle="dropdown" aria-expanded="false">
                    Propose Conservation Status
                </button>
                <ul class="dropdown-menu" aria-labelledby="cs_proposal_type">
                    <li v-for="group in group_types">
                        <a class="dropdown-item" 
                            @click.prevent="createConservationStatus(group.id)">{{ group.display }}
                        </a>
                    </li>
                </ul>
            </div>
        </div>
        <div class="row">
            <div class="col-lg-12">
                <datatable
                        ref="conservation_status_datatable"
                        :id="datatable_id"
                        :dtOptions="datatable_options"
                        :dtHeaders="datatable_headers"
                />
            </div>
        </div>
    </div>
</template>
<script>
//require('@popperjs/core');
//import { createPopper } from '@popperjs/core';
//require('popperjs');
//require('bootstrap');
import "babel-polyfill"
import datatable from '@/utils/vue/datatable.vue'
import CollapsibleFilters from '@/components/forms/collapsible_component.vue'
import FormSection from '@/components/forms/section_toggle.vue'
import Vue from 'vue'
// var select2 = require('select2');
// require("select2/dist/css/select2.min.css");
// require("select2-bootstrap-5-theme/dist/select2-bootstrap-5-theme.min.css")
import {
    api_endpoints,
    helpers
}from '@/utils/hooks'
export default {
    name: 'ExternalConservationStatusDatatable',
    props: {
        level:{
            type: String,
            required: true,
            validator:function(val) {
                let options = ['internal','referral','external'];
                return options.indexOf(val) != -1 ? true: false;
            }
        },
        url:{
            type: String,
            required: true
        },
        filterCSGroupType_cache: {
            type: String,
            required: false,
            default: 'filterCSGroupType',
        },
        filterCSScientificName_cache: {
            type: String,
            required: false,
            default: 'filterCSScientificName',
        },
        filterCSExCommunityName_cache: {
            type: String,
            required: false,
            default: 'filterCSExCommunityName',
        },
        filterCSConservationList_cache: {
            type: String,
            required: false,
            default: 'filterCSConservationList',
        },
        filterCSConservationCategory_cache: {
            type: String,
            required: false,
            default: 'filterCSConservationCategory',
        },
        filterCSApplicationStatus_cache: {
            type: String,
            required: false,
            default: 'filterCSApplicationStatus',
        },
    },
    data() {
        let vm = this;
        return {
            datatable_id: 'conservation_status-datatable-'+vm._uid,
     
            //Profile to check if user has access to process Proposal
            profile: {},
            
            

            // selected values for filtering
            
            filterCSGroupType: sessionStorage.getItem(this.filterCSGroupType_cache) ? 
                                   sessionStorage.getItem(this.filterCSGroupType_cache) : 'all',

            filterCSScientificName: sessionStorage.getItem(this.filterCSScientificName_cache) ? 
                                   sessionStorage.getItem(this.filterCSScientificName_cache) : 'all',

            filterCSExCommunityName: sessionStorage.getItem(this.filterCSExCommunityName_cache) ? 
                                   sessionStorage.getItem(this.filterCSExCommunityName_cache) : 'all',

            filterCSConservationList: sessionStorage.getItem(this.filterCSConservationList_cache) ? 
                                    sessionStorage.getItem(this.filterCSConservationList_cache) : 'all',

            filterCSConservationCategory: sessionStorage.getItem(this.filterCSConservationCategory_cache) ? 
                                    sessionStorage.getItem(this.filterCSConservationCategory_cache) : 'all',

            filterCSApplicationStatus: sessionStorage.getItem(this.filterCSApplicationStatus_cache) ?
                                    sessionStorage.getItem(this.filterCSApplicationStatus_cache) : 'all',

            //Filter list for scientific name and common name
            group_types: [],
            filterListsSpecies: {},
            conservation_list_dict: [],
            conservation_category_list: [],
            filtered_conservation_category_list: [],
            
            // filtering options
            external_status:[
                {value: 'draft', name: 'Draft'},
                {value: 'with_assessor', name: 'Under Review'},
                {value: 'ready_for_agenda', name: 'In Meeting'},
                {value: 'approved', name: 'Approved'},
                {value: 'declined', name: 'Declined'},
                {value: 'closed', name: 'Closed'},
            ],
            proposal_status: [],
        }
    },
    components:{
        datatable,
        CollapsibleFilters,
        FormSection,
    },
    watch:{
        filterCSGroupType: function(){
            let vm = this;
            vm.$refs.conservation_status_datatable.vmDataTable.ajax.reload(); // This calls ajax() backend call.
            sessionStorage.setItem(vm.filterCSGroupType_cache, vm.filterCSGroupType);  
        },
        filterCSScientificName: function(){
            let vm = this;
            vm.$refs.conservation_status_datatable.vmDataTable.ajax.reload(); // This calls ajax() backend call.
            sessionStorage.setItem(vm.filterCSScientificName_cache, vm.filterCSScientificName);  
        },
        filterCSExCommunityName: function(){
            let vm = this;
            vm.$refs.conservation_status_datatable.vmDataTable.ajax.reload(); // This calls ajax() backend call.
            sessionStorage.setItem(vm.filterCSExCommunityName_cache, vm.filterCSExCommunityName);  
        },
        filterCSScientificName: function(){
            let vm = this;
            vm.$refs.conservation_status_datatable.vmDataTable.ajax.reload(); // This calls ajax() backend call.
            sessionStorage.setItem(vm.filterCSScientificName_cache, vm.filterCSScientificName);  
        },
        filterCSConservationList: function() {
            let vm = this;
            vm.$refs.conservation_status_datatable.vmDataTable.ajax.reload(); // This calls ajax() backend call.  
            sessionStorage.setItem(vm.filterCSConservationList_cache, vm.filterCSConservationList);
        },
        filterCSConservationCategory: function() {
            let vm = this;
            vm.$refs.conservation_status_datatable.vmDataTable.ajax.reload(); // This calls ajax() backend call.  
            sessionStorage.setItem(vm.filterCSConservationCategory_cache, vm.filterCSConservationCategory);
        },
        filterCSApplicationStatus: function() {
            let vm = this;
            vm.$refs.conservation_status_datatable.vmDataTable.ajax.reload(); // This calls ajax() backend call.  
            sessionStorage.setItem(vm.filterCSApplicationStatus_cache, vm.filterCSApplicationStatus);
        },
        filterApplied: function(){
            if (this.$refs.collapsible_filters){
                // Collapsible component exists
                this.$refs.collapsible_filters.show_warning_icon(this.filterApplied)
            }
        },
    },
    computed: {
        filterApplied: function(){
            if(this.filterCSGroupType === 'all' && 
                this.filterCSScientificName === 'all' && 
                this.filterCSExCommunityName === 'all' &&
                this.filterCSConservationList === 'all' && 
                this.filterCSConservationCategory === 'all' && 
                this.filterCSApplicationStatus === 'all'){ 
                return false
            } else {
                return true
            }
        },
        is_external: function(){
            return this.level == 'external';
        },
        addCSVisibility: function() {
            let visibility = false;
            visibility = true;
            return visibility;
        },
        datatable_headers: function(){
            if (this.is_external){
                return ['Number','Type','Scientific Name','Community Name','Conservation List', 
                    'Conservation Category','Status', 'Action']
            }
        },
        column_id: function(){
            return {
                data: "id",
                orderable: true,
                searchable: false,
                visible: false,
                'render': function(data, type, full){
                    return full.id
                },
                name: "id",
            }
        },
        column_number: function(){
            return {
                data: "conservation_status_number",
                orderable: true,
                searchable: true,
                visible: true,
                'render': function(data, type, full){
                    return full.conservation_status_number
                },
                name: "id",
            }
        },
        column_type: function(){
            return {
                data: "group_type",
                orderable: true,
                searchable: true,
                visible: true,
                'render': function(data, type, full){
                    return full.group_type
                },
                name: "species__group_type__name",
            }
        },
        column_scientific_name: function(){
            return {
                data: "scientific_name",
                orderable: true,
                searchable: true,
                visible: true,
                'render': function(value, type){
                    let result = helpers.dtPopover(value, 30, 'hover');
                    return type=='export' ? value : result;
                },
                name: "species__scientific_name__name",
            }
        },
        column_community_name: function(){
            return {
                data: "community_name",
                orderable: true,
                searchable: true,
                visible: true,
                'render': function(value, type){
                    let result = helpers.dtPopover(value, 30, 'hover');
                    return type=='export' ? value : result;
                },
                name: "community__community_name__name",
            }
        },
        column_conservation_list: function(){
            return {
                data: "conservation_list",
                orderable: true,
                searchable: true,
                visible: true,
                'render': function(data, type, full){
                    if(full.conservation_list){
                        return full.conservation_list;
                    }
                    // Should not reach here
                    return ''
                },
                name: "conservation_list__code",
            }
        },
        column_conservation_category: function(){
            return {
                data: "conservation_category",
                orderable: true,
                searchable: true,
                visible: true,
                'render': function(data, type, full){
                    if(full.conservation_category){
                        return full.conservation_category;
                    }
                    // Should not reach here
                    return ''
                },
                name: "conservation_category__code",
            }
        },
        column_status: function(){
            return {
                // 9. Workflow Status
                data: "customer_status",
                orderable: true,
                searchable: true,
                visible: true,
                'render': function(data, type, full){
                    if (full.customer_status){
                        return full.customer_status;
                    }
                    // Should not reach here
                    return ''
                },
                name: "customer_status",
            }
        },
        column_action: function(){
            let vm = this
            return {
                // 10. Action
                data: "id",
                orderable: false,
                searchable: false,
                visible: true,
                'render': function(data, type, full){
                    let links = "";
                    if (full.can_user_edit) {
                            links +=  `<a href='/external/conservation_status/${full.id}'>Continue</a><br/>`;
                            links +=  `<a href='#${full.id}' data-discard-cs-proposal='${full.id}'>Discard</a><br/>`;
                        }
                        else if (full.can_user_view) {
                            links +=  `<a href='/external/conservation_status/${full.id}'>View</a>`;
                        }

                    return links;
                }
            }
        },
        datatable_options: function(){
            let vm = this

            let columns = []
            let search = null
            let buttons = []
            if(vm.is_external){
                columns = [
                    vm.column_number,
                    vm.column_type,
                    vm.column_scientific_name,
                    vm.column_community_name,
                    vm.column_conservation_list,
                    vm.column_conservation_category,
                    vm.column_status,
                    vm.column_action,
                ]
                search = false
                buttons = []
            }
            
            return {
                autoWidth: false,
                language: {
                    processing: "<i class='fa fa-4x fa-spinner fa-spin'></i>"
                },
                order: [
                    [0, 'desc']
                ],
                lengthMenu: [ [10, 25, 50, 100, -1], [10, 25, 50, 100, "All"] ],
                responsive: true,
                serverSide: true,
                 //  to show the "workflow Status","Action" columns always in the last position
                columnDefs: [
                    { responsivePriority: 1, targets: 0 },
                    { responsivePriority: 3, targets: -1 },
                    { responsivePriority: 2, targets: -2 }
                ],
                ajax: {
                    "url": this.url,
                    "dataSrc": 'data',

                    // adding extra GET params for Custom filtering
                    "data": function ( d ) {
                        d.filter_group_type = vm.filterCSGroupType;
                        d.filter_scientific_name = vm.filterCSScientificName;
                        d.filter_community_name = vm.filterCSExCommunityName;
                        d.filter_conservation_list = vm.filterCSConservationList;
                        d.filter_conservation_category = vm.filterCSConservationCategory;
                        d.filter_application_status = vm.filterCSApplicationStatus
                        d.is_internal = vm.is_internal;
                    }
                },
                //dom: 'lBfrtip',
                dom: "<'d-flex align-items-center'<'me-auto'l>fB>" +
                     "<'row'<'col-sm-12'tr>>" +
                     "<'d-flex align-items-center'<'me-auto'i>p>",
                buttons: buttons,

                columns: columns,
                processing: true,
                initComplete: function() {
                    helpers.enablePopovers();
                },
            }
        }
    
    },
    methods:{
        collapsible_component_mounted: function(){
            this.$refs.collapsible_filters.show_warning_icon(this.filterApplied)
        },
        initialiseScientificNameLookup: function(){
                let vm = this;
                $(vm.$refs.cs_scientific_name_lookup).select2({
                    minimumInputLength: 2,
                    "theme": "bootstrap-5",
                    allowClear: true,
                    placeholder:"Select Scientific Name",
                    ajax: {
                        url: api_endpoints.species_lookup,
                        dataType: 'json',
                        data: function(params) {
                            var query = {
                                term: params.term,
                                type: 'public',
                            }
                            return query;
                        },
                    },
                }).
                on("select2:select", function (e) {
                    var selected = $(e.currentTarget);
                    let data = e.params.data.id;
                    vm.filterCSScientificName = data;
                    sessionStorage.setItem("filterCSScientificNameText", e.params.data.text);
                }).
                on("select2:unselect",function (e) {
                    var selected = $(e.currentTarget);
                    vm.filterCSScientificName = 'all';
                    sessionStorage.setItem("filterCSScientificNameText",'');
                }).
                on("select2:open",function (e) {
                    const searchField = $('[aria-controls="select2-cs_scientific_name_lookup-results"]')
                    // move focus to select2 field
                    searchField[0].focus();
                });
        },
        initialiseCommunityNameLookup: function(){
                let vm = this;
                $(vm.$refs.cs_community_name_lookup).select2({
                    minimumInputLength: 2,
                    "theme": "bootstrap-5",
                    allowClear: true,
                    placeholder:"Select Community Name",
                    ajax: {
                        url: api_endpoints.communities_lookup,
                        dataType: 'json',
                        data: function(params) {
                            var query = {
                                term: params.term,
                                type: 'public',
                            }
                            return query;
                        },
                    },
                }).
                on("select2:select", function (e) {
                    var selected = $(e.currentTarget);
                    let data = e.params.data.id;
                    vm.filterCSExCommunityName = data;
                    sessionStorage.setItem("filterCSExCommunityNameText", e.params.data.text);
                }).
                on("select2:unselect",function (e) {
                    var selected = $(e.currentTarget);
                    vm.filterCSExCommunityName = 'all';
                    sessionStorage.setItem("filterCSExCommunityNameText",'');
                }).
                on("select2:open",function (e) {
                    const searchField = $('[aria-controls="select2-cs_community_name_lookup-results"]')
                    // move focus to select2 field
                    searchField[0].focus();
                });
        },
        fetchFilterLists: function(){
            let vm = this;
            //large FilterList
            vm.$http.get(api_endpoints.conservation_list_dict).then((response) => {
                vm.filterListsCS = response.body;
                vm.conservation_list_dict = vm.filterListsCS.conservation_list;
                vm.conservation_category_list = vm.filterListsCS.conservation_category_list;
                vm.filterConservationCategory();
                vm.proposal_status = vm.external_status;
                //vm.proposal_status = vm.level == 'internal' ? response.body.processing_status_choices: response.body.customer_status_choices;
            },(error) => {
                console.log(error);
            })
            vm.$http.get(api_endpoints.group_types_dict).then((response) => {
                vm.group_types= response.body;
                },(error) => {
                console.log(error);
            });
        },
        //-------filter category dropdown dependent on conservation_list selected
        filterConservationCategory: function(event) {
                //this.$nextTick(() => {
                    if(event){
                      this.filterCSConservationCategory='all'; //-----to remove the previous selection
                    }
                    this.filtered_conservation_category_list=[];
                    //---filter conservation_categories as per cons_list selected
                    for(let choice of this.conservation_category_list){
                        if(choice.conservation_list_id.toString() === this.filterCSConservationList.toString())
                        {
                          this.filtered_conservation_category_list.push(choice);
                        }
                    }
                //});
        },
        createConservationStatus: async function (group_type) {
            let newCSId = null
            try {
                    const createUrl = api_endpoints.conservation_status+"/";
                    let payload = new Object();
                    payload.application_type_id = group_type
                    let savedCS = await Vue.http.post(createUrl, payload);
                    if (savedCS) {
                        newCSId = savedCS.body.id;
                    }
                }
            catch (err) {
                console.log(err);
                if (this.is_external) {
                    return err;
                }
            }
            this.$router.push({
                name: 'draft_cs_proposal',
                params: {conservation_status_id: newCSId},
                });
        },
        discardCSProposal:function (conservation_status_id) {
            let vm = this;
            swal({
                title: "Discard Application",
                text: "Are you sure you want to discard this proposal?",
                type: "warning",
                showCancelButton: true,
                confirmButtonText: 'Discard Application',
                confirmButtonColor:'#d9534f'
            }).then(() => {
                vm.$http.delete(api_endpoints.discard_cs_proposal(conservation_status_id))
                .then((response) => {
                    swal(
                        'Discarded',
                        'Your proposal has been discarded',
                        'success'
                    )
                    vm.$refs.conservation_status_datatable.vmDataTable.ajax.reload();
                }, (error) => {
                    console.log(error);
                });
            },(error) => {

            });
        },
        addEventListeners: function(){
            let vm = this;
            // External Discard listener
            vm.$refs.conservation_status_datatable.vmDataTable.on('click', 'a[data-discard-cs-proposal]', function(e) {
                e.preventDefault();
                var id = $(this).attr('data-discard-cs-proposal');
                vm.discardCSProposal(id);
            });
        },
        initialiseSearch:function(){
            this.submitterSearch();
        },
        submitterSearch:function(){
            let vm = this;
            vm.$refs.conservation_status_datatable.table.dataTableExt.afnFiltering.push(
                function(settings,data,dataIndex,original){
                    let filtered_submitter = vm.filterProposalSubmitter;
                    if (filtered_submitter == 'All'){ return true; } 
                    return filtered_submitter == original.submitter.email;
                }
            );
        },
        fetchProfile: function(){
            let vm = this;
            /*Vue.http.get(api_endpoints.profile).then((response) => {
                vm.profile = response.body;
            
            },(error) => {
                console.log(error);
                
            })*/
        },

        check_assessor: function(proposal){
            let vm = this;
            if (proposal.assigned_officer)
                {
                    { if(proposal.assigned_officer== vm.profile.full_name)
                        return true;
                    else
                        return false;
                }
            }
            else{
                 var assessor = proposal.allowed_assessors.filter(function(elem){
                    return(elem.id=vm.profile.id)
                });
                
                if (assessor.length > 0)
                    return true;
                else
                    return false;
              
            }
            
        },
    },


    mounted: function(){
        this.fetchFilterLists();
        this.fetchProfile();
        let vm = this;
        $( 'a[data-toggle="collapse"]' ).on( 'click', function () {
            var chev = $( this ).children()[ 0 ];
            window.setTimeout( function () {
                $( chev ).toggleClass( "glyphicon-chevron-down glyphicon-chevron-up" );
            }, 100 );
        });
        this.$nextTick(() => {
            vm.initialiseScientificNameLookup();
            vm.initialiseCommunityNameLookup();
            vm.addEventListeners();
            vm.initialiseSearch();
            // -- to set the select2 field with the session value if exists onload()
            if(sessionStorage.getItem("filterCSScientificName")!='all' && sessionStorage.getItem("filterCSScientificName")!=null)
            {
                // contructor new Option(text, value, defaultSelected, selected)
                var newOption = new Option(sessionStorage.getItem("filterCSScientificNameText"), vm.filterCSScientificName, false, true);
                $('#cs_scientific_name_lookup').append(newOption);
            }
            if(sessionStorage.getItem("filterCSExCommunityName")!='all' && sessionStorage.getItem("filterCSExCommunityName")!=null)
            {
                // contructor new Option(text, value, defaultSelected, selected)
                var newOption = new Option(sessionStorage.getItem("filterCSExCommunityNameText"), vm.filterCSExCommunityName, false, true);
                $('#cs_community_name_lookup').append(newOption);
            }
        });
    }
}
</script>
<style scoped>
.dt-buttons{
    float: right;
}
.collapse-icon {
    cursor: pointer;
}
.collapse-icon::before {
    top: 5px;
    left: 4px;
    height: 14px;
    width: 14px;
    border-radius: 14px;
    line-height: 14px;
    border: 2px solid white;
    line-height: 14px;
    content: '-';
    color: white;
    background-color: #d33333;
    display: inline-block;
    box-shadow: 0px 0px 3px #444;
    box-sizing: content-box;
    text-align: center;
    text-indent: 0 !important;
    font-family: 'Courier New', Courier monospace;
    margin: 5px;
}
.expand-icon {
    cursor: pointer;
}
.expand-icon::before {
    top: 5px;
    left: 4px;
    height: 14px;
    width: 14px;
    border-radius: 14px;
    line-height: 14px;
    border: 2px solid white;
    line-height: 14px;
    content: '+';
    color: white;
    background-color: #337ab7;
    display: inline-block;
    box-shadow: 0px 0px 3px #444;
    box-sizing: content-box;
    text-align: center;
    text-indent: 0 !important;
    font-family: 'Courier New', Courier monospace;
    margin: 5px;
}

</style>
