<template id="species_fauna_dashboard">
    <div>
        <CollapsibleFilters ref="collapsible_filters" @created="collapsible_component_mounted" label= "Filter">
            <div class="row">
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="">Scientific Name:</label>
                        <select class="form-control" v-model="filterFaunaScientificName">
                            <option value="all">All</option>
                            <option v-for="species in species_data_list" :value="species.scientific_name">{{species.scientific_name}}</option>
                        </select>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="">Common Name:</label>
                        <select class="form-control" v-model="filterFaunaCommonName">
                            <option value="all">All</option>
                            <option v-for="species in species_data_list" :value="species.common_name">{{species.common_name}}</option>
                        </select>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="">Phylo Group:</label>
                        <select class="form-control" v-model="filterFaunaPhylogeneticGroup">
                            <option value="all">All</option>
                            <option v-for="species in species_data_list" :value="species.phylogenetic_group">
                                {{species.phylogenetic_group}}</option>
                        </select>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="">Family:</label>
                        <select class="form-control" v-model="filterFaunaFamily">
                            <option value="all">All</option>
                            <option v-for="species in species_data_list" :value="species.family">{{species.family}}</option>
                        </select>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="">Genera:</label>
                        <select class="form-control" v-model="filterFaunaGenus">
                            <option value="all">All</option>
                            <option v-for="species in species_data_list" :value="species.genus">{{species.genus}}</option>
                        </select>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="">Conservation List:</label>
                        <select class="form-control" v-model="filterFaunaConservationList">
                            <option value="all">All</option>
                            <option v-for="list in conservation_list_dict" :value="list.id">{{list.code}}</option>
                        </select>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="">Conservation Category:</label>
                        <select class="form-control" v-model="filterFaunaConservationCategory">
                            <option value="all">All</option>
                            <option v-for="list in conservation_category_list" :value="list.id">{{list.code}}</option>
                        </select>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="">Workflow Status:</label>
                        <select class="form-control">
                            <option value="all">All</option>
                        </select>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="">Region:</label>
                        <select class="form-control" v-model="filterFaunaRegion">
                            <option value="all">All</option>
                            <option v-for="region in region_list" :value="region.id">{{region.name}}</option>
                        </select>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="">District:</label>
                        <select class="form-control" v-model="filterFaunaDistrict">
                            <option value="all">All</option>
                            <option v-for="district in district_list" :value="district.id">{{district.name}}</option>
                        </select>
                    </div>
                </div>

                <div v-if="is_external" class="col-md-6">
                    <router-link  style="margin-top:25px;" class="btn btn-primary pull-right" :to="{ name: 'apply_proposal' }">New Application</router-link>
                </div>
            </div>
        </CollapsibleFilters>

        <div class="row">
            <div class="col-lg-12">
                <datatable
                        ref="fauna_datatable"
                        :id="datatable_fauna_id"
                        :dtOptions="datatable_options"
                        :dtHeaders="datatable_headers"
                />
            </div>
        </div>
    </div>
</template>
<script>
import "babel-polyfill"
import datatable from '@/utils/vue/datatable.vue'
import CollapsibleFilters from '@/components/forms/collapsible_component.vue'
import FormSection from '@/components/forms/section_toggle.vue'
import Vue from 'vue'
require("select2/dist/css/select2.min.css");
require("select2-bootstrap-theme/dist/select2-bootstrap.min.css");
import {
    api_endpoints,
    helpers
}from '@/utils/hooks'
export default {
    name: 'SpeciesFaunaTable',
    props: {
        level:{
            type: String,
            required: true,
            validator:function(val) {
                let options = ['internal','referral','external'];
                return options.indexOf(val) != -1 ? true: false;
            }
        },
        group_type_name:{
            type: String,
            required: true
        },
        url:{
            type: String,
            required: true
        },
        filterFaunaScientificName_cache: {
            type: String,
            required: false,
            default: 'filterFaunaScientificName',
        },
        filterFaunaCommonName_cache: {
            type: String,
            required: false,
            default: 'filterFaunaCommonName',
        },
        filterFaunaPhylogeneticGroup_cache: {
            type: String,
            required: false,
            default: 'filterFaunaPhylogeneticGroup',
        },
        filterFaunaFamily_cache: {
            type: String,
            required: false,
            default: 'filterFaunaFamily',
        },
        filterFaunaGenus_cache: {
            type: String,
            required: false,
            default: 'filterFaunaGenus',
        },
        filterFaunaConservationList_cache: {
            type: String,
            required: false,
            default: 'filterFaunaConservationList',
        },
        filterFaunaConservationCategory_cache: {
            type: String,
            required: false,
            default: 'filterFaunaConservationCategory',
        },
        filterFaunaRegion_cache: {
            type: String,
            required: false,
            default: 'filterFaunaRegion',
        },
        filterFaunaDistrict_cache: {
            type: String,
            required: false,
            default: 'filterFaunaDistrict',
        },
    },
    data() {
        let vm = this;
        return {
            datatable_fauna_id: 'species_fauna-datatable-'+vm._uid,
     
            //Profile to check if user has access to process Proposal
            profile: {},
            is_payment_admin: false,
            
            // selected values for filtering
            filterFaunaScientificName: sessionStorage.getItem(this.filterFaunaScientificName_cache) ? 
                                        sessionStorage.getItem(this.filterFaunaScientificName_cache) : 'all',

            filterFaunaCommonName: sessionStorage.getItem(this.filterFaunaCommonName_cache) ? 
                                    sessionStorage.getItem(this.filterFaunaCommonName_cache) : 'all',

            filterFaunaPhylogeneticGroup: sessionStorage.getItem(this.filterFaunaPhylogeneticGroup_cache) ? 
                                            sessionStorage.getItem(this.filterFaunaPhylogeneticGroup_cache) : 'all',

            filterFaunaFamily: sessionStorage.getItem(this.filterFaunaFamily_cache) ? 
                                sessionStorage.getItem(this.filterFaunaFamily_cache) : 'all',

            filterFaunaGenus: sessionStorage.getItem(this.filterFaunaGenus_cache) ? 
                                sessionStorage.getItem(this.filterFaunaGenus_cache) : 'all',

            filterFaunaConservationList: sessionStorage.getItem(this.filterFaunaConservationList_cache) ? 
                                    sessionStorage.getItem(this.filterFaunaConservationList_cache) : 'all',

            filterFaunaConservationCategory: sessionStorage.getItem(this.filterFaunaConservationCategory_cache) ? 
                                    sessionStorage.getItem(this.filterFaunaConservationCategory_cache) : 'all',

            filterFaunaRegion: sessionStorage.getItem(this.filterFaunaRegion_cache) ? 
                                sessionStorage.getItem(this.filterFaunaRegion_cache) : 'all',

            filterFaunaDistrict: sessionStorage.getItem(this.filterFaunaDistrict_cache) ? 
                                    sessionStorage.getItem(this.filterFaunaDistrict_cache) : 'all',


            //Filter list for scientific name and common name
            filterListsSpecies: {},
            species_data_list: [],
            conservation_list_dict: [],
            conservation_category_list: [],
            filterRegionDistrict: {},
            region_list: [],
            district_list: [],

            // filtering options
            external_status:[
                {value: 'draft', name: 'Draft'},
                {value: 'with_assessor', name: 'Under Review'},
                {value: 'approved', name: 'Approved'},
                {value: 'declined', name: 'Declined'},
                {value: 'discarded', name: 'Discarded'},
                {value: 'awaiting_payment', name: 'Awaiting Payment'},
            ],
            internal_status:[
                {value: 'draft', name: 'Draft'},
                {value: 'with_assessor', name: 'With Assessor'},
                {value: 'on_hold', name: 'On Hold'},
                {value: 'with_qa_officer', name: 'With QA Officer'},
                {value: 'with_referral', name: 'With Referral'},
                {value: 'with_assessor_requirements', name: 'With Assessor (Requirements)'},
                {value: 'with_approver', name: 'With Approver'},
                {value: 'approved', name: 'Approved'},
                {value: 'declined', name: 'Declined'},
                {value: 'discarded', name: 'Discarded'},
                {value: 'awaiting_payment', name: 'Awaiting Payment'},
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
        filterFaunaScientificName: function(){
            let vm = this;
            vm.$refs.fauna_datatable.vmDataTable.ajax.reload(); // This calls ajax() backend call.  
            sessionStorage.setItem(vm.filterFaunaScientificName_cache, vm.filterFaunaScientificName);
        },
        filterFaunaCommonName: function() {
            let vm = this;
            vm.$refs.fauna_datatable.vmDataTable.ajax.reload(); // This calls ajax() backend call. 
            sessionStorage.setItem(vm.filterFaunaCommonName_cache, vm.filterFaunaCommonName);
        },
        filterFaunaPhylogeneticGroup: function() {
            let vm = this;
            vm.$refs.fauna_datatable.vmDataTable.ajax.reload(); // This calls ajax() backend call. 
            sessionStorage.setItem(vm.filterFaunaPhylogeneticGroup_cache, vm.filterFaunaPhylogeneticGroup);
        },
        filterFaunaFamily: function() {
            let vm = this;
            vm.$refs.fauna_datatable.vmDataTable.ajax.reload(); // This calls ajax() backend call.
            sessionStorage.setItem(vm.filterFaunaFamily_cache, vm.filterFaunaFamily);  
        },
        filterFaunaGenus: function() {
            let vm = this;
            vm.$refs.fauna_datatable.vmDataTable.ajax.reload(); // This calls ajax() backend call.
            sessionStorage.setItem(vm.filterFaunaGenus_cache, vm.filterFaunaGenus);  
        },
        filterFaunaConservationList: function() {
            let vm = this;
            vm.$refs.fauna_datatable.vmDataTable.ajax.reload(); // This calls ajax() backend call.  
            sessionStorage.setItem(vm.filterFaunaConservationList_cache, vm.filterFaunaConservationList);
        },
        filterFaunaConservationCategory: function() {
            let vm = this;
            vm.$refs.fauna_datatable.vmDataTable.ajax.reload(); // This calls ajax() backend call.  
            sessionStorage.setItem(vm.filterFaunaConservationCategory_cache, vm.filterFaunaConservationCategory);
        },
        filterFaunaRegion: function(){
            let vm = this;
            vm.$refs.fauna_datatable.vmDataTable.ajax.reload(); // This calls ajax() backend call.
            sessionStorage.setItem(vm.filterFaunaRegion_cache, vm.filterFaunaRegion);
        },
        filterFaunaDistrict: function(){
            let vm = this;
            vm.$refs.fauna_datatable.vmDataTable.ajax.reload(); // This calls ajax() backend call.
            sessionStorage.setItem(vm.filterFaunaDistrict_cache, vm.filterFaunaDistrict);
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
            if(this.filterFaunaScientificName === 'all' && 
                this.filterFaunaCommonName === 'all' && 
                this.filterFaunaPhylogeneticGroup === 'all' && 
                this.filterFaunaConservationList === 'all' && 
                this.filterFaunaConservationCategory === 'all' && 
                this.filterFaunaFamily === 'all' && 
                this.filterFaunaGenus === 'all' && 
                this.filterFaunaRegion === 'all' && 
                this.filterFaunaDistrict === 'all'){
                return false
            } else {
                return true
            }
        },
        is_external: function(){
            return this.level == 'external';
        },
        is_internal: function() {
            return this.level == 'internal'
        },
        is_referral: function(){
            return this.level == 'referral';
        },
        datatable_headers: function(){
            if (this.is_external){
                return ['id', 'Number', 'Scientific Name', 'Common Name', 'Phylo Group', 'Family', 'Action', 'Genera',' Conservation List', 'Conservation Category','Workflow Status', 'Region', 'District']
            }
            if (this.is_internal){
                return ['id', 'Number', 'Scientific Name', 'Common Name', 'Phylo Group', 'Family', 'Action', 'Genera', 'Conservation List', 'Conservation Category','Workflow Status', 'Region', 'District']
            }
        },
        column_id: function(){
            return {
                data: "id",
                orderable: false,
                searchable: false,
                visible: false,
                'render': function(data, type, full){
                    return full.id
                }
            }
        },
        column_number: function(){
            return {
                data: "id",
                orderable: true,
                searchable: true,
                visible: true,
                'render': function(data, type, full){
                    return full.id
                },
                name: "id",
            }
        },
        column_scientific_name: function(){
            return {
                data: "scientific_name",
                orderable: true,
                searchable: true,
                visible: true,
                'render': function(value, type){
                        var ellipsis = '...',
                                truncated = _.truncate(value, {
                                    length: 25,
                                    omission: ellipsis,
                                    separator: ' '
                                }),
                                result = '<span>' + truncated + '</span>',
                                popTemplate = _.template('<a href="#" ' +
                                    'role="button" ' +
                                    'data-toggle="popover" ' +
                                    'data-trigger="click" ' +
                                    'data-placement="top auto"' +
                                    'data-html="true" ' +
                                    'data-content="<%= text %>" ' +
                                    '>more</a>');
                            if (_.endsWith(truncated, ellipsis)) {
                                result += popTemplate({
                                    text: value
                                });
                            }
                            //return result;
                            return type=='export' ? value : result;
                },
                'createdCell': helpers.dtPopoverCellFn,
                name: "scientific_name",
            }
        },
        column_common_name: function(){
            return {
                data: "common_name",
                orderable: true,
                searchable: true,
                visible: true,
                'render': function(value, type){
                        var ellipsis = '...',
                                truncated = _.truncate(value, {
                                    length: 25,
                                    omission: ellipsis,
                                    separator: ' '
                                }),
                                result = '<span>' + truncated + '</span>',
                                popTemplate = _.template('<a href="#" ' +
                                    'role="button" ' +
                                    'data-toggle="popover" ' +
                                    'data-trigger="click" ' +
                                    'data-placement="top auto"' +
                                    'data-html="true" ' +
                                    'data-content="<%= text %>" ' +
                                    '>more</a>');
                            if (_.endsWith(truncated, ellipsis)) {
                                result += popTemplate({
                                    text: value
                                });
                            }
                            //return result;
                            return type=='export' ? value : result;
                },
                'createdCell': helpers.dtPopoverCellFn,
                name: "common_name",
            }
        },
        column_phylogenetic_group: function(){
            return {
                data: "phylogenetic_group",
                orderable: true,
                searchable: true,
                visible: true,
                'render': function(value, type){
                        var ellipsis = '...',
                                truncated = _.truncate(value, {
                                    length: 25,
                                    omission: ellipsis,
                                    separator: ' '
                                }),
                                result = '<span>' + truncated + '</span>',
                                popTemplate = _.template('<a href="#" ' +
                                    'role="button" ' +
                                    'data-toggle="popover" ' +
                                    'data-trigger="click" ' +
                                    'data-placement="top auto"' +
                                    'data-html="true" ' +
                                    'data-content="<%= text %>" ' +
                                    '>more</a>');
                            if (_.endsWith(truncated, ellipsis)) {
                                result += popTemplate({
                                    text: value
                                });
                            }
                            //return result;
                            return type=='export' ? value : result;
                },
                'createdCell': helpers.dtPopoverCellFn,
                name: "phylogenetic_group",
            }
        },
        column_family: function(){
            return {
                data: "family",
                orderable: true,
                searchable: true,
                visible: true,
                'render': function(value, type){
                        var ellipsis = '...',
                                truncated = _.truncate(value, {
                                    length: 25,
                                    omission: ellipsis,
                                    separator: ' '
                                }),
                                result = '<span>' + truncated + '</span>',
                                popTemplate = _.template('<a href="#" ' +
                                    'role="button" ' +
                                    'data-toggle="popover" ' +
                                    'data-trigger="click" ' +
                                    'data-placement="top auto"' +
                                    'data-html="true" ' +
                                    'data-content="<%= text %>" ' +
                                    '>more</a>');
                            if (_.endsWith(truncated, ellipsis)) {
                                result += popTemplate({
                                    text: value
                                });
                            }
                            //return result;
                            return type=='export' ? value : result;
                },
                'createdCell': helpers.dtPopoverCellFn,
                name: "family",
            }
        },
        column_genera: function(){
            return {
                data: "genus",
                orderable: true,
                searchable: true,
                visible: true,
                'render': function(data, type, full){
                    if(full.genus){
                        return full.genus;
                    }
                    // Should not reach here
                    return ''
                },
                name: "genus",
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
                name: "conservation_list",
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
                name: "conservation_category",
            }
        },
        column_workflow_status: function(){
            return {
                data: "processing_status",
                orderable: true,
                searchable: true,
                visible: true,
                'render': function(data, type, full){
                    if (full.processing_status){
                        return full.processing_status;
                    }
                    // Should not reach here
                    return ''
                },
                name: "processing_status",
            }
        },
        column_region: function(){
            return {
                data: "region",
                orderable: true,
                searchable: false, // handles by filter_queryset override method - class ProposalFilterBackend
                visible: true,
                'render': function(data, type, full){
                    if (full.region){
                        return full.region
                    }
                    // Should not reach here
                    return ''
                },
                name: "region",
            }
        },
        column_district: function(){
            return {
                data: "district",
                orderable: true,
                searchable: false, // handles by filter_queryset override method - class ProposalFilterBackend
                visible: true,
                'render': function(data, type, full){
                    if (full.district){
                        return full.district
                    }
                    // Should not reach here
                    return ''
                },
                name: "district",
            }
        },
        column_action: function(){
            let vm = this
            return {
                data: "id",
                orderable: false,
                searchable: false,
                visible: true,
                'render': function(data, type, full){
                    let links = "";
                    if (!vm.is_external){
                        /*if(vm.check_assessor(full) && full.can_officer_process)*/
                        if(full.assessor_process){   
                                links +=  `<a href='/internal/species_communities/${full.id}'>Process</a><br/>`;    
                        }
                        else{
                            links +=  `<a href='/internal/species_communities/${full.id}'>View</a><br/>`;
                        }
                    }
                    else{
                        if (full.can_user_edit) {
                            links +=  `<a href='/external/species_communities/${full.id}'>Continue</a><br/>`;
                            links +=  `<a href='#${full.id}' data-discard-proposal='${full.id}'>Discard</a><br/>`;
                        }
                        else if (full.can_user_view) {
                            links +=  `<a href='/external/species_communities/${full.id}'>View</a>`;
                        }
                    }

                    links +=  `<a href='/internal/species_communities/${full.id}'>Edit</a><br/>`; // Dummy addition for Boranaga demo

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
                    vm.column_id,
                    vm.column_number,
                    vm.column_scientific_name,
                    vm.column_common_name,
                    vm.column_phylogenetic_group,
                    vm.column_family,
                    vm.column_action,
                    vm.column_genera,
                    vm.column_conservation_list,
                    vm.column_conservation_category,
                    vm.column_workflow_status,
                    vm.column_region,
                    vm.column_district,
                ]
                search = false
                buttons = []
            }
            if(vm.is_internal){
                columns = [
                    vm.column_id,
                    vm.column_number,
                    vm.column_scientific_name,
                    vm.column_common_name,
                    vm.column_phylogenetic_group,
                    vm.column_family,
                    vm.column_action,
                    vm.column_genera,
                    vm.column_conservation_list,
                    vm.column_conservation_category,
                    vm.column_workflow_status,
                    vm.column_region,
                    vm.column_district,
                ]
                search = true
                buttons = [
                    {
                        extend: 'excel',
                        exportOptions: {
                            columns: ':visible'
                        }
                    },
                    {
                        extend: 'csv',
                        exportOptions: {
                            columns: ':visible'
                        }
                    },
                ]
            }

            return {
                autoWidth: false,
                language: {
                    processing: "<i class='fa fa-4x fa-spinner fa-spin'></i>"
                },
                //lengthMenu: [ [10, 25, 50, 100, -1], [10, 25, 50, 100, "All"] ],
                responsive: true,
                serverSide: true,
                searching: search,
                ajax: {
                    "url": this.url,
                    "dataSrc": 'data',

                    // adding extra GET params for Custom filtering
                    "data": function ( d ) {
                        d.filter_group_type = vm.group_type_name;
                        d.filter_scientific_name = vm.filterFaunaScientificName;
                        d.filter_common_name = vm.filterFaunaCommonName;
                        d.filter_phylogenetic_group = vm.filterFaunaPhylogeneticGroup;
                        d.filter_family = vm.filterFaunaFamily;
                        d.filter_genus = vm.filterFaunaGenus;
                        d.filter_conservation_list = vm.filterFaunaConservationList;
                        d.filter_conservation_category = vm.filterFaunaConservationCategory;
                        d.filter_region = vm.filterFaunaRegion;
                        d.filter_district = vm.filterFaunaDistrict;
                        d.is_internal = vm.is_internal;
                    }
                },
                dom: 'lBfrtip',
                buttons: buttons,

                columns: columns,
                processing: true,
                initComplete: function() {
                },
            }
        }
    
    },
    methods:{
        collapsible_component_mounted: function(){
            this.$refs.collapsible_filters.show_warning_icon(this.filterApplied)
        },

        fetchFilterLists: function(){
            let vm = this;

            vm.$http.get(api_endpoints.filter_lists_species+ '?group_type_name=' + vm.group_type_name).then((response) => {
                vm.filterListsSpecies = response.body;
                vm.species_data_list = vm.filterListsSpecies.species_data_list;
                vm.conservation_list_dict = vm.filterListsSpecies.conservation_list_dict;
                vm.conservation_category_list = vm.filterListsSpecies.conservation_category_list;
                //vm.proposal_status = vm.level == 'internal' ? response.body.processing_status_choices: response.body.customer_status_choices;
                //vm.proposal_status = vm.level == 'internal' ? vm.internal_status: vm.external_status;
            },(error) => {
                console.log(error);
            })
            vm.$http.get(api_endpoints.region_district_filter_dict).then((response) => {
                vm.filterRegionDistrict= response.body;
                vm.region_list= vm.filterRegionDistrict.region_list;
                vm.district_list= vm.filterRegionDistrict.district_list;
            },(error) => {
                console.log(error);
            })
        },

        discardProposal:function (proposal_id) {
            let vm = this;
            swal({
                title: "Discard Application",
                text: "Are you sure you want to discard this proposal?",
                type: "warning",
                showCancelButton: true,
                confirmButtonText: 'Discard Application',
                confirmButtonColor:'#d9534f'
            }).then(() => {
                vm.$http.delete(api_endpoints.discard_proposal(proposal_id))
                .then((response) => {
                    swal(
                        'Discarded',
                        'Your proposal has been discarded',
                        'success'
                    )
                    vm.$refs.fauna_datatable.vmDataTable.ajax.reload();
                }, (error) => {
                    console.log(error);
                });
            },(error) => {

            });
        },
        addEventListeners: function(){
            let vm = this;
            // External Discard listener
            vm.$refs.fauna_datatable.vmDataTable.on('click', 'a[data-discard-proposal]', function(e) {
                e.preventDefault();
                var id = $(this).attr('data-discard-proposal');
                vm.discardProposal(id);
            });
        },
        initialiseSearch:function(){
            this.submitterSearch();
        },
        submitterSearch:function(){
            let vm = this;
            vm.$refs.fauna_datatable.table.dataTableExt.afnFiltering.push(
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
                vm.is_payment_admin=response.body.is_payment_admin;
                              
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
            vm.initialiseSearch();
            vm.addEventListeners();
        });
    },
}
</script>
<style scoped>
.dt-buttons{
    float: right;
}
</style>
