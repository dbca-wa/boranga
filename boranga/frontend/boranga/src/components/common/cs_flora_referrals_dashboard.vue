<template id="species_flora_cs_referrals_dashboard">
    <div>
        <CollapsibleFilters component_title="Filters" ref="collapsible_filters" @created="collapsible_component_mounted" class="mb-2">
            <div class="row">
                <div class="col-md-3">
                    <div class="form-group">
                        <!-- <label for="">Scientific Name:</label>
                        <select class="form-select" v-model="filterCSRefFloraScientificName">
                            <option value="all">All</option>
                            <option v-for="option in scientific_name_list" :value="option.id">{{option.name}}
                            </option>
                        </select> -->
                        <label for="cs_ref_scientific_name_lookup">Scientific Name:</label>
                        <select 
                            id="cs_ref_scientific_name_lookup"  
                            name="cs_ref_scientific_name_lookup"  
                            ref="cs_ref_scientific_name_lookup" 
                            class="form-control" /> 
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <!-- <label for="">Common Name:</label>
                        <select class="form-select" v-model="filterCSRefFloraCommonName">
                            <option value="all">All</option>
                            <option v-for="option in common_name_list" :value="option.id">{{option.name}}</option>
                        </select> -->
                        <label for="cs_ref_common_name_lookup">Common Name:</label>
                        <select 
                            id="cs_ref_common_name_lookup"  
                            name="cs_ref_common_name_lookup"  
                            ref="cs_ref_common_name_lookup" 
                            class="form-control" />
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="cs_ref_phylo_group_lookup">Phylo Group:</label>
                        <select 
                            id="cs_ref_phylo_group_lookup"  
                            name="cs_ref_phylo_group_lookup"  
                            ref="cs_ref_phylo_group_lookup" 
                            class="form-control" />
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <!-- <label for="">Family:</label>
                        <select class="form-select" v-model="filterCSRefFloraFamily">
                            <option value="all">All</option>
                            <option v-for="option in family_list" :value="option.id">{{option.name}}</option>
                        </select> -->
                        <label for="cs_ref_family_lookup">Family:</label>
                        <select 
                            id="cs_ref_family_lookup"  
                            name="cs_ref_family_lookup"  
                            ref="cs_ref_family_lookup" 
                            class="form-control" />
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <!-- <label for="">Genera:</label>
                        <select class="form-select" v-model="filterCSRefFloraGenus">
                            <option value="all">All</option>
                            <option v-for="option in genus_list" :value="option.id">{{option.name}}</option>
                        </select> -->
                        <label for="cs_ref_genera_lookup">Genera:</label>
                        <select 
                            id="cs_ref_genera_lookup"  
                            name="cs_ref_genera_lookup"  
                            ref="cs_ref_genera_lookup" 
                            class="form-control" />
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="">Conservation List:</label>
                        <select class="form-select" v-model="filterCSRefFloraConservationList" 
                        @change="filterConservationCategory($event)">
                            <option value="all">All</option>
                            <option v-for="list in conservation_list_dict" :value="list.id">{{list.code}}</option>
                        </select>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="">Conservation Category:</label>
                        <select class="form-select" v-model="filterCSRefFloraConservationCategory">
                            <option value="all">All</option>
                            <option v-for="list in filtered_conservation_category_list" :value="list.id">{{list.code}}</option>
                        </select>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="">Status:</label>
                        <select class="form-select" v-model="filterCSRefFloraApplicationStatus">
                            <option value="all">All</option>
                            <option v-for="status in proposal_status" :value="status.value">{{ status.name }}</option>
                        </select>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="">Region:</label>
                        <select class="form-select" v-model="filterCSRefFloraRegion"
                        @change="filterDistrict($event)">
                            <option value="all">All</option>
                            <option v-for="region in region_list" :value="region.id" v-bind:key="region.id">{{region.name}}</option>
                        </select>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="">District:</label>
                        <select class="form-select" v-model="filterCSRefFloraDistrict">
                            <option value="all">All</option>
                            <option v-for="district in filtered_district_list" :value="district.id">{{district.name}}</option>
                        </select>
                    </div>
                </div>
            </div>
        </CollapsibleFilters>

        <div class="row">
            <div class="col-lg-12">
                <datatable
                        ref="flora_cs_ref_datatable"
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
import Vue from 'vue'
// var select2 = require('select2');
// require("select2/dist/css/select2.min.css");
// require("select2-bootstrap-5-theme/dist/select2-bootstrap-5-theme.min.css")
import {
    api_endpoints,
    helpers
}from '@/utils/hooks'
export default {
    name: 'CSReferralsFloraTable',
    props: {
        group_type_name:{
            type: String,
            required: true
        },
        group_type_id:{
            type: Number,
            required: true,
            default:0
        },
        url:{
            type: String,
            required: true
        },
        filterCSRefFloraScientificName_cache: {
            type: String,
            required: false,
            default: 'filterCSRefFloraScientificName',
        },
        filterCSRefFloraCommonName_cache: {
            type: String,
            required: false,
            default: 'filterCSRefFloraCommonName',
        },
        filterCSRefFloraPhylogeneticGroup_cache: {
            type: String,
            required: false,
            default: 'filterCSRefFloraPhylogeneticGroup',
        },
        filterCSRefFloraFamily_cache: {
            type: String,
            required: false,
            default: 'filterCSRefFloraFamily',
        },
        filterCSRefFloraGenus_cache: {
            type: String,
            required: false,
            default: 'filterCSRefFloraGenus',
        },
        filterCSRefFloraConservationList_cache: {
            type: String,
            required: false,
            default: 'filterCSRefFloraConservationList',
        },
        filterCSRefFloraConservationCategory_cache: {
            type: String,
            required: false,
            default: 'filterCSRefFloraConservationCategory',
        },
        filterCSRefFloraRegion_cache: {
            type: String,
            required: false,
            default: 'filterCSRefFloraRegion',
        },
        filterCSRefFloraDistrict_cache: {
            type: String,
            required: false,
            default: 'filterCSRefFloraDistrict',
        },
        filterCSRefFloraApplicationStatus_cache: {
            type: String,
            required: false,
            default: 'filterCSRefFloraApplicationStatus',
        },
    },
    data() {
        let vm = this;
        return {
            datatable_id: 'species_flora_cs_ref-datatable-'+vm._uid,
     
            // selected values for filtering
            //filterCSRefFloraScientificName:null,
            filterCSRefFloraScientificName: sessionStorage.getItem(this.filterCSRefFloraScientificName_cache) ? 
                                   sessionStorage.getItem(this.filterCSRefFloraScientificName_cache) : 'all',

            filterCSRefFloraCommonName: sessionStorage.getItem(this.filterCSRefFloraCommonName_cache) ? 
                                    sessionStorage.getItem(this.filterCSRefFloraCommonName_cache) : 'all',
            
            filterCSRefFloraPhylogeneticGroup: sessionStorage.getItem(this.filterCSRefFloraPhylogeneticGroup_cache) ? 
                                    sessionStorage.getItem(this.filterCSRefFloraPhylogeneticGroup_cache) : 'all',

            filterCSRefFloraFamily: sessionStorage.getItem(this.filterCSRefFloraFamily_cache) ? 
                                sessionStorage.getItem(this.filterCSRefFloraFamily_cache) : 'all',

            filterCSRefFloraGenus: sessionStorage.getItem(this.filterCSRefFloraGenus_cache) ? 
                                sessionStorage.getItem(this.filterCSRefFloraGenus_cache) : 'all',

            filterCSRefFloraConservationList: sessionStorage.getItem(this.filterCSRefFloraConservationList_cache) ? 
                                    sessionStorage.getItem(this.filterCSRefFloraConservationList_cache) : 'all',

            filterCSRefFloraConservationCategory: sessionStorage.getItem(this.filterCSRefFloraConservationCategory_cache) ? 
                                    sessionStorage.getItem(this.filterCSRefFloraConservationCategory_cache) : 'all',

            filterCSRefFloraRegion: sessionStorage.getItem(this.filterCSRefFloraRegion_cache) ? 
                                    sessionStorage.getItem(this.filterCSRefFloraRegion_cache) : 'all',

            filterCSRefFloraDistrict: sessionStorage.getItem(this.filterCSRefFloraDistrict_cache) ? 
                                    sessionStorage.getItem(this.filterCSRefFloraDistrict_cache) : 'all',

            filterCSRefFloraApplicationStatus: sessionStorage.getItem(this.filterCSRefFloraApplicationStatus_cache) ?
                                    sessionStorage.getItem(this.filterCSRefFloraApplicationStatus_cache) : 'all',

            //Filter list for scientific name and common name
            filterListsSpecies: {},
            scientific_name_list: [],
            common_name_list: [],
            family_list: [],
            genus_list: [],
            conservation_list_dict: [],
            conservation_category_list: [],
            filtered_conservation_category_list: [],
            filterRegionDistrict: {},
            region_list: [],
            district_list: [],
            proposal_status: [],
            filtered_district_list: [],
        }
    },
    components:{
        datatable,
        CollapsibleFilters,
    },
    watch:{
        filterCSRefFloraScientificName: function(){
            let vm = this;
            vm.$refs.flora_cs_ref_datatable.vmDataTable.ajax.reload(helpers.enablePopovers,false); // This calls ajax() backend call.
            sessionStorage.setItem(vm.filterCSRefFloraScientificName_cache, vm.filterCSRefFloraScientificName);  
        },
        filterCSRefFloraCommonName: function() {
            let vm = this;
            vm.$refs.flora_cs_ref_datatable.vmDataTable.ajax.reload(helpers.enablePopovers,false); // This calls ajax() backend call.
            sessionStorage.setItem(vm.filterCSRefFloraCommonName_cache, vm.filterCSRefFloraCommonName);  
        },
        filterCSRefFloraPhylogeneticGroup: function() {
            let vm = this;
            vm.$refs.flora_cs_ref_datatable.vmDataTable.ajax.reload(helpers.enablePopovers,false); // This calls ajax() backend call.
            sessionStorage.setItem(vm.filterCSRefFloraPhylogeneticGroup_cache, vm.filterCSRefFloraPhylogeneticGroup);  
        },
        filterCSRefFloraFamily: function() {
            let vm = this;
            vm.$refs.flora_cs_ref_datatable.vmDataTable.ajax.reload(helpers.enablePopovers,false); // This calls ajax() backend call.
            sessionStorage.setItem(vm.filterCSRefFloraFamily_cache, vm.filterCSRefFloraFamily);  
        },
        filterCSRefFloraGenus: function() {
            let vm = this;
            vm.$refs.flora_cs_ref_datatable.vmDataTable.ajax.reload(helpers.enablePopovers,false); // This calls ajax() backend call.
            sessionStorage.setItem(vm.filterCSRefFloraGenus_cache, vm.filterCSRefFloraGenus);  
        },
        filterCSRefFloraConservationList: function() {
            let vm = this;
            vm.$refs.flora_cs_ref_datatable.vmDataTable.ajax.reload(helpers.enablePopovers,false); // This calls ajax() backend call.  
            sessionStorage.setItem(vm.filterCSRefFloraConservationList_cache, vm.filterCSRefFloraConservationList);
        },
        filterCSRefFloraConservationCategory: function() {
            let vm = this;
            vm.$refs.flora_cs_ref_datatable.vmDataTable.ajax.reload(helpers.enablePopovers,false); // This calls ajax() backend call.  
            sessionStorage.setItem(vm.filterCSRefFloraConservationCategory_cache, vm.filterCSRefFloraConservationCategory);
        },
        filterCSRefFloraRegion: function(){
            let vm = this;
            vm.$refs.flora_cs_ref_datatable.vmDataTable.ajax.reload(helpers.enablePopovers,false); // This calls ajax() backend call.
            sessionStorage.setItem(vm.filterCSRefFloraRegion_cache, vm.filterCSRefFloraRegion);
        },
        filterCSRefFloraDistrict: function(){
            let vm = this;
            vm.$refs.flora_cs_ref_datatable.vmDataTable.ajax.reload(helpers.enablePopovers,false); // This calls ajax() backend call.
            sessionStorage.setItem(vm.filterCSRefFloraDistrict_cache, vm.filterCSRefFloraDistrict);
        },
        filterCSRefFloraApplicationStatus: function() {
            let vm = this;
            vm.$refs.flora_cs_ref_datatable.vmDataTable.ajax.reload(helpers.enablePopovers,false); // This calls ajax() backend call.  
            sessionStorage.setItem(vm.filterCSRefFloraApplicationStatus_cache, vm.filterCSRefFloraApplicationStatus);
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
            if(this.filterCSRefFloraScientificName === 'all' && 
                this.filterCSRefFloraCommonName === 'all' && 
                this.filterCSRefFloraPhylogeneticGroup === 'all' && 
                this.filterCSRefFloraFamily === 'all' && 
                this.filterCSRefFloraGenus === 'all' && 
                this.filterCSRefFloraConservationList === 'all' && 
                this.filterCSRefFloraConservationCategory === 'all' && 
                this.filterCSRefFloraRegion === 'all' && 
                this.filterCSRefFloraDistrict === 'all' && 
                this.filterCSRefFloraApplicationStatus === 'all'){
                return false
            } else {
                return true
            }
        },
        datatable_headers: function(){
            return ['Number','Species','Scientific Name','Conservation List', 
            'Conservation Category', 'Family', 'Genera' /*, 'Region', 'District'*/, 'Status', 'Action']
        },
        column_number: function(){
            return {
                data: "conservation_status_number",
                orderable: true,
                searchable: true,
                visible: true,
                'render': function(data, type, full){
                    let tick='';
                            if (full.can_be_processed){
                                tick = "<i class='fa fa-exclamation-circle' style='color:#FFBF00'></i>";
                            }
                            else
                            {
                                tick = "<i class='fa fa-check-circle' style='color:green'></i>";
                            }
                    return full.conservation_status_number+tick;
                },
                name: "conservation_status__id, conservation_status__conservation_status_number",
            }
        },
        column_species_number: function(){
            return {
                data: "species_number",
                orderable: true,
                searchable: true,
                visible: true,
                'render': function(data, type, full){
                    return full.species_number
                },
                name: "conservation_status__species__species_number",
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
                name: "conservation_status__species__taxonomy__scientific_name",
            }
        },
        /*column_common_name: function(){
            return {
                data: "common_name",
                orderable: true,
                searchable: true,
                visible: true,
                'render': function(value, type){
                    let result = helpers.dtPopover(value, 30, 'hover');
                    return type=='export' ? value : result;
                },
                //'createdCell': helpers.dtPopoverCellFn,
                name: "conservation_status__species__taxonomy__vernaculars__vernacular_name",
            }
        },*/
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
                name: "conservation_status__conservation_list__code",
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
                name: "conservation_status__conservation_category__code",
            }
        },
        column_family: function(){
            return {
                data: "family",
                orderable: true,
                searchable: true,
                visible: true,
                'render': function(value, type){
                    let result = helpers.dtPopover(value, 30, 'hover');
                    return type=='export' ? value : result;
                },
                //'createdCell': helpers.dtPopoverCellFn,
                name: "species__taxonomy__family__name",
            }
        },
        column_genera: function(){
            return {
                data: "genus",
                orderable: true,
                searchable: true,
                visible: true,
                'render': function(value, type){
                    let result = helpers.dtPopover(value, 30, 'hover');
                    return type=='export' ? value : result;
                },
                //'createdCell': helpers.dtPopoverCellFn,
                name: "species__taxonomy__genus__name",
            }
        },
        column_status: function(){
            return {
                // 9. Workflow Status
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
                name: "conservation_status__processing_status",
            }
        },
        /*column_region: function(){
            return {
                data: "region",
                orderable: true,
                searchable: true, // handles by filter_queryset override method
                visible: true,
                'render': function(data, type, full){
                    if (full.region){
                        return full.region
                    }
                    // Should not reach here
                    return ''
                },
                name: "conservation_status__species__region__name",
            }
        },
        column_district: function(){
            return {
                data: "district",
                orderable: true,
                searchable: true, // handles by filter_queryset override method
                visible: true,
                'render': function(data, type, full){
                    if (full.district){
                        return full.district
                    }
                    // Should not reach here
                    return ''
                },
                name: "conservation_status__species__district__name",
            }
        },*/
        column_action: function(){
            let vm = this
            return {
                // 10. Action
                data: "id",
                orderable: false,
                searchable: false,
                visible: true,
                'render': function(data, type, full){
                    let links = '';
                    /*links +=  full.can_user_process ? `<a href='/internal/conservation_status/${full.conservation_status}/referral/${full.id}'>Process</a><br/>`: `<a href='/internal/conservation_status/${full.conservation_status}/referral/${full.id}'>View</a><br/>`;*/
                     links +=  full.can_be_processed ? `<a href='/internal/conservation_status/${full.conservation_status}/referral/${full.id}'>Process</a><br/>`: `<a href='/internal/conservation_status/${full.conservation_status}/referral/${full.id}'>View</a><br/>`;
                    return links;
                },
            }
        },
        column_conservation_status: function(){  
            let vm = this
            return{
                data: "conservation_status", 
                visible: false,
            }
        },
        column_can_be_processed: function(){  
            let vm = this
            return{
                data: "can_be_processed", 
                visible: false,
            }
        },
        column_can_user_process: function(){  
            let vm = this
            return{
                data: "can_user_process", 
                visible: false,
            }
        },
        datatable_options: function(){
            let vm = this

            let columns = [
                vm.column_number,
                vm.column_species_number,
                vm.column_scientific_name,
                /*vm.column_common_name,*/
                vm.column_conservation_list,
                vm.column_conservation_category,
                vm.column_family,
                vm.column_genera,
                /*vm.column_region,
                vm.column_district,*/
                vm.column_conservation_status,
                vm.column_can_be_processed,
                vm.column_can_user_process,
                vm.column_status,
                vm.column_action,
            ]
            let search = false
            let buttons = [
                {
                    text: '<i class="fa-solid fa-download"></i> Excel',
                    className: 'btn btn-primary ml-2',
                    action: function (e, dt, node, config) {
                        vm.exportData("excel");
                    }
                },
                {
                    text: '<i class="fa-solid fa-download"></i> CSV',
                    className: 'btn btn-primary',
                    action: function (e, dt, node, config) {
                        vm.exportData("csv");
                    }
                }
            ]
            
            return {
                autoWidth: false,
                language: {
                    processing: "<b>Loading...</b>"
                },
                order: [
                    [0, 'desc']
                ],
                lengthMenu: [ [10, 25, 50, 100, -1], [10, 25, 50, 100, "All"] ],
                responsive: true,
                serverSide: true,
                searching: true,
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
                        d.filter_group_type = vm.group_type_name;
                        d.filter_scientific_name = vm.filterCSRefFloraScientificName;
                        d.filter_common_name = vm.filterCSRefFloraCommonName;
                        d.filter_phylogenetic_group = vm.filterCSRefFloraPhylogeneticGroup;
                        d.filter_family = vm.filterCSRefFloraFamily;
                        d.filter_genus = vm.filterCSRefFloraGenus;
                        d.filter_conservation_list = vm.filterCSRefFloraConservationList;
                        d.filter_conservation_category = vm.filterCSRefFloraConservationCategory;
                        d.filter_region = vm.filterCSRefFloraRegion;
                        d.filter_district = vm.filterCSRefFloraDistrict;
                        d.filter_application_status = vm.filterCSRefFloraApplicationStatus;
                        //d.is_internal = vm.is_internal;
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
                $(vm.$refs.cs_ref_scientific_name_lookup).select2({
                    minimumInputLength: 2,
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
                                group_type_id: vm.group_type_id,
                                cs_referral: true,
                            }
                            return query;
                        },
                    },
                }).
                on("select2:select", function (e) {
                    var selected = $(e.currentTarget);
                    let data = e.params.data.id;
                    vm.filterCSRefFloraScientificName = data;
                    sessionStorage.setItem("filterCSRefFloraScientificNameText", e.params.data.text);
                }).
                on("select2:unselect",function (e) {
                    var selected = $(e.currentTarget);
                    vm.filterCSRefFloraScientificName = 'all';
                    sessionStorage.setItem("filterCSRefFloraScientificNameText",'');
                }).
                on("select2:open",function (e) {
                    const searchField = $('[aria-controls="select2-cs_ref_scientific_name_lookup-results"]')
                    // move focus to select2 field
                    searchField[0].focus();
                });
        },
        initialiseCommonNameLookup: function(){
                let vm = this;
                $(vm.$refs.cs_ref_common_name_lookup).select2({
                    minimumInputLength: 2,
                    "theme": "bootstrap-5",
                    allowClear: true,
                    placeholder:"Select Common Name",
                    ajax: {
                        url: api_endpoints.common_name_lookup,
                        dataType: 'json',
                        data: function(params) {
                            var query = {
                                term: params.term,
                                type: 'public',
                                group_type_id: vm.group_type_id,
                                cs_referral: true,
                            }
                            return query;
                        },
                    },
                }).
                on("select2:select", function (e) {
                    var selected = $(e.currentTarget);
                    let data = e.params.data.id;
                    vm.filterCSRefFloraCommonName = data;
                    sessionStorage.setItem("filterCSRefFloraCommonNameText", e.params.data.text);
                }).
                on("select2:unselect",function (e) {
                    var selected = $(e.currentTarget);
                    vm.filterCSRefFloraCommonName = 'all';
                    sessionStorage.setItem("filterCSRefFloraCommonNameText",'');
                }).
                on("select2:open",function (e) {
                    const searchField = $('[aria-controls="select2-cs_ref_common_name_lookup-results"]')
                    // move focus to select2 field
                    searchField[0].focus();
                });
        },
        initialisePhyloGroupLookup: function(){
                let vm = this;
                $(vm.$refs.cs_ref_phylo_group_lookup).select2({
                    minimumInputLength: 2,
                    "theme": "bootstrap-5",
                    allowClear: true,
                    placeholder:"Select Phylo Group",
                    ajax: {
                        url: api_endpoints.phylo_group_lookup,
                        dataType: 'json',
                        data: function(params) {
                            var query = {
                                term: params.term,
                                type: 'public',
                                group_type_id: vm.group_type_id,
                                cs_referral: true,
                            }
                            return query;
                        },
                    },
                }).
                on("select2:select", function (e) {
                    var selected = $(e.currentTarget);
                    let data = e.params.data.id;
                    vm.filterCSRefFloraPhylogeneticGroup = data;
                    sessionStorage.setItem("filterCSRefFloraPhylogeneticGroupText", e.params.data.text);
                }).
                on("select2:unselect",function (e) {
                    var selected = $(e.currentTarget);
                    vm.filterCSRefFloraPhylogeneticGroup = 'all';
                    sessionStorage.setItem("filterCSRefFloraPhylogeneticGroupText",'');
                }).
                on("select2:open",function (e) {
                    const searchField = $('[aria-controls="select2-cs_ref_phylo_group_lookup-results"]')
                    // move focus to select2 field
                    searchField[0].focus();
                });
        },
        initialiseFamilyLookup: function(){
                let vm = this;
                $(vm.$refs.cs_ref_family_lookup).select2({
                    minimumInputLength: 2,
                    "theme": "bootstrap-5",
                    allowClear: true,
                    placeholder:"Select Family",
                    ajax: {
                        url: api_endpoints.family_lookup,
                        dataType: 'json',
                        data: function(params) {
                            var query = {
                                term: params.term,
                                type: 'public',
                                group_type_id: vm.group_type_id,
                                cs_referral: true,
                            }
                            return query;
                        },
                    },
                }).
                on("select2:select", function (e) {
                    var selected = $(e.currentTarget);
                    let data = e.params.data.id;
                    vm.filterCSRefFloraFamily = data;
                    sessionStorage.setItem("filterCSRefFloraFamilyText", e.params.data.text);
                }).
                on("select2:unselect",function (e) {
                    var selected = $(e.currentTarget);
                    vm.filterCSRefFloraFamily = 'all';
                    sessionStorage.setItem("filterCSRefFloraFamilyText",'');
                }).
                on("select2:open",function (e) {
                    //const searchField = $(".select2-search__field")
                    const searchField = $('[aria-controls="select2-cs_ref_family_lookup-results"]')
                    // move focus to select2 field
                    searchField[0].focus();
                });
        },
        initialiseGeneraLookup: function(){
                let vm = this;
                $(vm.$refs.cs_ref_genera_lookup).select2({
                    minimumInputLength: 2,
                    "theme": "bootstrap-5",
                    allowClear: true,
                    placeholder:"Select Genera",
                    ajax: {
                        url: api_endpoints.genera_lookup,
                        dataType: 'json',
                        data: function(params) {
                            var query = {
                                term: params.term,
                                type: 'public',
                                group_type_id: vm.group_type_id,
                                cs_referral: true,
                            }
                            return query;
                        },
                    },
                }).
                on("select2:select", function (e) {
                    var selected = $(e.currentTarget);
                    let data = e.params.data.id;
                    vm.filterCSRefFloraGenus = data;
                    sessionStorage.setItem("filterCSRefFloraGenusText", e.params.data.text);
                }).
                on("select2:unselect",function (e) {
                    var selected = $(e.currentTarget);
                    vm.filterCSRefFloraGenus = 'all';
                    sessionStorage.setItem("filterCSRefFloraGenusText",'');
                }).
                on("select2:open",function (e) {
                    //const searchField = $(".select2-search__field")
                    const searchField = $('[aria-controls="select2-cs_ref_genera_lookup-results"]')
                    // move focus to select2 field
                    searchField[0].focus();
                });
        },
        fetchFilterLists: function(){
            let vm = this;
            //large FilterList of Species Values object
            vm.$http.get(api_endpoints.filter_list_cs_referrals+ '?group_type_name=' + vm.group_type_name).then((response) => {
                vm.filterListsSpecies = response.body;
                vm.scientific_name_list = vm.filterListsSpecies.scientific_name_list;
                vm.common_name_list = vm.filterListsSpecies.common_name_list;
                vm.family_list = vm.filterListsSpecies.family_list;
                vm.genus_list = vm.filterListsSpecies.genus_list;
                vm.conservation_list_dict = vm.filterListsSpecies.conservation_list_dict;
                vm.conservation_category_list = vm.filterListsSpecies.conservation_category_list;
                vm.filterConservationCategory();
                vm.filterDistrict();
                vm.proposal_status = vm.filterListsSpecies.processing_status_list;
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
        //-------filter category dropdown dependent on conservation_list selected
        filterConservationCategory: function(event) {
                //this.$nextTick(() => {
                    if(event){
                      this.filterCSRefFloraConservationCategory='all'; //-----to remove the previous selection
                    }
                    this.filtered_conservation_category_list=[];
                    //---filter conservation_categories as per cons_list selected
                    for(let choice of this.conservation_category_list){
                        if(choice.conservation_list_id.toString() === this.filterCSRefFloraConservationList.toString())
                        {
                          this.filtered_conservation_category_list.push(choice);
                        }
                    }
                //});
        },
         //-------filter district dropdown dependent on region selected
         filterDistrict: function(event) {
                this.$nextTick(() => {
                    if(event){
                      this.filterCSRefFloraDistrict='all'; //-----to remove the previous selection
                    }
                    this.filtered_district_list=[];
                    //---filter districts as per region selected
                    for(let choice of this.district_list){
                        if(choice.region_id.toString() === this.filterCSRefFloraRegion.toString())
                        {
                          this.filtered_district_list.push(choice);
                        }
                        
                    }
                });
        },
        addEventListeners: function(){
            let vm = this;
        },
        initialiseSearch:function(){
            this.submitterSearch();
        },
        submitterSearch:function(){
            let vm = this;
            vm.$refs.flora_cs_ref_datatable.table.dataTableExt.afnFiltering.push(
                function(settings,data,dataIndex,original){
                    let filtered_submitter = vm.filterProposalSubmitter;
                    if (filtered_submitter == 'All'){ return true; } 
                    return filtered_submitter == original.submitter.email;
                }
            );
        },
        exportData: function (format) {
            let vm = this;
            const columns_new = {
                "0":{
                    "data":"conservation_status_number",
                    "name":"conservation_status__id, conservation_status__conservation_status_number",
                    "searchable":"true",
                    "orderable":"true",
                    "search":{
                        "value":"",
                        "regex":"false"
                    }
                },
                "1":{
                    "data":"species_number",
                    "name":"conservation_status__species__species_number",
                    "searchable":"true",
                    "orderable":"true",
                    "search":{
                        "value":"",
                        "regex":"false"
                    }
                },
                "2":{
                    "data":"scientific_name",
                    "name":"conservation_status__species__taxonomy__scientific_name",
                    "searchable":"true",
                    "orderable":"true",
                    "search":{
                        "value":"",
                        "regex":"false"
                    }
                },
                "3":{
                    "data":"conservation_list",
                    "name":"conservation_status__conservation_list__code",
                    "searchable":"true",
                    "orderable":"true",
                    "search":{
                        "value":"",
                        "regex":"false"
                    }
                },
                "4":{
                    "data":"conservation_category",
                    "name":"conservation_status__conservation_category__code",
                    "searchable":"true",
                    "orderable":"true",
                    "search":{
                        "value":"",
                        "regex":"false"
                    }
                },
                "5":{
                    "data":"family",
                    "name":"species__taxonomy__family__name",
                    "searchable":"true",
                    "orderable":"true",
                    "search":{
                        "value":"",
                        "regex":"false"
                    }
                },
                "6":{
                    "data":"genus",
                    "name":"species__taxonomy__genus__name",
                    "searchable":"true",
                    "orderable":"true",
                    "search":{
                        "value":"",
                        "regex":"false"
                    }
                },
                "7":{
                    "data":"processing_status",
                    "name":"conservation_status__processing_status",
                    "searchable":"true",
                    "orderable":"true",
                    "search":{
                        "value":"",
                        "regex":"false"
                    }
                },
                "8":{
                    "data":"id",
                    "name":"",
                    "searchable":"false",
                    "orderable":"false",
                    "search":{
                        "value":"",
                        "regex":"false"
                    }
                },
                // "9":{
                //     "data":"conservation_status",
                //     "name":"",
                //     "searchable":"true",
                //     "orderable":"true",
                //     "search":{
                //         "value":"",
                //         "regex":"false"
                //     }
                // },
            };

            const object_load = {
                columns: columns_new,
                filter_group_type: vm.group_type_name,
                filter_scientific_name: vm.filterCSRefFloraScientificName,
                filter_common_name: vm.filterCSRefFloraScientificName,
                filter_family: vm.filterCSRefFloraFamily,
                filter_genus: vm.filterCSRefFloraGenus,
                filter_conservation_list: vm.filterCSRefFloraConservationList,
                filter_conservation_category: vm.filterCSRefFloraConservationCategory,
                filter_application_status: vm.filterCSRefFloraApplicationStatus,
                filter_region: vm.filterCSRefFloraRegion,
                filter_district: vm.filterCSRefFloraDistrict,
                is_internal: vm.is_internal,
                export_format: format
            };

            const url = api_endpoints.species_cs_referrals_internal_export;
            const keyValuePairs = [];

            for (const key in object_load) {
                if (object_load.hasOwnProperty(key)) {
                    const encodedKey = encodeURIComponent(key);
                    let encodedValue = '';

                    if (typeof object_load[key] === 'object') {
                        encodedValue = encodeURIComponent(JSON.stringify(object_load[key]));
                    }
                    else {
                        encodedValue = encodeURIComponent(object_load[key]);
                    }
                    keyValuePairs.push(`${encodedKey}=${encodedValue}`);
                }
            }
            const params = keyValuePairs.join('&');
            const fullUrl = `${url}?${params}`;
            try {
                if (format === "excel") {
                    $.ajax({
                        type: "GET",
                        url: fullUrl,
                        contentType: "application/vnd.ms-excel",
                        dataType: "binary",
                        xhrFields: {
                            responseType: 'blob'
                        },

                        success: function (response, status, request) {
                            var contentDispositionHeader = request.getResponseHeader('Content-Disposition');
                            var filename = contentDispositionHeader.split('filename=')[1];
                            window.URL = window.URL || window.webkitURL;
                            var blob = new Blob([response], { type: "application/vnd.ms-excel" });

                            var downloadUrl = window.URL.createObjectURL(blob);
                            var a = document.createElement("a");
                            a.href = downloadUrl;
                            a.download = filename;
                            document.body.appendChild(a);
                            a.click();
                            document.body.removeChild(a);
                        },
                        error: function (xhr, status, error) {
                            console.log(error);
                        },
                    });
                }
                else if (format === "csv") {
                    $.ajax({
                        type: "GET",
                        url: fullUrl,
                        success: function (response, status, request) {
                            var contentDispositionHeader = request.getResponseHeader('Content-Disposition');
                            var filename = contentDispositionHeader.split('filename=')[1];
                            window.URL = window.URL || window.webkitURL;
                            var blob = new Blob([response], { type: "text/csv" });

                            var downloadUrl = window.URL.createObjectURL(blob);
                            var a = document.createElement("a");
                            a.href = downloadUrl;
                            a.download = filename;
                            document.body.appendChild(a);
                            a.click();
                            document.body.removeChild(a);
                        },
                        error: function (xhr, status, error) {
                            console.log(error);
                        },
                    });
                }
            }
            catch (err) {
                console.log(err);
                if (vm.is_internal) {
                    return err;
                }
            }
        },
    },
    mounted: function(){
        this.fetchFilterLists();
        let vm = this;
        $( 'a[data-toggle="collapse"]' ).on( 'click', function () {
            var chev = $( this ).children()[ 0 ];
            window.setTimeout( function () {
                $( chev ).toggleClass( "glyphicon-chevron-down glyphicon-chevron-up" );
            }, 100 );
        });
        this.$nextTick(() => {
            vm.initialiseScientificNameLookup();
            vm.initialiseCommonNameLookup();
            vm.initialisePhyloGroupLookup();
            vm.initialiseFamilyLookup();
            vm.initialiseGeneraLookup();
            vm.initialiseSearch();
            vm.addEventListeners();
            // -- to set the select2 field with the session value if exists onload()
            if(sessionStorage.getItem("filterCSRefFloraScientificName")!='all' && sessionStorage.getItem("filterCSRefFloraScientificName")!=null)
            {
                // contructor new Option(text, value, defaultSelected, selected)
                var newOption = new Option(sessionStorage.getItem("filterCSRefFloraScientificNameText"), vm.filterCSRefFloraScientificName, false, true);
                $('#cs_ref_scientific_name_lookup').append(newOption);
            }
            if(sessionStorage.getItem("filterCSRefFloraCommonName")!='all' && sessionStorage.getItem("filterCSRefFloraCommonName")!=null)
            {
                // contructor new Option(text, value, defaultSelected, selected)
                var newOption = new Option(sessionStorage.getItem("filterCSRefFloraCommonNameText"), vm.filterCSRefFloraCommonName, false, true);
                $('#cs_ref_common_name_lookup').append(newOption);
            }
            if(sessionStorage.getItem("filterCSRefFloraPhylogeneticGroup")!='all' && sessionStorage.getItem("filterCSRefFloraPhylogeneticGroup")!=null)
            {
                // contructor new Option(text, value, defaultSelected, selected)
                var newOption = new Option(sessionStorage.getItem("filterCSRefFloraPhylogeneticGroupText"), vm.filterCSRefFloraPhylogeneticGroup, false, true);
                $('#cs_ref_phylo_group_lookup').append(newOption);
            }
            if(sessionStorage.getItem("filterCSRefFloraFamily")!='all' && sessionStorage.getItem("filterCSRefFloraFamily")!=null)
            {
                // contructor new Option(text, value, defaultSelected, selected)
                var newOption = new Option(sessionStorage.getItem("filterCSRefFloraFamilyText"), vm.filterCSRefFloraFamily, false, true);
                $('#cs_ref_family_lookup').append(newOption);
            }
            if(sessionStorage.getItem("filterCSRefFloraGenus")!='all' && sessionStorage.getItem("filterCSRefFloraGenus")!=null)
            {
                // contructor new Option(text, value, defaultSelected, selected)
                var newOption = new Option(sessionStorage.getItem("filterCSRefFloraGenusText"), vm.filterCSRefFloraGenus, false, true);
                $('#cs_ref_genera_lookup').append(newOption);
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
