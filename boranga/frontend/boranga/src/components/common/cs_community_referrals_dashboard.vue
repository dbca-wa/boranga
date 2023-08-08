<template id="cs_communities_referrals_dashboard">
    <div>
        <CollapsibleFilters component_title="Filters" ref="collapsible_filters" @created="collapsible_component_mounted" class="mb-2">
            <div class="row">
                <div class="col-md-3">
                    <div class="form-group">
                        <!-- <label for="">Community ID:</label>
                        <select class="form-select" v-model="filterCSRefCommunityMigratedId">
                            <option value="all">All</option>
                            <option v-for="option in communities_data_list" :value="option.id">
                                {{option.community_migrated_id}}
                            </option>
                        </select> -->
                        <label for="cs_ref_community_id_lookup">Community ID:</label>
                        <select 
                            id="cs_ref_community_id_lookup"  
                            name="cs_ref_community_id_lookup"  
                            ref="cs_ref_community_id_lookup" 
                            class="form-control" />
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <!-- <label for="">Community Name:</label>
                        <select class="form-select" v-model="filterCSRefCommunityName">
                            <option value="all">All</option>
                            <option v-for="option in communities_data_list" :value="option.id">
                                {{option.community_name}}
                            </option>
                        </select> -->
                        <label for="cs_ref_community_name_lookup">Community Name:</label>
                        <select 
                            id="cs_ref_community_name_lookup"  
                            name="cs_ref_community_name_lookup"  
                            ref="cs_ref_community_name_lookup" 
                            class="form-control" />
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="">Conservation List:</label>
                        <select class="form-select" v-model="filterCSRefCommunityConservationList"
                        @change="filterConservationCategory($event)">
                            <option value="all">All</option>
                            <option v-for="list in conservation_list_dict" :value="list.id">{{list.code}}</option>
                        </select>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="">Conservation Category:</label>
                        <select class="form-select" v-model="filterCSRefCommunityConservationCategory">
                            <option value="all">All</option>
                            <option v-for="list in filtered_conservation_category_list" :value="list.id">{{list.code}}</option>
                        </select>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="">Status:</label>
                        <select class="form-select" v-model="filterCSRefCommunityApplicationStatus">
                            <option value="all">All</option>
                            <option v-for="status in proposal_status" :value="status.value">{{ status.name }}</option>
                        </select>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="">Region:</label>
                        <select class="form-select" v-model="filterCSRefCommunityRegion"
                        @change="filterDistrict($event)">
                            <option value="all">All</option>
                            <option v-for="region in region_list" :value="region.id" v-bind:key="region.id">{{region.name}}</option>
                        </select>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="">District:</label>
                        <select class="form-select" v-model="filterCSRefCommunityDistrict">
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
                        ref="cs_communities_ref_datatable"
                        :id="datatable_id"
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
import {
    api_endpoints,
    helpers
}from '@/utils/hooks'
export default {
    name: 'CSReferralsCommunityTable',
    props: {
        group_type_name:{
            type: String,
            required: true
        },
        group_type_id:{
            type: Number,
            required: true,
            default: 0
        },
        url:{
            type: String,
            required: true
        },
        filterCSRefCommunityMigratedId_cache: {
            type: String,
            required: false,
            default: 'filterCSRefCommunityMigratedId',
        },
        filterCSRefCommunityName_cache: {
            type: String,
            required: false,
            default: 'filterCSRefCommunityName',
        },
        filterCSRefCommunityConservationList_cache: {
            type: String,
            required: false,
            default: 'filterCSRefCommunityConservationList',
        },
        filterCSRefCommunityConservationCategory_cache: {
            type: String,
            required: false,
            default: 'filterCSRefCommunityConservationCategory',
        },
        filterCSRefCommunityRegion_cache: {
            type: String,
            required: false,
            default: 'filterCSRefCommunityRegion',
        },
        filterCSRefCommunityDistrict_cache: {
            type: String,
            required: false,
            default: 'filterCSRefCommunityDistrict',
        },
        filterCSRefCommunityApplicationStatus_cache: {
            type: String,
            required: false,
            default: 'filterCSRefCommunityApplicationStatus',
        },
    },
    data() {
        let vm = this;
        return {
            datatable_id: 'cs-communities-ref-datatable-'+vm._uid,
     
            // selected values for filtering
            filterCSRefCommunityMigratedId: sessionStorage.getItem(this.filterCSRefCommunityMigratedId_cache) ? 
                                sessionStorage.getItem(this.filterCSRefCommunityMigratedId_cache) : 'all',

            filterCSRefCommunityName: sessionStorage.getItem(this.filterCSRefCommunityName_cache) ? 
                                    sessionStorage.getItem(this.filterCSRefCommunityName_cache) : 'all',

            filterCSRefCommunityConservationList: sessionStorage.getItem(this.filterCSRefCommunityConservationList_cache) ?sessionStorage.getItem(this.filterCSRefCommunityConservationList_cache) : 'all',

            filterCSRefCommunityConservationCategory: 
                sessionStorage.getItem(this.filterCSRefCommunityRefConservationCategory_cache) ? 
                sessionStorage.getItem(this.filterCSRefCommunityRefConservationCategory_cache) : 'all',

            filterCSRefCommunityRegion: sessionStorage.getItem(this.filterCSRefCommunityRegion_cache) ? 
                                    sessionStorage.getItem(this.filterCSRefCommunityRegion_cache) : 'all',

            filterCSRefCommunityDistrict: sessionStorage.getItem(this.filterCSRefCommunityDistrict_cache) ? 
                                        sessionStorage.getItem(this.filterCSRefCommunityDistrict_cache) : 'all',

            filterCSRefCommunityApplicationStatus: 
                    sessionStorage.getItem(this.filterCSRefCommunityApplicationStatus_cache) ?
                    sessionStorage.getItem(this.filterCSRefCommunityApplicationStatus_cache) : 'all',

            //Filter list for Community select box
            filterListsCommunities: {},
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
        FormSection,
    },
    watch:{
        filterCSRefCommunityMigratedId: function(){
            let vm = this;
            vm.$refs.cs_communities_ref_datatable.vmDataTable.ajax.reload(helpers.enablePopovers,false); // This calls ajax() backend call.  
            sessionStorage.setItem(vm.filterCSRefCommunityMigratedId_cache, vm.filterCSRefCommunityMigratedId);
        },
        filterCSRefCommunityName: function() {
            let vm = this;
            vm.$refs.cs_communities_ref_datatable.vmDataTable.ajax.reload(helpers.enablePopovers,false); // This calls ajax() backend call.  
            sessionStorage.setItem(vm.filterCSRefCommunityName_cache, vm.filterCSRefCommunityName);
        },
        filterCSRefCommunityConservationList: function() {
            let vm = this;
            vm.$refs.cs_communities_ref_datatable.vmDataTable.ajax.reload(helpers.enablePopovers,false); // This calls ajax() backend call.  
            sessionStorage.setItem(vm.filterCSRefCommunityConservationList_cache, vm.filterCSRefCommunityConservationList);
        },
        filterCSRefCommunityConservationCategory: function() {
            let vm = this;
            vm.$refs.cs_communities_ref_datatable.vmDataTable.ajax.reload(helpers.enablePopovers,false); // This calls ajax() backend call.  
            sessionStorage.setItem(vm.filterCSRefCommunityConservationCategory_cache, 
                vm.filterCSRefCommunityConservationCategory);
        },
        filterCSRefCommunityRegion: function(){
            let vm = this;
            vm.$refs.cs_communities_ref_datatable.vmDataTable.ajax.reload(helpers.enablePopovers,false); // This calls ajax() backend call.
            sessionStorage.setItem(vm.filterCSRefCommunityRegion_cache, vm.filterCSRefCommunityRegion);
        },
        filterCSRefCommunityDistrict: function(){
            let vm = this;
            vm.$refs.cs_communities_ref_datatable.vmDataTable.ajax.reload(helpers.enablePopovers,false); // This calls ajax() backend call.
            sessionStorage.setItem(vm.filterCSRefCommunityDistrict_cache, vm.filterCSRefCommunityDistrict);
        },
        filterCSRefCommunityApplicationStatus: function() {
            let vm = this;
            vm.$refs.cs_communities_ref_datatable.vmDataTable.ajax.reload(helpers.enablePopovers,false); // This calls ajax() backend call.  
            sessionStorage.setItem(
                vm.filterCSRefCommunityApplicationStatus_cache, vm.filterCSRefCommunityApplicationStatus
                );
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
            if(this.filterCSRefCommunityMigratedId === 'all' && 
                this.filterCSRefCommunityName === 'all' && 
                this.filterCSRefCommunityConservationList === 'all' && 
                this.filterCSRefCommunityConservationCategory === 'all' && 
                this.filterCSRefCommunityRegion === 'all' && 
                this.filterCSRefCommunityDistrict === 'all' && 
                this.filterCSRefCommunityApplicationStatus === 'all'){
                return false
            } else {
                return true
            }
        },
        datatable_headers: function(){
           return ['Number', 'Community','Community Id' ,'Community Name', 'Conservation List' , 'Conservation Category', /*'Region', 'District',*/ 'Status', 'Action']
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
        column_community_number: function(){
            return {
                data: "community_number",
                orderable: true,
                searchable: true,
                visible: true,
                'render': function(data, type, full){
                    return full.community_number
                },
                name: "conservation_status__community__community_number",
            }
        },
        column_community_id: function(){
            return {
                data: "community_migrated_id",
                orderable: true,
                searchable: true,
                visible: true,
                'render': function(value, type){
                    let result = helpers.dtPopover(value, 30, 'hover');
                    return type=='export' ? value : result;
                },
                name: "conservation_status__community__taxonomy__community_migrated_id",
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
                //'createdCell': helpers.dtPopoverCellFn,
                name: "conservation_status__community__taxonomy__community_name",
            }
        },
        column_conservation_list: function(){
            return {
                data: "conservation_list",
                orderable: true,
                searchable: true,
                visible: true,
                'render': function(value, type){
                    let result = helpers.dtPopover(value, 30, 'hover');
                    return type=='export' ? value : result;
                },
                //'createdCell': helpers.dtPopoverCellFn,
                name: "conservation_status__conservation_list__code",
            }
        },
        column_conservation_category: function(){
            return {
                data: "conservation_category",
                orderable: true,
                searchable: true,
                visible: true,
                'render': function(value, type){
                    let result = helpers.dtPopover(value, 30, 'hover');
                    return type=='export' ? value : result;
                },
                //'createdCell': helpers.dtPopoverCellFn,
                name: "conservation_status__conservation_category__code",
            }
        },
        column_status: function(){
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
                name: "conservation_status__processing_status",
            }
        },
        /*column_region: function(){
            return {
                data: "region",
                orderable: true,
                searchable: false,
                visible: true,
                'render': function(data, type, full){
                    if(full.region){
                        return full.region;
                    }
                    // Should not reach here
                    return ''
                },
                name: "conservation_status__community__region__name",
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
                name: "conservation_status__community__district__name",
            }
        },*/
        column_action: function(){
            let vm = this
            return {
                // 9. Action
                data: "id",
                orderable: false,
                searchable: false,
                visible: true,
                'render': function(data, type, full){
                    let links = '';
                    links +=  full.can_be_processed ? `<a href='/internal/conservation_status/${full.conservation_status}/referral/${full.id}'>Process</a><br/>`: `<a href='/internal/conservation_status/${full.conservation_status}/referral/${full.id}'>View</a><br/>`;
                    return links;
                }
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
                vm.column_community_number,
                vm.column_community_id,
                vm.column_community_name,
                vm.column_conservation_list,
                vm.column_conservation_category,
                // vm.column_region,
                // vm.column_district,
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
                searching: search,
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
                        d.filter_community_migrated_id = vm.filterCSRefCommunityMigratedId;
                        d.filter_community_name = vm.filterCSRefCommunityName;
                        d.filter_conservation_list = vm.filterCSRefCommunityConservationList;
                        d.filter_conservation_category = vm.filterCSRefCommunityConservationCategory;
                        d.filter_group_type = vm.group_type_name;
                        d.filter_region = vm.filterCSRefCommunityRegion;
                        d.filter_district = vm.filterCSRefCommunityDistrict;
                        d.filter_application_status = vm.filterCSRefCommunityApplicationStatus;
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
        initialiseCommunityNameLookup: function(){
                let vm = this;
                $(vm.$refs.cs_ref_community_name_lookup).select2({
                    minimumInputLength: 2,
                    "theme": "bootstrap-5",
                    allowClear: true,
                    placeholder:"Select Community Name",
                    ajax: {
                        url: api_endpoints.community_name_lookup,
                        dataType: 'json',
                        data: function(params) {
                            var query = {
                                term: params.term,
                                type: 'public',
                                cs_referral: true,
                            }
                            return query;
                        },
                    },
                }).
                on("select2:select", function (e) {
                    var selected = $(e.currentTarget);
                    let data = e.params.data.id;
                    vm.filterCSRefCommunityName = data;
                    sessionStorage.setItem("filterCSRefCommunityNameText", e.params.data.text);
                }).
                on("select2:unselect",function (e) {
                    var selected = $(e.currentTarget);
                    vm.filterCSRefCommunityName = 'all';
                    sessionStorage.setItem("filterCSRefCommunityNameText",'');
                }).
                on("select2:open",function (e) {
                    const searchField = $('[aria-controls="select2-cs_ref_community_name_lookup-results"]')
                    // move focus to select2 field
                    searchField[0].focus();
                });
        },
        initialiseCommunityIdLookup: function(){
                let vm = this;
                $(vm.$refs.cs_ref_community_id_lookup).select2({
                    minimumInputLength: 1,
                    "theme": "bootstrap-5",
                    allowClear: true,
                    placeholder:"Select Community ID",
                    ajax: {
                        url: api_endpoints.community_id_lookup,
                        dataType: 'json',
                        data: function(params) {
                            var query = {
                                term: params.term,
                                type: 'public',
                                cs_referral: true,
                            }
                            return query;
                        },
                    },
                }).
                on("select2:select", function (e) {
                    var selected = $(e.currentTarget);
                    let data = e.params.data.id;
                    vm.filterCSRefCommunityMigratedId = data;
                    sessionStorage.setItem("filterCSRefCommunityMigratedIdText", e.params.data.text);
                }).
                on("select2:unselect",function (e) {
                    var selected = $(e.currentTarget);
                    vm.filterCSRefCommunityMigratedId = 'all';
                    sessionStorage.setItem("filterCSRefCommunityMigratedIdText",'');
                }).
                on("select2:open",function (e) {
                    const searchField = $('[aria-controls="select2-cs_ref_community_id_lookup-results"]')
                    // move focus to select2 field
                    searchField[0].focus();
                });
        },
        fetchFilterLists: function(){
            let vm = this;

            vm.$http.get(api_endpoints.filter_list_cs_referrals_community+ '?group_type_name=' + vm.group_type_name).then((response) => {
                vm.filterListsCommunities= response.body;
                vm.conservation_list_dict = vm.filterListsCommunities.conservation_list_dict;
                vm.conservation_category_list = vm.filterListsCommunities.conservation_category_list;
                vm.filterConservationCategory();
                vm.filterDistrict();
                vm.proposal_status = vm.filterListsCommunities.processing_status_list
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
                this.$nextTick(() => {
                    if(event){
                      this.filterCSRefCommunityConservationCategory='all'; //-----to remove the previous selection
                    }
                    this.filtered_conservation_category_list=[];
                    //---filter conservation_categories as per cons_list selected
                    for(let choice of this.conservation_category_list){
                        if(choice.conservation_list_id.toString() === this.filterCSRefCommunityConservationList.toString())
                        {
                          this.filtered_conservation_category_list.push(choice);
                        }
                    }
                });
        },
          //-------filter district dropdown dependent on region selected
          filterDistrict: function(event) {
                this.$nextTick(() => {
                    if(event){
                      this.filterCSRefCommunityDistrict='all'; //-----to remove the previous selection
                    }
                    this.filtered_district_list=[];
                    //---filter districts as per region selected
                    for(let choice of this.district_list){
                        if(choice.region_id.toString() === this.filterCSRefCommunityRegion.toString())
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
            vm.$refs.cs_communities_ref_datatable.table.dataTableExt.afnFiltering.push(
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
                    "data":"community_number",
                    "name":"conservation_status__community__community_number",
                    "searchable":"true",
                    "orderable":"true",
                    "search":{
                        "value":"",
                        "regex":"false"
                    }
                },
                "2":{
                    "data":"community_migrated_id",
                    "name":"conservation_status__community__taxonomy__community_migrated_id",
                    "searchable":"true",
                    "orderable":"true",
                    "search":{
                        "value":"",
                        "regex":"false"
                    }
                },
                "3":{
                    "data":"community_name",
                    "name":"conservation_status__community__taxonomy__community_name",
                    "searchable":"true",
                    "orderable":"true",
                    "search":{
                        "value":"",
                        "regex":"false"
                    }
                },
                "4":{
                    "data":"conservation_list",
                    "name":"conservation_status__conservation_list__code",
                    "searchable":"true",
                    "orderable":"true",
                    "search":{
                        "value":"",
                        "regex":"false"
                    }
                },
                "5":{
                    "data":"conservation_category",
                    "name":"conservation_status__conservation_category__code",
                    "searchable":"true",
                    "orderable":"true",
                    "search":{
                        "value":"",
                        "regex":"false"
                    }
                },
                "6":{
                    "data":"processing_status",
                    "name":"conservation_status__processing_status",
                    "searchable":"true",
                    "orderable":"true",
                    "search":{
                        "value":"",
                        "regex":"false"
                    }
                },
                "7":{
                    "data":"id",
                    "name":"",
                    "searchable":"false",
                    "orderable":"false",
                    "search":{
                        "value":"",
                        "regex":"false"
                    }
                },
                // "10":{
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
                filter_community_migrated_id: vm.filterCSRefCommunityMigratedId,
                filter_group_type: vm.group_type_name,
                filter_community_name: vm.filterCSRefCommunityName,
                filter_conservation_list: vm.filterCSRefCommunityConservationList,
                filter_conservation_category: vm.filterCSRefCommunityConservationCategory,
                filter_application_status: vm.filterCSRefCommunityApplicationStatus,
                filter_region: vm.filterCSRefCommunityRegion,
                filter_district: vm.filterCSRefCommunityDistrict,
                is_internal: vm.is_internal,
                export_format: format
            };

            const url = api_endpoints.community_cs_referrals_internal_export;
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
            vm.initialiseCommunityNameLookup();
            vm.initialiseCommunityIdLookup();
            vm.initialiseSearch();
            vm.addEventListeners();
            // -- to set the select2 field with the session value if exists onload()
            if(sessionStorage.getItem("filterCSRefCommunityName")!='all' && sessionStorage.getItem("filterCSRefCommunityName")!=null)
            {
                // contructor new Option(text, value, defaultSelected, selected)
                var newOption = new Option(sessionStorage.getItem("filterCSRefCommunityNameText"), vm.filterCSRefCommunityName, false, true);
                $('#cs_ref_community_name_lookup').append(newOption);
            }
            if(sessionStorage.getItem("filterCSRefCommunityMigratedId")!='all' && sessionStorage.getItem("filterCSRefCommunityMigratedId")!=null)
            {
                // contructor new Option(text, value, defaultSelected, selected)
                var newOption = new Option(sessionStorage.getItem("filterCSRefCommunityMigratedIdText"), vm.filterCSRefCommunityMigratedId, false, true);
                $('#cs_ref_community_id_lookup').append(newOption);
            }
        });
    }
}
</script>
<style scoped>
.dt-buttons{
    float: right;
}
</style>
