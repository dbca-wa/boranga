<template id="cs_communities_dashboard">
    <div>
        <CollapsibleFilters component_title="Filters" ref="collapsible_filters" @created="collapsible_component_mounted" class="mb-2">
            <div class="row">
                <div class="col-md-3">
                    <div class="form-group" id="select_community_id">
                        <label for="cs_community_id_lookup">Community ID:</label>
                        <select 
                            id="cs_community_id_lookup"  
                            name="cs_community_id_lookup"  
                            ref="cs_community_id_lookup" 
                            class="form-control" />
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group" id="select_community_name">
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
                        <select class="form-select" v-model="filterCSCommunityConservationList"
                        @change="filterConservationCategory($event)">
                            <option value="all">All</option>
                            <option v-for="list in conservation_list_dict" :value="list.id">{{list.code}}</option>
                        </select>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="">Conservation Category:</label>
                        <select class="form-select" v-model="filterCSCommunityConservationCategory">
                            <option value="all">All</option>
                            <option v-for="list in filtered_conservation_category_list" :value="list.id">{{list.code}}</option>
                        </select>
                    </div>
                </div>
                <div class="col-md-3" v-show="!is_for_agenda">
                    <div class="form-group">
                        <label for="">Status:</label>
                        <select class="form-select" v-model="filterCSCommunityApplicationStatus">
                            <option value="all">All</option>
                            <option v-for="status in proposal_status" :value="status.value">{{ status.name }}</option>
                        </select>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="">Region:</label>
                        <select class="form-select" v-model="filterCSCommunityRegion"
                        @change="filterDistrict($event)">
                            <option value="all">All</option>
                            <option v-for="region in region_list" :value="region.id" v-bind:key="region.id">{{region.name}}</option>
                        </select>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="">District:</label>
                        <select class="form-select" v-model="filterCSCommunityDistrict">
                            <option value="all">All</option>
                            <option v-for="district in filtered_district_list" :value="district.id">{{district.name}}</option>
                        </select>
                    </div>
                </div>
                <div class="col-md-3" v-show="!is_for_agenda">
                    <div class="form-group">
                        <label for="">Effective From Date:</label>
                        <input type="date" class="form-control" placeholder="DD/MM/YYYY" id="effective_from_date" v-model="filterCSCommunityEffectiveFromDate">
                    </div>
                </div>
                <div class="col-md-3" v-show="!is_for_agenda">
                    <div class="form-group">
                        <label for="">Effective To Date:</label>
                        <input type="date" class="form-control" placeholder="DD/MM/YYYY" id="effective_from_date" v-model="filterCSCommunityEffectiveToDate">
                    </div>
                </div>
            </div>
        </CollapsibleFilters>

        <div v-if="addCommunityCSVisibility && is_for_agenda==false" class="col-md-12">
            <div class="text-end">
                <button type="button" class="btn btn-primary mb-2 " @click.prevent="createCommunityConservationStatus"><i class="fa-solid fa-circle-plus"></i> Add Conservation Satus</button>
            </div>
        </div>

        <div class="row">
            <div class="col-lg-12">
                <datatable
                        ref="cs_communities_datatable"
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
    name: 'ConservationStatusCommunityTable',
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
        group_type_id:{
            type: Number,
            required: true,
            default: 0
        },
        url:{
            type: String,
            required: true
        },
        // when the datable need to be shown for agenda_items in meeting check this variable is true
        is_for_agenda:{
            type: Boolean,
            default:false
        },
        // for adding agendaitems for the meeting_obj.id
        meeting_obj:{
            type: Object,
            required:false
        },
        filterCSCommunityMigratedId_cache: {
            type: String,
            required: false,
            default: 'filterCSCommunityMigratedId',
        },
        filterCSCommunityName_cache: {
            type: String,
            required: false,
            default: 'filterCSCommunityName',
        },
        filterCSCommunityConservationList_cache: {
            type: String,
            required: false,
            default: 'filterCSCommunityConservationList',
        },
        filterCSCommunityConservationCategory_cache: {
            type: String,
            required: false,
            default: 'filterCSCommunityConservationCategory',
        },
        filterCSCommunityRegion_cache: {
            type: String,
            required: false,
            default: 'filterCSCommunityRegion',
        },
        filterCSCommunityDistrict_cache: {
            type: String,
            required: false,
            default: 'filterCSCommunityDistrict',
        },
        filterCSCommunityApplicationStatus_cache: {
            type: String,
            required: false,
            default: 'filterCSCommunityApplicationStatus',
        },
        filterCSCommunityEffectiveFromDate_cache: {
            type: String,
            required: false,
            default: 'filterCSCommunityEffectiveFromDate',
        },
        filterCSCommunityEffectiveToDate_cache: {
            type: String,
            required: false,
            default: 'filterCSCommunityEffectiveToDate',
        },
    },
    data() {
        let vm = this;
        return {
            datatable_id: 'cs-communities-datatable-'+vm._uid,
     
            //Profile to check if user has access to process Proposal
            profile: {},
            is_payment_admin: false,
            
            // selected values for filtering
            filterCSCommunityMigratedId: sessionStorage.getItem(this.filterCSCommunityMigratedId_cache) ? 
                                sessionStorage.getItem(this.filterCSCommunityMigratedId_cache) : 'all',

            filterCSCommunityName: sessionStorage.getItem(this.filterCSCommunityName_cache) ? 
                                    sessionStorage.getItem(this.filterCSCommunityName_cache) : 'all',

            filterCSCommunityConservationList: sessionStorage.getItem(this.filterCSCommunityConservationList_cache) ? 
                                    sessionStorage.getItem(this.filterCSCommunityConservationList_cache) : 'all',

            filterCSCommunityConservationCategory: sessionStorage.getItem(this.filterCSCommunityConservationCategory_cache) ? 
                                    sessionStorage.getItem(this.filterCSCommunityConservationCategory_cache) : 'all',

            filterCSCommunityRegion: sessionStorage.getItem(this.filterCSCommunityRegion_cache) ? 
                                    sessionStorage.getItem(this.filterCSCommunityRegion_cache) : 'all',

            filterCSCommunityDistrict: sessionStorage.getItem(this.filterCSCommunityDistrict_cache) ? 
                                        sessionStorage.getItem(this.filterCSCommunityDistrict_cache) : 'all',

            filterCSCommunityApplicationStatus: sessionStorage.getItem(this.filterCSCommunityApplicationStatus_cache) ?
                                    sessionStorage.getItem(this.filterCSCommunityApplicationStatus_cache) : 'all',

            filterCSCommunityEffectiveFromDate: sessionStorage.getItem(this.filterCSCommunityEffectiveFromDate_cache) ?
                                    sessionStorage.getItem(this.filterCSCommunityEffectiveFromDate_cache) : '',

            filterCSCommunityEffectiveToDate: sessionStorage.getItem(this.filterCSCommunityEffectiveToDate_cache) ?
                                    sessionStorage.getItem(this.filterCSCommunityEffectiveToDate_cache) : '',

            //Filter list for Community select box
            filterListsCommunities: {},
            communities_data_list: [],
            conservation_list_dict: [],
            conservation_category_list: [],
            filtered_conservation_category_list: [],
            filterRegionDistrict: {},
            region_list: [],
            district_list: [],
            filtered_district_list: [],
            
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
                {value: 'ready_for_agenda', name: 'Ready For Agenda'},
                // {value: 'with_approver', name: 'With Approver'},
                {value: 'with_referral', name: 'With Referral'},
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
        filterCSCommunityMigratedId: function(){
            let vm = this;
            vm.$refs.cs_communities_datatable.vmDataTable.ajax.reload(helpers.enablePopovers,false); // This calls ajax() backend call.  
            sessionStorage.setItem(vm.filterCSCommunityMigratedId_cache, vm.filterCSCommunityMigratedId);
        },
        filterCSCommunityName: function() {
            let vm = this;
            vm.$refs.cs_communities_datatable.vmDataTable.ajax.reload(helpers.enablePopovers,false); // This calls ajax() backend call.  
            sessionStorage.setItem(vm.filterCSCommunityName_cache, vm.filterCSCommunityName);
        },
        filterCSCommunityConservationList: function() {
            let vm = this;
            vm.$refs.cs_communities_datatable.vmDataTable.ajax.reload(helpers.enablePopovers,false); // This calls ajax() backend call.  
            sessionStorage.setItem(vm.filterCSCommunityConservationList_cache, vm.filterCSCommunityConservationList);
        },
        filterCSCommunityConservationCategory: function() {
            let vm = this;
            vm.$refs.cs_communities_datatable.vmDataTable.ajax.reload(helpers.enablePopovers,false); // This calls ajax() backend call.  
            sessionStorage.setItem(vm.filterCSCommunityConservationCategory_cache, 
                vm.filterCSCommunityConservationCategory);
        },
        filterCSCommunityRegion: function(){
            let vm = this;
            vm.$refs.cs_communities_datatable.vmDataTable.ajax.reload(helpers.enablePopovers,false); // This calls ajax() backend call.
            sessionStorage.setItem(vm.filterCSCommunityRegion_cache, vm.filterCSCommunityRegion);
        },
        filterCSCommunityDistrict: function(){
            let vm = this;
            vm.$refs.cs_communities_datatable.vmDataTable.ajax.reload(helpers.enablePopovers,false); // This calls ajax() backend call.
            sessionStorage.setItem(vm.filterCSCommunityDistrict_cache, vm.filterCSCommunityDistrict);
        },
        filterCSCommunityEffectiveFromDate: function(){
            let vm = this;
            vm.$refs.cs_communities_datatable.vmDataTable.ajax.reload(helpers.enablePopovers,false); // This calls ajax() backend call.
            sessionStorage.setItem(vm.filterCSCommunityEffectiveFromDate_cache, vm.filterCSCommunityEffectiveFromDate);
        },
        filterCSCommunityEffectiveToDate: function(){
            let vm = this;
            vm.$refs.cs_communities_datatable.vmDataTable.ajax.reload(helpers.enablePopovers,false); // This calls ajax() backend call.
            sessionStorage.setItem(vm.filterCSCommunityEffectiveToDate_cache, vm.filterCSCommunityEffectiveToDate);
        },
        filterCSCommunityApplicationStatus: function() {
            let vm = this;
            vm.$refs.cs_communities_datatable.vmDataTable.ajax.reload(helpers.enablePopovers,false); // This calls ajax() backend call.  
            sessionStorage.setItem(vm.filterCSCommunityApplicationStatus_cache, vm.filterCSCommunityApplicationStatus);
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
            if(this.filterCSCommunityMigratedId === 'all' && 
                this.filterCSCommunityName === 'all' && 
                this.filterCSCommunityConservationList === 'all' && 
                this.filterCSCommunityConservationCategory === 'all' && 
                this.filterCSCommunityRegion === 'all' && 
                this.filterCSCommunityDistrict === 'all' && 
                this.filterCSCommunityApplicationStatus === 'all' &&
                this.filterCSCommunityEffectiveFromDate === '' &&
                this.filterCSCommunityEffectiveToDate === ''){
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
        addCommunityCSVisibility: function() {
            let visibility = false;
            if (this.is_internal) {
                visibility = true;
            }
            return visibility;
        },
        datatable_headers: function(){
            if (this.is_external){
                return ['Number', 'Community','Community Id' ,'Community Name', 'Conservation List' , 'Conservation Category', 'Region', 'District', 'Effective From Date', 'Effective To Date', 'Status', 'Action']
            }
            if (this.is_internal){
                return ['Number', 'Community','Community Id' ,'Community Name', 'Conservation List', 'Conservation Category', 'Region', 'District', 'Effective From Date', 'Effective To Date', 'Status', 'Action']
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
        column_community_number: function(){
            return {
                data: "community_number",
                orderable: true,
                searchable: true,
                visible: true,
                'render': function(data, type, full){
                    return full.community_number
                },
                name: "community__community_number",
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
                name: "community__taxonomy__community_migrated_id",
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
                name: "community__taxonomy__community_name",
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
                name: "conservation_list__code",
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
                name: "conservation_category__code",
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
                name: "processing_status",
            }
        },
        column_region: function(){
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
                name: "community__region__name",
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
                name: "community__district__name",
            }
        },
        column_effective_from_date: function(){
            return {
                data: "effective_from_date",
                orderable: true,
                searchable: true, // handles by filter_queryset override method
                visible: true,
                'render': function(data, type, full){
                    if (full.effective_from_date){
                        return full.effective_from_date
                    }
                    // Should not reach here
                    return ''
                },
                name: "conservationstatusissuanceapprovaldetails__effective_from_date",
            }
        },
        column_effective_to_date: function(){
            return {
                data: "effective_to_date",
                orderable: true,
                searchable: true, // handles by filter_queryset override method
                visible: true,
                'render': function(data, type, full){
                    if (full.effective_to_date){
                        return full.effective_to_date
                    }
                    // Should not reach here
                    return ''
                },
                name: "conservationstatusissuanceapprovaldetails__effective_to_date",
            }
        },
        column_action: function(){
            let vm = this
            return {
                // 9. Action
                data: "id",
                orderable: false,
                searchable: false,
                visible: true,
                'render': function(data, type, full){
                    let links = "";
                    if(vm.is_for_agenda==false){
                        if (!vm.is_external){
                            /*if(vm.check_assessor(full) && full.can_officer_process)*/
                            if(full.internal_user_edit)
                            {
                                links +=  `<a href='/internal/conservation_status/${full.id}'>Continue</a><br/>`;
                                links +=  `<a href='#${full.id}' data-discard-cs-proposal='${full.id}'>Discard</a><br/>`;
                            }
                            else{
                                if(full.assessor_process){
                                        links +=  `<a href='/internal/conservation_status/${full.id}'>Process</a><br/>`;
                                }
                                else{
                                    if(full.assessor_edit){
                                        links +=  `<a href='/internal/conservation_status/${full.id}?action=edit'>Edit</a><br/>`;
                                    }
                                    links +=  `<a href='/internal/conservation_status/${full.id}?action=view'>View</a><br/>`;
                                }
                            }
                        }
                    }
                    else{
                        if(vm.meeting_obj.agenda_items_arr.includes(full.id)){
                            links +=  `<a>Added</a><br/>`;
                        }
                        else{
                            links +=  `<a href='#${full.id}' data-add-to-agenda='${full.id}'>Add</a><br/>`;
                        }
                    }
                    return links;
                }
            }
        },
        datatable_options: function(){
            let vm = this

            let columns = []
            let search = null
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
            if(vm.is_external){
                columns = [
                    vm.column_number,
                    vm.column_community_number,
                    vm.column_community_id,
                    vm.column_community_name,
                    vm.column_conservation_list,
                    vm.column_conservation_category,
                    vm.column_region,
                    vm.column_district,
                    vm.column_effective_from_date,
                    vm.column_effective_to_date,
                    vm.column_status,
                    vm.column_action,
                ]
                search = false
            }
            if(vm.is_internal){
                columns = [
                    vm.column_number,
                    vm.column_community_number,
                    vm.column_community_id,
                    vm.column_community_name,
                    vm.column_conservation_list,
                    vm.column_conservation_category,
                    vm.column_region,
                    vm.column_district,
                    vm.column_effective_from_date,
                    vm.column_effective_to_date,
                    vm.column_status,
                    vm.column_action,
                ]
                search = true
            }

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
                        d.filter_community_migrated_id = vm.filterCSCommunityMigratedId;
                        d.filter_community_name = vm.filterCSCommunityName;
                        d.filter_conservation_list = vm.filterCSCommunityConservationList;
                        d.filter_conservation_category = vm.filterCSCommunityConservationCategory;
                        d.filter_group_type = vm.group_type_name;
                        d.filter_region = vm.filterCSCommunityRegion;
                        d.filter_district = vm.filterCSCommunityDistrict;
                        d.filter_application_status = vm.filterCSCommunityApplicationStatus;
                        d.filter_effective_from_date = vm.filterCSCommunityEffectiveFromDate;
                        d.filter_effective_to_date = vm.filterCSCommunityEffectiveToDate;
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
                $(vm.$refs.cs_community_name_lookup).select2({
                    minimumInputLength: 2,
                    dropdownParent: $("#select_community_name"),
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
                            }
                            return query;
                        },
                    },
                }).
                on("select2:select", function (e) {
                    var selected = $(e.currentTarget);
                    let data = e.params.data.id;
                    vm.filterCSCommunityName = data;
                    sessionStorage.setItem("filterCSCommunityNameText", e.params.data.text);
                }).
                on("select2:unselect",function (e) {
                    var selected = $(e.currentTarget);
                    vm.filterCSCommunityName = 'all';
                    sessionStorage.setItem("filterCSCommunityNameText",'');
                }).
                on("select2:open",function (e) {
                    const searchField = $('[aria-controls="select2-cs_community_name_lookup-results"]')
                    // move focus to select2 field
                    searchField[0].focus();
                });
        },
        initialiseCommunityIdLookup: function(){
                let vm = this;
                $(vm.$refs.cs_community_id_lookup).select2({
                    minimumInputLength: 1,
                    dropdownParent: $("#select_community_id"),
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
                            }
                            return query;
                        },
                    },
                }).
                on("select2:select", function (e) {
                    var selected = $(e.currentTarget);
                    let data = e.params.data.id;
                    vm.filterCSCommunityMigratedId = data;
                    sessionStorage.setItem("filterCSCommunityMigratedIdText", e.params.data.text);
                }).
                on("select2:unselect",function (e) {
                    var selected = $(e.currentTarget);
                    vm.filterCSCommunityMigratedId = 'all';
                    sessionStorage.setItem("filterCSCommunityMigratedIdText",'');
                }).
                on("select2:open",function (e) {
                    const searchField = $('[aria-controls="select2-cs_community_id_lookup-results"]')
                    // move focus to select2 field
                    searchField[0].focus();
                });
        },
        fetchFilterLists: function(){
            let vm = this;

            vm.$http.get(api_endpoints.community_filter_dict+ '?group_type_name=' + vm.group_type_name).then((response) => {
                vm.filterListsCommunities= response.body;
                vm.communities_data_list= vm.filterListsCommunities.community_data_list;
                vm.conservation_list_dict = vm.filterListsCommunities.conservation_list_dict;
                vm.conservation_category_list = vm.filterListsCommunities.conservation_category_list;
                vm.filterConservationCategory();
                vm.filterDistrict();
                vm.proposal_status = vm.internal_status.slice().sort((a, b) => {
                    return a.name.trim().localeCompare(b.name.trim());
                });
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
        //-------filter category dropdown dependent on conservation_list selected
        filterConservationCategory: function(event) {
                this.$nextTick(() => {
                    if(event){
                      this.filterCSCommunityConservationCategory='all'; //-----to remove the previous selection
                    }
                    this.filtered_conservation_category_list=[];
                    //---filter conservation_categories as per cons_list selected
                    for(let choice of this.conservation_category_list){
                        if(choice.conservation_list_id.toString() === this.filterCSCommunityConservationList.toString())
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
                      this.filterCSCommunityDistrict='all'; //-----to remove the previous selection
                    }
                    this.filtered_district_list=[];
                    //---filter districts as per region selected
                    for(let choice of this.district_list){
                        if(choice.region_id.toString() === this.filterCSCommunityRegion.toString())
                        {
                          this.filtered_district_list.push(choice);
                        }
                        
                    }
                });
        },
        createCommunityConservationStatus: async function () {
            let newCommunityCSId = null
            try {
                    const createUrl = api_endpoints.conservation_status+"/";
                    let payload = new Object();
                    payload.application_type_id = this.group_type_id
                    payload.internal_application = true
                    let savedCommunityCS = await Vue.http.post(createUrl, payload);
                    if (savedCommunityCS) {
                        newCommunityCSId = savedCommunityCS.body.id;
                    }
                }
            catch (err) {
                console.log(err);
                if (this.is_internal) {
                    return err;
                }
            }
            this.$router.push({
                name: 'internal-conservation_status',
                params: {conservation_status_id: newCommunityCSId},
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
                    vm.$refs.cs_communities_datatable.vmDataTable.ajax.reload(helpers.enablePopovers,false);
                }, (error) => {
                    console.log(error);
                });
            },(error) => {

            });
        },
        addToMeetingAgenda:function (conservation_status_id) {
            let vm=this;
            let payload = new Object();
            payload.conservation_status_id = conservation_status_id;
            Vue.http.post(`/api/meeting/${vm.meeting_obj.id}/add_agenda_item.json`,payload).then(res => {
                vm.meeting_obj.agenda_items_arr=res.body;
                vm.$refs.cs_communities_datatable.vmDataTable.ajax.reload(helpers.enablePopovers,false);
                this.$emit('updateAgendaItems');
            },
            err => {
              console.log(err);
            });
        },
        addEventListeners: function(){
            let vm = this;
            // internal Discard listener
            vm.$refs.cs_communities_datatable.vmDataTable.on('click', 'a[data-discard-cs-proposal]', function(e) {
                e.preventDefault();
                var id = $(this).attr('data-discard-cs-proposal');
                vm.discardCSProposal(id);
            });
            vm.$refs.cs_communities_datatable.vmDataTable.on('click', 'a[data-add-to-agenda]', function(e) {
                e.preventDefault();
                var id = $(this).attr('data-add-to-agenda');
                vm.addToMeetingAgenda(id);
            });
        },
        initialiseSearch:function(){
            this.submitterSearch();
        },
        submitterSearch:function(){
            let vm = this;
            vm.$refs.cs_communities_datatable.table.dataTableExt.afnFiltering.push(
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
                    "name":"conservation_status__community__community_migrated_id",
                    "searchable":"true",
                    "orderable":"true",
                    "search":{
                        "value":"",
                        "regex":"false"
                    }
                },
                "3":{
                    "data":"community_name",
                    "name":"conservation_status__community__community_name__name",
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
                    "data":"family",
                    "name":"species__taxonomy__family__name",
                    "searchable":"true",
                    "orderable":"true",
                    "search":{
                        "value":"",
                        "regex":"false"
                    }
                },
                "7":{
                    "data":"genus",
                    "name":"species__taxonomy__genus__name",
                    "searchable":"true",
                    "orderable":"true",
                    "search":{
                        "value":"",
                        "regex":"false"
                    }
                },
                "8":{
                    "data":"processing_status",
                    "name":"conservation_status__processing_status",
                    "searchable":"true",
                    "orderable":"true",
                    "search":{
                        "value":"",
                        "regex":"false"
                    }
                },
                "9":{
                    "data":"id",
                    "name":"",
                    "searchable":"false",
                    "orderable":"false",
                    "search":{
                        "value":"",
                        "regex":"false"
                    }
                },
                "10":{
                    "data":"conservation_status",
                    "name":"",
                    "searchable":"true",
                    "orderable":"true",
                    "search":{
                        "value":"",
                        "regex":"false"
                    }
                },
            };

            const object_load = {
                columns: columns_new,
                filter_community_migrated_id: vm.filterCSCommunityMigratedId,
                filter_group_type: vm.group_type_name,
                filter_community_name: vm.filterCSCommunityName,
                filter_conservation_list: vm.filterCSCommunityConservationList,
                filter_conservation_category: vm.filterCSCommunityConservationCategory,
                filter_application_status: vm.filterCSCommunityApplicationStatus,
                filter_region: vm.filterCSCommunityRegion,
                filter_district: vm.filterCSCommunityDistrict,
                filter_effective_from_date: vm.filterCSCommunityEffectiveFromDate,
                filter_effective_to_date: vm.filterCSCommunityEffectiveToDate,
                is_internal: vm.is_internal,
                export_format: format
            };

            const url = api_endpoints.community_cs_internal_export;
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
        this.fetchProfile();
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
            //vm.initialiseSearch();
            vm.addEventListeners();
            // -- to set the select2 field with the session value if exists onload()
            if(sessionStorage.getItem("filterCSCommunityName")!='all' && sessionStorage.getItem("filterCSCommunityName")!=null)
            {
                // contructor new Option(text, value, defaultSelected, selected)
                var newOption = new Option(sessionStorage.getItem("filterCSCommunityNameText"), vm.filterCSCommunityName, false, true);
                $('#cs_community_name_lookup').append(newOption);
            }
            if(sessionStorage.getItem("filterCSCommunityMigratedId")!='all' && sessionStorage.getItem("filterCSCommunityMigratedId")!=null)
            {
                // contructor new Option(text, value, defaultSelected, selected)
                var newOption = new Option(sessionStorage.getItem("filterCSCommunityMigratedIdText"), vm.filterCSCommunityMigratedId, false, true);
                $('#cs_community_id_lookup').append(newOption);
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
