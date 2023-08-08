<template lang="html">
    <div id="community_threats">
        <FormSection :formCollapse="false" label="Threats" Index="threats">
            <form class="form-horizontal" action="index.html" method="post">
                <div class="col-sm-12">
                    <div class="text-end">
                        <button type="button" class="btn btn-primary mb-2 " @click.prevent="newThreat">
                            <i class="fa-solid fa-circle-plus"></i>
                                Add Threat
                        </button>
                    </div>
                </div>
                <div>
                    <datatable ref="threats_datatable" :id="panelBody" :dtOptions="threats_options"
                    :dtHeaders="threats_headers"/>
                </div>
            </form>
        </FormSection>
        <ThreatDetail ref="threat_detail" @refreshFromResponse="refreshFromResponse" :url="threat_url"></ThreatDetail>
    </div>
</template>
<script>
import Vue from 'vue' 
import datatable from '@vue-utils/datatable.vue';
import ThreatDetail from './add_threat.vue'
import FormSection from '@/components/forms/section_toggle.vue';
import {
  api_endpoints,
  helpers,
}
from '@/utils/hooks'


export default {
        name: 'CommunityThreats',
        props:{
            species_community:{
                type: Object,
                required:true
            },
        },
        data:function () {
            let vm = this;
            return{
                uuid:0,
                panelBody: "community-threats-"+vm._uid,
                values:null,
                threat_url: api_endpoints.threat,
                threats_headers:['Number','Category', 'Threat Source', 'Date Observed', 'Threat Agent', 'Comments',
                                'Current Impact', 'Potential Impact','Action'],
                threats_options:{
                    autowidth: false,
                    language:{
                        processing: "<i class='fa fa-4x fa-spinner'></i>"
                    },
                    responsive: true,
                    searching: true,
                    //  to show the "workflow Status","Action" columns always in the last position
                    columnDefs: [
                        { responsivePriority: 1, targets: 0 },
                        { responsivePriority: 2, targets: -1 },
                    ],
                    ajax:{
                        "url": helpers.add_endpoint_json(api_endpoints.community,vm.species_community.id+'/threats'),
                        "dataSrc": ''
                    },
                    order: [],
                    dom: "<'d-flex align-items-center'<'me-auto'l>fB>" +
                     "<'row'<'col-sm-12'tr>>" +
                     "<'d-flex align-items-center'<'me-auto'i>p>",
                    buttons:[
                        {
                            extend: 'excel',
                            text: '<i class="fa-solid fa-download"></i> Excel',
                            className: 'btn btn-primary ml-2',
                            exportOptions: {
                                orthogonal: 'export' 
                            }
                        },
                        {
                            extend: 'csv',
                            text: '<i class="fa-solid fa-download"></i> CSV',
                            className: 'btn btn-primary',
                            exportOptions: {
                                orthogonal: 'export' 
                            }
                        },
                    ],
                    columns: [
                        {
                            data: "threat_number",
                            orderable: true,
                            searchable: true,
                            mRender: function(data,type,full){
                                if(full.visible){
                                    return full.threat_number;
                                }
                                else{
                                    return '<s>'+ full.threat_number + '</s>'
                                }
                            },

                        },
                        {
                            data: "threat_category",
                            orderable: true,
                            searchable: true,
                            mRender: function(data,type,full){
                                if(full.visible){
                                    return full.threat_category;
                                }
                                else{
                                    return '<s>'+ full.threat_category + '</s>'
                                }
                            },

                        },
                        {
                            data: "source",
                            orderable: true,
                            searchable: true,
                            mRender: function(data,type,full){
                                if(full.visible){
                                    return full.source;
                                }
                                else{
                                    return '<s>'+ full.source + '</s>'
                                }
                            },

                        },
                        {
                            data: "date_observed",
                            mRender:function (data,type,full){
                                if(full.visible){
                                    return data != '' && data != null ? moment(data).format('DD/MM/YYYY'):'';
                                }
                                else{
                                    return data != '' && data != null ? '<s>'+ moment(data).format('DD/MM/YYYY'):'' + '</s>'
                                }
                            }
                        },
                        {
                            data: "threat_agent",
                            orderable: true,
                            searchable: true,
                            mRender: function(data,type,full){
                                if(full.visible){
                                    return full.threat_agent;
                                }
                                else{
                                    return '<s>'+ full.threat_agent + '</s>'
                                }
                            },

                        },
                        {
                            data: "comment",
                            orderable: true,
                            searchable: true,
                            'render': function(value, type, full){
                                let result = helpers.dtPopover(value, 30, 'hover');
                                if(full.visible){
                                    return type=='export' ? value : result;
                                }
                                else{
                                    return type=='export' ? '<s>' + value + '</s>' : '<s>' + result + '</s>';
                                }
                            },
                        },
                        {
                            data: "current_impact_name",
                            orderable: true,
                            searchable: true,
                            mRender: function(data,type,full){
                                if(full.visible){
                                    return full.current_impact_name;
                                }
                                else{
                                    return '<s>'+ full.current_impact_name + '</s>'
                                }
                            },

                        },
                        {
                            data: "potential_impact_name",
                            orderable: true,
                            searchable: true,
                            mRender: function(data,type,full){
                                if(full.visible){
                                    return full.potential_impact_name;
                                }
                                else{
                                    return '<s>'+ full.potential_impact_name + '</s>'
                                }
                            },
                        },
                        {
                            data: "id",
                            mRender:function (data,type,full){
                                let links = '';
                                if(full.visible){
                                    links +=  `<a href='#${full.id}' data-view-threat='${full.id}'>View</a><br/>`;
                                    links +=  `<a href='#${full.id}' data-edit-threat='${full.id}'>Edit</a><br/>`;
                                    links += `<a href='#' data-discard-threat='${full.id}'>Remove</a><br>`;
                                }
                                else{
                                    links += `<a href='#' data-reinstate-threat='${full.id}'>Reinstate</a><br>`;
                                }
                                return links;
                            }
                        },
                    ],
                    processing:true,
                    initComplete: function() {
                        helpers.enablePopovers();
                    }, 
                }
            }
        },
        components: {
            FormSection,
            datatable,
            ThreatDetail,
        },
        computed: {
        },
        watch:{
            
        },
        methods:{
            newThreat: function(){
                let vm=this;
                this.$refs.threat_detail.threat_id = '';
                //-----for adding new community threat
                var new_community_threat_another={
                    community: vm.species_community.id,
                    source:  vm.species_community.id,
                    threat_category: '',
                    threat_agent: '',
                    comment: '',
                    current_impact: '',
                    potential_impact: '',
                    potential_threat_onset: '',
                    date_observed: null,
                }
                this.$refs.threat_detail.threatObj=new_community_threat_another;
                this.$refs.threat_detail.threat_action='add';
                this.$refs.threat_detail.isModalOpen = true;
            },
            editThreat: function(id){
                let vm=this;
                this.$refs.threat_detail.threat_id = id;
                this.$refs.threat_detail.threat_action='edit';
                Vue.http.get(helpers.add_endpoint_json(api_endpoints.threat,id)).then((response) => {
                      this.$refs.threat_detail.threatObj=response.body; 
                      this.$refs.threat_detail.threatObj.date_observed =  response.body.date_observed != null && response.body.date_observed != undefined ? moment(response.body.date_observed).format('yyyy-MM-DD'): '';
                    },
                  err => { 
                            console.log(err);
                      });
                this.$refs.threat_detail.isModalOpen = true;
            },
            viewThreat: function(id){
                let vm=this;
                this.$refs.threat_detail.threat_id = id;
                this.$refs.threat_detail.threat_action='view';
                Vue.http.get(helpers.add_endpoint_json(api_endpoints.threat,id)).then((response) => {
                      this.$refs.threat_detail.threatObj=response.body; 
                      this.$refs.threat_detail.threatObj.date_observed =  response.body.date_observed != null && response.body.date_observed != undefined ? moment(response.body.date_observed).format('yyyy-MM-DD'): '';
                    },
                  err => { 
                            console.log(err);
                      });
                this.$refs.threat_detail.isModalOpen = true;
            },
            discardThreat:function (id) {
                let vm = this;
                swal({
                    title: "Remove Threat",
                    text: "Are you sure you want to remove this Threat?",
                    type: "warning",
                    showCancelButton: true,
                    confirmButtonText: 'Remove Threat',
                    confirmButtonColor:'#d9534f'
                }).then(() => {
                    vm.$http.get(helpers.add_endpoint_json(api_endpoints.threat,id+'/discard'))
                    .then((response) => {
                        swal(
                            'Discarded',
                            'Your threat has been removed',
                            'success'
                        )
                        vm.$refs.threats_datatable.vmDataTable.ajax.reload();
                    }, (error) => {
                        console.log(error);
                    });
                },(error) => {

                });
            },
            reinstateThreat:function (id) {
                let vm = this;
                swal({
                    title: "Reinstate Threat",
                    text: "Are you sure you want to Reinstate this Threat?",
                    type: "question",
                    showCancelButton: true,
                    confirmButtonText: 'Reinstate Threat',
                }).then(() => {
                    vm.$http.get(helpers.add_endpoint_json(api_endpoints.threat,id+'/reinstate'))
                    .then((response) => {
                        swal(
                            'Reinstated',
                            'Your threat has been reinstated',
                            'success'
                        )
                        vm.$refs.threats_datatable.vmDataTable.ajax.reload();
                    }, (error) => {
                        console.log(error);
                    });
                },(error) => {

                });
            },
            updatedThreats(){
                this.$refs.threats_datatable.vmDataTable.ajax.reload();
            },
            addEventListeners:function (){
                let vm=this;
                vm.$refs.threats_datatable.vmDataTable.on('click', 'a[data-edit-threat]', function(e) {
                    e.preventDefault();
                    var id = $(this).attr('data-edit-threat');
                    vm.editThreat(id);
                });
                vm.$refs.threats_datatable.vmDataTable.on('click', 'a[data-view-threat]', function(e) {
                    e.preventDefault();
                    var id = $(this).attr('data-view-threat');
                    vm.viewThreat(id);
                });
                 // External Discard listener
                vm.$refs.threats_datatable.vmDataTable.on('click', 'a[data-discard-threat]', function(e) {
                    e.preventDefault();
                    var id = $(this).attr('data-discard-threat');
                    vm.discardThreat(id);
                });
                // External Reinstate listener
                vm.$refs.threats_datatable.vmDataTable.on('click', 'a[data-reinstate-threat]', function(e) {
                    e.preventDefault();
                    var id = $(this).attr('data-reinstate-threat');
                    vm.reinstateThreat(id);
                });
            },
            refreshFromResponse: function(){
                this.$refs.threats_datatable.vmDataTable.ajax.reload();
            },
        },
        mounted: function(){
            let vm = this;
            this.$nextTick(() => {
                vm.addEventListeners();
        });
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

