<template>
<div class="container" id="internalDash">
    <ul class="nav nav-pills mb-3" id="pills-tab" role="tablist">
        <li v-if="showFloraTab" class="nav-item">
            <a class="nav-link active" id="pills-flora-tab" data-toggle="pill" href="#pills-flora" role="tab" aria-controls="pills-flora" aria-selected="true" @click="load_group_datatable('flora')">
              Flora
            </a>
        </li>
        <li v-if='showFaunaTab' class="nav-item">
            <a class="nav-link" id="pills-fauna-tab" data-toggle="pill" href="#pills-fauna" role="tab" aria-controls="pills-fauna" aria-selected="false" @click="load_group_datatable('fauna')">
              Fauna
            </a>
        </li>
        <li v-if='showCommunityTab' class="nav-item">
            <a class="nav-link" id="pills-community-tab" data-toggle="pill" href="#pills-community" role="tab" aria-controls="pills-community" aria-selected="false" @click="load_group_datatable('community')">
            Communities
            </a>
        </li>
    </ul>
    <div class="tab-content" id="pills-tabContent">
        <div v-if="isFlora" class="tab-pane fade" id="pills-flora" role="tabpanel" aria-labelledby="pills-flora-tab">
            <FormSection label="Flora" >
                <SpeciesFloraDashTable v-if="isFlora" level="internal" :group_type_name="filterGroupType" :url="species_url" />
            </FormSection>
        </div>
        <div v-if="isFauna" class="tab-pane fade" id="pills-fauna" role="tabpanel" aria-labelledby="pills-fauna-tab">
            <FormSection label="Fauna" >
                <SpeciesFaunaDashTable v-if="isFauna" level="internal" :group_type_name="filterGroupType" :url="species_url"/>
            </FormSection>
        </div>
        <div v-if="isCommunity" class="tab-pane fade" id="pills-community" role="tabpanel" aria-labelledby="pills-community-tab">
            <FormSection label="Community" >
                <CommunitiesDashTable v-if='isCommunity' level="internal" :group_type_name="filterGroupType" :url="community_url" />
            </FormSection>
        </div>
    </div>
</div>
</template>
<script>
import SpeciesFloraDashTable from '@common-utils/species_flora_dashboard.vue'
import SpeciesFaunaDashTable from '@common-utils/species_fauna_dashboard.vue'
import CommunitiesDashTable from '@common-utils/communities_dashboard.vue'
import FormSection from '@/components/forms/section_toggle.vue'
import {
  api_endpoints,
  helpers
}
from '@/utils/hooks'
export default {
    name: 'InternalSpeciesCommunitiesDashboard',
    data() {
        let vm = this;
        return {
            user_preference:'flora',    // TODO : set it to default user preference but for now is hardcoded value
            filterGroupType: 'flora',  // TODO : need to set to default user preferance as cannot call click event of Tab onload
            group_types: [],
            species_url: api_endpoints.species_paginated_internal,
            community_url: api_endpoints.communities_paginated_internal,
        }
    
    },
    watch: {},
    components: {
        SpeciesFloraDashTable,
        SpeciesFaunaDashTable,
        CommunitiesDashTable,
        FormSection,
    },
    computed: {
        /*------properties to show the user authenticated Tabs only-----------*/
        showFloraTab: function(){
            return this.group_types.includes('flora');
        },
        showFaunaTab: function(){
            return this.group_types.includes('fauna');
        },
        showCommunityTab: function(){
            return this.group_types.includes('community');
        },
        /*---------------------------------------------------------------------*/
        /*---------properties to load group related vue components-------------*/
        isFlora: function(){
            return this.filterGroupType == 'flora';
        },
        isFauna: function(){
            return this.filterGroupType == 'fauna';
        },
        isCommunity: function(){
            return this.filterGroupType == 'community';
        },
        /*---------------------------------------------------------------------*/
    },
    methods: {
        set_tabs: function(){
            let vm = this;
            /* set user preference tab by default on load of dashboard (Note: doesn't affect on the load group component)*/
            if(vm.user_preference === 'flora'){
                $('#pills-tab a[href="#pills-flora"]').tab('show');
            }
            if(vm.user_preference === 'fauna'){
                $('#pills-tab a[href="#pills-fauna"]').tab('show');
            }
            if(vm.user_preference === 'community'){
                $('#pills-tab a[href="#pills-community"]').tab('show');
            }
        },
        load_group_datatable: function(grouptype){
            /*----------to set the  filterGroupType to load the particular component only----------*/
            if(grouptype === 'flora'){
                this.filterGroupType = grouptype;
            }
            else if(grouptype === 'fauna'){
                this.filterGroupType = grouptype;
            }
            else if(grouptype === 'community'){
                this.filterGroupType = grouptype;
            }
        },
    },
    created: function () {
        this.$http.get(api_endpoints.group_types_dict).then((response) => {
                this.group_types= response.body;
                },(error) => {
                console.log(error);
            });
    },
    updated: function () {
        this.set_tabs();
    }
}
</script>

<style type="text/css" scoped>
     .nav-item {
        background-color: rgb(200,200,200,0.8) !important;
        margin-bottom: 2px;
    }

    .nav-item>li>a {
        background-color: yellow !important;
        color: #fff;
    }

    .nav-item>li.active>a, .nav-item>li.active>a:hover, .nav-item>li.active>a:focus {
      color: white;
      background-color: blue;
      border: 1px solid #888888;
    }

</style>
