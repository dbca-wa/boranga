<template lang="html">
    <div>
        <div class="col-md-12">

            <ul v-if="is_internal" class="nav nav-pills mb-3" id="pills-tab" role="tablist">
              <li class="nav-item">
                <a 
                    class="nav-link" 
                    id="pills-profile-tab" 
                    data-bs-toggle="pill" 
                    :href="'#' + profileBody" 
                    role="tab" 
                    :aria-controls="profileBody" 
                    aria-selected="true"
                    @click="tabClicked()">
                  Profile
                </a>
              </li>
              <li class="nav-item">
                <a 
                    class="nav-link" 
                    id="pills-documents-tab" 
                    data-bs-toggle="pill" 
                    :href="'#' + documentBody" 
                    role="tab" 
                    aria-controls="pills-documents" 
                    :aria-selected="documentBody"
                    @click="tabClicked()">
                  Documents
                </a>
              </li>
              <li class="nav-item">
                <a 
                    class="nav-link" 
                    id="pills-threats-tab" 
                    data-bs-toggle="pill" 
                    :href="'#' + threatBody " 
                    role="tab" 
                    :aria-controls="threatBody" 
                    aria-selected="false"
                    @click="tabClicked()">
                  Threats
                </a>
              </li>
              <li class="nav-item">
                <a 
                    class="nav-link" 
                    id="pills-related-items-tab" 
                    data-bs-toggle="pill" 
                    :href="'#' + relatedItemBody" 
                    role="tab" 
                    :aria-controls="relatedItemBody" 
                    aria-selected="false"
                    @click="tabClicked()">
                  Related Items
                </a>
              </li>
            </ul>
            <div class="tab-content" id="pills-tabContent">
              <div class="tab-pane fade show active" :id="profileBody" role="tabpanel" aria-labelledby="pills-profile-tab">
                <Community
                    v-if="isCommunity"  
                    :key="reloadcount"
                    ref="community_information" 
                    id="communityInformation" 
                    :is_internal="is_internal"
                    :species_community="species_community">
                </Community>
                <Species
                    v-else
                    :key="reloadcount"
                    ref="species_information" 
                    id="speciesInformation" 
                    :is_internal="is_internal"
                    :species_community="species_community"
                    :is_readonly="is_readonly"
                    :rename_species="rename_species">
                </Species>

              </div>
              <div class="tab-pane fade" :id="documentBody" role="tabpanel" aria-labelledby="pills-documents-tab">
                <CommunityDocuments 
                    v-if="isCommunity"
                    :key="reloadcount"
                    ref="community_documents" 
                    id="communityDocuments" 
                    :is_internal="is_internal"
                    :species_community="species_community"
                    :is_readonly="is_readonly">
                </CommunityDocuments>
                <SpeciesDocuments 
                    v-else 
                    :key="reloadcount"
                    ref="species_documents" 
                    id="speciesDocuments" 
                    :is_internal="is_internal"
                    :species_community="species_community"
                    :is_readonly="is_readonly">
                </SpeciesDocuments>
              </div>
              <div class="tab-pane fade" :id="threatBody" role="tabpanel" aria-labelledby="pills-threats-tab">
                <CommunityThreats 
                    v-if="isCommunity"
                    :key="reloadcount"
                    ref="community_threats" 
                    id="communityThreats" 
                    :is_internal="is_internal"
                    :species_community="species_community"
                    :is_readonly="is_readonly">
                </CommunityThreats>
                <SpeciesThreats
                    v-else 
                    :key="reloadcount"
                    ref="species_threats" 
                    id="speciesThreats" 
                    :is_internal="is_internal"
                    :species_community="species_community"
                    :is_readonly="is_readonly">
                </SpeciesThreats>
              </div>
              <div class="tab-pane fade" :id="relatedItemBody" role="tabpanel" aria-labelledby="pills-related-items-tab">
                <RelatedItems 
                    :key="reloadcount"
                    ref="species_communities_related_items" 
                    id="speciesCommunitiesRelatedItems" 
                    :ajax_url="related_items_ajax_url"
                    :filter_list_url="related_items_filter_list_url">
                </RelatedItems>
              </div>
            </div>
        </div>
    </div>
</template>

<script>
    import Species from '@/components/common/species_communities/species_profile.vue'
    import Community from '@/components/common/species_communities/community_profile.vue'
    import SpeciesDocuments from '@/components/common/species_communities/species_documents.vue'
    import CommunityDocuments from '@/components/common/species_communities/community_documents.vue'
    import SpeciesThreats from '@/components/common/species_communities/species_threats.vue'
    import CommunityThreats from '@/components/common/species_communities/community_threats.vue'
    import RelatedItems from '@/components/common/table_related_items.vue'

    export default {
        props:{
            species_community:{
                type: Object,
                required:true
            },
            is_external:{
              type: Boolean,
              default: false
            },
            is_internal:{
              type: Boolean,
              default: false
            },
            // this prop is only send from split species form to make the original species readonly
            is_readonly:{
              type: Boolean,
              default: false
            },
            rename_species:{
              type: Boolean,
              default: false
            },
        },
        data:function () {
            let vm = this;
            return{
                profileBody: 'profileBody' + vm._uid,
                documentBody: 'documentBody' + vm._uid,
                threatBody: 'threatBody' + vm._uid,
                relatedItemBody: 'relatedItemBody' + vm._uid,
                values:null,
                reloadcount:0,
            }
        },
        components: {
            Species,
            Community,
            SpeciesDocuments,
            CommunityDocuments,
            SpeciesThreats,
            CommunityThreats,
            RelatedItems,
        },
        computed:{
            isCommunity: function(){
                return this.species_community.group_type == "community";
            },
            related_items_ajax_url: function(){
              if(this.isCommunity){
                return '/api/community/' + this.species_community.id + '/get_related_items/';
              }
              else{
                return '/api/species/' + this.species_community.id + '/get_related_items/';
              }
            },
            related_items_filter_list_url: function(){
              if(this.isCommunity){
                return '/api/community/filter_list.json';
              }
              else{
                return '/api/species/filter_list.json';
              }
            },
        },
        methods:{
            //----function to resolve datatable exceeding beyond the div
            tabClicked: function(param){
                this.reloadcount = this.reloadcount+1;
            },
            /*set_tabs:function(){
                let vm = this;

                 set profile tab Active 
                //$('#pills-tab a[href="#pills-profile"]').tab('show');
            },*/
            eventListener: function(){
              let vm=this;
            },

        },
        mounted: function() {
            let vm = this;
            //vm.set_tabs();
            vm.form = document.forms.new_species;
            vm.eventListener();
        }
 
    }
</script>

<style lang="css" scoped>
    .section{
        text-transform: capitalize;
    }
    .list-group{
        margin-bottom: 0;
    }
    .fixed-top{
        position: fixed;
        top:56px;
    }

    .nav-item {
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

        .admin > div {
          display: inline-block;
          vertical-align: top;
          margin-right: 1em;
        }
    .nav-pills .nav-link {
        border-bottom-left-radius: 0;
        border-bottom-right-radius: 0;
        border-top-left-radius: 0.5em;
        border-top-right-radius: 0.5em;
        margin-right: 0.25em;
    }
    .nav-pills .nav-link {
        background: lightgray;
    }
    .nav-pills .nav-link.active {
        background: gray;
    }
</style>

