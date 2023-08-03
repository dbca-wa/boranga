<template lang="html">
    <div>
        <div class="col-md-12">

            <ul v-if="is_internal" class="nav nav-pills mb-3" id="pills-tab" role="tablist">
              <li class="nav-item">
                <a 
                    class="nav-link" 
                    id="pills-status-tab" 
                    data-bs-toggle="pill" 
                    :href="'#' + statusBody"
                    role="tab" 
                    :aria-controls="statusBody" 
                    aria-selected="true">
                  Status
                </a>
              </li>
              <li class="nav-item">
                <a 
                    class="nav-link" 
                    id="pills-documents-tab" 
                    data-bs-toggle="pill" 
                    :href="'#' + documentBody" 
                    role="tab" 
                    :aria-selected="documentBody" 
                    aria-selected="false"
                    @click="tabClicked()">
                  Documents
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
              <div class="tab-pane fade show active" :id="statusBody" role="tabpanel" aria-labelledby="pills-status-tab">
                <CommunityStatus
                    v-if="isCommunity"  
                    ref="community_conservation_status" 
                    id="communityStatus" 
                    :is_external="is_external"
                    :canEditStatus="canEditStatus"
                    :conservation_status_obj="conservation_status_obj"
                    :referral="referral">
                </CommunityStatus>
                <SpeciesStatus
                    v-else
                    ref="species_conservation_status" 
                    id="speciesStatus" 
                    :is_external="is_external"
                    :canEditStatus="canEditStatus"
                    :conservation_status_obj="conservation_status_obj"
                    :referral="referral">
                </SpeciesStatus>
              </div>
              <div class="tab-pane fade" :id="documentBody" role="tabpanel" aria-labelledby="pills-documents-tab">
                <CSDocuments 
                    :key="reloadcount"
                    ref="cs_documents" 
                    id="csDocuments" 
                    :is_internal="is_internal"
                    :conservation_status_obj="conservation_status_obj">
                </CSDocuments>
              </div>
              <div class="tab-pane fade" :id="relatedItemBody" role="tabpanel" aria-labelledby="pills-related-items-tab">
                <RelatedItems
                    :key="reloadcount" 
                    ref="cs_related_items" 
                    id="csRelatedItems" 
                    :ajax_url="related_items_ajax_url"
                    :filter_list_url="related_items_filter_list_url">
                </RelatedItems>
              </div>
            </div>
        </div>
    </div>
</template>

<script>
    import SpeciesStatus from '@/components/common/conservation_status/species_status.vue'
    import CommunityStatus from '@/components/common/conservation_status/community_status.vue'
    import CSDocuments from '@/components/common/conservation_status/cs_documents.vue'
    import RelatedItems from '@/components/common/table_related_items.vue'
    
    export default {
        props:{
            conservation_status_obj:{
                type: Object,
                required:true
            },
            referral:{
                type: Object,
                required:false
            },
            is_external:{
              type: Boolean,
              default: false
            },
            is_internal:{
              type: Boolean,
              default: false
            },
            canEditStatus:{
              type: Boolean,
              default: true
            },
        },
        data:function () {
            let vm = this;
            return{
                values:null,
                reloadcount:0,
                statusBody: 'statusBody' + vm._uid,
                documentBody: 'documentBody' + vm._uid,
                relatedItemBody: 'relatedItemBody' + vm._uid,
            }
        },
        components: {
            SpeciesStatus,
            CommunityStatus,
            CSDocuments,
            RelatedItems,
        },
        computed:{
            isCommunity: function(){
                return this.conservation_status_obj.group_type == "community"
            },
            related_items_ajax_url: function(){
              return '/api/conservation_status/' + this.conservation_status_obj.id + '/get_related_items/'
            },
            related_items_filter_list_url: function(){
              return '/api/conservation_status/filter_list.json'
            },
        },
        methods:{
            //----function to resolve datatable exceeding beyond the div
            tabClicked: function(param){
                this.reloadcount = this.reloadcount+1;
            },
            eventListener: function(){
              let vm=this;
            },

        },
        mounted: function() {
            let vm = this;
            vm.form = document.forms.new_conservation_status;
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

