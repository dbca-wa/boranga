<template lang="html">
    <div>
        <div class="col-md-12">
            <ul
                v-if="!is_external"
                id="pills-tab"
                class="nav nav-pills"
                role="tablist"
            >
                <li class="nav-item">
                    <a
                        id="pills-status-tab"
                        class="nav-link active"
                        data-bs-toggle="pill"
                        :href="'#' + statusBody"
                        role="tab"
                        :aria-controls="statusBody"
                        aria-selected="true"
                        @click="tabClicked()"
                    >
                        Status
                    </a>
                </li>
                <li class="nav-item">
                    <a
                        id="pills-documents-tab"
                        class="nav-link"
                        data-bs-toggle="pill"
                        :href="'#' + documentBody"
                        role="tab"
                        :aria-selected="documentBody"
                        @click="tabClicked()"
                    >
                        Documents
                    </a>
                </li>
                <li class="nav-item">
                    <a
                        id="pills-related-items-tab"
                        class="nav-link"
                        data-bs-toggle="pill"
                        :href="'#' + relatedItemBody"
                        role="tab"
                        :aria-controls="relatedItemBody"
                        aria-selected="false"
                        @click="tabClicked()"
                    >
                        Related Items
                    </a>
                </li>
            </ul>
            <div id="pills-tabContent" class="tab-content">
                <div
                    :id="statusBody"
                    class="tab-pane fade show active"
                    role="tabpanel"
                    aria-labelledby="pills-status-tab"
                >
                    <CommunityStatus
                        v-if="isCommunity"
                        id="communityStatus"
                        :key="reloadcount"
                        ref="community_conservation_status"
                        :is_external="is_external"
                        :can-edit-status="canEditStatus"
                        :conservation_status_obj="conservation_status_obj"
                        :referral="referral"
                        @save-conservation-status="
                            $emit('saveConservationStatus')
                        "
                    >
                    </CommunityStatus>
                    <SpeciesStatus
                        v-else
                        id="speciesStatus"
                        :key="reloadcount + 'else'"
                        ref="species_conservation_status"
                        :is_external="is_external"
                        :can-edit-status="canEditStatus"
                        :conservation_status_obj="conservation_status_obj"
                        :referral="referral"
                        @save-conservation-status="
                            $emit('saveConservationStatus')
                        "
                    >
                    </SpeciesStatus>
                    <CSDocuments
                        v-if="!is_internal && !referral"
                        id="csDocuments"
                        :key="reloadcount + 'cs_documents'"
                        ref="cs_documents"
                        :is_internal="is_internal"
                        :conservation_status_obj="conservation_status_obj"
                    >
                    </CSDocuments>
                    <SubmitterInformation
                        v-if="conservation_status_obj.submitter_information"
                        id="submitter_information"
                        :key="reloadcount + 'submitter_information'"
                        ref="submitter_information"
                        :show_submitter_contact_details="
                            show_submitter_contact_details
                        "
                        :submitter_information="
                            conservation_status_obj.submitter_information
                        "
                        :disabled="
                            !(
                                conservation_status_obj.can_user_edit &&
                                conservation_status_obj.is_submitter
                            ) || referral
                        "
                    />
                </div>
                <div
                    :id="documentBody"
                    class="tab-pane fade"
                    role="tabpanel"
                    aria-labelledby="pills-documents-tab"
                >
                    <CSDocuments
                        id="csDocuments"
                        :key="reloadcount"
                        ref="cs_documents"
                        :is_internal="is_internal"
                        :conservation_status_obj="conservation_status_obj"
                    >
                    </CSDocuments>
                </div>
                <div
                    v-if="!is_external"
                    :id="relatedItemBody"
                    class="tab-pane fade"
                    role="tabpanel"
                    aria-labelledby="pills-related-items-tab"
                >
                    <RelatedItems
                        id="csRelatedItems"
                        :key="reloadcount"
                        ref="cs_related_items"
                        :ajax_url="related_items_ajax_url"
                        :filter_list_url="related_items_filter_list_url"
                        :display_profile_column="true"
                    >
                    </RelatedItems>
                </div>
            </div>
        </div>
    </div>
</template>

<script>
import { v4 as uuid } from 'uuid';
import SubmitterInformation from '@/components/common/submitter_information.vue';
import SpeciesStatus from '@/components/common/conservation_status/species_status.vue';
import CommunityStatus from '@/components/common/conservation_status/community_status.vue';
import CSDocuments from '@/components/common/conservation_status/cs_documents.vue';
import RelatedItems from '@/components/common/table_related_items.vue';

export default {
    components: {
        SubmitterInformation,
        SpeciesStatus,
        CommunityStatus,
        CSDocuments,
        RelatedItems,
    },
    props: {
        conservation_status_obj: {
            type: Object,
            required: true,
        },
        referral: {
            type: Object,
            required: false,
        },
        is_external: {
            type: Boolean,
            default: false,
        },
        is_internal: {
            type: Boolean,
            default: false,
        },
        canEditStatus: {
            type: Boolean,
            default: true,
        },
    },
    emits: ['saveConservationStatus'],
    data: function () {
        return {
            values: null,
            reloadcount: 0,
            statusBody: 'statusBody' + uuid(),
            documentBody: 'documentBody' + uuid(),
            relatedItemBody: 'relatedItemBody' + uuid(),
        };
    },
    computed: {
        show_submitter_contact_details: function () {
            return 'ConservationStatusReferral' != this.$parent.$options.name;
        },
        isCommunity: function () {
            return this.conservation_status_obj.group_type == 'community';
        },
        related_items_ajax_url: function () {
            return (
                '/api/conservation_status/' +
                this.conservation_status_obj.id +
                '/get_related_items/'
            );
        },
        related_items_filter_list_url: function () {
            return '/api/conservation_status/filter_list.json';
        },
    },
    mounted: function () {
        let vm = this;
        vm.form = document.forms.new_conservation_status;
    },
    methods: {
        //----function to resolve datatable exceeding beyond the div
        tabClicked: function () {
            this.reloadcount = this.reloadcount + 1;
        },
    },
};
</script>

<style lang="css" scoped>
.section {
    text-transform: capitalize;
}

.list-group {
    margin-bottom: 0;
}

.fixed-top {
    position: fixed;
    top: 56px;
}

.nav-item {
    margin-bottom: 2px;
}

.nav-item > li > a {
    background-color: yellow !important;
    color: #fff;
}

.nav-item > li.active > a,
.nav-item > li.active > a:hover,
.nav-item > li.active > a:focus {
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
