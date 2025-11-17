<template lang="html">
    <div
        v-if="species_community"
        id="externalSpeciesCommunity"
        class="container"
    >
        <div class="row" style="padding-bottom: 50px">
            <h3>
                {{ display_group_type }} {{ display_number }} -
                {{ display_name }}
            </h3>
            <h4 v-if="species_community.conservation_status">
                {{
                    species_community.conservation_status.conservation_category
                }}
            </h4>
            <div class="col-md-3">
                <div class="">
                    <div class="card card-default mb-3">
                        <div class="card-header">Image</div>
                        <div class="card-body card-collapse">
                            <div class="row">
                                <div class="col-sm-12">
                                    <template v-if="uploadedID">
                                        <div
                                            class="rounded"
                                            :class="
                                                downloadingImage
                                                    ? 'animated-background bg-secondary'
                                                    : ''
                                            "
                                            style="width: 258px"
                                            :style="
                                                downloadingImage
                                                    ? 'height:258px;'
                                                    : ''
                                            "
                                        >
                                            <img
                                                v-show="!downloadingImage"
                                                width="258"
                                                :src="image_url"
                                                @error="onImageError"
                                                class="img-thumbnail img-fluid"
                                                :alt="display_name"
                                                @load="onImageLoad"
                                            />
                                        </div>
                                    </template>
                                    <template v-else>
                                        <div
                                            class="d-flex bg-light bg-gradient justify-content-center align-content-middle"
                                            style="height: 258px"
                                        >
                                            <div
                                                class="align-self-center text-muted"
                                            >
                                                No Image Available
                                            </div>
                                        </div>
                                    </template>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
            <div class="col-md-9">
                <div class="row">
                    <div class="">
                        <div class="row">
                            <form>
                                <ProposalSpeciesCommunities
                                    id="speciesCommunityStart"
                                    ref="species_communities"
                                    :species_community="species_community"
                                    :species_community_original="
                                        species_community
                                    "
                                    :is_internal="false"
                                    :is_readonly="true"
                                >
                                </ProposalSpeciesCommunities>
                                <div class="row" style="margin-bottom: 50px">
                                    <div
                                        class="navbar fixed-bottom"
                                        style="background-color: #f5f5f5"
                                    >
                                        <div class="container">
                                            <div class="col-md-6">
                                                <button
                                                    class="btn btn-primary me-2 pull-left"
                                                    style="margin-top: 5px"
                                                    @click.prevent="
                                                        returnToDashboard
                                                    "
                                                >
                                                    Return to Dashboard
                                                </button>
                                            </div>
                                        </div>
                                    </div>
                                </div>
                            </form>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>
</template>
<script>
import ProposalSpeciesCommunities from '@/components/form_species_communities.vue';
import { api_endpoints } from '@/utils/hooks';

export default {
    name: 'ExternalSpeciesCommunity',
    components: {
        ProposalSpeciesCommunities,
    },
    beforeRouteEnter: function (to, from, next) {
        //-------------get species object if received species id
        if (
            to.query.group_type_name === 'flora' ||
            to.query.group_type_name === 'fauna'
        ) {
            const speciesUrl = `${api_endpoints.external_species}/${to.params.species_community_id}/`;
            fetch(speciesUrl).then(
                async (response) => {
                    next(async (vm) => {
                        vm.species_community = await response.json(); //--temp species_obj
                        vm.uploadedID = vm.species_community.image_doc;
                        // reset the downloading flag when a new image id is present
                        vm.downloadingImage = !!vm.uploadedID;
                    });
                },
                (err) => {
                    console.log(err);
                }
            );
        }
        //------get community object if received community id
        else {
            const communityUrl = `${api_endpoints.external_community}/${to.params.species_community_id}/`;
            fetch(communityUrl).then(
                async (response) => {
                    next(async (vm) => {
                        vm.species_community = await response.json(); //--temp community_obj
                        vm.uploadedID = vm.species_community.image_doc;
                        // reset the downloading flag when a new image id is present
                        vm.downloadingImage = !!vm.uploadedID;
                    });
                },
                (err) => {
                    console.log(err);
                }
            );
        }
    },
    data: function () {
        return {
            species_community: null,
            uploadedID: null,
            downloadingImage: true,
        };
    },
    computed: {
        isCommunity: function () {
            return this.species_community.group_type === 'community';
        },
        display_group_type: function () {
            let group_type_string = this.species_community.group_type;
            // to Capitalize only first character
            return (
                group_type_string.charAt(0).toUpperCase() +
                group_type_string.slice(1)
            );
        },
        display_number: function () {
            return this.species_community.group_type === 'community'
                ? this.species_community.community_number
                : this.species_community.species_number;
        },
        display_name: function () {
            return this.species_community.group_type === 'community'
                ? this.species_community.taxonomy_details != null
                    ? this.species_community.taxonomy_details
                          .community_common_id
                    : ''
                : this.species_community.taxonomy_details != null
                  ? this.species_community.taxonomy_details.scientific_name +
                    ' (' +
                    this.species_community.taxonomy_details.taxon_name_id +
                    ')'
                  : '';
        },
        image_url: function () {
            return this.species_community.group_type === 'community'
                ? `${api_endpoints.external_community}/${this.species_community.id}/public_image/`
                : `${api_endpoints.external_species}/${this.species_community.id}/public_image/`;
        },
    },
    methods: {
        onImageLoad: function () {
            this.downloadingImage = false;
        },
        onImageError: function () {
            // stop the spinner and clear uploadedID so the 'No Image Available' placeholder shows
            this.downloadingImage = false;
            this.uploadedID = null;
        },
        returnToDashboard: function () {
            let vm = this;
            vm.$router.push({
                name: 'external-species-communities-dash',
            });
        },
    },
};
</script>
<style scoped>
@keyframes placeHolderShimmer {
    0% {
        background-position: -468px 0;
    }

    100% {
        background-position: 468px 0;
    }
}

.animated-background {
    animation-duration: 1.25s;
    animation-fill-mode: forwards;
    animation-iteration-count: infinite;
    animation-name: placeHolderShimmer;
    animation-timing-function: linear;
    background: darkgray;
    background: linear-gradient(
        to right,
        #eeeeee 10%,
        #dddddd 18%,
        #eeeeee 33%
    );
    background-size: 800px 104px;
    height: 100px;
    position: relative;
}
</style>
