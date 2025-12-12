<template lang="html">
    <div
        v-if="species_community"
        id="internalSpeciesCommunity"
        class="container"
    >
        <div class="row" style="padding-bottom: 50px">
            <h3>
                {{ display_group_type }} {{ display_number }} -
                {{ display_name }}
            </h3>
            <div v-if="!comparing" class="col-md-3">
                <div class="">
                    <div class="card card-default mb-3">
                        <div class="card-header">Image</div>
                        <div class="card-body">
                            <div class="row mb-2 pb-2">
                                <div
                                    v-if="
                                        !uploadingImage &&
                                        speciesCommunitiesImage
                                    "
                                    class="col"
                                >
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
                                            :src="speciesCommunitiesImage"
                                            width="258"
                                            class="img-thumbnail img-fluid rounded"
                                            @load="onImageLoad"
                                            @error="onImageError"
                                            :alt="imageAlt"
                                        />
                                    </div>
                                </div>
                                <div
                                    v-else
                                    class="col d-flex bg-light bg-gradient justify-content-center align-content-middle mx-2"
                                    style="height: 258px"
                                >
                                    <div class="align-self-center text-muted">
                                        No Image Uploaded
                                    </div>
                                </div>
                            </div>
                            <div
                                v-if="
                                    hasUserEditMode &&
                                    species_community.processing_status !=
                                        'Discarded'
                                "
                                class="row border-top pt-3 mb-2"
                            >
                                <div class="col">
                                    <div class="d-flex justify-content-center">
                                        <div class="text-muted">
                                            Image Actions
                                        </div>
                                    </div>
                                </div>
                            </div>
                            <div
                                v-if="
                                    hasUserEditMode &&
                                    species_community.processing_status !=
                                        'Discarded'
                                "
                                class="row"
                            >
                                <div class="col">
                                    <div
                                        class="d-flex align-items-center flex-column"
                                    >
                                        <label
                                            v-if="!speciesCommunitiesImage"
                                            for="image-upload"
                                            role="button"
                                            class="btn btn-primary btn-sm w-50 mb-2 text-start"
                                            ><i class="bi bi-upload me-3"></i>
                                            Upload</label
                                        >
                                        <input
                                            id="image-upload"
                                            ref="speciesCommunitiesImage"
                                            class="d-none"
                                            type="file"
                                            accept="image/*"
                                            @change="uploadImage"
                                        />
                                        <button
                                            class="btn btn-secondary btn-sm w-50 mb-2 text-start"
                                            @click="showReinstateImageModal"
                                        >
                                            <i
                                                class="bi bi-clock-history me-3"
                                            ></i>
                                            Reinstate
                                        </button>
                                        <template
                                            v-if="
                                                !uploadingImage &&
                                                speciesCommunitiesImage
                                            "
                                        >
                                            <label
                                                for="image-upload"
                                                role="button"
                                                class="btn btn-primary btn-sm w-50 mb-2 text-start"
                                                ><i
                                                    class="bi bi-pencil-fill me-3"
                                                ></i>
                                                Replace</label
                                            >
                                            <input
                                                id="image-upload"
                                                ref="speciesCommunitiesImage"
                                                class="d-none"
                                                type="file"
                                                accept="image/*"
                                                @change="uploadImage"
                                            />

                                            <button
                                                class="btn btn-danger btn-sm w-50 mb-2 text-start"
                                                @click="confirmDiscardImage"
                                            >
                                                <i
                                                    class="bi bi-trash3-fill me-3"
                                                ></i>
                                                Discard
                                            </button>
                                        </template>
                                        <button
                                            v-if="uploadingImage"
                                            class="btn btn-primary btn-sm w-50 mb-2 text-start"
                                        >
                                            <span
                                                class="spinner-border spinner-border-sm me-3"
                                                role="status"
                                                aria-hidden="true"
                                            ></span>
                                            Uploading
                                            <span class="visually-hidden"
                                                >Loading...</span
                                            >
                                        </button>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>

                <CommsLogs
                    :comms_url="comms_url"
                    :logs_url="logs_url"
                    :comms_add_url="comms_add_url"
                    :disable_add_entry="!species_community.can_add_log"
                    class="mb-3"
                />

                <ActivatedBy
                    v-if="canSeeActivatedBy"
                    :submitter_first_name="submitter_first_name"
                    :submitter_last_name="submitter_last_name"
                    :lodgement_date="species_community.lodgement_date"
                    class="mb-3"
                />

                <div class="card card-default sticky-top">
                    <div class="card-header">Workflow</div>
                    <div class="card-body">
                        <div class="row">
                            <div class="col-sm-12">
                                <strong>Status</strong><br />
                                {{ species_community.processing_status }}
                                <span v-if="showPublicationStatus">
                                    -
                                    {{
                                        species_community.publishing_status
                                            .public_status
                                    }}</span
                                >
                            </div>
                        </div>
                    </div>
                    <div v-if="inViewMode" class="card-body border-top">
                        <div class="row">
                            <div class="col-sm-12">
                                <div class="row mb-2">
                                    <div class="col-sm-12">
                                        <strong>Action</strong><br />
                                    </div>
                                </div>
                                <div class="row">
                                    <div class="col-sm-12">
                                        <button
                                            class="btn btn-primary w-100"
                                            @click.prevent="activateEditMode()"
                                        >
                                            Edit</button
                                        ><br />
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                    <div
                        v-else-if="
                            hasUserEditMode || (isCommunity && isHistorical)
                        "
                        class="card-body border-top"
                    >
                        <div class="row">
                            <div class="col-sm-12">
                                <div class="row mb-2">
                                    <div class="col-sm-12">
                                        <strong>Action</strong><br />
                                    </div>
                                </div>
                                <div v-if="isActive" class="row">
                                    <template v-if="!isCommunity">
                                        <div class="row mb-2">
                                            <div class="col-sm-12">
                                                <button
                                                    class="btn btn-primary w-100"
                                                    @click.prevent="
                                                        splitSpecies()
                                                    "
                                                >
                                                    Split</button
                                                ><br />
                                            </div>
                                        </div>
                                        <div class="row mb-2">
                                            <div class="col-sm-12">
                                                <button
                                                    class="btn btn-primary w-100"
                                                    @click.prevent="
                                                        combineSpecies()
                                                    "
                                                >
                                                    Combine</button
                                                ><br />
                                            </div>
                                        </div>
                                        <div class="row mb-2">
                                            <div class="col-sm-12">
                                                <button
                                                    class="btn btn-primary w-100"
                                                    @click.prevent="
                                                        renameSpecies()
                                                    "
                                                >
                                                    Rename</button
                                                ><br />
                                            </div>
                                        </div>
                                    </template>
                                    <template v-else>
                                        <div class="row mb-2">
                                            <div class="col-sm-12">
                                                <button
                                                    class="btn btn-primary w-100"
                                                    @click.prevent="
                                                        renameCommunity()
                                                    "
                                                >
                                                    Rename</button
                                                ><br />
                                            </div>
                                        </div>
                                        <div class="row mb-2">
                                            <div class="col-sm-12">
                                                <button
                                                    class="btn btn-primary w-100"
                                                    @click.prevent="
                                                        deactivateCommunity()
                                                    "
                                                >
                                                    Deactivate</button
                                                ><br />
                                            </div>
                                        </div>
                                    </template>
                                </div>
                                <div v-if="isHistorical" class="row">
                                    <template v-if="isCommunity">
                                        <div class="row mb-2">
                                            <div class="col-sm-12">
                                                <button
                                                    class="btn btn-primary w-100"
                                                    @click.prevent="
                                                        reactivateCommunity()
                                                    "
                                                >
                                                    Reactivate</button
                                                ><br />
                                            </div>
                                        </div>
                                    </template>
                                </div>
                                <div v-if="isActive" class="row">
                                    <div v-if="!isPublic" class="row mb-2">
                                        <div class="col-sm-12">
                                            <button
                                                class="btn btn-primary w-100"
                                                @click.prevent="makePublic()"
                                            >
                                                Make Public</button
                                            ><br />
                                        </div>
                                    </div>
                                    <div v-else class="row mb-2">
                                        <div class="col-sm-12">
                                            <button
                                                class="btn btn-primary w-100"
                                                @click.prevent="makePrivate()"
                                            >
                                                Make Private</button
                                            ><br />
                                        </div>
                                    </div>
                                </div>
                                <div v-if="canDiscard">
                                    <div class="row mb-2">
                                        <div class="col-sm-12">
                                            <strong>Action</strong><br />
                                        </div>
                                    </div>
                                    <div class="row">
                                        <div class="col-sm-12">
                                            <button
                                                class="btn btn-primary w-100"
                                                @click.prevent="
                                                    discardSpeciesProposal()
                                                "
                                            >
                                                Discard</button
                                            ><br />
                                        </div>
                                    </div>
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
                            <form
                                :action="species_community_form_url"
                                method="post"
                                name="new_species"
                                enctype="multipart/form-data"
                            >
                                <ProposalSpeciesCommunities
                                    id="speciesCommunityStart"
                                    ref="species_communities"
                                    :species_community="species_community"
                                    :species_community_original="
                                        species_community_original
                                    "
                                    :is_internal="true"
                                    :is_readonly="
                                        species_community.readonly ||
                                        species_community.processing_status ==
                                            'Discarded'
                                    "
                                    @save-species-community="save_wo()"
                                    :key="formKey"
                                >
                                </ProposalSpeciesCommunities>
                                <input
                                    type="hidden"
                                    name="csrfmiddlewaretoken"
                                    :value="csrf_token"
                                />
                                <input
                                    type="hidden"
                                    name="species_community_id"
                                    :value="1"
                                />
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
                                            <div
                                                v-if="
                                                    species_community.can_user_edit &&
                                                    species_community.processing_status !=
                                                        'Discarded'
                                                "
                                                class="col-md-6 text-end"
                                            >
                                                <button
                                                    v-if="
                                                        savingSpeciesCommunity
                                                    "
                                                    class="btn btn-primary me-2 pull-right"
                                                    style="margin-top: 5px"
                                                    disabled
                                                >
                                                    Save and Continue
                                                    <span
                                                        class="spinner-border spinner-border-sm"
                                                        role="status"
                                                        aria-hidden="true"
                                                    ></span>
                                                    <span
                                                        class="visually-hidden"
                                                        >Loading...</span
                                                    >
                                                </button>
                                                <button
                                                    v-else
                                                    class="btn btn-primary me-2 ull-right"
                                                    style="margin-top: 5px"
                                                    :disabled="
                                                        saveExitSpeciesCommunity ||
                                                        submitSpeciesCommunity
                                                    "
                                                    @click.prevent="save()"
                                                >
                                                    Save and Continue
                                                </button>

                                                <button
                                                    v-if="
                                                        saveExitSpeciesCommunity
                                                    "
                                                    class="btn btn-primary me-2 pull-right"
                                                    style="margin-top: 5px"
                                                    disabled
                                                >
                                                    Save and Exit
                                                    <span
                                                        class="spinner-border spinner-border-sm"
                                                        role="status"
                                                        aria-hidden="true"
                                                    ></span>
                                                    <span
                                                        class="visually-hidden"
                                                        >Loading...</span
                                                    >
                                                </button>
                                                <button
                                                    v-else
                                                    class="btn btn-primary me-2 pull-right"
                                                    style="margin-top: 5px"
                                                    :disabled="
                                                        savingSpeciesCommunity ||
                                                        submitSpeciesCommunity
                                                    "
                                                    @click.prevent="save_exit()"
                                                >
                                                    Save and Exit
                                                </button>

                                                <button
                                                    v-if="
                                                        submitSpeciesCommunity
                                                    "
                                                    class="btn btn-primary pull-right"
                                                    style="margin-top: 5px"
                                                    disabled
                                                >
                                                    Activate
                                                    <span
                                                        class="spinner-border spinner-border-sm"
                                                        role="status"
                                                        aria-hidden="true"
                                                    ></span>
                                                    <span
                                                        class="visually-hidden"
                                                        >Loading...</span
                                                    >
                                                </button>
                                                <button
                                                    v-else
                                                    class="btn btn-primary pull-right"
                                                    style="margin-top: 5px"
                                                    :disabled="
                                                        saveExitSpeciesCommunity ||
                                                        savingSpeciesCommunity
                                                    "
                                                    @click.prevent="submit()"
                                                >
                                                    Activate
                                                </button>
                                            </div>
                                            <div
                                                v-else-if="
                                                    hasUserEditMode &&
                                                    species_community.processing_status !=
                                                        'Discarded'
                                                "
                                                class="col-md-6 text-end"
                                            >
                                                <button
                                                    v-if="
                                                        savingSpeciesCommunity
                                                    "
                                                    class="btn btn-primary pull-right"
                                                    style="margin-top: 5px"
                                                    disabled
                                                >
                                                    Save Changes
                                                    <span
                                                        class="spinner-border spinner-border-sm"
                                                        role="status"
                                                        aria-hidden="true"
                                                    ></span>
                                                    <span
                                                        class="visually-hidden"
                                                        >Loading...</span
                                                    >
                                                </button>
                                                <button
                                                    v-else
                                                    class="btn btn-primary pull-right"
                                                    style="margin-top: 5px"
                                                    @click.prevent="save()"
                                                >
                                                    Save Changes
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

        <SpeciesSplit
            v-if="species_community && !isCommunity && isActive"
            ref="species_split"
            :species_community="species_community"
            :is_internal="true"
            :scrollable="true"
            @refresh-from-response="refreshFromResponse"
        />
        <SpeciesCombine
            v-if="species_community && !isCommunity && isActive"
            ref="species_combine"
            :species_community="species_community"
            :is_internal="true"
            @refresh-from-response="refreshFromResponse"
        />
        <SpeciesRename
            v-if="species_community && !isCommunity && isActive"
            ref="species_rename"
            :species_community_original="species_community"
            :is_internal="true"
            @refresh-from-response="refreshFromResponse"
        />
        <CommunityRename
            v-if="species_community && isCommunity"
            ref="community_rename"
            :species_community_original="species_community"
            :is_internal="true"
            @refresh-from-response="refreshFromResponse"
        />

        <MakePublic
            ref="make_public"
            :species_community="species_community"
            :species_community_original="species_community_original"
            :is_internal="true"
            @refresh-from-response="refreshFromResponse"
        />
        <ReinstateImage
            ref="reinstateImage"
            :title="reinstate_image_title"
            :image-history-url="image_history_url"
            @reinstate-image="reinstateImage"
        />
    </div>
</template>
<script>
import CommsLogs from '@common-utils/comms_logs.vue';
import ActivatedBy from '@common-utils/activated_by.vue';
import ProposalSpeciesCommunities from '@/components/form_species_communities.vue';
import SpeciesSplit from './species_split.vue';
import SpeciesCombine from './species_combine.vue';
import CommunityRename from './community_rename.vue';
import SpeciesRename from './species_rename.vue';
import MakePublic from './make_public.vue';
import ReinstateImage from '@common-utils/reinstate_image.vue';

import { api_endpoints, helpers } from '@/utils/hooks';
export default {
    name: 'InternalSpeciesCommunity',
    components: {
        CommsLogs,
        ActivatedBy,
        ProposalSpeciesCommunities,
        SpeciesSplit,
        SpeciesCombine,
        SpeciesRename,
        CommunityRename,
        MakePublic,
        ReinstateImage,
    },
    filters: {
        formatDate: function (data) {
            return data ? moment(data).format('DD/MM/YYYY HH:mm:ss') : '';
        },
    },
    beforeRouteEnter: function (to, from, next) {
        let end_point_type = 'species';
        if (to.query.group_type_name === 'community') {
            end_point_type = 'community';
        }
        fetch(
            `/api/${end_point_type}/${to.params.species_community_id}/internal_${end_point_type}.json`
        ).then(
            async (response) => {
                next(async (vm) => {
                    const data = await response.json();
                    vm.species_community = data[`${end_point_type}_obj`]; //--temp community_obj
                    vm.species_community_original = helpers.copyObject(
                        vm.species_community
                    );
                    vm.speciesCommunitiesImage = vm.species_community.image_doc;
                });
            },
            (err) => {
                console.log(err);
            }
        );
    },
    data: function () {
        return {
            species_community: null,
            species_community_original: null,
            form: null,
            formKey: 0,
            savingSpeciesCommunity: false,
            saveExitSpeciesCommunity: false,
            submitSpeciesCommunity: false,
            speciesCommunitiesImage: null,
            uploadingImage: false,
            isSaved: false,
            DATE_TIME_FORMAT: 'DD/MM/YYYY HH:mm:ss',
            comparing: false,
            downloadingImage: true,
        };
    },
    computed: {
        csrf_token: function () {
            return helpers.getCookie('csrftoken');
        },
        isCommunity: function () {
            return this.species_community.group_type === 'community';
        },
        species_community_form_url: function () {
            return this.species_community.group_type === 'community'
                ? `/api/community/${this.species_community.id}/community_save.json`
                : `/api/species/${this.species_community.id}/species_save.json`;
        },
        species_community_submit_url: function () {
            return this.species_community.group_type === 'community'
                ? `community`
                : `species`;
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
        imageAlt: function () {
            // Prefer the scientific name if present, otherwise fallback to display_name
            if (
                this.species_community &&
                this.species_community.taxonomy_details &&
                this.species_community.taxonomy_details.scientific_name
            ) {
                return this.species_community.taxonomy_details.scientific_name;
            }
            return this.display_name || 'Species image';
        },
        class_ncols: function () {
            return this.comparing ? 'col-md-12' : 'col-md-8';
        },
        submitter_first_name: function () {
            if (this.species_community && this.species_community.submitter) {
                return this.species_community.submitter.first_name;
            } else {
                return '';
            }
        },
        submitter_last_name: function () {
            if (this.species_community && this.species_community.submitter) {
                return this.species_community.submitter.last_name;
            } else {
                return '';
            }
        },
        canSeeActivatedBy: function () {
            return (
                this.species_community &&
                this.species_community.submitter &&
                this.species_community.lodgement_date
            );
        },
        hasUserEditMode: function () {
            return this.species_community &&
                this.species_community.user_edit_mode
                ? true
                : false;
        },
        isPublic: function () {
            return this.species_community.group_type === 'community'
                ? this.species_community.publishing_status.community_public
                    ? true
                    : false
                : this.species_community.publishing_status.species_public
                  ? true
                  : false;
        },
        isActive: function () {
            return this.species_community.processing_status === 'Active'
                ? true
                : false;
        },
        isHistorical: function () {
            return this.species_community.processing_status === 'Historical'
                ? true
                : false;
        },
        showPublicationStatus: function () {
            return ['Active', 'Historical'].includes(
                this.species_community.processing_status
            );
        },
        canDiscard: function () {
            return this.species_community &&
                this.species_community.processing_status === 'Draft'
                ? true
                : false;
        },
        comms_url: function () {
            return this.species_community.group_type === 'community'
                ? helpers.add_endpoint_json(
                      api_endpoints.community,
                      this.$route.params.species_community_id + '/comms_log'
                  )
                : helpers.add_endpoint_json(
                      api_endpoints.species,
                      this.$route.params.species_community_id + '/comms_log'
                  );
        },
        comms_add_url: function () {
            return this.species_community.group_type === 'community'
                ? helpers.add_endpoint_json(
                      api_endpoints.community,
                      this.$route.params.species_community_id + '/add_comms_log'
                  )
                : helpers.add_endpoint_json(
                      api_endpoints.species,
                      this.$route.params.species_community_id + '/add_comms_log'
                  );
        },
        logs_url: function () {
            return this.species_community.group_type === 'community'
                ? helpers.add_endpoint_json(
                      api_endpoints.community,
                      this.$route.params.species_community_id + '/action_log'
                  )
                : helpers.add_endpoint_json(
                      api_endpoints.species,
                      this.$route.params.species_community_id + '/action_log'
                  );
        },
        image_history_url: function () {
            return this.species_community.group_type == 'community'
                ? `/api/community/${this.species_community.id}/image_history/`
                : `/api/species/${this.species_community.id}/image_history/`;
        },
        reinstate_image_title: function () {
            return this.species_community
                ? `Reinstate Image for ${this.display_name}`
                : '';
        },
        inViewMode: function () {
            return this.$route.query.action === 'view';
        },
    },
    created: function () {
        if (!this.species_community) {
            let end_point_type = 'species';
            if (this.$route.query.group_type_name === 'community') {
                end_point_type = 'community';
            }
            fetch(
                `/api/${end_point_type}/${this.$route.params.species_community_id}/internal_${end_point_type}.json`
            ).then(
                async (response) => {
                    const data = await response.json();
                    this.species_community = data[`${end_point_type}_obj`]; //--temp community_obj
                    this.species_community_original = helpers.copyObject(
                        this.species_community
                    );
                    this.speciesCommunitiesImage =
                        this.species_community.image_doc;
                },
                (err) => {
                    console.log(err);
                }
            );
        }
    },
    updated: function () {
        let vm = this;
        this.$nextTick(() => {
            vm.form = document.forms.new_species;
        });
    },
    methods: {
        commaToNewline(s) {
            return s.replace(/[,;]/g, '\n');
        },
        returnToDashboard: function () {
            let vm = this;
            vm.$router.push({
                name: 'internal-species-communities-dash',
            });
        },
        uploadImage: function (event) {
            let vm = this;
            const files = event.target.files;
            const imageFile = files[0];
            if (!imageFile) {
                vm.uploadingImage = false;
                swal.fire({
                    title: 'Upload Image',
                    html: 'Please select an Image to upload.',
                    icon: 'error',
                    customClass: {
                        confirmButton: 'btn btn-primary',
                    },
                });
            } else {
                vm.uploadingImage = true;
                let data = new FormData();
                data.append('speciesCommunitiesImage', imageFile);
                var api_url = api_endpoints.species;
                if (this.species_community.group_type == 'community') {
                    api_url = api_endpoints.community;
                }
                fetch(
                    helpers.add_endpoint_json(
                        api_url,
                        this.$route.params.species_community_id +
                            '/upload_image'
                    ),
                    {
                        method: 'POST',
                        body: data,
                        // Do not set Content-Type when sending FormData; the browser
                        // will set the correct multipart/form-data boundary.
                    }
                ).then(
                    async (response) => {
                        const data = await response.json();
                        vm.uploadingImage = false;
                        vm.speciesCommunitiesImage = null;
                        vm.speciesCommunitiesImage = data.image_doc;
                        vm.species_community.image_doc = data.image_doc;
                    },
                    (error) => {
                        console.log(error);
                        vm.uploadingImage = false;
                        vm.speciesCommunitiesImage = null;
                        let error_msg = '<br/>';
                        for (var key in error.body) {
                            error_msg += key + ': ' + error.body[key] + '<br/>';
                        }
                        swal.fire({
                            title: 'Upload ID',
                            html:
                                'There was an error uploading your ID.<br/>' +
                                error_msg,
                            icon: 'error',
                            customClass: {
                                confirmButton: 'btn btn-primary',
                            },
                        });
                    }
                );
            }
        },
        showReinstateImageModal: function () {
            this.$refs.reinstateImage.isModalOpen = true;
        },
        reinstateImage: function (image) {
            var api_url = api_endpoints.species;
            if (this.species_community.group_type == 'community') {
                api_url = api_endpoints.community;
            }
            fetch(
                helpers.add_endpoint_json(
                    api_url,
                    this.$route.params.species_community_id + '/reinstate_image'
                ),
                {
                    method: 'PATCH',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify({
                        pk: image.id,
                    }),
                }
            )
                .then(async (response) => {
                    if (!response.ok) {
                        const data = await response.json();
                        swal.fire({
                            title: 'Error',
                            text: JSON.stringify(data),
                            icon: 'error',
                            customClass: {
                                confirmButton: 'btn btn-primary',
                            },
                        });
                        return;
                    }
                    this.speciesCommunitiesImage = image.url;
                    this.species_community.image_doc = image.url;
                })
                .catch((error) => {
                    swal.fire({
                        title: 'Reinstate Image',
                        text: 'There was an error reinstating the image',
                        icon: 'error',
                        customClass: {
                            confirmButton: 'btn btn-primary',
                        },
                    });
                    console.log(error);
                });
        },
        confirmDiscardImage: function () {
            swal.fire({
                title: 'Discard Image',
                text: 'Are you sure you want to discard this image?',
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Discard',
                customClass: {
                    confirmButton: 'btn btn-primary',
                    cancelButton: 'btn btn-secondary',
                },
                reverseButtons: true,
            }).then((swalresult) => {
                if (swalresult.isConfirmed) {
                    this.discardImage();
                }
            });
        },
        discardImage: async function () {
            let vm = this;
            var api_url = api_endpoints.species;
            if (this.species_community.group_type == 'community') {
                api_url = api_endpoints.community;
            }

            if (vm.speciesCommunitiesImage == null) {
                swal.fire({
                    title: 'Delete Image',
                    html: 'No Image uploaded.',
                    icon: 'error',
                    customClass: {
                        confirmButton: 'btn btn-primary',
                    },
                });
            } else {
                if (vm.species_community.image_doc) {
                    fetch(
                        helpers.add_endpoint_json(
                            api_url,
                            this.$route.params.species_community_id +
                                '/delete_image'
                        ),
                        {
                            method: 'POST',
                            headers: {
                                'Content-Type': 'application/json',
                            },
                        }
                    ).then(
                        async (response) => {
                            const data = await response.json();
                            vm.uploadingImage = false;
                            vm.speciesCommunitiesImage = null;
                            vm.speciesCommunitiesImage = data.image_doc;
                            vm.species_community.image_doc = data.image_doc;
                        },
                        (error) => {
                            console.log(error);
                            vm.uploadingImage = false;
                            vm.speciesCommunitiesImage = null;
                            let error_msg = '<br/>';
                            for (var key in error.body) {
                                error_msg +=
                                    key + ': ' + error.body[key] + '<br/>';
                            }
                            swal.fire({
                                title: 'Delete Image',
                                html:
                                    'There was an error deleting your image.<br/>' +
                                    error_msg,
                                icon: 'error',
                                customClass: {
                                    confirmButton: 'btn btn-primary',
                                },
                            });
                        }
                    );
                }
            }
        },
        discardSpeciesProposal: function () {
            let vm = this;
            swal.fire({
                title: 'Discard Proposal',
                text: 'Are you sure you want to discard this proposal?',
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Discard Proposal',
                customClass: {
                    confirmButton: 'btn btn-primary',
                    cancelButton: 'btn btn-secondary',
                },
                reverseButtons: true,
            }).then((swalresult) => {
                if (swalresult.isConfirmed) {
                    fetch(
                        api_endpoints.discard_species_proposal(
                            vm.species_community.id
                        ),
                        {
                            method: 'PATCH',
                            headers: {
                                'Content-Type': 'application/json',
                            },
                        }
                    ).then(
                        async (response) => {
                            if (!response.ok) {
                                const data = await response.json();
                                swal.fire({
                                    title: 'Error',
                                    text: JSON.stringify(data),
                                    icon: 'error',
                                    customClass: {
                                        confirmButton: 'btn btn-primary',
                                    },
                                });
                                return;
                            }
                            swal.fire({
                                title: 'Discarded',
                                text: 'The proposal has been discarded',
                                icon: 'success',
                                customClass: {
                                    confirmButton: 'btn btn-primary',
                                },
                            });
                            vm.$router.push({
                                name: 'internal-species-communities-dash',
                            });
                        },
                        (error) => {
                            console.log(error);
                        }
                    );
                }
            });
        },
        save: async function () {
            let vm = this;
            let check_action =
                this.species_community.processing_status === 'Active'
                    ? 'submit'
                    : '';
            var missing_data = vm.can_submit(check_action);
            vm.isSaved = false;
            if (missing_data != true) {
                swal.fire({
                    title: 'Please fix following errors before saving',
                    text: missing_data,
                    icon: 'error',
                    customClass: {
                        confirmButton: 'btn btn-primary',
                    },
                });
                return false;
            }
            vm.savingSpeciesCommunity = true;
            let payload = new Object();
            Object.assign(payload, vm.species_community);

            await fetch(vm.species_community_form_url, {
                method: 'POST',
                body: JSON.stringify(payload),
                headers: {
                    'Content-Type': 'application/json',
                },
            })
                .then(
                    async (response) => {
                        const data = await response.json();
                        if (!response.ok) {
                            swal.fire({
                                title: 'Error',
                                text: JSON.stringify(data),
                                icon: 'error',
                                customClass: {
                                    confirmButton: 'btn btn-primary',
                                },
                            });
                            return;
                        }
                        vm.species_community_original = helpers.copyObject(
                            vm.species_community
                        ); //update original after save
                        swal.fire({
                            title: 'Saved',
                            text: 'Your changes have been saved',
                            icon: 'success',
                            showConfirmButton: false,
                            timer: 1200,
                        });
                        vm.isSaved = true;
                    },
                    (err) => {
                        var errorText = helpers.apiVueResourceError(err);
                        swal.fire({
                            title: 'Save Error',
                            text: errorText,
                            icon: 'error',
                            customClass: {
                                confirmButton: 'btn btn-primary',
                            },
                        });
                        vm.isSaved = false;
                    }
                )
                .finally(() => {
                    vm.savingSpeciesCommunity = false;
                });
        },
        save_exit: async function () {
            let vm = this;
            let check_action =
                this.species_community.processing_status === 'Active'
                    ? 'submit'
                    : '';
            var missing_data = vm.can_submit(check_action);
            if (missing_data != true) {
                swal.fire({
                    title: 'Please fix following errors before saving',
                    text: missing_data,
                    icon: 'error',
                    customClass: {
                        confirmButton: 'btn btn-primary',
                    },
                });
                //vm.paySubmitting=false;
                return false;
            }
            vm.saveExitSpeciesCommunity = true;
            await vm.save().then(() => {
                if (vm.isSaved === true) {
                    vm.$router.push({
                        name: 'internal-species-communities-dash',
                    });
                } else {
                    vm.saveExitSpeciesCommunity = false;
                }
            });
        },
        save_before_submit: async function () {
            let vm = this;
            vm.saveError = false;

            //only save if something has changed
            if (vm.can_submit('') != true) {
                return false;
            }

            let payload = new Object();
            Object.assign(payload, vm.species_community);
            const result = await fetch(vm.species_community_form_url, {
                method: 'POST',
                body: JSON.stringify(payload),
                headers: {
                    'Content-Type': 'application/json',
                },
            }).then(
                async (response) => {
                    if (!response.ok) {
                        const data = await response.json();
                        swal.fire({
                            title: 'Error',
                            text: JSON.stringify(data),
                            icon: 'error',
                            customClass: {
                                confirmButton: 'btn btn-primary',
                            },
                        });
                        vm.saveError = true;
                        return;
                    }
                },
                (err) => {
                    var errorText = helpers.apiVueResourceError(err);
                    swal.fire({
                        title: 'Submit Error',
                        //helpers.apiVueResourceError(err),
                        text: errorText,
                        icon: 'error',
                        customClass: {
                            confirmButton: 'btn btn-primary',
                        },
                    });
                    vm.submitSpeciesCommunity = false;
                    vm.saveError = true;
                    //return false;
                }
            );
            return result;
        },
        can_submit: function (check_action) {
            let vm = this;

            let blank_fields = [];
            if (
                vm.species_community.group_type == 'flora' ||
                vm.species_community.group_type == 'fauna'
            ) {
                if (
                    vm.species_community.taxonomy_id == null ||
                    vm.species_community.taxonomy_id == ''
                ) {
                    blank_fields.push(' Scientific Name is required');
                }
            }
            if (check_action == 'submit') {
                if (vm.species_community.group_type == 'community') {
                    if (
                        vm.species_community.taxonomy_details.community_name ==
                            null ||
                        vm.species_community.taxonomy_details.community_name ==
                            ''
                    ) {
                        blank_fields.push(' Community Name is required');
                    }
                    if (
                        vm.species_community.taxonomy_details
                            .community_common_id == null ||
                        vm.species_community.taxonomy_details
                            .community_common_id == ''
                    ) {
                        blank_fields.push(' Community ID is required');
                    }
                }
                if (
                    vm.species_community.distribution.distribution == null ||
                    vm.species_community.distribution.distribution == ''
                ) {
                    blank_fields.push(' Distribution is required');
                }
                if (
                    vm.species_community.regions == null ||
                    vm.species_community.regions.length == 0 ||
                    vm.species_community.regions == ''
                ) {
                    blank_fields.push(' Region is required');
                }
                if (
                    vm.species_community.districts == null ||
                    vm.species_community.districts == '' ||
                    vm.species_community.districts.length == 0
                ) {
                    blank_fields.push(' District is required');
                }
            }
            if (blank_fields.length == 0) {
                return true;
            } else {
                return blank_fields;
            }
        },
        submit: async function () {
            let vm = this;

            var missing_data = vm.can_submit('submit');
            if (missing_data != true) {
                swal.fire({
                    title: 'Please fix following errors before submitting',
                    text: missing_data,
                    icon: 'error',
                    customClass: {
                        confirmButton: 'btn btn-primary',
                    },
                });
                //vm.paySubmitting=false;
                return false;
            }

            vm.submitSpeciesCommunity = true;
            swal.fire({
                title: 'Activate Record',
                text: 'Are you sure you want to activate this record?',
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Activate',
                customClass: {
                    confirmButton: 'btn btn-primary',
                    cancelButton: 'btn btn-secondary',
                },
                reverseButtons: true,
            })
                .then(
                    async (swalresult) => {
                        if (swalresult.isConfirmed) {
                            await vm.save_before_submit();
                            if (!vm.saveError) {
                                let payload = new Object();
                                Object.assign(payload, vm.species_community);
                                let submit_url =
                                    this.species_community.group_type ===
                                    'community'
                                        ? helpers.add_endpoint_json(
                                              api_endpoints.community,
                                              vm.species_community.id +
                                                  '/submit'
                                          )
                                        : helpers.add_endpoint_json(
                                              api_endpoints.species,
                                              vm.species_community.id +
                                                  '/submit'
                                          );
                                fetch(submit_url, {
                                    method: 'POST',
                                    body: JSON.stringify(payload),
                                    headers: {
                                        'Content-Type': 'application/json',
                                    },
                                }).then(
                                    async (response) => {
                                        vm.species_community =
                                            await response.json();
                                        vm.species_community_original =
                                            helpers.copyObject(
                                                vm.species_community
                                            );
                                    },
                                    (err) => {
                                        swal.fire({
                                            title: 'Submit Error',
                                            text: helpers.apiVueResourceError(
                                                err
                                            ),
                                            icon: 'error',
                                            customClass: {
                                                confirmButton:
                                                    'btn btn-primary',
                                            },
                                        });
                                    }
                                );
                            }
                        }
                    },
                    (error) => {
                        console.log(error);
                    }
                )
                .finally(() => {
                    vm.submitSpeciesCommunity = false;
                });
        },
        save_wo: function () {
            let vm = this;
            let check_action =
                this.species_community.processing_status === 'Active'
                    ? 'submit'
                    : '';
            var missing_data = vm.can_submit(check_action);
            if (missing_data != true) {
                swal.fire({
                    title: 'Please fix following errors before saving',
                    text: missing_data,
                    icon: 'error',
                    customClass: {
                        confirmButton: 'btn btn-primary',
                    },
                });
                //vm.paySubmitting=false;
                return false;
            }
            fetch(vm.species_community_form_url, {
                method: 'POST',
                body: JSON.stringify(vm.species_community),
                headers: {
                    'Content-Type': 'application/json',
                },
            }).then(
                async (response) => {
                    if (!response.ok) {
                        const data = await response.json();
                        swal.fire({
                            title: 'Error',
                            text: JSON.stringify(data),
                            icon: 'error',
                            customClass: {
                                confirmButton: 'btn btn-primary',
                            },
                        });
                        return;
                    }
                    vm.species_community_original = helpers.copyObject(
                        vm.species_community
                    ); //update original after save
                },
                (err) => {
                    var errorText = helpers.apiVueResourceError(err);
                    swal.fire({
                        title: 'Save Error',
                        text: errorText,
                        icon: 'error',
                        customClass: {
                            confirmButton: 'btn btn-primary',
                        },
                    });
                }
            );
        },
        refreshFromResponse: async function (response) {
            let vm = this;
            const data = await response.json();
            vm.species_community = helpers.copyObject(data);
            vm.species_community_original = helpers.copyObject(
                vm.species_community
            );
        },
        refreshSpeciesCommunity: function () {
            let vm = this;
            if (
                vm.species_community.group_type === 'flora' ||
                vm.species_community.group_type === 'fauna'
            ) {
                fetch(
                    `/api/species/${vm.species_community.id}/internal_species.json`
                ).then(
                    async (response) => {
                        const data = await response.json();
                        vm.species_community = data.species_obj;
                        vm.species_community_original = helpers.copyObject(
                            vm.species_community
                        );
                        vm.speciesCommunitiesImage =
                            vm.species_community.image_doc;
                    },
                    (err) => {
                        console.log(err);
                    }
                );
            } else {
                fetch(
                    `/api/community/${vm.species_community.id}/internal_community.json`
                ).then(
                    async (response) => {
                        const data = await response.json();
                        vm.species_community = data.community_obj;
                        vm.species_community_original = helpers.copyObject(
                            vm.species_community
                        );
                        vm.speciesCommunitiesImage =
                            vm.species_community.image_doc;
                    },
                    (err) => {
                        console.log(err);
                    }
                );
            }
        },
        splitSpecies: async function () {
            this.$refs.species_split.isModalOpen = true;
        },
        combineSpecies: async function () {
            this.$refs.species_combine.isModalOpen = true;
        },
        renameSpecies: async function () {
            this.$refs.species_rename.isModalOpen = true;
        },
        renameCommunity: async function () {
            this.$refs.community_rename.isModalOpen = true;
        },
        reactivateCommunity: function () {
            let vm = this;
            swal.fire({
                title: 'Reactivate Community',
                text: 'Are you sure you want to reactivate this community?',
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Reactivate',
                customClass: {
                    confirmButton: 'btn btn-primary',
                    cancelButton: 'btn btn-secondary',
                },
                reverseButtons: true,
            }).then((swalresult) => {
                if (swalresult.isConfirmed) {
                    fetch(
                        helpers.add_endpoint_json(
                            api_endpoints.community,
                            vm.species_community.id + '/reactivate'
                        ),
                        {
                            method: 'POST',
                            headers: {
                                'Content-Type': 'application/json',
                            },
                        }
                    ).then(
                        async (response) => {
                            const data = await response.json();
                            if (!response.ok) {
                                swal.fire({
                                    title: 'Error',
                                    text: JSON.stringify(data),
                                    icon: 'error',
                                    customClass: {
                                        confirmButton: 'btn btn-primary',
                                    },
                                });
                                return;
                            }
                            vm.species_community = helpers.copyObject(data);
                            vm.species_community_original = helpers.copyObject(
                                vm.species_community
                            );
                            swal.fire({
                                title: 'Reactivated',
                                text: 'Community has been reactivated',
                                icon: 'success',
                                customClass: {
                                    confirmButton: 'btn btn-primary',
                                },
                            });
                        },
                        (error) => {
                            console.log(error);
                            swal.fire({
                                title: 'Error',
                                text: 'There was an error reactivating the community',
                                icon: 'error',
                                customClass: {
                                    confirmButton: 'btn btn-primary',
                                },
                            });
                        }
                    );
                }
            });
        },
        deactivateCommunity: function () {
            let vm = this;
            swal.fire({
                title: 'Deactivate Community',
                text: 'Are you sure you want to deactivate this community?',
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Deactivate',
                customClass: {
                    confirmButton: 'btn btn-primary',
                    cancelButton: 'btn btn-secondary',
                },
                reverseButtons: true,
            }).then((swalresult) => {
                if (swalresult.isConfirmed) {
                    fetch(
                        helpers.add_endpoint_json(
                            api_endpoints.community,
                            vm.species_community.id + '/deactivate'
                        ),
                        {
                            method: 'POST',
                            headers: {
                                'Content-Type': 'application/json',
                            },
                        }
                    ).then(
                        async (response) => {
                            const data = await response.json();
                            if (!response.ok) {
                                swal.fire({
                                    title: 'Error',
                                    text: JSON.stringify(data),
                                    icon: 'error',
                                    customClass: {
                                        confirmButton: 'btn btn-primary',
                                    },
                                });
                                return;
                            }
                            vm.species_community = helpers.copyObject(data);
                            vm.species_community_original = helpers.copyObject(
                                vm.species_community
                            );
                            swal.fire({
                                title: 'Deactivated',
                                text: 'Community has been deactivated',
                                icon: 'success',
                                customClass: {
                                    confirmButton: 'btn btn-primary',
                                },
                            });
                        },
                        (error) => {
                            console.log(error);
                            swal.fire({
                                title: 'Error',
                                text: 'There was an error deactivating the community',
                                icon: 'error',
                                customClass: {
                                    confirmButton: 'btn btn-primary',
                                },
                            });
                        }
                    );
                }
            });
        },
        makePublic: async function () {
            this.$refs.make_public.isModalOpen = true;
        },
        makePrivate: function () {
            let vm = this;
            swal.fire({
                title: 'Make Private',
                text: 'Are you sure you want to make this record private?',
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Make Private',
                customClass: {
                    confirmButton: 'btn btn-primary',
                    cancelButton: 'btn btn-secondary',
                },
                reverseButtons: true,
            }).then((swalresult) => {
                if (swalresult.isConfirmed) {
                    let endpoint = api_endpoints.species;
                    if (this.species_community.group_type === 'community') {
                        vm.species_community.publishing_status.community_public = false;
                        endpoint = api_endpoints.community;
                    } else {
                        vm.species_community.publishing_status.species_public = false;
                    }
                    let data = JSON.stringify(
                        vm.species_community.publishing_status
                    );
                    fetch(
                        helpers.add_endpoint_json(
                            endpoint,
                            vm.species_community.id +
                                '/update_publishing_status'
                        ),
                        {
                            method: 'PATCH',
                            body: data,
                            headers: {
                                'Content-Type': 'application/json',
                            },
                        }
                    ).then(
                        async (response) => {
                            vm.updatingPublishing = false;
                            vm.species_community.publishing_status =
                                await response.json();
                            vm.species_community_original.publishing_status =
                                helpers.copyObject(
                                    vm.species_community.publishing_status
                                );
                            swal.fire({
                                title: 'Saved',
                                text: 'Record has been made private',
                                icon: 'success',
                                showConfirmButton: false,
                                timer: 1200,
                            });
                        },
                        (error) => {
                            var text = helpers.apiVueResourceError(error);
                            swal.fire({
                                title: 'Error',
                                text:
                                    'Publishing settings cannot be updated because of the following error: ' +
                                    text,
                                icon: 'error',
                                customClass: {
                                    confirmButton: 'btn btn-primary',
                                },
                            });
                            vm.updatingPublishing = false;
                        }
                    );
                }
            });
        },
        onImageLoad: function () {
            this.downloadingImage = false;
        },
        onImageError: function () {
            // Show a simple inline SVG placeholder if the image cannot be loaded
            const placeholderSVG = `data:image/svg+xml;utf8,${encodeURIComponent(
                '<svg xmlns="http://www.w3.org/2000/svg" width="258" height="258">' +
                    '<rect width="100%" height="100%" fill="#f5f5f5"/>' +
                    '<text x="50%" y="50%" dominant-baseline="middle" text-anchor="middle" fill="#6c757d" font-family="Arial, Helvetica, sans-serif" font-size="16">Image File Not Found</text>' +
                    '</svg>'
            )}`;

            // Avoid repeatedly setting the same failing src to prevent loops
            if (this.speciesCommunitiesImage !== placeholderSVG) {
                this.speciesCommunitiesImage = placeholderSVG;
                // keep downloadingImage true so placeholder's load event will clear the shimmer
            } else {
                // If even the placeholder fails for some reason, stop the loading animation
                this.downloadingImage = false;
            }
        },
        activateEditMode: function () {
            this.$router
                .push({
                    name: 'internal-species-communities',
                    query: {
                        group_type_name: this.$route.query.group_type_name,
                        action: 'edit',
                    },
                })
                .then(() => {
                    this.formKey++;
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
