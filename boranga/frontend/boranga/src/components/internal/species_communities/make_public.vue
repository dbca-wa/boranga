<template lang="html">
    <div id="makePublic">
        <modal
            id="make-public-modal"
            transition="modal fade"
            :title="title"
            large
            @ok="ok()"
            @cancel="cancel()"
        >
            <div class="container-fluid">
                <div class="row">
                    <form class="form-horizontal" name="makePublicForm">
                        <div class="row mb-3">
                            <label class="col-sm-6 control-label"
                                >Distribution:
                            </label>
                            <div class="col-sm-6">
                                <label
                                    for="make_public_distribution_publishing_private"
                                    class="me-2"
                                    >Private</label
                                >
                                <input
                                    id="make_public_distribution_publishing_private"
                                    v-model="
                                        species_community.publishing_status
                                            .distribution_public
                                    "
                                    :disabled="!isActive"
                                    type="radio"
                                    :value="false"
                                    class="form-check-input me-2"
                                />
                                <label
                                    for="make_public_distribution_publishing_public"
                                    class="me-2"
                                    >Public</label
                                >
                                <input
                                    id="make_public_distribution_publishing_public"
                                    v-model="
                                        species_community.publishing_status
                                            .distribution_public
                                    "
                                    :disabled="!isActive"
                                    type="radio"
                                    :value="true"
                                    class="form-check-input"
                                />
                            </div>
                        </div>
                        <div class="row mb-3">
                            <label class="col-sm-6 control-label"
                                >Conservation Status:
                            </label>
                            <div class="col-sm-6">
                                <label
                                    for="make_public_conservation_status_publishing_private"
                                    class="me-2"
                                    >Private</label
                                >
                                <input
                                    id="make_public_conservation_status_publishing_private"
                                    v-model="
                                        species_community.publishing_status
                                            .conservation_status_public
                                    "
                                    :disabled="!isActive"
                                    type="radio"
                                    :value="false"
                                    class="form-check-input me-2"
                                />
                                <label
                                    for="make_public_conservation_status_publishing"
                                    class="me-2"
                                    >Public</label
                                >
                                <input
                                    id="make_public_conservation_status_publishing"
                                    v-model="
                                        species_community.publishing_status
                                            .conservation_status_public
                                    "
                                    :disabled="!isActive"
                                    type="radio"
                                    :value="true"
                                    class="form-check-input"
                                />
                            </div>
                        </div>
                        <!-- <div class="row mb-3">
                            <label for="conservation_attributes_publishing" class="col-sm-6 control-label">Conservation
                                Attributes: </label>
                            <div class="col-sm-6">
                                <label for="conservation_attributes_publishing" class="me-2">Private</label>
                                <input :disabled="!isActive" type="radio" :value="false" class="form-check-input me-2"
                                    id="conservation_attributes_publishing"
                                    v-model="species_community.publishing_status.conservation_attributes_public">
                                <label for="conservation_attributes_publishing" class="me-2">Public</label>
                                <input :disabled="!isActive" type="radio" :value="true" class="form-check-input"
                                    id="conservation_attributes_publishing"
                                    v-model="species_community.publishing_status.conservation_attributes_public">
                            </div>
                        </div> -->
                        <div class="row mb-3">
                            <label class="col-sm-6 control-label"
                                >Threats:
                            </label>
                            <div class="col-sm-6">
                                <label
                                    for="make_public_threats_publishing_private"
                                    class="me-2"
                                    >Private</label
                                >
                                <input
                                    id="make_public_threats_publishing_private"
                                    v-model="
                                        species_community.publishing_status
                                            .threats_public
                                    "
                                    :disabled="!isActive"
                                    type="radio"
                                    :value="false"
                                    class="form-check-input me-2"
                                />
                                <label
                                    for="make_public_threats_publishing_public"
                                    class="me-2"
                                    >Public</label
                                >
                                <input
                                    id="make_public_threats_publishing_public"
                                    v-model="
                                        species_community.publishing_status
                                            .threats_public
                                    "
                                    :disabled="!isActive"
                                    type="radio"
                                    :value="true"
                                    class="form-check-input"
                                />
                            </div>
                        </div>
                    </form>
                </div>
            </div>
            <template #footer>
                <div>
                    <button
                        type="button"
                        class="btn btn-secondary me-2"
                        @click="cancel"
                    >
                        Cancel
                    </button>
                    <button
                        v-if="updatingPublishing"
                        class="btn btn-primary pull-right"
                        style="margin-top: 5px"
                        disabled
                    >
                        Make Public
                        <span
                            class="spinner-border spinner-border-sm"
                            role="status"
                            aria-hidden="true"
                        ></span>
                        <span class="visually-hidden">Loading...</span>
                    </button>
                    <button
                        v-else
                        class="btn btn-primary"
                        :disabled="updatingPublishing"
                        @click.prevent="ok()"
                    >
                        Make Public
                    </button>
                </div>
            </template>
        </modal>
    </div>
</template>

<script>
import modal from '@vue-utils/bootstrap-modal.vue';
import { helpers, api_endpoints } from '@/utils/hooks.js';
export default {
    name: 'MakePublic',
    components: {
        modal,
    },
    props: {
        species_community: {
            type: Object,
            required: true,
        },
        species_community_original: {
            type: Object,
            required: true,
        },
        is_internal: {
            type: Boolean,
            required: true,
        },
    },
    data: function () {
        return {
            updatingPublishing: false,
            isModalOpen: false,
            form: null,
        };
    },
    computed: {
        csrf_token: function () {
            return helpers.getCookie('csrftoken');
        },
        title: function () {
            return 'Make Public';
        },
        distribution_public: function () {
            return (
                this.isPublic &&
                this.species_community.publishing_status.distribution_public
            );
        },
        conservation_status_public: function () {
            return (
                this.isPublic &&
                this.species_community.publishing_status
                    .conservation_status_public
            );
        },
        conservation_attributes_public: function () {
            return (
                this.isPublic &&
                this.species_community.publishing_status
                    .conservation_attributes_public
            );
        },
        isActive: function () {
            return this.species_community.processing_status === 'Active'
                ? true
                : false;
        },
        isPublic: function () {
            return this.isActive &&
                this.species_community.publishing_status.species_public
                ? true
                : false;
        },
    },
    mounted: function () {
        let vm = this;
        vm.form = document.forms.makePublicForm;
    },
    methods: {
        updatePublishing(data) {
            let vm = this;

            let endpoint = api_endpoints.species;
            if (this.species_community.group_type === 'community') {
                endpoint = api_endpoints.community;
            }

            fetch(
                helpers.add_endpoint_json(
                    endpoint,
                    vm.species_community.id + '/update_publishing_status'
                ),
                {
                    method: 'PATCH',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: data,
                }
            )
                .then(async (response) => {
                    const data = await response.json();
                    if (!response.ok) {
                        swal.fire({
                            title: 'Error',
                            text:
                                'Publishing settings cannot be updated because of the following error: ' +
                                data,
                            icon: 'error',
                            customClass: {
                                confirmButton: 'btn btn-primary',
                            },
                        });
                        return;
                    }
                    vm.species_community.publishing_status = data;
                    vm.species_community_original.publishing_status =
                        helpers.copyObject(
                            vm.species_community.publishing_status
                        );
                    swal.fire({
                        title: 'Saved',
                        text: 'Record has been made public',
                        icon: 'success',
                        showConfirmButton: false,
                        timer: 1200,
                    });
                    vm.close();
                })
                .finally(() => {
                    vm.updatingPublishing = false;
                });
        },
        ok: function () {
            let vm = this;
            vm.updatingPublishing = true;
            //if not already public, we make it public (notify user first)
            //but only if it is active
            if (!vm.isPublic && vm.isActive) {
                swal.fire({
                    title: 'Make Public',
                    text: 'Are you sure you want to make this record public?',
                    icon: 'question',
                    showCancelButton: true,
                    confirmButtonText: 'Make Public',
                    customClass: {
                        confirmButton: 'btn btn-primary',
                        cancelButton: 'btn btn-secondary',
                    },
                    reverseButtons: true,
                }).then((swalresult) => {
                    if (swalresult.isConfirmed) {
                        //send with make public set to true
                        if (this.species_community.group_type === 'community') {
                            vm.species_community.publishing_status.community_public = true;
                        } else {
                            vm.species_community.publishing_status.species_public = true;
                        }
                        let data = JSON.stringify(
                            vm.species_community.publishing_status
                        );
                        vm.updatePublishing(data);
                    } else {
                        vm.updatingPublishing = false;
                    }
                });
            } else {
                swal.fire({
                    title: 'Error',
                    text: 'Record already public',
                    icon: 'error',
                    customClass: {
                        confirmButton: 'btn btn-primary',
                    },
                });
                vm.updatingPublishing = false;
            }
        },
        cancel: function () {
            swal.fire({
                title: 'Are you sure you want to close this pop-up?',
                text: 'You will lose any unsaved changes.',
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Yes, close it',
                cancelButtonText: 'Return to pop-up',
                reverseButtons: true,
                customClass: {
                    confirmButton: 'btn btn-primary',
                    cancelButton: 'btn btn-secondary',
                },
            }).then((result) => {
                if (result.isConfirmed) {
                    this.close();
                }
            });
        },
        close: function () {
            this.isModalOpen = false;
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

.nav-pills > li {
    position: relative;
}

.nav-pills > li > a {
    display: inline-block;
}

.nav-pills > li > span {
    display: none;
    cursor: pointer;
    position: absolute;
    right: 6px;
    top: 8px;
    color: red;
}

.nav-pills > li:hover > span {
    display: inline-block;
}
</style>
