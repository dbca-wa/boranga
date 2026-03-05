<template>
    <div id="internalUserInfo" class="container-fluid">
        <div class="row">
            <div class="col-md-10 col-md-offset-1">
                <div class="row">
                    <h3>
                        {{ user.first_name }} {{ user.last_name }} -
                        {{ user.dob }} ({{ user.email }})
                    </h3>
                    <div class="col-md-3">
                        <CommsLogs
                            :comms_url="comms_url"
                            :logs_url="logs_url"
                            :comms_add_url="comms_add_url"
                            :is_user_log="true"
                            :disable_add_entry="false"
                        />
                    </div>
                    <div class="col-md-9">
                        <ul class="nav nav-tabs">
                            <li class="active">
                                <a data-toggle="tab" :href="'#' + dTab"
                                    >Details</a
                                >
                            </li>
                        </ul>
                        <div class="tab-content">
                            <div :id="dTab" class="tab-pane fade in active">
                                <div class="row">
                                    <div class="col-sm-12">
                                        <div class="panel panel-default">
                                            <div class="panel-heading">
                                                <h3 class="panel-title">
                                                    Personal Details
                                                    <a
                                                        class="panelClicker"
                                                        :href="'#' + pdBody"
                                                        data-toggle="collapse"
                                                        data-parent="#userInfo"
                                                        expanded="true"
                                                        :aria-controls="pdBody"
                                                    >
                                                        <span
                                                            class="glyphicon glyphicon-chevron-up pull-right"
                                                        ></span>
                                                    </a>
                                                </h3>
                                            </div>
                                            <div
                                                :id="pdBody"
                                                class="panel-body collapse in"
                                            >
                                                <form
                                                    class="form-horizontal"
                                                    name="personal_form"
                                                    method="post"
                                                >
                                                    <div class="form-group">
                                                        <label
                                                            for=""
                                                            class="col-sm-3 control-label"
                                                            >Given
                                                            Name(s)</label
                                                        >
                                                        <div class="col-sm-6">
                                                            <input
                                                                v-model="
                                                                    user.first_name
                                                                "
                                                                type="text"
                                                                class="form-control"
                                                                name="first_name"
                                                                placeholder=""
                                                            />
                                                        </div>
                                                    </div>
                                                    <div class="form-group">
                                                        <label
                                                            for=""
                                                            class="col-sm-3 control-label"
                                                            >Last Name</label
                                                        >
                                                        <div class="col-sm-6">
                                                            <input
                                                                v-model="
                                                                    user.last_name
                                                                "
                                                                type="text"
                                                                class="form-control"
                                                                name="last_name"
                                                                placeholder=""
                                                            />
                                                        </div>
                                                    </div>
                                                    <div class="form-group">
                                                        <div class="col-sm-12">
                                                            <button
                                                                v-if="
                                                                    !updatingPersonal
                                                                "
                                                                class="pull-right btn btn-primary"
                                                                @click.prevent="
                                                                    updatePersonal()
                                                                "
                                                            >
                                                                Update
                                                            </button>
                                                            <button
                                                                v-else
                                                                disabled
                                                                class="pull-right btn btn-primary"
                                                            >
                                                                Updating
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
                                                        </div>
                                                    </div>
                                                </form>
                                            </div>
                                        </div>
                                    </div>
                                </div>
                                <div class="row">
                                    <div class="col-sm-12">
                                        <div class="panel panel-default">
                                            <div class="panel-heading">
                                                <h3 class="panel-title">
                                                    Address Details
                                                    <a
                                                        class="panelClicker"
                                                        :href="'#' + adBody"
                                                        data-toggle="collapse"
                                                        expanded="false"
                                                        data-parent="#userInfo"
                                                        :aria-controls="adBody"
                                                    >
                                                        <span
                                                            class="glyphicon glyphicon-chevron-up pull-right"
                                                        ></span>
                                                    </a>
                                                </h3>
                                            </div>
                                            <div
                                                v-if="loading.length == 0"
                                                :id="adBody"
                                                class="panel-body collapse in"
                                            >
                                                <form
                                                    class="form-horizontal"
                                                    action="index.html"
                                                    method="post"
                                                >
                                                    <div class="form-group">
                                                        <label
                                                            for=""
                                                            class="col-sm-3 control-label"
                                                            >Street</label
                                                        >
                                                        <div class="col-sm-6">
                                                            <input
                                                                v-model="
                                                                    user
                                                                        .residential_address
                                                                        .line1
                                                                "
                                                                type="text"
                                                                class="form-control"
                                                                name="street"
                                                                placeholder=""
                                                            />
                                                        </div>
                                                    </div>
                                                    <div class="form-group">
                                                        <label
                                                            for=""
                                                            class="col-sm-3 control-label"
                                                            >Town/Suburb</label
                                                        >
                                                        <div class="col-sm-6">
                                                            <input
                                                                v-model="
                                                                    user
                                                                        .residential_address
                                                                        .locality
                                                                "
                                                                type="text"
                                                                class="form-control"
                                                                name="surburb"
                                                                placeholder=""
                                                            />
                                                        </div>
                                                    </div>
                                                    <div class="form-group">
                                                        <label
                                                            for=""
                                                            class="col-sm-3 control-label"
                                                            >State</label
                                                        >
                                                        <div class="col-sm-2">
                                                            <input
                                                                v-model="
                                                                    user
                                                                        .residential_address
                                                                        .state
                                                                "
                                                                type="text"
                                                                class="form-control"
                                                                name="country"
                                                                placeholder=""
                                                            />
                                                        </div>
                                                        <label
                                                            for=""
                                                            class="col-sm-2 control-label"
                                                            >Postcode</label
                                                        >
                                                        <div class="col-sm-2">
                                                            <input
                                                                v-model="
                                                                    user
                                                                        .residential_address
                                                                        .postcode
                                                                "
                                                                type="text"
                                                                class="form-control"
                                                                name="postcode"
                                                                placeholder=""
                                                            />
                                                        </div>
                                                    </div>
                                                    <div class="form-group">
                                                        <label
                                                            for=""
                                                            class="col-sm-3 control-label"
                                                            >Country</label
                                                        >
                                                        <div class="col-sm-4">
                                                            <select
                                                                v-model="
                                                                    user
                                                                        .residential_address
                                                                        .country
                                                                "
                                                                class="form-control"
                                                                name="country"
                                                            >
                                                                <option
                                                                    v-for="c in countries"
                                                                    :value="
                                                                        c.code
                                                                    "
                                                                    :key="
                                                                        c.code
                                                                    "
                                                                >
                                                                    {{ c.name }}
                                                                </option>
                                                            </select>
                                                        </div>
                                                    </div>
                                                    <div class="form-group">
                                                        <div class="col-sm-12">
                                                            <button
                                                                v-if="
                                                                    !updatingAddress
                                                                "
                                                                class="pull-right btn btn-primary"
                                                                @click.prevent="
                                                                    updateAddress()
                                                                "
                                                            >
                                                                Update
                                                            </button>
                                                            <button
                                                                v-else
                                                                disabled
                                                                class="pull-right btn btn-primary"
                                                            >
                                                                Updating
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
                                                        </div>
                                                    </div>
                                                </form>
                                            </div>
                                        </div>
                                    </div>
                                </div>
                                <div class="row">
                                    <div class="col-sm-12">
                                        <div class="panel panel-default">
                                            <div class="panel-heading">
                                                <h3 class="panel-title">
                                                    Contact Details
                                                    <small></small>
                                                    <a
                                                        class="panelClicker"
                                                        :href="'#' + cdBody"
                                                        data-toggle="collapse"
                                                        data-parent="#userInfo"
                                                        expanded="false"
                                                        :aria-controls="cdBody"
                                                    >
                                                        <span
                                                            class="glyphicon glyphicon-chevron-up pull-right"
                                                        ></span>
                                                    </a>
                                                </h3>
                                            </div>
                                            <div
                                                :id="cdBody"
                                                class="panel-body collapse in"
                                            >
                                                <form
                                                    class="form-horizontal"
                                                    action="index.html"
                                                    method="post"
                                                >
                                                    <div class="form-group">
                                                        <label
                                                            for=""
                                                            class="col-sm-3 control-label"
                                                            >Phone (work)</label
                                                        >
                                                        <div class="col-sm-6">
                                                            <input
                                                                v-model="
                                                                    user.phone_number
                                                                "
                                                                type="text"
                                                                class="form-control"
                                                                name="phone"
                                                                placeholder=""
                                                            />
                                                        </div>
                                                    </div>
                                                    <div class="form-group">
                                                        <label
                                                            for=""
                                                            class="col-sm-3 control-label"
                                                            >Mobile</label
                                                        >
                                                        <div class="col-sm-6">
                                                            <input
                                                                v-model="
                                                                    user.mobile_number
                                                                "
                                                                type="text"
                                                                class="form-control"
                                                                name="mobile"
                                                                placeholder=""
                                                            />
                                                        </div>
                                                    </div>
                                                    <div class="form-group">
                                                        <label
                                                            for=""
                                                            class="col-sm-3 control-label"
                                                            >Email</label
                                                        >
                                                        <div class="col-sm-6">
                                                            <input
                                                                v-model="
                                                                    user.email
                                                                "
                                                                type="email"
                                                                class="form-control"
                                                                disabled="disabled"
                                                                name="email"
                                                                placeholder=""
                                                            />
                                                        </div>
                                                    </div>
                                                    <div class="form-group">
                                                        <div class="col-sm-12">
                                                            <button
                                                                v-if="
                                                                    !updatingContact
                                                                "
                                                                class="pull-right btn btn-primary"
                                                                @click.prevent="
                                                                    updateContact()
                                                                "
                                                            >
                                                                Update
                                                            </button>
                                                            <button
                                                                v-else
                                                                disabled
                                                                class="pull-right btn btn-primary"
                                                            >
                                                                Updating
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
                                                        </div>
                                                    </div>
                                                </form>
                                            </div>
                                        </div>
                                    </div>
                                </div>
                                <div class="row">
                                    <div class="col-sm-12">
                                        <div class="panel panel-default">
                                            <div class="panel-heading">
                                                <h3 class="panel-title">
                                                    Organisations
                                                    <small></small>
                                                    <a
                                                        class="panelClicker"
                                                        :href="'#' + odBody"
                                                        data-toggle="collapse"
                                                        data-parent="#userInfo"
                                                        expanded="false"
                                                        :aria-controls="odBody"
                                                    >
                                                        <span
                                                            class="glyphicon glyphicon-chevron-up pull-right"
                                                        ></span>
                                                    </a>
                                                </h3>
                                            </div>
                                            <div
                                                :id="odBody"
                                                class="panel-body collapse in"
                                            >
                                                <div
                                                    v-for="org in user.boranga_organisations"
                                                    :key="org.id"
                                                >
                                                    <div class="form-group">
                                                        <label
                                                            for=""
                                                            class="col-sm-2 control-label"
                                                            >Organisation</label
                                                        >
                                                        <div class="col-sm-3">
                                                            <input
                                                                v-model="
                                                                    org.name
                                                                "
                                                                type="text"
                                                                disabled
                                                                class="form-control"
                                                                name="organisation"
                                                                placeholder=""
                                                            />
                                                        </div>
                                                        <label
                                                            for=""
                                                            class="col-sm-2 control-label"
                                                            >ABN/ACN</label
                                                        >
                                                        <div class="col-sm-3">
                                                            <input
                                                                v-model="
                                                                    org.abn
                                                                "
                                                                type="text"
                                                                disabled
                                                                class="form-control"
                                                                name="organisation"
                                                                placeholder=""
                                                            />
                                                        </div>
                                                        <a
                                                            style="
                                                                cursor: pointer;
                                                                text-decoration: none;
                                                            "
                                                            @click.prevent="
                                                                unlinkUser(org)
                                                            "
                                                            ><i
                                                                class="bi bi-scissors"
                                                                style="
                                                                    font-size: 2em;
                                                                "
                                                            ></i
                                                            >&nbsp;Unlink</a
                                                        >
                                                    </div>
                                                </div>
                                                <div
                                                    v-for="orgReq in orgRequest_pending"
                                                    :key="orgReq.id"
                                                >
                                                    <div class="form-group">
                                                        <label
                                                            for=""
                                                            class="col-sm-2 control-label"
                                                            >Organisation</label
                                                        >
                                                        <div class="col-sm-3">
                                                            <input
                                                                v-model="
                                                                    orgReq.name
                                                                "
                                                                type="text"
                                                                disabled
                                                                class="form-control"
                                                                name="organisation"
                                                                placeholder=""
                                                            />
                                                        </div>
                                                        <label
                                                            for=""
                                                            class="col-sm-2 control-label"
                                                            >ABN/ACN</label
                                                        >
                                                        <div class="col-sm-3">
                                                            <input
                                                                v-model="
                                                                    orgReq.abn
                                                                "
                                                                type="text"
                                                                disabled
                                                                class="form-control"
                                                                name="organisation"
                                                                placeholder=""
                                                            />
                                                        </div>
                                                        <label
                                                            >Pending for
                                                            Approval (#{{
                                                                orgReq.id
                                                            }})</label
                                                        >
                                                    </div>
                                                </div>
                                            </div>
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>
</template>

<script>
import { v4 as uuid } from 'uuid';
import { api_endpoints, helpers } from '@/utils/hooks';
import CommsLogs from '@common-utils/comms_logs.vue';
import utils from '../utils';

export default {
    name: 'ManageUser',
    components: {
        CommsLogs,
    },
    beforeRouteEnter: function (to, from, next) {
        let initialisers = [
            utils.fetchCountries(),
            utils.fetchUser(to.params.user_id),
            utils.fetchOrgRequestPending(to.params.user_id),
        ];
        Promise.all(initialisers).then((data) => {
            next((vm) => {
                vm.countries = data[0];
                vm.user = data[1];
                vm.user.residential_address =
                    vm.user.residential_address != null
                        ? vm.user.residential_address
                        : {};
                vm.orgRequest_pending = data[2];
            });
        });
    },
    beforeRouteUpdate: function (to, from, next) {
        let initialisers = [utils.fetchUser(to.params.user_id)];
        Promise.all(initialisers).then((data) => {
            next((vm) => {
                vm.user = data[0];
                vm.user.residential_address =
                    vm.user.residential_address != null
                        ? vm.user.residential_address
                        : {};
            });
        });
    },
    data() {
        let vm = this;
        return {
            adBody: 'adBody' + uuid(),
            pdBody: 'pdBody' + uuid(),
            cdBody: 'cdBody' + uuid(),
            odBody: 'odBody' + uuid(),
            idBody: 'idBody' + uuid(),
            dTab: 'dTab' + uuid(),
            user: {
                residential_address: {},
                borangacompliance_organisations: [],
            },
            loading: [],
            countries: [],
            updatingPersonal: false,
            updatingAddress: false,
            updatingContact: false,
            uploadingID: false,
            uploadedID: null,
            empty_list: '/api/empty_list',
            logsTable: null,
            DATE_TIME_FORMAT: 'DD/MM/YYYY HH:mm:ss',
            activate_tables: false,
            comms_url: helpers.add_endpoint_json(
                api_endpoints.users,
                vm.$route.params.user_id + '/comms_log'
            ),
            logs_url: helpers.add_endpoint_json(
                api_endpoints.users,
                vm.$route.params.user_id + '/action_log'
            ),
            comms_add_url: helpers.add_endpoint_json(
                api_endpoints.users,
                vm.$route.params.user_id + '/add_comms_log'
            ),
            proposals_url:
                api_endpoints.proposals_paginated_external +
                '&submitter_id=' +
                vm.$route.params.user_id,
            approvals_url:
                api_endpoints.approvals_paginated_external +
                '&submitter_id=' +
                vm.$route.params.user_id,
            compliance_url:
                api_endpoints.compliances_paginated_external +
                '&submitter_id=' +
                vm.$route.params.user_id,
            orgRequest_pending: [],
        };
    },
    computed: {
        isLoading: function () {
            return this.loading.length == 0;
        },
        uploadedIDFileName: function () {
            return this.uploadedID != null ? this.uploadedID.name : '';
        },
    },
    mounted: function () {
        this.personal_form = document.forms.personal_form;
    },
    methods: {
        updatePersonal: function () {
            let vm = this;
            vm.updatingPersonal = true;
            if (vm.user.residential_address == null) {
                vm.user.residential_address = {};
            }
            if (vm.user.first_name == '' || vm.user.last_name == '') {
                let error_msg = 'Please ensure all fields are filled in.';
                swal({
                    title: 'Update Personal Details',
                    html:
                        'There was an error updating the user personal details.<br/>' +
                        error_msg,
                    type: 'error',
                }).then(() => {
                    vm.updatingPersonal = false;
                });
                return;
            }
            fetch(
                helpers.add_endpoint_json(
                    api_endpoints.users,
                    vm.user.id + '/update_personal'
                ),
                {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify(vm.user),
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
                    swal({
                        title: 'Update Personal Details',
                        html: 'User personal details has been successfully updated.',
                        type: 'success',
                    }).then(() => {
                        vm.updatingPersonal = false;
                    });
                },
                (error) => {
                    vm.updatingPersonal = false;
                    let error_msg = '<br/>';
                    for (var key in error.body) {
                        if (key === 'dob') {
                            error_msg += 'dob: Please enter a valid date.<br/>';
                        } else {
                            error_msg += key + ': ' + error.body[key] + '<br/>';
                        }
                    }
                    swal({
                        title: 'Update Personal Details',
                        html:
                            'There was an error updating the user personal details.<br/>' +
                            error_msg,
                        type: 'error',
                    });
                }
            );
        },
        updateContact: function () {
            let vm = this;
            vm.updatingContact = true;
            fetch(
                helpers.add_endpoint_json(
                    api_endpoints.users,
                    vm.user.id + '/update_contact'
                ),
                {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify(vm.user),
                }
            ).then(
                async (response) => {
                    vm.updatingContact = false;
                    vm.user = await response.json();
                    if (vm.user.residential_address == null) {
                        vm.user.residential_address = {};
                    }
                    swal({
                        title: 'Update Contact Details',
                        html: 'User contact details has been successfully updated.',
                        type: 'success',
                    });
                },
                (error) => {
                    vm.updatingContact = false;
                    let error_msg = '<br/>';
                    for (var key in error.body) {
                        error_msg += key + ': ' + error.body[key] + '<br/>';
                    }
                    swal({
                        title: 'Update Contact Details',
                        html:
                            'There was an error updating the user contact details.<br/>' +
                            error_msg,
                        type: 'error',
                    });
                }
            );
        },
        updateAddress: function () {
            let vm = this;
            vm.updatingAddress = true;
            fetch(
                helpers.add_endpoint_json(
                    api_endpoints.users,
                    vm.user.id + '/update_address'
                ),
                {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify(vm.user),
                }
            ).then(
                async (response) => {
                    vm.updatingAddress = false;
                    vm.user = await response.json();
                    if (vm.user.residential_address == null) {
                        vm.user.residential_address = {};
                    }
                    swal({
                        title: 'Update Address Details',
                        html: 'User address details has been successfully updated.',
                        type: 'success',
                    });
                },
                (error) => {
                    vm.updatingAddress = false;
                    let error_msg = '<br/>';
                    for (var key in error.body) {
                        error_msg += key + ': ' + error.body[key] + '<br/>';
                    }
                    swal({
                        title: 'Update Address Details',
                        html:
                            'There was an error updating the user address details.<br/>' +
                            error_msg,
                        type: 'error',
                    });
                }
            );
        },
        unlinkUser: function (org) {
            let vm = this;
            let org_name = org.name;
            swal({
                title: 'Unlink From Organisation',
                text:
                    'Are you sure you want to unlink this user from ' +
                    org.name +
                    ' ?',
                type: 'question',
                showCancelButton: true,
                confirmButtonText: 'Accept',
            }).then((result) => {
                if (result.value) {
                    fetch(
                        helpers.add_endpoint_json(
                            api_endpoints.organisations,
                            org.id + '/unlink_user'
                        ),
                        {
                            method: 'POST',
                            headers: {
                                'Content-Type': 'application/json',
                            },
                            body: JSON.stringify({ user_id: vm.user.id }),
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
                            fetch(
                                helpers.add_endpoint_json(
                                    api_endpoints.users,
                                    vm.user.id
                                )
                            ).then(async (response) => {
                                vm.user = await response.json();
                                if (vm.user.residential_address == null) {
                                    vm.user.residential_address = {};
                                }
                                if (
                                    vm.user.borangacompliance_organisations &&
                                    vm.user.borangacompliance_organisations
                                        .length > 0
                                ) {
                                    vm.managesOrg = 'Yes';
                                }
                                swal(
                                    'Unlink',
                                    'The user has been successfully unlinked from ' +
                                        org_name +
                                        '.',
                                    'success'
                                );
                            });
                        },
                        () => {
                            swal(
                                'Unlink',
                                'There was an error unlinking the user from ' +
                                    org_name +
                                    '.',
                                'error'
                            );
                        }
                    );
                }
            });
        },
        readFileID: function () {
            let vm = this;
            let _file = null;
            var input = $(vm.$refs.uploadedID)[0];
            if (input.files && input.files[0]) {
                var reader = new FileReader();
                reader.readAsDataURL(input.files[0]);
                reader.onload = function (e) {
                    _file = e.target.result;
                };
                _file = input.files[0];
            }
            vm.uploadedID = _file;
        },
        uploadID: function () {
            let vm = this;
            console.log('uploading id');
            vm.uploadingID = true;
            let data = new FormData();
            data.append('identification', vm.uploadedID);
            console.log(data);
            if (vm.uploadedID == null) {
                vm.uploadingID = false;
                swal({
                    title: 'Upload ID',
                    html: 'Please select a file to upload.',
                    type: 'error',
                });
            } else {
                fetch(
                    helpers.add_endpoint_json(
                        api_endpoints.users,
                        vm.user.id + '/upload_id'
                    ),
                    {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json',
                        },
                        body: data,
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
                        vm.uploadingID = false;
                        vm.uploadedID = null;
                        swal({
                            title: 'Upload ID',
                            html: 'The user ID has been successfully uploaded.',
                            type: 'success',
                        }).then(() => {
                            window.location.reload(true);
                        });
                    },
                    (error) => {
                        console.log(error);
                        vm.uploadingID = false;
                        let error_msg = '<br/>';
                        for (var key in error.body) {
                            error_msg += key + ': ' + error.body[key] + '<br/>';
                        }
                        swal({
                            title: 'Upload ID',
                            html:
                                'There was an error uploading the user ID.<br/>' +
                                error_msg,
                            type: 'error',
                        });
                    }
                );
            }
        },
    },
};
</script>
