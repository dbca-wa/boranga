<template>
    <div v-if="profile" id="internal-home" class="container pt-4">
        <h2 class="mb-4">
            Welcome back {{ profile ? profile.first_name : '' }}
        </h2>
        <div>
            <div class="row">
                <div class="col-4">
                    <div class="card">
                        <div class="card-body">
                            <h4 class="card-title mb-3">
                                My Profile<i
                                    class="bi bi-person-fill ps-2 text-primary"
                                ></i>
                            </h4>
                            <div class="table-responsive">
                                <table class="table table-sm">
                                    <tbody>
                                        <tr>
                                            <td scope="row" class="fw-bold">
                                                Name
                                            </td>
                                            <td>
                                                {{ profile.user.full_name }}
                                            </td>
                                        </tr>
                                        <tr>
                                            <td scope="row" class="fw-bold">
                                                Email
                                            </td>
                                            <td>{{ profile.user.email }}</td>
                                        </tr>
                                        <tr v-if="profile.user.phone_number">
                                            <td scope="row" class="fw-bold">
                                                Phone Number
                                            </td>
                                            <td>
                                                {{ profile.user.phone_number }}
                                            </td>
                                        </tr>
                                        <tr v-if="profile.user.mobile_number">
                                            <td scope="row" class="fw-bold">
                                                Mobile
                                            </td>
                                            <td>
                                                {{ profile.user.mobile_number }}
                                            </td>
                                        </tr>
                                    </tbody>
                                </table>
                                <a
                                    role="button"
                                    class="btn btn-primary btn-sm"
                                    href="/ledger-ui/accounts"
                                >
                                    Manage Account
                                    <i class="bi bi-person-gear"></i>
                                </a>
                            </div>
                        </div>
                    </div>
                </div>
                <div class="col-4">
                    <div class="card">
                        <div class="card-body">
                            <h4 class="card-title mb-3">
                                My Area of Interest<i
                                    class="bi bi-search-heart ps-2 text-primary"
                                ></i>
                            </h4>
                            <div class="mb-4">
                                <select
                                    id="area-of-interest"
                                    v-model="profile.user.area_of_interest"
                                    class="form-select text-muted"
                                    name="area-of-interest"
                                    placeholder="Name"
                                    @change="updateAreaOfInterest"
                                >
                                    <option :value="null">
                                        No Specific Area
                                    </option>
                                    <option value="flora">Flora</option>
                                    <option value="fauna">Fauna</option>
                                    <option value="community">
                                        Communities
                                    </option>
                                </select>
                            </div>
                            <div id="emailHelp" class="form-text">
                                <p>
                                    This setting controls which tab will
                                    automatically be opened when browsing the
                                    Species and Communities<template
                                        v-if="profile.user.is_internal"
                                        >, Conservation Status and Occurrences
                                    </template>
                                    page<template
                                        v-if="profile.user.is_internal"
                                        >s</template
                                    >.
                                </p>
                                <p>
                                    If you are planning to access more than one
                                    area regularly it is recommended that you
                                    leave this set as 'No Specific Area'
                                </p>
                            </div>
                        </div>
                    </div>
                </div>
                <div class="col-4">
                    <div class="card">
                        <div class="card-body">
                            <h4 class="card-title mb-3">
                                My Groups<i
                                    class="bi bi-people-fill ps-2 text-primary"
                                ></i>
                            </h4>
                            <p class="card-text text-muted">
                                You are a member of the following groups:
                            </p>
                            <span
                                v-for="group in profile.user.groups"
                                :key="group"
                                class="badge bg-primary me-2 p-2 mb-2"
                                >{{ group }}</span
                            >
                            <div v-if="profile.user.is_superuser" class="mt-3">
                                <span class="badge bg-success me-2 p-2 mb-2"
                                    >You are a Superuser</span
                                >
                            </div>
                            <div v-if="profile.user.is_superuser">
                                <small class="text-muted"
                                    >* Superusers are a member of every group by
                                    default</small
                                >
                            </div>
                        </div>
                    </div>
                </div>
            </div>
            <div v-if="homeCardLeft || homeCardRight" class="row mt-4">
                <div v-if="homeCardLeft" class="col-6">
                    <div class="card h-100">
                        <div class="card-body">
                            <div v-html="homeCardLeft.text"></div>
                            <a
                                v-if="homeCardLeft.user_can_administer"
                                :href="`/admin/boranga/helptextentry/${homeCardLeft.id}/change/`"
                                role="button"
                                class="ms-2"
                                target="_blank"
                                title="Edit help text"
                                ><i class="bi bi-pencil-square"></i
                            ></a>
                        </div>
                    </div>
                </div>
                <div v-if="homeCardRight" class="col-6">
                    <div class="card h-100">
                        <div class="card-body">
                            <div v-html="homeCardRight.text"></div>
                            <a
                                v-if="homeCardRight.user_can_administer"
                                :href="`/admin/boranga/helptextentry/${homeCardRight.id}/change/`"
                                role="button"
                                class="ms-2"
                                target="_blank"
                                title="Edit help text"
                                ><i class="bi bi-pencil-square"></i
                            ></a>
                        </div>
                    </div>
                </div>
            </div>
            <div
                class="row mt-4"
                v-if="outstanding_referrals && outstanding_referrals.length"
            >
                <div class="col-12">
                    <div class="card">
                        <div class="card-body">
                            <h4 class="card-title">
                                My Referrals<i
                                    class="bi bi-exclamation-circle-fill text-warning ps-2"
                                ></i>
                            </h4>
                            <div class="border-bottom mb-3"></div>
                            <table class="table table-sm">
                                <thead>
                                    <tr>
                                        <th scope="col">Number</th>
                                        <th scope="col">Type</th>
                                        <th scope="col">Name</th>
                                        <th scope="col">Status</th>
                                        <th scope="col">
                                            First Referred to You
                                        </th>
                                        <th scope="col">Action</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    <tr
                                        v-for="outstanding_referral in outstanding_referrals"
                                        :key="outstanding_referral.id"
                                    >
                                        <td>
                                            {{ outstanding_referral.number }}
                                        </td>
                                        <td class="text-capitalize">
                                            {{
                                                outstanding_referral.referral_type
                                            }}
                                            {{ outstanding_referral.type }}
                                        </td>
                                        <td>{{ outstanding_referral.name }}</td>
                                        <td>
                                            <span class="badge bg-warning"
                                                ><i
                                                    class="bi bi-clock-fill me-2"
                                                ></i
                                                >{{
                                                    outstanding_referral.processing_status
                                                }}</span
                                            >
                                        </td>
                                        <td>
                                            <TimeSince
                                                :date="
                                                    outstanding_referral.lodged_on
                                                "
                                            ></TimeSince>
                                        </td>
                                        <td>
                                            <a :href="outstanding_referral.link"
                                                >Process</a
                                            >
                                        </td>
                                    </tr>
                                </tbody>
                            </table>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>
    <div v-else class="text-center mt-5">
        <div class="spinner-border text-primary" role="status">
            <span class="visually-hidden">Loading...</span>
        </div>
    </div>
</template>

<script>
import { api_endpoints } from '@/utils/hooks';
import TimeSince from '@/components/common/TimeSince.vue';

export default {
    name: 'HomeComponent',
    data: function () {
        return {
            profile: null,
            outstanding_referrals: null,
            homeCardLeft: null,
            homeCardRight: null,
        };
    },
    components: {
        TimeSince,
    },
    computed: {
        lastLogin: function () {
            if (!this.profile?.user) {
                return '';
            }
            let localDate = new Date(this.profile.user.last_login);
            return `${localDate.toLocaleDateString()} at ${localDate.toLocaleTimeString()}`;
        },
    },
    created: function () {
        // env variable is set in base.html template from django
        var user_is_authenticated = `${env['user_is_authenticated']}`;
        if (user_is_authenticated.toString() === 'true') {
            this.fetchProfile();
        }
        this.fetchOutstandingReferrals();
        this.fetchHomeCards();
    },
    methods: {
        updateAreaOfInterest: function () {
            let vm = this;
            fetch(api_endpoints.save_area_of_interest, {
                method: 'PATCH',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    area_of_interest: vm.profile.user.area_of_interest,
                }),
            }).then(
                async (response) => {
                    const json = await response.json();
                    vm.profile.user.area_of_interest = json.area_of_interest;
                },
                (error) => {
                    console.log(error);
                }
            );
            // Reset default tabs for each page
            localStorage.removeItem('speciesCommunitiesActiveTab');
            localStorage.removeItem('conservationStatusActiveTab');
            localStorage.removeItem('occurrenceActiveTab');
        },
        fetchProfile: function () {
            let vm = this;
            fetch(api_endpoints.profile, {
                method: 'GET',
                headers: {
                    'Content-Type': 'application/json',
                },
            }).then(
                async (response) => {
                    vm.profile = await response.json();
                },
                (error) => {
                    console.log(error);
                }
            );
        },
        fetchHomeCards: function () {
            let vm = this;
            const sectionIds = [
                'internal_home_card_left',
                'internal_home_card_right',
            ];
            sectionIds.forEach((sectionId, index) => {
                fetch(`${api_endpoints.help_text_entries}/${sectionId}/`).then(
                    async (response) => {
                        if (!response.ok) return;
                        const data = await response.json();
                        if (index === 0) {
                            vm.homeCardLeft = data;
                        } else {
                            vm.homeCardRight = data;
                        }
                    }
                );
            });
        },
        fetchOutstandingReferrals: function () {
            let vm = this;
            fetch(api_endpoints.outstanding_referrals, {
                method: 'GET',
                headers: {
                    'Content-Type': 'application/json',
                },
            }).then(
                async (response) => {
                    vm.outstanding_referrals = await response.json();
                },
                (error) => {
                    console.log(error);
                }
            );
        },
    },
};
</script>
