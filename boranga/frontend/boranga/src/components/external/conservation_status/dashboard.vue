<template>
    <div id="externalCSDash" class="container">
        <FormSection
            :form-collapse="false"
            label="Conservation Status"
            subtitle="- submit and view your proposed conservation status listings"
            Index="applications"
        >
            <ConservationStatusTable
                level="external"
                :url="conservation_status_url"
                :profile="profile"
            />
        </FormSection>
        <FormSection
            v-if="profile && profile.user && profile.user.cs_referral_count > 0"
            :form-collapse="false"
            label="Conservation Status Proposals Referred to Me"
            Index="cs_referred_to_me"
        >
            <ConservationStatusExternalReferralsDashboard
                level="external"
                :url="cs_external_referrals_url"
            />
        </FormSection>
    </div>
</template>

<script>
import FormSection from '@/components/forms/section_toggle.vue';
import ConservationStatusTable from './conservation_status_proposal_table.vue';
import ConservationStatusExternalReferralsDashboard from '@common-utils/cs_external_referrals_dashboard.vue';
import { api_endpoints } from '@/utils/hooks';

export default {
    name: 'ExternalConservationStatusDashboard',
    components: {
        FormSection,
        ConservationStatusTable,
        ConservationStatusExternalReferralsDashboard,
    },
    data() {
        return {
            profile: null,
            conservation_status_url:
                api_endpoints.conservation_status_paginated_external,
            cs_external_referrals_url:
                api_endpoints.conservation_status_referred_to_me,
        };
    },
    computed: {
        is_external: function () {
            return this.level == 'external';
        },
    },
    created: function () {
        this.fetchProfile();
    },
    methods: {
        fetchProfile() {
            fetch(api_endpoints.profile)
                .then(async (response) => {
                    this.profile = await response.json();
                })
                .catch((error) => {
                    console.log(error);
                });
        },
    },
};
</script>
