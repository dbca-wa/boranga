<template>
    <div id="externalOCRDash" class="container">
        <FormSection
            :form-collapse="false"
            :profile="profile"
            label="Occurrence Report Forms"
            subtitle="- submit and view your reported occurrences"
            Index="applications"
        >
            <OccurrenceReportTable
                level="external"
                :url="occurrence_report_url"
            />
        </FormSection>
        <FormSection
            v-if="
                profile && profile.user && profile.user.ocr_referral_count > 0
            "
            :form-collapse="false"
            label="Occurrence Reports Forms Referred to Me"
            Index="ocr_referred_to_me"
        >
            <OccurrenceReportExternalReferralsDashboard
                level="external"
                :url="ocr_external_referrals_url"
            />
        </FormSection>
    </div>
</template>

<script>
import FormSection from '@/components/forms/section_toggle.vue';
import OccurrenceReportTable from './occurrence_report_table.vue';
import OccurrenceReportExternalReferralsDashboard from '@common-utils/ocr_external_referrals_dashboard.vue';

import { api_endpoints } from '@/utils/hooks';

export default {
    name: 'ExternalOccurrenceReportDashboard',
    components: {
        FormSection,
        OccurrenceReportTable,
        OccurrenceReportExternalReferralsDashboard,
    },
    data() {
        return {
            profile: null,
            occurrence_report_url:
                api_endpoints.occurrence_report_paginated_external,
            ocr_external_referrals_url:
                api_endpoints.occurrence_report_paginated_referred_to_me,
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
