<template>
    <div id="internal-occurrence-dash" class="container">
        <ul id="pills-tab" class="nav nav-pills" role="tablist">
            <li class="nav-item">
                <a
                    id="pills-flora-tab"
                    class="nav-link"
                    data-bs-toggle="pill"
                    href="#pills-flora"
                    role="tab"
                    aria-controls="pills-flora"
                    aria-selected="true"
                    @click="set_active_tab('flora')"
                    >Flora</a
                >
            </li>
            <li class="nav-item">
                <a
                    id="pills-fauna-tab"
                    class="nav-link"
                    data-bs-toggle="pill"
                    href="#pills-fauna"
                    role="tab"
                    aria-controls="pills-fauna"
                    aria-selected="false"
                    @click="set_active_tab('fauna')"
                    >Fauna</a
                >
            </li>
            <li class="nav-item">
                <a
                    id="pills-community-tab"
                    class="nav-link"
                    data-bs-toggle="pill"
                    href="#pills-community"
                    role="tab"
                    aria-controls="pills-community"
                    aria-selected="false"
                    @click="set_active_tab('community')"
                    >Communities</a
                >
            </li>
        </ul>
        <div id="pills-tabContent" class="tab-content">
            <div
                id="pills-flora"
                class="tab-pane"
                role="tabpanel"
                aria-labelledby="pills-flora-tab"
            >
                <FormSection
                    v-if="show_occurrences"
                    :form-collapse="getSectionCollapsed('occurrence-flora')"
                    label="Occurrences - Flora"
                    Index="occurrence-flora"
                    @toggle="onSectionToggle($event, 'occurrence-flora')"
                    @opened="
                        reloadDatatable(
                            'occ_flora_table',
                            'flora_occ_datatable'
                        )
                    "
                >
                    <OccurrenceFloraDashboard
                        v-if="isFlora"
                        ref="occ_flora_table"
                        level="internal"
                        :group_type_name="group_name"
                        :group_type_id="getGroupId"
                        :url="species_occ_url"
                        :profile="profile"
                    />
                </FormSection>
                <FormSection
                    v-if="show_occurrence_reports"
                    :form-collapse="
                        getSectionCollapsed('occurrence-report-flora')
                    "
                    label="Occurrence Report Forms - Flora"
                    Index="occurrence-report-flora"
                    @toggle="onSectionToggle($event, 'occurrence-report-flora')"
                    @opened="
                        reloadDatatable('flora_table', 'flora_ocr_datatable')
                    "
                >
                    <OccurrenceReportFloraDashTable
                        v-if="isFlora"
                        ref="flora_table"
                        level="internal"
                        :group_type_name="group_name"
                        :group_type_id="getGroupId"
                        :url="species_ocr_url"
                        :profile="profile"
                    />
                </FormSection>
                <FormSection
                    v-if="
                        profile &&
                        profile.user &&
                        profile.user.ocr_referral_count > 0
                    "
                    :form-collapse="
                        getSectionCollapsed(
                            'occurrence-report-flora-referred-to-me'
                        )
                    "
                    label="Occurrence Report Forms - Flora Referred to Me"
                    Index="occurrence-report-flora-referred-to-me"
                    @toggle="
                        onSectionToggle(
                            $event,
                            'occurrence-report-flora-referred-to-me'
                        )
                    "
                    @opened="
                        reloadDatatable(
                            'flora_referrals_table',
                            'flora_ocr_referrals_datatable'
                        )
                    "
                >
                    <OccurrenceReportReferredToMeDashTable
                        v-if="isFlora"
                        ref="flora_referrals_table"
                        level="internal"
                        :group_type_name="group_name"
                        :group_type_id="getGroupId"
                        :url="species_ocr_referrals_url"
                        :profile="profile"
                        filterOCRReferralsOccurrence_cache="filterOCRFloraReferralsOccurrence"
                        filterOCRReferralsScientificName_cache="filterOCRFloraReferralsScientificName"
                        filterOCRReferralsName_cache="filterOCRFloraReferralsName"
                        filterOCRReferralsStatus_cache="filterOCRFloraReferralsStatus"
                    />
                </FormSection>
            </div>
            <div
                id="pills-fauna"
                class="tab-pane"
                role="tabpanel"
                aria-labelledby="pills-fauna-tab"
            >
                <FormSection
                    v-if="show_occurrences"
                    :form-collapse="getSectionCollapsed('occurrence-fauna')"
                    label="Occurrences - Fauna"
                    Index="occurrence-fauna"
                    @toggle="onSectionToggle($event, 'occurrence-fauna')"
                    @opened="
                        reloadDatatable(
                            'occ_fauna_table',
                            'fauna_occ_datatable'
                        )
                    "
                >
                    <OccurrenceFaunaDashboard
                        v-if="isFauna"
                        ref="occ_fauna_table"
                        level="internal"
                        :group_type_name="group_name"
                        :group_type_id="getGroupId"
                        :url="species_occ_url"
                        :profile="profile"
                    />
                </FormSection>
                <FormSection
                    v-if="show_occurrence_reports"
                    :form-collapse="
                        getSectionCollapsed('occurrence-report-fauna')
                    "
                    label="Occurrence Report Forms - Fauna"
                    Index="fauna"
                    @toggle="onSectionToggle($event, 'occurrence-report-fauna')"
                    @opened="
                        reloadDatatable('fauna_table', 'fauna_ocr_datatable')
                    "
                >
                    <OccurrenceReportFaunaDashTable
                        v-if="isFauna"
                        ref="fauna_table"
                        level="internal"
                        :group_type_name="group_name"
                        :group_type_id="getGroupId"
                        :url="species_ocr_url"
                        :profile="profile"
                    />
                </FormSection>
                <FormSection
                    v-if="
                        profile &&
                        profile.user &&
                        profile.user.ocr_referral_count > 0
                    "
                    :form-collapse="
                        getSectionCollapsed(
                            'occurrence-report-fauna-referred-to-me'
                        )
                    "
                    label="Occurrence Report Forms - Fauna Referred to Me"
                    Index="occurrence-report-fauna-referred-to-me"
                    @toggle="
                        onSectionToggle(
                            $event,
                            'occurrence-report-fauna-referred-to-me'
                        )
                    "
                    @opened="
                        reloadDatatable(
                            'fauna_referrals_table',
                            'fauna_ocr_referrals_datatable'
                        )
                    "
                >
                    <OccurrenceReportReferredToMeDashTable
                        v-if="isFauna"
                        ref="fauna_referrals_table"
                        level="internal"
                        :group_type_name="group_name"
                        :group_type_id="getGroupId"
                        :url="species_ocr_referrals_url"
                        :profile="profile"
                        filterOCRReferralsOccurrence_cache="filterOCRCommunityReferralsOccurrence"
                        filterOCRReferralsScientificName_cache="filterOCRCommunityReferralsScientificName"
                        filterOCRReferralsName_cache="filterOCRCommunityReferralsName"
                        filterOCRReferralsStatus_cache="filterOCRCommunityReferralsStatus"
                    />
                </FormSection>
            </div>
            <div
                id="pills-community"
                class="tab-pane"
                role="tabpanel"
                aria-labelledby="pills-community-tab"
            >
                <FormSection
                    v-if="show_occurrences"
                    :form-collapse="getSectionCollapsed('occurrence-community')"
                    label="Occurrences - Community"
                    Index="occurrence-community"
                    @toggle="onSectionToggle($event, 'occurrence-community')"
                    @opened="
                        reloadDatatable(
                            'occ_community_table',
                            'community_occ_datatable'
                        )
                    "
                >
                    <OccurrenceCommunityDashboard
                        v-if="isCommunity"
                        ref="occ_community_table"
                        level="internal"
                        :group_type_name="group_name"
                        :group_type_id="getGroupId"
                        :url="community_occ_url"
                        :profile="profile"
                    />
                </FormSection>
                <FormSection
                    v-if="show_occurrence_reports"
                    :form-collapse="
                        getSectionCollapsed('occurrence-report-community')
                    "
                    label="Occurrence Report Forms - Community"
                    Index="community"
                    @toggle="
                        onSectionToggle($event, 'occurrence-report-community')
                    "
                    @opened="
                        reloadDatatable(
                            'community_table',
                            'community_ocr_datatable'
                        )
                    "
                >
                    <OccurrenceReportCommunityDashTable
                        v-if="isCommunity"
                        ref="community_table"
                        level="internal"
                        :group_type_name="group_name"
                        :group_type_id="getGroupId"
                        :url="community_ocr_url"
                        :profile="profile"
                    />
                </FormSection>
                <FormSection
                    v-if="
                        profile &&
                        profile.user &&
                        profile.user.ocr_referral_count > 0
                    "
                    :form-collapse="
                        getSectionCollapsed(
                            'occurrence-report-community-referred-to-me'
                        )
                    "
                    label="Occurrence Report Forms - Community Referred to Me"
                    Index="occurrence-report-community-referred-to-me"
                    @toggle="
                        onSectionToggle(
                            $event,
                            'occurrence-report-community-referred-to-me'
                        )
                    "
                    @opened="
                        reloadDatatable(
                            'community_referrals_table',
                            'community_ocr_referrals_datatable'
                        )
                    "
                >
                    <OccurrenceReportReferredToMeDashTable
                        v-if="isCommunity"
                        ref="community_referrals_table"
                        level="internal"
                        :group_type_name="group_name"
                        :group_type_id="getGroupId"
                        :url="species_ocr_referrals_url"
                        :profile="profile"
                        filterOCRReferralsOccurrence_cache="filterOCRFaunaReferralsOccurrence"
                        filterOCRReferralsScientificName_cache="filterOCRFaunaReferralsScientificName"
                        filterOCRReferralsName_cache="filterOCRFaunaReferralsName"
                        filterOCRReferralsStatus_cache="filterOCRFaunaReferralsStatus"
                    />
                </FormSection>
            </div>
        </div>
    </div>
</template>
<script>
import OccurrenceFloraDashboard from '@/components/common/occurrence_flora_dashboard.vue';
import OccurrenceFaunaDashboard from '@/components/common/occurrence_fauna_dashboard.vue';
import OccurrenceCommunityDashboard from '@/components/common/occurrence_community_dashboard.vue';

import OccurrenceReportFloraDashTable from '@/components/common/occurrence_report_flora_dashboard.vue';
import OccurrenceReportFaunaDashTable from '@common-utils/occurrence_report_fauna_dashboard.vue';
import OccurrenceReportCommunityDashTable from '@common-utils/occurrence_report_community_dashboard.vue';

import OccurrenceReportReferredToMeDashTable from '@common-utils/ocr_referrals_dashboard.vue';

import FormSection from '@/components/forms/section_toggle.vue';

import { api_endpoints, constants, helpers } from '@/utils/hooks';
export default {
    name: 'InternalOccurrenceDashboard',
    components: {
        OccurrenceFloraDashboard,
        OccurrenceFaunaDashboard,
        OccurrenceCommunityDashboard,
        OccurrenceReportFloraDashTable,
        OccurrenceReportFaunaDashTable,
        OccurrenceReportCommunityDashTable,
        OccurrenceReportReferredToMeDashTable,
        FormSection,
    },
    data() {
        return {
            group_types: [],
            group_name: null,
            species_occ_url: api_endpoints.occurrence_paginated_internal,
            community_occ_url: api_endpoints.occurrence_paginated_internal,
            species_ocr_url: api_endpoints.occurrence_report_paginated_internal,
            community_ocr_url:
                api_endpoints.occurrence_report_paginated_internal,
            species_ocr_referrals_url:
                api_endpoints.occurrence_report_paginated_referred_to_me,
            profile: null,
        };
    },
    computed: {
        show_occurrences: function () {
            return (
                this.profile?.user &&
                this.profile.user.groups.some((i) =>
                    [
                        constants.GROUPS.READ_ONLY_USERS,
                        constants.GROUPS.CONSERVATION_STATUS_ASSESSORS,
                        constants.GROUPS.CONSERVATION_STATUS_APPROVERS,
                        constants.GROUPS.OCCURRENCE_APPROVERS,
                        constants.GROUPS.OCCURRENCE_ASSESSORS,
                        constants.GROUPS.SPECIES_AND_COMMUNITIES_APPROVERS,
                    ].includes(i)
                )
            );
        },
        show_occurrence_reports: function () {
            return (
                this.profile?.user &&
                this.profile.user.groups.some((i) =>
                    [
                        constants.GROUPS.READ_ONLY_USERS,
                        constants.GROUPS.CONSERVATION_STATUS_ASSESSORS,
                        constants.GROUPS.CONSERVATION_STATUS_APPROVERS,
                        constants.GROUPS.OCCURRENCE_APPROVERS,
                        constants.GROUPS.OCCURRENCE_ASSESSORS,
                        constants.GROUPS.SPECIES_AND_COMMUNITIES_APPROVERS,
                        constants.GROUPS.INTERNAL_CONTRIBUTORS,
                    ].includes(i)
                )
            );
        },
        isFlora: function () {
            return this.group_name == 'flora';
        },
        isFauna: function () {
            return this.group_name == 'fauna';
        },
        isCommunity: function () {
            return this.group_name == 'community';
        },
        getGroupId: function () {
            for (var i = 0; i < this.group_types.length; i++) {
                if (this.group_name === this.group_types[i].name) {
                    return this.group_types[i].id;
                }
            }
            return null;
        },
    },
    created: function () {
        fetch(api_endpoints.group_types_dict).then(
            async (response) => {
                this.group_types = await response.json();
            },
            (error) => {
                console.log(error);
            }
        );
        this.fetchProfile();
    },
    mounted: function () {
        // eslint-disable-next-line no-undef
        chevron_toggle.init();
    },
    methods: {
        set_active_tab: function (group_name) {
            this.group_name = group_name;
            if (!this.profile?.user || !this.profile.user.area_of_interest) {
                localStorage.setItem('occurrenceActiveTab', group_name);
            }
            let elem = $('#pills-tab a[href="#pills-' + group_name + '"]');
            let tab = bootstrap.Tab.getInstance(elem);
            if (!tab) tab = new bootstrap.Tab(elem);
            tab.show();
        },
        fetchProfile: function () {
            let vm = this;
            fetch(api_endpoints.profile).then(
                async (response) => {
                    vm.profile = await response.json();
                    vm.$nextTick(() => {
                        if (
                            vm.profile?.user &&
                            vm.profile.user.area_of_interest
                        ) {
                            vm.set_active_tab(vm.profile.user.area_of_interest);
                            return;
                        }
                        let occurrenceActiveTab = localStorage.getItem(
                            'occurrenceActiveTab'
                        );
                        if (occurrenceActiveTab === null) {
                            vm.set_active_tab('flora');
                        } else {
                            vm.set_active_tab(occurrenceActiveTab);
                        }
                    });
                },
                (error) => {
                    console.log(error);
                }
            );
        },
        getSectionCollapsed(key) {
            // Returns true if collapsed, false if expanded
            const val = sessionStorage.getItem('formSectionCollapsed_' + key);
            return val === null ? true : val === 'true';
        },
        setSectionCollapsed(key, collapsed) {
            sessionStorage.setItem('formSectionCollapsed_' + key, collapsed);
        },
        onSectionToggle(event, key) {
            this.setSectionCollapsed(key, event);
        },
        reloadDatatable: function (dashRef, datatableRef) {
            this.$refs[dashRef].$refs[datatableRef].vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            );
        },
    },
};
</script>
