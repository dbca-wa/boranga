import { RouterView } from 'vue-router';

import User from '../users/manage.vue';
import SpeciesCommunitiesDash from '../species_communities/dashboard.vue';
import SpeciesCommunities from '../species_communities/species_communities.vue';
import ConservationStatusDash from '../conservation_status/dashboard.vue';
import ConservationStatus from '../conservation_status/conservation_status.vue';
import ConservationStatusReferral from '../conservation_status/referral.vue';
import MeetingsDash from '../meetings/dashboard.vue';
import Meeting from '../meetings/meeting.vue';
import OccurrenceDash from '../occurrence/dashboard.vue';
import Occurrence from '../occurrence/occurrence.vue';
import OccurrenceReport from '../occurrence/occurrence_report_form.vue';
import OccurrenceReportReferral from '../occurrence/referral.vue';
import BulkImport from '../occurrence/bulk_import.vue';
import BulkImportSchemaList from '../occurrence/bulk_import_schema_list.vue';
import BulkImportSchema from '../occurrence/bulk_import_schema.vue';
import QueueReports from '../reports/queue_reports.vue';

export default {
    path: '/internal',
    component: RouterView,
    children: [
        {
            path: '/',
            component: SpeciesCommunitiesDash,
        },
        {
            path: 'species-communities',
            component: SpeciesCommunitiesDash,
            name: 'internal-species-communities-dash',
        },
        {
            path: 'conservation-status',
            component: ConservationStatusDash,
            name: 'internal-conservation-status-dash',
        },
        {
            path: 'meetings',
            component: MeetingsDash,
            name: 'internal-meetings-dash',
        },
        {
            path: 'meetings/:meeting_id',
            component: Meeting,
            name: 'internal-meetings',
        },
        {
            path: 'occurrence',
            component: OccurrenceDash,
            name: 'internal-occurrence-dash',
        },
        {
            path: 'occurrence/:occurrence_id',
            component: Occurrence,
            name: 'internal-occurrence-detail',
        },
        {
            path: 'occurrence-report',
            component: RouterView,
            children: [
                {
                    path: 'bulk_import_schema/:bulk_import_schema_id',
                    name: 'occurrence-report-bulk-import-schema-details',
                    component: BulkImportSchema,
                },
                {
                    path: 'bulk_import_schema/',
                    name: 'occurrence-report-bulk-import-schema-list',
                    component: BulkImportSchemaList,
                },
                {
                    path: 'bulk_import/',
                    name: 'occurrence-report-bulk-import',
                    component: BulkImport,
                },
                {
                    path: ':occurrence_report_id',
                    component: RouterView,
                    children: [
                        {
                            path: '',
                            component: OccurrenceReport,
                            name: 'internal-occurrence-report-detail',
                        },
                        {
                            path: 'referral/:referral_id',
                            component: OccurrenceReportReferral,
                            name: 'internal-occurrence-report-referral',
                        },
                    ],
                },
            ],
        },
        {
            path: 'reports',
            component: QueueReports,
            name: 'internal-reports-dash',
        },
        {
            path: 'users',
            component: RouterView,
            children: [
                {
                    path: ':user_id',
                    component: User,
                    name: 'internal-user-detail',
                },
            ],
        },
        {
            path: 'species-communities',
            component: RouterView,
            children: [
                {
                    path: ':species_community_id',
                    component: SpeciesCommunities,
                    name: 'internal-species-communities',
                },
            ],
        },
        {
            path: 'conservation-status',
            component: RouterView,
            children: [
                {
                    path: ':conservation_status_id',
                    component: RouterView,
                    children: [
                        {
                            path: '',
                            component: ConservationStatus,
                            name: 'internal-conservation-status',
                        },
                        {
                            path: 'referral/:referral_id',
                            component: ConservationStatusReferral,
                            name: 'internal-conservation-status-referral',
                        },
                    ],
                },
            ],
        },
    ],
};
