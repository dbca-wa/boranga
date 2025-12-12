export default {
    PROPOSAL_STATUS: {
        DRAFT: { ID: 'draft', TEXT: 'Draft' },
        DISCARDED: { ID: 'discarded', TEXT: 'Discarded' },
        WITH_ASSESSOR: { ID: 'with_assessor', TEXT: 'With Assessor' },
        WITH_REFERRAL: { ID: 'with_referral', TEXT: 'With Referral' },
        DEFERRED: { ID: 'deferred', TEXT: 'Deferred' },
        PROPOSED_FOR_AGENDA: {
            ID: 'proposed_for_agenda',
            TEXT: 'Proposed For Agenda',
        },
        READY_FOR_AGENDA: { ID: 'ready_for_agenda', TEXT: 'Ready For Agenda' },
        ON_AGENDA: { ID: 'on_agenda', TEXT: 'On Agenda' },
        WITH_APPROVER: { ID: 'with_approver', TEXT: 'Proposed DeListed' },
        APPROVED: { ID: 'approved', TEXT: 'Approved' },
        DECLINED: { ID: 'declined', TEXT: 'Declined' },
        DELISTED: { ID: 'delisted', TEXT: 'DeListed' },
        CLOSED: { ID: 'closed', TEXT: 'Closed' },
        UNLOCKED: { ID: 'unlocked', TEXT: 'Unlocked' },
    },
    EFFECTIVE_FROM_STATUSES: [
        'Approved',
        'Proposed DeListed',
        'DeListed',
        'Closed',
    ],
    EFFECTIVE_TO_STATUSES: ['Proposed DeListed', 'DeListed', 'Closed'],
    EFFECTIVE_DATES_STATUSES: [
        'Approved',
        'Proposed DeListed',
        'DeListed',
        'Closed',
    ],
    GROUPS: {
        CONSERVATION_STATUS_ASSESSORS: 'Conservation Status Assessors',
        CONSERVATION_STATUS_APPROVERS: 'Conservation Status Approvers',
        DJANGO_ADMIN: 'Django Admin',
        EXTERNAL_CONTRIBUTORS: 'External Contributors',
        INTERNAL_CONTRIBUTORS: 'Internal Contributors',
        OCCURRENCE_APPROVERS: 'Occurrence Approvers',
        OCCURRENCE_ASSESSORS: 'Occurrence Assessors',
        READ_ONLY_USERS: 'Query Users',
        SPECIES_AND_COMMUNITIES_APPROVERS: 'Species and Communities Approvers',
    },
    DATATABLE_PROCESSING_HTML:
        '<div class="d-flex justify-content-center"><div class="d-flex spinner-border text-primary my-4" role="status"><span class="visually-hidden">Loading...</span></div></div>',
    MODELS: {
        OCCURRENCE_REPORT: {
            MODEL_PREFIX: 'ORF',
        },
    },
};
