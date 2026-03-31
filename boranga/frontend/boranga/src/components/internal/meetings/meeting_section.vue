<template lang="html">
    <div id="communityStatus">
        <FormSection :form-collapse="false" label="Meeting" Index="meeting">
            <alert v-if="!isMeetingDateValid" type="danger"
                ><strong
                    >Please select End Date that is later than Start
                    Date</strong
                ></alert
            >
            <div class="row mb-3">
                <label for="" class="col-sm-4 control-label fw-bold"
                    >Title: <span class="text-danger">*</span></label
                >
                <div class="col-sm-8">
                    <textarea
                        id="title"
                        ref="title"
                        v-model="meeting_obj.title"
                        :disabled="isReadOnly"
                        class="form-control"
                        placeholder=""
                        autofocus
                    ></textarea>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-4 control-label fw-bold"
                    >Start Date/ Time: <span class="text-danger">*</span></label
                >
                <div class="col-sm-8">
                    <input
                        ref="start_date"
                        v-model="start_date"
                        :disabled="isReadOnly"
                        type="datetime-local"
                        class="form-control"
                        name="start_date"
                        @change="validateMeetingDate()"
                    />
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-4 control-label fw-bold"
                    >End Date/ Time: <span class="text-danger">*</span></label
                >
                <div class="col-sm-8">
                    <input
                        id="end_date"
                        v-model="end_date"
                        :disabled="isReadOnly"
                        type="datetime-local"
                        class="form-control"
                        @change="validateMeetingDate()"
                    />
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-4 control-label fw-bold"
                    >Meeting Type: <span class="text-danger">*</span></label
                >
                <div class="col-sm-8">
                    <select
                        v-model="meeting_obj.meeting_type"
                        :disabled="isReadOnly"
                        style="width: 100%"
                        class="form-select"
                        @change="toggleAttendees(meeting_obj.meeting_type)"
                    >
                        <option
                            v-for="option in meeting_type_list"
                            :key="option.id"
                            :value="option.id"
                        >
                            {{ option.display_name }}
                        </option>
                    </select>
                </div>
            </div>
            <div v-show="!isCommitteeMeeting" class="row mb-3">
                <label for="" class="col-sm-4 control-label fw-bold"
                    >Attendees: <span class="text-danger">*</span></label
                >
                <div class="col-sm-8">
                    <textarea
                        id="title"
                        v-model="meeting_obj.attendees"
                        :disabled="isReadOnly"
                        class="form-control"
                        placeholder=""
                    ></textarea>
                </div>
            </div>
            <div v-show="isCommitteeMeeting">
                <div class="row mb-3">
                    <label for="" class="col-sm-4 control-label fw-bold"
                        >Committee: <span class="text-danger">*</span></label
                    >
                    <div class="col-sm-8">
                        <template v-if="!isReadOnly">
                            <template
                                v-if="
                                    committee_list &&
                                    committee_list.length > 0 &&
                                    meeting_obj.committee_id &&
                                    !committee_list
                                        .map((d) => d.id)
                                        .includes(meeting_obj.committee_id)
                                "
                            >
                                <input
                                    v-if="meeting_obj.committee"
                                    type="text"
                                    class="form-control mb-3"
                                    :value="
                                        meeting_obj.committee +
                                        ' (Now Archived)'
                                    "
                                    disabled
                                />
                                <div class="mb-3 text-muted">
                                    Change committee to:
                                </div>
                            </template>
                            <select
                                v-model="meeting_obj.committee_id"
                                class="form-select"
                                @change="committeeChanged"
                            >
                                <option
                                    v-for="committee in committee_list"
                                    :key="committee.id"
                                    :value="committee.id"
                                >
                                    {{ committee.name }}
                                </option>
                            </select>
                        </template>
                        <template v-else>
                            <input
                                v-model="meeting_obj.committee"
                                class="form-control"
                                type="text"
                                :disabled="isReadOnly"
                            />
                        </template>
                    </div>
                </div>
                <div
                    v-if="meeting_obj.committee_id && committee_members"
                    class="row mb-3"
                >
                    <label for="" class="col-sm-4 control-label fw-bold"
                        >Members
                        {{
                            meeting_obj.processing_status == 'completed'
                                ? 'Who Attended'
                                : 'Attending'
                        }}: <span class="text-danger">*</span></label
                    >
                    <div class="col-sm-8">
                        <template v-if="committee_members.length > 0">
                            <div
                                v-if="!isReadOnly"
                                class="border-bottom pb-2 mb-2"
                            >
                                <input
                                    id="all-members-attending"
                                    v-model="all_members_attending"
                                    class="form-check-input me-2"
                                    type="checkbox"
                                    :value="true"
                                    @click="toggleAllMembersAttending"
                                />
                                <label
                                    class="form-check-label"
                                    for="all-members-attending"
                                    ><i
                                        class="bi bi-people-fill h5"
                                        :class="
                                            all_members_attending
                                                ? 'text-primary'
                                                : 'text-secondary'
                                        "
                                    ></i>
                                    All Members are Attending</label
                                >
                            </div>
                            <div
                                v-for="committee_member in committee_members"
                                class="fs-6 mb-2"
                                :key="committee_member.id"
                            >
                                <input
                                    :id="'member-' + committee_member.id"
                                    v-model="
                                        meeting_obj.selected_committee_members
                                    "
                                    class="form-check-input me-2"
                                    type="checkbox"
                                    :value="committee_member.id"
                                    :disabled="
                                        isReadOnly || committee_member.archived
                                    "
                                    @change="updateAllMembersAttending"
                                />
                                <label
                                    class="form-check-label"
                                    :class="
                                        meeting_obj.selected_committee_members.includes(
                                            committee_member.id
                                        )
                                            ? ''
                                            : 'text-secondary'
                                    "
                                    :for="'member-' + committee_member.id"
                                    ><i
                                        class="bi h5"
                                        :class="
                                            meeting_obj.selected_committee_members.includes(
                                                committee_member.id
                                            )
                                                ? 'bi-person-fill-check text-success'
                                                : 'bi-person-fill-dash text-secondary'
                                        "
                                    ></i>
                                    {{ committee_member.first_name }}
                                    {{ committee_member.last_name }}
                                    <template v-if="committee_member.archived">
                                        (Now Archived)</template
                                    ></label
                                >
                                <template
                                    v-if="
                                        !isReadOnly &&
                                        committee_member.archived &&
                                        meeting_obj.selected_committee_members.includes(
                                            committee_member.id
                                        )
                                    "
                                    ><a
                                        class="ps-2"
                                        role="button"
                                        @click.prevent="
                                            removeArchivedCommitteeMember(
                                                committee_member.id
                                            )
                                        "
                                        >Remove from meeting</a
                                    ></template
                                >
                            </div>
                        </template>
                        <template v-else>
                            <div class="text-muted">
                                No members found for this committee
                            </div>
                        </template>
                    </div>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-4 control-label fw-bold"
                    >Location: <span class="text-danger">*</span></label
                >
                <div class="col-sm-8">
                    <select
                        v-if="!isReadOnly"
                        v-model="meeting_obj.location_id"
                        class="form-select"
                    >
                        <option
                            v-for="option in location_list"
                            :key="option.id"
                            :value="option.id"
                        >
                            {{ option.name }}
                        </option>
                    </select>
                    <input
                        v-else
                        id="location"
                        v-model="meeting_obj.location"
                        type="text"
                        readonly
                        class="form-control"
                        placeholder=""
                    />
                </div>
            </div>
        </FormSection>
    </div>
</template>

<script>
import alert from '@vue-utils/alert.vue';
import FormSection from '@/components/forms/section_toggle.vue';

import { api_endpoints } from '@/utils/hooks';

const calculateDefaultDate = () => {
    const now = new Date();
    now.setHours(9, 0, 0, 0); // Set time to 9:00 AM

    const year = now.getFullYear();
    const month = String(now.getMonth() + 1).padStart(2, '0'); // Months are zero-indexed
    const day = String(now.getDate()).padStart(2, '0');
    const hours = String(now.getHours()).padStart(2, '0');
    const minutes = String(now.getMinutes()).padStart(2, '0');

    return `${year}-${month}-${day}T${hours}:${minutes}`;
};
const calculateDefaultEndDate = () => {
    const now = new Date();
    now.setHours(17, 0, 0, 0); // Set time to 9:00 AM

    const year = now.getFullYear();
    const month = String(now.getMonth() + 1).padStart(2, '0'); // Months are zero-indexed
    const day = String(now.getDate()).padStart(2, '0');
    const hours = String(now.getHours()).padStart(2, '0');
    const minutes = String(now.getMinutes()).padStart(2, '0');

    return `${year}-${month}-${day}T${hours}:${minutes}`;
};
export default {
    name: 'MeetingSection',
    components: {
        FormSection,
        alert,
    },
    props: {
        meeting_obj: {
            type: Object,
            required: true,
        },
        is_external: {
            type: Boolean,
            default: false,
        },
        userCanEdit: {
            type: Boolean,
            default: true,
        },
    },
    data: function () {
        return {
            datepickerOptions: {
                format: 'DD/MM/YYYY',
                showClear: true,
                useCurrent: false,
                keepInvalid: true,
                allowInputToggle: true,
            },
            start_date: calculateDefaultDate(),
            end_date: calculateDefaultEndDate(),
            isMeetingDateValid: true,
            isCommitteeMeeting: false,
            meetingStatusEditable: true,

            //----list of values dictionary
            meeting_dict: {},
            location_list: [],
            meeting_type_list: [],
            status_list: [],
            committee_list: [],
            committee_members: [],
            all_members_attending: false,
        };
    },
    computed: {
        isStatusDraft: function () {
            return this.meeting_obj.processing_status == 'draft' ? true : false;
        },
        isReadOnly: function () {
            return !this.userCanEdit;
        },
        activeMembers: function () {
            return this.committee_members.filter((member) => !member.archived);
        },
    },
    watch: {
        start_date: function (newVal) {
            let vm = this;
            console.log(newVal, vm.meeting_obj.start_date);
            var selectedValue = newVal;
            if (selectedValue === '') {
                vm.meeting_obj.start_date = null;
            } else {
                vm.meeting_obj.start_date = vm.start_date;
                vm.end_date = vm.start_date;
            }
        },
        end_date: function (newVal) {
            let vm = this;
            var selectedValue = newVal;
            if (selectedValue === '') {
                vm.meeting_obj.end_date = null;
            } else {
                vm.meeting_obj.end_date = vm.end_date;
            }
        },
    },
    created: async function () {
        let vm = this;
        //------fetch list of values
        fetch(api_endpoints.meeting_dict).then(
            async (response) => {
                vm.meeting_dict = await response.json();
                if (vm.meeting_obj.processing_status == 'completed') {
                    vm.meetingStatusEditable = false;
                }
                //--meeting room list
                vm.location_list = vm.meeting_dict.location_list;
                vm.location_list.splice(0, 0, {
                    id: null,
                    name: 'Select the Location for the Meeting',
                });
                vm.committee_list = vm.meeting_dict.committee_list;
                vm.committee_list.splice(0, 0, {
                    id: null,
                    name: 'Select the Committee for the Meeting',
                });
                //--meeting type list
                vm.meeting_type_list = vm.meeting_dict.meeting_type_list;
                //--status choices list
                vm.status_list = vm.meeting_dict.status_list;
                vm.toggleAttendees(vm.meeting_obj.meeting_type);
            },
            (error) => {
                console.log(error);
            }
        );
    },
    mounted: function () {
        let vm = this;
        vm.setMeetingDates();
    },
    methods: {
        toggleAttendees: function (id) {
            let vm = this;
            if (id === 'committee_meeting') {
                vm.isCommitteeMeeting = true;
                vm.fetchCommitteeMembers();
            } else {
                vm.meeting_obj.committee_id = null;
                vm.isCommitteeMeeting = false;
                vm.committee_members = [];
                vm.meeting_obj.selected_committee_members = [];
            }
        },
        validateMeetingDate: function () {
            if (this.meeting_obj.start_date && this.meeting_obj.end_date) {
                const startDate = new Date(this.meeting_obj.start_date);
                const endDate = new Date(this.meeting_obj.end_date);
                this.isMeetingDateValid = startDate <= endDate;
            } else {
                this.isMeetingDateValid = true;
            }
        },
        setMeetingDates: function () {
            let vm = this;
            if (vm.meeting_obj.start_date) {
                vm.start_date = vm.meeting_obj.start_date;
            }
            if (vm.meeting_obj.end_date) {
                vm.end_date = vm.meeting_obj.end_date;
            }
        },
        fetchCommitteeMembers: function () {
            let vm = this;
            if (vm.meeting_obj.committee_id) {
                fetch(
                    api_endpoints.committee_members(vm.meeting_obj.committee_id)
                ).then(
                    async (response) => {
                        vm.committee_members = await response.json();
                        if (
                            vm.meeting_obj.selected_committee_members.length ===
                            vm.activeMembers.length
                        ) {
                            vm.all_members_attending = true;
                        }
                    },
                    (error) => {
                        console.log(error);
                    }
                );
            }
        },
        toggleAllMembersAttending: function () {
            let vm = this;
            if (
                vm.meeting_obj.selected_committee_members.length ===
                vm.activeMembers.length
            ) {
                vm.meeting_obj.selected_committee_members = [];
            } else {
                vm.meeting_obj.selected_committee_members =
                    vm.activeMembers.map((member) => member.id);
            }
        },
        updateAllMembersAttending: function () {
            let vm = this;
            if (
                vm.meeting_obj.selected_committee_members.length ===
                vm.activeMembers.length
            ) {
                vm.all_members_attending = true;
            } else {
                vm.all_members_attending = false;
            }
        },
        committeeChanged: function () {
            let vm = this;
            vm.meeting_obj.selected_committee_members = [];
            vm.all_members_attending = false;
            vm.fetchCommitteeMembers();
        },
        removeArchivedCommitteeMember: function (member_id) {
            this.meeting_obj.selected_committee_members.splice(
                this.meeting_obj.selected_committee_members.indexOf(member_id),
                1
            );
            this.updateAllMembersAttending();
        },
    },
};
</script>
