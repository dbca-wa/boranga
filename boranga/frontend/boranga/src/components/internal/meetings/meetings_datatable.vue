<template id="meetings_datatable">
    <div>
        <CollapsibleFilters
            ref="collapsible_filters"
            component_title="Filters"
            class="mb-2"
            :show-warning-icon="filterApplied"
        >
            <div class="row">
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="">Start Date Range:</label>
                        <input
                            id="from_start_date"
                            v-model="filterFromMeetingStartDate"
                            type="datetime-local"
                            class="form-control"
                            placeholder="DD/MM/YYYY"
                        />
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for=""></label>
                        <input
                            id="to_start_date"
                            v-model="filterToMeetingStartDate"
                            type="datetime-local"
                            class="form-control"
                            placeholder="DD/MM/YYYY"
                        />
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="">End Date:</label>
                        <input
                            id="from_end_date"
                            v-model="filterFromMeetingEndDate"
                            type="datetime-local"
                            class="form-control"
                            placeholder="DD/MM/YYYY"
                        />
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for=""></label>
                        <input
                            id="to_end_date"
                            v-model="filterToMeetingEndDate"
                            type="datetime-local"
                            class="form-control"
                            placeholder="DD/MM/YYYY"
                        />
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="form-group">
                        <label for="">Status:</label>
                        <select
                            v-model="filterMeetingStatus"
                            class="form-select"
                        >
                            <option value="all">All</option>
                            <option
                                v-for="status in processing_statuses"
                                :value="status.value"
                                :key="status.value"
                            >
                                {{ status.name }}
                            </option>
                        </select>
                    </div>
                </div>
            </div>
        </CollapsibleFilters>
        <div
            v-if="
                profile?.user &&
                profile.user.groups.includes(
                    constants.GROUPS.CONSERVATION_STATUS_APPROVERS
                )
            "
            class="col-md-12"
        >
            <div class="text-end">
                <button
                    type="button"
                    class="btn btn-primary mb-2"
                    @click.prevent="createMeeting"
                >
                    <i class="fa-solid fa-circle-plus"></i> Add Meeting
                </button>
            </div>
        </div>
        <div class="row">
            <div class="col-lg-12">
                <datatable
                    :id="datatable_id"
                    ref="meetings_datatable"
                    :dt-options="datatable_options"
                    :dt-headers="datatable_headers"
                />
            </div>
        </div>
    </div>
</template>
<script>
import { v4 as uuid } from 'uuid';
import { api_endpoints, constants, helpers } from '@/utils/hooks';
import datatable from '@/utils/vue/datatable.vue';
import CollapsibleFilters from '@/components/forms/collapsible_component.vue';

export default {
    name: 'MeetingsDatatable',
    components: {
        datatable,
        CollapsibleFilters,
    },
    props: {
        url: {
            type: String,
            required: true,
        },
        filterFromMeetingStartDate_cache: {
            type: String,
            required: false,
            default: 'filterFromMeetingStartDate',
        },
        filterToMeetingStartDate_cache: {
            type: String,
            required: false,
            default: 'filterToMeetingStartDate',
        },
        filterFromMeetingEndDate_cache: {
            type: String,
            required: false,
            default: 'filterFromMeetingEndDate',
        },
        filterToMeetingEndDate_cache: {
            type: String,
            required: false,
            default: 'filterToMeetingEndDate',
        },
        filterMeetingStatus_cache: {
            type: String,
            required: false,
            default: 'filterMeetingStatus',
        },
    },
    data: function () {
        return {
            datatable_id: 'meetings-datatable-' + uuid(),

            filterFromMeetingStartDate: sessionStorage.getItem(
                this.filterFromMeetingStartDate_cache
            )
                ? sessionStorage.getItem(this.filterFromMeetingStartDate_cache)
                : '',
            filterToMeetingStartDate: sessionStorage.getItem(
                this.filterToMeetingStartDate_cache
            )
                ? sessionStorage.getItem(this.filterToMeetingStartDate_cache)
                : '',

            filterFromMeetingEndDate: sessionStorage.getItem(
                this.filterFromMeetingEndDate_cache
            )
                ? sessionStorage.getItem(this.filterFromMeetingEndDate_cache)
                : '',
            filterToMeetingEndDate: sessionStorage.getItem(
                this.filterToMeetingEndDate_cache
            )
                ? sessionStorage.getItem(this.filterToMeetingEndDate_cache)
                : '',

            filterMeetingStatus: sessionStorage.getItem(
                this.filterMeetingStatus_cache
            )
                ? sessionStorage.getItem(this.filterMeetingStatus_cache)
                : 'all',

            processing_statuses: [
                { value: 'draft', name: 'Draft' },
                { value: 'discarded', name: 'Discarded' },
                { value: 'scheduled', name: 'Scheduled' },
                { value: 'completed', name: 'Completed' },
            ],

            profile: null,
            constants: constants,
        };
    },
    computed: {
        filterApplied: function () {
            if (
                this.filterFromMeetingStartDate === '' &&
                this.filterToMeetingStartDate === '' &&
                this.filterFromMeetingEndDate === '' &&
                this.filterToMeetingEndDate === '' &&
                this.filterMeetingStatus === 'all'
            ) {
                return false;
            } else {
                return true;
            }
        },
        datatable_headers: function () {
            return [
                'Number',
                'Title',
                'Location',
                'Start Date',
                'End date',
                'Status',
                'Action',
            ];
        },
        column_id: function () {
            return {
                data: 'meeting_number',
                orderable: true,
                searchable: false,
                visible: true,
                name: 'id',
            };
        },
        column_location: function () {
            return {
                data: 'location',
                orderable: true,
                searchable: true,
                visible: true,
                name: 'location__room_name',
            };
        },
        column_title: function () {
            return {
                data: 'title',
                orderable: true,
                searchable: true,
                visible: true,
                name: 'title',
            };
        },
        column_start_date: function () {
            return {
                data: 'start_date',
                orderable: true,
                searchable: true,
                visible: true,
                render: function (data, type, full) {
                    if (full.start_date) {
                        return (
                            moment(full.start_date).format('DD/MM/YYYY') +
                            moment(full.start_date).format(' h:mm:ss a')
                        );
                    }
                    return '';
                },
                name: 'start_date',
            };
        },
        column_end_date: function () {
            return {
                data: 'end_date',
                orderable: true,
                searchable: true,
                visible: true,
                render: function (data, type, full) {
                    if (full.end_date) {
                        return (
                            moment(full.end_date).format('DD/MM/YYYY') +
                            moment(full.end_date).format(' h:mm:ss a')
                        );
                    }
                    return '';
                },
                name: 'end_date',
            };
        },
        column_status: function () {
            return {
                data: 'processing_status',
                orderable: true,
                searchable: true,
                visible: true,
                name: 'processing_status',
            };
        },
        column_action: function () {
            return {
                data: 'id',
                orderable: false,
                searchable: false,
                visible: true,
                render: function (data, type, full) {
                    let links = '';
                    if (full.processing_status == 'Discarded') {
                        links += `<a href='#${full.id}' data-reinstate-meeting='${full.id}'>Reinstate</a><br/>`;
                    } else {
                        if (full.can_user_edit) {
                            if (full.processing_status == 'Scheduled') {
                                links += `<a href='/internal/meetings/${full.id}?action=edit'>Edit</a><br/>`;
                            } else {
                                links += `<a href='/internal/meetings/${full.id}'>Continue</a><br/>`;
                            }
                            if (full.processing_status == 'Draft') {
                                links += `<a href='#${full.id}' data-discard-meeting='${full.id}'>Discard</a><br/>`;
                            }
                        } else {
                            links += `<a href='/internal/meetings/${full.id}?action=view'>View</a><br/>`;
                        }
                    }
                    return links;
                },
            };
        },
        datatable_options: function () {
            let vm = this;

            let columns = [];
            let search = null;
            columns = [
                vm.column_id,
                vm.column_title,
                vm.column_location,
                vm.column_start_date,
                vm.column_end_date,
                vm.column_status,
                vm.column_action,
            ];
            search = true;
            let buttons = [
                {
                    extend: 'excel',
                    title: 'Boranga Meeting Excel Export',
                    text: '<i class="fa-solid fa-download"></i> Excel',
                    className: 'btn btn-primary me-2 rounded',
                    exportOptions: {
                        columns: ':not(.no-export)',
                        orthogonal: 'export',
                    },
                },
                {
                    extend: 'csv',
                    title: 'Boranga Meeting CSV Export',
                    text: '<i class="fa-solid fa-download"></i> CSV',
                    className: 'btn btn-primary rounded',
                    exportOptions: {
                        columns: ':not(.no-export)',
                        orthogonal: 'export',
                    },
                },
            ];
            return {
                autoWidth: false,
                language: {
                    processing: constants.DATATABLE_PROCESSING_HTML,
                },
                order: [[0, 'desc']],
                lengthMenu: [
                    [10, 25, 50, 100, 100000000],
                    [10, 25, 50, 100, 'All'],
                ],
                responsive: true,
                serverSide: true,
                searching: search,
                //  to show the "workflow Status","Action" columns always in the last position
                columnDefs: [
                    { responsivePriority: 1, targets: 0 },
                    {
                        responsivePriority: 3,
                        targets: -1,
                        className: 'no-export',
                    },
                    { responsivePriority: 2, targets: -2 },
                ],
                ajax: {
                    url: this.url,
                    dataSrc: 'data',

                    // adding extra GET params for Custom filtering
                    data: function (d) {
                        d.filter_to_start_date = vm.filterToMeetingStartDate;
                        d.filter_from_start_date =
                            vm.filterFromMeetingStartDate;
                        d.filter_to_end_date = vm.filterToMeetingEndDate;
                        d.filter_from_end_date = vm.filterFromMeetingEndDate;
                        d.filter_meeting_status = vm.filterMeetingStatus;
                    },
                },
                dom:
                    "<'d-flex align-items-center'<'me-auto'l>fB>" +
                    "<'row'<'col-sm-12'tr>>" +
                    "<'d-flex align-items-center'<'me-auto'i>p>",
                buttons: buttons,
                columns: columns,
                processing: true,
                drawCallback: function () {
                    helpers.enablePopovers();
                },
                initComplete: function () {
                    helpers.enablePopovers();
                },
            };
        },
    },
    watch: {
        filterFromMeetingStartDate: function () {
            let vm = this;
            vm.$refs.meetings_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterFromMeetingStartDate_cache,
                vm.filterFromMeetingStartDate
            );
        },
        filterToMeetingStartDate: function () {
            let vm = this;
            vm.$refs.meetings_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterToMeetingStartDate_cache,
                vm.filterToMeetingStartDate
            );
        },
        filterFromMeetingEndDate: function () {
            let vm = this;
            vm.$refs.meetings_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterFromMeetingEndDate_cache,
                vm.filterFromMeetingEndDate
            );
        },
        filterToMeetingEndDate: function () {
            let vm = this;
            vm.$refs.meetings_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterToMeetingEndDate_cache,
                vm.filterToMeetingEndDate
            );
        },
        filterMeetingStatus: function () {
            let vm = this;
            vm.$refs.meetings_datatable.vmDataTable.ajax.reload(
                helpers.enablePopovers,
                true
            ); // This calls ajax() backend call.
            sessionStorage.setItem(
                vm.filterMeetingStatus_cache,
                vm.filterMeetingStatus
            );
        },
    },
    created: function () {
        let vm = this;
        vm.fetchProfile();
        this.$nextTick(() => {
            vm.addEventListeners();
        });
    },
    methods: {
        constructMeetingsTable: function () {
            this.$refs.meetings_datatable.vmDataTable.clear().draw();
        },
        createMeeting: async function () {
            swal.fire({
                title: `Add Meeting`,
                text: 'Are you sure you want to add a new meeting?',
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Add Meeting',
                reverseButtons: true,
                customClass: {
                    confirmButton: 'btn btn-primary',
                    cancelButton: 'btn btn-secondary',
                },
            }).then(async (swalresult) => {
                if (swalresult.isConfirmed) {
                    let newMeetingId = null;
                    try {
                        swal.fire;
                        const createUrl = api_endpoints.meeting + '/';
                        let payload = new Object();
                        payload.meeting_type = 'meeting';
                        let response = await fetch(createUrl, {
                            method: 'POST',
                            headers: {
                                'Content-Type': 'application/json',
                            },
                            body: JSON.stringify(payload),
                        });
                        let savedMeeting = await response.json();
                        if (savedMeeting) {
                            newMeetingId = savedMeeting.id;
                        }
                        this.$router.push({
                            name: 'internal-meetings',
                            params: { meeting_id: newMeetingId },
                        });
                    } catch (err) {
                        console.log(err);
                        if (this.is_internal) {
                            return err;
                        }
                    }
                }
            });
        },
        discardMeeting: function (meeting_id) {
            let vm = this;
            swal.fire({
                title: 'Discard Meeting',
                text: 'Are you sure you want to discard this meeting?',
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Discard Meeting',
                customClass: {
                    confirmButton: 'btn btn-primary',
                    cancelButton: 'btn btn-secondary',
                },
                reverseButtons: true,
            }).then((result) => {
                if (result.isConfirmed) {
                    fetch(api_endpoints.discard_meeting(meeting_id), {
                        method: 'PATCH',
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
                            swal.fire({
                                title: 'Discarded',
                                text: 'Your meeting has been discarded',
                                icon: 'success',
                                customClass: {
                                    confirmButton: 'btn btn-primary',
                                },
                            });
                            vm.$refs.meetings_datatable.vmDataTable.ajax.reload(
                                helpers.enablePopovers,
                                true
                            );
                        },
                        (error) => {
                            console.log(error);
                        }
                    );
                }
            });
        },
        reinstateMeeting: function (meeting_id) {
            let vm = this;
            swal.fire({
                title: 'Reinstate Meeting',
                text: 'Are you sure you want to reinstate this meeting?',
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Reinstate Meeting',
                customClass: {
                    confirmButton: 'btn btn-primary',
                    cancelButton: 'btn btn-secondary',
                },
                reverseButtons: true,
            }).then((result) => {
                if (result.isConfirmed) {
                    fetch(api_endpoints.reinstate_meeting(meeting_id), {
                        method: 'PATCH',
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
                            swal.fire({
                                title: 'Reinstated',
                                text: 'Your meeting has been reinstated',
                                icon: 'success',
                                customClass: {
                                    confirmButton: 'btn btn-primary',
                                },
                            });
                            vm.$refs.meetings_datatable.vmDataTable.ajax.reload(
                                helpers.enablePopovers,
                                true
                            );
                        },
                        (error) => {
                            console.log(error);
                        }
                    );
                }
            });
        },
        addEventListeners: function () {
            let vm = this;
            // External Discard listener
            vm.$refs.meetings_datatable.vmDataTable.on(
                'click',
                'a[data-discard-meeting]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-discard-meeting');
                    vm.discardMeeting(id);
                }
            );
            vm.$refs.meetings_datatable.vmDataTable.on(
                'click',
                'a[data-reinstate-meeting]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-reinstate-meeting');
                    vm.reinstateMeeting(id);
                }
            );
            vm.$refs.meetings_datatable.vmDataTable.on(
                'childRow.dt',
                function () {
                    helpers.enablePopovers();
                }
            );
        },
        fetchProfile: function () {
            let vm = this;
            fetch(api_endpoints.profile).then(async (response) => {
                vm.profile = await response.json();
            });
        },
    },
};
</script>
<style scoped>
.dt-buttons {
    float: right;
}

.collapse-icon {
    cursor: pointer;
}

.collapse-icon::before {
    top: 5px;
    left: 4px;
    height: 14px;
    width: 14px;
    border-radius: 14px;
    line-height: 14px;
    border: 2px solid white;
    line-height: 14px;
    content: '-';
    color: white;
    background-color: #d33333;
    display: inline-block;
    box-shadow: 0px 0px 3px #444;
    box-sizing: content-box;
    text-align: center;
    text-indent: 0 !important;
    font-family:
        'Courier New',
        Courier monospace;
    margin: 5px;
}

.expand-icon {
    cursor: pointer;
}

.expand-icon::before {
    top: 5px;
    left: 4px;
    height: 14px;
    width: 14px;
    border-radius: 14px;
    line-height: 14px;
    border: 2px solid white;
    line-height: 14px;
    content: '+';
    color: white;
    background-color: #337ab7;
    display: inline-block;
    box-shadow: 0px 0px 3px #444;
    box-sizing: content-box;
    text-align: center;
    text-indent: 0 !important;
    font-family:
        'Courier New',
        Courier monospace;
    margin: 5px;
}
</style>
