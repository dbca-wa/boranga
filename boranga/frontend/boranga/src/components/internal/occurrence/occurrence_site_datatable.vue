<template id="occurrence_site_datatable_template">
    <div>
        <form class="form-horizontal" action="index.html" method="post">
            <div class="col-sm-12">
                <div class="text-end">
                    <button
                        :disabled="isReadOnly"
                        type="button"
                        class="btn btn-primary mb-2"
                        @click.prevent="newSite"
                    >
                        <i class="bi bi-plus-circle"></i>
                        Add New Site
                    </button>
                </div>
            </div>
        </form>
        <datatable
            :id="datatable_id"
            ref="occurrence_site_datatable"
            :dt-options="options"
            :dt-headers="headers"
        />
        <SiteDetail
            ref="site_detail"
            :url="occ_site_url"
            :occurrence_obj="occurrence_obj"
            @refresh-from-response="updatedSites"
        >
        </SiteDetail>
        <div v-if="occSiteHistoryId">
            <OCCSiteHistory
                ref="occ_site_history"
                :key="occSiteHistoryId"
                :site-id="occSiteHistoryId"
            />
        </div>
    </div>
</template>

<script>
import { v4 as uuid } from 'uuid';
import datatable from '@/utils/vue/datatable.vue';
import SiteDetail from '@/components/internal/occurrence/add_site.vue';
import OCCSiteHistory from '../../internal/occurrence/occ_site_history.vue';
import { api_endpoints, constants, helpers } from '@/utils/hooks';

export default {
    name: 'OccurrenceSiteDatatable',
    components: {
        datatable,
        SiteDetail,
        OCCSiteHistory,
    },
    props: {
        occurrence_obj: {
            type: Object,
            required: true,
        },
    },
    data: function () {
        return {
            uuid: 0,
            occSiteHistoryId: null,
            occ_site_url: api_endpoints.occ_site,
            datatable_id: 'occurrence-site-datatable-' + uuid(),
            headers: [
                'Site Number',
                'Site Name',
                'Point Coordinates (Lat, Long)',
                'Datum',
                'Comments',
                'Related Reports',
                'Action',
            ],
        };
    },
    computed: {
        isReadOnly: function () {
            return !this.occurrence_obj.can_user_edit;
        },
        column_site_number: function () {
            let vm = this;
            return {
                data: 'site_number',
                orderable: true,
                searchable: true,
                mRender: function (data, type, full) {
                    if (full.visible) {
                        return (
                            vm.occurrence_obj.occurrence_number +
                            ' - ' +
                            full.site_number
                        );
                    } else {
                        return (
                            '<s>' +
                            vm.occurrence_obj.occurrence_number +
                            ' - ' +
                            full.site_number +
                            '</s>'
                        );
                    }
                },
            };
        },
        column_site_name: function () {
            return {
                data: 'site_name',
                orderable: true,
                searchable: true,
                mRender: function (data, type, full) {
                    if (full.visible) {
                        return full.site_name;
                    } else {
                        return '<s>' + full.site_name + '</s>';
                    }
                },
            };
        },
        column_point_coordinates: function () {
            return {
                data: 'id',
                searchable: true,
                mRender: function (data, type, full) {
                    if (full.point_coord1 && full.point_coord2) {
                        let coord1 = full.point_coord1.toString();
                        let coord2 = full.point_coord2.toString();

                        if (Number.isInteger(full.point_coord1)) {
                            coord1 += '.0';
                        }
                        if (Number.isInteger(full.point_coord2)) {
                            coord2 += '.0';
                        }

                        let value = coord2 + ', ' + coord1;
                        let result = helpers.dtPopover(value, 30, 'hover');
                        if (full.visible) {
                            return result;
                        } else {
                            return '<s>' + result + '</s>';
                        }
                    } else {
                        return '';
                    }
                },
            };
        },
        column_datum: function () {
            return {
                data: 'id',
                searchable: true,
                mRender: function (data, type, full) {
                    if (full.visible) {
                        return full.datum_name;
                    } else {
                        return '<s>' + full.datum_name + '</s>';
                    }
                },
            };
        },
        column_comments: function () {
            return {
                data: 'comments',
                searchable: true,
                mRender: function (data, type, full) {
                    let value = full.comments;
                    let result = helpers.dtPopover(value, 30, 'hover');
                    if (full.visible) {
                        return result;
                    } else {
                        return '<s>' + result + '</s>';
                    }
                },
            };
        },
        column_related_reports: function () {
            return {
                data: 'related_occurrence_report_numbers',
                mRender: function (data, type, full) {
                    let value = full.related_occurrence_report_numbers;
                    let result = helpers.dtPopover(value, 30, 'hover');
                    if (full.visible) {
                        return result;
                    } else {
                        return '<s>' + result + '</s>';
                    }
                },
            };
        },
        column_action: function () {
            let vm = this;
            return {
                data: 'id',
                mRender: function (data, type, full) {
                    let links = '';
                    if (full.visible) {
                        links += `<a href='#${full.id}' data-view-site='${full.id}'>View</a><br/>`;
                        if (!vm.isReadOnly) {
                            links += `<a href='#${full.id}' data-edit-site='${full.id}'>Edit</a><br/>`;
                            links += `<a href='#' data-delete-site='${full.id}'>Discard</a><br>`;
                        }
                    } else if (!vm.isReadOnly) {
                        links += `<a href='#' data-reinstate-site='${full.id}'>Reinstate</a><br>`;
                    }
                    links += `<a href='#' data-history-site='${full.id}'>History</a><br>`;
                    return links;
                },
            };
        },
        options: function () {
            let vm = this;
            let columns = [
                this.column_site_number,
                this.column_site_name,
                this.column_point_coordinates,
                this.column_datum,
                this.column_comments,
                this.column_related_reports,
                this.column_action,
            ];
            return {
                autoWidth: false,
                language: {
                    processing: constants.DATATABLE_PROCESSING_HTML,
                },
                responsive: true,
                serverSide: false,
                searching: true,
                ordering: true,
                order: [
                    [1, 'asc'],
                    [0, 'desc'],
                ],
                columnDefs: [
                    { responsivePriority: 1, targets: 0 },
                    { responsivePriority: 2, targets: -1 },
                ],
                ajax: {
                    url: helpers.add_endpoint_json(
                        api_endpoints.occurrence,
                        vm.occurrence_obj.id + '/sites'
                    ),
                    dataSrc: '',
                },
                dom:
                    "<'d-flex align-items-center'<'me-auto'l>fB>" +
                    "<'row'<'col-sm-12'tr>>" +
                    "<'d-flex align-items-center'<'me-auto'i>p>",
                buttons: [],
                columns: columns,
                processing: true,
                drawCallback: function () {
                    helpers.enablePopovers();
                },
                initComplete: function () {
                    helpers.enablePopovers();
                    // another option to fix the responsive table overflow css on tab switch
                    setTimeout(function () {
                        vm.adjust_table_width();
                    }, 100);
                },
            };
        },
    },
    mounted: function () {
        this.$nextTick(() => {
            this.addEventListeners();
        });
    },
    methods: {
        adjust_table_width: function () {
            let vm = this;
            if (vm.$refs.occurrence_site_datatable) {
                vm.$refs.occurrence_site_datatable.vmDataTable.columns
                    .adjust()
                    .responsive.recalc();
            }
        },
        newSite: function () {
            let vm = this;
            this.$refs.site_detail.site_id = '';
            //----for adding new species Site
            var new_occ_site = {
                occurrence: vm.occurrence_obj.id,
                related_occurrence_reports: [],
            };
            this.$refs.site_detail.siteObj = new_occ_site;
            this.$refs.site_detail.site_action = 'add';
            this.$refs.site_detail.isModalOpen = true;
        },
        editSite: function (id) {
            this.$refs.site_detail.site_id = id;
            this.$refs.site_detail.site_action = 'edit';
            fetch(helpers.add_endpoint_json(api_endpoints.occ_site, id)).then(
                async (response) => {
                    this.$refs.site_detail.siteObj = await response.json();
                },
                (err) => {
                    console.log(err);
                }
            );
            this.$refs.site_detail.isModalOpen = true;
        },
        viewSite: function (id) {
            this.$refs.site_detail.site_id = id;
            this.$refs.site_detail.site_action = 'view';
            fetch(helpers.add_endpoint_json(api_endpoints.occ_site, id)).then(
                async (response) => {
                    this.$refs.site_detail.siteObj = await response.json();
                },
                (err) => {
                    console.log(err);
                }
            );
            this.$refs.site_detail.isModalOpen = true;
        },
        updatedSites: function () {
            this.$refs.occurrence_site_datatable.vmDataTable.ajax.reload();
            this.$emit('updatedSites');
        },
        discardSite: function (id) {
            let vm = this;
            swal.fire({
                title: 'Discard Site',
                text: 'Are you sure you want to discard this Site?',
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Discard Site',
                customClass: {
                    confirmButton: 'btn btn-primary',
                    cancelButton: 'btn btn-secondary me-2',
                },
                reverseButtons: true,
            }).then((result) => {
                if (result.isConfirmed) {
                    fetch(
                        helpers.add_endpoint_json(
                            api_endpoints.occ_site,
                            id + '/discard'
                        ),
                        {
                            method: 'PATCH',
                            headers: {
                                'Content-Type': 'application/json',
                            },
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
                            swal.fire({
                                title: 'Discarded',
                                text: 'The Site has been discarded',
                                icon: 'success',
                                customClass: {
                                    confirmButton: 'btn btn-primary',
                                },
                            }).then(() => {
                                vm.updatedSites();
                            });
                        },
                        (error) => {
                            console.log(error);
                        }
                    );
                }
            });
        },
        reinstateSite: function (id) {
            let vm = this;
            fetch(
                helpers.add_endpoint_json(
                    api_endpoints.occ_site,
                    id + '/reinstate'
                ),
                {
                    method: 'PATCH',
                    headers: {
                        'Content-Type': 'application/json',
                    },
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
                    swal.fire({
                        title: 'Reinstated',
                        text: 'The Site has been reinstated',
                        icon: 'success',
                        customClass: {
                            confirmButton: 'btn btn-primary',
                        },
                    }).then(() => {
                        vm.updatedSites();
                    });
                },
                (error) => {
                    var errorText = helpers.apiVueResourceError(error);
                    swal.fire({
                        title: 'Error',
                        text: errorText,
                        icon: 'error',
                        customClass: {
                            confirmButton: 'btn btn-primary',
                        },
                    });
                }
            );
        },
        prepareSiteFromCoordinates: function (coordinates) {
            this.newSite();
            this.$refs.site_detail.prepareNewSiteAtCoordinates(
                coordinates,
                this.occurrence_obj.id
            );
        },
        historySite: function (id) {
            this.occSiteHistoryId = parseInt(id);
            this.uuid++;
            this.$nextTick(() => {
                this.$refs.occ_site_history.isModalOpen = true;
            });
        },
        addEventListeners: function () {
            const vm = this;
            this.$refs.occurrence_site_datatable.vmDataTable.on(
                'click',
                'a[data-view-site]',
                function (e) {
                    e.preventDefault();
                    const id = $(this).attr('data-view-site');
                    vm.viewSite(id);
                }
            );
            this.$refs.occurrence_site_datatable.vmDataTable.on(
                'click',
                'a[data-edit-site]',
                function (e) {
                    e.preventDefault();
                    const id = $(this).attr('data-edit-site');
                    vm.editSite(id);
                }
            );
            vm.$refs.occurrence_site_datatable.vmDataTable.on(
                'click',
                'a[data-delete-site]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-delete-site');
                    vm.discardSite(id);
                }
            );
            vm.$refs.occurrence_site_datatable.vmDataTable.on(
                'click',
                'a[data-reinstate-site]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-reinstate-site');
                    vm.reinstateSite(id);
                }
            );
            vm.$refs.occurrence_site_datatable.vmDataTable.on(
                'click',
                'a[data-history-site]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-history-site');
                    vm.historySite(id);
                }
            );
            vm.$refs.occurrence_site_datatable.vmDataTable.on(
                'childRow.dt',
                function () {
                    helpers.enablePopovers();
                }
            );
        },
    },
};
</script>
