<template lang="html">
    <div id="existingThreats">
        <modal
            transition="modal fade"
            :title="`Add Threat from ${ocr_model_prefix}`"
            :large="true"
            :full="true"
            :show-o-k="false"
            cancel-text="Close"
            :data-loss-warning-on-cancel="false"
            @cancel="close()"
        >
            <div class="container-fluid">
                <div class="row">
                    <alert v-if="errorString" type="danger"
                        ><strong>{{ errorString }}</strong></alert
                    >
                    <div class="col-sm-12">
                        <div class="form-group">
                            <div class="row">
                                <div v-if="occurrenceId" class="col-lg-12">
                                    <datatable
                                        :id="datatable_id"
                                        ref="threat_datatable"
                                        :dt-options="datatable_options"
                                        :dt-headers="datatable_headers"
                                    />
                                </div>
                                <ThreatDetail
                                    ref="threat_detail"
                                    :url="occ_threat_url"
                                    @refresh-from-response="refreshFromResponse"
                                ></ThreatDetail>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </modal>
    </div>
</template>
<script>
import modal from '@vue-utils/bootstrap-modal.vue';
import alert from '@vue-utils/alert.vue';
import { helpers, api_endpoints, constants } from '@/utils/hooks.js';
import datatable from '@/utils/vue/datatable.vue';
import ThreatDetail from '@/components/common/species_communities/add_threat.vue';
import { v4 as uuid } from 'uuid';

export default {
    name: 'OCCExistingThreats',
    components: {
        modal,
        alert,
        datatable,
        ThreatDetail,
    },
    props: {
        occurrenceId: {
            type: Number,
            required: true,
        },
    },
    data: function () {
        return {
            datatable_id: 'threat-datatable-' + uuid(),
            threatId: null,
            isModalOpen: false,
            errorString: '',
            successString: '',
            success: false,
            occ_threat_url: api_endpoints.occ_threat,
            threat_obj: null,
            ocr_model_prefix: constants.MODELS.OCCURRENCE_REPORT.MODEL_PREFIX,
        };
    },
    computed: {
        csrf_token: function () {
            return helpers.getCookie('csrftoken');
        },
        datatable_headers: function () {
            return [
                'Number',
                'Occurrence Report',
                'Category',
                'Date Observed',
                'Action',
                'Threat Agent',
                'Threat Source',
                'Comments',
                'Current Impact',
                'Potential Impact',
            ];
        },
        column_number: function () {
            return {
                data: 'threat_number',
                orderable: true,
                searchable: true,
                mRender: function (data, type, full) {
                    if (full.visible) {
                        return full.threat_number;
                    } else {
                        return '<s>' + full.threat_number + '</s>';
                    }
                },
            };
        },
        column_ocr: function () {
            return {
                data: 'occurrence_report',
                orderable: true,
                searchable: true,
                mRender: function (data, type, full) {
                    if (full.visible) {
                        return `${constants.MODELS.OCCURRENCE_REPORT.MODEL_PREFIX}${full.occurrence_report}`;
                    } else {
                        return `<s>${constants.MODELS.OCCURRENCE_REPORT.MODEL_PREFIX}${full.occurrence_report}</s>`;
                    }
                },
            };
        },
        column_category: function () {
            return {
                data: 'threat_category',
                orderable: true,
                searchable: true,
                mRender: function (data, type, full) {
                    if (full.visible) {
                        return full.threat_category;
                    } else {
                        return '<s>' + full.threat_category + '</s>';
                    }
                },
            };
        },
        column_source: function () {
            return {
                data: 'source',
                orderable: true,
                searchable: true,
                mRender: function (data, type, full) {
                    if (full.visible) {
                        return full.source;
                    } else {
                        return '<s>' + full.source + '</s>';
                    }
                },
            };
        },
        column_observed: function () {
            return {
                data: 'date_observed',
                mRender: function (data, type, full) {
                    if (full.visible) {
                        return data != '' && data != null
                            ? moment(data).format('DD/MM/YYYY')
                            : '';
                    } else {
                        return data != '' && data != null
                            ? '<s>' + moment(data).format('DD/MM/YYYY')
                            : '' + '</s>';
                    }
                },
            };
        },
        column_threat_agent: function () {
            return {
                data: 'threat_agent',
                orderable: true,
                searchable: true,
                mRender: function (data, type, full) {
                    if (full.visible) {
                        return full.threat_agent;
                    } else {
                        return '<s>' + full.threat_agent + '</s>';
                    }
                },
            };
        },
        column_comment: function () {
            return {
                data: 'comment',
                orderable: true,
                searchable: true,
                render: function (value, type, full) {
                    let result = helpers.dtPopover(value, 30, 'hover');
                    if (full.visible) {
                        return type == 'export' ? value : result;
                    } else {
                        return type == 'export'
                            ? '<s>' + value + '</s>'
                            : '<s>' + result + '</s>';
                    }
                },
            };
        },
        column_current_impact: function () {
            return {
                data: 'current_impact_name',
                orderable: true,
                searchable: true,
                mRender: function (data, type, full) {
                    if (full.visible) {
                        return full.current_impact_name;
                    } else {
                        return '<s>' + full.current_impact_name + '</s>';
                    }
                },
            };
        },
        column_potential_impact: function () {
            return {
                data: 'potential_impact_name',
                orderable: true,
                searchable: true,
                mRender: function (data, type, full) {
                    if (full.visible) {
                        return full.potential_impact_name;
                    } else {
                        return '<s>' + full.potential_impact_name + '</s>';
                    }
                },
            };
        },
        column_action: function () {
            return {
                data: 'id',
                orderable: false,
                searchable: false,
                visible: true,
                mRender: function (data, type, full) {
                    let links = '';
                    links += `<a href='#' data-view-threat='${full.id}'>View</a><br>`;
                    links += `<a href='#' data-add-threat='${full.id}'>Add</a><br>`;
                    return links;
                },
            };
        },
        datatable_options: function () {
            let vm = this;
            let columns = [
                vm.column_number,
                vm.column_ocr,
                vm.column_category,
                vm.column_observed,
                vm.column_action,
                vm.column_threat_agent,
                vm.column_source,
                vm.column_comment,
                vm.column_current_impact,
                vm.column_potential_impact,
            ];
            return {
                autoWidth: false,
                language: {
                    processing: constants.DATATABLE_PROCESSING_HTML,
                },
                buttons: [],
                responsive: true,
                searching: true,
                ordering: true,
                order: [[0, 'desc']],
                //serverSide: true,
                columnDefs: [
                    { responsivePriority: 1, targets: 0 },
                    { responsivePriority: 2, targets: 4 },
                ],
                ajax: {
                    url:
                        '/api/occurrence/' +
                        this.occurrenceId +
                        '/get_existing_ocr_threats/',
                    dataSrc: '',
                },
                dom: 'lBfrtip',
                columns: columns,
                processing: true,
            };
        },
    },
    mounted: function () {
        let vm = this;
        this.$nextTick(() => {
            vm.addEventListeners();
        });
    },
    methods: {
        close: function () {
            this.errorString = '';
            this.isModalOpen = false;
            $('.has-error').removeClass('has-error');
        },
        sendData: function () {
            let vm = this;
            vm.errors = false;

            let newThreatObj = new Object();
            newThreatObj.occurrence_report_threat_id = vm.threatObj.id;
            newThreatObj.date_observed = vm.threatObj.date_observed;
            newThreatObj.potential_threat_onset =
                vm.threatObj.potential_threat_onset;
            newThreatObj.potential_impact = vm.threatObj.potential_impact;
            newThreatObj.current_impact = vm.threatObj.current_impact;
            newThreatObj.comment = vm.threatObj.comment;
            newThreatObj.threat_agent_id = vm.threatObj.threat_agent_id;
            newThreatObj.threat_category_id = vm.threatObj.threat_category_id;
            newThreatObj.occurrence = vm.occurrenceId;

            let threatObj = JSON.parse(JSON.stringify(newThreatObj));
            let formData = new FormData();

            formData.append('data', JSON.stringify(threatObj));
            fetch(vm.occ_threat_url, {
                method: 'POST',
                body: formData,
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
                    vm.refreshFromResponse();
                    vm.$parent.updatedThreats();
                },
                (error) => {
                    vm.errors = true;
                    vm.errorString = helpers.apiVueResourceError(error);
                }
            );
        },
        viewThreat: function (id) {
            this.$refs.threat_detail.threat_id = id;
            this.$refs.threat_detail.threat_action = 'view';
            fetch(helpers.add_endpoint_json(api_endpoints.ocr_threat, id)).then(
                async (response) => {
                    const data = await response.json();
                    this.$refs.threat_detail.threatObj = data;
                    this.$refs.threat_detail.threatObj.date_observed =
                        data.date_observed != null &&
                        data.date_observed != undefined
                            ? moment(data.date_observed).format('yyyy-MM-DD')
                            : '';
                },
                (err) => {
                    console.log(err);
                }
            );
            this.$refs.threat_detail.isModalOpen = true;
        },
        addThreat: function (id) {
            let vm = this;
            fetch(helpers.add_endpoint_json(api_endpoints.ocr_threat, id)).then(
                async (response) => {
                    const data = await response.json();
                    this.threatObj = data;
                    console.log(this.threatObj);
                    vm.sendData();
                },
                (err) => {
                    console.log(err);
                }
            );
        },
        addEventListeners: function () {
            let vm = this;
            vm.$refs.threat_datatable.vmDataTable.on(
                'click',
                'a[data-view-threat]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-view-threat');
                    vm.viewThreat(id);
                }
            );
            vm.$refs.threat_datatable.vmDataTable.on(
                'click',
                'a[data-add-threat]',
                function (e) {
                    e.preventDefault();
                    var id = $(this).attr('data-add-threat');
                    vm.addThreat(id);
                }
            );
        },
        refreshFromResponse: function () {
            this.$refs.threat_datatable.vmDataTable.ajax.reload();
        },
    },
};
</script>

<style lang="css" scoped>
/*ul, li {
        zoom:1;
        display: inline;
    }*/
fieldset.scheduler-border {
    border: 1px groove #ddd !important;
    padding: 0 1.4em 1.4em 1.4em !important;
    margin: 0 0 1.5em 0 !important;
    -webkit-box-shadow: 0px 0px 0px 0px #000;
    box-shadow: 0px 0px 0px 0px #000;
}

legend.scheduler-border {
    width: inherit;
    /* Or auto */
    padding: 0 10px;
    /* To give a bit of padding on the left and right */
    border-bottom: none;
}
</style>
