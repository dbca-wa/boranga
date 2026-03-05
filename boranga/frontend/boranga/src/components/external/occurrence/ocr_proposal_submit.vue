<template lang="html">
    <div class="container">
        <div class="row">
            <div class="col-sm-12">
                <div class="row justify-content-center">
                    <div class="col-9 p-3 border rounded">
                        <template v-if="isOCRProposal">
                            <h3>
                                Occurrence Report Form Submitted Successfully
                            </h3>
                            <div class="mb-3">
                                <strong
                                    >Your
                                    {{ occurrence_report_obj.group_type }}
                                    occurrence report form has been
                                    received.</strong
                                >
                            </div>

                            <table class="table table-sm w-50 mb-4">
                                <tbody>
                                    <tr>
                                        <td>
                                            <strong
                                                >Occurrence Report Form
                                                Number:</strong
                                            >
                                        </td>
                                        <td>
                                            {{
                                                occurrence_report_obj.occurrence_report_number
                                            }}
                                        </td>
                                    </tr>
                                    <tr>
                                        <td><strong>Date/Time:</strong></td>
                                        <td>
                                            {{
                                                formatDate(
                                                    occurrence_report_obj.lodgement_date
                                                )
                                            }}
                                        </td>
                                    </tr>
                                </tbody>
                            </table>

                            <p>
                                You will receive a notification email if there
                                is any incomplete information or documents
                                missing from the occurrence report form.
                            </p>
                        </template>
                        <template v-else>
                            <strong
                                >Sorry it looks like there isn't any occurrence
                                report form currently in your session.</strong
                            >
                        </template>
                        <router-link
                            :to="{ name: 'external-occurrence-report-dash' }"
                            style="margin-top: 15px"
                            class="btn btn-primary"
                            >Return to Occurrence Report Dashboard</router-link
                        >
                    </div>
                </div>
            </div>
        </div>
    </div>
</template>
<script>
import { api_endpoints } from '@/utils/hooks';

export default {
    data: function () {
        return {
            occurrence_report_obj: {},
        };
    },
    computed: {
        isOCRProposal: function () {
            return this.occurrence_report_obj && this.occurrence_report_obj.id
                ? true
                : false;
        },
        application_type_flora: function () {
            return api_endpoints.group_type_flora;
        },
        application_type_fauna: function () {
            return api_endpoints.group_type_fauna;
        },
        application_type_community: function () {
            return api_endpoints.group_type_community;
        },
    },
    created: function () {
        if (!window.history.state.occurrence_report_obj) {
            this.$router.push({
                name: 'external-occurrence-report-dash',
            });
            return;
        }
        Object.assign(
            this.occurrence_report_obj,
            JSON.parse(window.history.state.occurrence_report_obj)
        );
    },
    methods: {
        formatDate: function (data) {
            return data ? moment(data).format('DD/MM/YYYY HH:mm:ss') : '';
        },
    },
};
</script>

<style lang="css" scoped>
.borderDecoration {
    border: 1px solid;
    border-radius: 5px;
    padding: 50px;
    margin-top: 70px;
}
</style>
