<template>
    <div id="occurence-report-bulk-import" class="container gx-4">
        <div class="row mb-2">
            <div class="col">
                <div class="mb-4">
                    <h2 class="text-capitalize">{{ title }}</h2>
                </div>
                <div class="mb-3">
                    <alert type="primary">
                        <i
                            class="bi bi-info-circle-fill text-primary fs-4 help-text-popover me-2"
                        ></i>
                        Some help text about the import process.
                    </alert>
                </div>
                <div class="mb-3">
                    <div class="card">
                        <div class="card-body">
                            <form
                                id="bulk-import-form"
                                class="needs-validation"
                                no-validate
                            >
                                <div v-if="schema_versions" class="mb-3">
                                    <label
                                        for="schema-version"
                                        class="form-label"
                                        ><span class="fw-bold">Step 1: </span
                                        >Select the bulk import schema version
                                        to use:</label
                                    >
                                    <select
                                        id="schema-version"
                                        ref="schema-version"
                                        v-model="selected_schema"
                                        class="form-select text-secondary w-50"
                                        aria-label="Select Schema Version"
                                        @change="resetFileField"
                                    >
                                        <option :value="null" selected>
                                            Select Bulk Import Schema Version
                                        </option>
                                        <option
                                            v-for="schema_version in schema_versions"
                                            :value="schema_version"
                                            :key="schema_version.id"
                                            :disabled="!schema_version.is_valid"
                                        >
                                            {{
                                                getSchemaVersionText(
                                                    schema_version
                                                )
                                            }}<template
                                                v-if="!schema_version.is_valid"
                                            >
                                                (Schema not selectable: No
                                                longer valid)</template
                                            >
                                        </option>
                                    </select>
                                </div>
                                <div
                                    v-else
                                    class="spinner-border text-primary"
                                    role="status"
                                >
                                    <span class="visually-hidden"
                                        >Loading...</span
                                    >
                                </div>
                                <div
                                    v-if="selected_schema_version"
                                    class="border-top mb-3 pt-2"
                                >
                                    <label
                                        for="schema-version"
                                        class="form-label"
                                        ><span class="fw-bold">Step 2: </span
                                        >Make sure your import file matches the
                                        schema:</label
                                    >
                                    <div class="table-responsive mb-2">
                                        <table
                                            class="table table-sm table-bordered custom-table"
                                        >
                                            <thead>
                                                <tr>
                                                    <th
                                                        v-for="column in selected_schema_version.columns"
                                                        :key="column.id"
                                                        scope="col"
                                                    >
                                                        {{
                                                            column.xlsx_column_header_name
                                                        }}
                                                        <span
                                                            v-if="
                                                                column.xlsx_data_validation_allow_blank ==
                                                                false
                                                            "
                                                            class="text-danger"
                                                            title="Mandatory Column"
                                                            >*</span
                                                        >
                                                    </th>
                                                </tr>
                                            </thead>
                                            <tbody>
                                                <tr>
                                                    <td
                                                        v-for="column in selected_schema_version.columns"
                                                        :key="column.id"
                                                    >
                                                        <i
                                                            class="bi bi-info-circle-fill text-primary"
                                                            role="button"
                                                            data-bs-toggle="popover"
                                                            data-bs-placement="bottom"
                                                            title="Column Information"
                                                            :data-bs-html="true"
                                                            :data-bs-content="
                                                                columnInformation(
                                                                    column
                                                                )
                                                            "
                                                        ></i>
                                                    </td>
                                                </tr>
                                            </tbody>
                                        </table>
                                    </div>
                                    <div>
                                        <a
                                            :href="`/api/occurrence_report_bulk_import_schemas/${selected_schema_version.id}/preview_import_file/?updated=${selected_schema_version.datetime_updated}`"
                                            class="btn btn-primary"
                                            target="_blank"
                                            ><i class="bi bi-filetype-xlsx"></i>
                                            Download Preview Bulk Import File</a
                                        >
                                    </div>
                                </div>
                                <div
                                    v-if="selected_schema_version"
                                    class="border-top w-75 mb-3 pt-2"
                                >
                                    <label
                                        for="bulk-import-file"
                                        class="form-label"
                                        ><span class="fw-bold">Step 3: </span>
                                        Select the .zip file containing any
                                        associated documents (Optional)</label
                                    >
                                    <div class="input-group">
                                        <input
                                            id="bulk-import-associated-files-zip"
                                            ref="bulk-import-associated-files-zip"
                                            type="file"
                                            class="form-control text-secondary"
                                            accept=".zip"
                                        />
                                    </div>
                                </div>
                                <div
                                    v-if="selected_schema_version"
                                    class="border-top w-75 mb-3 pt-2"
                                >
                                    <label
                                        for="bulk-import-file"
                                        class="form-label"
                                        ><span class="fw-bold">Step 4: </span>
                                        Select the bulk import file
                                        (.xlsx)</label
                                    >
                                    <div class="input-group">
                                        <input
                                            id="bulk-import-file"
                                            ref="bulk-import-file"
                                            type="file"
                                            class="form-control text-secondary"
                                            :class="
                                                importFileErrors
                                                    ? 'is-invalid'
                                                    : ''
                                            "
                                            accept="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                                            @change="bulkImportFileSelected"
                                        />
                                        <div
                                            v-if="importFileErrors"
                                            class="invalid-feedback mt-3"
                                        >
                                            <ul>
                                                <li
                                                    v-for="error in importFileErrors"
                                                    :key="error"
                                                >
                                                    {{ error }}
                                                </li>
                                            </ul>
                                        </div>
                                    </div>
                                </div>
                            </form>
                        </div>
                    </div>
                </div>
                <div
                    v-if="queuedImports && queuedImports.length > 0"
                    class="mb-3"
                >
                    <div class="card">
                        <div class="card-body">
                            <h5 class="card-title text-secondary mb-3">
                                Queued Imports
                            </h5>
                            <div class="table-responsive">
                                <table class="table table-sm">
                                    <thead>
                                        <tr>
                                            <th scope="col">
                                                <i
                                                    class="bi bi-clock text-secondary"
                                                ></i>
                                                Datetime Queued
                                            </th>
                                            <th scope="col">
                                                <i
                                                    class="bi bi-filetype-xlsx text-secondary"
                                                ></i>
                                                File Name
                                            </th>
                                            <th scope="col">
                                                <i
                                                    class="bi bi-hdd-fill text-secondary"
                                                ></i>
                                                File Size
                                            </th>
                                            <th scope="col">
                                                <i
                                                    class="bi bi-list-ol text-secondary"
                                                ></i>
                                                Row Count
                                            </th>
                                            <th scope="col">
                                                <i
                                                    class="bi bi-hourglass-split text-secondary"
                                                ></i>
                                                Time Estimate
                                            </th>
                                        </tr>
                                    </thead>
                                    <tbody class="text-muted">
                                        <tr
                                            v-for="queuedImport in queuedImports"
                                            :key="queuedImport.id"
                                        >
                                            <td>
                                                {{
                                                    new Date(
                                                        queuedImport.datetime_queued
                                                    ).toLocaleString()
                                                }}
                                            </td>
                                            <td
                                                class="text-truncate"
                                                style="max-width: 350px"
                                                :title="queuedImport.file_name"
                                            >
                                                {{ queuedImport.file_name }}
                                            </td>
                                            <td>
                                                {{
                                                    queuedImport.file_size_megabytes
                                                }}
                                                MB
                                            </td>
                                            <td class="text-end pe-3">
                                                {{
                                                    queuedImport.rows
                                                        ? queuedImport.rows
                                                        : 'Not Counted'
                                                }}
                                            </td>
                                            <td>
                                                {{
                                                    queuedImport.estimated_processing_time_human_readable
                                                }}
                                            </td>
                                        </tr>
                                    </tbody>
                                </table>
                            </div>
                        </div>
                    </div>
                </div>
                <div
                    v-if="
                        currentlyRunningImports &&
                        currentlyRunningImports.length > 0
                    "
                    class="mb-3"
                >
                    <div class="card currently-running">
                        <div class="card-body align-items-center">
                            <h5 class="card-title text-primary mb-3">
                                Currently Running Imports
                                <span
                                    class="spinner-border spinner-border-sm me-3"
                                    role="status"
                                    aria-hidden="true"
                                ></span>
                                <span class="visually-hidden">Loading...</span>
                            </h5>
                            <div class="table-responsive">
                                <table class="table table-sm">
                                    <thead>
                                        <tr>
                                            <th scope="col">
                                                <i
                                                    class="bi bi-clock text-primary"
                                                ></i>
                                                Datetime Started
                                            </th>
                                            <th scope="col">
                                                <i
                                                    class="bi bi-filetype-xlsx text-primary"
                                                ></i>
                                                File Name
                                            </th>
                                            <th scope="col">
                                                <i
                                                    class="bi bi-hdd-fill text-primary"
                                                ></i>
                                                File Size
                                            </th>
                                            <th scope="col">
                                                <i
                                                    class="bi bi-hourglass-split text-primary"
                                                ></i>
                                                Progress
                                            </th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        <tr
                                            v-for="currentlyRunningImport in currentlyRunningImports"
                                            :key="currentlyRunningImport.id"
                                        >
                                            <td>
                                                {{
                                                    new Date(
                                                        currentlyRunningImport.datetime_started
                                                    ).toLocaleString()
                                                }}
                                            </td>
                                            <td
                                                class="text-truncate"
                                                style="max-width: 350px"
                                                :title="
                                                    currentlyRunningImport.file_name
                                                "
                                            >
                                                {{
                                                    currentlyRunningImport.file_name
                                                }}
                                            </td>
                                            <td>
                                                {{
                                                    currentlyRunningImport.file_size_megabytes
                                                }}
                                                MB
                                            </td>
                                            <td style="min-width: 200px">
                                                <div class="progress">
                                                    <div
                                                        class="progress-bar progress-bar-striped progress-bar-animated"
                                                        role="progressbar"
                                                        :style="`width: ${currentlyRunningImport.percentage_complete}%;`"
                                                        :aria-valuenow="
                                                            currentlyRunningImport.percentage_complete
                                                        "
                                                        aria-valuemin="0"
                                                        aria-valuemax="100"
                                                    >
                                                        {{
                                                            currentlyRunningImport.percentage_complete
                                                        }}%
                                                    </div>
                                                </div>
                                            </td>
                                        </tr>
                                    </tbody>
                                </table>
                            </div>
                        </div>
                    </div>
                </div>
                <div
                    v-if="failedImports && failedImports.count > 0"
                    class="mb-3"
                >
                    <div class="card border-danger">
                        <div class="card-body">
                            <h5 class="card-title text-danger mb-3">
                                <i class="bi bi-list-checkme-2"></i>Failed Bulk
                                Imports
                            </h5>
                            <div class="table-responsive">
                                <table class="table table-sm">
                                    <thead>
                                        <tr>
                                            <th scope="col">
                                                Datetime Started
                                            </th>
                                            <th scope="col">File Name</th>
                                            <th scope="col">File Size</th>
                                            <th scope="col">Rows Attempted</th>
                                            <th scope="col">Actions</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        <tr
                                            v-for="failedImport in failedImports.results"
                                            :id="`failed-import-${failedImport.id}`"
                                            :key="failedImport.id"
                                        >
                                            <td>
                                                {{
                                                    new Date(
                                                        failedImport.datetime_started
                                                    ).toLocaleString()
                                                }}
                                            </td>
                                            <td
                                                class="text-truncate"
                                                style="max-width: 350px"
                                                :title="failedImport.file_name"
                                            >
                                                {{ failedImport.file_name }}
                                            </td>
                                            <td>
                                                {{
                                                    failedImport.file_size_megabytes
                                                }}
                                                MB
                                            </td>
                                            <td class="">
                                                {{
                                                    failedImport.rows
                                                        ? failedImport.rows
                                                        : 'Not Counted'
                                                }}
                                            </td>
                                            <td>
                                                <button
                                                    class="btn btn-sm btn-danger me-2"
                                                    data-bs-target="#errors-modal"
                                                    data-bs-toggle="modal"
                                                    @click="
                                                        selectedErrors =
                                                            failedImport.error_message
                                                    "
                                                >
                                                    <i class="bi bi-eye"></i>
                                                    View Errors
                                                </button>
                                            </td>
                                        </tr>
                                    </tbody>
                                </table>
                            </div>
                            <p
                                v-if="
                                    failedImports.count >
                                    failedImports.results.length
                                "
                                class="card-text text-center"
                            >
                                <a
                                    href=""
                                    @click.prevent="loadMoreFailedImports"
                                    >load more</a
                                >
                            </p>
                        </div>
                    </div>
                    <!-- Modal -->
                    <div
                        id="errors-modal"
                        class="modal fade"
                        data-bs-backdrop="static"
                        data-bs-keyboard="false"
                        tabindex="-1"
                        aria-labelledby="errors-modal-label"
                        aria-hidden="true"
                    >
                        <div class="modal-dialog modal-lg">
                            <div class="modal-content">
                                <div class="modal-header">
                                    <h5
                                        id="errors-modal-label"
                                        class="modal-title"
                                    >
                                        Bulk Import Task Errors
                                    </h5>
                                    <button
                                        type="button"
                                        class="btn-close"
                                        data-bs-dismiss="modal"
                                        aria-label="Close"
                                    ></button>
                                </div>
                                <div class="modal-body">
                                    <pre>{{ selectedErrors }}</pre>
                                </div>
                                <div class="modal-footer">
                                    <button
                                        type="button"
                                        class="btn btn-secondary"
                                        data-bs-dismiss="modal"
                                    >
                                        Dismiss
                                    </button>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
                <div
                    v-if="completedImports && completedImports.count > 0"
                    class="mb-3"
                >
                    <div class="card border-success">
                        <div class="card-body">
                            <h5 class="card-title text-success mb-3">
                                <i class="bi bi-list-checkme-2"></i>Completed
                                Bulk Imports
                            </h5>
                            <div class="table-responsive">
                                <table class="table table-sm">
                                    <thead>
                                        <tr>
                                            <th scope="col">
                                                <i
                                                    class="bi bi-clock-history text-success"
                                                ></i>
                                                Datetime Completed
                                            </th>
                                            <th scope="col">
                                                <i
                                                    class="bi bi-filetype-xlsx text-success"
                                                ></i>
                                                File Name
                                            </th>
                                            <th scope="col">
                                                <i
                                                    class="bi bi-list-ol text-success"
                                                ></i>
                                                Rows Processed
                                            </th>
                                            <th scope="col">
                                                <i
                                                    class="bi bi-hourglass-bottom text-success"
                                                ></i>
                                                Total Time Taken
                                            </th>
                                            <th scope="col">Actions</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        <tr
                                            v-for="completedImport in completedImports.results"
                                            :key="completedImport.id"
                                        >
                                            <td>
                                                {{
                                                    new Date(
                                                        completedImport.datetime_completed
                                                    ).toLocaleString()
                                                }}
                                            </td>
                                            <td
                                                class="text-truncate"
                                                style="max-width: 350px"
                                                :title="
                                                    completedImport.file_name
                                                "
                                            >
                                                {{ completedImport.file_name }}
                                            </td>
                                            <td class="">
                                                {{
                                                    completedImport.rows_processed
                                                }}
                                            </td>
                                            <td>
                                                {{
                                                    completedImport.total_time_taken_human_readable
                                                }}
                                            </td>
                                            <td>
                                                <button
                                                    class="btn btn-sm btn-primary"
                                                    @click.prevent="
                                                        revert(
                                                            completedImport.id
                                                        )
                                                    "
                                                >
                                                    <i
                                                        class="bi bi-arrow-counterclockwise"
                                                    ></i>
                                                    Revert &amp; Archive
                                                </button>
                                            </td>
                                        </tr>
                                    </tbody>
                                </table>
                            </div>
                            <p
                                v-if="
                                    completedImports.count >
                                    completedImports.results.length
                                "
                                class="card-text text-center"
                            >
                                <a
                                    href=""
                                    @click.prevent="loadMoreCompletedImports"
                                    >load more</a
                                >
                            </p>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>
</template>

<script>
import alert from '@vue-utils/alert.vue';

import { api_endpoints } from '@/utils/hooks.js';

export default {
    name: 'OccurrenceReportBulkImport',
    components: {
        alert,
    },
    data() {
        return {
            form: null,
            queuedImports: null,
            currentlyRunningImports: null,
            failedImports: null,
            failedImportsLimit: 3,
            completedImports: null,
            completedImportsLimit: 3,
            timer: null,
            currentlyRunningTimer: null,
            selectedErrors: '',
            schema_versions: null,
            selected_schema: null,
            selected_schema_version: null,
            importFileErrors: null,
        };
    },
    computed: {
        title: function () {
            return (
                this.$route.query.group_type +
                ' Occurrence Report Form Bulk Import'
            );
        },
    },
    created() {
        this.fetchSchemas();
        this.fetchImports();
        this.fetchCurrentlyRunningImports();
    },
    mounted() {
        this.form = document.getElementById('bulk-import-form');
        this.startPolling();
        document.addEventListener('visibilitychange', this.onVisibilityChange);
    },
    beforeUnmount() {
        clearInterval(this.timer);
        clearInterval(this.currentlyRunningTimer);
        document.removeEventListener(
            'visibilitychange',
            this.onVisibilityChange
        );
    },
    methods: {
        startPolling() {
            this.timer = setInterval(() => {
                this.fetchImports();
            }, 5000);
            this.currentlyRunningTimer = setInterval(() => {
                this.fetchCurrentlyRunningImports();
            }, 1000);
        },
        onVisibilityChange() {
            if (document.hidden) {
                clearInterval(this.timer);
                clearInterval(this.currentlyRunningTimer);
            } else {
                this.fetchImports();
                this.fetchCurrentlyRunningImports();
                this.startPolling();
            }
        },
        getSchemaVersionText(schema_version) {
            return `Version: ${schema_version.version} - ${schema_version.name ? schema_version.name : 'No Name'}`;
        },
        resetFileField() {
            fetch(
                `${api_endpoints.occurrence_report_bulk_import_schemas}${this.selected_schema.id}/`
            ).then(
                async (response) => {
                    this.selected_schema_version = await response.json();
                    this.$nextTick(() => {
                        this.importFileErrors = null;
                        this.form.classList.remove('was-validated');
                        // Enable all bootstrap 5 popovers
                        var popoverTriggerList = [].slice.call(
                            document.querySelectorAll(
                                '[data-bs-toggle="popover"]'
                            )
                        );
                        var myDefaultAllowList =
                            bootstrap.Tooltip.Default.allowList;
                        myDefaultAllowList.table = [];
                        myDefaultAllowList.tr = [];
                        myDefaultAllowList.td = [];
                        myDefaultAllowList.th = [];
                        myDefaultAllowList.thead = [];
                        myDefaultAllowList.tbody = [];
                        popoverTriggerList.map(function (popoverTriggerEl) {
                            return bootstrap.Popover.getOrCreateInstance(
                                popoverTriggerEl
                            );
                        });
                    });
                },
                (error) => {
                    console.log(error);
                }
            );
        },
        bulkImportFileSelected(event) {
            this.importFileErrors = null;
            this.form.classList.remove('was-validated');

            // If there is a file call the initial import file check api end point
            const file = event.target.files[0];
            const formData = new FormData();
            formData.append('_file', file);
            const associated_files =
                this.$refs['bulk-import-associated-files-zip'].files;
            if (associated_files.length > 0) {
                formData.append('_associated_files_zip', associated_files[0]);
            }
            formData.append('schema_id', this.selected_schema_version.id);

            fetch(api_endpoints.occurrence_report_bulk_imports, {
                method: 'POST',
                body: formData,
            }).then(
                async (response) => {
                    const data = await response.json();
                    if (response.status >= 200 && response.status < 300) {
                        console.log(data);
                        this.importFileErrors = null;
                        this.form.classList.remove('was-validated');
                        this.$refs['bulk-import-file'].value = '';
                        this.$refs['bulk-import-associated-files-zip'].value =
                            '';
                        swal.fire({
                            title: 'Bulk Import Added to Queue',
                            text: 'The bulk import of occurrence reports has been added to the queue for processing',
                            icon: 'success',
                            showCancelButton: false,
                            confirmButtonText: 'Ok',
                            customClass: {
                                confirmButton: 'btn btn-primary',
                            },
                        });
                    } else {
                        this.importFileErrors = data;
                        event.target.value = '';
                        this.$refs['bulk-import-associated-files-zip'].value =
                            '';
                        this.$refs['bulk-import-file'].setCustomValidity(
                            'Invalid field'
                        );
                        this.form.classList.add('was-validated');
                    }
                },
                (error) => {
                    this.importFileErrors = error.body;
                    event.target.value = '';
                    this.$refs['bulk-import-associated-files-zip'].value = '';
                    this.$refs['bulk-import-file'].setCustomValidity(
                        'Invalid field'
                    );
                    this.form.classList.add('was-validated');
                    console.log(error.body);
                }
            );
        },
        confirmBeginBulkImport() {
            swal.fire({
                title: 'Are you sure?',
                text: 'You are you want to queue this bulk import of occurrence reports',
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Queue Bulk Import',
                cancelButtonText: 'Cancel',
                customClass: {
                    confirmButton: 'btn btn-primary',
                    cancelButton: 'btn btn-secondary',
                },
                reverseButtons: true,
            }).then((result) => {
                if (result.isConfirmed) {
                    this.queueBulkImport();
                }
            });
        },
        fetchSchemas() {
            fetch(
                `${api_endpoints.occurrence_report_bulk_import_schemas_by_group_type}?group_type=${this.$route.query.group_type}`
            ).then(
                async (response) => {
                    this.schema_versions = await response.json();
                },
                (error) => {
                    console.log(error);
                }
            );
        },
        async fetchSchema() {},
        fetchQueuedImports() {
            fetch(
                `${api_endpoints.occurrence_report_bulk_imports}?processing_status=queued&schema__group_type__name=${this.$route.query.group_type}`
            ).then(
                async (response) => {
                    const data = await response.json();
                    this.queuedImports = data.results;
                },
                (error) => {
                    console.log(error);
                }
            );
        },
        fetchCurrentlyRunningImports() {
            fetch(
                `${api_endpoints.occurrence_report_bulk_imports}?processing_status=started&schema__group_type__name=${this.$route.query.group_type}`
            ).then(
                async (response) => {
                    const data = await response.json();
                    this.currentlyRunningImports = data.results;
                },
                (error) => {
                    console.log(error);
                }
            );
        },
        fetchFailedImports() {
            fetch(
                `${api_endpoints.occurrence_report_bulk_imports}?processing_status=failed&schema__group_type__name=${this.$route.query.group_type}&limit=${this.failedImportsLimit}&ordering=-datetime_started`
            ).then(
                async (response) => {
                    this.failedImports = await response.json();
                },
                (error) => {
                    console.log(error);
                }
            );
        },
        loadMoreFailedImports() {
            this.failedImportsLimit += 3;
            this.fetchFailedImports();
        },
        fetchCompletedImports() {
            fetch(
                `${api_endpoints.occurrence_report_bulk_imports}?processing_status=completed&schema__group_type__name=${this.$route.query.group_type}&limit=${this.completedImportsLimit}&ordering=-datetime_completed`
            ).then(
                async (response) => {
                    this.completedImports = await response.json();
                },
                (error) => {
                    console.log(error);
                }
            );
        },
        loadMoreCompletedImports() {
            this.completedImportsLimit += 3;
            this.fetchCompletedImports();
        },
        retryBulkImportTask(bulkImportTaskId) {
            // Call the api to retry the bulk import task
            fetch(
                `${api_endpoints.occurrence_report_bulk_imports}${bulkImportTaskId}/retry/`,
                {
                    method: 'PATCH',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                }
            ).then(
                (response) => {
                    console.log(response);
                    // Remove the failed import from the failed imports list
                    this.failedImports = this.failedImports.filter(
                        (failedImport) => {
                            return failedImport.id !== bulkImportTaskId;
                        }
                    );
                    this.fetchQueuedImports();
                },
                (error) => {
                    console.log(error);
                }
            );
        },
        revert(bulkImportTaskId) {
            swal.fire({
                title: 'Revert and Archive',
                text: 'Are you sure you want to revert this bulk import of occurrence reports and archive it?',
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Revert and Archive',
                cancelButtonText: 'Cancel',
                customClass: {
                    confirmButton: 'btn btn-primary',
                    cancelButton: 'btn btn-secondary',
                },
                reverseButtons: true,
            }).then((result) => {
                if (result.isConfirmed) {
                    // Call the api to revert the bulk import task
                    fetch(
                        `${api_endpoints.occurrence_report_bulk_imports}${bulkImportTaskId}/revert/`,
                        {
                            method: 'PATCH',
                            headers: {
                                'Content-Type': 'application/json',
                            },
                        }
                    ).then(
                        (response) => {
                            console.log(response);
                            // Remove the completed import from the completed imports list
                            this.completedImports =
                                this.completedImports.results.filter(
                                    (completedImport) => {
                                        return (
                                            completedImport.id !==
                                            bulkImportTaskId
                                        );
                                    }
                                );
                            this.fetchCompletedImports();
                        },
                        (error) => {
                            console.log(error);
                        }
                    );
                }
            });
        },
        fetchImports() {
            this.fetchQueuedImports();
            this.fetchFailedImports();
            this.fetchCompletedImports();
        },
        columnInformation(column) {
            let html = '<table class="table table-sm table-striped">';
            html += `<tr><th>Column Name</th><td>${column.xlsx_column_header_name}</td></tr>`;
            html += `<tr><th>Django Model Name</th><td class="text-capitalize">${column.model_name}</td></tr>`;
            html += `<tr><th>Django Field Name</th><td>${column.django_import_field_name}</td></tr>`;
            html += `<tr><th>Django Field Type</th><td>${column.field_type}</td></tr>`;
            html += `<tr><th>Allow Blank</th><td>${column.xlsx_data_validation_allow_blank ? 'Yes' : 'No'}</td></tr>`;
            if ('textLength' == column.xlsx_validation_type) {
                html += `<tr><th>Maximum Text Length</th><td>${column.text_length}</td></tr>`;
            }
            if (column.xlsx_validation_type) {
                html += `<tr><th>Validation Type${column.requires_lookup_field ? ' (Django)' : ` (.xlsx)`}</th><td>${column.xlsx_validation_type}</td></tr>`;
            }
            if ('list' == column.xlsx_validation_type) {
                if (column.choices && column.choices.length > 0) {
                    html += '<tr><th class="align-top">Choices</th><td>';
                    for (let choice of column.choices) {
                        html += `${choice[1]}<br>`;
                    }
                    html += '</td></tr>';
                } else {
                    if (column.requires_lookup_field) {
                        html += `<tr><th>Lookup Field</th><td>${column.django_lookup_field_name}</td></tr>`;
                        if (
                            column.lookup_filters &&
                            column.lookup_filters.length > 0
                        ) {
                            html += `<tr><th>Lookup Filters</th><td>`;
                            for (let lookupFilter of column.lookup_filters) {
                                html += `<div>Field: ${lookupFilter.filter_field_name}, Filter Type: ${lookupFilter.filter_type}, Value: ${lookupFilter.values[0].filter_value}</div>`;
                            }
                            html += '</td></tr>';
                        }
                        html += `<tr><th>Preview Choices (.xlsx)</th><td><a class="ms-0 ps-0" href="/api/occurrence_report_bulk_import_schema_columns/${column.id}/preview_foreign_key_values_xlsx/">Preview ${column.filtered_foreign_key_count} Choices</a></td></tr>`;
                    }
                }
            }
            html += '</table>';
            return html;
        },
    },
};
</script>
<style scoped>
table.custom-table th,
td {
    white-space: nowrap;
}

div.currently-running {
    border-color: rgba(34, 111, 187, 1);
    animation: border-pulsate 1s infinite;
}

@keyframes border-pulsate {
    0% {
        border-color: rgba(34, 111, 187, 1);
    }

    50% {
        border-color: rgba(34, 111, 187, 0);
    }

    100% {
        border-color: rgba(34, 111, 187, 1);
    }
}

option:disabled {
    color: #6c757d;
}
</style>
