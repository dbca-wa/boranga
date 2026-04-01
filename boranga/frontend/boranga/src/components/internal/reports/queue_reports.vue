<template>
    <div id="internal-reports" class="container pt-4">
        <h2 class="mb-4">Queue Reports</h2>
        <div class="alert alert-info">
            Email reports will be sent to the requesting user some time after
            the request has been made. Reports are processed every 2 minutes.
        </div>

        <div v-if="errorMessage" class="alert alert-danger">
            {{ errorMessage }}
        </div>

        <div class="card">
            <div class="card-body">
                <div class="row mb-3">
                    <label class="col-sm-3 col-form-label fw-bold"
                        >Report Type</label
                    >
                    <div class="col-sm-6">
                        <select v-model="selectedCategory" class="form-select">
                            <option value="">-- Select a report --</option>
                            <option
                                v-for="cat in reportCategories"
                                :key="cat.key"
                                :value="cat.key"
                            >
                                {{ cat.label }}
                            </option>
                        </select>
                    </div>
                </div>
                <template v-if="selectedCategory">
                    <div class="row mb-3">
                        <label class="col-sm-3 col-form-label fw-bold"
                            >Group Type</label
                        >
                        <div class="col-sm-6">
                            <select
                                v-model="selectedGroupType"
                                class="form-select"
                            >
                                <option
                                    v-for="gt in groupTypes"
                                    :key="gt.key"
                                    :value="gt.key"
                                >
                                    {{ gt.label }}
                                </option>
                            </select>
                        </div>
                    </div>
                    <div class="row mb-3">
                        <label class="col-sm-3 col-form-label fw-bold"
                            >Format</label
                        >
                        <div class="col-sm-6">
                            <select v-model="format" class="form-select">
                                <option value="excel">Excel</option>
                                <option value="csv">CSV</option>
                            </select>
                        </div>
                    </div>
                    <div class="row mb-3">
                        <label class="col-sm-3 col-form-label fw-bold"
                            >Max. Records</label
                        >
                        <div class="col-sm-6">
                            <input
                                v-model.number="numRecords"
                                type="number"
                                min="1"
                                max="500000"
                                class="form-control"
                            />
                        </div>
                    </div>
                    <div class="row">
                        <div class="col-sm-6 offset-sm-3">
                            <button
                                class="btn btn-primary"
                                :disabled="submitting"
                                @click="queueReport"
                            >
                                <i class="bi bi-envelope me-1"></i>
                                {{
                                    submitting
                                        ? 'Queuing...'
                                        : 'Generate Report'
                                }}
                            </button>
                        </div>
                    </div>
                </template>
            </div>
        </div>

        <div v-if="queueHistory && queueHistory.length > 0" class="card mt-4">
            <div class="card-body">
                <h5 class="card-title mb-3">Recent Queue Items</h5>
                <table class="table table-sm table-hover mb-0">
                    <thead>
                        <tr>
                            <th>Report Type</th>
                            <th>Format</th>
                            <th>Status</th>
                            <th>Queued</th>
                            <th>Processed</th>
                        </tr>
                    </thead>
                    <tbody>
                        <tr
                            v-for="job in queueHistory"
                            :key="job.id"
                            :class="rowClass(job)"
                        >
                            <td>{{ job.report_type }}</td>
                            <td>
                                <i
                                    :class="
                                        job.format === 'excel'
                                            ? 'bi bi-file-earmark-excel text-success'
                                            : 'bi bi-filetype-csv text-primary'
                                    "
                                ></i>
                                {{ job.format.toUpperCase() }}
                            </td>
                            <td>
                                <span v-if="job.status_id === 0">
                                    <i
                                        class="bi bi-clock text-secondary me-1"
                                    ></i>
                                    {{ job.status }}
                                </span>
                                <span v-else-if="job.status_id === 1">
                                    <span
                                        class="spinner-border spinner-border-sm text-primary me-1"
                                        role="status"
                                    ></span>
                                    {{ job.status }}
                                </span>
                                <span v-else-if="job.status_id === 2">
                                    <i
                                        class="bi bi-check-circle-fill text-success me-1"
                                    ></i>
                                    {{ job.status }}
                                </span>
                                <span v-else-if="job.status_id === 3">
                                    <i
                                        class="bi bi-x-circle-fill text-danger me-1"
                                    ></i>
                                    {{ job.status }}
                                    <i
                                        v-if="job.error_message"
                                        class="bi bi-info-circle text-danger ms-1 error-popover"
                                        role="button"
                                        tabindex="0"
                                        data-bs-toggle="popover"
                                        data-bs-trigger="hover focus"
                                        data-bs-placement="right"
                                        title="Error Details"
                                        :data-bs-content="job.error_message"
                                    ></i>
                                </span>
                            </td>
                            <td>{{ formatDate(job.created) }}</td>
                            <td>
                                {{
                                    job.processed_dt
                                        ? formatDate(job.processed_dt)
                                        : '-'
                                }}
                            </td>
                        </tr>
                    </tbody>
                </table>
            </div>
        </div>
    </div>
</template>

<script>
import swal from 'sweetalert2';

export default {
    name: 'QueueReports',
    data() {
        return {
            reportCategories: [],
            groupTypes: [],
            selectedCategory: '',
            selectedGroupType: 'all',
            format: 'csv',
            numRecords: 100000,
            submitting: false,
            errorMessage: '',
            queueHistory: null,
            pollTimer: null,
        };
    },
    mounted() {
        this.fetchReportConfig();
        this.fetchQueueHistory();
        this.pollTimer = setInterval(() => {
            this.fetchQueueHistory();
        }, 5000);
    },
    beforeUnmount() {
        clearInterval(this.pollTimer);
        this.disposePopovers();
    },
    updated() {
        this.initPopovers();
    },
    methods: {
        fetchReportConfig() {
            fetch('/api/queue_report/')
                .then((response) => response.json())
                .then((data) => {
                    this.reportCategories = data.report_categories || [];
                    this.groupTypes = data.group_types || [];
                })
                .catch(() => {
                    this.errorMessage = 'Failed to load report types.';
                });
        },
        fetchQueueHistory() {
            fetch('/api/queue_report_history/')
                .then((response) => response.json())
                .then((data) => {
                    this.queueHistory = data.results || [];
                })
                .catch(() => {});
        },
        queueReport() {
            this.submitting = true;
            this.errorMessage = '';

            fetch('/api/queue_report/', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    report_type: this.selectedCategory,
                    group_type: this.selectedGroupType,
                    format: this.format,
                    num_records: this.numRecords,
                }),
            })
                .then((response) => {
                    return response.json().then((data) => {
                        if (!response.ok) {
                            this.errorMessage =
                                data.message || 'An error occurred.';
                        } else if (data.message) {
                            swal.fire({
                                title: 'Report Queued',
                                text: data.message,
                                icon: 'success',
                                confirmButtonText: 'OK',
                                customClass: {
                                    confirmButton: 'btn btn-primary',
                                },
                            });
                        }
                        this.fetchQueueHistory();
                    });
                })
                .catch(() => {
                    this.errorMessage =
                        'An error occurred while queuing the report.';
                })
                .finally(() => {
                    this.submitting = false;
                });
        },
        formatDate(isoString) {
            return new Date(isoString).toLocaleString();
        },
        rowClass(job) {
            if (job.status_id === 1) return 'table-info';
            if (job.status_id === 3) return 'table-danger';
            return '';
        },
        initPopovers() {
            this.disposePopovers();
            this.$nextTick(() => {
                this.$el
                    .querySelectorAll('[data-bs-toggle="popover"]')
                    .forEach((el) => {
                        new window.bootstrap.Popover(el, { container: 'body' });
                    });
            });
        },
        disposePopovers() {
            this.$el
                ?.querySelectorAll('[data-bs-toggle="popover"]')
                .forEach((el) => {
                    const instance = window.bootstrap.Popover.getInstance(el);
                    if (instance) instance.dispose();
                });
        },
    },
};
</script>
