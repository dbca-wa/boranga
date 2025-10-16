<template>
    <div id="occurence-report-bulk-import-schema" class="container gx-4">
        <div class="row mb-2">
            <div class="col">
                <div class="mb-4">
                    <h2 class="text-capitalize">
                        Occurrence Report Form Bulk Import Schemas
                    </h2>
                </div>
                <div class="mb-3">
                    <div class="card">
                        <div class="card-body">
                            <form
                                id="bulk-import-form"
                                class="needs-validation"
                                no-validate
                            >
                                <div class="mb-3">
                                    <label
                                        for="schema-version"
                                        class="form-label"
                                        >Group Type</label
                                    >
                                    <select
                                        id="schema-version"
                                        ref="schema-version"
                                        v-model="groupType"
                                        class="form-select text-secondary w-25"
                                        aria-label="Select Schema Version"
                                        @change="fetchBulkSchemas"
                                    >
                                        <option :value="null" selected>
                                            Select Group Type
                                        </option>
                                        <option
                                            v-for="groupType in groupTypes"
                                            :value="groupType.id"
                                            :key="groupType.id"
                                        >
                                            {{
                                                groupType.name
                                                    .charAt(0)
                                                    .toUpperCase() +
                                                groupType.name.substring(1)
                                            }}
                                        </option>
                                    </select>
                                </div>
                            </form>
                            <div>
                                <button
                                    v-if="groupType"
                                    class="btn btn-primary float"
                                    @click="createNewVersion"
                                >
                                    <i class="bi bi-plus-circle-fill me-2"></i
                                    >Create New Version
                                </button>
                            </div>
                        </div>
                    </div>
                </div>
                <div class="mb-3">
                    <div
                        v-if="bulkSchemas && bulkSchemas.length > 0"
                        class="table-responsive"
                    >
                        <table class="table table-sm">
                            <thead>
                                <tr>
                                    <th scope="col" style="width: 80px">
                                        Version
                                    </th>
                                    <th scope="col" style="width: 250px">
                                        Name
                                    </th>
                                    <th scope="col" style="width: 260px">
                                        Tags
                                    </th>
                                    <th scope="col" style="width: 170px">
                                        Created
                                    </th>
                                    <th scope="col" style="width: 170px">
                                        Updated
                                    </th>
                                    <th scope="col">Actions</th>
                                </tr>
                            </thead>
                            <tbody class="text-muted">
                                <tr
                                    v-for="schema in bulkSchemas"
                                    :class="
                                        schema.is_master ? 'master-schema' : ''
                                    "
                                    :key="schema.id"
                                >
                                    <td>
                                        {{ schema.version
                                        }}<i
                                            v-if="schema.is_master"
                                            class="bi bi-lock-fill text-warning fs-5 ps-2"
                                            title="Master Schema"
                                        ></i>
                                        <i
                                            v-if="!schema.is_valid"
                                            class="bi bi-exclamation-circle-fill text-danger fs-5 ps-2"
                                            title="Schema contains models and/or fields no longer present in the underlying database"
                                        ></i>
                                    </td>
                                    <td
                                        class="text-truncate text-align-end"
                                        style="max-width: 285px"
                                        :title="schema.name"
                                    >
                                        {{ schema.name }}
                                    </td>
                                    <td class="text-truncate">
                                        <span
                                            v-for="tag in tags_to_show(
                                                schema.tags
                                            )"
                                            :key="tag"
                                            class="badge bg-info fs-6 me-2"
                                            >{{ tag }}</span
                                        >
                                        <span
                                            v-if="
                                                schema.tags.length >
                                                tags_to_show(schema.tags).length
                                            "
                                            :title="
                                                String(schema.tags).replace(
                                                    /,/g,
                                                    ', '
                                                )
                                            "
                                            @click.prevent=""
                                            >+{{
                                                schema.tags.length -
                                                tags_to_show(schema.tags).length
                                            }}</span
                                        >
                                    </td>
                                    <td>
                                        {{
                                            new Date(
                                                schema.datetime_created
                                            ).toLocaleDateString()
                                        }}
                                        {{
                                            new Date(
                                                schema.datetime_created
                                            ).toLocaleTimeString()
                                        }}
                                    </td>
                                    <td>
                                        {{
                                            new Date(
                                                schema.datetime_updated
                                            ).toLocaleDateString()
                                        }}
                                        {{
                                            new Date(
                                                schema.datetime_updated
                                            ).toLocaleTimeString()
                                        }}
                                    </td>
                                    <td>
                                        <a
                                            class="btn btn-sm btn-primary my-0 me-2"
                                            role="button"
                                            :href="`/internal/occurrence-report/bulk_import_schema/${schema.id}`"
                                            ><i
                                                class="bi me-2"
                                                :class="
                                                    schema.can_user_edit
                                                        ? 'bi-pencil-fill'
                                                        : 'bi-eye-fill'
                                                "
                                            ></i>
                                            <template
                                                v-if="schema.can_user_edit"
                                                >Edit</template
                                            ><template v-else>View</template></a
                                        >

                                        <button
                                            class="btn btn-sm btn-primary my-0"
                                            @click.prevent="
                                                copySchema(schema.id)
                                            "
                                        >
                                            <i class="bi bi-copy me-2"></i>
                                            Create a Copy
                                        </button>
                                    </td>
                                </tr>
                            </tbody>
                        </table>
                    </div>
                    <div v-else-if="loadingSchemas" class="text-center">
                        <div class="spinner-border text-primary" role="status">
                            <span class="visually-hidden">Loading...</span>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>
</template>

<script>
import { api_endpoints } from '@/utils/hooks.js';

export default {
    name: 'OccurrenceReportBulkImportSchemaList',
    data() {
        return {
            groupTypes: null,
            groupType: null,
            bulkSchemas: null,
            loadingSchemas: false,
        };
    },
    created() {
        this.fetchGroupTypes();
        if (localStorage.getItem('ocr-bulk-import-group-type')) {
            this.groupType = localStorage.getItem('ocr-bulk-import-group-type');
            this.fetchBulkSchemas();
        }
    },
    methods: {
        fetchGroupTypes() {
            fetch(api_endpoints.group_types_dict)
                .then(async (response) => {
                    this.groupTypes = await response.json();
                })
                .catch((error) => {
                    console.error(error);
                });
        },
        fetchBulkSchemas() {
            if (!this.groupType) {
                this.bulkSchemas = null;
                return;
            }
            localStorage.setItem('ocr-bulk-import-group-type', this.groupType);
            this.loadingSchemas = true;
            fetch(
                api_endpoints.occurrence_report_bulk_import_schemas +
                    '?' +
                    new URLSearchParams({
                        group_type: this.groupType,
                    }).toString()
            )
                .then(async (response) => {
                    const data = await response.json();
                    this.bulkSchemas = data.results;
                })
                .catch((error) => {
                    console.error(error);
                })
                .finally(() => {
                    this.loadingSchemas = false;
                });
        },
        copySchema(id) {
            fetch(
                `${api_endpoints.occurrence_report_bulk_import_schemas}${id}/copy/`,
                {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                }
            )
                .then(async (response) => {
                    const data = await response.json();
                    if (response.status === 201) {
                        swal.fire({
                            title: 'Success',
                            text: 'Schema copied successfully',
                            icon: 'success',
                            showConfirmButton: false,
                            timer: 1200,
                        });
                        this.$router.push(
                            `/internal/occurrence-report/bulk_import_schema/${data.id}`
                        );
                    }
                })
                .catch((error) => {
                    console.error(error);
                });
        },
        createNewVersion() {
            fetch(api_endpoints.occurrence_report_bulk_import_schemas, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    group_type: this.groupType,
                }),
            })
                .then(async (response) => {
                    const data = await response.json();
                    if (response.status === 201) {
                        swal.fire({
                            title: 'Success',
                            text: 'New version created successfully',
                            icon: 'success',
                            showConfirmButton: false,
                            timer: 1200,
                        });
                        this.$router.push(
                            `/internal/occurrence-report/bulk_import_schema/${data.id}`
                        );
                    }
                })
                .catch((error) => {
                    console.error(error);
                });
        },
        tags_to_show(tags) {
            let total_chars = tags.length * 2;
            let tags_to_show = [];
            for (let i = 0; i < tags.length; i++) {
                total_chars += tags[i].length;
                if (total_chars <= 30) {
                    tags_to_show.push(tags[i]);
                } else {
                    break;
                }
            }
            return tags_to_show;
        },
    },
};
</script>
<style scoped>
.master-schema {
    background-color: rgba(255, 193, 7, 0.3) !important;
}
</style>
