<template>
    <div id="occurence-report-bulk-import-schema" class="container gx-4">
        <div class="row mb-2">
            <div class="col">
                <div class="mb-4">
                    <h2 class="text-capitalize">
                        Occurrence Report Form Bulk Import Schema
                    </h2>
                </div>
                <div class="mb-3">
                    <alert type="primary">
                        <i
                            class="bi bi-info-circle-fill text-primary fs-4 help-text-popover me-2"
                        ></i>
                        Some help text about defining a bulk import schema
                    </alert>
                </div>
            </div>
        </div>
        <template v-if="schema">
            <div v-if="!schema.is_valid" class="row mb-2">
                <div class="col">
                    <alert type="danger">
                        <i
                            class="bi bi-exclamation-circle-fill text-danger fs-4 me-2"
                        ></i>
                        This schema is no longer valid.
                        <p class="mt-2">
                            You must resolve the following issues before it can
                            be used again:
                        </p>
                        <template
                            v-if="
                                schema.invalid_models &&
                                schema.invalid_models.length
                            "
                        >
                            <h6>
                                The following model(s) no longer exist in the
                                database and must be modified or removed:
                            </h6>
                            <ul>
                                <li
                                    v-for="invalidModel in schema.invalid_models"
                                    :key="invalidModel"
                                >
                                    {{ invalidModel }}
                                </li>
                            </ul>
                        </template>

                        <template
                            v-if="
                                schema.invalid_fields &&
                                schema.invalid_fields.length
                            "
                        >
                            <h6>
                                The following field(s) no longer exist in the
                                database and must be modified or removed:
                            </h6>
                            <ul>
                                <li
                                    v-for="invalidField in schema.invalid_fields"
                                    :key="invalidField"
                                >
                                    {{ invalidField }}
                                </li>
                            </ul>
                        </template>
                        <p>
                            <strong>Note:</strong> It is recommended that you
                            make a copy of the schema and make the changes to
                            the new copy.
                        </p>
                    </alert>
                </div>
            </div>
        </template>
        <div v-if="schema" class="row mb-3">
            <div class="col">
                <form>
                    <fieldset
                        id="main-schema-fieldset"
                        :class="!schema.can_user_edit ? 'disabled' : ''"
                    >
                        <div class="card">
                            <div class="card-body">
                                <div class="border-bottom pb-3 mb-3">
                                    <div class="row">
                                        <div class="col-5">
                                            <h4 class="text-capitalize">
                                                {{ schema.group_type_display }}
                                                Bulk Import Schema
                                                <span
                                                    class="badge bg-secondary ms-3"
                                                    >V{{ schema.version }}</span
                                                >
                                            </h4>
                                        </div>
                                        <div class="col-7">
                                            <div
                                                class="input-group float-start me-3"
                                                style="width: 250px"
                                            >
                                                <span
                                                    id="basic-addon1"
                                                    class="input-group-text"
                                                    ><i
                                                        class="bi bi-tag-fill text-secondary"
                                                    ></i
                                                ></span>
                                                <input
                                                    type="text"
                                                    class="form-control form-control-sm"
                                                    placeholder="Add tag"
                                                    @keydown="addTag"
                                                />
                                            </div>
                                            <div class="float-end">
                                                <button
                                                    id="copy-schema"
                                                    type="button"
                                                    class="btn btn-primary me-2"
                                                    :disabled="false"
                                                    @click.prevent="
                                                        copySchema(schema.id)
                                                    "
                                                >
                                                    <i
                                                        class="bi bi-copy me-2"
                                                    ></i>
                                                    Copy
                                                </button>
                                                <button
                                                    type="button"
                                                    class="btn btn-primary me-2"
                                                    :disabled="
                                                        validatingSchema ||
                                                        !schema.is_valid
                                                    "
                                                    @click.prevent="validate()"
                                                >
                                                    <span
                                                        v-if="validatingSchema"
                                                        class="spinner-border spinner-border-sm me-1"
                                                        role="status"
                                                        aria-hidden="true"
                                                    ></span
                                                    ><i
                                                        v-else
                                                        class="bi bi-card-checklist me-1"
                                                    ></i>
                                                    Validat<template
                                                        v-if="validatingSchema"
                                                        >ing</template
                                                    ><template v-else
                                                        >e</template
                                                    >
                                                </button>
                                                <a
                                                    role="button"
                                                    class="btn btn-primary"
                                                    :class="{
                                                        disabled:
                                                            !schema.is_valid,
                                                    }"
                                                    :aria-disabled="
                                                        (!schema.is_valid).toString()
                                                    "
                                                    :tabindex="
                                                        !schema.is_valid
                                                            ? -1
                                                            : 0
                                                    "
                                                    :href="`/api/occurrence_report_bulk_import_schemas/${schema.id}/preview_import_file/?updated=${schema.datetime_updated}`"
                                                    ><i
                                                        class="bi bi-filetype-xlsx me-1"
                                                    ></i>
                                                    Preview</a
                                                >
                                            </div>
                                        </div>
                                    </div>
                                </div>
                                <div class="border-bottom pb-3 mb-2">
                                    <div class="row align-items-center">
                                        <label
                                            for="schema-name"
                                            class="col-sm-1 col-form-label"
                                            >Name</label
                                        >
                                        <div class="col-sm-7">
                                            <input
                                                id="schema-name"
                                                ref="schema-name"
                                                v-model="schema.name"
                                                type="text"
                                                class="form-control"
                                                placeholder="Enter Schema Name"
                                                autofocus
                                            />
                                        </div>
                                    </div>
                                </div>
                                <div class="pb-0 mb-0">
                                    <div class="row align-items-center">
                                        <label
                                            for="schema-name"
                                            class="col-sm-2 col-form-label"
                                            >Master Schema<i
                                                v-if="schema.is_master"
                                                class="bi bi-lock-fill text-warning ms-2"
                                                title="This is a Master Schema. When copied by an Occurrence Approver, any columns from the master will be locked."
                                            ></i
                                        ></label>
                                        <div class="col-sm-1">
                                            <div
                                                class="form-check form-switch form-switch-lg"
                                                style="transform: scale(1.3)"
                                            >
                                                <input
                                                    id="is-master-schema"
                                                    v-model="schema.is_master"
                                                    class="form-check-input"
                                                    type="checkbox"
                                                    :disabled="
                                                        !schema.can_user_toggle_master
                                                    "
                                                    @change="save()"
                                                />
                                            </div>
                                        </div>
                                        <label
                                            for="schema-tags"
                                            class="col-sm-1 col-form-label"
                                            >Tags</label
                                        >
                                        <div class="col-sm-6">
                                            <template
                                                v-if="
                                                    schema.tags &&
                                                    schema.tags.length > 0
                                                "
                                            >
                                                <span
                                                    v-for="(
                                                        tag, index
                                                    ) in schema.tags"
                                                    :key="tag"
                                                    class="d-inline-flex align-items-center badge bg-info fs-6 my-2 me-2"
                                                    >{{ tag
                                                    }}<i
                                                        v-if="
                                                            schema.can_user_edit
                                                        "
                                                        class="bi bi-x-circle-fill ps-2"
                                                        role="button"
                                                        @click="
                                                            removeTag(index)
                                                        "
                                                    ></i
                                                ></span>
                                            </template>
                                            <template v-else>
                                                <span class="text-muted"
                                                    >No tags added</span
                                                >
                                            </template>
                                        </div>
                                    </div>
                                </div>
                                <div v-if="errors" class="mb-3">
                                    <alert type="danger">
                                        {{ errors }}
                                    </alert>
                                </div>
                            </div>
                        </div>
                    </fieldset>
                </form>
            </div>
        </div>
        <div v-if="schema" class="row">
            <div class="col-4">
                <div class="card">
                    <div class="card-body">
                        <h5 class="card-title border-bottom pb-3 mb-3">
                            <i class="bi bi-filetype-xlsx text-success me-3"></i
                            >Columns
                            <span class="text-muted ms-2 fs-6">(.xlsx)</span>
                        </h5>
                        <div class="">
                            <table
                                class="table table-hover table-sm table-schema"
                            >
                                <tbody class="columns">
                                    <tr
                                        v-for="(
                                            column, index
                                        ) in schema.columns"
                                        :key="column.id"
                                        :class="
                                            classesForRow(
                                                index,
                                                column,
                                                selectedColumn
                                            )
                                        "
                                        :style="
                                            selectedColumn == column
                                                ? ''
                                                : `background-color: ${stringToRGBAColor(column.model_name)} !important;`
                                        "
                                        role="button"
                                        @click="selectColumn(column)"
                                    >
                                        <td
                                            style="width: 5%"
                                            class="text-center fw-bold"
                                            :class="
                                                selectedColumn == column
                                                    ? 'text-light'
                                                    : ''
                                            "
                                        >
                                            {{ index + 1 }}
                                        </td>
                                        <td
                                            class="ps-3 fw-bold"
                                            style="font-size: 0.9em; width: 40%"
                                            :class="
                                                selectedColumn == column
                                                    ? 'py-1'
                                                    : ''
                                            "
                                        >
                                            <span
                                                class="text-truncate pe-1"
                                                :class="
                                                    selectedColumn == column
                                                        ? 'text-light'
                                                        : ''
                                                "
                                                :title="
                                                    column.xlsx_column_header_name
                                                "
                                                >{{
                                                    column
                                                        .xlsx_column_header_name
                                                        .length < 34
                                                        ? column.xlsx_column_header_name
                                                        : column.xlsx_column_header_name.substr(
                                                              0,
                                                              31
                                                          ) + '...'
                                                }}</span
                                            >
                                            <span
                                                v-if="
                                                    column.xlsx_data_validation_allow_blank ==
                                                    false
                                                "
                                                class="text-danger"
                                                title="Mandatory Column"
                                                >*</span
                                            >
                                            <i
                                                v-if="
                                                    column.field_exists ===
                                                    false
                                                "
                                                class="bi bi-exclamation-circle-fill text-danger"
                                                title="This column is no longer present in the underlying database"
                                            ></i>
                                            <small
                                                class="d-block text-capitalize mb-0"
                                                :class="
                                                    selectedColumn == column
                                                        ? 'text-light'
                                                        : 'text-muted'
                                                "
                                            >
                                                <span
                                                    :class="
                                                        column.model_exists ===
                                                        false
                                                            ? 'text-danger'
                                                            : ''
                                                    "
                                                    :title="
                                                        column.model_exists ===
                                                        false
                                                            ? `Model ${column.model_name} no longer exists in the database`
                                                            : ''
                                                    "
                                                >
                                                    {{ column.model_name }}
                                                </span>
                                                <i
                                                    v-if="
                                                        column.model_exists ===
                                                        false
                                                    "
                                                    class="bi bi-exclamation-circle-fill text-danger ps-1"
                                                    title="This model is no longer present in the underlying database"
                                                ></i>
                                            </small>
                                        </td>
                                        <td
                                            class="text-center"
                                            :class="
                                                selectedColumn == column
                                                    ? 'text-light'
                                                    : 'text-muted'
                                            "
                                            style="width: 10%"
                                        >
                                            <i
                                                class="bi bi-eye-fill"
                                                role="button"
                                                :class="
                                                    schema.can_user_edit &&
                                                    column.is_editable_by_user &&
                                                    (index == 0 ||
                                                        schema.columns.length <=
                                                            2 ||
                                                        !column.id)
                                                        ? 'me-4'
                                                        : 'me-2'
                                                "
                                            ></i>
                                            <i
                                                v-if="
                                                    schema.can_user_edit &&
                                                    column.is_editable_by_user &&
                                                    !index == 0 &&
                                                    schema.columns.length > 2 &&
                                                    column.id
                                                "
                                                class="bi bi-arrow-down-up"
                                                role="button"
                                                style="cursor: move"
                                            ></i>
                                            <i
                                                v-if="
                                                    column.id &&
                                                    !column.is_editable_by_user
                                                "
                                                class="bi bi-lock-fill"
                                                :title="`Column ${column.xlsx_column_header_name} is locked as it was copied from a master schema`"
                                            ></i>
                                        </td>
                                    </tr>
                                    <tr
                                        v-if="schema.can_user_edit"
                                        class="border-bottom-0"
                                        :class="
                                            schema.columns.length == 0
                                                ? 'border-top-0'
                                                : ''
                                        "
                                        role="button"
                                        @click.prevent="addNewColumn"
                                    >
                                        <th
                                            class="border-0 ps-3 pt-2 text-muted"
                                            colspan="2"
                                        >
                                            Add
                                            <template
                                                v-if="
                                                    schema.columns.length == 0
                                                "
                                                >a</template
                                            ><template v-else>Another</template>
                                            Column
                                        </th>
                                        <td
                                            class="border-0 text-muted text-center pt-2"
                                        >
                                            <i
                                                class="bi bi-plus-circle-fill text-success"
                                                role="button"
                                            ></i>
                                        </td>
                                    </tr>
                                </tbody>
                            </table>
                        </div>
                    </div>
                </div>
            </div>
            <div class="col-8">
                <div
                    v-if="addEditMode && selectedColumn"
                    class="card sticky-top"
                >
                    <div class="card-body">
                        <h5 class="card-title border-bottom pb-3 mb-3">
                            Selected Column Details
                        </h5>
                        <form
                            class="needs-validation"
                            novalidate
                            @submit.prevent=""
                        >
                            <fieldset
                                :disabled="
                                    !schema.can_user_edit ||
                                    !selectedColumn.is_editable_by_user
                                "
                            >
                                <div
                                    v-if="selectedColumn.model_exists === false"
                                    class="row d-flex align-items-start mb-2"
                                >
                                    <label
                                        for="model-no-longer-exists"
                                        class="col-sm-4 col-form-label"
                                        >Invalid Django Import Model
                                        <i
                                            class="fas fa-exclamation-circle text-danger"
                                            title="This model no longer exists in the underlying database"
                                        ></i
                                    ></label>
                                    <div class="col-sm-8">
                                        <input
                                            class="form-control form-control-sm text-danger"
                                            type="text"
                                            disabled
                                            :value="`${selectedColumn.invalid_model} - No longer exists in the database`"
                                        />
                                        <div class="pt-2">
                                            <small class="text-danger">
                                                Please select a new model from
                                                the dropdown below or delete
                                                this column
                                            </small>
                                        </div>
                                    </div>
                                </div>
                                <div class="row d-flex align-items-center mb-2">
                                    <label
                                        for="inputEmail3"
                                        class="col-sm-4 col-form-label"
                                        >Django Import Model</label
                                    >
                                    <div class="col-sm-8">
                                        <select
                                            ref="django-import-model"
                                            v-model="
                                                selectedColumn.django_import_content_type
                                            "
                                            class="form-select form-select-sm"
                                            aria-label="Django Import Model"
                                            :disabled="
                                                selectedColumn.order == 0
                                            "
                                            @change="
                                                selectDjangoImportContentType
                                            "
                                        >
                                            <option value="">
                                                Select Django Import Model
                                            </option>
                                            <template v-if="selectedColumn.id">
                                                <option
                                                    v-for="djangoContentType in djangoContentTypes"
                                                    :key="djangoContentType.id"
                                                    :value="
                                                        djangoContentType.id
                                                    "
                                                >
                                                    {{
                                                        djangoContentType.model_verbose_name
                                                            ? djangoContentType.model_verbose_name
                                                            : djangoContentType.model
                                                    }}
                                                </option>
                                            </template>
                                            <template v-else>
                                                <option
                                                    v-for="djangoContentType in djangoContentTypesFiltered"
                                                    :value="
                                                        djangoContentType.id
                                                    "
                                                    :key="djangoContentType.id"
                                                >
                                                    {{
                                                        djangoContentType.model_verbose_name
                                                            ? djangoContentType.model_verbose_name
                                                            : djangoContentType.model
                                                    }}
                                                </option>
                                            </template>
                                        </select>
                                    </div>
                                </div>
                                <template
                                    v-if="selectedColumn.model_exists !== false"
                                >
                                    <div
                                        v-if="
                                            selectedContentType &&
                                            !selectedColumn?.id &&
                                            !showDjangoImportFieldSelect
                                        "
                                        class="row d-flex align-items-start mb-2"
                                    >
                                        <label
                                            for="inputEmail3"
                                            class="col-sm-4 col-form-label"
                                            >Operation</label
                                        >
                                        <div class="col-sm-8">
                                            <button
                                                type="button"
                                                class="btn btn-primary btn-sm d-block mb-2"
                                                @click.prevent="
                                                    addAllColumns(false)
                                                "
                                            >
                                                <i
                                                    class="bi bi-plus-circle-fill me-1"
                                                ></i>
                                                Add All Fields
                                            </button>
                                            <button
                                                type="button"
                                                class="btn btn-primary btn-sm me-2 d-block mb-2"
                                                @click.prevent="
                                                    addAllColumns(true)
                                                "
                                            >
                                                <i
                                                    class="bi bi-plus-circle-fill me-1"
                                                ></i>
                                                Add All Mandatory Fields
                                            </button>
                                            <button
                                                type="button"
                                                class="btn btn-primary btn-sm d-block"
                                                @click.prevent="
                                                    addSingleColumn()
                                                "
                                            >
                                                <i
                                                    class="bi bi-plus-circle-fill me-1"
                                                ></i>
                                                Add a Single Field
                                            </button>
                                        </div>
                                    </div>
                                    <div
                                        v-if="
                                            selectedColumn.field_exists ===
                                            false
                                        "
                                        class="row d-flex align-items-center mb-2"
                                    >
                                        <label
                                            for="inputEmail3"
                                            class="col-sm-4 col-form-label"
                                            >Invalid Django Import Field
                                            <i
                                                class="fas fa-exclamation-circle text-danger"
                                                title="This field no longer exists in the underlying database"
                                            ></i
                                        ></label>
                                        <div class="col-sm-8">
                                            <input
                                                class="form-control form-control-sm text-danger"
                                                type="text"
                                                disabled
                                                :value="`${selectedColumn.invalid_field} - No longer exists in the database`"
                                            />
                                            <div class="pt-2">
                                                <small class="text-danger">
                                                    Please select a new field
                                                    from the dropdown below or
                                                    delete this column
                                                </small>
                                            </div>
                                        </div>
                                    </div>
                                    <div
                                        v-if="showDjangoImportFieldSelect"
                                        class="row d-flex align-items-center mb-2"
                                    >
                                        <label
                                            for="inputEmail3"
                                            class="col-sm-4 col-form-label"
                                            >Django Import Field</label
                                        >
                                        <div class="col-sm-8">
                                            <select
                                                ref="django-import-field"
                                                v-model="
                                                    selectedColumn.django_import_field_name
                                                "
                                                class="form-select form-select-sm"
                                                aria-label="Django Import Field"
                                                :disabled="
                                                    selectedColumn.order == 0
                                                "
                                                @change="
                                                    selectDjangoImportField
                                                "
                                            >
                                                <option value="">
                                                    Select Django Import Field
                                                </option>
                                                <template
                                                    v-if="selectedColumn.id"
                                                >
                                                    <option
                                                        v-for="modelField in selectedContentType.model_fields"
                                                        :value="modelField.name"
                                                        :key="modelField.name"
                                                    >
                                                        {{
                                                            modelField.display_name
                                                        }}
                                                        ({{ modelField.type }})
                                                        <template
                                                            v-if="
                                                                !modelField.allow_null
                                                            "
                                                            >*</template
                                                        >
                                                    </option>
                                                </template>
                                                <template v-else>
                                                    <option
                                                        v-for="modelField in djangoImportFieldsFiltered"
                                                        :value="modelField.name"
                                                        :key="modelField.name"
                                                    >
                                                        {{
                                                            modelField.display_name
                                                        }}
                                                        ({{ modelField.type }})
                                                        <template
                                                            v-if="
                                                                !modelField.allow_null
                                                            "
                                                            >*</template
                                                        >
                                                    </option>
                                                </template>
                                            </select>
                                        </div>
                                    </div>
                                    <template v-if="selectedField">
                                        <div
                                            class="row d-flex align-items-center mb-2"
                                        >
                                            <label
                                                for="column-name"
                                                class="col-sm-4 col-form-label"
                                                >Column Name</label
                                            >
                                            <div class="col-sm-8">
                                                <div
                                                    class="input-group input-group-sm"
                                                >
                                                    <input
                                                        id="column-name"
                                                        ref="column-name"
                                                        v-model="
                                                            selectedColumn.xlsx_column_header_name
                                                        "
                                                        type="text"
                                                        class="form-control"
                                                        name="column-name"
                                                    />
                                                </div>
                                            </div>
                                        </div>
                                        <div
                                            v-if="
                                                selectedField.type ==
                                                    'IntegerField' &&
                                                defaultValueChoices &&
                                                defaultValueChoices.length > 0
                                            "
                                            class="row d-flex align-items-center mb-2"
                                        >
                                            <label
                                                for="default-value"
                                                class="col-sm-4 col-form-label"
                                                >Default Value</label
                                            >
                                            <div class="col-sm-8">
                                                <div
                                                    class="input-group input-group-sm"
                                                >
                                                    <select
                                                        id="default-value"
                                                        v-model="
                                                            selectedColumn.default_value
                                                        "
                                                        class="form-select"
                                                    >
                                                        <option :value="null">
                                                            No Default
                                                        </option>
                                                        <option
                                                            v-for="choice in defaultValueChoices"
                                                            :value="choice[0]"
                                                            :key="choice[0]"
                                                        >
                                                            {{ choice[1] }}
                                                        </option>
                                                    </select>
                                                </div>
                                            </div>
                                        </div>
                                        <div
                                            v-if="
                                                selectedField.type ==
                                                    'IntegerField' &&
                                                !selectedColumn.default_value
                                            "
                                            class="row d-flex align-items-center mb-2"
                                        >
                                            <label
                                                for="default-value"
                                                class="col-sm-4 col-form-label"
                                                >Ledger Email User Column?<i
                                                    id="is-email-user-column-help-text"
                                                    class="bi bi-info-circle-fill text-primary ms-2"
                                                    data-bs-toggle="popover"
                                                    data-bs-trigger="hover focus"
                                                    data-bs-content="If set to 'Yes' the column value will be validated as an email address during the import and  the email will be used to lookup the user id in ledger"
                                                    data-bs-placement="top"
                                                ></i
                                            ></label>
                                            <div class="col-sm-8">
                                                <div
                                                    class="input-group input-group-sm"
                                                >
                                                    <select
                                                        v-model="
                                                            selectedColumn.is_emailuser_column
                                                        "
                                                        class="form-select w-50"
                                                        aria-label="Allow Blank"
                                                    >
                                                        <option :value="true">
                                                            Yes
                                                        </option>
                                                        <option :value="false">
                                                            No
                                                        </option>
                                                    </select>
                                                </div>
                                            </div>
                                        </div>
                                        <template
                                            v-if="selectedColumn.is_valid"
                                        >
                                            <div
                                                class="row d-flex align-items-center mb-2"
                                            >
                                                <label
                                                    for="inputEmail3"
                                                    class="col-sm-4 col-form-label"
                                                    >Django Field Type</label
                                                >
                                                <div class="col-sm-8">
                                                    <div
                                                        class="input-group input-group-sm"
                                                    >
                                                        <input
                                                            id=""
                                                            type="text"
                                                            class="form-control"
                                                            name=""
                                                            placeholder=""
                                                            disabled
                                                            :value="
                                                                selectedField.type
                                                            "
                                                        />
                                                    </div>
                                                </div>
                                            </div>
                                            <div
                                                class="row d-flex align-items-center mb-2"
                                            >
                                                <label
                                                    for="xlsx-validation-type"
                                                    class="col-sm-4 col-form-label"
                                                    >Excel Validation
                                                    Type</label
                                                >
                                                <div class="col-sm-8">
                                                    <div
                                                        class="input-group input-group-sm"
                                                    >
                                                        <input
                                                            id="xlsx-validation-type"
                                                            type="text"
                                                            class="form-control form-control-sm"
                                                            name="xlsx-validation-type"
                                                            disabled
                                                            :value="
                                                                excelValidationType()
                                                            "
                                                        />
                                                        <template
                                                            v-if="
                                                                selectedField.xlsx_validation_type ==
                                                                'date'
                                                            "
                                                        >
                                                            <span
                                                                id="datetime-format"
                                                                class="input-group-text"
                                                                >Format:</span
                                                            >
                                                            <span
                                                                id="datetime-format-mask"
                                                                class="input-group-text"
                                                                >DD/MM/YYYY
                                                                HH:MM:SS</span
                                                            >
                                                        </template>
                                                        <template
                                                            v-if="
                                                                selectedField.xlsx_validation_type ==
                                                                'list'
                                                            "
                                                        >
                                                            <button
                                                                v-if="
                                                                    selectedField.choices &&
                                                                    selectedField
                                                                        .choices
                                                                        .length >
                                                                        0
                                                                "
                                                                type="button"
                                                                class="btn btn-primary"
                                                                data-bs-toggle="modal"
                                                                data-bs-target="#preview-choices"
                                                            >
                                                                <i
                                                                    class="bi bi-search"
                                                                    role="button"
                                                                ></i>
                                                                Preview List
                                                                Choices
                                                            </button>
                                                        </template>
                                                    </div>
                                                </div>
                                            </div>
                                            <div
                                                v-if="
                                                    selectedField.xlsx_validation_type
                                                "
                                                class="row mb-2"
                                            >
                                                <label
                                                    for="inputEmail3"
                                                    class="col-sm-4 col-form-label"
                                                    >Excel Data Validations<i
                                                        id="pre-import-validation-help-text"
                                                        class="bi bi-info-circle-fill text-primary ms-2"
                                                        data-bs-toggle="popover"
                                                        data-bs-trigger="hover focus"
                                                        data-bs-content="Basic validations embedded in the .xlsx file"
                                                        data-bs-placement="top"
                                                    ></i
                                                ></label>
                                                <div class="col-sm-4">
                                                    <div
                                                        class="input-group input-group-sm mb-2"
                                                    >
                                                        <span
                                                            id="allow-blank-label"
                                                            class="input-group-text"
                                                            >Allow Blank</span
                                                        >
                                                        <select
                                                            v-model="
                                                                selectedColumn.xlsx_data_validation_allow_blank
                                                            "
                                                            class="form-select w-50"
                                                            aria-label="Allow Blank"
                                                            :disabled="
                                                                !selectedField.allow_null &&
                                                                selectedField.name !=
                                                                    'occurrence_number'
                                                            "
                                                        >
                                                            <option
                                                                :value="true"
                                                            >
                                                                Yes
                                                            </option>
                                                            <option
                                                                :value="false"
                                                            >
                                                                No
                                                            </option>
                                                        </select>
                                                    </div>
                                                    <div
                                                        class="input-group input-group-sm mb-2"
                                                    >
                                                        <span
                                                            id="max-length"
                                                            class="input-group-text"
                                                            >Max Length</span
                                                        >
                                                        <input
                                                            type="text"
                                                            class="form-control"
                                                            aria-label="Max Length"
                                                            aria-describedby="max-length"
                                                            :value="
                                                                selectedField.max_length
                                                                    ? selectedField.max_length
                                                                    : 'N/A'
                                                            "
                                                            disabled
                                                        />
                                                    </div>
                                                </div>
                                            </div>
                                        </template>
                                    </template>
                                </template>
                                <div
                                    v-if="
                                        selectedColumn.is_valid &&
                                        showDjangoLookupField
                                    "
                                    class="row mb-2"
                                >
                                    <label
                                        for="inputEmail3"
                                        class="col-sm-4 col-form-label"
                                        >Django Lookup Field<i
                                            id="django-lookup-field-help-text"
                                            class="bi bi-info-circle-fill text-primary ms-2"
                                            data-bs-toggle="popover"
                                            data-bs-trigger="hover focus"
                                            :data-bs-content="`Which field to use when looking up the ${selectedField.display_name} record. <br>(Advanced: You can use a custom field name if it's not in the list and it may span model relationships e.g. 'taxonomy__scientific_name' )`"
                                            data-bs-placement="top"
                                            :data-bs-html="true"
                                        ></i
                                    ></label>
                                    <div class="col-sm-8">
                                        <fieldset class="border p-2 mb-2">
                                            <div
                                                class="input-group input-group-sm mb-1"
                                            >
                                                <span
                                                    id="lookup-field"
                                                    class="input-group-text w-25"
                                                >
                                                    Lookup Field</span
                                                >
                                                <template
                                                    v-if="!customLookupField"
                                                >
                                                    <select
                                                        v-model="
                                                            selectedColumn.django_lookup_field_name
                                                        "
                                                        class="form-select"
                                                        aria-label="Lookup Field"
                                                        @change="
                                                            selectLookupField()
                                                        "
                                                    >
                                                        <option
                                                            v-for="lookupField in selectedField.lookup_field_options"
                                                            :value="lookupField"
                                                            :key="lookupField"
                                                        >
                                                            {{ lookupField }}
                                                        </option>
                                                        <option value="custom">
                                                            custom
                                                        </option>
                                                    </select>
                                                    <a
                                                        v-if="selectedColumn.id"
                                                        class="btn btn-primary"
                                                        :href="`/api/occurrence_report_bulk_import_schema_columns/${selectedColumn.id}/preview_foreign_key_values_xlsx/`"
                                                        ><i
                                                            class="bi bi-filetype-xlsx"
                                                            role="button"
                                                        ></i>
                                                        Preview Choices</a
                                                    >
                                                </template>
                                                <select
                                                    v-else
                                                    ref="lookup-field"
                                                    class="form-select"
                                                    aria-label="Lookup Field"
                                                    @change="
                                                        selectLookupField()
                                                    "
                                                >
                                                    <option
                                                        v-for="lookupField in selectedField.lookup_field_options"
                                                        :value="lookupField"
                                                        :key="lookupField"
                                                    >
                                                        {{ lookupField }}
                                                    </option>
                                                    <option
                                                        value="custom"
                                                        selected
                                                    >
                                                        custom
                                                    </option>
                                                </select>
                                            </div>
                                            <div
                                                v-if="customLookupField"
                                                class="input-group input-group-sm mb-2 mt-2"
                                            >
                                                <input
                                                    ref="custom-lookup-field"
                                                    v-model="
                                                        selectedColumn.django_lookup_field_name
                                                    "
                                                    class="form-control form-control-sm"
                                                    type="text"
                                                    placeholder="Enter Custom Lookup Field"
                                                    aria-label="Custom Lookup Field"
                                                />
                                                <a
                                                    v-if="selectedColumn.id"
                                                    class="btn btn-primary"
                                                    :href="`/api/occurrence_report_bulk_import_schema_columns/${selectedColumn.id}/preview_foreign_key_values_xlsx/`"
                                                    ><i
                                                        class="bi bi-filetype-xlsx"
                                                        role="button"
                                                    ></i>
                                                    Preview
                                                    {{
                                                        selectedColumn.filtered_foreign_key_count
                                                    }}
                                                    Choices</a
                                                >
                                            </div>
                                            <div>
                                                <button
                                                    type="button"
                                                    class="btn btn-primary btn-sm mb-2"
                                                    @click.prevent="
                                                        addLookupFilter()
                                                    "
                                                >
                                                    <i
                                                        class="bi bi-plus-circle-fill me-2"
                                                    ></i
                                                    >Add Lookup Filter
                                                </button>
                                                <template
                                                    v-if="
                                                        selectedColumn.lookup_filters &&
                                                        selectedColumn
                                                            .lookup_filters
                                                            .length > 0
                                                    "
                                                >
                                                    <div
                                                        v-for="(
                                                            lookupFilter, index
                                                        ) in selectedColumn.lookup_filters"
                                                        :key="index"
                                                        class="input-group input-group-sm mb-2"
                                                    >
                                                        <select
                                                            v-model="
                                                                selectedColumn
                                                                    .lookup_filters[
                                                                    index
                                                                ]
                                                                    .filter_field_name
                                                            "
                                                            class="form-select"
                                                            @change="
                                                                lookupFilterFieldChanged(
                                                                    selectedColumn
                                                                        .lookup_filters[
                                                                        index
                                                                    ]
                                                                )
                                                            "
                                                        >
                                                            <option
                                                                value=""
                                                                disabled
                                                            >
                                                                Select Field
                                                            </option>
                                                            <option
                                                                v-for="field in selectedField.filter_field_options"
                                                                :value="field"
                                                                :key="field"
                                                            >
                                                                {{ field }}
                                                            </option>
                                                        </select>
                                                        <select
                                                            v-model="
                                                                selectedColumn
                                                                    .lookup_filters[
                                                                    index
                                                                ].filter_type
                                                            "
                                                            class="form-select"
                                                        >
                                                            <option
                                                                v-for="lookupFilter in lookupSchematypes"
                                                                :value="
                                                                    lookupFilter[0]
                                                                "
                                                                :key="
                                                                    lookupFilter[0]
                                                                "
                                                            >
                                                                {{
                                                                    lookupFilter[1]
                                                                }}
                                                            </option>
                                                        </select>
                                                        <input
                                                            v-if="
                                                                selectedColumn
                                                                    .lookup_filters[
                                                                    index
                                                                ].values &&
                                                                selectedColumn
                                                                    .lookup_filters[
                                                                    index
                                                                ].values
                                                                    .length > 0
                                                            "
                                                            v-model="
                                                                selectedColumn
                                                                    .lookup_filters[
                                                                    index
                                                                ].values[0]
                                                                    .filter_value
                                                            "
                                                            type="text"
                                                            class="form-control"
                                                            placeholder="Enter Value"
                                                            aria-label="Enter Value"
                                                            @change="
                                                                lookupFilterValueChanged(
                                                                    $event,
                                                                    selectedColumn
                                                                        .lookup_filters[
                                                                        index
                                                                    ]
                                                                )
                                                            "
                                                        />
                                                        <input
                                                            v-else
                                                            type="text"
                                                            class="form-control"
                                                            placeholder="Select Field First"
                                                            disabled
                                                        />

                                                        <button
                                                            type="button"
                                                            class="btn btn-danger"
                                                            @click.prevent="
                                                                removeLookupFilter(
                                                                    selectedColumn,
                                                                    index
                                                                )
                                                            "
                                                        >
                                                            <i
                                                                class="bi bi-x-circle-fill"
                                                            ></i>
                                                        </button>
                                                    </div>
                                                </template>
                                            </div>
                                        </fieldset>
                                    </div>
                                </div>
                            </fieldset>

                            <div
                                class="border-top pt-3 d-flex justify-content-end"
                            >
                                <button
                                    v-if="selectedColumn.is_valid"
                                    class="btn btn-primary btn-sm me-2"
                                    @click.prevent="selectedColumn = null"
                                >
                                    <i class="bi bi-x-square me-1"></i> Unselect
                                    Column
                                </button>
                                <template
                                    v-if="
                                        schema.can_user_edit &&
                                        selectedColumn.is_editable_by_user
                                    "
                                >
                                    <button
                                        v-if="
                                            selectedColumn.django_import_content_type &&
                                            selectedColumn.django_import_content_type &&
                                            selectedColumn.django_import_field_name
                                        "
                                        class="btn btn-primary btn-sm me-2"
                                        :disabled="saving"
                                        @click.prevent="save()"
                                    >
                                        <i class="bi bi-floppy-fill me-1"></i>
                                        Save Column
                                        <template v-if="saving"
                                            ><span
                                                class="spinner-border spinner-border-sm ms-2"
                                                role="status"
                                                aria-hidden="true"
                                            ></span>
                                            <span class="visually-hidden"
                                                >Loading...</span
                                            ></template
                                        >
                                    </button>
                                    <button
                                        v-if="!selectedColumn.id"
                                        class="btn btn-danger btn-sm"
                                        :disabled="saving"
                                        @click.prevent="
                                            cancelAddingColumn(selectedColumn)
                                        "
                                    >
                                        <i class="bi bi-x-circle-fill me-1"></i>
                                        Cancel Adding Column
                                    </button>
                                    <button
                                        v-else-if="selectedColumn.order != 0"
                                        class="btn btn-danger btn-sm"
                                        :disabled="saving"
                                        @click.prevent="
                                            removeColumn(selectedColumn)
                                        "
                                    >
                                        <i class="bi bi-trash3-fill me-1"></i>
                                        Delete Column
                                    </button>
                                </template>
                            </div>
                        </form>
                    </div>
                </div>
                <div v-else class="card sticky-top">
                    <div class="card-body">
                        <h5 class="card-title border-bottom pb-2 mb-3">
                            No Column Selected
                        </h5>
                        <div>
                            <alert type="info"
                                ><i
                                    class="bi bi-arrow-left-square-fill me-2"
                                ></i>
                                Select a column from the left panel to view it's
                                details
                            </alert>
                        </div>
                    </div>
                </div>
            </div>
        </div>
        <div v-else>
            <div class="d-flex justify-content-center">
                <div class="spinner-border text-primary" role="status">
                    <span class="visually-hidden">Loading...</span>
                </div>
            </div>
        </div>
        <div v-if="schema" class="row">
            <div
                class="navbar fixed-bottom"
                style="background-color: rgb(245, 245, 245)"
            >
                <div class="container">
                    <div class="col-md-6">
                        <div class="col-md-6">
                            <a
                                role="button"
                                class="btn btn-primary me-2 float-start"
                                href="/internal/occurrence-report/bulk_import_schema/"
                                >Return to Dashboard
                            </a>
                        </div>
                    </div>
                    <div v-if="schema.can_user_edit" class="col-md-6 text-end">
                        <button
                            class="btn btn-primary me-2 float-end"
                            :disabled="saving"
                            @click.prevent="save()"
                        >
                            Save and Continue
                            <template v-if="saving"
                                ><span
                                    class="spinner-border spinner-border-sm ms-2"
                                    role="status"
                                    aria-hidden="true"
                                ></span>
                                <span class="visually-hidden"
                                    >Loading...</span
                                ></template
                            ></button
                        ><button
                            class="btn btn-primary me-2 float-end"
                            :disabled="saving"
                            @click.prevent="saveAndExit()"
                        >
                            Save and Exit
                            <template v-if="saving"
                                ><span
                                    class="spinner-border spinner-border-sm ms-2"
                                    role="status"
                                    aria-hidden="true"
                                ></span>
                                <span class="visually-hidden">Loading...</span>
                            </template>
                        </button>
                    </div>
                </div>
            </div>
        </div>
        <template
            v-if="
                selectedField &&
                selectedField.xlsx_validation_type == 'list' &&
                selectedField.choices &&
                selectedField.choices.length > 0
            "
        >
            <div
                id="preview-choices"
                class="modal fade"
                data-bs-backdrop="static"
                data-bs-keyboard="false"
                tabindex="-1"
                aria-labelledby="preview-choices-label"
                aria-hidden="true"
            >
                <div class="modal-dialog">
                    <div class="modal-content">
                        <div class="modal-header">
                            <h5 id="preview-choices-label" class="modal-title">
                                {{ selectedField.display_name }} List Choices
                            </h5>
                            <button
                                type="button"
                                class="btn-close"
                                data-bs-dismiss="modal"
                                aria-label="Close"
                            ></button>
                        </div>
                        <div class="modal-body">
                            <div class="table-responsive">
                                <table
                                    class="table table-bordered table-hover table-sm"
                                >
                                    <tbody>
                                        <tr
                                            v-for="choice in selectedField.choices"
                                            :key="choice[0]"
                                        >
                                            <td>
                                                <template
                                                    v-if="
                                                        [
                                                            'MultiSelectField',
                                                            'ForeignKey',
                                                            'ManyToManyField',
                                                        ].includes(
                                                            selectedField.type
                                                        )
                                                    "
                                                    >{{ choice[1] }}</template
                                                >
                                                <template v-else>{{
                                                    choice[0]
                                                }}</template>
                                            </td>
                                        </tr>
                                    </tbody>
                                </table>
                            </div>
                        </div>
                        <div class="modal-footer">
                            <button
                                type="button"
                                class="btn btn-secondary"
                                data-bs-dismiss="modal"
                            >
                                Close
                            </button>
                        </div>
                    </div>
                </div>
            </div>
        </template>
    </div>
</template>

<script>
import alert from '@vue-utils/alert.vue';
import Sortable from 'sortablejs';

import { api_endpoints } from '@/utils/hooks.js';

export default {
    name: 'OccurrenceReportBulkImportSchema',
    components: {
        alert,
    },
    data() {
        return {
            schema: null,
            djangoContentTypes: null,
            djangoContentTypesFiltered: null,
            djangoImportFieldsFiltered: null,
            defaultValueChoices: null,
            selectedColumn: null,
            selectedColumnIndex: null,
            selectedContentType: null,
            selectedField: null,
            customLookupField: false,
            previousCustomLookupField: null,
            lookupSchematypes: null,
            sortable: null,
            addEditMode: false,
            showDjangoImportFieldSelect: false,
            newColumn: null,
            saving: false,
            validatingSchema: false,
            errors: null,
        };
    },
    computed: {
        showDjangoLookupField() {
            return (
                this.selectedColumn &&
                this.selectedColumn.xlsx_column_header_name &&
                this.selectedColumn.django_import_content_type &&
                this.selectedColumn.requires_lookup_field &&
                this.selectedColumn.django_import_field_name &&
                this.selectedField &&
                ['ManyToManyField', 'ForeignKey'].includes(
                    this.selectedField.type
                )
            );
        },
    },
    created() {
        this.fetchBulkImportSchema();
        this.fetchDefaultValueChoices();
        this.fetchLookupSchematypes();
    },
    methods: {
        fetchLookupSchematypes() {
            fetch(api_endpoints.lookup_schema_types)
                .then(async (response) => {
                    this.lookupSchematypes = await response.json();
                })
                .catch((error) => {
                    console.error(error);
                });
        },
        fetchBulkImportSchema() {
            fetch(
                `${api_endpoints.occurrence_report_bulk_import_schemas}${this.$route.params.bulk_import_schema_id}/`
            )
                .then(async (response) => {
                    this.schema = await response.json();
                    this.fetchContentTypes();
                    this.$nextTick(() => {
                        if (!this.schema.can_user_edit) {
                            this.disableMainFieldsetInputs();
                        } else {
                            this.enableMainFieldsetInputs();
                            var el = document.querySelector('.columns');
                            this.sortable = Sortable.create(el, {
                                animation: 150,
                                handle: '.bi-arrow-down-up',
                                onEnd: (event) => {
                                    let column = this.schema.columns.splice(
                                        event.oldIndex,
                                        1
                                    )[0];
                                    this.schema.columns.splice(
                                        event.newIndex,
                                        0,
                                        column
                                    );
                                    // Update the order of the columns
                                    this.schema.columns.forEach(
                                        (column, index) => {
                                            column.order = index;
                                        }
                                    );
                                    this.save();
                                },
                            });
                        }
                    });
                })
                .catch((error) => {
                    console.error(error);
                });
        },
        fetchContentTypes() {
            fetch(
                `${api_endpoints.content_types}ocr_bulk_import_content_types/`
            )
                .then(async (response) => {
                    this.djangoContentTypes = await response.json();
                    // Filter out content types that have no fields
                    this.djangoContentTypesFiltered =
                        this.djangoContentTypes.filter(
                            (djangoContentType) =>
                                djangoContentType.model_fields.length > 0
                        );
                    // Filter out content types where all their fields are already
                    // columns in the schema. Ignore schema columns that are
                    // known to be invalid (field_exists === false) so that
                    // recovered/invalid columns do not block adding new columns.
                    this.djangoContentTypesFiltered =
                        this.djangoContentTypesFiltered.filter(
                            (djangoContentType) =>
                                !djangoContentType.model_fields.every(
                                    (modelField) =>
                                        this.schema.columns.some(
                                            (column) =>
                                                column.field_exists !== false &&
                                                column.field_exists === true &&
                                                column.django_import_field_name ==
                                                    modelField.name &&
                                                column.django_import_content_type ==
                                                    modelField.content_type
                                        )
                                )
                        );
                })
                .catch((error) => {
                    console.error(error);
                });
        },
        fetchDefaultValueChoices() {
            fetch(
                `${api_endpoints.occurrence_report_bulk_import_schemas}default_value_choices/`
            )
                .then(async (response) => {
                    this.defaultValueChoices = await response.json();
                })
                .catch((error) => {
                    console.error(error);
                });
        },
        selectDjangoImportContentType() {
            if (!this.selectedColumn.django_import_content_type) {
                this.selectedField = null;
                this.selectedContentType = null;
                this.showDjangoImportFieldSelect = false;
                this.$refs['django-import-field'].focus();
                return;
            }
            this.selectedContentType = this.djangoContentTypes.filter(
                (djangoContentType) =>
                    djangoContentType.id ==
                    this.selectedColumn.django_import_content_type
            )[0];
            // Filter out fields that are already columns in the schema.
            // Ignore schema columns that are known to be invalid so users
            // can add fields even if an invalid column remains.
            this.djangoImportFieldsFiltered =
                this.selectedContentType.model_fields.filter(
                    (modelField) =>
                        !this.schema.columns.some(
                            (column) =>
                                column.field_exists !== false &&
                                column.field_exists === true &&
                                column.django_import_field_name ==
                                    modelField.name &&
                                column.django_import_content_type ==
                                    modelField.content_type
                        )
                );
            // If there are already other columns with the same django content type
            // then move the selected column to the end of the list of those columns
            if (
                !this.selectedColumn.id &&
                this.schema.columns.filter(
                    (column) =>
                        column.field_exists === true &&
                        column.django_import_content_type ==
                            this.selectedColumn.django_import_content_type
                ).length > 1
            ) {
                let lastColumn = this.schema.columns.findLast(
                    (column) =>
                        column.field_exists === true &&
                        column.django_import_content_type ==
                            this.selectedColumn.django_import_content_type &&
                        column.django_import_field_name !=
                            this.selectedColumn.django_import_field_name
                );
                let lastColumnIndex =
                    this.schema.columns.indexOf(lastColumn) + 1;
                this.schema.columns.splice(this.selectedColumnIndex, 1);
                this.schema.columns.splice(
                    lastColumnIndex,
                    0,
                    this.selectedColumn
                );
                this.selectedColumnIndex = lastColumnIndex;
                // Update the order of the columns to reflect the new order
                this.applyOrderToColumns();
            }
            this.$nextTick(() => {
                this.enablePopovers();
                this.selectedColumn.model_name =
                    this.selectedContentType.model_verbose_name;
                if (!this.selectedColumn.id) {
                    document.querySelector('tr.active').scrollIntoView();
                }
            });
        },
        selectDjangoImportField() {
            if (!this.selectedColumn.django_import_field_name) {
                this.selectedField = null;
                this.$refs['django-import-field'].focus();
                return;
            }
            this.selectedField = this.selectedContentType.model_fields.filter(
                (modelField) =>
                    modelField.name ==
                    this.selectedColumn.django_import_field_name
            )[0];
            this.$nextTick(() => {
                this.enablePopovers();
                this.selectedColumn.xlsx_column_header_name = `${this.selectedContentType.model_abbreviation.toUpperCase()} ${this.selectedField.display_name}`;
                if (!this.selectedColumn.id) {
                    this.selectedColumn.xlsx_data_validation_allow_blank =
                        this.selectedField.allow_null;
                }
                this.$refs['column-name'].focus();
            });
        },
        selectLookupField() {
            if (this.selectedColumn.django_lookup_field_name == 'custom') {
                this.customLookupField = true;
                if (this.previousCustomLookupField) {
                    this.selectedColumn.django_lookup_field_name =
                        this.previousCustomLookupField;
                } else {
                    this.selectedColumn.django_lookup_field_name = '';
                }
                this.$nextTick(() => {
                    this.$refs['custom-lookup-field'].focus();
                });
            } else {
                this.customLookupField = false;
                this.selectedColumn.django_lookup_field_name =
                    this.$refs['lookup-field'].value;
            }
        },
        getNewColumnData() {
            return {
                id: null,
                order: this.schema.columns.length,
                schema: this.schema.id,
                django_import_content_type: '',
                django_import_field_name: '',
                django_lookup_field_name: null,
                xlsx_column_header_name: '',
                xlsx_data_validation_allow_blank: true,
                default_value: null,
                import_validations: [],
                lookup_filters: [],
                is_editable_by_user: true,
                is_emailuser_column: false,
            };
        },
        addSingleColumn() {
            this.showDjangoImportFieldSelect = true;
            this.$nextTick(() => {
                this.enablePopovers();
                this.$refs['django-import-field'].focus();
            });
        },
        addNewColumn() {
            this.newColumn = Object.assign({}, this.getNewColumnData());
            this.schema.columns.push(this.newColumn);
            this.selectedContentType = null;
            this.selectedField = null;
            this.selectedColumn = this.newColumn;
            this.selectedColumnIndex = this.schema.columns.indexOf(
                this.newColumn
            );
            this.addEditMode = true;
            this.showDjangoImportFieldSelect = false;
            this.$nextTick(() => {
                this.enablePopovers();
                this.$refs['django-import-model'].focus();
            });
        },
        addAllColumns(onlyMandatory) {
            swal.fire({
                title: onlyMandatory
                    ? `Add All Mandatory Fields`
                    : `Add All Fields`,
                text: onlyMandatory
                    ? `Are you sure you want to add all mandatory fields from the "${this.selectedContentType.model_verbose_name ? this.selectedContentType.model_verbose_name : this.selectedContentType.model}" model?`
                    : `Are you sure you want to add all fields from the "${this.selectedContentType.model_verbose_name}" model?`,
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Confirm',
                cancelButtonText: 'Cancel',
                customClass: {
                    confirmButton: 'btn btn-primary',
                    cancelButton: 'btn btn-secondary me-2',
                },
                reverseButtons: true,
            }).then((result) => {
                if (result.isConfirmed) {
                    let newColumns = this.selectedContentType.model_fields.map(
                        (modelField) => {
                            return {
                                id: null,
                                schema: this.schema.id,
                                django_import_content_type:
                                    this.selectedContentType.id,
                                django_import_field_name: modelField.name,
                                xlsx_column_header_name: `${this.selectedContentType.model_abbreviation.toUpperCase()} ${modelField.display_name}`,
                                xlsx_data_validation_allow_blank:
                                    modelField.allow_null,
                                default_value: null,
                                import_validations: [],
                                // Set these so the UI shows the new columns as
                                // valid immediately (they'll be persisted on save)
                                model_name:
                                    this.selectedContentType.model_verbose_name,
                                model_exists: true,
                                field_exists: true,
                            };
                        }
                    );
                    // DEBUG: show selected content type fields and current schema columns
                    // (debug logs removed)

                    // Remove columns that are already in the schema. Ignore
                    // invalid schema columns (field_exists === false) so they
                    // don't block adding the new field again.
                    newColumns = newColumns.filter(
                        (newColumn) =>
                            !this.schema.columns.some(
                                (column) =>
                                    column.field_exists === true &&
                                    column.django_import_content_type ==
                                        newColumn.django_import_content_type &&
                                    column.django_import_field_name ==
                                        newColumn.django_import_field_name
                            )
                    );

                    // (debug logs removed)

                    if (newColumns.length == 0) {
                        swal.fire({
                            title: 'No New Columns Added',
                            text: 'All fields from the selected model are already in the schema',
                            icon: 'info',
                            confirmButtonText: 'OK',
                            customClass: {
                                confirmButton: 'btn btn-primary',
                            },
                        });
                        return;
                    }

                    if (onlyMandatory) {
                        newColumns = newColumns.filter(
                            (column) => !column.xlsx_data_validation_allow_blank
                        );
                    }

                    if (newColumns.length == 0) {
                        swal.fire({
                            title: 'No New Columns Added',
                            text: 'There are no mandatory fields from the selected model that are not already in the schema',
                            icon: 'info',
                            confirmButtonText: 'OK',
                            customClass: {
                                confirmButton: 'btn btn-primary',
                            },
                        });
                        return;
                    }

                    let selectedContentType =
                        this.selectedColumn.django_import_content_type;
                    this.schema.columns.splice(this.selectedColumnIndex, 1);
                    this.selectedColumn = null;
                    this.newColumn = null;
                    this.addEditMode = false;

                    let lastColumnIndex = this.schema.columns.length;
                    if (
                        this.schema.columns.filter(
                            (column) =>
                                column.field_exists === true &&
                                column.django_import_content_type ==
                                    selectedContentType
                        ).length > 0
                    ) {
                        let lastColumn = this.schema.columns.findLast(
                            (column) =>
                                column.field_exists === true &&
                                column.django_import_content_type ==
                                    selectedContentType
                        );
                        lastColumnIndex =
                            this.schema.columns.indexOf(lastColumn) + 1;
                    }

                    for (let i = 0; i < newColumns.length; i++) {
                        // inserting newColumn
                        this.schema.columns.splice(
                            lastColumnIndex + i,
                            0,
                            newColumns[i]
                        );
                    }
                    this.applyOrderToColumns();

                    // snapshot after insertion

                    this.save();
                    this.addEditMode = false;
                } else {
                    if (this.selectedColumn) {
                        this.$refs['django-import-model'].focus();
                    }
                }
            });
        },
        applyOrderToColumns() {
            this.schema.columns.forEach((column, index) => {
                column.order = index;
            });
        },
        selectColumn(column) {
            if (column.django_import_content_type) {
                if (column.model_exists === false) {
                    this.selectedContentType = 'invalid_content_type';
                } else {
                    this.selectedContentType = this.djangoContentTypes.filter(
                        (djangoContentType) =>
                            djangoContentType.id ==
                            column.django_import_content_type
                    )[0];
                }

                if (column.django_import_field_name) {
                    if (this.selectedContentType != 'invalid_content_type') {
                        this.selectedField =
                            this.selectedContentType.model_fields.filter(
                                (modelField) =>
                                    modelField.name ==
                                    column.django_import_field_name
                            )[0] || 'invalid_field';
                    } else {
                        this.selectedField = 'invalid_field';
                    }

                    if (this.selectedField != 'invalid_field') {
                        if (
                            this.selectedField.lookup_field_options &&
                            !this.selectedField.lookup_field_options.includes(
                                column.django_lookup_field_name
                            )
                        ) {
                            this.customLookupField = true;
                            if (column.django_lookup_field_name) {
                                this.previousCustomLookupField =
                                    column.django_lookup_field_name;
                            }
                        }
                    } else {
                        if (column.model_exists === false) {
                            column.invalid_model = column.model_name;
                        }
                        column.invalid_field = column.django_import_field_name;
                    }
                }
            }
            this.selectedColumn = column;
            this.selectedColumnIndex = this.schema.columns.indexOf(column);
            this.addEditMode = true;
            if (this.selectedColumn.id && column.django_import_field_name) {
                this.showDjangoImportFieldSelect = true;
            }
            this.$nextTick(() => {
                this.enablePopovers();
                this.$refs['django-import-model'].focus();
            });
        },
        cancelAddingColumn(column) {
            this.schema.columns = this.schema.columns.filter(
                (col) => col !== column
            );
            this.selectedColumn = null;
            this.selectedColumnIndex = null;
            this.addEditMode = false;
            this.showDjangoImportFieldSelect = false;
        },
        removeColumn(column) {
            let columnTitle = column.xlsx_column_header_name
                ? `Are you sure you want to delete column: ${column.xlsx_column_header_name}?`
                : `Are you sure you want to delete this column?`;
            swal.fire({
                title: `Delete Column ${column.xlsx_column_header_name}`,
                text: columnTitle,
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Confirm Delete',
                cancelButtonText: 'Cancel',
                customClass: {
                    confirmButton: 'btn btn-primary',
                    cancelButton: 'btn btn-secondary me-2',
                },
                reverseButtons: true,
            }).then((result) => {
                if (result.isConfirmed) {
                    this.schema.columns = this.schema.columns.filter(
                        (column) => column !== this.selectedColumn
                    );
                    this.selectedColumn = null;
                    this.addEditMode = false;
                    this.applyOrderToColumns();
                    if (column.id) {
                        this.save();
                    }
                    this.fetchContentTypes();
                } else {
                    if (this.selectedColumn) {
                        this.$refs['django-import-model'].focus();
                    }
                }
            });
        },
        addTag(event) {
            if (event.key === 'Enter') {
                if (this.schema.tags.includes(event.target.value)) {
                    event.target.value = '';
                    return;
                }
                this.schema.tags.push(event.target.value);
                this.save();
                event.target.value = '';
            }
        },
        removeTag(index) {
            this.schema.tags.splice(index, 1);
            this.save();
        },
        validate() {
            this.validatingSchema = true;
            fetch(
                `${api_endpoints.occurrence_report_bulk_import_schemas}${this.schema.id}/validate/`
            )
                .then(async (response) => {
                    if (!response.ok) {
                        throw response;
                    }
                    const data = await response.text();
                    swal.fire({
                        title: 'Schema Validated Successfully',
                        text: data,
                        icon: 'success',
                        confirmButtonText: 'OK',
                        customClass: {
                            confirmButton: 'btn btn-primary',
                        },
                    });
                })
                .catch(async (response) => {
                    let error = null;
                    try {
                        error = await response.json();
                    } catch {
                        error = await response.text();
                    }
                    let errors = null;
                    if (Object.hasOwn(error, 'data')) {
                        errors = error.data;
                    } else if (Object.hasOwn(error, 'body')) {
                        errors = error.body;
                    } else {
                        errors = error;
                    }

                    let error_message_string = 'Something went wrong :-(';
                    if (errors instanceof Object) {
                        error_message_string = '';
                        for (let i = 0; i < errors.length; i++) {
                            let error_message = errors[i].error_message
                                ? errors[i].error_message
                                : errors[i];
                            error_message_string += `<li class="mb-2">${error_message}</li>`;
                        }
                        console.log(error_message_string);
                    } else if (typeof errors === 'string') {
                        error_message_string = errors;
                    }
                    console.error(error_message_string);
                    swal.fire({
                        title: 'Schema Validation Failed',
                        html: error_message_string,
                        icon: 'error',
                        confirmButtonText: 'OK',
                        customClass: {
                            confirmButton: 'btn btn-primary',
                        },
                    });
                })
                .finally(() => {
                    this.validatingSchema = false;
                });
        },
        save() {
            // If there is a column with no django_import_content_type or django_import_field_name, remove it
            const wasSelected = !!this.selectedColumn;
            const prevSelectedId = wasSelected ? this.selectedColumn.id : null;
            const prevSelectedIndex = this.selectedColumnIndex;

            if (
                this.schema.columns.some(
                    (column) =>
                        !column.django_import_content_type ||
                        !column.django_import_field_name
                )
            ) {
                this.schema.columns = this.schema.columns.filter(
                    (column) =>
                        column.django_import_content_type &&
                        column.django_import_field_name
                );
                if (
                    wasSelected &&
                    (!prevSelectedId ||
                        !this.schema.columns.some(
                            (c) => c.id === prevSelectedId
                        ))
                ) {
                    // selected new/incomplete column was removed
                    this.selectedColumn = null;
                    this.selectedColumnIndex = null;
                    this.addEditMode = false;
                }
            }
            // If there is a lookup filter with no filter_field_name, remove it
            this.schema.columns.forEach((column) => {
                if (column.lookup_filters) {
                    column.lookup_filters = column.lookup_filters.filter(
                        (filter) => filter.filter_field_name
                    );
                }
            });

            this.saving = true;
            this.errors = null;
            fetch(
                `${api_endpoints.occurrence_report_bulk_import_schemas}${this.schema.id}/`,
                {
                    method: 'PUT',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify(this.schema),
                }
            )
                .then(async (response) => {
                    const data = await response.json();
                    this.saving = false;
                    this.schema = Object.assign({}, data);
                    // server returned schema
                    // Restore selection using id if possible, else fallback to previous index
                    if (wasSelected) {
                        let restored =
                            (prevSelectedId &&
                                this.schema.columns.find(
                                    (c) => c.id === prevSelectedId
                                )) ||
                            (Number.isInteger(prevSelectedIndex) &&
                                this.schema.columns[prevSelectedIndex]) ||
                            null;

                        if (restored) {
                            // Rehydrate selectedContentType/selectedField/etc.
                            this.selectColumn(restored);
                        } else {
                            this.selectedColumn = null;
                            this.selectedColumnIndex = null;
                            this.addEditMode = false;
                        }
                    }
                })
                .catch((error) => {
                    this.saving = false;
                    this.errors = error.data;
                    console.error(error);
                });
        },
        saveAndExit() {
            this.save();
            this.$router.push(
                `/internal/occurrence-report/bulk_import_schema/`
            );
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
                        this.$router
                            .push({
                                path: `/internal/occurrence-report/bulk_import_schema/${data.id}`,
                            })
                            .then(() => {
                                this.fetchBulkImportSchema();
                            });
                    }
                })
                .catch((error) => {
                    console.error(error);
                });
        },
        enablePopovers() {
            // enable all bootstrap 5 popovers
            var popoverTriggerList = [].slice.call(
                document.querySelectorAll('[data-bs-toggle="popover"]')
            );
            popoverTriggerList.map(function (popoverTriggerEl) {
                return new bootstrap.Popover(popoverTriggerEl);
            });
        },
        stringToRGBAColor(str) {
            // Return a valid CSS color string. Fallback to transparent so
            // the inline style is always valid. Alpha is low so the
            // background is subtle.
            if (!str) {
                return 'transparent';
            }
            let hash = 0;
            for (let i = 0; i < str.length; i++) {
                hash = str.charCodeAt(i) + ((hash << 5) - hash);
            }
            let c = (hash & 0x00ffffff).toString(16).toUpperCase();
            let color_hash = '#' + '00000'.substring(0, 6 - c.length) + c;
            // Consvert to RGB
            let r = parseInt(color_hash.slice(1, 3), 16);
            let g = parseInt(color_hash.slice(3, 5), 16);
            let b = parseInt(color_hash.slice(5, 7), 16);
            // Ensure numbers are valid
            if (isNaN(r) || isNaN(g) || isNaN(b)) return 'transparent';
            return `rgba(${r}, ${g}, ${b}, 0.10)`;
        },
        classesForRow(index, column, selectedColumn) {
            let classes = selectedColumn == column ? 'active bg-success' : '';
            let next_index = index + 1;
            if (next_index > this.schema.columns.length - 1) {
                return classes;
            }
            let next_column = this.schema.columns[next_index];
            if (
                next_column.django_import_content_type !=
                column.django_import_content_type
            ) {
                classes += ' border-bottom border-secondary last-row-for-model';
            }
            return classes;
        },
        excelValidationType() {
            if (this.selectedColumn.is_emailuser_column) {
                return 'None';
            }
            return this.selectedField.xlsx_validation_type
                ? this.selectedField.xlsx_validation_type
                : 'None';
        },
        addLookupFilter() {
            if (!this.selectedColumn.lookup_filters) {
                this.selectedColumn.lookup_filters = [];
            }
            this.selectedColumn.lookup_filters.push({
                id: null,
                schema_column: this.selectedColumn.id,
                filter_field_name: '',
                filter_type: 'exact',
                values: [],
            });
        },
        removeLookupFilter(column, index) {
            let lookup_filter_id = column.lookup_filters[index].id;
            column.lookup_filters.splice(index, 1);
            if (lookup_filter_id) {
                this.save();
            }
        },
        lookupFilterFieldChanged(lookupFilter) {
            if (lookupFilter.values.length == 0) {
                lookupFilter.values.push({
                    id: null,
                    filter_value: '',
                });
            } else {
                lookupFilter.values[0].filter_value = '';
            }
            this.save();
        },
        lookupFilterValueChanged() {
            this.save();
        },
        disableMainFieldsetInputs() {
            document
                .querySelectorAll('fieldset#main-schema-fieldset input')
                .forEach((input) => {
                    input.disabled = true;
                });
        },
        enableMainFieldsetInputs() {
            document
                .querySelectorAll('fieldset#main-schema-fieldset input')
                .forEach((input) => {
                    input.disabled = false;
                });
        },
    },
    onRouteEnter() {
        this.fetchBulkImportSchema();
    },
};
</script>

<style scoped>
div.scroll {
    margin: 4px, 4px;
    padding: 4px;
    width: 100%;
    overflow-x: auto;
    overflow-y: hidden;
    white-space: nowrap;
}

table.table-schema {
    border-collapse: collapse;
}

.sticky-top {
    top: 1.5em;
}

/* Ensure active/bg-success row shows green even if cells have their own background */
table.table-schema tbody tr.active.bg-success > td {
    background-color: var(--bs-success, #198754) !important;
    color: var(--bs-white, #fff) !important;
}
table.table-schema tbody tr.active.bg-success {
    background-color: var(--bs-success, #198754) !important;
}

/* Let per-row background colours (calculated from model name) show through:
   force cells to be transparent for non-active rows so the TR background is visible. */
table.table-schema tbody tr:not(.active) > td {
    background-color: transparent !important;
    color: inherit !important;
}
</style>
