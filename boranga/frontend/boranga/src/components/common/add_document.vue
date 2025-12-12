<template lang="html">
    <div id="document_detail">
        <modal
            transition="modal fade"
            :title="title"
            large
            @ok="ok()"
            @cancel="cancel()"
        >
            <div class="container-fluid">
                <div class="row">
                    <form class="form-horizontal" name="documentForm">
                        <alert v-if="errorString" type="danger"
                            ><strong>{{ errorString }}</strong></alert
                        >
                        <div class="col-sm-12">
                            <div class="form-group">
                                <div class="row mb-3">
                                    <div class="col-sm-3">
                                        <label
                                            for="document_category"
                                            class="control-label"
                                            >Category</label
                                        >
                                    </div>
                                    <div class="col-sm-9">
                                        <template
                                            v-if="
                                                documentCategories &&
                                                documentCategories.length > 0 &&
                                                !documentCategories
                                                    .map(
                                                        (category) =>
                                                            category.id
                                                    )
                                                    .includes(
                                                        documentObj.document_category
                                                    )
                                            "
                                        >
                                            <input
                                                v-if="
                                                    documentObj.document_category_name
                                                "
                                                type="text"
                                                class="form-control mb-3"
                                                :value="
                                                    documentObj.document_category_name +
                                                    ' (Now Archived)'
                                                "
                                                disabled
                                            />
                                            <div class="mb-3 text-muted">
                                                Change document category to:
                                            </div>
                                        </template>
                                        <select
                                            id="document_category"
                                            ref="document_category"
                                            v-model="
                                                documentObj.document_category
                                            "
                                            class="form-select"
                                            @change="filterSubCategory($event)"
                                        >
                                            <option
                                                v-for="category in documentCategories"
                                                :key="category.id"
                                                :value="category.id"
                                            >
                                                {{ category.name }}
                                            </option>
                                        </select>
                                    </div>
                                </div>
                                <div
                                    v-if="documentObj.document_category"
                                    class="row mb-3"
                                >
                                    <div class="col-sm-3">
                                        <label
                                            for="document_sub_category"
                                            class="control-label"
                                            >Sub Category</label
                                        >
                                    </div>
                                    <div class="col-sm-9">
                                        <template
                                            v-if="
                                                filteredDocumentSubCategories &&
                                                filteredDocumentSubCategories.length >
                                                    0 &&
                                                !filteredDocumentSubCategories
                                                    .map(
                                                        (category) =>
                                                            category.id
                                                    )
                                                    .includes(
                                                        documentObj.document_sub_category
                                                    )
                                            "
                                        >
                                            <input
                                                v-if="
                                                    documentObj.document_sub_category_name
                                                "
                                                type="text"
                                                class="form-control mb-3"
                                                :value="
                                                    documentObj.document_sub_category_name +
                                                    ' (Now Archived)'
                                                "
                                                disabled
                                            />
                                            <div class="mb-3 text-muted">
                                                Change document sub category to:
                                            </div>
                                        </template>
                                        <select
                                            id="document_sub_category"
                                            ref="document_sub_category"
                                            v-model="
                                                documentObj.document_sub_category
                                            "
                                            class="form-select"
                                            @change="focusDescription"
                                        >
                                            <option
                                                v-for="sub_category in filteredDocumentSubCategories"
                                                :key="sub_category.id"
                                                :value="sub_category.id"
                                            >
                                                {{ sub_category.name }}
                                            </option>
                                        </select>
                                    </div>
                                </div>
                                <div class="row mb-3">
                                    <div class="col-sm-3">
                                        <label
                                            for="description"
                                            class="control-label"
                                            >Description</label
                                        >
                                    </div>
                                    <div class="col-sm-9">
                                        <textarea
                                            id="description"
                                            ref="description"
                                            v-model="documentObj.description"
                                            class="form-control"
                                        >
                                        </textarea>
                                    </div>
                                </div>
                                <div
                                    v-if="documentObj.uploaded_date"
                                    class="row mb-3"
                                >
                                    <div class="col-sm-3">
                                        <label
                                            for="uploaded_date"
                                            class="control-label"
                                            >Date/Time</label
                                        >
                                    </div>
                                    <div class="col-sm-9">
                                        <input
                                            id="uploaded_date"
                                            ref="uploaded_date"
                                            v-model="documentObj.uploaded_date"
                                            disabled
                                            type="datetime-local"
                                            class="form-control"
                                            name="uploaded_date"
                                        />
                                    </div>
                                </div>
                                <div
                                    v-if="showSubmitterCanAccess"
                                    class="row mb-3"
                                >
                                    <div class="col-sm-3">
                                        <label
                                            for="can_submitter_access"
                                            class="control-label"
                                            >Submitter Can Access?
                                        </label>
                                    </div>
                                    <div class="col-sm-9">
                                        <div
                                            class="form-check form-check-inline"
                                        >
                                            <label
                                                for="can_submitter_access_no"
                                                class="form-check-label"
                                                >No</label
                                            >
                                            <input
                                                id="can_submitter_access_no"
                                                ref="can_submitter_access_no"
                                                v-model="
                                                    documentObj.can_submitter_access
                                                "
                                                type="radio"
                                                class="form-check-input"
                                                name="can_submitter_access"
                                                :value="false"
                                            />
                                        </div>
                                        <div
                                            class="form-check form-check-inline"
                                        >
                                            <label
                                                for="can_submitter_access_yes"
                                                class="form-check-label"
                                                >Yes</label
                                            >
                                            <input
                                                id="can_submitter_access_yes"
                                                ref="can_submitter_access_yes"
                                                v-model="
                                                    documentObj.can_submitter_access
                                                "
                                                type="radio"
                                                class="form-check-input"
                                                name="can_submitter_access"
                                                :value="true"
                                            />
                                        </div>
                                    </div>
                                </div>
                                <div class="row mb-3">
                                    <label for="documents_file" class="mb-3"
                                        >Document</label
                                    >
                                    <div class="col">
                                        <FileField2
                                            id="documents_file"
                                            ref="filefield"
                                            :proposal_id="document_id"
                                            :uploaded_documents="
                                                uploaded_document
                                            "
                                            :is-repeatable="false"
                                            name="documents_file"
                                        />
                                    </div>
                                </div>
                            </div>
                        </div>
                    </form>
                </div>
            </div>
            <template #footer>
                <div>
                    <button
                        type="button"
                        class="btn btn-secondary me-2"
                        @click="cancel"
                    >
                        Cancel
                    </button>
                    <template v-if="document_id">
                        <button
                            v-if="updatingDocument"
                            type="button"
                            disabled
                            class="btn btn-primary"
                            @click="ok"
                        >
                            Updating
                            <span
                                class="spinner-border spinner-border-sm"
                                role="status"
                                aria-hidden="true"
                            ></span>
                            <span class="visually-hidden">Loading...</span>
                        </button>
                        <button
                            v-else
                            type="button"
                            class="btn btn-primary"
                            @click="ok"
                        >
                            Update
                        </button>
                    </template>
                    <template v-else>
                        <button
                            v-if="addingDocument"
                            type="button"
                            disabled
                            class="btn btn-primary"
                            @click="ok"
                        >
                            Adding
                            <span
                                class="spinner-border spinner-border-sm"
                                role="status"
                                aria-hidden="true"
                            ></span>
                            <span class="visually-hidden">Loading...</span>
                        </button>
                        <button
                            v-else
                            type="button"
                            class="btn btn-primary"
                            @click="ok"
                        >
                            Add Document
                        </button>
                    </template>
                </div>
            </template>
        </modal>
    </div>
</template>

<script>
import FileField2 from '@/components/forms/filefield.vue';
import modal from '@vue-utils/bootstrap-modal.vue';
import alert from '@vue-utils/alert.vue';
import { helpers, api_endpoints } from '@/utils/hooks.js';
export default {
    name: 'DocumentDetail',
    components: {
        FileField2,
        modal,
        alert,
    },
    props: {
        url: {
            type: String,
            required: true,
        },
        is_internal: {
            type: Boolean,
            default: false,
        },
    },
    data: function () {
        return {
            isModalOpen: false,
            form: null,
            document_id: String,
            document_action: String,
            documentObj: Object,
            uploaded_document: [],
            documentCategories: [],
            documentSubCategories: [],
            filteredDocumentSubCategories: [],
            addingDocument: false,
            updatingDocument: false,
            validation_form: null,
            type: '1',
            errorString: '',
            successString: '',
            success: false,
            validDate: false,
            title: null,
            originalDocumentObj: null,
        };
    },
    computed: {
        showSubmitterCanAccess: function () {
            return (
                this.is_internal &&
                Object.hasOwn(this.documentObj, 'can_submitter_access')
            );
        },
    },
    watch: {
        isModalOpen: function (val) {
            if (val) {
                this.$nextTick(() => {
                    this.$refs.document_category.focus();
                    this.originalDocumentObj = JSON.parse(
                        JSON.stringify(this.documentObj)
                    );
                });
            }
        },
    },
    created: async function () {
        fetch(api_endpoints.document_categories_dict).then(
            async (response) => {
                let document_dict = await response.json();
                this.documentCategories = document_dict.document_category_list;
                // blank entry allows user to clear selection
                this.documentCategories.splice(0, 0, {
                    id: '',
                    name: '',
                });
                // documentSubCategories
                this.documentSubCategories =
                    document_dict.document_sub_category_list;
                this.filterSubCategory();
            },
            (error) => {
                console.log(error);
            }
        );
    },
    mounted: function () {
        let vm = this;
        vm.form = document.forms.documentForm;
    },
    methods: {
        hasUnsavedChanges: function () {
            return (
                JSON.stringify(this.documentObj) !==
                JSON.stringify(this.originalDocumentObj)
            );
        },
        focusDescription: function () {
            this.$nextTick(() => {
                this.$refs['description'].focus();
            });
        },
        filterSubCategory: function (event) {
            this.$nextTick(() => {
                if (event) {
                    this.documentObj.document_sub_category = null; //-----to remove the previous selection
                }
                this.filteredDocumentSubCategories = [];
                this.filteredDocumentSubCategories = [
                    {
                        id: null,
                        name: '',
                        category_id: null,
                    },
                ];
                //---filter sub categories as per category selected
                for (let choice of this.documentSubCategories) {
                    if (
                        choice.category_id ===
                        this.documentObj.document_category
                    ) {
                        this.filteredDocumentSubCategories.push(choice);
                    }
                }
                if (this.$refs['document_sub_category']) {
                    this.$refs['document_sub_category'].focus();
                }
            });
        },
        fetchSubCategory: function (category_id) {
            this.$nextTick(() => {
                this.filteredDocumentSubCategories = [];
                this.filteredDocumentSubCategories = [
                    {
                        id: null,
                        name: '',
                        category_id: null,
                    },
                ];
                //---filter sub categories as per category selected
                for (let choice of this.documentSubCategories) {
                    if (choice.category_id === category_id) {
                        this.filteredDocumentSubCategories.push(choice);
                    }
                }
            });
        },
        ok: function () {
            let vm = this;
            if ($(vm.form).valid()) {
                vm.sendData();
                vm.$refs.filefield.reset_files();
            }
        },
        cancel: function () {
            if (!this.hasUnsavedChanges()) {
                this.close();
                this.$refs.filefield.reset_files();
                return;
            }
            swal.fire({
                title: 'Are you sure you want to close this pop-up?',
                text: 'You will lose any unsaved changes.',
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Yes, close it',
                cancelButtonText: 'Return to pop-up',
                reverseButtons: true,
                customClass: {
                    confirmButton: 'btn btn-primary',
                    cancelButton: 'btn btn-secondary',
                },
            }).then((result) => {
                if (result.isConfirmed) {
                    this.close();
                    this.$refs.filefield.reset_files();
                }
            });
        },
        close: function () {
            this.isModalOpen = false;
            this.documentObj = {};
            this.$refs.filefield.files = [{ file: null, name: '' }];
            this.$refs.filefield.reset_files();
            $('.has-error').removeClass('has-error');
        },
        sendData: function () {
            let vm = this;
            vm.errorString = '';
            let documentObj = JSON.parse(JSON.stringify(vm.documentObj));
            let formData = new FormData();

            // Add files to formData
            var files = vm.$refs.filefield.files;
            $.each(files, function (idx, v) {
                var file = v['file'];
                var filename = v['name'];
                var name = 'file-' + idx;
                formData.append(name, file, filename);
            });
            documentObj.num_files = files.length;

            if (vm.documentObj.id) {
                vm.updatingDocument = true;
                formData.append('data', JSON.stringify(documentObj));
                fetch(helpers.add_endpoint_json(vm.url, documentObj.id), {
                    method: 'PUT',
                    body: formData,
                })
                    .then(async (response) => {
                        let data = await response.json();
                        if (response.status !== 200) {
                            vm.errorString = data;
                            return;
                        }

                        vm.$parent.updatedDocuments();
                        vm.close();
                    })
                    .finally(() => {
                        vm.updatingDocument = false;
                    });
            } else {
                vm.addingDocument = true;
                formData.append('data', JSON.stringify(documentObj));
                fetch(vm.url, {
                    method: 'POST',
                    body: formData,
                })
                    .then(async (response) => {
                        let data = await response.json();
                        if (response.status !== 200) {
                            vm.errorString = data;
                            return;
                        }
                        vm.addingDocument = false;
                        vm.close();
                        vm.$parent.updatedDocuments();
                    })
                    .finally(() => {
                        vm.addingDocument = false;
                    });
            }
        },
    },
};
</script>
