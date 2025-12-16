<template lang="html">
    <div id="AddComms">
        <modal
            transition="modal fade"
            title="Communication Log - Add Entry"
            large
            @ok="ok()"
            @cancel="cancel()"
        >
            <div class="container-fluid">
                <div class="row">
                    <form
                        class="form-horizontal needs-validation"
                        name="commsForm"
                        novalidate
                    >
                        <alert v-if="errorString" type="danger"
                            ><strong>{{ errorString }}</strong></alert
                        >
                        <div class="col-sm-12">
                            <div class="form-group">
                                <div class="row mb-3">
                                    <div class="col-sm-3">
                                        <label
                                            class="control-label pull-left"
                                            for="Name"
                                            >To</label
                                        >
                                    </div>
                                    <div class="col-sm-4">
                                        <input
                                            ref="to"
                                            v-model="comms.to"
                                            type="text"
                                            class="form-control"
                                            name="to"
                                            required
                                        />
                                        <div class="invalid-feedback">
                                            Please enter the recipient.
                                        </div>
                                    </div>
                                </div>
                            </div>
                            <div class="form-group">
                                <div class="row mb-3">
                                    <div class="col-sm-3">
                                        <label
                                            class="control-label pull-left"
                                            for="Name"
                                            >From</label
                                        >
                                    </div>
                                    <div class="col-sm-4">
                                        <input
                                            v-model="comms.fromm"
                                            type="text"
                                            class="form-control"
                                            name="fromm"
                                            required
                                        />
                                        <div class="invalid-feedback">
                                            Please enter the sender.
                                        </div>
                                    </div>
                                </div>
                            </div>
                            <div class="form-group">
                                <div class="row mb-3">
                                    <div class="col-sm-3">
                                        <label
                                            class="control-label pull-left"
                                            for="Name"
                                            >Type</label
                                        >
                                    </div>
                                    <div class="col-sm-4">
                                        <select
                                            v-model="comms.type"
                                            class="form-select"
                                            name="type"
                                            required
                                        >
                                            <option value="">
                                                Select Type
                                            </option>
                                            <option value="email">Email</option>
                                            <option value="mail">Mail</option>
                                            <option value="phone">Phone</option>
                                        </select>
                                        <div class="invalid-feedback">
                                            Please select a type.
                                        </div>
                                    </div>
                                </div>
                            </div>
                            <div class="form-group">
                                <div class="row mb-3">
                                    <div class="col-sm-3">
                                        <label
                                            class="control-label pull-left"
                                            for="Name"
                                            >Subject/Description</label
                                        >
                                    </div>
                                    <div class="col-sm-9">
                                        <input
                                            v-model="comms.subject"
                                            type="text"
                                            class="form-control"
                                            name="subject"
                                            style="width: 70%"
                                            required
                                        />
                                        <div class="invalid-feedback">
                                            Please enter a subject/description.
                                        </div>
                                    </div>
                                </div>
                            </div>
                            <div class="form-group">
                                <div class="row mb-3">
                                    <div class="col-sm-3">
                                        <label
                                            class="control-label pull-left"
                                            for="Name"
                                            >Text</label
                                        >
                                    </div>
                                    <div class="col-sm-9">
                                        <textarea
                                            v-model="comms.text"
                                            name="text"
                                            class="form-control"
                                            style="width: 70%"
                                            required
                                        ></textarea>
                                        <div class="invalid-feedback">
                                            Please enter text.
                                        </div>
                                    </div>
                                </div>
                            </div>
                            <div class="form-group">
                                <div class="row mb-3 border-top pt-3">
                                    <div class="col-sm-3">
                                        <label
                                            class="control-label pull-left"
                                            for="Name"
                                            >Attachments</label
                                        >
                                    </div>
                                    <div class="col-sm-9">
                                        <button
                                            class="btn btn-primary btn-sm"
                                            @click.prevent="attachAnother"
                                        >
                                            <i class="bi bi-plus-lg"></i> Add
                                            Another File
                                        </button>
                                        <hr class="my-3" />
                                        <template
                                            v-for="(f, i) in files"
                                            :key="f.id"
                                        >
                                            <div class="input-group mb-2">
                                                <span
                                                    class="btn btn-primary btn-file"
                                                    :title="
                                                        f.file
                                                            ? 'Change file'
                                                            : 'Select file'
                                                    "
                                                >
                                                    <i class="bi bi-upload"></i>
                                                    {{
                                                        f.file
                                                            ? 'Change'
                                                            : 'Attach'
                                                    }}
                                                    <input
                                                        type="file"
                                                        @change="
                                                            uploadFile(
                                                                $event,
                                                                f
                                                            )
                                                        "
                                                    />
                                                </span>
                                                <input
                                                    type="text"
                                                    class="form-control"
                                                    :value="f.name"
                                                    readonly
                                                    placeholder="No file selected"
                                                />
                                                <button
                                                    v-if="
                                                        files.length > 1 ||
                                                        f.file
                                                    "
                                                    class="btn btn-danger"
                                                    type="button"
                                                    @click="removeFile(i)"
                                                    title="Remove file"
                                                >
                                                    <i class="bi bi-trash"></i>
                                                </button>
                                            </div>
                                        </template>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </form>
                </div>
            </div>
        </modal>
    </div>
</template>

<script>
import modal from '@vue-utils/bootstrap-modal.vue';
import alert from '@vue-utils/alert.vue';
export default {
    name: 'AddComms',
    components: {
        modal,
        alert,
    },
    props: {
        url: {
            type: String,
            required: true,
        },
    },
    data: function () {
        return {
            isModalOpen: false,
            form: null,
            comms: {
                type: '',
            },
            state: 'proposed_approval',
            addingComms: false,
            validation_form: null,
            errorString: '',
            successString: '',
            success: false,
            datepickerOptions: {
                format: 'DD/MM/YYYY',
                showClear: true,
                useCurrent: false,
                keepInvalid: true,
                allowInputToggle: true,
            },
            files: [
                {
                    id: 1,
                    file: null,
                    name: '',
                },
            ],
            originalComms: null,
            fileIdCounter: 1,
        };
    },
    watch: {
        isModalOpen: function (val) {
            let vm = this;
            if (val) {
                vm.$nextTick(function () {
                    vm.$refs.to.focus();
                    vm.originalComms = JSON.parse(JSON.stringify(vm.comms));
                });
            }
        },
    },
    mounted: function () {
        let vm = this;
        vm.form = document.forms.commsForm;
    },
    methods: {
        hasUnsavedChanges: function () {
            return (
                JSON.stringify(this.comms) !==
                JSON.stringify(this.originalComms)
            );
        },
        ok: function () {
            let vm = this;
            if (vm.form.checkValidity()) {
                vm.sendData();
            } else {
                vm.form.classList.add('was-validated');
            }
        },
        uploadFile(e, file_obj) {
            let _file = null;
            var input = e.target;
            if (input.files && input.files[0]) {
                var reader = new FileReader();
                reader.readAsDataURL(input.files[0]);
                reader.onload = function (e) {
                    _file = e.target.result;
                };
                _file = input.files[0];
            }
            file_obj.file = _file;
            file_obj.name = _file.name;
        },
        removeFile(index) {
            this.files.splice(index, 1);
            if (this.files.length === 0) {
                this.attachAnother();
            }
        },
        attachAnother() {
            this.fileIdCounter++;
            this.files.push({
                id: this.fileIdCounter,
                file: null,
                name: '',
            });
        },
        cancel: function () {
            this.close();
        },
        close: function () {
            let vm = this;
            this.isModalOpen = false;
            this.comms = {};
            this.errorString = '';
            $('.has-error').removeClass('has-error');
            $(vm.form).removeClass('was-validated');
            this.files = [];
            this.attachAnother();
        },
        sendData: function () {
            let vm = this;
            vm.errorString = '';
            let comms = new FormData(vm.form);
            for (let i = 0; i < vm.files.length; i++) {
                comms.append('files', vm.files[i].file);
            }
            vm.addingComms = true;
            fetch(vm.url, {
                method: 'POST',
                body: comms,
            }).then(async (response) => {
                const data = await response.json();
                if (!response.ok) {
                    vm.addingComms = false;
                    vm.errorString = data;
                    return;
                }
                vm.addingComms = false;
                vm.close();
            });
        },
    },
};
</script>

<style lang="css">
.btn-file {
    position: relative;
    overflow: hidden;
}

.btn-file input[type='file'] {
    position: absolute;
    top: 0;
    right: 0;
    min-width: 100%;
    min-height: 100%;
    font-size: 100px;
    text-align: right;
    filter: alpha(opacity=0);
    opacity: 0;
    outline: none;
    background: white;
    cursor: inherit;
    display: block;
}

.top-buffer {
    margin-top: 5px;
}

.top-buffer-2x {
    margin-top: 10px;
}

input[type='text'],
select {
    padding: 0.375rem 2.25rem 0.375rem 0.75rem;
}

.truncate-text {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    display: block;
}

input[type='text'],
select {
    width: 100%;
    padding: 0.375rem 2.25rem 0.375rem 0.75rem;
}
</style>
