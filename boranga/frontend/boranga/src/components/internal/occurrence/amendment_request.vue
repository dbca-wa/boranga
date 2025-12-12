<template lang="html">
    <div id="internal-ocr-amendment-request">
        <modal
            id="amendment-request-modal"
            transition="modal fade"
            title="Amendment Request"
            large
            @ok="ok()"
            @cancel="cancel()"
        >
            <div class="container-fluid">
                <div class="row">
                    <form class="form-horizontal" name="amendForm">
                        <alert v-if="errorString" type="danger"
                            ><strong>{{ errorString }}</strong></alert
                        >
                        <div class="col-sm-12">
                            <div class="row mb-3">
                                <div class="col-sm-6">
                                    <div class="form-group">
                                        <label class="control-label" for="Name"
                                            >Reason</label
                                        >
                                        <select
                                            ref="reason"
                                            v-model="amendment.reason"
                                            class="form-select"
                                            name="reason"
                                        >
                                            <option
                                                v-for="r in reason_choices"
                                                :value="r.key"
                                                :key="r.key"
                                            >
                                                {{ r.value }}
                                            </option>
                                        </select>
                                    </div>
                                </div>
                            </div>
                            <div class="row mb-3">
                                <div class="col">
                                    <div class="form-group">
                                        <label
                                            class="control-label"
                                            for="amendment_text"
                                            >Details</label
                                        >
                                        <textarea
                                            id="amendment_text"
                                            ref="amendment_text"
                                            v-model="amendment.text"
                                            class="form-control"
                                            name="amendment_text"
                                        ></textarea>
                                    </div>
                                </div>
                            </div>
                            <div class="row">
                                <div class="col">
                                    <label
                                        class="control-label"
                                        for="amendment_request_file"
                                        >Documents</label
                                    >
                                    <FileField2
                                        ref="filefield"
                                        :uploaded_documents="
                                            amendment.amendment_request_documents
                                        "
                                        :delete_url="delete_url"
                                        :proposal_id="occurrence_report_id"
                                        :is-repeatable="true"
                                        name="amendment_request_file"
                                    />
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
import FileField2 from '@/components/forms/filefield.vue';

export default {
    name: 'AmendmentRequest',
    components: {
        modal,
        alert,
        FileField2,
    },
    props: {
        occurrence_report_id: {
            type: Number,
        },
    },
    data: function () {
        let vm = this;
        return {
            isModalOpen: false,
            form: null,
            amendment: {
                reason: '',
                reason_id: null,
                text: null,
                amendingProposal: false,
                occurrence_report: vm.occurrence_report_id,
                num_files: 0,
                input_name: 'amendment_request_doc',
                amendment_request_documents: [],
            },
            reason_choices: {},
            errorString: '',
            validation_form: null,
            originalAmendment: null,
        };
    },
    computed: {
        delete_url: function () {
            return this.amendment.id
                ? '/api/ocr_amendment_request/' +
                      this.amendment.id +
                      '/delete_document/'
                : '';
        },
    },
    watch: {
        isModalOpen: function (val) {
            if (val) {
                this.$nextTick(() => {
                    $(this.$refs.reason).select2('open');
                });
                this.originalAmendment = JSON.parse(
                    JSON.stringify(this.amendment)
                );
            }
        },
    },
    mounted: function () {
        let vm = this;
        vm.form = document.forms.amendForm;
        vm.fetchAmendmentChoices();
        vm.addFormValidations();
        this.$nextTick(() => {
            vm.eventListerners();
        });
    },
    methods: {
        hasUnsavedChanges: function () {
            return (
                JSON.stringify(this.amendment) !==
                JSON.stringify(this.originalAmendment)
            );
        },
        ok: function () {
            let vm = this;
            if ($(vm.form).valid()) {
                vm.sendData();
            }
        },
        cancel: function () {
            let vm = this;
            vm.close();
        },
        close: function () {
            this.isModalOpen = false;
            this.amendment = {
                reason: '',
                reason_id: null,
                occurrence_report: this.occurrence_report_id,
            };
            this.errorString = '';
            $(this.$refs.reason).val(null).trigger('change');
            $('.has-error').removeClass('has-error');

            this.validation_form.resetForm();
        },
        fetchAmendmentChoices: function () {
            let vm = this;
            fetch('/api/proposal_amendment_request_reason_choices.json').then(
                async (response) => {
                    vm.reason_choices = await response.json();
                },
                (error) => {
                    console.log(error);
                }
            );
        },
        sendData: function () {
            let vm = this;
            vm.errorString = '';
            let amendment = JSON.parse(JSON.stringify(vm.amendment));
            let formData = new FormData();
            var files = vm.$refs.filefield.files;
            $.each(files, function (idx, v) {
                var file = v['file'];
                var filename = v['name'];
                var name = 'file-' + idx;
                formData.append(name, file, filename);
            });
            amendment.num_files = files.length;
            amendment.input_name = 'amendment_request_doc';
            amendment.occurrence_report = vm.occurrence_report_id;
            amendment.update = true;
            formData.append('data', JSON.stringify(amendment));

            fetch('/api/ocr_amendment_request.json', {
                method: 'POST',
                body: formData,
            })
                .then(async (response) => {
                    const data = await response.json();
                    if (!response.ok) {
                        vm.errorString = data;
                        return;
                    }
                    swal.fire({
                        title: 'Sent',
                        text: 'An email has been sent to proponent with the request to amend this Proposal',
                        icon: 'success',
                        customClass: {
                            confirmButton: 'btn btn-primary',
                        },
                    });
                    vm.amendingProposal = true;
                    vm.close();

                    fetch(
                        `/api/occurrence_report/${vm.occurrence_report_id}`
                    ).then(async (response) => {
                        const data = await response.json();
                        if (!response.ok) {
                            vm.errorString = data;
                            return;
                        }
                        vm.$emit('refreshFromResponse', data);
                        vm.$router.push({ path: '/internal/occurrence' }); //Navigate to dashboard after creating Amendment request
                    });
                })
                .finally(() => {
                    vm.amendingProposal = false;
                });
        },
        addFormValidations: function () {
            let vm = this;
            vm.validation_form = $(vm.form).validate({
                rules: {
                    reason: 'required',
                },
                messages: {
                    reason: 'field is required',
                },
                showErrors: function (errorMap, errorList) {
                    $.each(this.validElements(), function (index, element) {
                        var $element = $(element);
                        $element
                            .attr('data-original-title', '')
                            .parents('.form-group')
                            .removeClass('has-error');
                    });
                    // add or update tooltips
                    for (var i = 0; i < errorList.length; i++) {
                        var error = errorList[i];
                        $(error.element)
                            .tooltip({
                                trigger: 'focus',
                            })
                            .attr('data-original-title', error.message)
                            .parents('.form-group')
                            .addClass('has-error');
                    }
                },
            });
        },
        eventListerners: function () {
            let vm = this;

            // Intialise select2
            $(vm.$refs.reason)
                .select2({
                    theme: 'bootstrap-5',
                    allowClear: true,
                    placeholder: 'Select Reason',
                    dropdownParent: $(
                        'div#internal-ocr-amendment-request .modal-body'
                    ),
                })
                .on('select2:select', function (e) {
                    var selected = $(e.currentTarget);
                    vm.amendment.reason = selected.val();
                    vm.amendment.reason_id = selected.val();
                })
                .on('select2:unselect', function (e) {
                    var selected = $(e.currentTarget);
                    vm.amendment.reason = selected.val();
                    vm.amendment.reason_id = selected.val();
                });
        },
    },
};
</script>
