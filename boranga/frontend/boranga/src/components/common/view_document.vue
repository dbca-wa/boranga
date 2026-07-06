<template>
    <div id="view-document">
        <modal
            transition="modal fade"
            :show-o-k="false"
            cancel-text="Close"
            :title="title"
            large
            :data-loss-warning-on-cancel="false"
            @cancel="close()"
        >
            <form
                v-if="document"
                class="form-horizontal px-3 pt-2"
                name="documentForm"
            >
                <div class="row border-bottom mb-2 pb-2">
                    <label for="" class="col-sm-3 col-form-label fw-bold"
                        >Document Number</label
                    >
                    <div class="col form-control-plaintext">
                        {{ document.document_number }}
                    </div>
                </div>
                <div
                    v-if="document.document_category_name"
                    class="row border-bottom mb-2 pb-2"
                >
                    <label for="" class="col-sm-3 col-form-label fw-bold"
                        >Category</label
                    >
                    <div class="col form-control-plaintext">
                        {{ document.document_category_name }}
                    </div>
                </div>
                <div
                    v-if="document.document_sub_category_name"
                    class="row border-bottom mb-2 pb-2"
                >
                    <label for="" class="col-sm-3 col-form-label fw-bold"
                        >Sub Category</label
                    >
                    <div class="col form-control-plaintext">
                        {{ document.document_sub_category_name }}
                    </div>
                </div>
                <div class="row border-bottom mb-2 pb-2">
                    <label for="" class="col-sm-3 col-form-label fw-bold"
                        >Description</label
                    >
                    <div class="col form-control-plaintext">
                        {{ document.description }}
                    </div>
                </div>
                <div class="row border-bottom mb-2 pb-2">
                    <label for="" class="col-sm-3 col-form-label fw-bold"
                        >Document</label
                    >
                    <div class="col form-control-plaintext">
                        <a :href="document._file" target="_blank">{{
                            document.name
                        }}</a>
                    </div>
                </div>
                <div
                    class="row pb-2"
                    :class="is_internal ? 'mb-2 border-bottom' : ''"
                >
                    <label for="" class="col-sm-3 col-form-label fw-bold"
                        >Date Uploaded</label
                    >
                    <div class="col form-control-plaintext">
                        {{
                            new Date(
                                document.uploaded_date
                            ).toLocaleDateString()
                        }}
                        at
                        {{
                            new Date(
                                document.uploaded_date
                            ).toLocaleTimeString()
                        }}
                    </div>
                </div>
                <div v-if="is_internal" class="row border-bottom mb-2 pb-2">
                    <label for="" class="col-sm-3 col-form-label fw-bold"
                        >Active</label
                    >
                    <div class="col form-control-plaintext">
                        {{ document.active ? 'Yes' : 'No' }}
                    </div>
                </div>
                <div v-if="is_internal" class="row pb-3">
                    <label for="" class="col-sm-3 col-form-label fw-bold"
                        >Can Submitter Access</label
                    >
                    <div class="col form-control-plaintext">
                        {{ document.can_submitter_access ? 'Yes' : 'No' }}
                    </div>
                </div>
            </form>
        </modal>
    </div>
</template>

<script>
import modal from '@vue-utils/bootstrap-modal.vue';

export default {
    name: 'ViewDocument',
    components: {
        modal,
    },
    props: {
        is_internal: {
            type: Boolean,
            default: false,
        },
        titleProperty: {
            type: String,
            default: 'document_number',
        },
    },
    data() {
        return {
            document: null,
            isModalOpen: false,
        };
    },
    computed: {
        title() {
            let documentNumber = this.document
                ? this.document[this.titleProperty]
                : '';
            return `View Document ${documentNumber}`;
        },
    },
    methods: {
        close() {
            this.document = null;
            this.isModalOpen = false;
        },
    },
};
</script>
