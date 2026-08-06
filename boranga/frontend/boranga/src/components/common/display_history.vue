<template lang="html">
    <div id="displayHistory">
        <modal
            transition="modal fade"
            :title="
                primary_model +
                ' ' +
                primary_model_number +
                ' History - Revision ' +
                revision_sequence
            "
            :large="true"
            :full="true"
            :show-o-k="false"
            cancel-text="Close"
            :data-loss-warning-on-cancel="false"
            @cancel="close()"
        >
            <div>
                <div v-if="revision_sequence > 0">
                    <strong>Modified By:</strong> {{ revision_user }}
                </div>
                <div v-else>
                    <strong>Created By:</strong> {{ revision_user }}
                </div>
                <div v-if="revision_sequence > 0">
                    <strong>Date Modified:</strong> {{ revision_date }}
                </div>
                <div v-else>
                    <strong>Date Created:</strong> {{ revision_date }}
                </div>
                <div>
                    <label for="checkbox" class="control-label"
                        ><strong>Show Null Fields:&nbsp;</strong></label
                    >
                    <input
                        id="checkbox"
                        v-model="showNullFields"
                        type="checkbox"
                        class="form-check-input"
                        @change="formatHistoryData()"
                    />
                </div>
                <div>
                    <strong>Data:</strong>
                </div>
                <div v-if="loading" class="text-center py-4">
                    <span
                        class="spinner-border spinner-border-sm text-primary me-2"
                        role="status"
                        aria-hidden="true"
                    ></span>
                    Loading…
                </div>
                <textarea
                    v-else
                    disabled
                    class="form-control"
                    rows="25"
                    v-model="version_data_formatted_json"
                ></textarea>
            </div>
        </modal>
    </div>
</template>

<script>
import modal from '@vue-utils/bootstrap-modal.vue';
import { api_endpoints } from '@/utils/hooks';

export default {
    name: 'DisplayHistory',
    components: {
        modal,
    },
    props: {
        primary_model: {
            type: String,
            required: true,
        },
        primary_model_number: {
            type: String,
            required: true,
        },
        revision_id: {
            type: Number,
            required: true,
        },
        revision_sequence: {
            type: Number,
            required: true,
        },
    },

    data: function () {
        return {
            isModalOpen: false,
            errorString: '',
            loading: false,
            version_data: [],
            version_data_formatted: [],
            revision_date: '',
            revision_user: '',
            showNullFields: false,
        };
    },
    mounted: function () {
        let vm = this;
        this.$nextTick(() => {
            vm.fetchHistoryData();
        });
    },
    computed: {
        version_data_formatted_json: function () {
            return JSON.stringify(this.version_data_formatted, null, '\t');
        },
    },
    methods: {
        close: function () {
            this.errorString = '';
            this.isModalOpen = false;
            $('.has-error').removeClass('has-error');
        },
        removeNullValues: function (values) {
            let vm = this;
            const result = {};
            for (const key in values) {
                if (
                    values[key] !== null &&
                    values[key] !== undefined &&
                    typeof values[key] !== 'string' &&
                    (values[key].isArray || typeof values[key] === 'object')
                ) {
                    let subResult = vm.removeNullValues(values[key]);
                    if (Object.keys(subResult).length > 0) {
                        result[key] = subResult;
                    }
                } else if (
                    values[key] !== null &&
                    values[key] !== undefined &&
                    values[key] !== ''
                ) {
                    result[key] = values[key];
                }
            }
            return result;
        },
        formatHistoryData: function () {
            let vm = this;
            if (this.showNullFields) {
                vm.version_data_formatted = vm.version_data;
            } else {
                vm.version_data_formatted = vm.removeNullValues(
                    vm.version_data
                );
            }
        },
        fetchHistoryData: function () {
            let vm = this;
            vm.loading = true;
            fetch(
                api_endpoints.lookup_revision_versions(
                    vm.primary_model,
                    vm.revision_id
                )
            ).then(
                async (response) => {
                    const data = await response.json();
                    vm.revision_date = data['date_created'];
                    vm.revision_user = data['revision_user'];
                    vm.version_data = data['version_data'];
                    vm.formatHistoryData();
                    vm.loading = false;
                },
                (error) => {
                    vm.loading = false;
                    console.log(error);
                }
            );
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
    width: inherit; /* Or auto */
    padding: 0 10px; /* To give a bit of padding on the left and right */
    border-bottom: none;
}
input[type='text'],
select {
    width: 100%;
    padding: 0.375rem 2.25rem 0.375rem 0.75rem;
}
input[type='number'] {
    width: 50%;
}
.interval-margin {
    margin-right: -100px;
}
.interval-range-true-input {
    width: 20%;
    margin-left: -80px;
}
.interval-range-true-choice {
    width: 20%;
}
</style>
