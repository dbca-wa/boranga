<template lang="html">
    <div id="communityOccurrence">
        <FormSection
            :form-collapse="false"
            label="Occurrence"
            Index="occurrence"
        >
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label fw-bold"
                    >Occurrence Name: <span class="text-danger">*</span></label
                >
                <div class="col-sm-9">
                    <textarea
                        id="occurrence_name"
                        v-model="occurrence_obj.occurrence_name"
                        class="form-control"
                        :disabled="isReadOnly"
                        rows="1"
                        placeholder=""
                    />
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label fw-bold"
                    >Community Name: <span class="text-danger">*</span></label
                >
                <div :id="select_community_name" class="col-sm-9">
                    <select
                        :id="community_name_lookup"
                        :ref="community_name_lookup"
                        :disabled="isReadOnly"
                        :name="community_name_lookup"
                        class="form-select"
                    />
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label"
                    >Occurrence Source:</label
                >
                <div
                    v-for="source in occurrence_source_list"
                    :key="source[0]"
                    class="col-sm-auto"
                >
                    <input
                        :id="source[1]"
                        v-model="occurrence_obj.occurrence_source"
                        :disabled="isReadOnly"
                        type="checkbox"
                        :value="source[0]"
                    />
                    {{ source[1] }}
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label"
                    >Wild Status:</label
                >
                <div class="col-sm-9">
                    <template
                        v-if="
                            wild_status_list &&
                            wild_status_list.length > 0 &&
                            occurrence_obj.wild_status &&
                            !wild_status_list
                                .map((d) => d.id)
                                .includes(occurrence_obj.wild_status)
                        "
                    >
                        <input
                            v-if="occurrence_obj.wild_status_name"
                            type="text"
                            class="form-control mb-3"
                            :value="
                                occurrence_obj.wild_status_name +
                                ' (Now Archived)'
                            "
                            disabled
                        />
                        <div
                            v-if="occurrence_obj.wild_status"
                            class="mb-3 text-muted"
                        >
                            Change wild status to:
                        </div>
                    </template>
                    <select
                        v-model="occurrence_obj.wild_status"
                        :disabled="isReadOnly"
                        class="form-select"
                    >
                        <option :value="null" selected disabled>
                            Select Wild Status
                        </option>
                        <option
                            v-for="option in wild_status_list"
                            :key="option.id"
                            :value="option.id"
                        >
                            {{ option.name }}
                        </option>
                    </select>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Comments:</label>
                <div class="col-sm-9">
                    <textarea
                        id="occurrence_comments"
                        v-model="occurrence_obj.comment"
                        class="form-control"
                        :disabled="isReadOnly"
                        placeholder=""
                    />
                </div>
            </div>
            <ContactDatatable
                ref="contact_datatable"
                :occurrence_obj="occurrence_obj"
                :is-read-only="isReadOnly"
                @refresh-occurrence="refreshOccurrence()"
            >
            </ContactDatatable>
            <RelatedReports
                :is-read-only="isReadOnly"
                :occurrence_obj="occurrence_obj"
                label="Related Occurrence Reports"
            />
        </FormSection>
    </div>
</template>

<script>
import FormSection from '@/components/forms/section_toggle.vue';
import ContactDatatable from './contact_datatable.vue';
import RelatedReports from '@/components/common/occurrence/occ_related_ocr_table.vue';
import { api_endpoints, helpers } from '@/utils/hooks';

export default {
    name: 'CommunityOccurrence',
    components: {
        FormSection,
        RelatedReports,
        ContactDatatable,
    },
    props: {
        occurrence_obj: {
            type: Object,
            required: true,
        },
    },
    data: function () {
        let vm = this;
        return {
            community_name_lookup:
                'community_name_lookup' + vm.occurrence_obj.id,
            select_community_name:
                'select_community_name' + vm.occurrence_obj.id,
            occ_profile_dict: {},
            wild_status_list: [],
            occurrence_source_list: [],
        };
    },
    computed: {
        isReadOnly: function () {
            return !this.occurrence_obj.can_user_edit;
        },
    },
    watch: {},
    created: async function () {
        let vm = this;
        let action = this.$route.query.action;
        let dict_url =
            action == 'view'
                ? api_endpoints.occ_profile_dict +
                  '?group_type=' +
                  vm.occurrence_obj.group_type_id +
                  '&action=' +
                  action
                : api_endpoints.occ_profile_dict +
                  '?group_type=' +
                  vm.occurrence_obj.group_type_id;
        fetch(dict_url).then(
            async (response) => {
                vm.occ_profile_dict = await response.json();
                vm.wild_status_list = vm.occ_profile_dict.wild_status_list;
                vm.occurrence_source_list =
                    vm.occ_profile_dict.occurrence_source_list;
                this.getCommunityDisplay();
            },
            (error) => {
                console.log(error);
            }
        );
    },
    mounted: function () {
        let vm = this;
        this.$nextTick(() => {
            vm.initialiseCommunityNameLookup();
        });
    },
    methods: {
        initialiseCommunityNameLookup: function () {
            let vm = this;
            $(vm.$refs[vm.community_name_lookup])
                .select2({
                    minimumInputLength: 2,
                    dropdownParent: $('#' + vm.select_community_name),
                    theme: 'bootstrap-5',
                    allowClear: true,
                    placeholder: 'Select Community Name',
                    ajax: {
                        url: api_endpoints.community_name_lookup,
                        dataType: 'json',
                        data: function (params) {
                            var query = {
                                term: params.term,
                                type: 'public',
                                group_type_id: vm.occurrence_obj.group_type_id,
                                cs_community: true,
                            };
                            return query;
                        },
                    },
                })
                .on('select2:select', function (e) {
                    let data = e.params.data.id;
                    vm.occurrence_obj.community = data;
                    vm.occurrence_obj.community_id = e.params.data.id;
                    vm.community_display = e.params.data.text;
                    vm.taxon_previous_name = e.params.data.taxon_previous_name;
                })
                .on('select2:unselect', function () {
                    vm.occurrence_obj.community_id = null;
                    vm.community_display = '';
                    vm.taxon_previous_name = '';
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-' +
                            vm.community_name_lookup +
                            '-results"]'
                    );
                    searchField[0].focus();
                })
                .on('select2:selecting select2:unselecting', function (e) {
                    const hasRelatedReports =
                        vm.occurrence_obj.id &&
                        vm.occurrence_obj.occurrence_reports &&
                        vm.occurrence_obj.occurrence_reports.length > 0;
                    helpers.confirmChangeOfSpeciesOrCommunityName(
                        e,
                        vm.$refs[vm.community_name_lookup],
                        'community',
                        hasRelatedReports
                    );
                });
        },
        getCommunityDisplay: function () {
            let vm = this;
            if (vm.occurrence_obj?.community_id) {
                let community_display_url =
                    api_endpoints.community_display +
                    '?community_id=' +
                    vm.occurrence_obj.community_id;
                fetch(community_display_url).then(async (response) => {
                    const data = await response.json();
                    var newOption = new Option(data.name, data.id, false, true);
                    $('#' + vm.community_name_lookup).append(newOption);
                    vm.community_display = data.name;
                });
            }
        },
    },
};
</script>

<style lang="css" scoped>
fieldset.scheduler-border {
    border: 1px groove #ddd !important;
    padding: 0 1.4em 1.4em 1.4em !important;
    margin: 0 0 1.5em 0 !important;
    -webkit-box-shadow: 0px 0px 0px 0px #000;
    box-shadow: 0px 0px 0px 0px #000;
}

legend.scheduler-border {
    width: inherit;
    /* Or auto */
    padding: 0 10px;
    /* To give a bit of padding on the left and right */
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
</style>
