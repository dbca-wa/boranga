<template lang="html">
    <div id="occurrenceCombine">
        <modal
            transition="modal fade"
            :title="'Combine Occurrences'"
            :extra-large="true"
            :full="true"
            :show-o-k="false"
            cancel-text="Cancel"
            @cancel="close()"
        >
            <!--OCC Selection Drop Down-->
            <div id="occurrence_name_lookup_form_group_id" class="row mb-3">
                <label
                    class="col-sm-3 control-label"
                    for="occurrence_name_lookup"
                >
                    Select Occurrence:
                </label>
                <div class="col-sm-6">
                    <select
                        id="occurrence_name_lookup"
                        ref="occurrence_name_lookup"
                        name="occurrence_name_lookup"
                        class="form-control"
                    />
                </div>
            </div>
            <!--OCC Selection Table-->
            <div class="row mb-3">
                <OccurrenceCombineSelect
                    :selected-occurrences="selectedOccurrences"
                    :selected-occurrence-ids="selectedOccurrenceIds"
                    :main-occurrence-id="main_occurrence_obj.id"
                />
            </div>
            <template v-if="occ_combine_data.combine_ids.length >= 2">
                <alert
                    type="primary"
                    class="d-flex align-items-center py-1 my-4"
                >
                    <div class="float-start pe-3">
                        <span class="align-middle"
                            ><i
                                class="bi bi-info-circle-fill text-primary fs-4"
                            ></i
                        ></span>
                    </div>
                    <ul class="list-group list-group-flush">
                        <li class="list-group-item bg-transparent">
                            Browse the contents of each to tab to select the
                            data you would like to combine into Occurrence
                            {{ main_occurrence_obj.occurrence_number }}
                        </li>
                        <li class="list-group-item bg-transparent">
                            Once you are finished click the 'Finalise Combine'
                            tab to completed the process.
                        </li>
                    </ul>
                </alert>

                <!--OCC Tabs-->
                <div class="row mb-3">
                    <div class="col-md-12">
                        <ul id="pills-tab" class="nav nav-pills" role="tablist">
                            <li class="nav-item">
                                <a
                                    id="pills-occurrence-tab"
                                    class="nav-link active"
                                    data-bs-toggle="pill"
                                    :href="'#' + occurrenceBody"
                                    role="tab"
                                    :aria-controls="occurrenceBody"
                                    aria-selected="true"
                                >
                                    Occurrence
                                </a>
                            </li>
                            <li class="nav-item">
                                <a
                                    id="pills-location-tab"
                                    class="nav-link"
                                    data-bs-toggle="pill"
                                    :href="'#' + locationBody"
                                    role="tab"
                                    aria-selected="false"
                                >
                                    Location
                                </a>
                            </li>
                            <li class="nav-item">
                                <a
                                    id="pills-habitat-tab"
                                    class="nav-link"
                                    data-bs-toggle="pill"
                                    :href="'#' + habitatBody"
                                    role="tab"
                                    aria-selected="false"
                                >
                                    Habitat
                                </a>
                            </li>
                            <li class="nav-item">
                                <a
                                    id="pills-observation-tab"
                                    class="nav-link"
                                    data-bs-toggle="pill"
                                    :href="'#' + observationBody"
                                    role="tab"
                                    aria-selected="false"
                                >
                                    Observation
                                </a>
                            </li>
                            <li class="nav-item">
                                <a
                                    id="pills-documents-tab"
                                    class="nav-link"
                                    data-bs-toggle="pill"
                                    :href="'#' + documentBody"
                                    role="tab"
                                    aria-selected="false"
                                >
                                    Documents
                                </a>
                            </li>
                            <li class="nav-item">
                                <a
                                    id="pills-threats-tab"
                                    class="nav-link"
                                    data-bs-toggle="pill"
                                    :href="'#' + threatBody"
                                    role="tab"
                                    aria-selected="false"
                                >
                                    Threats
                                </a>
                            </li>
                            <li class="nav-item">
                                <a
                                    id="pills-finalise-combine-tab"
                                    class="nav-link"
                                    data-bs-toggle="pill"
                                    href="'#finalise-occurrence-combine"
                                    role="tab"
                                    aria-selected="false"
                                    ><i class="bi bi-check2-circle"></i>
                                    Finalise Combine
                                </a>
                            </li>
                        </ul>
                        <div id="pills-tabContent" class="tab-content">
                            <div
                                :id="occurrenceBody"
                                class="tab-pane fade show active"
                                role="tabpanel"
                                aria-labelledby="pills-occurrence-tab"
                            >
                                <!--Main OCC Form-->
                                <FormSection
                                    :form-collapse="false"
                                    label="Occurrence"
                                    Index="combine_occurrence"
                                >
                                    <div class="row mb-3">
                                        <label
                                            for=""
                                            class="col-sm-3 control-label"
                                            >Occurrence Name:</label
                                        >
                                        <div class="col-sm-9">
                                            <textarea
                                                v-model="
                                                    main_occurrence_obj.occurrence_name
                                                "
                                                disabled
                                                class="form-control"
                                                rows="1"
                                            />
                                        </div>
                                    </div>
                                    <div
                                        v-if="
                                            main_occurrence_obj.group_type ==
                                                'flora' ||
                                            main_occurrence_obj.group_type ==
                                                'fauna'
                                        "
                                        class="row mb-3"
                                    >
                                        <label
                                            for=""
                                            class="col-sm-3 control-label"
                                            >Scientific Name:</label
                                        >
                                        <div class="col-sm-9">
                                            <textarea
                                                v-model="
                                                    main_occurrence_obj.scientific_name
                                                "
                                                disabled
                                                class="form-control"
                                                rows="1"
                                            />
                                        </div>
                                    </div>
                                    <div v-else class="row mb-3">
                                        <label
                                            for=""
                                            class="col-sm-3 control-label"
                                            >Community Name:</label
                                        >
                                        <div class="col-sm-9">
                                            <textarea
                                                v-model="
                                                    main_occurrence_obj.community_name
                                                "
                                                disabled
                                                class="form-control"
                                                rows="1"
                                            />
                                        </div>
                                    </div>
                                    <div class="row mb-3">
                                        <label
                                            for=""
                                            class="col-sm-3 control-label"
                                            >Occurrence Source:</label
                                        >
                                        <div class="col-sm-9">
                                            <select
                                                id="combine_occurrence_source"
                                                ref="combine_occurrence_source"
                                                :key="occ_form_key"
                                                v-model="
                                                    occ_combine_data.occurrence_source
                                                "
                                                class="form-select"
                                            >
                                                <option
                                                    v-for="occurrence in selectedOccurrences"
                                                    :key="occurrence.id"
                                                    :value="occurrence.id"
                                                >
                                                    {{
                                                        occurrence.occurrence_number
                                                    }}:
                                                    <span
                                                        v-for="source in occurrence.occurrence_source"
                                                        :key="source"
                                                    >
                                                        <span
                                                            v-for="(
                                                                occ_source, i
                                                            ) in occurrence_source_list"
                                                            :key="i"
                                                        >
                                                            <span
                                                                v-if="
                                                                    occ_source[0] ==
                                                                    source
                                                                "
                                                                >{{
                                                                    occ_source[1]
                                                                }}</span
                                                            >
                                                        </span>
                                                        <span
                                                            v-if="
                                                                source !=
                                                                occurrence
                                                                    .occurrence_source[
                                                                    occurrence
                                                                        .occurrence_source
                                                                        .length -
                                                                        1
                                                                ]
                                                            "
                                                            >,
                                                        </span>
                                                    </span>
                                                </option>
                                            </select>
                                        </div>
                                    </div>
                                    <div class="row mb-3">
                                        <label
                                            for=""
                                            class="col-sm-3 control-label"
                                            >Wild Status:</label
                                        >
                                        <div class="col-sm-9">
                                            <select
                                                id="combine_wild_status"
                                                ref="combine_wild_status"
                                                :key="occ_form_key"
                                                v-model="
                                                    occ_combine_data.wild_status
                                                "
                                                class="form-select"
                                            >
                                                <option
                                                    v-for="occurrence in selectedOccurrences"
                                                    :key="occurrence.id"
                                                    :value="occurrence.id"
                                                >
                                                    {{
                                                        occurrence.occurrence_number
                                                    }}:
                                                    <span
                                                        v-for="wild_status in wild_status_list"
                                                        :key="wild_status.id"
                                                    >
                                                        <span
                                                            v-if="
                                                                wild_status.id ==
                                                                occurrence.wild_status
                                                            "
                                                            >{{
                                                                wild_status.name
                                                            }}</span
                                                        >
                                                    </span>
                                                </option>
                                            </select>
                                        </div>
                                    </div>
                                    <div class="row mb-3">
                                        <label
                                            for=""
                                            class="col-sm-3 control-label"
                                            >Review Due Date:</label
                                        >
                                        <div class="col-sm-9">
                                            <select
                                                id="combine_review_due_date"
                                                ref="combine_review_due_date"
                                                :key="occ_form_key"
                                                v-model="
                                                    occ_combine_data.review_due_date
                                                "
                                                class="form-select"
                                            >
                                                <option
                                                    v-for="occurrence in selectedOccurrences"
                                                    :key="occurrence.id"
                                                    :value="occurrence.id"
                                                >
                                                    {{
                                                        occurrence.occurrence_number
                                                    }}:
                                                    {{
                                                        occurrence.review_due_date
                                                    }}
                                                </option>
                                            </select>
                                        </div>
                                    </div>
                                    <div class="row mb-3">
                                        <label
                                            for=""
                                            class="col-sm-3 control-label"
                                            >Comment:</label
                                        >
                                        <div class="col-sm-9">
                                            <select
                                                id="combine_comment"
                                                ref="combine_comment"
                                                :key="occ_form_key"
                                                v-model="
                                                    occ_combine_data.comment
                                                "
                                                class="form-select"
                                            >
                                                <option
                                                    v-for="occurrence in selectedOccurrences"
                                                    :key="occurrence.id"
                                                    :value="occurrence.id"
                                                >
                                                    {{
                                                        occurrence.occurrence_number
                                                    }}: {{ occurrence.comment }}
                                                </option>
                                            </select>
                                        </div>
                                    </div>
                                </FormSection>
                                <!--Key Contacts Table-->
                                <FormSection
                                    :form-collapse="true"
                                    label="Key Contacts"
                                    Index="combine_keyContacts"
                                    @toggle-collapse="toggleKeyContacts"
                                >
                                    <div class="row mb-3">
                                        <OccurrenceCombineContacts
                                            :key="contact_table_key"
                                            ref="key_contacts_section"
                                            :selected-key-contacts="
                                                key_contacts
                                            "
                                            :combine-key-contact-ids="
                                                occ_combine_data.combine_key_contact_ids
                                            "
                                            :main-occurrence-id="
                                                main_occurrence_obj.id
                                            "
                                        />
                                    </div>
                                </FormSection>
                            </div>
                            <div
                                :id="locationBody"
                                class="tab-pane fade"
                                role="tabpanel"
                                aria-labelledby="pills-location-tab"
                            >
                                <!--Location Form-->
                                <FormSection
                                    :form-collapse="true"
                                    label="Location"
                                    :subtitle="
                                        ' - OCC' +
                                        occ_combine_data.chosen_location_section
                                    "
                                    Index="combine_location"
                                >
                                    <div class="row mb-3">
                                        <OccurrenceCombineSelect
                                            :occ_chosen_section="
                                                occ_combine_data.chosen_location_section
                                            "
                                            :section_type="'location'"
                                            :selected-occurrences="
                                                selectedOccurrences
                                            "
                                            :selected-occurrence-ids="
                                                selectedOccurrenceIds
                                            "
                                            :main-occurrence-id="
                                                main_occurrence_obj.id
                                            "
                                            @update-chosen-section="
                                                updateChosenSection
                                            "
                                        />
                                    </div>
                                </FormSection>
                                <FormSection
                                    :form-collapse="true"
                                    label="Sites"
                                    Index="combine_sites"
                                    @toggle-collapse="toggleSites"
                                >
                                    <div class="row mb-3">
                                        <OccurrenceCombineSites
                                            :key="site_table_key"
                                            ref="sites_section"
                                            :selected-sites="sites"
                                            :combine-site-ids="
                                                occ_combine_data.combine_site_ids
                                            "
                                            :main-occurrence-id="
                                                main_occurrence_obj.id
                                            "
                                        />
                                    </div>
                                </FormSection>
                                <FormSection
                                    :form-collapse="true"
                                    label="Tenures"
                                    Index="combine_tenures"
                                    @toggle-collapse="toggleTenures"
                                >
                                    <div class="row mb-3">
                                        <OccurrenceCombineTenures
                                            :key="tenure_table_key"
                                            ref="tenures_section"
                                            :selected-tenures="tenures"
                                            :combine-tenure-ids="
                                                occ_combine_data.combine_tenure_ids
                                            "
                                            :main-occurrence-id="
                                                main_occurrence_obj.id
                                            "
                                        />
                                    </div>
                                </FormSection>
                            </div>
                            <div
                                :id="habitatBody"
                                class="tab-pane fade"
                                role="tabpanel"
                                aria-labelledby="pills-habitat-tab"
                            >
                                <!--Habitat Composition Form-->
                                <FormSection
                                    :form-collapse="true"
                                    label="Habitat Composition"
                                    :subtitle="
                                        ' - OCC' +
                                        occ_combine_data.chosen_habitat_composition_section
                                    "
                                    Index="combine_habitat_composition"
                                >
                                    <div class="row mb-3">
                                        <OccurrenceCombineSelect
                                            :occ_chosen_section="
                                                occ_combine_data.chosen_habitat_composition_section
                                            "
                                            :section_type="'habitat_composition'"
                                            :selected-occurrences="
                                                selectedOccurrences
                                            "
                                            :selected-occurrence-ids="
                                                selectedOccurrenceIds
                                            "
                                            :main-occurrence-id="
                                                main_occurrence_obj.id
                                            "
                                            @update-chosen-section="
                                                updateChosenSection
                                            "
                                        />
                                    </div>
                                </FormSection>
                                <!--Habitat Condition Form-->
                                <FormSection
                                    :form-collapse="true"
                                    label="Habitat Condition"
                                    :subtitle="
                                        ' - OCC' +
                                        occ_combine_data.chosen_habitat_condition_section
                                    "
                                    Index="combine_habitat_condition"
                                >
                                    <div class="row mb-3">
                                        <OccurrenceCombineSelect
                                            :occ_chosen_section="
                                                occ_combine_data.chosen_habitat_condition_section
                                            "
                                            :section_type="'habitat_condition'"
                                            :selected-occurrences="
                                                selectedOccurrences
                                            "
                                            :selected-occurrence-ids="
                                                selectedOccurrenceIds
                                            "
                                            :main-occurrence-id="
                                                main_occurrence_obj.id
                                            "
                                            @update-chosen-section="
                                                updateChosenSection
                                            "
                                        />
                                    </div>
                                </FormSection>
                                <!--Vegetation Structure Form-->
                                <FormSection
                                    :form-collapse="true"
                                    label="Vegetation Structure"
                                    :subtitle="
                                        ' - OCC' +
                                        occ_combine_data.chosen_vegetation_structure_section
                                    "
                                    Index="combine_vegetation_structure"
                                >
                                    <div class="row mb-3">
                                        <OccurrenceCombineSelect
                                            :occ_chosen_section="
                                                occ_combine_data.chosen_vegetation_structure_section
                                            "
                                            :section_type="'vegetation_structure'"
                                            :selected-occurrences="
                                                selectedOccurrences
                                            "
                                            :selected-occurrence-ids="
                                                selectedOccurrenceIds
                                            "
                                            :main-occurrence-id="
                                                main_occurrence_obj.id
                                            "
                                            @update-chosen-section="
                                                updateChosenSection
                                            "
                                        />
                                    </div>
                                </FormSection>
                                <!--Fire History Form-->
                                <FormSection
                                    :form-collapse="true"
                                    label="Fire History"
                                    :subtitle="
                                        ' - OCC' +
                                        occ_combine_data.chosen_fire_history_section
                                    "
                                    Index="combine_fire_history"
                                >
                                    <div class="row mb-3">
                                        <OccurrenceCombineSelect
                                            :occ_chosen_section="
                                                occ_combine_data.chosen_fire_history_section
                                            "
                                            :section_type="'fire_history'"
                                            :selected-occurrences="
                                                selectedOccurrences
                                            "
                                            :selected-occurrence-ids="
                                                selectedOccurrenceIds
                                            "
                                            :main-occurrence-id="
                                                main_occurrence_obj.id
                                            "
                                            @update-chosen-section="
                                                updateChosenSection
                                            "
                                        />
                                    </div>
                                </FormSection>
                                <!--Associated Species Form-->
                                <FormSection
                                    :form-collapse="true"
                                    label="Associated Species"
                                    :subtitle="
                                        ' - OCC' +
                                        occ_combine_data.chosen_associated_species_section
                                    "
                                    Index="combine_associated_species"
                                >
                                    <div class="row mb-3">
                                        <OccurrenceCombineSelect
                                            :occ_chosen_section="
                                                occ_combine_data.chosen_associated_species_section
                                            "
                                            :section_type="'associated_species'"
                                            :selected-occurrences="
                                                selectedOccurrences
                                            "
                                            :selected-occurrence-ids="
                                                selectedOccurrenceIds
                                            "
                                            :main-occurrence-id="
                                                main_occurrence_obj.id
                                            "
                                            @update-chosen-section="
                                                updateChosenSection
                                            "
                                        />
                                    </div>
                                </FormSection>
                            </div>
                            <div
                                :id="observationBody"
                                class="tab-pane fade"
                                role="tabpanel"
                                aria-labelledby="pills-observation-tab"
                            >
                                <!--Observation Details Form-->
                                <FormSection
                                    :form-collapse="true"
                                    label="Observation Details"
                                    :subtitle="
                                        ' - OCC' +
                                        occ_combine_data.chosen_observation_detail_section
                                    "
                                    Index="combine_observation_details"
                                >
                                    <div class="row mb-3">
                                        <OccurrenceCombineSelect
                                            :occ_chosen_section="
                                                occ_combine_data.chosen_observation_detail_section
                                            "
                                            :section_type="'observation_detail'"
                                            :selected-occurrences="
                                                selectedOccurrences
                                            "
                                            :selected-occurrence-ids="
                                                selectedOccurrenceIds
                                            "
                                            :main-occurrence-id="
                                                main_occurrence_obj.id
                                            "
                                            @update-chosen-section="
                                                updateChosenSection
                                            "
                                        />
                                    </div>
                                </FormSection>
                                <!--Animal Observation Form (fauna only)-->
                                <FormSection
                                    v-if="
                                        main_occurrence_obj.group_type ==
                                        'fauna'
                                    "
                                    :form-collapse="true"
                                    label="Animal Observation"
                                    :subtitle="
                                        ' - OCC' +
                                        occ_combine_data.chosen_animal_observation_section
                                    "
                                    Index="combine_animal_observation"
                                >
                                    <div class="row mb-3">
                                        <OccurrenceCombineSelect
                                            :occ_chosen_section="
                                                occ_combine_data.chosen_animal_observation_section
                                            "
                                            :section_type="'animal_observation'"
                                            :selected-occurrences="
                                                selectedOccurrences
                                            "
                                            :selected-occurrence-ids="
                                                selectedOccurrenceIds
                                            "
                                            :main-occurrence-id="
                                                main_occurrence_obj.id
                                            "
                                            @update-chosen-section="
                                                updateChosenSection
                                            "
                                        />
                                    </div>
                                </FormSection>
                                <!--Plant Count Form (flora only)-->
                                <FormSection
                                    v-if="
                                        main_occurrence_obj.group_type ==
                                        'flora'
                                    "
                                    :form-collapse="true"
                                    label="Plant Count"
                                    :subtitle="
                                        ' - OCC' +
                                        occ_combine_data.chosen_plant_count_section
                                    "
                                    Index="combine_plant_count"
                                >
                                    <div class="row mb-3">
                                        <OccurrenceCombineSelect
                                            :occ_chosen_section="
                                                occ_combine_data.chosen_plant_count_section
                                            "
                                            :section_type="'plant_count'"
                                            :selected-occurrences="
                                                selectedOccurrences
                                            "
                                            :selected-occurrence-ids="
                                                selectedOccurrenceIds
                                            "
                                            :main-occurrence-id="
                                                main_occurrence_obj.id
                                            "
                                            @update-chosen-section="
                                                updateChosenSection
                                            "
                                        />
                                    </div>
                                </FormSection>
                                <!--Identification Form-->
                                <FormSection
                                    :form-collapse="true"
                                    label="Identification"
                                    :subtitle="
                                        ' - OCC' +
                                        occ_combine_data.chosen_identification_section
                                    "
                                    Index="combine_identification"
                                >
                                    <div class="row mb-3">
                                        <OccurrenceCombineSelect
                                            :occ_chosen_section="
                                                occ_combine_data.chosen_identification_section
                                            "
                                            :section_type="'identification'"
                                            :selected-occurrences="
                                                selectedOccurrences
                                            "
                                            :selected-occurrence-ids="
                                                selectedOccurrenceIds
                                            "
                                            :main-occurrence-id="
                                                main_occurrence_obj.id
                                            "
                                            @update-chosen-section="
                                                updateChosenSection
                                            "
                                        />
                                    </div>
                                </FormSection>
                            </div>
                            <div
                                :id="documentBody"
                                class="tab-pane fade"
                                role="tabpanel"
                                aria-labelledby="pills-documents-tab"
                            >
                                <!--Documents Table-->
                                <FormSection
                                    :form-collapse="false"
                                    label="Documents"
                                    Index="combine_documents"
                                    @toggle-collapse="toggleDocuments"
                                >
                                    <div class="row mb-3">
                                        <OccurrenceCombineDocuments
                                            :key="document_table_key"
                                            ref="documents_section"
                                            :selected-documents="documents"
                                            :combine-document-ids="
                                                occ_combine_data.combine_document_ids
                                            "
                                            :main-occurrence-id="
                                                main_occurrence_obj.id
                                            "
                                        />
                                    </div>
                                </FormSection>
                            </div>
                            <div
                                :id="threatBody"
                                class="tab-pane fade"
                                role="tabpanel"
                                aria-labelledby="pills-threats-tab"
                            >
                                <!--Threats Table-->
                                <FormSection
                                    :form-collapse="false"
                                    label="Threats"
                                    Index="combine_threats"
                                >
                                    <div class="row mb-3">
                                        <OccurrenceCombineThreats
                                            :key="threat_table_key"
                                            ref="threats_section"
                                            :selected-threats="threats"
                                            :combine-threat-ids="
                                                occ_combine_data.combine_threat_ids
                                            "
                                            :main-occurrence-id="
                                                main_occurrence_obj.id
                                            "
                                        />
                                    </div>
                                </FormSection>
                            </div>
                            <div
                                id="finalise-occurrence-combine"
                                class="tab-pane fade"
                                role="tabpanel"
                                aria-labelledby="pills-occurrence-combine-tab"
                            >
                                <FormSection
                                    :form-collapse="false"
                                    label="Finalise Combine"
                                    Index="finalise_combine"
                                >
                                    <div class="row mb-3">
                                        <div class="col-sm-12">
                                            <button
                                                type="button"
                                                class="btn btn-primary"
                                                @click="ok()"
                                            >
                                                <i
                                                    class="bi bi-check2-circle"
                                                ></i>
                                                Finalise Combine
                                            </button>
                                        </div>
                                    </div>
                                </FormSection>
                            </div>
                        </div>
                    </div>
                </div>
            </template>
        </modal>
    </div>
</template>

<script>
import { v4 as uuid } from 'uuid';
import modal from '@vue-utils/bootstrap-modal.vue';
import alert from '@vue-utils/alert.vue';
import FormSection from '@/components/forms/section_toggle.vue';
import { helpers, api_endpoints } from '@/utils/hooks.js';
import OccurrenceCombineSelect from './occurrence_combine_selection.vue';
import OccurrenceCombineContacts from './occurrence_combine_contacts.vue';
import OccurrenceCombineSites from './occurrence_combine_sites.vue';
import OccurrenceCombineTenures from './occurrence_combine_tenures.vue';
import OccurrenceCombineDocuments from './occurrence_combine_documents.vue';
import OccurrenceCombineThreats from './occurrence_combine_threats.vue';
export default {
    name: 'OccurrenceCombine',
    components: {
        alert,
        modal,
        OccurrenceCombineSelect,
        FormSection,
        OccurrenceCombineContacts,
        OccurrenceCombineDocuments,
        OccurrenceCombineThreats,
        OccurrenceCombineSites,
        OccurrenceCombineTenures,
    },
    props: {
        main_occurrence_obj: {
            type: Object,
            required: true,
        },
    },
    data: function () {
        return {
            occ_form_key: 0,
            contact_table_key: 0,
            document_table_key: 0,
            threat_table_key: 0,
            site_table_key: 0,
            tenure_table_key: 0,
            locationBody: 'locationBody' + uuid(),
            habitatBody: 'habitatBody' + uuid(),
            observationBody: 'observationBody' + uuid(),
            threatBody: 'threatBody' + uuid(),
            documentBody: 'documentBody' + uuid(),
            occurrenceBody: 'occurrenceBody' + uuid(),
            isModalOpen: false,
            selectedOccurrences: [this.main_occurrence_obj],
            selectedOccurrenceIds: [this.main_occurrence_obj.id],
            selectedAddOccurrence: null,
            addOccurrenceLoading: false,
            occ_profile_dict: {},
            wild_status_list: [],
            occurrence_source_list: [],
            key_contact_ids: [],
            document_ids: [],
            threat_ids: [],
            site_ids: [],
            tenure_ids: [],
            key_contacts: [],
            documents: [],
            threats: [],
            sites: [],
            tenures: [],
            occ_combine_data: {
                combine_ids: [this.main_occurrence_obj.id],
                combine_key_contact_ids: [],
                combine_document_ids: [],
                combine_threat_ids: [],
                combine_site_ids: [],
                combine_tenure_ids: [],
                occurrence_source: this.main_occurrence_obj.id,
                wild_status: this.main_occurrence_obj.id,
                review_due_date: this.main_occurrence_obj.id,
                comment: this.main_occurrence_obj.id,
                chosen_location_section: this.main_occurrence_obj.id,
                chosen_habitat_composition_section: this.main_occurrence_obj.id,
                chosen_habitat_condition_section: this.main_occurrence_obj.id,
                chosen_vegetation_structure_section:
                    this.main_occurrence_obj.id,
                chosen_fire_history_section: this.main_occurrence_obj.id,
                chosen_associated_species_section: this.main_occurrence_obj.id,
                chosen_observation_detail_section: this.main_occurrence_obj.id,
                chosen_animal_observation_section: this.main_occurrence_obj.id,
                chosen_plant_count_section: this.main_occurrence_obj.id,
                chosen_identification_section: this.main_occurrence_obj.id,
            },
            originalOccCombineData: null,
        };
    },
    watch: {
        isModalOpen(newVal) {
            if (newVal) {
                let vm = this;
                vm.selectedOccurrences = [vm.main_occurrence_obj];
                vm.selectedOccurrenceIds = [vm.main_occurrence_obj.id];
                vm.getKeyContactIds();
                vm.getDocumentIds();
                vm.getThreatIds();
                vm.getSiteIds();
                vm.getTenureIds();

                let dict_url =
                    api_endpoints.occ_profile_dict +
                    '?group_type=' +
                    vm.main_occurrence_obj.group_type;
                fetch(dict_url).then(
                    async (response) => {
                        vm.occ_profile_dict = await response.json();
                        vm.wild_status_list =
                            vm.occ_profile_dict.wild_status_list;
                        vm.occurrence_source_list =
                            vm.occ_profile_dict.occurrence_source_list;
                    },
                    (error) => {
                        console.log(error);
                    }
                );
                vm.$nextTick(() => {
                    $(vm.$refs.occurrence_name_lookup).select2('open');
                    vm.originalOccCombineData = JSON.parse(
                        JSON.stringify(vm.occ_combine_data)
                    );
                });
            }
        },
        selectedOccurrenceIds: {
            handler(newVal) {
                let vm = this;
                vm.occ_combine_data.combine_ids = newVal;
                vm.getKeyContactIds();
                vm.getDocumentIds();
                vm.getThreatIds();
                vm.getSiteIds();
                vm.getTenureIds();
                vm.checkFormValues();
            },
            deep: true,
        },
    },
    mounted: function () {
        this.initialiseOccurrenceNameLookup();
    },
    methods: {
        hasUnsavedChanges: function () {
            return (
                JSON.stringify(this.occ_combine_data) !==
                JSON.stringify(this.originalOccCombineData)
            );
        },
        sendData: function () {
            let vm = this;
            let formData = new FormData();
            formData.append('data', JSON.stringify(vm.occ_combine_data));
            fetch(
                helpers.add_endpoint_json(
                    api_endpoints.occurrence,
                    vm.main_occurrence_obj.id + '/combine'
                ),
                {
                    method: 'POST',
                    body: formData,
                    headers: {
                        'Content-Type': 'application/json',
                    },
                }
            ).then(
                async (response) => {
                    if (!response.ok) {
                        const data = await response.json();
                        swal.fire({
                            title: 'Error',
                            text: JSON.stringify(data),
                            icon: 'error',
                            customClass: {
                                confirmButton: 'btn btn-primary',
                            },
                        });
                        return;
                    }
                    swal.fire({
                        title: 'Combined',
                        text: 'Selected Occurrences have been combined.',
                        icon: 'success',
                        customClass: {
                            confirmButton: 'btn btn-primary',
                        },
                    }).then(() => {
                        vm.$router.go();
                    });
                },
                (err) => {
                    var errorText = helpers.apiVueResourceError(err);
                    swal.fire({
                        title: 'Submit Error',
                        text: errorText,
                        icon: 'error',
                        customClass: {
                            confirmButton: 'btn btn-primary',
                        },
                    });
                    vm.paySubmitting = false;
                    vm.saveError = true;
                }
            );
        },
        ok: function () {
            let vm = this;

            //make sure there is at least two selected occurrences
            if (this.occ_combine_data.combine_ids.length < 2) {
                swal.fire({
                    title: 'Submit Error',
                    text: 'No additional Occurrences have been selected to combine.',
                    icon: 'error',
                    customClass: {
                        confirmButton: 'btn btn-primary',
                    },
                });
            } else {
                //confirm before sending
                swal.fire({
                    title: 'Combine Occurrences',
                    text: 'Are you sure you want to combine the selected Occurrences, with the selected sections, fields, and table rows?',
                    icon: 'question',
                    showCancelButton: true,
                    confirmButtonText: 'Combine Occurrences',
                    customClass: {
                        confirmButton: 'btn btn-primary',
                        cancelButton: 'btn btn-secondary',
                    },
                    reverseButtons: true,
                }).then((swalresult) => {
                    if (swalresult.isConfirmed) {
                        vm.sendData();
                    }
                });
            }
        },
        toggleKeyContacts: function () {
            let vm = this;
            vm.$refs.key_contacts_section.adjust_table_width();
        },
        toggleDocuments: function () {
            let vm = this;
            vm.$refs.documents_section.adjust_table_width();
        },
        toggleThreats: function () {
            let vm = this;
            vm.$refs.threats_section.adjust_table_width();
        },
        toggleSites: function () {
            let vm = this;
            vm.$refs.sites_section.adjust_table_width();
        },
        toggleTenures: function () {
            let vm = this;
            vm.$refs.tenures_section.adjust_table_width();
        },
        updateChosenSection: function (id, sectionType) {
            let vm = this;
            vm.occ_combine_data['chosen_' + sectionType + '_section'] =
                parseInt(id);
            console.log(
                vm.occ_combine_data['chosen_' + sectionType + '_section']
            );
        },
        close: function () {
            this.errorString = '';
            this.isModalOpen = false;
            $('.has-error').removeClass('has-error');
        },
        addOccurrence: function () {
            let vm = this;
            vm.addOccurrenceLoading = true;
            if (
                vm.selectedAddOccurrence != null &&
                !vm.selectedOccurrenceIds.includes(vm.selectedAddOccurrence.id)
            ) {
                vm.selectedOccurrenceIds.push(vm.selectedAddOccurrence.id);
                vm.selectedOccurrences.push(vm.selectedAddOccurrence);
            }
            vm.selectedAddOccurrence = null;
            $(vm.$refs.occurrence_name_lookup).val(null).trigger('change');
            vm.occ_form_key++;
            vm.$nextTick(() => {
                vm.addTabEventListeners();
            });
        },
        checkFormValues: function () {
            let vm = this;

            if (
                !vm.selectedOccurrenceIds.includes(
                    vm.occ_combine_data.chosen_location_section
                )
            ) {
                vm.occ_combine_data.chosen_location_section =
                    vm.main_occurrence_obj.id;
            }
            if (
                !vm.selectedOccurrenceIds.includes(
                    vm.occ_combine_data.chosen_habitat_composition_section
                )
            ) {
                vm.occ_combine_data.chosen_habitat_composition_section =
                    vm.main_occurrence_obj.id;
            }
            if (
                !vm.selectedOccurrenceIds.includes(
                    vm.occ_combine_data.chosen_habitat_condition_section
                )
            ) {
                vm.occ_combine_data.chosen_habitat_condition_section =
                    vm.main_occurrence_obj.id;
            }
            if (
                !vm.selectedOccurrenceIds.includes(
                    vm.occ_combine_data.chosen_vegetation_structure_section
                )
            ) {
                vm.occ_combine_data.chosen_vegetation_structure_section =
                    vm.main_occurrence_obj.id;
            }
            if (
                !vm.selectedOccurrenceIds.includes(
                    vm.occ_combine_data.chosen_fire_history_section
                )
            ) {
                vm.occ_combine_data.chosen_fire_history_section =
                    vm.main_occurrence_obj.id;
            }
            if (
                !vm.selectedOccurrenceIds.includes(
                    vm.occ_combine_data.chosen_associated_species_section
                )
            ) {
                vm.occ_combine_data.chosen_associated_species_section =
                    vm.main_occurrence_obj.id;
            }
            if (
                !vm.selectedOccurrenceIds.includes(
                    vm.occ_combine_data.chosen_observation_detail_section
                )
            ) {
                vm.occ_combine_data.chosen_observation_detail_section =
                    vm.main_occurrence_obj.id;
            }
            if (
                !vm.selectedOccurrenceIds.includes(
                    vm.occ_combine_data.chosen_animal_observation_section
                )
            ) {
                vm.occ_combine_data.chosen_animal_observation_section =
                    vm.main_occurrence_obj.id;
            }
            if (
                !vm.selectedOccurrenceIds.includes(
                    vm.occ_combine_data.chosen_plant_count_section
                )
            ) {
                vm.occ_combine_data.chosen_plant_count_section =
                    vm.main_occurrence_obj.id;
            }
            if (
                !vm.selectedOccurrenceIds.includes(
                    vm.occ_combine_data.chosen_identification_section
                )
            ) {
                vm.occ_combine_data.chosen_identification_section =
                    vm.main_occurrence_obj.id;
            }
            if (
                !vm.selectedOccurrenceIds.includes(
                    vm.occ_combine_data.occurrence_source
                )
            ) {
                vm.occ_combine_data.occurrence_source =
                    vm.main_occurrence_obj.id;
            }
            if (
                !vm.selectedOccurrenceIds.includes(
                    vm.occ_combine_data.wild_status
                )
            ) {
                vm.occ_combine_data.wild_status = vm.main_occurrence_obj.id;
            }
            if (
                !vm.selectedOccurrenceIds.includes(
                    vm.occ_combine_data.review_due_date
                )
            ) {
                vm.occ_combine_data.review_due_date = vm.main_occurrence_obj.id;
            }
            if (
                !vm.selectedOccurrenceIds.includes(vm.occ_combine_data.comment)
            ) {
                vm.occ_combine_data.comment = vm.main_occurrence_obj.id;
            }
        },
        getKeyContactIds: function () {
            let vm = this;
            let formData = new FormData();
            formData.append(
                'occurrence_ids',
                JSON.stringify(vm.selectedOccurrenceIds)
            );
            //get all key contact ids for all OCCs
            fetch(api_endpoints.combine_key_contacts_lookup, {
                method: 'POST',
                body: formData,
                headers: {
                    'Content-Type': 'application/json',
                },
            }).then(
                async (response) => {
                    const data = await response.json();
                    //copy old list
                    let old_list = vm.key_contact_ids;
                    //add to main list
                    vm.key_contact_ids = data.id_list;
                    vm.key_contacts = data.values_list;

                    let contact_names = {};
                    let taken_names = [];
                    vm.key_contacts.forEach((contact) => {
                        if (
                            vm.occ_combine_data.combine_key_contact_ids.includes(
                                contact.id
                            )
                        ) {
                            taken_names.push(contact.contact_name);
                        }
                        contact_names[contact.id] = contact.contact_name;
                    });

                    //remove ids from combine list if not in new list
                    vm.occ_combine_data.combine_key_contact_ids.forEach(
                        (id) => {
                            if (!vm.key_contact_ids.includes(id)) {
                                vm.occ_combine_data.combine_key_contact_ids.splice(
                                    vm.occ_combine_data.combine_key_contact_ids.indexOf(
                                        id
                                    ),
                                    1
                                );
                            }
                        }
                    );

                    //add new ids to combine list if not in old list - unless they share a name
                    data.id_list.forEach((id) => {
                        if (
                            !old_list.includes(id) &&
                            !taken_names.includes(contact_names[id])
                        ) {
                            vm.occ_combine_data.combine_key_contact_ids.push(
                                id
                            );
                            taken_names.push(contact_names[id]);
                        }
                    });

                    vm.contact_table_key++;
                },
                (error) => {
                    console.error(error);
                }
            );
        },
        getDocumentIds: function () {
            let vm = this;
            let formData = new FormData();
            formData.append(
                'occurrence_ids',
                JSON.stringify(vm.selectedOccurrenceIds)
            );
            //get all document ids for all OCCs
            fetch(api_endpoints.combine_documents_lookup, {
                method: 'POST',
                body: formData,
                headers: {
                    'Content-Type': 'application/json',
                },
            }).then(
                async (response) => {
                    const data = await response.json();
                    //copy old list
                    let old_list = vm.document_ids;
                    //add to main list
                    vm.document_ids = data.id_list;
                    vm.documents = data.values_list;
                    //remove ids from combine list if not in new list
                    vm.occ_combine_data.combine_document_ids.forEach((id) => {
                        if (!data.id_list.includes(id)) {
                            vm.occ_combine_data.combine_document_ids.splice(
                                vm.occ_combine_data.combine_document_ids.indexOf(
                                    id
                                ),
                                1
                            );
                        }
                    });
                    //add new ids to combine list if not in old list
                    data.id_list.forEach((id) => {
                        if (!old_list.includes(id)) {
                            vm.occ_combine_data.combine_document_ids.push(id);
                        }
                    });

                    vm.document_table_key++;
                },
                (error) => {
                    console.error(error);
                }
            );
        },
        getThreatIds: function () {
            let vm = this;
            let formData = new FormData();
            formData.append(
                'occurrence_ids',
                JSON.stringify(vm.selectedOccurrenceIds)
            );
            //get all threat ids for all OCCs
            fetch(api_endpoints.combine_threats_lookup, {
                method: 'POST',
                body: formData,
                headers: {
                    'Content-Type': 'application/json',
                },
            }).then(
                async (response) => {
                    const data = await response.json();
                    //copy old list
                    let old_list = vm.threat_ids;
                    //add to main list
                    vm.threat_ids = data.id_list;
                    vm.threats = data.values_list;

                    let threat_original_reports = {};
                    let taken_reports = [];
                    vm.threats.forEach((threat) => {
                        if (
                            vm.occ_combine_data.combine_threat_ids.includes(
                                threat.id
                            ) &&
                            threat.occurrence_report_threat__threat_number !=
                                null
                        ) {
                            taken_reports.push(
                                threat.occurrence_report_threat__threat_number
                            );
                        }
                        threat_original_reports[threat.id] =
                            threat.occurrence_report_threat__threat_number;
                    });

                    //remove ids from combine list if not in new list
                    vm.occ_combine_data.combine_threat_ids.forEach((id) => {
                        if (!data.id_list.includes(id)) {
                            vm.occ_combine_data.combine_threat_ids.splice(
                                vm.occ_combine_data.combine_threat_ids.indexOf(
                                    id
                                ),
                                1
                            );
                        }
                    });
                    //add new ids to combine list if not in old list - unless they share an original report
                    data.id_list.forEach((id) => {
                        if (
                            !old_list.includes(id) &&
                            !taken_reports.includes(threat_original_reports[id])
                        ) {
                            vm.occ_combine_data.combine_threat_ids.push(id);
                            taken_reports.push(threat_original_reports[id]);
                        }
                    });
                    vm.threat_table_key++;
                },
                (error) => {
                    console.error(error);
                }
            );
        },
        getSiteIds: function () {
            let vm = this;
            let formData = new FormData();
            formData.append(
                'occurrence_ids',
                JSON.stringify(vm.selectedOccurrenceIds)
            );
            //get all site ids for all OCCs
            fetch(api_endpoints.combine_sites_lookup, {
                method: 'POST',
                body: formData,
                headers: {
                    'Content-Type': 'application/json',
                },
            }).then(
                async (response) => {
                    const data = await response.json();
                    //copy old list
                    let old_list = vm.site_ids;
                    //add to main list
                    vm.site_ids = data.id_list;
                    vm.sites = data.values_list;

                    let site_names = {};
                    let taken_names = [];
                    vm.sites.forEach((site) => {
                        if (
                            vm.occ_combine_data.combine_site_ids.includes(
                                site.id
                            )
                        ) {
                            taken_names.push(site.site_name);
                        }
                        site_names[site.id] = site.site_name;
                    });

                    //remove ids from combine list if not in new list
                    vm.occ_combine_data.combine_site_ids.forEach((id) => {
                        if (!vm.site_ids.includes(id)) {
                            vm.occ_combine_data.combine_site_ids.splice(
                                vm.occ_combine_data.combine_site_ids.indexOf(
                                    id
                                ),
                                1
                            );
                        }
                    });

                    //add new ids to combine list if not in old list - unless they share a name
                    data.id_list.forEach((id) => {
                        if (
                            !old_list.includes(id) &&
                            !taken_names.includes(site_names[id])
                        ) {
                            vm.occ_combine_data.combine_site_ids.push(id);
                            taken_names.push(site_names[id]);
                        }
                    });

                    vm.site_table_key++;
                },
                (error) => {
                    console.error(error);
                }
            );
        },
        getTenureIds: function () {
            let vm = this;
            let formData = new FormData();
            formData.append(
                'occurrence_ids',
                JSON.stringify(vm.selectedOccurrenceIds)
            );
            //get all site ids for all OCCs
            fetch(api_endpoints.combine_tenures_lookup, {
                method: 'POST',
                body: formData,
                headers: {
                    'Content-Type': 'application/json',
                },
            }).then(
                async (response) => {
                    const data = await response.json();
                    //copy old list
                    let old_list = vm.tenure_ids;
                    //add to main list
                    vm.tenure_ids = data.id_list;
                    vm.tenures = data.values_list;

                    let tenure_feature_ids = {};
                    let taken_feature_ids = [];
                    vm.tenures.forEach((tenure) => {
                        if (
                            vm.occ_combine_data.combine_tenure_ids.includes(
                                tenure.id
                            ) &&
                            tenure.status_display == 'Current'
                        ) {
                            taken_feature_ids.push(tenure.featureid);
                        }
                        if (tenure.status_display == 'Current') {
                            tenure_feature_ids[tenure.id] = tenure.featureid;
                        }
                    });

                    //remove ids from combine list if not in new list
                    vm.occ_combine_data.combine_tenure_ids.forEach((id) => {
                        if (!vm.tenure_ids.includes(id)) {
                            vm.occ_combine_data.combine_tenure_ids.splice(
                                vm.occ_combine_data.combine_tenure_ids.indexOf(
                                    id
                                ),
                                1
                            );
                        }
                    });

                    //add new ids to combine list if not in old list - unless they share a feature id while current
                    data.id_list.forEach((id) => {
                        if (
                            !old_list.includes(id) &&
                            !(
                                tenure_feature_ids[id] &&
                                taken_feature_ids.includes(
                                    tenure_feature_ids[id]
                                )
                            )
                        ) {
                            vm.occ_combine_data.combine_tenure_ids.push(id);
                            if (tenure_feature_ids[id]) {
                                taken_feature_ids.push(tenure_feature_ids[id]);
                            }
                        }
                    });

                    vm.tenure_table_key++;
                },
                (error) => {
                    console.error(error);
                }
            );
        },
        initialiseOccurrenceNameLookup: function () {
            let vm = this;
            $(vm.$refs.occurrence_name_lookup)
                .select2({
                    width: '100%',
                    minimumInputLength: 2,
                    theme: 'bootstrap-5',
                    allowClear: true,
                    dropdownParent: $('#occurrence_name_lookup_form_group_id'),
                    placeholder: `Search Name of Occurrence to combine with ${vm.main_occurrence_obj.occurrence_number}`,
                    ajax: {
                        url: api_endpoints.combine_occurrence_name_lookup,
                        dataType: 'json',
                        data: function (params) {
                            var query = {
                                term: params.term,
                                occurrence_id: vm.main_occurrence_obj.id,
                            };
                            return query;
                        },
                    },
                })
                .on('select2:select', function (e) {
                    vm.selectedAddOccurrence = e.params.data;
                    vm.addOccurrence();
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-occurrence_name_lookup-results"]'
                    );
                    searchField[0].focus();
                })
                .on('select2:unselect', function () {
                    vm.selectedAddOccurrence = null;
                });
        },
        addTabEventListeners: function () {
            let vm = this;
            document
                .querySelectorAll('a[data-bs-toggle="pill"]')
                .forEach((el) => {
                    el.addEventListener('shown.bs.tab', () => {
                        if (el.id == 'pills-occurrence-tab') {
                            vm.$refs.key_contacts_section.adjust_table_width();
                        } else if (el.id == 'pills-location-tab') {
                            vm.$refs.sites_section.adjust_table_width();
                            vm.$refs.tenures_section.adjust_table_width();
                        } else if (el.id == 'pills-documents-tab') {
                            vm.$refs.documents_section.adjust_table_width();
                        } else if (el.id == 'pills-threats-tab') {
                            vm.$refs.threats_section.adjust_table_width();
                        }
                    });
                });
        },
    },
};
</script>
