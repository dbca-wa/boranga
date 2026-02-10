<template lang="html">
    <div id="communityStatus">
        <FormSection
            :form-collapse="false"
            label="Conservation Status"
            Index="conservation_status"
            class="pb-0"
        >
            <form class="pb-0" @change="saveConservationStatus($event)">
                <div class="row mb-3">
                    <label for="" class="col-sm-4 control-label fw-bold"
                        >Community Name:
                        <span class="text-danger">*</span></label
                    >
                    <div :id="select_community_name" class="col-sm-8">
                        <select
                            :id="community_name_lookup"
                            :ref="community_name_lookup"
                            :disabled="isCommunityNameReadOnly"
                            :name="community_name_lookup"
                            class="form-control"
                        />
                    </div>
                </div>
                <div class="row mb-3">
                    <label for="" class="col-sm-4 control-label"></label>
                    <div class="col-sm-8">
                        <textarea
                            id="community_display"
                            v-model="community_display"
                            disabled
                            class="form-control"
                            rows="3"
                        />
                    </div>
                </div>
                <div class="row mb-3">
                    <label for="" class="col-sm-4 control-label fw-bold"
                        >Community ID: <span class="text-danger">*</span></label
                    >
                    <div :id="select_community_id" class="col-sm-8">
                        <select
                            :id="community_id_lookup"
                            :ref="community_id_lookup"
                            :disabled="isCommunityNameReadOnly"
                            :name="community_id_lookup"
                            class="form-control"
                        />
                    </div>
                </div>
                <template v-if="show_administrative_information">
                    <div class="row mb-3 border-top pt-3">
                        <h5 class="text-muted mb-4">
                            Administrative Information
                        </h5>
                        <label
                            for="change_code"
                            class="col-sm-4 col-form-label fw-bold"
                            >Change Type:
                            <span class="text-danger">*</span></label
                        >
                        <div class="col-sm-8">
                            <template v-if="!isReadOnly">
                                <template
                                    v-if="
                                        change_codes &&
                                        change_codes.length > 0 &&
                                        conservation_status_obj.change_code_id &&
                                        !change_codes
                                            .map((d) => d.id)
                                            .includes(
                                                conservation_status_obj.change_code_id
                                            )
                                    "
                                >
                                    <input
                                        v-if="
                                            conservation_status_obj.change_code
                                        "
                                        type="text"
                                        class="form-control mb-3"
                                        :value="
                                            conservation_status_obj.change_code +
                                            ' (Now Archived)'
                                        "
                                        disabled
                                    />
                                    <div class="mb-3 text-muted">
                                        Change change type to:
                                    </div>
                                </template>
                                <select
                                    v-model="
                                        conservation_status_obj.change_code_id
                                    "
                                    class="form-select"
                                >
                                    <option :value="null">
                                        Select Appropriate Change Type
                                    </option>
                                    <option
                                        v-for="change_code in change_codes"
                                        :key="change_code.id"
                                        :value="change_code.id"
                                        :disabled="
                                            isOptionDisabled(change_code)
                                        "
                                    >
                                        {{ getOptionLabel(change_code) }}
                                    </option>
                                </select>
                            </template>
                            <template v-else>
                                <input
                                    class="form-control"
                                    type="text"
                                    disabled
                                    :value="
                                        conservation_status_obj.change_code
                                            ? conservation_status_obj.change_code
                                            : 'Select Appropriate Change Type'
                                    "
                                />
                            </template>
                        </div>
                    </div>
                    <div class="row mb-3">
                        <label
                            for="approval_level"
                            class="col-sm-4 col-form-label fw-bold"
                            >Applicable Workflow:
                            <span class="text-danger">*</span></label
                        >
                        <div class="col-sm-8">
                            <select
                                id="approval_level"
                                v-model="conservation_status_obj.approval_level"
                                class="form-select"
                                :disabled="approval_level_disabled"
                                @change="approvalLevelChanged"
                            >
                                <option :value="null">
                                    Select Appropriate Workflow
                                </option>
                                <option :value="'immediate'">Immediate</option>
                                <option :value="'minister'">Ministerial</option>
                            </select>
                        </div>
                    </div>
                    <template
                        v-if="
                            conservation_status_obj.approval_level == 'minister'
                        "
                    >
                        <div class="row mb-3 border-top pt-3">
                            <label
                                for="listing_date"
                                class="col-sm-3 col-form-label"
                                >Public Consultation:</label
                            >
                            <div class="col-sm-9 d-flex align-items-center">
                                <div class="form-check form-check-inline">
                                    Yes
                                    <input
                                        id="public_consultation_yes"
                                        v-model="
                                            conservation_status_obj.public_consultation
                                        "
                                        :disabled="
                                            listing_and_review_due_date_disabled
                                        "
                                        type="radio"
                                        class="form-check-input"
                                        name="public_consultation"
                                        :value="true"
                                    />
                                </div>
                                <div class="form-check form-check-inline">
                                    No
                                    <input
                                        id="public_consultation_no"
                                        v-model="
                                            conservation_status_obj.public_consultation
                                        "
                                        :disabled="
                                            listing_and_review_due_date_disabled
                                        "
                                        type="radio"
                                        class="form-check-input"
                                        name="public_consultation"
                                        :value="false"
                                    />
                                </div>
                            </div>
                        </div>
                        <div
                            v-if="conservation_status_obj.public_consultation"
                            class="row mb-3 pt-1"
                        >
                            <label
                                for="review_due_date"
                                class="col-sm-3 col-form-label"
                                >Start Date:</label
                            >
                            <div class="col-sm-3">
                                <input
                                    id="public_consultation_start_date"
                                    v-model="
                                        conservation_status_obj.public_consultation_start_date
                                    "
                                    type="date"
                                    placeholder="DD/MM/YYYY"
                                    class="form-control"
                                    :disabled="
                                        listing_and_review_due_date_disabled
                                    "
                                />
                            </div>
                            <template
                                v-if="
                                    conservation_status_obj.public_consultation_start_date
                                "
                            >
                                <label
                                    for="review_due_date"
                                    class="col-sm-3 col-form-label"
                                    >End Date:</label
                                >
                                <div class="col-sm-3">
                                    <input
                                        id="public_consultation_end_date"
                                        v-model="
                                            conservation_status_obj.public_consultation_end_date
                                        "
                                        type="date"
                                        placeholder="DD/MM/YYYY"
                                        class="form-control"
                                        :disabled="
                                            listing_and_review_due_date_disabled
                                        "
                                        :min="
                                            conservation_status_obj.public_consultation_start_date
                                                ? new Date(
                                                      conservation_status_obj.public_consultation_start_date
                                                  )
                                                      .toISOString()
                                                      .split('T')[0]
                                                : null
                                        "
                                    />
                                </div>
                            </template>
                        </div>
                    </template>
                    <div
                        v-if="show_effective_date_fields"
                        class="row mb-3 border-top pt-3"
                    >
                        <template v-if="show_effective_from">
                            <label
                                for="effective_from"
                                class="col-sm-3 col-form-label fw-bold"
                            >
                                Effective From:
                                <span class="text-danger">*</span></label
                            >
                            <div class="col-sm-3">
                                <input
                                    id="effective_from"
                                    v-model="
                                        conservation_status_obj.effective_from
                                    "
                                    type="date"
                                    placeholder="DD/MM/YYYY"
                                    :max="
                                        new Date().toISOString().split('T')[0]
                                    "
                                    class="form-control"
                                    :disabled="isReadOnly"
                                />
                            </div>
                        </template>
                        <!-- Empty column in case effective-from is not rendered to force effective-to into the right column -->
                        <template v-else><div class="col-sm-6"></div></template>
                        <template v-if="show_effective_to">
                            <label
                                for="effective_to"
                                class="col-sm-3 col-form-label"
                                :class="effective_to_mandatory ? 'fw-bold' : ''"
                                >Effective To:
                                <span
                                    v-if="effective_to_mandatory"
                                    class="text-danger"
                                    >*</span
                                ></label
                            >
                            <div class="col-sm-3">
                                <input
                                    id="effective_to"
                                    v-model="
                                        conservation_status_obj.effective_to
                                    "
                                    type="date"
                                    placeholder="DD/MM/YYYY"
                                    class="form-control"
                                    :disabled="isReadOnly"
                                />
                            </div>
                        </template>
                    </div>
                    <div
                        v-if="show_listing_and_review_due_date"
                        class="row mb-3"
                        :class="
                            conservation_status_obj.effective_from ||
                            conservation_status_obj.effective_to
                                ? ''
                                : 'border-top pt-3'
                        "
                    >
                        <label
                            for="listing_date"
                            class="col-sm-3 col-form-label"
                            >Date First Listed:</label
                        >
                        <div class="col-sm-3">
                            <input
                                id="listing_date"
                                v-model="conservation_status_obj.listing_date"
                                :type="
                                    listing_date_display_as_na ? 'text' : 'date'
                                "
                                :placeholder="
                                    listing_date_display_as_na
                                        ? 'N/A'
                                        : 'DD/MM/YYYY'
                                "
                                class="form-control"
                                :disabled="listing_and_review_due_date_disabled"
                            />
                        </div>
                        <label
                            for="review_due_date"
                            class="col-sm-3 col-form-label"
                            >Review Due Date:</label
                        >
                        <div class="col-sm-3">
                            <input
                                id="review_due_date"
                                v-model="
                                    conservation_status_obj.review_due_date
                                "
                                :type="
                                    review_due_date_display_as_na
                                        ? 'text'
                                        : 'date'
                                "
                                :placeholder="
                                    review_due_date_display_as_na
                                        ? 'N/A'
                                        : 'DD/MM/YYYY'
                                "
                                class="form-control"
                                :disabled="listing_and_review_due_date_disabled"
                            />
                        </div>
                    </div>
                    <div
                        v-if="
                            conservation_status_obj.approval_level == 'minister'
                        "
                        class="row mb-3 border-top pt-3"
                    >
                        <label
                            for="listing_date"
                            class="col-sm-3 col-form-label"
                            >CAM MOU:</label
                        >
                        <div class="col-sm-3 d-flex align-items-center">
                            <div class="form-check form-check-inline">
                                Yes
                                <input
                                    id="cam_mou_yes"
                                    v-model="conservation_status_obj.cam_mou"
                                    :disabled="
                                        listing_and_review_due_date_disabled
                                    "
                                    type="radio"
                                    class="form-check-input"
                                    name="cam_mou"
                                    :value="true"
                                />
                            </div>
                            <div class="form-check form-check-inline">
                                No
                                <input
                                    id="conservation_status_under_review_no"
                                    v-model="conservation_status_obj.cam_mou"
                                    :disabled="
                                        listing_and_review_due_date_disabled
                                    "
                                    type="radio"
                                    class="form-check-input"
                                    name="cam_mou"
                                    :value="false"
                                />
                            </div>
                            <div class="form-check form-check-inline">
                                N/A
                                <input
                                    id="conservation_status_under_review_no"
                                    v-model="conservation_status_obj.cam_mou"
                                    :disabled="
                                        listing_and_review_due_date_disabled
                                    "
                                    type="radio"
                                    class="form-check-input"
                                    name="cam_mou"
                                    :value="null"
                                />
                            </div>
                        </div>
                        <template v-if="conservation_status_obj.cam_mou">
                            <label
                                for="review_due_date"
                                class="col-sm-3 col-form-label"
                                >Date Sent:</label
                            >
                            <div class="col-sm-3">
                                <input
                                    id="cam_mou_date_sent"
                                    v-model="
                                        conservation_status_obj.cam_mou_date_sent
                                    "
                                    type="date"
                                    placeholder="DD/MM/YYYY"
                                    class="form-control"
                                    :disabled="
                                        listing_and_review_due_date_disabled
                                    "
                                    :max="
                                        new Date().toISOString().split('T')[0]
                                    "
                                />
                            </div>
                        </template>
                    </div>
                </template>
                <template v-if="show_proposed_conservation_status">
                    <div class="row mb-3 border-top pt-3">
                        <h5 class="text-muted mb-4">
                            <template v-if="conservation_list_proposed"
                                >Proposed </template
                            >Conservation Status
                        </h5>
                        <label
                            for="proposed_wa_legislative_list"
                            class="col-sm-5 col-form-label fw-bold"
                            >WA Legislative List:
                            <span class="text-warning">*</span></label
                        >
                        <div class="col-sm-7">
                            <template v-if="!isReadOnly">
                                <template
                                    v-if="
                                        wa_legislative_lists &&
                                        wa_legislative_lists.length > 0 &&
                                        conservation_status_obj.wa_legislative_list_id &&
                                        !wa_legislative_lists
                                            .map((d) => d.id)
                                            .includes(
                                                conservation_status_obj.wa_legislative_list_id
                                            )
                                    "
                                >
                                    <input
                                        v-if="
                                            conservation_status_obj.wa_legislative_list
                                        "
                                        type="text"
                                        class="form-control mb-3"
                                        :value="
                                            conservation_status_obj.wa_legislative_list +
                                            ' (Now Archived)'
                                        "
                                        disabled
                                    />
                                    <div class="mb-3 text-muted">
                                        Change wa legislative list to:
                                    </div>
                                </template>
                                <select
                                    id="proposed_wa_legislative_list"
                                    v-model="
                                        conservation_status_obj.wa_legislative_list_id
                                    "
                                    :disabled="isReadOnly"
                                    class="form-select"
                                    @change="
                                        filterWALegislativeCategories($event)
                                    "
                                >
                                    <option :value="null" disabled>
                                        Select the appropriate WA Legislative
                                        List
                                    </option>
                                    <option
                                        v-for="option in wa_legislative_lists"
                                        :key="option.id"
                                        :value="option.id"
                                    >
                                        {{ option.code }} - {{ option.label }}
                                    </option>
                                </select>
                            </template>
                            <template v-else>
                                <input
                                    class="form-control"
                                    type="text"
                                    :disabled="true"
                                    :value="
                                        conservation_status_obj.wa_legislative_list
                                            ? conservation_status_obj.wa_legislative_list
                                            : 'N/A'
                                    "
                                />
                            </template>
                        </div>
                    </div>
                    <div
                        v-if="conservation_status_obj.wa_legislative_list_id"
                        class="row mb-3"
                    >
                        <label
                            for="proposed_wa_legislative_category"
                            class="col-sm-5 col-form-label"
                            >WA Legislative Category:</label
                        >
                        <div class="col-sm-7">
                            <template v-if="!isReadOnly">
                                <template
                                    v-if="
                                        filtered_wa_legislative_categories &&
                                        filtered_wa_legislative_categories.length >
                                            0 &&
                                        conservation_status_obj.wa_legislative_category_id &&
                                        !filtered_wa_legislative_categories
                                            .map((d) => d.id)
                                            .includes(
                                                conservation_status_obj.wa_legislative_category_id
                                            )
                                    "
                                >
                                    <input
                                        v-if="
                                            conservation_status_obj.wa_legislative_category
                                        "
                                        type="text"
                                        class="form-control mb-3"
                                        :value="
                                            conservation_status_obj.wa_legislative_category +
                                            ' (Now Archived)'
                                        "
                                        disabled
                                    />
                                    <div class="mb-3 text-muted">
                                        Change wa legislative category to:
                                    </div>
                                </template>
                                <select
                                    id="proposed_wa_legislative_category"
                                    v-model="
                                        conservation_status_obj.wa_legislative_category_id
                                    "
                                    :disabled="isReadOnly"
                                    class="form-select"
                                >
                                    <option :value="null" disabled>
                                        Select the appropriate WA Legislative
                                        Category
                                    </option>
                                    <option
                                        v-for="option in filtered_wa_legislative_categories"
                                        :key="option.id"
                                        :value="option.id"
                                    >
                                        {{ option.code }} - {{ option.label }}
                                    </option>
                                </select>
                            </template>
                            <template v-else>
                                <input
                                    class="form-control"
                                    type="text"
                                    :disabled="true"
                                    :value="
                                        conservation_status_obj.wa_legislative_category
                                            ? conservation_status_obj.wa_legislative_category
                                            : 'N/A'
                                    "
                                />
                            </template>
                        </div>
                    </div>
                    <div class="row mb-3">
                        <label
                            for="proposed_iucnversion"
                            class="col-sm-5 col-form-label"
                            >IUCN Version:
                        </label>
                        <div class="col-sm-7">
                            <template v-if="!isReadOnly">
                                <template
                                    v-if="
                                        iucn_versions &&
                                        iucn_versions.length > 0 &&
                                        conservation_status_obj.iucn_version_id &&
                                        !iucn_versions
                                            .map((d) => d.id)
                                            .includes(
                                                conservation_status_obj.iucn_version_id
                                            )
                                    "
                                >
                                    <input
                                        v-if="
                                            conservation_status_obj.iucn_version
                                        "
                                        type="text"
                                        class="form-control mb-3"
                                        :value="
                                            conservation_status_obj.iucn_version +
                                            ' (Now Archived)'
                                        "
                                        disabled
                                    />
                                    <div class="mb-3 text-muted">
                                        Change iucn version to:
                                    </div>
                                </template>
                                <select
                                    id="proposed_iucnversion"
                                    v-model="
                                        conservation_status_obj.iucn_version_id
                                    "
                                    :disabled="isReadOnly"
                                    class="form-select"
                                >
                                    <option :value="null" disabled>
                                        Select the appropriate IUCN Version
                                    </option>
                                    <option
                                        v-for="option in iucn_versions"
                                        :key="option.id"
                                        :value="option.id"
                                    >
                                        {{ option.code }} - {{ option.label }}
                                    </option>
                                </select>
                            </template>
                            <template v-else>
                                <input
                                    class="form-control"
                                    type="text"
                                    :disabled="true"
                                    :value="
                                        conservation_status_obj.iucn_version
                                            ? conservation_status_obj.iucn_version
                                            : 'N/A'
                                    "
                                />
                            </template>
                        </div>
                    </div>
                    <div class="row mb-3">
                        <label
                            for="proposed_conservation_criteria"
                            class="col-sm-5 col-form-label"
                            >Conservation Criteria:</label
                        >
                        <div class="col-sm-7">
                            <input
                                id="proposed_conservation_criteria"
                                v-model="
                                    conservation_status_obj.conservation_criteria
                                "
                                :disabled="isReadOnly"
                                type="text"
                                class="form-control"
                                :placeholder="
                                    isReadOnly
                                        ? 'N/A'
                                        : 'Enter Conservation Criteria if Applicable'
                                "
                            />
                        </div>
                    </div>
                    <div class="row mb-3">
                        <label
                            for="proposed_wa_priority_list"
                            class="col-sm-5 col-form-label fw-bold"
                            >WA Priority List:
                            <span class="text-warning">*</span></label
                        >
                        <div class="col-sm-7">
                            <template v-if="!isReadOnly">
                                <template
                                    v-if="
                                        wa_priority_lists &&
                                        wa_priority_lists.length > 0 &&
                                        conservation_status_obj.wa_priority_list_id &&
                                        !wa_priority_lists
                                            .map((d) => d.id)
                                            .includes(
                                                conservation_status_obj.wa_priority_list_id
                                            )
                                    "
                                >
                                    <input
                                        v-if="
                                            conservation_status_obj.wa_priority_list
                                        "
                                        type="text"
                                        class="form-control mb-3"
                                        :value="
                                            conservation_status_obj.wa_priority_list +
                                            ' (Now Archived)'
                                        "
                                        disabled
                                    />
                                    <div class="mb-3 text-muted">
                                        Change wa priority list to:
                                    </div>
                                </template>
                                <select
                                    id="proposed_wa_priority_list"
                                    v-model="
                                        conservation_status_obj.wa_priority_list_id
                                    "
                                    :disabled="isReadOnly"
                                    class="form-select"
                                    @change="filterWAPriorityCategories($event)"
                                >
                                    <option :value="null" disabled>
                                        Select the appropriate WA Priority List
                                    </option>
                                    <option
                                        v-for="option in wa_priority_lists"
                                        :key="option.id"
                                        :value="option.id"
                                    >
                                        {{ option.code }} - {{ option.label }}
                                    </option>
                                </select>
                            </template>
                            <template v-else>
                                <input
                                    class="form-control"
                                    type="text"
                                    :disabled="true"
                                    :value="
                                        conservation_status_obj.wa_priority_list
                                            ? conservation_status_obj.wa_priority_list
                                            : 'N/A'
                                    "
                                />
                            </template>
                        </div>
                    </div>
                    <div
                        v-if="conservation_status_obj.wa_priority_list_id"
                        class="row mb-3"
                    >
                        <label
                            for="proposed_wa_priority_category"
                            class="col-sm-5 col-form-label"
                            >WA Priority Category:</label
                        >
                        <div class="col-sm-7">
                            <template v-if="!isReadOnly">
                                <template
                                    v-if="
                                        filtered_wa_priority_categories &&
                                        filtered_wa_priority_categories.length >
                                            0 &&
                                        conservation_status_obj.wa_priority_category_id &&
                                        !filtered_wa_priority_categories
                                            .map((d) => d.id)
                                            .includes(
                                                conservation_status_obj.wa_priority_category_id
                                            )
                                    "
                                >
                                    <input
                                        v-if="
                                            conservation_status_obj.wa_priority_category
                                        "
                                        type="text"
                                        class="form-control mb-3"
                                        :value="
                                            conservation_status_obj.wa_priority_category +
                                            ' (Now Archived)'
                                        "
                                        disabled
                                    />
                                    <div class="mb-3 text-muted">
                                        Change wa priority category to:
                                    </div>
                                </template>
                                <select
                                    id="proposed_wa_priority_category"
                                    v-model="
                                        conservation_status_obj.wa_priority_category_id
                                    "
                                    :disabled="isReadOnly"
                                    class="form-select"
                                >
                                    <option :value="null" disabled>
                                        Select the appropriate WA Priority
                                        Category
                                    </option>
                                    <option
                                        v-for="option in filtered_wa_priority_categories"
                                        :key="option.id"
                                        :value="option.id"
                                    >
                                        {{ option.code }} - {{ option.label }}
                                    </option>
                                </select>
                            </template>
                            <template v-else>
                                <input
                                    class="form-control"
                                    type="text"
                                    :disabled="true"
                                    :value="
                                        conservation_status_obj.wa_priority_category
                                            ? conservation_status_obj.wa_priority_category
                                            : 'N/A'
                                    "
                                />
                            </template>
                        </div>
                    </div>
                    <div class="row mb-3">
                        <label
                            for="proposed_commonwealth_conservation_category"
                            class="col-sm-5 col-form-label"
                            >Commonwealth Conservation Category:</label
                        >
                        <div class="col-sm-7">
                            <template v-if="!isReadOnly">
                                <template
                                    v-if="
                                        commonwealth_conservation_categories &&
                                        commonwealth_conservation_categories.length >
                                            0 &&
                                        conservation_status_obj.commonwealth_conservation_category_id &&
                                        !commonwealth_conservation_categories
                                            .map((d) => d.id)
                                            .includes(
                                                conservation_status_obj.commonwealth_conservation_category_id
                                            )
                                    "
                                >
                                    <input
                                        v-if="
                                            conservation_status_obj.commonwealth_conservation_category
                                        "
                                        type="text"
                                        class="form-control mb-3"
                                        :value="
                                            conservation_status_obj.commonwealth_conservation_category +
                                            ' (Now Archived)'
                                        "
                                        disabled
                                    />
                                    <div class="mb-3 text-muted">
                                        Change commonwealth conservation list
                                        to:
                                    </div>
                                </template>
                                <select
                                    id="proposed_commonwealth_conservation_category"
                                    v-model="
                                        conservation_status_obj.commonwealth_conservation_category_id
                                    "
                                    :disabled="isReadOnly"
                                    class="form-select"
                                >
                                    <option :value="null" disabled>
                                        Select the appropriate Commonwealth
                                        Conservation Category
                                    </option>
                                    <option
                                        v-for="option in commonwealth_conservation_categories"
                                        :key="option.id"
                                        :value="option.id"
                                    >
                                        {{ option.code }} - {{ option.label }}
                                    </option>
                                </select>
                            </template>
                            <template v-else>
                                <input
                                    class="form-control"
                                    type="text"
                                    :disabled="true"
                                    :value="
                                        conservation_status_obj.commonwealth_conservation_category
                                            ? conservation_status_obj.commonwealth_conservation_category
                                            : 'N/A'
                                    "
                                />
                            </template>
                        </div>
                    </div>
                    <div class="row mb-3">
                        <label
                            for="proposed_other_conservation_assessment"
                            class="col-sm-5 col-form-label"
                            >Other Conservation Assessment:</label
                        >
                        <div class="col-sm-7">
                            <template v-if="!isReadOnly">
                                <template
                                    v-if="
                                        other_conservation_assessments &&
                                        other_conservation_assessments.length >
                                            0 &&
                                        conservation_status_obj.other_conservation_assessment_id &&
                                        !other_conservation_assessments
                                            .map((d) => d.id)
                                            .includes(
                                                conservation_status_obj.other_conservation_assessment_id
                                            )
                                    "
                                >
                                    <input
                                        v-if="
                                            conservation_status_obj.other_conservation_assessment
                                        "
                                        type="text"
                                        class="form-control mb-3"
                                        :value="
                                            conservation_status_obj.other_conservation_assessment +
                                            ' (Now Archived)'
                                        "
                                        disabled
                                    />
                                    <div class="mb-3 text-muted">
                                        Change other conservation assessment to:
                                    </div>
                                </template>
                                <select
                                    id="proposed_other_conservation_assessment"
                                    v-model="
                                        conservation_status_obj.other_conservation_assessment_id
                                    "
                                    :disabled="isReadOnly"
                                    class="form-select"
                                >
                                    <option :value="null" disabled>
                                        Select the appropriate Other
                                        Conservation Assessment
                                    </option>
                                    <option
                                        v-for="option in other_conservation_assessments"
                                        :key="option.id"
                                        :value="option.id"
                                    >
                                        {{ option.code }} - {{ option.label }}
                                    </option>
                                </select>
                            </template>
                            <template v-else>
                                <input
                                    class="form-control"
                                    type="text"
                                    :disabled="true"
                                    :value="
                                        conservation_status_obj.other_conservation_assessment
                                            ? conservation_status_obj.other_conservation_assessment
                                            : 'N/A'
                                    "
                                />
                            </template>
                        </div>
                    </div>
                    <div class="row mb-3">
                        <label
                            for="comment"
                            class="col-sm-5 col-form-label fw-bold"
                            >Comments: <span class="text-danger">*</span></label
                        >
                        <div class="col-sm-7">
                            <textarea
                                id="comment"
                                v-model="conservation_status_obj.comment"
                                :disabled="isReadOnly"
                                class="form-control"
                                rows="3"
                                placeholder=""
                            />
                        </div>
                    </div>
                    <div v-if="!is_external" class="row mb-3">
                        <label
                            for="conservation_status_under_review"
                            class="col-sm-6 col-form-label"
                            >Is there a Conservation Status proposal under
                            review?</label
                        >
                        <div class="col-sm-6 d-flex align-items-center">
                            <div class="form-check form-check-inline">
                                Yes
                                <input
                                    id="conservation_status_under_review_yes"
                                    v-model="isConservationStatusUnderReview"
                                    disabled
                                    type="radio"
                                    class="form-check-input"
                                    name="conservation_status_under_review"
                                    :value="true"
                                />
                            </div>
                            <div class="form-check form-check-inline">
                                No
                                <input
                                    id="conservation_status_under_review_no"
                                    v-model="isConservationStatusUnderReview"
                                    disabled
                                    type="radio"
                                    class="form-check-input"
                                    name="conservation_status_under_review"
                                    :value="false"
                                />
                            </div>
                            <div
                                v-if="
                                    conservation_status_obj.conservation_status_under_review &&
                                    conservation_status_obj.id !=
                                        conservation_status_obj
                                            .conservation_status_under_review.id
                                "
                            >
                                <div>
                                    <a
                                        :href="`/internal/conservation-status/${conservation_status_obj.conservation_status_under_review.id}`"
                                        target="_blank"
                                        class="btn btn-primary"
                                        >{{
                                            conservation_status_obj
                                                .conservation_status_under_review
                                                .conservation_status_number
                                        }}<i
                                            class="bi bi-box-arrow-up-right ps-2"
                                        ></i
                                    ></a>
                                </div>
                            </div>
                        </div>
                    </div>
                </template>
                <template
                    v-if="
                        canViewCurrentList &&
                        conservation_status_obj.current_conservation_status
                    "
                >
                    <div class="row border-top pt-3">
                        <h5 class="text-muted mb-4">
                            Current Conservation Status
                        </h5>
                    </div>
                    <div class="row mb-3">
                        <label
                            for="conservation_status_number"
                            class="col-sm-5 col-form-label"
                            >Conservation Status Number:</label
                        >
                        <div class="col-sm-7">
                            <a
                                :href="`/internal/conservation-status/${conservation_status_obj.current_conservation_status.id}`"
                                target="_blank"
                                class="btn btn-primary"
                                >{{
                                    conservation_status_obj
                                        .current_conservation_status
                                        .conservation_status_number
                                }}<i class="bi bi-box-arrow-up-right ps-2"></i
                            ></a>
                        </div>
                    </div>
                    <div
                        v-if="
                            conservation_status_obj.current_conservation_status
                                .wa_legislative_list_id
                        "
                        class="row mb-3"
                    >
                        <label
                            for="current_wa_legislative_list"
                            class="col-sm-5 col-form-label"
                            >WA Legislative List:</label
                        >
                        <div class="col-sm-7">
                            <select
                                id="current_wa_legislative_list"
                                v-model="
                                    conservation_status_obj
                                        .current_conservation_status
                                        .wa_legislative_list_id
                                "
                                :disabled="true"
                                class="form-select"
                                @change="filterWALegislativeCategories($event)"
                            >
                                <option
                                    v-for="option in wa_legislative_lists"
                                    :key="option.id"
                                    :value="option.id"
                                >
                                    {{ option.code }}
                                </option>
                            </select>
                        </div>
                    </div>
                    <div
                        v-if="
                            conservation_status_obj.current_conservation_status
                                .wa_legislative_category_id
                        "
                        class="row mb-3"
                    >
                        <label
                            for="current_wa_legislative_category"
                            class="col-sm-5 col-form-label"
                            >WA Legislative Category:</label
                        >
                        <div class="col-sm-7">
                            <select
                                id="current_wa_legislative_category"
                                v-model="
                                    conservation_status_obj
                                        .current_conservation_status
                                        .wa_legislative_category_id
                                "
                                :disabled="true"
                                class="form-select"
                            >
                                <option
                                    v-for="option in wa_legislative_categories"
                                    :key="option.id"
                                    :value="option.id"
                                >
                                    {{ option.code }}
                                </option>
                            </select>
                        </div>
                    </div>
                    <div
                        v-if="
                            conservation_status_obj.current_conservation_status
                                .iucn_version_id
                        "
                        class="row mb-3"
                    >
                        <label
                            for="current_iucn_version"
                            class="col-sm-5 col-form-label"
                            >IUCN Version:</label
                        >
                        <div class="col-sm-7">
                            <select
                                id="current_iucn_version"
                                v-model="
                                    conservation_status_obj
                                        .current_conservation_status
                                        .iucn_version_id
                                "
                                :disabled="true"
                                class="form-select"
                            >
                                <option
                                    v-for="option in iucn_versions"
                                    :key="option.id"
                                    :value="option.id"
                                >
                                    {{ option.code }}
                                </option>
                            </select>
                        </div>
                    </div>
                    <div
                        v-if="
                            conservation_status_obj.current_conservation_status
                                .conservation_criteria
                        "
                        class="row mb-3"
                    >
                        <label
                            for="current_conservation_criteria"
                            class="col-sm-5 col-form-label"
                            >Conservation Criteria:</label
                        >
                        <div class="col-sm-7">
                            <input
                                id="current_conservation_criteria"
                                v-model="
                                    conservation_status_obj
                                        .current_conservation_status
                                        .conservation_criteria
                                "
                                :disabled="true"
                                type="text"
                                class="form-control"
                            />
                        </div>
                    </div>
                    <div
                        v-if="
                            conservation_status_obj.current_conservation_status
                                .wa_priority_list_id
                        "
                        class="row mb-3"
                    >
                        <label
                            for="current_wa_priority_list"
                            class="col-sm-5 col-form-label"
                            >WA Priority List:</label
                        >
                        <div class="col-sm-7">
                            <select
                                id="current_wa_priority_list"
                                v-model="
                                    conservation_status_obj
                                        .current_conservation_status
                                        .wa_priority_list_id
                                "
                                :disabled="true"
                                class="form-select"
                                @change="filterWAPriorityCategories($event)"
                            >
                                <option
                                    v-for="option in wa_priority_lists"
                                    :key="option.id"
                                    :value="option.id"
                                >
                                    {{ option.code }}
                                </option>
                            </select>
                        </div>
                    </div>
                    <div
                        v-if="
                            conservation_status_obj.current_conservation_status
                                .wa_priority_category_id
                        "
                        class="row mb-3"
                    >
                        <label
                            for="current_wa_priority_category"
                            class="col-sm-5 col-form-label"
                            >WA Priority Category:</label
                        >
                        <div class="col-sm-7">
                            <select
                                id="current_wa_priority_category"
                                v-model="
                                    conservation_status_obj
                                        .current_conservation_status
                                        .wa_priority_category_id
                                "
                                :disabled="true"
                                class="form-select"
                            >
                                <option
                                    v-for="option in wa_priority_categories"
                                    :key="option.id"
                                    :value="option.id"
                                >
                                    {{ option.code }}
                                </option>
                            </select>
                        </div>
                    </div>
                    <div
                        v-if="
                            conservation_status_obj.current_conservation_status
                                .commonwealth_conservation_category_id
                        "
                        class="row mb-3"
                    >
                        <label
                            for="current_commonwealth_conservation_category"
                            class="col-sm-5 col-form-label"
                            >Commonwealth Conservation Category:</label
                        >
                        <div class="col-sm-7">
                            <select
                                id="current_commonwealth_conservation_category"
                                v-model="
                                    conservation_status_obj
                                        .current_conservation_status
                                        .commonwealth_conservation_category_id
                                "
                                :disabled="true"
                                class="form-select"
                            >
                                <option
                                    v-for="option in commonwealth_conservation_categories"
                                    :key="option.id"
                                    :value="option.id"
                                >
                                    {{ option.code }}
                                </option>
                            </select>
                        </div>
                    </div>
                    <div
                        v-if="
                            conservation_status_obj.current_conservation_status
                                .other_conservation_assessment
                        "
                        class="row mb-3"
                    >
                        <label
                            for="current_other_conservation_assessment"
                            class="col-sm-5 col-form-label"
                            >Other Conservation Assessment:</label
                        >
                        <div class="col-sm-7">
                            <input
                                id="current_other_conservation_assessment"
                                v-model="
                                    conservation_status_obj
                                        .current_conservation_status
                                        .other_conservation_assessment
                                "
                                :disabled="true"
                                type="text"
                                class="form-control"
                            />
                        </div>
                    </div>
                    <div
                        v-if="
                            conservation_status_obj.current_conservation_status
                                .comment
                        "
                        class="row mb-3"
                    >
                        <label
                            for="comment"
                            class="col-sm-5 col-form-label fw-bold"
                            >Comments:</label
                        >
                        <div class="col-sm-7">
                            <textarea
                                id="current_comment"
                                v-model="
                                    conservation_status_obj
                                        .current_conservation_status.comment
                                "
                                :disabled="true"
                                class="form-control"
                                rows="3"
                            />
                        </div>
                    </div>
                </template>
                <template v-if="!is_external && isStatusApproved">
                    <div class="row border-top pt-3 pb-3">
                        <label for="" class="col-sm-5 col-form-label"
                            >Approval Document:</label
                        >
                        <div class="col-sm-8">
                            <p class="col-form-label">
                                <template
                                    v-if="
                                        conservation_status_obj.conservation_status_approval_document
                                    "
                                >
                                    <strong
                                        ><a
                                            :href="
                                                conservation_status_obj
                                                    .conservation_status_approval_document[1]
                                            "
                                            target="_blank"
                                            >{{
                                                conservation_status_obj
                                                    .conservation_status_approval_document[0]
                                            }}</a
                                        ></strong
                                    >
                                </template>
                                <template v-else
                                    >No approval document uploaded</template
                                >
                            </p>
                        </div>
                    </div>
                </template>
            </form>
            <template
                v-if="!is_external && !conservation_status_obj.can_user_edit"
            >
                <div class="row border-top pt-3 pb-0 mb-0">
                    <CollapsibleComponent
                        ref="assessment_comments"
                        component_title="Assessment Comments"
                        :collapsed="true"
                    >
                        <div class="row pb-0 mb-0">
                            <div class="col rounded">
                                <div
                                    v-if="assessorCommentVisibility"
                                    class="row"
                                >
                                    <div class="col mt-3">
                                        <div class="form-floating m-3 mt-1">
                                            <textarea
                                                id="assessor_comment"
                                                v-model="
                                                    conservation_status_obj.assessor_data
                                                "
                                                :disabled="
                                                    assessor_comment_readonly
                                                "
                                                class="form-control"
                                                rows="3"
                                                placeholder="Assessor Comments"
                                            />
                                            <label
                                                for=""
                                                class="col-sm-4 col-form-label"
                                                >Assessor Comments</label
                                            >
                                        </div>
                                    </div>
                                </div>
                                <div v-if="referral_comments_boxes.length > 0">
                                    <div>
                                        <div class="row mt-2">
                                            <div class="col ms-3">
                                                <h6 class="text-muted">
                                                    Referral Comments
                                                </h6>
                                            </div>
                                        </div>
                                        <template
                                            v-if="
                                                referral_comments_boxes &&
                                                referral_comments_boxes.length >
                                                    0
                                            "
                                        >
                                            <div
                                                v-for="ref in referral_comments_boxes"
                                                class="row mb-3"
                                                :key="ref.name"
                                            >
                                                <div class="col">
                                                    <div
                                                        class="form-floating m-3 mt-1"
                                                    >
                                                        <textarea
                                                            v-if="!ref.readonly"
                                                            :id="ref.name"
                                                            v-model="
                                                                referral.referral_comment
                                                            "
                                                            :disabled="true"
                                                            :name="ref.name"
                                                            class="form-control"
                                                            :placeholder="
                                                                ref.label
                                                            "
                                                        />
                                                        <textarea
                                                            v-else
                                                            :disabled="true"
                                                            :name="ref.name"
                                                            :value="
                                                                ref.value || ''
                                                            "
                                                            class="form-control"
                                                            :placeholder="
                                                                ref.label
                                                            "
                                                        />
                                                        <label
                                                            :for="ref.name"
                                                            class="form-label"
                                                            >{{
                                                                ref.label
                                                            }}</label
                                                        >
                                                    </div>
                                                </div>
                                            </div>
                                        </template>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </CollapsibleComponent>
                </div>
            </template>
        </FormSection>
    </div>
</template>

<script>
import { v4 as uuid } from 'uuid';
import FormSection from '@/components/forms/section_toggle.vue';
import CollapsibleComponent from '@/components/forms/collapsible_component.vue';
import { api_endpoints, constants } from '@/utils/hooks';

export default {
    name: 'CommunityStatus',
    components: {
        CollapsibleComponent,
        FormSection,
    },
    props: {
        conservation_status_obj: {
            type: Object,
            required: true,
        },
        referral: {
            type: Object,
            required: false,
        },
        is_external: {
            type: Boolean,
            default: false,
        },
        canEditStatus: {
            type: Boolean,
            default: true,
        },
    },
    emits: ['saveConservationStatus'],
    data: function () {
        return {
            community_name_lookup: 'community_name_lookup' + uuid(),
            community_id_lookup: 'community_id_lookup' + uuid(),
            select_community_name: 'select_community_name' + uuid(),
            select_community_id: 'select_community_id' + uuid(),
            isShowComment: false,
            cs_profile_dict: {},
            wa_legislative_lists: [],
            wa_legislative_categories: [],
            iucn_versions: [],
            wa_priority_lists: [],
            wa_priority_categories: [],
            commonwealth_conservation_categories: [],
            change_codes: [],
            filtered_wa_legislative_categories: [],
            filtered_wa_priority_categories: [],
            referral_comments_boxes: [],
            other_conservation_assessments: [],
            community_display: '',
            isSyncingCommunitySelects: false,
        };
    },
    computed: {
        show_administrative_information: function () {
            return (
                !this.is_external &&
                this.conservation_status_obj.community_id &&
                this.conservation_status_obj.processing_status != 'Draft'
            );
        },
        show_proposed_conservation_status: function () {
            return this.conservation_status_obj.community_id;
        },
        show_effective_date_fields: function () {
            return (
                this.conservation_status_obj.effective_from ||
                this.conservation_status_obj.effective_to ||
                constants.EFFECTIVE_FROM_STATUSES.includes(
                    this.conservation_status_obj.processing_status
                )
            );
        },
        show_effective_from: function () {
            return constants.EFFECTIVE_FROM_STATUSES.includes(
                this.conservation_status_obj.processing_status
            );
        },
        show_effective_to: function () {
            return constants.EFFECTIVE_TO_STATUSES.includes(
                this.conservation_status_obj.processing_status
            );
        },
        effective_to_mandatory: function () {
            return constants.EFFECTIVE_TO_STATUSES.includes(
                this.conservation_status_obj.processing_status
            );
        },
        show_listing_and_review_due_date: function () {
            return !['Draft', 'Discarded'].includes(
                this.conservation_status_obj.processing_status
            );
        },
        listing_and_review_due_date_disabled: function () {
            return (
                this.isReadOnly ||
                ![
                    'With Assessor',
                    'Deferred',
                    'With Referral',
                    'Approved',
                    'Declined',
                    'DeListed',
                    'Closed',
                ].includes(this.conservation_status_obj.processing_status) ||
                this.conservation_status_obj.locked
            );
        },
        listing_date_display_as_na: function () {
            return (
                !this.conservation_status_obj.listing_date &&
                this.listing_and_review_due_date_disabled
            );
        },
        review_due_date_display_as_na: function () {
            return (
                !this.conservation_status_obj.review_due_date &&
                this.listing_and_review_due_date_disabled
            );
        },
        approval_level_disabled: function () {
            return (
                this.isReadOnly ||
                !['With Assessor', 'With Referral'].includes(
                    this.conservation_status_obj.processing_status
                )
            );
        },
        assessorCommentVisibility: function () {
            return this.conservation_status_obj.assessor_mode.assessor_box_view;
        },
        has_comment_value: function () {
            let has_value = false;
            for (var i = 0; i < this.referral_comments_boxes.length; i++) {
                if (
                    Object.prototype.hasOwnProperty.call(
                        this.referral_comments_boxes[i],
                        'value'
                    )
                ) {
                    if (
                        this.referral_comments_boxes[i].value != null &&
                        this.referral_comments_boxes[i].value != undefined &&
                        this.referral_comments_boxes[i].value != ''
                    ) {
                        has_value = true;
                    }
                }
            }
            return has_value;
        },
        isStatusApproved: function () {
            return this.conservation_status_obj.processing_status == 'Approved'
                ? true
                : false;
        },
        isAssignedOfficer: function () {
            if (
                ['With Assessor', 'With Referral', 'Deferred'].includes(
                    this.conservation_status_obj.processing_status
                )
            ) {
                return (
                    Boolean(this.conservation_status_obj.assigned_officer) &&
                    this.conservation_status_obj.assigned_officer ==
                        this.conservation_status_obj.current_assessor.id
                );
            }
            if (this.conservation_status_obj.processing_status != 'Draft') {
                return (
                    Boolean(this.conservation_status_obj.assigned_approver) &&
                    this.conservation_status_obj.assigned_approver ==
                        this.conservation_status_obj.current_assessor.id
                );
            }
            return false;
        },
        isReadOnly: function () {
            if (this.is_external) {
                return !this.conservation_status_obj.can_user_edit;
            } else if (
                this.conservation_status_obj.processing_status ==
                    'With Referral' &&
                this.referral
            ) {
                return true;
            } else {
                if (
                    ['Ready For Agenda'].includes(
                        this.conservation_status_obj.processing_status
                    ) ||
                    this.conservation_status_obj.locked
                ) {
                    return true;
                }
                if (
                    (this.conservation_status_obj.assessor_mode
                        .assessor_can_assess &&
                        this.isAssignedOfficer) ||
                    this.conservation_status_obj.internal_user_edit
                ) {
                    return false;
                }
            }

            return true;
        },
        isCommunityNameReadOnly: function () {
            const readOnlyStatuses = [
                constants.PROPOSAL_STATUS.WITH_APPROVER.TEXT,
                'With Approver',
                constants.PROPOSAL_STATUS.READY_FOR_AGENDA.TEXT,
                constants.PROPOSAL_STATUS.ON_AGENDA.TEXT,
                constants.PROPOSAL_STATUS.APPROVED.TEXT,
                constants.PROPOSAL_STATUS.DECLINED.TEXT,
                constants.PROPOSAL_STATUS.CLOSED.TEXT,
                constants.PROPOSAL_STATUS.DELISTED.TEXT,
            ];
            return (
                this.isReadOnly ||
                (!this.is_external &&
                    readOnlyStatuses.includes(
                        this.conservation_status_obj.processing_status
                    ) &&
                    !this.conservation_status_obj.locked)
            );
        },
        conservation_list_proposed: function () {
            return !['Approved', 'DeListed', 'Closed'].includes(
                this.conservation_status_obj.processing_status
            );
        },
        canViewCurrentList: function () {
            return this.conservation_status_obj.processing_status ==
                'Approved' ||
                this.conservation_status_obj.processing_status == 'DeListed'
                ? false
                : true;
        },
        isConservationStatusUnderReview: function () {
            return Boolean(
                this.conservation_status_obj.conservation_status_under_review
            );
        },
        assessor_comment_readonly: function () {
            let status_without_assessor =
                this.conservation_status_obj.assessor_mode
                    .status_without_assessor;
            if (
                this.conservation_status_obj.processing_status === 'Approved' &&
                !this.conservation_status_obj.locked
            ) {
                status_without_assessor = false;
            }
            return !this.is_external &&
                !this.conservation_status_obj.can_user_edit &&
                this.conservation_status_obj.assessor_mode.assessor_level ==
                    'assessor' &&
                this.conservation_status_obj.assessor_mode.has_assessor_mode &&
                !status_without_assessor
                ? false
                : true;
        },
    },
    created: async function () {
        let vm = this;
        let action = this.$route.query.action;
        let dict_url =
            action == 'view'
                ? api_endpoints.cs_profile_dict +
                  '?group_type=' +
                  vm.conservation_status_obj.group_type +
                  '&action=' +
                  action
                : api_endpoints.cs_profile_dict +
                  '?group_type=' +
                  vm.conservation_status_obj.group_type;
        fetch(dict_url).then(
            async (response) => {
                vm.cs_profile_dict = await response.json();
                vm.wa_legislative_lists =
                    vm.cs_profile_dict.wa_legislative_lists;
                vm.wa_legislative_categories =
                    vm.cs_profile_dict.wa_legislative_categories;
                vm.iucn_versions = vm.cs_profile_dict.iucn_versions;
                vm.wa_priority_lists = vm.cs_profile_dict.wa_priority_lists;
                vm.wa_priority_categories =
                    vm.cs_profile_dict.wa_priority_categories;
                vm.commonwealth_conservation_categories =
                    vm.cs_profile_dict.commonwealth_conservation_categories;
                vm.other_conservation_assessments =
                    vm.cs_profile_dict.other_conservation_assessments;
                vm.change_codes = vm.cs_profile_dict.active_change_codes;
                this.getCommunityDisplay();
                this.filterWALegislativeCategories();
                this.filterWAPriorityCategories();
                if (!vm.is_external) {
                    this.generateReferralCommentBoxes();
                }
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
            vm.initialiseCommunityIDLookup();
        });
    },
    methods: {
        isOptionDisabled: function (option) {
            if (option.code && option.code.toLowerCase() === 'delist') {
                return !this.conservation_status_obj
                    .current_conservation_status;
            }
            return false;
        },
        getOptionLabel: function (option) {
            let label = option.code;
            if (
                this.isOptionDisabled(option) &&
                option.code &&
                option.code.toLowerCase() === 'delist'
            ) {
                label += ' (Disabled: No Active Conservation Status Found)';
            }
            return label;
        },
        approvalLevelChanged: function () {
            this.conservation_status_obj.effective_from = null;
        },
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
                                cs_community: true,
                            };
                            return query;
                        },
                    },
                })
                .on('select2:select', function (e) {
                    if (vm.isSyncingCommunitySelects) {
                        vm.isSyncingCommunitySelects = false;
                        return;
                    }
                    const selected = e.params.data;
                    const communityId = selected.id;
                    const communityMigratedId =
                        selected.community_common_id || communityId;
                    vm.conservation_status_obj.community_id = communityId;
                    vm.community_display = selected.text;
                    vm.isSyncingCommunitySelects = true;
                    vm.setSelect2Value(
                        vm.community_id_lookup,
                        communityId,
                        communityMigratedId
                    );
                    vm.$emit('saveConservationStatus');
                    setTimeout(() => {
                        vm.isSyncingCommunitySelects = false;
                    }, 0);
                })
                .on('select2:clear', function () {
                    vm.isSyncingCommunitySelects = true;
                    vm.clearSelect2Value(vm.community_id_lookup);
                    vm.conservation_status_obj.community_id = null;
                    vm.community_display = '';
                    vm.$emit('saveConservationStatus');
                    setTimeout(() => {
                        vm.isSyncingCommunitySelects = false;
                    }, 0);
                })
                .on('select2:unselect', function () {
                    if (vm.isSyncingCommunitySelects) {
                        vm.isSyncingCommunitySelects = false;
                        return;
                    }
                    vm.isSyncingCommunitySelects = true;
                    vm.clearSelect2Value(vm.community_id_lookup);
                    vm.conservation_status_obj.community_id = null;
                    vm.community_display = '';
                    vm.$emit('saveConservationStatus');
                    setTimeout(() => {
                        vm.isSyncingCommunitySelects = false;
                    }, 0);
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-' +
                            vm.community_name_lookup +
                            '-results"]'
                    );
                    searchField[0].focus();
                });

            if (!vm.conservation_status_obj?.community_id) {
                $(vm.$refs[vm.community_name_lookup]).select2('open');
            }
        },
        initialiseCommunityIDLookup: function () {
            let vm = this;
            $(vm.$refs[vm.community_id_lookup])
                .select2({
                    minimumInputLength: 2,
                    dropdownParent: $('#' + vm.select_community_id),
                    theme: 'bootstrap-5',
                    allowClear: true,
                    placeholder: 'Select Community ID',
                    ajax: {
                        url: api_endpoints.community_id_lookup,
                        dataType: 'json',
                        data: function (params) {
                            var query = {
                                term: params.term,
                                type: 'public',
                                cs_community: true,
                            };
                            return query;
                        },
                    },
                })
                .on('select2:select', function (e) {
                    if (vm.isSyncingCommunitySelects) {
                        vm.isSyncingCommunitySelects = false;
                        return;
                    }
                    const selected = e.params.data;
                    const communityId = selected.community_id || selected.id;
                    const migratedId = selected.text || communityId;
                    const communityName =
                        selected.community_name || selected.text || '';
                    vm.conservation_status_obj.community_id = communityId;
                    vm.community_display =
                        communityName || vm.community_display || '';
                    vm.isSyncingCommunitySelects = true;
                    vm.setSelect2Value(
                        vm.community_id_lookup,
                        communityId,
                        migratedId
                    );
                    if (communityName) {
                        vm.setSelect2Value(
                            vm.community_name_lookup,
                            communityId,
                            communityName
                        );
                    } else {
                        vm.clearSelect2Value(vm.community_name_lookup);
                    }
                    vm.$emit('saveConservationStatus');
                    setTimeout(() => {
                        vm.isSyncingCommunitySelects = false;
                    }, 0);
                })
                .on('select2:clear', function () {
                    vm.isSyncingCommunitySelects = true;
                    vm.clearSelect2Value(vm.community_name_lookup);
                    vm.conservation_status_obj.community_id = null;
                    vm.community_display = '';
                    vm.$emit('saveConservationStatus');
                    setTimeout(() => {
                        vm.isSyncingCommunitySelects = false;
                    }, 0);
                })
                .on('select2:unselect', function () {
                    if (vm.isSyncingCommunitySelects) {
                        vm.isSyncingCommunitySelects = false;
                        return;
                    }
                    vm.isSyncingCommunitySelects = true;
                    vm.clearSelect2Value(vm.community_name_lookup);
                    vm.conservation_status_obj.community_id = null;
                    vm.community_display = '';
                    vm.$emit('saveConservationStatus');
                    setTimeout(() => {
                        vm.isSyncingCommunitySelects = false;
                    }, 0);
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-' +
                            vm.community_id_lookup +
                            '-results"]'
                    );
                    searchField[0].focus();
                });
        },
        getCommunityDisplay: function () {
            let vm = this;
            const communityId = vm.conservation_status_obj?.community_id;
            if (!communityId) {
                return;
            }

            vm.isSyncingCommunitySelects = true;
            vm.fetchCommunityDetails(communityId)
                .then((community) => {
                    if (!community) {
                        return;
                    }

                    const applySelections = () => {
                        vm.isSyncingCommunitySelects = true;
                        vm.community_display = community.name;
                        vm.setSelect2Value(
                            vm.community_name_lookup,
                            community.id,
                            community.name
                        );
                        vm.setSelect2Value(
                            vm.community_id_lookup,
                            community.id,
                            community.community_common_id || community.id
                        );
                        vm.isSyncingCommunitySelects = false;
                    };

                    if (
                        vm.$refs[vm.community_name_lookup] &&
                        vm.$refs[vm.community_id_lookup]
                    ) {
                        applySelections();
                    } else {
                        vm.$nextTick(applySelections);
                    }
                })
                .catch((error) => {
                    console.log(error);
                })
                .finally(() => {
                    vm.isSyncingCommunitySelects = false;
                });
        },
        setSelect2Value: function (refKey, value, label) {
            const selectEl = this.$refs[refKey];
            if (!selectEl) {
                return;
            }

            const optionValue =
                value === undefined || value === null ? '' : value.toString();
            const optionLabel =
                label === undefined || label === null
                    ? optionValue
                    : label.toString();

            let option = Array.from(selectEl.options || []).find((opt) => {
                return opt.value === optionValue;
            });

            if (!option) {
                option = new Option(optionLabel, optionValue, true, true);
                selectEl.add(option);
            } else {
                option.selected = true;
                option.text = optionLabel;
            }

            selectEl.value = optionValue;
            $(selectEl).trigger('change');
        },
        clearSelect2Value: function (refKey) {
            const selectEl = this.$refs[refKey];
            if (!selectEl) {
                return;
            }

            selectEl.value = '';
            $(selectEl).val(null).trigger('change');
        },
        fetchCommunityDetails: function (communityId) {
            if (!communityId) {
                return Promise.resolve(null);
            }

            const url =
                api_endpoints.community_display +
                '?community_id=' +
                communityId;

            return fetch(url)
                .then((response) => {
                    if (!response.ok) {
                        throw response;
                    }
                    return response.json();
                })
                .then((data) => {
                    if (!data || !data.id) {
                        return null;
                    }
                    return data;
                });
        },
        filterWALegislativeCategories: function (event) {
            this.$nextTick(() => {
                if (event) {
                    this.conservation_status_obj.wa_legislative_category_id =
                        null;
                }

                this.filtered_wa_legislative_categories =
                    this.wa_legislative_categories.filter((choice) => {
                        return choice.list_ids.includes(
                            this.conservation_status_obj.wa_legislative_list_id
                        );
                    });
            });
        },
        filterWAPriorityCategories: function (event) {
            this.$nextTick(() => {
                if (event) {
                    this.conservation_status_obj.wa_priority_category_id = null;
                }

                this.filtered_wa_priority_categories =
                    this.wa_priority_categories.filter((choice) => {
                        return choice.list_ids.includes(
                            this.conservation_status_obj.wa_priority_list_id
                        );
                    });
            });
        },
        generateReferralCommentBoxes: function () {
            var box_visibility =
                this.conservation_status_obj.assessor_mode.assessor_box_view;
            var assessor_mode =
                this.conservation_status_obj.assessor_mode.assessor_level;
            if (!this.conservation_status_obj.can_user_edit) {
                $.each(this.conservation_status_obj.referrals, (i, v) => {
                    var referral_name = `comment-field-Referral-${v.referral.email}`;
                    var referral_visibility =
                        assessor_mode == 'referral' &&
                        this.conservation_status_obj.assessor_mode
                            .assessor_can_assess &&
                        this.referral.referral == v.referral.id
                            ? false
                            : true;
                    var referral_label = `${v.referral.fullname}`;
                    var referral_comment_val = `${v.referral_comment}`;
                    this.referral_comments_boxes.push({
                        box_view: box_visibility,
                        name: referral_name,
                        label: referral_label,
                        readonly: referral_visibility,
                        value: referral_comment_val,
                    });
                });
            }
        },
        toggleComment: function (updatedShowComment) {
            this.isShowComment = updatedShowComment;
        },
        saveConservationStatus: function (e) {
            if (this.isSyncingCommunitySelects) {
                return;
            }
            if (e.target.classList.contains('input.select2-search__field')) {
                // We will emit this save from the select 2 event instead
                // as it requires some time to populate the selected value
                // and we don't want to save the conservation status before the
                // select2 value is populated
                return;
            }
            this.$emit('saveConservationStatus');
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
</style>
