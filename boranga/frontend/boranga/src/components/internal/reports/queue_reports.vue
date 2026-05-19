<template>
    <div id="internal-reports" class="container">
        <h2 class="mb-4">Queue Reports</h2>
        <div class="alert alert-primary">
            <i class="bi bi-info-circle-fill text-primary me-1 fs-5"></i>
            Email reports will be sent to the requesting user some time after
            the request has been made. Reports are processed every 2 minutes.
        </div>

        <div v-if="errorMessage" class="alert alert-danger">
            {{ errorMessage }}
        </div>

        <div class="card">
            <div class="card-body">
                <div class="row mb-3">
                    <label class="col-sm-3 col-form-label fw-bold"
                        >Report Type</label
                    >
                    <div class="col-sm-6">
                        <select
                            v-model="selectedCategory"
                            class="form-select"
                            @change="resetFilters"
                        >
                            <option value="">-- Select a report --</option>
                            <option
                                v-for="cat in reportCategories"
                                :key="cat.key"
                                :value="cat.key"
                            >
                                {{ cat.label }}
                            </option>
                        </select>
                    </div>
                </div>
                <template v-if="selectedCategory">
                    <div class="row mb-3">
                        <label class="col-sm-3 col-form-label fw-bold"
                            >Format</label
                        >
                        <div class="col-sm-6">
                            <select v-model="format" class="form-select">
                                <option value="excel">Excel</option>
                                <option value="csv">CSV</option>
                            </select>
                        </div>
                    </div>
                    <div class="row mb-3">
                        <label class="col-sm-3 col-form-label fw-bold"
                            >Max. Records</label
                        >
                        <div class="col-sm-6">
                            <input
                                v-model.number="numRecords"
                                type="number"
                                min="1"
                                max="500000"
                                class="form-control"
                            />
                        </div>
                    </div>

                    <!-- ── Filters ─────────────────────────────────── -->
                    <hr class="my-3" />
                    <h6 class="mb-3 text-muted">
                        Filters
                        <small class="fw-normal">(optional)</small>
                    </h6>

                    <!-- Group Type -->
                    <div class="row mb-3">
                        <label class="col-sm-3 col-form-label fw-bold"
                            >Group Type</label
                        >
                        <div class="col-sm-6">
                            <select
                                v-model="selectedGroupType"
                                class="form-select"
                                @change="resetFilters"
                            >
                                <option
                                    v-for="gt in groupTypes"
                                    :key="gt.key"
                                    :value="gt.key"
                                >
                                    {{
                                        selectedCategory ===
                                            'conservation_status' &&
                                        gt.key === 'all'
                                            ? 'Species'
                                            : gt.label
                                    }}
                                </option>
                            </select>
                        </div>
                    </div>

                    <!-- Status -->
                    <div class="row mb-3">
                        <label class="col-sm-3 col-form-label fw-bold"
                            >Status</label
                        >
                        <div class="col-sm-6">
                            <select v-model="filterStatus" class="form-select">
                                <option value="all">All</option>
                                <option
                                    v-for="s in currentProcessingStatuses"
                                    :key="s.value"
                                    :value="s.value"
                                >
                                    {{ s.name }}
                                </option>
                            </select>
                        </div>
                    </div>

                    <!-- Scientific Name (flora/fauna only) -->
                    <div v-if="showScientificNameFilter" class="row mb-3">
                        <label class="col-sm-3 col-form-label fw-bold"
                            >Scientific Name</label
                        >
                        <div class="col-sm-6">
                            <select
                                ref="scientific_name_lookup"
                                class="form-select"
                            ></select>
                        </div>
                    </div>

                    <!-- Common Name (flora/fauna, not community) -->
                    <div v-if="showCommonNameFilter" class="row mb-3">
                        <label class="col-sm-3 col-form-label fw-bold"
                            >Common Name</label
                        >
                        <div class="col-sm-6">
                            <select
                                ref="common_name_lookup"
                                class="form-select"
                            ></select>
                        </div>
                    </div>

                    <!-- Community Name (community / all-species) -->
                    <div v-if="showCommunityNameFilter" class="row mb-3">
                        <label class="col-sm-3 col-form-label fw-bold"
                            >Community Name</label
                        >
                        <div class="col-sm-6">
                            <select
                                ref="community_name_lookup"
                                class="form-select"
                            ></select>
                        </div>
                    </div>

                    <!-- Community Common ID (community group type) -->
                    <div v-if="showCommunityCommonIdFilter" class="row mb-3">
                        <label class="col-sm-3 col-form-label fw-bold"
                            >Community ID</label
                        >
                        <div class="col-sm-6">
                            <select
                                ref="community_id_lookup"
                                class="form-select"
                            ></select>
                        </div>
                    </div>

                    <!-- Occurrence Name (OCC and OCR) -->
                    <div
                        v-if="
                            selectedCategory === 'occurrence' ||
                            selectedCategory === 'occurrence_report'
                        "
                        class="row mb-3"
                    >
                        <label class="col-sm-3 col-form-label fw-bold"
                            >Occurrence Name</label
                        >
                        <div class="col-sm-6">
                            <input
                                v-model="filterOccurrenceName"
                                type="text"
                                class="form-control"
                                placeholder="Contains…"
                            />
                        </div>
                    </div>

                    <!-- Linked Occurrence number (OCR only) -->
                    <div
                        v-if="selectedCategory === 'occurrence_report'"
                        class="row mb-3"
                    >
                        <label class="col-sm-3 col-form-label fw-bold"
                            >Occurrence Number</label
                        >
                        <div class="col-sm-6">
                            <input
                                v-model="filterOccurrence"
                                type="text"
                                class="form-control"
                                placeholder="e.g. OCC00001"
                            />
                        </div>
                    </div>

                    <!-- WA Legislative List (species / CS) -->
                    <div
                        v-if="
                            selectedCategory === 'species' ||
                            selectedCategory === 'conservation_status'
                        "
                        class="row mb-3"
                    >
                        <label class="col-sm-3 col-form-label fw-bold"
                            >WA Legislative List</label
                        >
                        <div class="col-sm-6">
                            <select
                                v-model="filterWaLegislativeList"
                                class="form-select"
                            >
                                <option value="all">All</option>
                                <option
                                    v-for="item in waLegislativeLists"
                                    :key="item.id"
                                    :value="item.id"
                                >
                                    {{ item.code }} – {{ item.label }}
                                </option>
                            </select>
                        </div>
                    </div>

                    <!-- WA Legislative Category (species / CS) -->
                    <div
                        v-if="
                            selectedCategory === 'species' ||
                            selectedCategory === 'conservation_status'
                        "
                        class="row mb-3"
                    >
                        <label class="col-sm-3 col-form-label fw-bold"
                            >WA Legislative Category</label
                        >
                        <div class="col-sm-6">
                            <select
                                v-model="filterWaLegislativeCategory"
                                class="form-select"
                            >
                                <option value="all">All</option>
                                <option
                                    v-for="item in waLegislativeCategories"
                                    :key="item.id"
                                    :value="item.id"
                                >
                                    {{ item.code }} – {{ item.label }}
                                </option>
                            </select>
                        </div>
                    </div>

                    <!-- WA Priority Category (species / CS) -->
                    <div
                        v-if="
                            selectedCategory === 'species' ||
                            selectedCategory === 'conservation_status'
                        "
                        class="row mb-3"
                    >
                        <label class="col-sm-3 col-form-label fw-bold"
                            >WA Priority Category</label
                        >
                        <div class="col-sm-6">
                            <SelectFilter
                                id="wa-priority-category-filter"
                                title=""
                                :show-title="false"
                                :options="waPriorityCategoryOptions"
                                :multiple="true"
                                :pre-selected-filter-item="
                                    filterWaPriorityCategory
                                "
                                placeholder="Select WA Priority Categories"
                                @input="
                                    (val) => {
                                        filterWaPriorityCategory = val || [];
                                    }
                                "
                            />
                        </div>
                    </div>

                    <!-- Name Status (species only, not community) -->
                    <div
                        v-if="
                            selectedCategory === 'species' &&
                            selectedGroupType !== 'community'
                        "
                        class="row mb-3"
                    >
                        <label class="col-sm-3 col-form-label fw-bold"
                            >Name Status</label
                        >
                        <div class="col-sm-6">
                            <select
                                v-model="filterNameStatus"
                                class="form-select"
                            >
                                <option value="all">All</option>
                                <option value="true">Current</option>
                                <option value="false">Non-Current</option>
                            </select>
                        </div>
                    </div>

                    <!-- Publication Status (species / community) -->
                    <div v-if="selectedCategory === 'species'" class="row mb-3">
                        <label class="col-sm-3 col-form-label fw-bold"
                            >Publication Status</label
                        >
                        <div class="col-sm-6">
                            <select
                                v-model="filterPublicationStatus"
                                class="form-select"
                            >
                                <option value="all">All</option>
                                <option value="true">Public</option>
                                <option value="false">Private</option>
                            </select>
                        </div>
                    </div>

                    <!-- Commonwealth Relevance (species / CS) -->
                    <div
                        v-if="
                            selectedCategory === 'species' ||
                            selectedCategory === 'conservation_status'
                        "
                        class="row mb-3"
                    >
                        <label class="col-sm-3 col-form-label fw-bold"
                            >Commonwealth Relevance</label
                        >
                        <div class="col-sm-6 d-flex align-items-center">
                            <div class="form-check mb-0">
                                <input
                                    id="filterCommonwealthRelevance"
                                    v-model="filterCommonwealthRelevance"
                                    class="form-check-input"
                                    type="checkbox"
                                />
                                <label
                                    class="form-check-label"
                                    for="filterCommonwealthRelevance"
                                    >Commonwealth listed only</label
                                >
                            </div>
                        </div>
                    </div>

                    <!-- International Relevance (species / CS) -->
                    <div
                        v-if="
                            selectedCategory === 'species' ||
                            selectedCategory === 'conservation_status'
                        "
                        class="row mb-3"
                    >
                        <label class="col-sm-3 col-form-label fw-bold"
                            >International Relevance</label
                        >
                        <div class="col-sm-6 d-flex align-items-center">
                            <div class="form-check mb-0">
                                <input
                                    id="filterInternationalRelevance"
                                    v-model="filterInternationalRelevance"
                                    class="form-check-input"
                                    type="checkbox"
                                />
                                <label
                                    class="form-check-label"
                                    for="filterInternationalRelevance"
                                    >Internationally assessed only</label
                                >
                            </div>
                        </div>
                    </div>

                    <!-- Conservation Criteria (species / CS) -->
                    <div
                        v-if="
                            selectedCategory === 'species' ||
                            selectedCategory === 'conservation_status'
                        "
                        class="row mb-3"
                    >
                        <label class="col-sm-3 col-form-label fw-bold"
                            >Conservation Criteria</label
                        >
                        <div class="col-sm-6">
                            <input
                                v-model="filterConservationCriteria"
                                type="text"
                                class="form-control"
                                placeholder="Contains…"
                            />
                        </div>
                    </div>

                    <!-- Family (species / CS, not community) -->
                    <div v-if="showFamilyGenusFilter" class="row mb-3">
                        <label class="col-sm-3 col-form-label fw-bold"
                            >Family</label
                        >
                        <div class="col-sm-6">
                            <select
                                ref="family_lookup"
                                class="form-select"
                            ></select>
                        </div>
                    </div>

                    <!-- Genus (species / CS, not community) -->
                    <div v-if="showFamilyGenusFilter" class="row mb-3">
                        <label class="col-sm-3 col-form-label fw-bold"
                            >Genus</label
                        >
                        <div class="col-sm-6">
                            <select
                                ref="genera_lookup"
                                class="form-select"
                            ></select>
                        </div>
                    </div>

                    <!-- Informal Group (species / CS, flora or all) -->
                    <div v-if="showInformalGroupFilter" class="row mb-3">
                        <label class="col-sm-3 col-form-label fw-bold"
                            >Informal Group</label
                        >
                        <div class="col-sm-6">
                            <select
                                v-model="filterInformalGroup"
                                class="form-select"
                            >
                                <option value="all">All</option>
                                <option
                                    v-for="ig in informalGroups"
                                    :key="ig.id"
                                    :value="ig.id"
                                >
                                    {{ ig.class_desc }}
                                </option>
                            </select>
                        </div>
                    </div>

                    <!-- Fauna Group (fauna species / fauna CS) -->
                    <div v-if="showFaunaGroupFilter" class="row mb-3">
                        <label class="col-sm-3 col-form-label fw-bold"
                            >Fauna Group</label
                        >
                        <div class="col-sm-6">
                            <select
                                v-model="filterFaunaGroup"
                                class="form-select"
                            >
                                <option value="all">All</option>
                                <option
                                    v-for="fg in faunaGroups"
                                    :key="fg.id"
                                    :value="fg.id"
                                >
                                    {{ fg.name }}
                                </option>
                            </select>
                        </div>
                    </div>

                    <!-- Fauna Sub-Group (fauna species only) -->
                    <div v-if="showFaunaSubGroupFilter" class="row mb-3">
                        <label class="col-sm-3 col-form-label fw-bold"
                            >Fauna Sub-Group</label
                        >
                        <div class="col-sm-6">
                            <select
                                v-model="filterFaunaSubGroup"
                                class="form-select"
                            >
                                <option value="all">All</option>
                                <option
                                    v-for="sg in filteredFaunaSubGroups"
                                    :key="sg.id"
                                    :value="sg.id"
                                >
                                    {{ sg.name }}
                                </option>
                            </select>
                        </div>
                    </div>

                    <!-- Change Type (CS only) -->
                    <div
                        v-if="selectedCategory === 'conservation_status'"
                        class="row mb-3"
                    >
                        <label class="col-sm-3 col-form-label fw-bold"
                            >Change Type</label
                        >
                        <div class="col-sm-6">
                            <select
                                v-model="filterChangeCode"
                                class="form-select"
                            >
                                <option value="all">All</option>
                                <option
                                    v-for="cc in changeCodes"
                                    :key="cc.id"
                                    :value="cc.id"
                                >
                                    {{ cc.code
                                    }}<template v-if="cc.label">
                                        – {{ cc.label }}</template
                                    >
                                </option>
                            </select>
                        </div>
                    </div>

                    <!-- Submitter Category (CS only) -->
                    <div
                        v-if="selectedCategory === 'conservation_status'"
                        class="row mb-3"
                    >
                        <label class="col-sm-3 col-form-label fw-bold"
                            >Submitter Category</label
                        >
                        <div class="col-sm-6">
                            <select
                                v-model="filterSubmitterCategory"
                                class="form-select"
                            >
                                <option value="all">All</option>
                                <option
                                    v-for="sc in submitterCategories"
                                    :key="sc.id"
                                    :value="sc.id"
                                >
                                    {{ sc.name }}
                                </option>
                            </select>
                        </div>
                    </div>

                    <!-- Locked (CS only) -->
                    <div
                        v-if="selectedCategory === 'conservation_status'"
                        class="row mb-3"
                    >
                        <label class="col-sm-3 col-form-label fw-bold"
                            >Locked</label
                        >
                        <div class="col-sm-6">
                            <select v-model="filterLocked" class="form-select">
                                <option value="all">All</option>
                                <option value="true">Yes</option>
                                <option value="false">No</option>
                            </select>
                        </div>
                    </div>

                    <!-- Assessor (CS / OCR) -->
                    <div v-if="showAssessorSubmitterFilter" class="row mb-3">
                        <label class="col-sm-3 col-form-label fw-bold"
                            >Assessor</label
                        >
                        <div class="col-sm-6">
                            <select
                                ref="assessor_lookup"
                                class="form-select"
                            ></select>
                        </div>
                    </div>

                    <!-- Submitter (CS / OCR) -->
                    <div v-if="showAssessorSubmitterFilter" class="row mb-3">
                        <label class="col-sm-3 col-form-label fw-bold"
                            >Submitter</label
                        >
                        <div class="col-sm-6">
                            <select
                                ref="submitter_lookup"
                                class="form-select"
                            ></select>
                        </div>
                    </div>

                    <!-- Last Modified By (OCC / OCR) -->
                    <div v-if="showLastModifiedByFilter" class="row mb-3">
                        <label class="col-sm-3 col-form-label fw-bold"
                            >Last Modified By</label
                        >
                        <div class="col-sm-6">
                            <select
                                ref="last_modified_by_lookup"
                                class="form-select"
                            ></select>
                        </div>
                    </div>

                    <!-- Region (species / OCC / OCR) -->
                    <div
                        v-if="
                            selectedCategory === 'species' ||
                            selectedCategory === 'occurrence' ||
                            selectedCategory === 'occurrence_report'
                        "
                        class="row mb-3"
                    >
                        <label class="col-sm-3 col-form-label fw-bold"
                            >Region(s)</label
                        >
                        <div class="col-sm-6">
                            <SelectFilter
                                id="region-filter"
                                title=""
                                :show-title="false"
                                :options="regionList"
                                :multiple="true"
                                :pre-selected-filter-item="filterRegions"
                                placeholder="Select Regions"
                                @input="onRegionFilterChange"
                            />
                        </div>
                    </div>

                    <!-- District (species / OCC / OCR) -->
                    <div
                        v-if="
                            selectedCategory === 'species' ||
                            selectedCategory === 'occurrence' ||
                            selectedCategory === 'occurrence_report'
                        "
                        class="row mb-3"
                    >
                        <label class="col-sm-3 col-form-label fw-bold"
                            >District(s)</label
                        >
                        <div class="col-sm-6">
                            <SelectFilter
                                id="district-filter"
                                title=""
                                :show-title="false"
                                :options="filteredDistricts"
                                :multiple="true"
                                :pre-selected-filter-item="filterDistricts"
                                placeholder="Select Districts"
                                @input="
                                    (val) => {
                                        filterDistricts = val || [];
                                    }
                                "
                            />
                        </div>
                    </div>

                    <!-- CS date filters -->
                    <template v-if="selectedCategory === 'conservation_status'">
                        <div class="row mb-3">
                            <label class="col-sm-3 col-form-label fw-bold"
                                >Effective From Date Range</label
                            >
                            <div class="col-sm-9">
                                <div class="input-group">
                                    <span class="input-group-text">From</span>
                                    <input
                                        v-model="filterFromEffectiveFromDate"
                                        type="date"
                                        class="form-control"
                                    />
                                    <span class="input-group-text">to</span>
                                    <input
                                        v-model="filterToEffectiveFromDate"
                                        type="date"
                                        class="form-control"
                                    />
                                </div>
                            </div>
                        </div>
                        <div class="row mb-3">
                            <label class="col-sm-3 col-form-label fw-bold"
                                >Effective To Date Range</label
                            >
                            <div class="col-sm-9">
                                <div class="input-group">
                                    <span class="input-group-text">From</span>
                                    <input
                                        v-model="filterFromEffectiveToDate"
                                        type="date"
                                        class="form-control"
                                    />
                                    <span class="input-group-text">to</span>
                                    <input
                                        v-model="filterToEffectiveToDate"
                                        type="date"
                                        class="form-control"
                                    />
                                </div>
                            </div>
                        </div>
                        <div class="row mb-3">
                            <label class="col-sm-3 col-form-label fw-bold"
                                >Review Due Date Range</label
                            >
                            <div class="col-sm-9">
                                <div class="input-group">
                                    <span class="input-group-text">From</span>
                                    <input
                                        v-model="filterFromReviewDueDate"
                                        type="date"
                                        class="form-control"
                                    />
                                    <span class="input-group-text">to</span>
                                    <input
                                        v-model="filterToReviewDueDate"
                                        type="date"
                                        class="form-control"
                                    />
                                </div>
                            </div>
                        </div>
                    </template>

                    <!-- Occurrence date filters -->
                    <template v-if="selectedCategory === 'occurrence'">
                        <div class="row mb-3">
                            <label class="col-sm-3 col-form-label fw-bold"
                                >Due Date Range</label
                            >
                            <div class="col-sm-9">
                                <div class="input-group">
                                    <span class="input-group-text">From</span>
                                    <input
                                        v-model="filterFromDueDate"
                                        type="date"
                                        class="form-control"
                                    />
                                    <span class="input-group-text">to</span>
                                    <input
                                        v-model="filterToDueDate"
                                        type="date"
                                        class="form-control"
                                    />
                                </div>
                            </div>
                        </div>
                        <div class="row mb-3">
                            <label class="col-sm-3 col-form-label fw-bold"
                                >Created Date Range</label
                            >
                            <div class="col-sm-9">
                                <div class="input-group">
                                    <span class="input-group-text">From</span>
                                    <input
                                        v-model="filterCreatedFromDate"
                                        type="date"
                                        class="form-control"
                                    />
                                    <span class="input-group-text">to</span>
                                    <input
                                        v-model="filterCreatedToDate"
                                        type="date"
                                        class="form-control"
                                    />
                                </div>
                            </div>
                        </div>
                        <div class="row mb-3">
                            <label class="col-sm-3 col-form-label fw-bold"
                                >Activated Date Range</label
                            >
                            <div class="col-sm-9">
                                <div class="input-group">
                                    <span class="input-group-text">From</span>
                                    <input
                                        v-model="filterActivatedFromDate"
                                        type="date"
                                        class="form-control"
                                    />
                                    <span class="input-group-text">to</span>
                                    <input
                                        v-model="filterActivatedToDate"
                                        type="date"
                                        class="form-control"
                                    />
                                </div>
                            </div>
                        </div>
                        <div class="row mb-3">
                            <label class="col-sm-3 col-form-label fw-bold"
                                >Last Modified Date Range</label
                            >
                            <div class="col-sm-9">
                                <div class="input-group">
                                    <span class="input-group-text">From</span>
                                    <input
                                        v-model="filterLastModifiedFromDate"
                                        type="date"
                                        class="form-control"
                                    />
                                    <span class="input-group-text">to</span>
                                    <input
                                        v-model="filterLastModifiedToDate"
                                        type="date"
                                        class="form-control"
                                    />
                                </div>
                            </div>
                        </div>
                    </template>

                    <!-- Occurrence Report date filters -->
                    <template v-if="selectedCategory === 'occurrence_report'">
                        <div class="row mb-3">
                            <label class="col-sm-3 col-form-label fw-bold"
                                >Observation Date Range</label
                            >
                            <div class="col-sm-9">
                                <div class="input-group">
                                    <span class="input-group-text">From</span>
                                    <input
                                        v-model="filterObservationFromDate"
                                        type="date"
                                        class="form-control"
                                    />
                                    <span class="input-group-text">to</span>
                                    <input
                                        v-model="filterObservationToDate"
                                        type="date"
                                        class="form-control"
                                    />
                                </div>
                            </div>
                        </div>
                        <div class="row mb-3">
                            <label class="col-sm-3 col-form-label fw-bold"
                                >Submitted Date Range</label
                            >
                            <div class="col-sm-9">
                                <div class="input-group">
                                    <span class="input-group-text">From</span>
                                    <input
                                        v-model="filterSubmittedFromDate"
                                        type="date"
                                        class="form-control"
                                    />
                                    <span class="input-group-text">to</span>
                                    <input
                                        v-model="filterSubmittedToDate"
                                        type="date"
                                        class="form-control"
                                    />
                                </div>
                            </div>
                        </div>
                        <div class="row mb-3">
                            <label class="col-sm-3 col-form-label fw-bold"
                                >Approved Date Range</label
                            >
                            <div class="col-sm-9">
                                <div class="input-group">
                                    <span class="input-group-text">From</span>
                                    <input
                                        v-model="filterApprovedFromDate"
                                        type="date"
                                        class="form-control"
                                    />
                                    <span class="input-group-text">to</span>
                                    <input
                                        v-model="filterApprovedToDate"
                                        type="date"
                                        class="form-control"
                                    />
                                </div>
                            </div>
                        </div>
                        <div class="row mb-3">
                            <label class="col-sm-3 col-form-label fw-bold"
                                >Last Modified Date Range</label
                            >
                            <div class="col-sm-9">
                                <div class="input-group">
                                    <span class="input-group-text">From</span>
                                    <input
                                        v-model="filterLastModifiedFromDate"
                                        type="date"
                                        class="form-control"
                                    />
                                    <span class="input-group-text">to</span>
                                    <input
                                        v-model="filterLastModifiedToDate"
                                        type="date"
                                        class="form-control"
                                    />
                                </div>
                            </div>
                        </div>
                    </template>

                    <!-- Clear filters -->
                    <div v-if="hasActiveFilters" class="row mb-3">
                        <div class="col-sm-6 offset-sm-3">
                            <button
                                class="btn btn-sm btn-outline-secondary"
                                @click="resetFilters"
                            >
                                <i class="bi bi-x-circle me-1"></i>Clear Filters
                            </button>
                        </div>
                    </div>

                    <hr class="my-3" />
                    <div class="row">
                        <div class="col-sm-6 offset-sm-3">
                            <button
                                class="btn btn-primary"
                                :disabled="submitting"
                                @click="queueReport"
                            >
                                <i class="bi bi-envelope me-1"></i>
                                {{
                                    submitting
                                        ? 'Queuing...'
                                        : 'Generate Report'
                                }}
                            </button>
                        </div>
                    </div>
                </template>
            </div>
        </div>

        <div v-if="queueHistory && queueHistory.length > 0" class="card mt-4">
            <div class="card-body">
                <h5 class="card-title mb-3">
                    Queue Items <i class="bi bi-clock-history fs-5"></i>
                </h5>
                <p v-if="historyLimit" class="text-muted small mb-3">
                    Showing your {{ historyLimit }} most recent requests.
                </p>
                <table class="table table-sm table-hover mb-0">
                    <thead>
                        <tr>
                            <th>Report Type</th>
                            <th>Format</th>
                            <th>Status</th>
                            <th>Queued</th>
                            <th>Processed</th>
                        </tr>
                    </thead>
                    <tbody>
                        <tr
                            v-for="job in queueHistory"
                            :key="job.id"
                            :class="rowClass(job)"
                        >
                            <td>{{ job.report_type }}</td>
                            <td>
                                <i
                                    :class="
                                        job.format === 'excel'
                                            ? 'bi bi-file-earmark-excel text-success'
                                            : 'bi bi-filetype-csv text-primary'
                                    "
                                ></i>
                                {{ job.format.toUpperCase() }}
                            </td>
                            <td>
                                <span v-if="job.status_id === 0">
                                    <i
                                        class="bi bi-clock text-secondary me-1"
                                    ></i>
                                    {{ job.status }}
                                </span>
                                <span v-else-if="job.status_id === 1">
                                    <span
                                        class="spinner-border spinner-border-sm text-primary me-1"
                                        role="status"
                                    ></span>
                                    {{ job.status }}
                                </span>
                                <span v-else-if="job.status_id === 2">
                                    <i
                                        class="bi bi-check-circle-fill text-success me-1"
                                    ></i>
                                    {{ job.status }}
                                </span>
                                <span v-else-if="job.status_id === 3">
                                    <i
                                        class="bi bi-x-circle-fill text-danger me-1"
                                    ></i>
                                    {{ job.status }}
                                    <i
                                        v-if="job.error_message"
                                        class="bi bi-info-circle text-danger ms-1 error-popover"
                                        role="button"
                                        tabindex="0"
                                        data-bs-toggle="popover"
                                        data-bs-trigger="hover focus"
                                        data-bs-placement="right"
                                        title="Error Details"
                                        :data-bs-content="job.error_message"
                                    ></i>
                                </span>
                            </td>
                            <td>{{ formatDate(job.created) }}</td>
                            <td>
                                {{
                                    job.processed_dt
                                        ? formatDate(job.processed_dt)
                                        : '-'
                                }}
                            </td>
                        </tr>
                    </tbody>
                </table>
            </div>
        </div>
    </div>
</template>

<script>
import swal from 'sweetalert2';
import SelectFilter from '@/components/common/SelectFilter.vue';
import { api_endpoints } from '@/utils/hooks';

export default {
    name: 'QueueReports',
    components: { SelectFilter },
    data() {
        return {
            reportCategories: [],
            groupTypes: [],
            groupTypeDbIds: {},
            processingStatusesByCategory: {},
            regionList: [],
            districtList: [],
            waLegislativeLists: [],
            waLegislativeCategories: [],
            waPriorityCategories: [],
            faunaGroups: [],
            faunaSubGroups: [],
            changeCodes: [],
            submitterCategories: [],
            informalGroups: [],
            selectedCategory: '',
            selectedGroupType: 'all',
            format: 'csv',
            numRecords: 100000,
            submitting: false,
            errorMessage: '',
            queueHistory: null,
            historyLimit: null,
            pollTimer: null,
            // filter state
            filterStatus: 'all',
            filterScientificName: '',
            filterCommunityName: '',
            filterOccurrenceName: '',
            filterOccurrence: '',
            filterRegions: [],
            filterDistricts: [],
            filterWaLegislativeList: 'all',
            filterWaLegislativeCategory: 'all',
            filterWaPriorityCategory: [],
            filterObservationFromDate: '',
            filterObservationToDate: '',
            filterSubmittedFromDate: '',
            filterSubmittedToDate: '',
            filterApprovedFromDate: '',
            filterApprovedToDate: '',
            filterLastModifiedFromDate: '',
            filterLastModifiedToDate: '',
            filterFromDueDate: '',
            filterToDueDate: '',
            filterCreatedFromDate: '',
            filterCreatedToDate: '',
            filterActivatedFromDate: '',
            filterActivatedToDate: '',
            filterFromEffectiveFromDate: '',
            filterToEffectiveFromDate: '',
            filterFromEffectiveToDate: '',
            filterToEffectiveToDate: '',
            filterFromReviewDueDate: '',
            filterToReviewDueDate: '',
            filterNameStatus: 'all',
            filterPublicationStatus: 'all',
            filterCommonwealthRelevance: false,
            filterInternationalRelevance: false,
            filterConservationCriteria: '',
            filterFaunaGroup: 'all',
            filterFaunaSubGroup: 'all',
            filterChangeCode: 'all',
            filterSubmitterCategory: 'all',
            filterLocked: 'all',
            filterCommonName: '',
            filterFamily: '',
            filterGenus: '',
            filterInformalGroup: 'all',
            filterCommunityCommonId: '',
            filterAssessor: '',
            filterSubmitter: '',
            filterLastModifiedBy: '',
        };
    },
    computed: {
        waPriorityCategoryOptions() {
            return this.waPriorityCategories.map((item) => ({
                id: item.id,
                name: item.code + (item.label ? ' \u2013 ' + item.label : ''),
            }));
        },
        currentGroupTypeDbId() {
            // Returns the numeric GroupType DB id for the currently selected
            // group type key, or '' for 'all' (no filtering).
            if (!this.selectedGroupType || this.selectedGroupType === 'all') {
                return '';
            }
            return this.groupTypeDbIds[this.selectedGroupType] || '';
        },
        currentProcessingStatuses() {
            return (
                this.processingStatusesByCategory[this.selectedCategory] || []
            );
        },
        filteredDistricts() {
            if (this.filterRegions.length > 0) {
                const regionIds = new Set(this.filterRegions.map(Number));
                return this.districtList.filter((d) =>
                    regionIds.has(d.region_id)
                );
            }
            return this.districtList;
        },
        filteredFaunaSubGroups() {
            if (this.filterFaunaGroup && this.filterFaunaGroup !== 'all') {
                return this.faunaSubGroups.filter(
                    (sg) =>
                        String(sg.fauna_group_id) ===
                        String(this.filterFaunaGroup)
                );
            }
            return this.faunaSubGroups;
        },
        showFaunaGroupFilter() {
            return (
                (this.selectedCategory === 'species' ||
                    this.selectedCategory === 'conservation_status') &&
                (this.selectedGroupType === 'fauna' ||
                    this.selectedGroupType === 'all')
            );
        },
        showFaunaSubGroupFilter() {
            return (
                this.selectedCategory === 'species' &&
                (this.selectedGroupType === 'fauna' ||
                    this.selectedGroupType === 'all')
            );
        },
        showScientificNameFilter() {
            return this.selectedGroupType !== 'community';
        },
        showCommunityNameFilter() {
            return (
                this.selectedGroupType === 'community' ||
                (this.selectedCategory === 'species' &&
                    this.selectedGroupType === 'all')
            );
        },
        showCommonNameFilter() {
            return this.selectedGroupType !== 'community';
        },
        showFamilyGenusFilter() {
            return (
                (this.selectedCategory === 'species' ||
                    this.selectedCategory === 'conservation_status') &&
                this.selectedGroupType !== 'community'
            );
        },
        showInformalGroupFilter() {
            return (
                (this.selectedCategory === 'species' ||
                    this.selectedCategory === 'conservation_status') &&
                this.selectedGroupType !== 'fauna' &&
                this.selectedGroupType !== 'community'
            );
        },
        showCommunityCommonIdFilter() {
            return this.selectedGroupType === 'community';
        },
        showAssessorSubmitterFilter() {
            return (
                this.selectedCategory === 'conservation_status' ||
                this.selectedCategory === 'occurrence_report'
            );
        },
        showLastModifiedByFilter() {
            return (
                this.selectedCategory === 'occurrence' ||
                this.selectedCategory === 'occurrence_report'
            );
        },
        hasActiveFilters() {
            return (
                this.filterStatus !== 'all' ||
                this.filterScientificName !== '' ||
                this.filterCommunityName !== '' ||
                this.filterOccurrenceName !== '' ||
                this.filterOccurrence !== '' ||
                this.filterRegions.length > 0 ||
                this.filterDistricts.length > 0 ||
                this.filterWaLegislativeList !== 'all' ||
                this.filterWaLegislativeCategory !== 'all' ||
                this.filterWaPriorityCategory.length > 0 ||
                this.filterObservationFromDate !== '' ||
                this.filterObservationToDate !== '' ||
                this.filterSubmittedFromDate !== '' ||
                this.filterSubmittedToDate !== '' ||
                this.filterApprovedFromDate !== '' ||
                this.filterApprovedToDate !== '' ||
                this.filterLastModifiedFromDate !== '' ||
                this.filterLastModifiedToDate !== '' ||
                this.filterFromDueDate !== '' ||
                this.filterToDueDate !== '' ||
                this.filterCreatedFromDate !== '' ||
                this.filterCreatedToDate !== '' ||
                this.filterActivatedFromDate !== '' ||
                this.filterActivatedToDate !== '' ||
                this.filterFromEffectiveFromDate !== '' ||
                this.filterToEffectiveFromDate !== '' ||
                this.filterFromEffectiveToDate !== '' ||
                this.filterToEffectiveToDate !== '' ||
                this.filterFromReviewDueDate !== '' ||
                this.filterToReviewDueDate !== '' ||
                this.filterNameStatus !== 'all' ||
                this.filterPublicationStatus !== 'all' ||
                this.filterCommonwealthRelevance ||
                this.filterInternationalRelevance ||
                this.filterConservationCriteria !== '' ||
                this.filterFaunaGroup !== 'all' ||
                this.filterFaunaSubGroup !== 'all' ||
                this.filterChangeCode !== 'all' ||
                this.filterSubmitterCategory !== 'all' ||
                this.filterLocked !== 'all' ||
                this.filterCommonName !== '' ||
                this.filterFamily !== '' ||
                this.filterGenus !== '' ||
                this.filterInformalGroup !== 'all' ||
                this.filterCommunityCommonId !== '' ||
                this.filterAssessor !== '' ||
                this.filterSubmitter !== '' ||
                this.filterLastModifiedBy !== ''
            );
        },
    },
    watch: {
        filterFaunaGroup() {
            this.filterFaunaSubGroup = 'all';
        },
        selectedCategory() {
            this.$nextTick(() => {
                this.initialiseAllLookups();
            });
        },
        selectedGroupType() {
            this.$nextTick(() => {
                this.initialiseAllLookups();
            });
        },
    },
    mounted() {
        this.fetchReportConfig();
        this.fetchQueueHistory();
        this.startPolling();
        document.addEventListener('visibilitychange', this.onVisibilityChange);
        this.$nextTick(() => {
            this.initialiseAllLookups();
        });
    },
    beforeUnmount() {
        clearInterval(this.pollTimer);
        document.removeEventListener(
            'visibilitychange',
            this.onVisibilityChange
        );
        this.disposePopovers();
    },
    updated() {
        this.initPopovers();
    },
    methods: {
        onRegionFilterChange(val) {
            this.filterRegions = val || [];
            // Clear districts that no longer belong to selected regions
            if (
                this.filterRegions.length > 0 &&
                this.filterDistricts.length > 0
            ) {
                const validDistrictIds = new Set(
                    this.filteredDistricts.map((d) => String(d.id))
                );
                this.filterDistricts = this.filterDistricts.filter((id) =>
                    validDistrictIds.has(String(id))
                );
            }
        },
        initialiseAllLookups() {
            this.initialiseScientificNameLookup();
            this.initialiseCommonNameLookup();
            this.initialiseCommunityNameLookup();
            this.initialiseCommunityIdLookup();
            this.initialiseFamilyLookup();
            this.initialiseGeneraLookup();
            this.initialiseAssessorLookup();
            this.initialiseSubmitterLookup();
            this.initialiseLastModifiedByLookup();
        },
        _initialiseLookup(refName, url, placeholder, extraData) {
            let vm = this;
            const el = vm.$refs[refName];
            if (!el) return;
            $(el)
                .select2({
                    minimumInputLength: 2,
                    theme: 'bootstrap-5',
                    allowClear: true,
                    placeholder: placeholder,
                    ajax: {
                        url: url,
                        dataType: 'json',
                        data: function (params) {
                            const q = { term: params.term };
                            if (extraData) Object.assign(q, extraData());
                            return q;
                        },
                    },
                })
                .on('select2:select', function (e) {
                    vm['filter' + vm._capitalize(refName)] = e.params.data.id;
                })
                .on('select2:unselect', function () {
                    vm['filter' + vm._capitalize(refName)] = '';
                })
                .on('select2:open', function () {
                    const searchField = $(
                        `[aria-controls="select2-${el.id || refName}-results"]`
                    );
                    if (searchField.length) searchField[0].focus();
                });
        },
        _capitalize(str) {
            // Convert ref names like 'scientific_name_lookup' to 'ScientificNameLookup'
            return str
                .split('_')
                .map((s) => s.charAt(0).toUpperCase() + s.slice(1))
                .join('');
        },
        initialiseScientificNameLookup() {
            let vm = this;
            const el = vm.$refs.scientific_name_lookup;
            if (!el) return;
            if ($(el).data('select2')) {
                $(el).select2('destroy').off('select2:select select2:unselect');
            }
            $(el)
                .select2({
                    minimumInputLength: 2,
                    language: {
                        inputTooShort: function () {
                            return 'Use % for wildcard search';
                        },
                    },
                    theme: 'bootstrap-5',
                    allowClear: true,
                    placeholder: 'Search for Scientific Name',
                    ajax: {
                        url: api_endpoints.scientific_name_lookup,
                        dataType: 'json',
                        data: function (params) {
                            return {
                                term: params.term,
                                group_type_id: vm.currentGroupTypeDbId,
                            };
                        },
                    },
                })
                .on('select2:select', function (e) {
                    vm.filterScientificName = e.params.data.text;
                })
                .on('select2:unselect', function () {
                    vm.filterScientificName = '';
                });
        },
        initialiseCommonNameLookup() {
            let vm = this;
            const el = vm.$refs.common_name_lookup;
            if (!el) return;
            if ($(el).data('select2')) {
                $(el).select2('destroy').off('select2:select select2:unselect');
            }
            $(el)
                .select2({
                    minimumInputLength: 2,
                    language: {
                        inputTooShort: function () {
                            return 'Use % for wildcard search';
                        },
                    },
                    theme: 'bootstrap-5',
                    allowClear: true,
                    placeholder: 'Search for Common Name',
                    ajax: {
                        url: api_endpoints.common_name_lookup,
                        dataType: 'json',
                        data: function (params) {
                            return {
                                term: params.term,
                                group_type_id: vm.currentGroupTypeDbId,
                            };
                        },
                    },
                })
                .on('select2:select', function (e) {
                    vm.filterCommonName = e.params.data.text;
                })
                .on('select2:unselect', function () {
                    vm.filterCommonName = '';
                });
        },
        initialiseCommunityNameLookup() {
            let vm = this;
            const el = vm.$refs.community_name_lookup;
            if (!el) return;
            if ($(el).data('select2')) {
                $(el).select2('destroy').off('select2:select select2:unselect');
            }
            $(el)
                .select2({
                    minimumInputLength: 2,
                    language: {
                        inputTooShort: function () {
                            return 'Use % for wildcard search';
                        },
                    },
                    theme: 'bootstrap-5',
                    allowClear: true,
                    placeholder: 'Search for Community Name',
                    ajax: {
                        url: api_endpoints.community_name_lookup,
                        dataType: 'json',
                        data: function (params) {
                            return { term: params.term };
                        },
                    },
                })
                .on('select2:select', function (e) {
                    vm.filterCommunityName = e.params.data.text;
                })
                .on('select2:unselect', function () {
                    vm.filterCommunityName = '';
                });
        },
        initialiseCommunityIdLookup() {
            let vm = this;
            const el = vm.$refs.community_id_lookup;
            if (!el) return;
            if ($(el).data('select2')) {
                $(el).select2('destroy').off('select2:select select2:unselect');
            }
            $(el)
                .select2({
                    minimumInputLength: 2,
                    theme: 'bootstrap-5',
                    allowClear: true,
                    placeholder: 'Search for Community ID',
                    ajax: {
                        url: api_endpoints.community_id_lookup,
                        dataType: 'json',
                        data: function (params) {
                            return { term: params.term };
                        },
                    },
                })
                .on('select2:select', function (e) {
                    vm.filterCommunityCommonId = e.params.data.text;
                })
                .on('select2:unselect', function () {
                    vm.filterCommunityCommonId = '';
                });
        },
        initialiseFamilyLookup() {
            let vm = this;
            const el = vm.$refs.family_lookup;
            if (!el) return;
            if ($(el).data('select2')) {
                $(el).select2('destroy').off('select2:select select2:unselect');
            }
            $(el)
                .select2({
                    minimumInputLength: 2,
                    language: {
                        inputTooShort: function () {
                            return 'Use % for wildcard search';
                        },
                    },
                    theme: 'bootstrap-5',
                    allowClear: true,
                    placeholder: 'Search for Family',
                    ajax: {
                        url: api_endpoints.family_lookup,
                        dataType: 'json',
                        data: function (params) {
                            return {
                                term: params.term,
                                group_type_id: vm.currentGroupTypeDbId,
                            };
                        },
                    },
                })
                .on('select2:select', function (e) {
                    vm.filterFamily = e.params.data.text;
                })
                .on('select2:unselect', function () {
                    vm.filterFamily = '';
                });
        },
        initialiseGeneraLookup() {
            let vm = this;
            const el = vm.$refs.genera_lookup;
            if (!el) return;
            if ($(el).data('select2')) {
                $(el).select2('destroy').off('select2:select select2:unselect');
            }
            $(el)
                .select2({
                    minimumInputLength: 2,
                    language: {
                        inputTooShort: function () {
                            return 'Use % for wildcard search';
                        },
                    },
                    theme: 'bootstrap-5',
                    allowClear: true,
                    placeholder: 'Search for Genus',
                    ajax: {
                        url: api_endpoints.genera_lookup,
                        dataType: 'json',
                        data: function (params) {
                            return {
                                term: params.term,
                                group_type_id: vm.currentGroupTypeDbId,
                            };
                        },
                    },
                })
                .on('select2:select', function (e) {
                    vm.filterGenus = e.params.data.text;
                })
                .on('select2:unselect', function () {
                    vm.filterGenus = '';
                });
        },
        initialiseAssessorLookup() {
            let vm = this;
            const el = vm.$refs.assessor_lookup;
            if (!el) return;
            if ($(el).data('select2')) {
                $(el).select2('destroy').off('select2:select select2:unselect');
            }
            $(el)
                .select2({
                    minimumInputLength: 2,
                    theme: 'bootstrap-5',
                    allowClear: true,
                    placeholder: 'Search for Assessor',
                    ajax: {
                        url:
                            api_endpoints.users_api +
                            '/get_department_users_ledger_id/',
                        dataType: 'json',
                        data: function (params) {
                            return { term: params.term };
                        },
                    },
                })
                .on('select2:select', function (e) {
                    vm.filterAssessor = e.params.data.id;
                })
                .on('select2:unselect', function () {
                    vm.filterAssessor = '';
                });
        },
        initialiseSubmitterLookup() {
            let vm = this;
            const el = vm.$refs.submitter_lookup;
            if (!el) return;
            if ($(el).data('select2')) {
                $(el).select2('destroy').off('select2:select select2:unselect');
            }
            $(el)
                .select2({
                    minimumInputLength: 2,
                    theme: 'bootstrap-5',
                    allowClear: true,
                    placeholder: 'Search for Submitter',
                    ajax: {
                        url: api_endpoints.users_api + '/get_users_ledger_id/',
                        dataType: 'json',
                        data: function (params) {
                            return { term: params.term };
                        },
                    },
                })
                .on('select2:select', function (e) {
                    vm.filterSubmitter = e.params.data.id;
                })
                .on('select2:unselect', function () {
                    vm.filterSubmitter = '';
                });
        },
        initialiseLastModifiedByLookup() {
            let vm = this;
            const el = vm.$refs.last_modified_by_lookup;
            if (!el) return;
            if ($(el).data('select2')) {
                $(el).select2('destroy').off('select2:select select2:unselect');
            }
            $(el)
                .select2({
                    minimumInputLength: 2,
                    theme: 'bootstrap-5',
                    allowClear: true,
                    placeholder: 'Search for Last Modified By',
                    ajax: {
                        url: api_endpoints.users_api + '/get_users_ledger_id/',
                        dataType: 'json',
                        data: function (params) {
                            return { term: params.term };
                        },
                    },
                })
                .on('select2:select', function (e) {
                    vm.filterLastModifiedBy = e.params.data.id;
                })
                .on('select2:unselect', function () {
                    vm.filterLastModifiedBy = '';
                });
        },
        _clearSelect2(refName) {
            const el = this.$refs[refName];
            if (!el) return;
            try {
                $(el).val(null).trigger('change');
            } catch (e) {
                // ignore if not initialised
            }
        },
        startPolling() {
            this.pollTimer = setInterval(() => {
                this.fetchQueueHistory();
            }, 5000);
        },
        onVisibilityChange() {
            if (document.hidden) {
                clearInterval(this.pollTimer);
            } else {
                this.fetchQueueHistory();
                this.startPolling();
            }
        },
        fetchReportConfig() {
            fetch('/api/queue_report/')
                .then((response) => response.json())
                .then((data) => {
                    this.reportCategories = data.report_categories || [];
                    this.groupTypes = data.group_types || [];
                    this.processingStatusesByCategory =
                        data.processing_statuses_by_category || {};
                    this.regionList = data.region_list || [];
                    this.districtList = data.district_list || [];
                    this.waLegislativeLists = data.wa_legislative_lists || [];
                    this.waLegislativeCategories =
                        data.wa_legislative_categories || [];
                    this.waPriorityCategories =
                        data.wa_priority_categories || [];
                    this.faunaGroups = data.fauna_groups || [];
                    this.faunaSubGroups = data.fauna_sub_groups || [];
                    this.changeCodes = data.change_codes || [];
                    this.submitterCategories = data.submitter_categories || [];
                    this.informalGroups = data.informal_groups || [];
                    this.groupTypeDbIds = data.group_type_db_ids || {};
                })
                .catch(() => {
                    this.errorMessage = 'Failed to load report types.';
                });
        },
        resetFilters() {
            this.filterStatus = 'all';
            this.filterScientificName = '';
            this.filterCommunityName = '';
            this.filterOccurrenceName = '';
            this.filterOccurrence = '';
            this.filterRegions = [];
            this.filterDistricts = [];
            this.filterWaLegislativeList = 'all';
            this.filterWaLegislativeCategory = 'all';
            this.filterWaPriorityCategory = [];
            this.filterObservationFromDate = '';
            this.filterObservationToDate = '';
            this.filterSubmittedFromDate = '';
            this.filterSubmittedToDate = '';
            this.filterApprovedFromDate = '';
            this.filterApprovedToDate = '';
            this.filterLastModifiedFromDate = '';
            this.filterLastModifiedToDate = '';
            this.filterFromDueDate = '';
            this.filterToDueDate = '';
            this.filterCreatedFromDate = '';
            this.filterCreatedToDate = '';
            this.filterActivatedFromDate = '';
            this.filterActivatedToDate = '';
            this.filterFromEffectiveFromDate = '';
            this.filterToEffectiveFromDate = '';
            this.filterFromEffectiveToDate = '';
            this.filterToEffectiveToDate = '';
            this.filterFromReviewDueDate = '';
            this.filterToReviewDueDate = '';
            this.filterNameStatus = 'all';
            this.filterPublicationStatus = 'all';
            this.filterCommonwealthRelevance = false;
            this.filterInternationalRelevance = false;
            this.filterConservationCriteria = '';
            this.filterFaunaGroup = 'all';
            this.filterFaunaSubGroup = 'all';
            this.filterChangeCode = 'all';
            this.filterSubmitterCategory = 'all';
            this.filterLocked = 'all';
            this.filterCommonName = '';
            this.filterFamily = '';
            this.filterGenus = '';
            this.filterInformalGroup = 'all';
            this.filterCommunityCommonId = '';
            this.filterAssessor = '';
            this.filterSubmitter = '';
            this.filterLastModifiedBy = '';
            // Clear Select2 controls
            this.$nextTick(() => {
                [
                    'scientific_name_lookup',
                    'common_name_lookup',
                    'community_name_lookup',
                    'community_id_lookup',
                    'family_lookup',
                    'genera_lookup',
                    'assessor_lookup',
                    'submitter_lookup',
                    'last_modified_by_lookup',
                ].forEach((ref) => this._clearSelect2(ref));
            });
        },
        buildFiltersPayload() {
            const f = {};
            if (this.filterStatus && this.filterStatus !== 'all') {
                f.filter_status = this.filterStatus;
            }
            if (this.filterScientificName) {
                f.filter_scientific_name = this.filterScientificName;
            }
            if (this.filterCommunityName) {
                f.filter_community_name = this.filterCommunityName;
            }
            if (this.filterOccurrenceName) {
                f.filter_occurrence_name = this.filterOccurrenceName;
            }
            if (this.filterOccurrence) {
                f.filter_occurrence = this.filterOccurrence;
            }
            if (this.filterRegions.length > 0) {
                f.filter_region = this.filterRegions.join(',');
            }
            if (this.filterDistricts.length > 0) {
                f.filter_district = this.filterDistricts.join(',');
            }
            if (
                this.filterWaLegislativeList &&
                this.filterWaLegislativeList !== 'all'
            ) {
                f.filter_wa_legislative_list = this.filterWaLegislativeList;
            }
            if (
                this.filterWaLegislativeCategory &&
                this.filterWaLegislativeCategory !== 'all'
            ) {
                f.filter_wa_legislative_category =
                    this.filterWaLegislativeCategory;
            }
            if (this.filterWaPriorityCategory.length > 0) {
                f.filter_wa_priority_category =
                    this.filterWaPriorityCategory.join(',');
            }
            if (this.filterObservationFromDate) {
                f.filter_observation_from_date = this.filterObservationFromDate;
            }
            if (this.filterObservationToDate) {
                f.filter_observation_to_date = this.filterObservationToDate;
            }
            if (this.filterSubmittedFromDate) {
                f.filter_submitted_from_date = this.filterSubmittedFromDate;
            }
            if (this.filterSubmittedToDate) {
                f.filter_submitted_to_date = this.filterSubmittedToDate;
            }
            if (this.filterApprovedFromDate) {
                f.filter_approved_from_date = this.filterApprovedFromDate;
            }
            if (this.filterApprovedToDate) {
                f.filter_approved_to_date = this.filterApprovedToDate;
            }
            if (this.filterLastModifiedFromDate) {
                f.filter_last_modified_from_date =
                    this.filterLastModifiedFromDate;
            }
            if (this.filterLastModifiedToDate) {
                f.filter_last_modified_to_date = this.filterLastModifiedToDate;
            }
            if (this.filterFromDueDate) {
                f.filter_from_due_date = this.filterFromDueDate;
            }
            if (this.filterToDueDate) {
                f.filter_to_due_date = this.filterToDueDate;
            }
            if (this.filterCreatedFromDate) {
                f.filter_created_from_date = this.filterCreatedFromDate;
            }
            if (this.filterCreatedToDate) {
                f.filter_created_to_date = this.filterCreatedToDate;
            }
            if (this.filterActivatedFromDate) {
                f.filter_activated_from_date = this.filterActivatedFromDate;
            }
            if (this.filterActivatedToDate) {
                f.filter_activated_to_date = this.filterActivatedToDate;
            }
            if (this.filterFromEffectiveFromDate) {
                f.filter_from_effective_from_date =
                    this.filterFromEffectiveFromDate;
            }
            if (this.filterToEffectiveFromDate) {
                f.filter_to_effective_from_date =
                    this.filterToEffectiveFromDate;
            }
            if (this.filterFromEffectiveToDate) {
                f.filter_from_effective_to_date =
                    this.filterFromEffectiveToDate;
            }
            if (this.filterToEffectiveToDate) {
                f.filter_to_effective_to_date = this.filterToEffectiveToDate;
            }
            if (this.filterFromReviewDueDate) {
                f.filter_from_review_due_date = this.filterFromReviewDueDate;
            }
            if (this.filterToReviewDueDate) {
                f.filter_to_review_due_date = this.filterToReviewDueDate;
            }
            if (this.filterNameStatus && this.filterNameStatus !== 'all') {
                f.filter_name_status = this.filterNameStatus;
            }
            if (
                this.filterPublicationStatus &&
                this.filterPublicationStatus !== 'all'
            ) {
                f.filter_publication_status = this.filterPublicationStatus;
            }
            if (this.filterCommonwealthRelevance) {
                f.filter_commonwealth_relevance = 'true';
            }
            if (this.filterInternationalRelevance) {
                f.filter_international_relevance = 'true';
            }
            if (this.filterConservationCriteria) {
                f.filter_conservation_criteria =
                    this.filterConservationCriteria;
            }
            if (this.filterFaunaGroup && this.filterFaunaGroup !== 'all') {
                f.filter_fauna_group = this.filterFaunaGroup;
            }
            if (
                this.filterFaunaSubGroup &&
                this.filterFaunaSubGroup !== 'all'
            ) {
                f.filter_fauna_sub_group = this.filterFaunaSubGroup;
            }
            if (this.filterChangeCode && this.filterChangeCode !== 'all') {
                f.filter_change_code = this.filterChangeCode;
            }
            if (
                this.filterSubmitterCategory &&
                this.filterSubmitterCategory !== 'all'
            ) {
                f.filter_submitter_category = this.filterSubmitterCategory;
            }
            if (this.filterLocked && this.filterLocked !== 'all') {
                f.filter_locked = this.filterLocked;
            }
            if (this.filterCommonName) {
                f.filter_common_name = this.filterCommonName;
            }
            if (this.filterFamily) {
                f.filter_family = this.filterFamily;
            }
            if (this.filterGenus) {
                f.filter_genus = this.filterGenus;
            }
            if (
                this.filterInformalGroup &&
                this.filterInformalGroup !== 'all'
            ) {
                f.filter_informal_group = this.filterInformalGroup;
            }
            if (this.filterCommunityCommonId) {
                f.filter_community_common_id = this.filterCommunityCommonId;
            }
            if (this.filterAssessor) {
                f.filter_assessor = this.filterAssessor;
            }
            if (this.filterSubmitter) {
                f.filter_submitter = this.filterSubmitter;
            }
            if (this.filterLastModifiedBy) {
                f.filter_last_modified_by = this.filterLastModifiedBy;
            }
            return f;
        },
        fetchQueueHistory() {
            fetch('/api/queue_report_history/')
                .then((response) => response.json())
                .then((data) => {
                    this.queueHistory = data.results || [];
                    this.historyLimit = data.history_limit ?? null;
                })
                .catch(() => {});
        },
        queueReport() {
            this.submitting = true;
            this.errorMessage = '';

            fetch('/api/queue_report/', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    report_type: this.selectedCategory,
                    group_type: this.selectedGroupType,
                    format: this.format,
                    num_records: this.numRecords,
                    filters: this.buildFiltersPayload(),
                }),
            })
                .then((response) => {
                    return response.json().then((data) => {
                        if (!response.ok) {
                            this.errorMessage =
                                data.message || 'An error occurred.';
                        } else if (data.message) {
                            swal.fire({
                                title: 'Report Queued',
                                text: data.message,
                                icon: 'success',
                                confirmButtonText: 'OK',
                                customClass: {
                                    confirmButton: 'btn btn-primary',
                                },
                            });
                        }
                        this.fetchQueueHistory();
                    });
                })
                .catch(() => {
                    this.errorMessage =
                        'An error occurred while queuing the report.';
                })
                .finally(() => {
                    this.submitting = false;
                });
        },
        formatDate(isoString) {
            return new Date(isoString).toLocaleString();
        },
        rowClass(job) {
            if (job.status_id === 1) return 'table-info';
            if (job.status_id === 2) return 'table-success';
            if (job.status_id === 3) return 'table-danger';
            return '';
        },
        initPopovers() {
            this.disposePopovers();
            this.$nextTick(() => {
                this.$el
                    .querySelectorAll('[data-bs-toggle="popover"]')
                    .forEach((el) => {
                        new window.bootstrap.Popover(el, { container: 'body' });
                    });
            });
        },
        disposePopovers() {
            this.$el
                ?.querySelectorAll('[data-bs-toggle="popover"]')
                .forEach((el) => {
                    const instance = window.bootstrap.Popover.getInstance(el);
                    if (instance) instance.dispose();
                });
        },
    },
};
</script>

<style>
#internal-reports .form-select,
#internal-reports .form-control,
#internal-reports .form-check-label {
    color: #6c757d;
}
</style>
