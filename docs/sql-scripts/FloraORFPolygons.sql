-- =============================================================================
-- Flora ORF Polygons  (KB Report)
-- DevOps Task #15546
--
-- One row per OccurrenceReportGeometry (Polygon type) for Flora OCRs.
-- Returns ALL processing statuses.
--
-- NOTE: ORF_MOD_BY returns an integer user ID from the ledger accounts_emailuser
-- table which lives in a separate database (ledger_db). A cross-database join is
-- not possible in standard PostgreSQL. If human-readable names are required,
-- either use dblink / postgres_fdw, or resolve IDs in application code.
-- =============================================================================

WITH
-- -- Group Type --------------------------------------------------------------
gt AS (
    SELECT id, name
    FROM boranga_grouptype
    WHERE name = 'flora'
),

-- -- Occurrence Reports (all statuses) ---------------------------------------
ocr AS (
    SELECT
        o.id,
        o.occurrence_report_number,
        o.occurrence_id,
        o.species_id,
        o.observation_date,
        o.lodgement_date,
        o.site,
        o.record_source,
        o.processing_status,
        o.group_type_id,
        o.ocr_for_occ_name
    FROM boranga_occurrencereport o
    INNER JOIN gt ON o.group_type_id = gt.id
),

-- -- Parent Occurrence (if linked) -------------------------------------------
occ AS (
    SELECT id, occurrence_number, occurrence_name
    FROM boranga_occurrence
),

-- -- Species + Taxonomy ------------------------------------------------------
species AS (
    SELECT
        s.id,
        s.species_number,
        t.scientific_name,
        string_agg(DISTINCT tv.vernacular_name, '; ') AS vernacular_names
    FROM boranga_species s
    LEFT JOIN boranga_taxonomy t ON s.taxonomy_id = t.id
    LEFT JOIN boranga_taxonvernacular tv ON t.id = tv.taxonomy_id
    GROUP BY s.id, t.scientific_name
),

-- -- Active Conservation Status (approved + delisted) ------------------------
-- Business rule: one active CS per species at a time.
active_cs AS (
    SELECT
        cs.species_id,
        wal.code  AS wa_legislative_list_code,
        ccl.code  AS commonwealth_conservation_code,
        cs.processing_status AS cs_status
    FROM boranga_conservationstatus cs
    LEFT JOIN boranga_walegislativelist wal ON cs.wa_legislative_list_id = wal.id
    LEFT JOIN boranga_commonwealthconservationlist ccl
        ON cs.commonwealth_conservation_category_id = ccl.id
    WHERE cs.processing_status IN ('approved', 'delisted')
      AND cs.species_id IS NOT NULL
),

-- -- Approved-only Conservation Status (exclude delisted) --------------------
approved_cs AS (
    SELECT
        cs.species_id,
        concat_ws('; ',
            NULLIF(walc.code, ''),
            NULLIF(wapc.code, '')
        ) AS wa_cons_code
    FROM boranga_conservationstatus cs
    LEFT JOIN boranga_walegislativecategory walc ON cs.wa_legislative_category_id = walc.id
    LEFT JOIN boranga_waprioritycategory wapc ON cs.wa_priority_category_id = wapc.id
    WHERE cs.processing_status = 'approved'
      AND cs.species_id IS NOT NULL
),

-- -- OCR Location ------------------------------------------------------------
loc AS (
    SELECT
        l.occurrence_report_id,
        l.location_description,
        l.locality,
        l.boundary_description,
        cs.name  AS coordinate_source,
        la.name  AS location_accuracy,
        r.name   AS region_name,
        d.name   AS district_name
    FROM boranga_ocrlocation l
    LEFT JOIN boranga_coordinatesource cs ON l.coordinate_source_id = cs.id
    LEFT JOIN boranga_locationaccuracy la ON l.location_accuracy_id = la.id
    LEFT JOIN boranga_region r ON l.region_id = r.id
    LEFT JOIN boranga_district d ON l.district_id = d.id
),

-- -- Observer (main observer only) -------------------------------------------
observer AS (
    SELECT
        od.occurrence_report_id,
        od.observer_name,
        od.organisation
    FROM boranga_ocrobserverdetail od
    WHERE od.main_observer = TRUE
      AND od.visible = TRUE
),

-- -- Observation Detail ------------------------------------------------------
obs_detail AS (
    SELECT
        obd.occurrence_report_id,
        aa.name  AS area_assessment,
        obd.area_surveyed
    FROM boranga_ocrobservationdetail obd
    LEFT JOIN boranga_areaassessment aa ON obd.area_assessment_id = aa.id
),

-- -- Plant Count -------------------------------------------------------------
plant_count AS (
    SELECT
        pc.occurrence_report_id,
        pcm.name AS plant_count_method,
        csb.name AS counted_subject,
        pc.simple_alive,
        pc.detailed_alive_mature,
        pc.comment,
        CASE WHEN pc.flower_present = TRUE THEN 'Yes'
             WHEN pc.flower_present = FALSE THEN 'No'
             ELSE NULL
        END AS flower_present,
        pcond.name AS plant_condition
    FROM boranga_ocrplantcount pc
    LEFT JOIN boranga_plantcountmethod pcm ON pc.plant_count_method_id = pcm.id
    LEFT JOIN boranga_countedsubject csb ON pc.counted_subject_id = csb.id
    LEFT JOIN boranga_plantcondition pcond ON pc.plant_condition_id = pcond.id
),

-- -- Identification ----------------------------------------------------------
identification AS (
    SELECT
        i.occurrence_report_id,
        ic.name AS identification_certainty,
        i.id_confirmed_by,
        i.identification_comment,
        i.collector_number
    FROM boranga_ocridentification i
    LEFT JOIN boranga_identificationcertainty ic ON i.identification_certainty_id = ic.id
),

-- -- Most recent User Action per OCR -----------------------------------------
latest_action AS (
    SELECT DISTINCT ON (ua.occurrence_report_id)
        ua.occurrence_report_id,
        ua."when" AS last_modified_date,
        ua.who    AS last_modified_by
    FROM boranga_occurrencereportuseraction ua
    ORDER BY ua.occurrence_report_id, ua."when" DESC
),

-- -- Habitat Condition -------------------------------------------------------
habitat AS (
    SELECT
        hc.occurrence_report_id,
        hc.completely_degraded,
        hc.degraded,
        hc.good,
        hc.very_good,
        hc.excellent,
        hc.pristine
    FROM boranga_ocrhabitatcondition hc
),

-- -- Geometry (Polygons only) ------------------------------------------------
geom AS (
    SELECT
        g.id              AS geom_id,
        g.occurrence_report_id,
        g.geometry,
        g.updated_date,
        ROUND(
            (ST_Area(ST_Transform(g.geometry, 4326)::geography) / 1000000.0)::numeric, 6
        ) AS area_sq_km,
        ROUND(
            ST_Area(ST_Transform(g.geometry, 4326)::geography)::numeric
        ) AS area_sq_m
    FROM boranga_occurrencereportgeometry g
    WHERE ST_GeometryType(g.geometry) IN ('ST_Polygon', 'ST_MultiPolygon')
)

-- ===========================================================================
-- Final SELECT
-- ===========================================================================
SELECT
    -- OCR core
    ocr.occurrence_report_number                   AS ORF_NUM,
    occ.occurrence_number                          AS OCC_NUM,
    occ.occurrence_name                            AS OCC_NAME,
    ocr.ocr_for_occ_name                           AS ENT_OCC_NM,
    ocr.site                                       AS ENT_ORF_ST,

    -- Species
    species.species_number                         AS SPECIE_NUM,
    species.scientific_name                        AS SPECIES,
    species.vernacular_names                       AS COMMON_NAM,

    -- Geometry
    geom.geometry                                  AS GEOMETRY,
    TO_CHAR(geom.updated_date, 'YYYY-MM-DD HH24:MI:SS') AS GEO_MODIFY,
    geom.geom_id                                   AS GEOM_ID,
    geom.area_sq_km                                AS G_AREA_SKM,
    geom.area_sq_m                                 AS G_AREA_SQM,

    -- Conservation Status
    active_cs.wa_legislative_list_code             AS WA_LEG_CS,
    approved_cs.wa_cons_code                       AS WACONSCODE,
    active_cs.commonwealth_conservation_code       AS COMWLTH_CS,

    -- Dates & status
    ocr.observation_date                           AS OBS_DATE,

    -- Location
    loc.location_description                       AS LOC_DESC,
    loc.locality                                   AS LOCALITY,
    loc.boundary_description                       AS BOUND_DESC,
    loc.coordinate_source                          AS COORD_SRC,
    loc.location_accuracy                          AS LOC_ACC,

    -- Observer
    observer.observer_name                         AS OBS_NAME,
    observer.organisation                          AS ORGANISTN,

    -- Observation Detail
    obs_detail.area_assessment                     AS AREA_ASSES,
    obs_detail.area_surveyed                       AS SURVEY_SQM,

    -- Plant Count (Flora-specific)
    plant_count.plant_count_method                 AS CNT_MTHD,
    plant_count.counted_subject                    AS CNT_SUBJ,
    plant_count.simple_alive                       AS PL_SMP_ALV,
    plant_count.detailed_alive_mature              AS PL_ALV_MAT,
    plant_count.comment                            AS PLCNT_COMM,
    plant_count.flower_present                     AS IN_FLOWER,
    plant_count.plant_condition                    AS PLNT_COND,

    -- Identification
    identification.identification_certainty        AS IDENT_CRTY,
    identification.id_confirmed_by                 AS ID_CONF_BY,
    identification.identification_comment          AS ID_COMMENT,
    identification.collector_number                AS COLL_NUM,

    -- Report metadata
    ocr.record_source                              AS OCR_SOURCE,
    ocr.processing_status                          AS ORF_STATUS,
    latest_action.last_modified_date               AS ORF_MOD_DA,
    latest_action.last_modified_by                 AS ORF_MOD_BY,
    ocr.lodgement_date                             AS LODG_DATE,

    -- Region / District
    loc.region_name                                AS REGION,
    loc.district_name                              AS DISTRICT,

    -- Habitat Condition
    habitat.completely_degraded                    AS COMP_DEGRD,
    habitat.degraded                               AS DEGRADED,
    habitat.good                                   AS GOOD,
    habitat.very_good                              AS VERY_GOOD,
    habitat.excellent                              AS EXCELLENT,
    habitat.pristine                               AS PRISTINE,

    -- WISH fields
    'Occurrence Report Geometry'                   AS G_DATATYPE,
    gt.name                                        AS GROUP_TYPE

FROM ocr
INNER JOIN gt       ON ocr.group_type_id = gt.id
INNER JOIN geom     ON ocr.id = geom.occurrence_report_id
LEFT JOIN occ       ON ocr.occurrence_id = occ.id
LEFT JOIN species   ON ocr.species_id = species.id
LEFT JOIN active_cs ON species.id = active_cs.species_id
LEFT JOIN approved_cs ON species.id = approved_cs.species_id
LEFT JOIN loc       ON ocr.id = loc.occurrence_report_id
LEFT JOIN observer  ON ocr.id = observer.occurrence_report_id
LEFT JOIN obs_detail ON ocr.id = obs_detail.occurrence_report_id
LEFT JOIN plant_count ON ocr.id = plant_count.occurrence_report_id
LEFT JOIN identification ON ocr.id = identification.occurrence_report_id
LEFT JOIN latest_action ON ocr.id = latest_action.occurrence_report_id
LEFT JOIN habitat   ON ocr.id = habitat.occurrence_report_id
ORDER BY ocr.occurrence_report_number, geom.geom_id;
