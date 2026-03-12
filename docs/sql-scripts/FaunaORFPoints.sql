-- =============================================================================
-- Fauna ORF Points  (KB Report)
-- DevOps Task #15553
--
-- One row per OccurrenceReportGeometry (Point type) for Fauna OCRs.
-- Returns ALL processing statuses.
--
-- NOTE: ORF_MOD_BY returns an integer user ID from the ledger accounts_emailuser
-- table which lives in a separate database (ledger_db). A cross-database join is
-- not possible in standard PostgreSQL. If human-readable names are required,
-- either use dblink / postgres_fdw, or resolve IDs in application code.
--
-- NOTE: primary_detection_method, secondary_sign, and reproductive_state are
-- MultiSelectFields that store comma-separated IDs. They are resolved to
-- display names via lateral unnest joins to their respective lookup tables.
-- =============================================================================

WITH
-- -- Group Type --------------------------------------------------------------
gt AS (
    SELECT id, name
    FROM boranga_grouptype
    WHERE name = 'fauna'
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

-- -- Species + Taxonomy + Fauna Groups ---------------------------------------
species AS (
    SELECT
        s.id,
        s.species_number,
        t.scientific_name,
        string_agg(DISTINCT tv.vernacular_name, '; ') AS vernacular_names,
        fg.name  AS fauna_group,
        fsg.name AS fauna_sub_group
    FROM boranga_species s
    LEFT JOIN boranga_taxonomy t ON s.taxonomy_id = t.id
    LEFT JOIN boranga_taxonvernacular tv ON t.id = tv.taxonomy_id
    LEFT JOIN boranga_faunagroup fg ON s.fauna_group_id = fg.id
    LEFT JOIN boranga_faunasubgroup fsg ON s.fauna_sub_group_id = fsg.id
    GROUP BY s.id, t.scientific_name, fg.name, fsg.name
),

-- -- Active Conservation Status (approved + delisted) ------------------------
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

-- -- Animal Observation ------------------------------------------------------
-- Resolve MultiSelectField IDs to display names
animal_obs AS (
    SELECT
        ao.occurrence_report_id,

        -- Sum of all 9 alive fields
        COALESCE(ao.alive_adult_male, 0)
          + COALESCE(ao.alive_adult_female, 0)
          + COALESCE(ao.alive_adult_unknown, 0)
          + COALESCE(ao.alive_juvenile_male, 0)
          + COALESCE(ao.alive_juvenile_female, 0)
          + COALESCE(ao.alive_juvenile_unknown, 0)
          + COALESCE(ao.alive_unsure_male, 0)
          + COALESCE(ao.alive_unsure_female, 0)
          + COALESCE(ao.alive_unsure_unknown, 0)       AS total_alive,

        -- Sum of all 9 dead fields
        COALESCE(ao.dead_adult_male, 0)
          + COALESCE(ao.dead_adult_female, 0)
          + COALESCE(ao.dead_adult_unknown, 0)
          + COALESCE(ao.dead_juvenile_male, 0)
          + COALESCE(ao.dead_juvenile_female, 0)
          + COALESCE(ao.dead_juvenile_unknown, 0)
          + COALESCE(ao.dead_unsure_male, 0)
          + COALESCE(ao.dead_unsure_female, 0)
          + COALESCE(ao.dead_unsure_unknown, 0)         AS total_dead,

        ao.simple_alive,
        ao.simple_dead,
        ab.name                                         AS animal_behaviour,
        ao.animal_observation_detail_comment,

        -- MultiSelectField: primary_detection_method (comma-separated IDs -> names)
        (
            SELECT string_agg(pdm.name, '; ' ORDER BY pdm.name)
            FROM unnest(string_to_array(ao.primary_detection_method, ',')) AS val(id_str)
            INNER JOIN boranga_primarydetectionmethod pdm
                ON pdm.id = NULLIF(trim(val.id_str), '')::integer
        ) AS primary_detection_method,

        -- MultiSelectField: secondary_sign (comma-separated IDs -> names)
        (
            SELECT string_agg(ss.name, '; ' ORDER BY ss.name)
            FROM unnest(string_to_array(ao.secondary_sign, ',')) AS val(id_str)
            INNER JOIN boranga_secondarysign ss
                ON ss.id = NULLIF(trim(val.id_str), '')::integer
        ) AS secondary_sign,

        -- MultiSelectField: reproductive_state (comma-separated IDs -> names)
        (
            SELECT string_agg(rs.name, '; ' ORDER BY rs.name)
            FROM unnest(string_to_array(ao.reproductive_state, ',')) AS val(id_str)
            INNER JOIN boranga_reproductivestate rs
                ON rs.id = NULLIF(trim(val.id_str), '')::integer
        ) AS reproductive_state

    FROM boranga_ocranimalobservation ao
    LEFT JOIN boranga_animalbehaviour ab ON ao.animal_behaviour_id = ab.id
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

-- -- Geometry (Points only) --------------------------------------------------
geom AS (
    SELECT
        g.id              AS geom_id,
        g.occurrence_report_id,
        g.geometry,
        g.updated_date
    FROM boranga_occurrencereportgeometry g
    WHERE ST_GeometryType(g.geometry) IN ('ST_Point', 'ST_MultiPoint')
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

    -- Species (Fauna-specific)
    species.species_number                         AS SPECIE_NUM,
    species.scientific_name                        AS SPECIES,
    species.vernacular_names                       AS COMMON_NAM,
    species.fauna_group                            AS FA_GROUP,
    species.fauna_sub_group                        AS FA_SUB_GRP,

    -- Geometry
    geom.geometry                                  AS GEOMETRY,
    TO_CHAR(geom.updated_date, 'YYYY-MM-DD HH24:MI:SS') AS GEO_MODIFY,
    geom.geom_id                                   AS GEOM_ID,
    -- No area fields for Points

    -- Conservation Status
    active_cs.wa_legislative_list_code             AS WA_LEG_CS,
    approved_cs.wa_cons_code                       AS WACONSCODE,
    active_cs.commonwealth_conservation_code       AS COMWLTH_CS,

    -- Dates & status
    ocr.observation_date                           AS OBS_DATE,

    -- Location (no boundary_description for Fauna)
    loc.location_description                       AS LOC_DESC,
    loc.locality                                   AS LOCALITY,
    loc.coordinate_source                          AS COORD_SRC,
    loc.location_accuracy                          AS LOC_ACC,

    -- Observer
    observer.observer_name                         AS OBS_NAME,
    observer.organisation                          AS ORGANISTN,

    -- Observation Detail
    obs_detail.area_assessment                     AS AREA_ASSES,
    obs_detail.area_surveyed                       AS SURVEY_SQM,

    -- Animal Observation (Fauna-specific)
    animal_obs.total_alive                         AS AN_ALIVE,
    animal_obs.total_dead                          AS AN_DEAD,
    animal_obs.simple_alive                        AS SIMP_ALIVE,
    animal_obs.simple_dead                         AS SIMP_DEAD,
    animal_obs.animal_behaviour                    AS AN_BEHAV,
    animal_obs.primary_detection_method            AS DET_METHOD,
    animal_obs.secondary_sign                      AS SEC_SIGN,
    animal_obs.reproductive_state                  AS BREEDING,
    animal_obs.animal_observation_detail_comment   AS AN_OBS_COM,

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
LEFT JOIN animal_obs ON ocr.id = animal_obs.occurrence_report_id
LEFT JOIN identification ON ocr.id = identification.occurrence_report_id
LEFT JOIN latest_action ON ocr.id = latest_action.occurrence_report_id
LEFT JOIN habitat   ON ocr.id = habitat.occurrence_report_id
ORDER BY ocr.occurrence_report_number, geom.geom_id;
