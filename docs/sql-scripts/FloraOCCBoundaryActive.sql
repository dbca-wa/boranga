-- =============================================================================
-- Flora OCC Boundary Active  (KB Report)
-- DevOps Task #15554
--
-- One row per OccurrenceGeometry (Polygon type) for Flora Occurrences.
-- Filters:
--   - OCC Processing Status = Active only
--   - Species must have a current Approved Conservation Status
--
-- NOTE: OCC_MOD_BY returns an integer user ID from the ledger accounts_emailuser
-- table which lives in a separate database (ledger_db). A cross-database join is
-- not possible in standard PostgreSQL. If human-readable names are required,
-- either use dblink / postgres_fdw, or resolve IDs in application code.
--
-- NOTE: OBS_DATE is sourced from boranga_occplantcount (plant_count.obs_date).
-- =============================================================================

WITH
-- -- Group Type --------------------------------------------------------------
gt AS (
    SELECT id, name
    FROM boranga_grouptype
    WHERE name = 'flora'
),

-- -- Occurrences (Active + must have Approved CS) ----------------------------
occ AS (
    SELECT
        o.id,
        o.occurrence_number,
        o.occurrence_name,
        o.species_id,
        o.group_type_id,
        o.wild_status_id,
        o.occurrence_source,
        o.processing_status,
        o.datetime_updated
    FROM boranga_occurrence o
    INNER JOIN gt ON o.group_type_id = gt.id
    WHERE o.processing_status = 'active'
      AND EXISTS (
          SELECT 1 FROM boranga_conservationstatus cs
          WHERE cs.species_id = o.species_id
            AND cs.processing_status = 'approved'
      )
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
active_cs AS (
    SELECT
        cs.species_id,
        wal.code  AS wa_legislative_list_code,
        ccl.code  AS commonwealth_conservation_code
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

-- -- OCC Location ------------------------------------------------------------
loc AS (
    SELECT
        l.occurrence_id,
        l.location_description,
        l.locality,
        l.boundary_description,
        cs.name  AS coordinate_source,
        la.name  AS location_accuracy,
        r.name   AS region_name,
        d.name   AS district_name
    FROM boranga_occlocation l
    LEFT JOIN boranga_coordinatesource cs ON l.coordinate_source_id = cs.id
    LEFT JOIN boranga_locationaccuracy la ON l.location_accuracy_id = la.id
    LEFT JOIN boranga_region r ON l.region_id = r.id
    LEFT JOIN boranga_district d ON l.district_id = d.id
),

-- -- Observation Detail ------------------------------------------------------
obs_detail AS (
    SELECT
        obd.occurrence_id,
        aa.name  AS area_assessment,
        obd.area_surveyed
    FROM boranga_occobservationdetail obd
    LEFT JOIN boranga_areaassessment aa ON obd.area_assessment_id = aa.id
),

-- -- Plant Count (Flora-specific) --------------------------------------------
plant_count AS (
    SELECT
        pc.occurrence_id,
        pcm.name AS plant_count_method,
        csb.name AS counted_subject,
        pc.simple_alive,
        pc.detailed_alive_mature,
        pc.comment,
        CASE WHEN pc.flower_present = TRUE THEN 'Yes'
             WHEN pc.flower_present = FALSE THEN 'No'
             ELSE NULL
        END AS flower_present,
        pcond.name AS plant_condition,
        pc.obs_date
    FROM boranga_occplantcount pc
    LEFT JOIN boranga_plantcountmethod pcm ON pc.plant_count_method_id = pcm.id
    LEFT JOIN boranga_countedsubject csb ON pc.counted_subject_id = csb.id
    LEFT JOIN boranga_plantcondition pcond ON pc.plant_condition_id = pcond.id
),

-- -- Identification ----------------------------------------------------------
identification AS (
    SELECT
        i.occurrence_id,
        ic.name AS identification_certainty
    FROM boranga_occidentification i
    LEFT JOIN boranga_identificationcertainty ic ON i.identification_certainty_id = ic.id
),

-- -- Habitat Condition -------------------------------------------------------
habitat AS (
    SELECT
        hc.occurrence_id,
        hc.completely_degraded,
        hc.degraded,
        hc.good,
        hc.very_good,
        hc.excellent,
        hc.pristine
    FROM boranga_occhabitatcondition hc
),

-- -- Geometry (Polygons only) ------------------------------------------------
geom AS (
    SELECT
        g.id              AS geom_id,
        g.occurrence_id,
        g.geometry,
        g.updated_date,
        ROUND(
            (ST_Area(ST_Transform(g.geometry, 4326)::geography) / 1000000.0)::numeric, 6
        ) AS area_sq_km,
        ROUND(
            ST_Area(ST_Transform(g.geometry, 4326)::geography)::numeric
        ) AS area_sq_m
    FROM boranga_occurrencegeometry g
    WHERE ST_GeometryType(g.geometry) IN ('ST_Polygon', 'ST_MultiPolygon')
)

-- ===========================================================================
-- Final SELECT
-- ===========================================================================
SELECT
    -- OCC core
    occ.occurrence_number                          AS OCC_NUM,
    occ.occurrence_name                            AS OCC_NAME,
    ws.name                                        AS WLD_STATUS,

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

    -- Location
    loc.location_description                       AS LOC_DESC,
    loc.locality                                   AS LOCALITY,
    loc.boundary_description                       AS BOUND_DESC,
    loc.coordinate_source                          AS COORD_SRC,
    loc.location_accuracy                          AS LOC_ACC,

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
    plant_count.obs_date                           AS OBS_DATE,

    -- Identification
    identification.identification_certainty        AS IDENT_CRTY,

    -- Report metadata
    CASE
        WHEN occ.occurrence_source IS NULL OR occ.occurrence_source = '' THEN NULL
        WHEN occ.occurrence_source = 'ocr' THEN 'ORF'
        WHEN occ.occurrence_source = 'non-ocr' THEN 'No ORF'
        WHEN occ.occurrence_source = 'ocr,non-ocr' THEN 'ORF; No ORF'
        ELSE NULL
    END                                            AS OCC_SOURCE,
    occ.processing_status                          AS OCC_STATUS,
    TO_CHAR(occ.datetime_updated, 'YYYY-MM-DD HH24:MI:SS') AS OCC_MOD_DA,

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
    'Occurrence Geometry'                          AS G_DATATYPE,
    gt.name                                        AS GROUP_TYPE

FROM occ
INNER JOIN gt            ON occ.group_type_id = gt.id
INNER JOIN geom          ON occ.id = geom.occurrence_id
LEFT JOIN boranga_wildstatus ws ON occ.wild_status_id = ws.id
LEFT JOIN species        ON occ.species_id = species.id
LEFT JOIN active_cs      ON occ.species_id = active_cs.species_id
LEFT JOIN approved_cs    ON occ.species_id = approved_cs.species_id
LEFT JOIN loc            ON occ.id = loc.occurrence_id
LEFT JOIN obs_detail     ON occ.id = obs_detail.occurrence_id
LEFT JOIN plant_count    ON occ.id = plant_count.occurrence_id
LEFT JOIN identification ON occ.id = identification.occurrence_id
LEFT JOIN habitat        ON occ.id = habitat.occurrence_id
ORDER BY occ.occurrence_number, geom.geom_id;
