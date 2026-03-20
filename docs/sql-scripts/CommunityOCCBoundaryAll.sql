-- =============================================================================
-- Community OCC Boundary All  (KB Report)
-- DevOps Task #15559
-- Frequency: Monthly
--
-- One row per OccurrenceGeometry (Polygon type) for Community Occurrences.
-- Returns ALL processing statuses.
--
-- NOTE: OCC_MOD_BY is resolved via accounts_emailuser and returns
-- first_name || ' ' || last_name for the last user to modify the record.
--
-- NOTE: OBS_DATE is sourced from boranga_occhabitatcondition (habitat.obs_date).
--
-- IMPORTANT — KB does not allow comments in SQL queries. Before pasting this
-- script into KB, strip all comments using:
--   python scripts/strip_sql_comments.py docs/sql-scripts/CommunityOCCBoundaryAll.sql
-- Or write the result to a file for easy copying:
--   python scripts/strip_sql_comments.py docs/sql-scripts/CommunityOCCBoundaryAll.sql > tmp.sql
-- =============================================================================

WITH
-- -- Group Type --------------------------------------------------------------
gt AS (
    SELECT id, name
    FROM boranga_grouptype
    WHERE name = 'community'
),

-- -- Occurrences (all statuses) ----------------------------------------------
occ AS (
    SELECT
        o.id,
        o.occurrence_number,
        o.occurrence_name,
        o.community_id,
        o.group_type_id,
        o.wild_status_id,
        o.occurrence_source,
        o.processing_status,
        o.lodgement_date,
        o.datetime_updated,
        o.last_modified_by
    FROM boranga_occurrence o
    INNER JOIN gt ON o.group_type_id = gt.id
),

-- -- Community + Taxonomy ----------------------------------------------------
community AS (
    SELECT
        c.id,
        c.community_number,
        ct.community_name,
        ct.community_common_id AS community_id
    FROM boranga_community c
    LEFT JOIN boranga_communitytaxonomy ct ON c.id = ct.community_id
),

-- -- Active Conservation Status (approved + delisted) ------------------------
active_cs AS (
    SELECT
        cs.community_id,
        wal.code  AS wa_legislative_list_code,
        ccl.code  AS commonwealth_conservation_code
    FROM boranga_conservationstatus cs
    LEFT JOIN boranga_walegislativelist wal ON cs.wa_legislative_list_id = wal.id
    LEFT JOIN boranga_commonwealthconservationlist ccl
        ON cs.commonwealth_conservation_category_id = ccl.id
    WHERE cs.processing_status IN ('approved', 'delisted')
      AND cs.community_id IS NOT NULL
),

-- -- Approved-only Conservation Status (exclude delisted) --------------------
approved_cs AS (
    SELECT
        cs.community_id,
        concat_ws('; ',
            NULLIF(walc.code, ''),
            NULLIF(wapc.code, '')
        ) AS wa_cons_code
    FROM boranga_conservationstatus cs
    LEFT JOIN boranga_walegislativecategory walc ON cs.wa_legislative_category_id = walc.id
    LEFT JOIN boranga_waprioritycategory wapc ON cs.wa_priority_category_id = wapc.id
    WHERE cs.processing_status = 'approved'
      AND cs.community_id IS NOT NULL
),

-- -- Concatenated site names per OCC -----------------------------------------
sites_concat AS (
    SELECT
        s.occurrence_id,
        string_agg(s.site_name, '; ' ORDER BY s.site_name) AS site_names
    FROM boranga_occurrencesite s
    WHERE s.visible = TRUE
    GROUP BY s.occurrence_id
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

-- -- Identification ----------------------------------------------------------
identification AS (
    SELECT
        i.occurrence_id,
        ic.name AS identification_certainty,
        i.id_confirmed_by,
        i.identification_comment,
        i.collector_number
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
        hc.pristine,
        hc.obs_date
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
            (ST_Area(ST_Transform(g.geometry, 4326)::geography) / 10000.0)::numeric, 4
        ) AS area_ha
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
    sites_concat.site_names                        AS SITE_NAME,
    ws.name                                        AS WLD_STATUS,

    -- Community
    community.community_number                     AS COMMU_NUM,
    community.community_name                       AS COMMU_NAME,
    community.community_id                         AS COMMU_ID,

    -- Geometry
    geom.geometry                                  AS GEOMETRY,
    TO_CHAR(geom.updated_date, 'YYYY-MM-DD HH24:MI:SS') AS GEO_MODIFY,
    geom.geom_id                                   AS GEOM_ID,
    geom.area_sq_km                                AS G_AREA_SKM,
    geom.area_ha                                   AS G_AREA_HA,

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

    -- Identification
    identification.identification_certainty        AS IDENT_CRTY,
    identification.id_confirmed_by                 AS ID_CONF_BY,
    identification.identification_comment          AS ID_COMMENT,
    identification.collector_number                AS COLL_NUM,

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
    (u_mod.first_name || ' ' || u_mod.last_name)   AS OCC_MOD_BY,
    occ.lodgement_date                             AS LODG_DATE,

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
    habitat.obs_date                               AS OBS_DATE,

    -- WISH fields
    'Occurrence Geometry'                          AS G_DATATYPE,
    gt.name                                        AS GROUP_TYPE

FROM occ
INNER JOIN gt            ON occ.group_type_id = gt.id
INNER JOIN geom          ON occ.id = geom.occurrence_id
LEFT JOIN boranga_wildstatus ws ON occ.wild_status_id = ws.id
LEFT JOIN community      ON occ.community_id = community.id
LEFT JOIN active_cs      ON occ.community_id = active_cs.community_id
LEFT JOIN approved_cs    ON occ.community_id = approved_cs.community_id
LEFT JOIN sites_concat   ON occ.id = sites_concat.occurrence_id
LEFT JOIN loc            ON occ.id = loc.occurrence_id
LEFT JOIN obs_detail     ON occ.id = obs_detail.occurrence_id
LEFT JOIN identification ON occ.id = identification.occurrence_id
LEFT JOIN habitat        ON occ.id = habitat.occurrence_id
LEFT JOIN accounts_emailuser u_mod ON occ.last_modified_by = u_mod.id
ORDER BY occ.occurrence_number, geom.geom_id;
