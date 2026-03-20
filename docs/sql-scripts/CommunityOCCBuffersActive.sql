-- =============================================================================
-- Community OCC Buffers Active  (KB Report)
-- DevOps Task #15560
-- Frequency: Daily
--
-- One row per BufferGeometry for Community Occurrences.
-- Filters:
--   - OCC Processing Status = Active only
--   - Community must have a current Approved Conservation Status
--
-- NOTE: OBS_DATE is sourced from boranga_occhabitatcondition (habitat.obs_date).
--
-- IMPORTANT — KB does not allow comments in SQL queries. Before pasting this
-- script into KB, strip all comments using:
--   python scripts/strip_sql_comments.py docs/sql-scripts/CommunityOCCBuffersActive.sql
-- Or write the result to a file for easy copying:
--   python scripts/strip_sql_comments.py docs/sql-scripts/CommunityOCCBuffersActive.sql > tmp.sql
-- =============================================================================

WITH
-- -- Group Type --------------------------------------------------------------
gt AS (
    SELECT id, name
    FROM boranga_grouptype
    WHERE name = 'community'
),

-- -- Occurrences (Active + must have Approved CS) ----------------------------
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
        o.datetime_updated
    FROM boranga_occurrence o
    INNER JOIN gt ON o.group_type_id = gt.id
    WHERE o.processing_status = 'active'
      AND EXISTS (
          SELECT 1 FROM boranga_conservationstatus cs
          WHERE cs.community_id = o.community_id
            AND cs.processing_status = 'approved'
      )
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

-- -- OCC Location (coordinate_source + region/district only) -----------------
loc AS (
    SELECT
        l.occurrence_id,
        cs.name  AS coordinate_source,
        r.name   AS region_name,
        d.name   AS district_name
    FROM boranga_occlocation l
    LEFT JOIN boranga_coordinatesource cs ON l.coordinate_source_id = cs.id
    LEFT JOIN boranga_region r ON l.region_id = r.id
    LEFT JOIN boranga_district d ON l.district_id = d.id
),

-- -- Observation Detail ------------------------------------------------------
obs_detail AS (
    SELECT
        obd.occurrence_id,
        aa.name  AS area_assessment
    FROM boranga_occobservationdetail obd
    LEFT JOIN boranga_areaassessment aa ON obd.area_assessment_id = aa.id
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
        hc.pristine,
        hc.obs_date
    FROM boranga_occhabitatcondition hc
),

-- -- Buffer Geometry (via OccurrenceGeometry) --------------------------------
buf AS (
    SELECT
        bg.id              AS buffer_geom_id,
        bg.geometry        AS buffer_geometry,
        og.id              AS occ_geom_id,
        og.occurrence_id,
        og.buffer_radius,
        og.updated_date,
        ROUND(
            (ST_Area(ST_Transform(bg.geometry, 4326)::geography) / 1000000.0)::numeric, 6
        ) AS area_sq_km,
        ROUND(
            (ST_Area(ST_Transform(bg.geometry, 4326)::geography) / 10000.0)::numeric, 4
        ) AS area_ha
    FROM boranga_buffergeometry bg
    INNER JOIN boranga_occurrencegeometry og ON bg.buffered_from_geometry_id = og.id
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

    -- Buffer Geometry
    buf.buffer_geometry                            AS GEOMETRY,
    buf.buffer_radius                              AS BUFF_VALUE,
    TO_CHAR(buf.updated_date, 'YYYY-MM-DD HH24:MI:SS') AS GEO_MODIFY,
    buf.occ_geom_id                                AS GEOM_ID,
    buf.area_sq_km                                 AS G_AREA_SKM,
    buf.area_ha                                    AS G_AREA_HA,

    -- Conservation Status
    active_cs.wa_legislative_list_code             AS WA_LEG_CS,
    approved_cs.wa_cons_code                       AS WACONSCODE,
    active_cs.commonwealth_conservation_code       AS COMWLTH_CS,

    -- Location (coordinate_source only for this report)
    loc.coordinate_source                          AS COORD_SRC,

    -- Observation Detail
    obs_detail.area_assessment                     AS AREA_ASSES,

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
    habitat.obs_date                               AS OBS_DATE,

    -- WISH fields
    'Buffer Geometry'                              AS G_DATATYPE,
    gt.name                                        AS GROUP_TYPE

FROM occ
INNER JOIN gt            ON occ.group_type_id = gt.id
INNER JOIN buf           ON occ.id = buf.occurrence_id
LEFT JOIN boranga_wildstatus ws ON occ.wild_status_id = ws.id
LEFT JOIN community      ON occ.community_id = community.id
LEFT JOIN active_cs      ON occ.community_id = active_cs.community_id
LEFT JOIN approved_cs    ON occ.community_id = approved_cs.community_id
LEFT JOIN sites_concat   ON occ.id = sites_concat.occurrence_id
LEFT JOIN loc            ON occ.id = loc.occurrence_id
LEFT JOIN obs_detail     ON occ.id = obs_detail.occurrence_id
LEFT JOIN identification ON occ.id = identification.occurrence_id
LEFT JOIN habitat        ON occ.id = habitat.occurrence_id
ORDER BY occ.occurrence_number, buf.occ_geom_id;
