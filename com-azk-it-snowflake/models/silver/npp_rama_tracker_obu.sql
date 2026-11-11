{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['ta', 'brand', 'study_name', 'indication', 'dedup_seq']
) }}

WITH cleaned AS (
    SELECT
        NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("_sheet_name"::STRING, CHR(12288), ' '), CHR(160), ' '), ' +', ' ')), '')                                    AS ta,
        NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("brand"::STRING, CHR(12288), ' '), CHR(160), ' '), ' +', ' ')), '')                                          AS brand,
        NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("study_name"::STRING, CHR(12288), ' '), CHR(160), ' '), ' +', ' ')), '')                                     AS study_name,
        NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("indication"::STRING, CHR(12288), ' '), CHR(160), ' '), ' +', ' ')), '')                                     AS indication,
        NULLIF(TRIM(REPLACE(REPLACE("current_status"::STRING, CHR(12288), ' '), CHR(160), ' ')), '')                                                            AS current_status,
        NULLIF(TRIM(REPLACE(REPLACE("ra_category"::STRING, CHR(12288), ' '), CHR(160), ' ')), '')                                                               AS ra_category,
        NULLIF(TRIM(REPLACE(REPLACE("gift_odd"::STRING, CHR(12288), ' '), CHR(160), ' ')), '')                                                                  AS gift_odd,
        NULLIF(TRIM(REPLACE(REPLACE("p_r_track"::STRING, CHR(12288), ' '), CHR(160), ' ')), '')                                                                 AS pnr_track,
        IFF(REGEXP_LIKE(NULLIF(TRIM(REPLACE(REPLACE("ra_submission_base"::STRING, CHR(12288), ' '), CHR(160), ' ')), ''), '^[0-9]{4}-[0-9]{2}-[0-9]{2}.*'),
            LEFT(NULLIF(TRIM(REPLACE(REPLACE("ra_submission_base"::STRING, CHR(12288), ' '), CHR(160), ' ')), ''), 10),
            NULLIF(TRIM(REPLACE(REPLACE("ra_submission_base"::STRING, CHR(12288), ' '), CHR(160), ' ')), ''))                                                   AS ra_submission_base,
        IFF(REGEXP_LIKE(NULLIF(TRIM(REPLACE(REPLACE("ra_approval_base"::STRING, CHR(12288), ' '), CHR(160), ' ')), ''), '^[0-9]{4}-[0-9]{2}-[0-9]{2}.*'),
            LEFT(NULLIF(TRIM(REPLACE(REPLACE("ra_approval_base"::STRING, CHR(12288), ' '), CHR(160), ' ')), ''), 10),
            NULLIF(TRIM(REPLACE(REPLACE("ra_approval_base"::STRING, CHR(12288), ' '), CHR(160), ' ')), ''))                                                     AS ra_approval_base,
        IFF(REGEXP_LIKE(NULLIF(TRIM(REPLACE(REPLACE("p_r_submission_base"::STRING, CHR(12288), ' '), CHR(160), ' ')), ''), '^[0-9]{4}-[0-9]{2}-[0-9]{2}.*'),
            LEFT(NULLIF(TRIM(REPLACE(REPLACE("p_r_submission_base"::STRING, CHR(12288), ' '), CHR(160), ' ')), ''), 10),
            NULLIF(TRIM(REPLACE(REPLACE("p_r_submission_base"::STRING, CHR(12288), ' '), CHR(160), ' ')), ''))                                                  AS pnr_submission_base,
        IFF(REGEXP_LIKE(NULLIF(TRIM(REPLACE(REPLACE("reim_launch_best"::STRING, CHR(12288), ' '), CHR(160), ' ')), ''), '^[0-9]{4}-[0-9]{2}-[0-9]{2}.*'),
            LEFT(NULLIF(TRIM(REPLACE(REPLACE("reim_launch_best"::STRING, CHR(12288), ' '), CHR(160), ' ')), ''), 10),
            NULLIF(TRIM(REPLACE(REPLACE("reim_launch_best"::STRING, CHR(12288), ' '), CHR(160), ' ')), ''))                                                     AS reim_launch_best,
        IFF(REGEXP_LIKE(NULLIF(TRIM(REPLACE(REPLACE("reim_launch_base"::STRING, CHR(12288), ' '), CHR(160), ' ')), ''), '^[0-9]{4}-[0-9]{2}-[0-9]{2}.*'),
            LEFT(NULLIF(TRIM(REPLACE(REPLACE("reim_launch_base"::STRING, CHR(12288), ' '), CHR(160), ' ')), ''), 10),
            NULLIF(TRIM(REPLACE(REPLACE("reim_launch_base"::STRING, CHR(12288), ' '), CHR(160), ' ')), ''))                                                     AS reim_launch_base,
        NULLIF(TRIM(REPLACE(REPLACE("ra_remarks_best_approval_ra_key_milestone_etc"::STRING, CHR(12288), ' '), CHR(160), ' ')), '')                             AS ra_remarks,
        NULLIF(TRIM(REPLACE(REPLACE("p_r_remarks_mtp_timelines_p_r_key_milestone_etc"::STRING, CHR(12288), ' '), CHR(160), ' ')), '')                           AS pnr_remarks,
        NULLIF(TRIM(REPLACE(REPLACE("ra_lead"::STRING, CHR(12288), ' '), CHR(160), ' ')), '')                                                                   AS ra_lead,
        NULLIF(TRIM(REPLACE(REPLACE("p_r_lead"::STRING, CHR(12288), ' '), CHR(160), ' ')), '')                                                                  AS pnr_lead,
        NULLIF(TRIM(REPLACE(REPLACE("col_1_st_country_submission"::STRING, CHR(12288), ' '), CHR(160), ' ')), '')                                               AS country_submission_1st,
        NULLIF(TRIM(REPLACE(REPLACE("col_1_st_country_approval"::STRING, CHR(12288), ' '), CHR(160), ' ')), '')                                                 AS country_approval_1st,
        NULLIF(TRIM(REPLACE(REPLACE("pivotal_study_y_n"::STRING, CHR(12288), ' '), CHR(160), ' ')), '')                                                         AS pivotal_study,
        NULLIF(TRIM(REPLACE(REPLACE("need_for_korean_bridging_purpose_y_n_tbd"::STRING, CHR(12288), ' '), CHR(160), ' ')), '')                                  AS need_for_korean_bridging_purpose,
        NULLIF(TRIM(REPLACE(REPLACE("required_for_reg_submission_full_analysis_set"::STRING, CHR(12288), ' '), CHR(160), ' ')), '')                             AS required_for_reg_submission,
        "_line"                                                                                                                                                 AS src_line
    FROM {{ source('bronze', 'npp_obu_rama_tracker') }}
    WHERE COALESCE("_fivetran_deleted", FALSE) = FALSE
)

SELECT
    ta,
    brand,
    study_name,
    indication,
    current_status,
    ra_category,
    gift_odd,
    pnr_track,
    ra_submission_base,
    ra_approval_base,
    pnr_submission_base,
    reim_launch_best,
    reim_launch_base,
    ra_remarks,
    pnr_remarks,
    ra_lead,
    pnr_lead,
    country_submission_1st,
    country_approval_1st,
    pivotal_study,
    need_for_korean_bridging_purpose,
    required_for_reg_submission,
    ROW_NUMBER() OVER (PARTITION BY ta, brand, study_name, indication ORDER BY src_line) AS dedup_seq
FROM cleaned
WHERE brand IS NOT NULL
   OR study_name IS NOT NULL
   OR indication IS NOT NULL

