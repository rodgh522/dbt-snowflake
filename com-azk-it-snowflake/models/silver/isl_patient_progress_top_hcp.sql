{{ config(
    materialized='table'
) }}

WITH src_synced AS (
    SELECT MAX(b."_fivetran_synced"::TIMESTAMP_NTZ(6)) AS _src_synced
    FROM {{ source('bronze', 'patient_progress_brands_tophca') }} AS b
),

bronze AS (
    SELECT OBJECT_CONSTRUCT_KEEP_NULL(b.*) AS r
    FROM {{ source('bronze', 'patient_progress_brands_tophca') }} AS b
),

transformed AS (
    SELECT
        GET_IGNORE_CASE(r, 'brick_name')::VARCHAR                            AS hcp,
        GET_IGNORE_CASE(r, 'top')::VARCHAR                                   AS tier,
        CONVERT_TIMEZONE('UTC', 'Asia/Seoul', SYSDATE())::TIMESTAMP_NTZ(6)   AS _load_dtm
    FROM bronze
    WHERE GET_IGNORE_CASE(r, 'brick_name')::VARCHAR IS NOT NULL
)

SELECT
    t.hcp,
    t.tier,
    t._load_dtm,
    s._src_synced
FROM transformed AS t
CROSS JOIN src_synced AS s

