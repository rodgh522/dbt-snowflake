{{ config(
    materialized='table'
) }}

WITH src_synced AS (
    SELECT MAX(b."_fivetran_synced"::TIMESTAMP_NTZ(6)) AS _src_synced
    FROM {{ source('bronze', 'fx_rates') }} AS b
),

bronze AS (
    SELECT OBJECT_CONSTRUCT_KEEP_NULL(b.*) AS r
    FROM {{ source('bronze', 'fx_rates') }} AS b
),

parsed AS (
    SELECT
        TRIM(GET_IGNORE_CASE(r, 'col_2')::VARCHAR)                                                     AS br_year,
        TRUNC(TRY_TO_DOUBLE(GET_IGNORE_CASE(r, 'krw_usd')::VARCHAR))::INTEGER                          AS rate,
        TRUNC(TRY_TO_DOUBLE(SPLIT_PART(TRIM(GET_IGNORE_CASE(r, 'col_2')::VARCHAR), ' ', 1)))::INTEGER  AS base_year
    FROM bronze
    WHERE TRIM(GET_IGNORE_CASE(r, 'col_2')::VARCHAR) IS NOT NULL
),

transformed AS (
    SELECT
        br_year,
        rate                                                                AS rt_exchange_usd_to_krw,
        base_year,
        IFF(base_year = MAX(base_year) OVER (), 'Y', 'N')                   AS use_yn,
        CONVERT_TIMEZONE('UTC', 'Asia/Seoul', SYSDATE())::TIMESTAMP_NTZ(6)  AS _load_dtm
    FROM parsed
)

SELECT
    t.br_year,
    t.rt_exchange_usd_to_krw,
    t.base_year,
    t.use_yn,
    t._load_dtm,
    s._src_synced
FROM transformed AS t
CROSS JOIN src_synced AS s

