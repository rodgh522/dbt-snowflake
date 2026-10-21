{{ config(
    materialized='table'
) }}

WITH src_synced AS (
    SELECT GREATEST(
        (SELECT MAX(b."_fivetran_synced"::TIMESTAMP_NTZ(6)) FROM {{ source('bronze', 'patient_progress_brands_tagrisso') }} AS b),
        (SELECT MAX(b."_fivetran_synced"::TIMESTAMP_NTZ(6)) FROM {{ source('bronze', 'patient_progress_brands_enhertu') }} AS b)
    ) AS _src_synced
),

bronze AS (
    SELECT OBJECT_CONSTRUCT_KEEP_NULL(b.*) AS r
    FROM {{ source('bronze', 'patient_progress_brands_tagrisso') }} AS b
    UNION ALL
    SELECT OBJECT_CONSTRUCT_KEEP_NULL(b.*) AS r
    FROM {{ source('bronze', 'patient_progress_brands_enhertu') }} AS b
),

parsed AS (
    SELECT
        GET_IGNORE_CASE(r, 'brand')::VARCHAR                                AS brand_name,
        GET_IGNORE_CASE(r, 'hca')::VARCHAR                                  AS hca,
        GET_IGNORE_CASE(r, 'tier')::VARCHAR                                 AS tier,
        GET_IGNORE_CASE(r, 'indication')::VARCHAR                           AS indication,
        GET_IGNORE_CASE(r, 'sub_indication')::VARCHAR                       AS sub_indication,
        TRY_TO_DECIMAL(GET_IGNORE_CASE(r, 'new_pts')::VARCHAR, 18, 2)       AS new_pts,
        TRUNC(TRY_TO_DOUBLE(GET_IGNORE_CASE(r, 'year')::VARCHAR))::INTEGER  AS year,
        GET_IGNORE_CASE(r, 'month')::VARCHAR                                AS month,
        GET_IGNORE_CASE(r, 'qtr')::VARCHAR                                  AS qtr,
        GET_IGNORE_CASE(r, 'business_unit')::VARCHAR                        AS business_unit,
        GET_IGNORE_CASE(r, 'ta')::VARCHAR                                   AS therapy_area,
        GET_IGNORE_CASE(r, '#_file')::VARCHAR                                AS _file
    FROM bronze
),

transformed AS (
    SELECT
        -- year: 2자리(25) / 4자리(2025) 모두 허용, month: 대소문자·전체 철자(Jan/JAN/January) 허용
        DATE_FROM_PARTS(
            IFF(year < 100, 2000 + year, year),
            DECODE(UPPER(LEFT(TRIM(month), 3)),
                   'JAN',1,'FEB',2,'MAR',3,'APR',4,'MAY',5,'JUN',6,
                   'JUL',7,'AUG',8,'SEP',9,'OCT',10,'NOV',11,'DEC',12),
            1)                                                               AS base_month,
        brand_name,
        hca,
        tier,
        indication,
        sub_indication,
        new_pts,
        year,
        month,
        qtr,
        business_unit,
        therapy_area,
        _file,
        CONVERT_TIMEZONE('UTC', 'Asia/Seoul', SYSDATE())::TIMESTAMP_NTZ(6)   AS _load_dtm
    FROM parsed
    WHERE brand_name IS NOT NULL
)

SELECT
    t.base_month,
    t.brand_name,
    t.hca,
    t.tier,
    t.indication,
    t.sub_indication,
    t.new_pts,
    t.year,
    t.month,
    t.qtr,
    t.business_unit,
    t.therapy_area,
    t._file,
    t._load_dtm,
    s._src_synced
FROM transformed AS t
CROSS JOIN src_synced AS s
