{{ config(
    materialized='table'
) }}

WITH src_synced AS (
    SELECT MAX(b."_fivetran_synced"::TIMESTAMP_NTZ(6)) AS _src_synced
    FROM {{ source('bronze', 'revenue_deduction_rates') }} AS b
),

bronze AS (
    SELECT OBJECT_CONSTRUCT_KEEP_NULL(b.*) AS r
    FROM {{ source('bronze', 'revenue_deduction_rates') }} AS b
),

unpivoted AS (
    SELECT
        r,
        TRIM(f.key::VARCHAR)            AS raw_month,
        TRY_TO_DOUBLE(f.value::VARCHAR) AS deduction_rate
    FROM bronze,
         LATERAL FLATTEN(INPUT => r) AS f
    WHERE REGEXP_LIKE(UPPER(TRIM(f.key::VARCHAR)),
                      '^([0-9]{4}|[0-9]{6}|JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)$')
),

transformed AS (
    SELECT
        CASE
            WHEN REGEXP_LIKE(raw_month, '^[0-9]{6}$') THEN TRY_TO_DATE(raw_month || '01', 'YYYYMMDD')
            WHEN REGEXP_LIKE(raw_month, '^[0-9]{4}$') THEN TRY_TO_DATE('20' || raw_month || '01', 'YYYYMMDD')
            ELSE DATE_FROM_PARTS(
                     YEAR(CURRENT_DATE()),
                     DECODE(UPPER(raw_month),
                            'JAN',1,'FEB',2,'MAR',3,'APR',4,'MAY',5,'JUN',6,
                            'JUL',7,'AUG',8,'SEP',9,'OCT',10,'NOV',11,'DEC',12),
                     1)
        END                                                                 AS base_month,
        GET_IGNORE_CASE(r, 'sku')::VARCHAR                                  AS material_code,
        GET_IGNORE_CASE(r, 'sku_description')::VARCHAR                      AS material_desc,
        GET_IGNORE_CASE(r, 'brand')::VARCHAR                                AS brand_name,
        deduction_rate,
        CONVERT_TIMEZONE('UTC', 'Asia/Seoul', SYSDATE())::TIMESTAMP_NTZ(6)  AS _load_dtm
    FROM unpivoted
    WHERE deduction_rate IS NOT NULL
      AND deduction_rate <> 0
      AND GET_IGNORE_CASE(r, 'sku')::VARCHAR IS NOT NULL
)

SELECT
    t.base_month,
    t.material_code,
    t.material_desc,
    t.brand_name,
    t.deduction_rate,
    t._load_dtm,
    s._src_synced
FROM transformed AS t
CROSS JOIN src_synced AS s

