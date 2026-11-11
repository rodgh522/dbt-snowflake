{{ config(
    materialized='table'
) }}

WITH src_synced AS (
    SELECT MAX(b."_fivetran_synced"::TIMESTAMP_NTZ(6)) AS _src_synced
    FROM {{ source('bronze', 'mmf') }} AS b
),

bronze AS (
    SELECT OBJECT_CONSTRUCT_KEEP_NULL(b.*) AS r
    FROM {{ source('bronze', 'mmf') }} AS b
),

unpivoted AS (
    SELECT
        r,
        TRIM(f.key::VARCHAR)    AS raw_month,
        f.value::VARCHAR        AS raw_value
    FROM bronze,
         LATERAL FLATTEN(INPUT => r) AS f
    WHERE REGEXP_LIKE(UPPER(TRIM(f.key::VARCHAR)),
                      '^([0-9]{4}|[0-9]{6}|JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)$')
),

parsed AS (
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
        GET_IGNORE_CASE(r, 'material')::VARCHAR                             AS material_code,
        COALESCE(GET_IGNORE_CASE(r, 'material_description_new')::VARCHAR,
                 GET_IGNORE_CASE(r, 'material_description_new_')::VARCHAR)  AS material_desc,
        GET_IGNORE_CASE(r, 'product_code')::VARCHAR                         AS brand_code,
        GET_IGNORE_CASE(r, 'product')::VARCHAR                              AS brand_name,
        GET_IGNORE_CASE(r, 'month_submission')::VARCHAR                     AS brand_group,
        TRY_TO_DOUBLE(raw_value)                                            AS amount
    FROM unpivoted
),

fx AS (
    SELECT rt_exchange_usd_to_krw
    FROM {{ ref('isl_fx_rates') }}
    WHERE use_yn = 'Y'
    ORDER BY base_year DESC, _load_dtm DESC
    LIMIT 1
),

transformed AS (
    SELECT
        p.base_month,
        p.material_code,
        p.material_desc,
        p.brand_code,
        p.brand_name,
        p.brand_group,
        p.amount,
        (p.amount * 1000000)::DECIMAL(20,4)                                                 AS amount_usd,
        ((p.amount * 1000000)::DECIMAL(20,4) * fx.rt_exchange_usd_to_krw)::DECIMAL(20,4)    AS amount_krw,
        CONVERT_TIMEZONE('UTC', 'Asia/Seoul', SYSDATE())::TIMESTAMP_NTZ(6)                  AS _load_dtm
    FROM parsed AS p
    CROSS JOIN fx
    WHERE p.amount IS NOT NULL
      AND p.amount <> 0
      AND p.material_code IS NOT NULL
      AND COALESCE(LOWER(p.brand_code), '') <> 'subtotal'
)

SELECT
    t.base_month,
    t.material_code,
    t.material_desc,
    t.brand_code,
    t.brand_name,
    t.brand_group,
    t.amount,
    t.amount_usd,
    t.amount_krw,
    t._load_dtm,
    s._src_synced
FROM transformed AS t
CROSS JOIN src_synced AS s

