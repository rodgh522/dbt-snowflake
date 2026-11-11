{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='base_month'
) }}

WITH src_synced AS (
    SELECT MAX(b."_fivetran_synced"::TIMESTAMP_NTZ(6)) AS _src_synced
    FROM {{ source('bronze', 'stock_in_market_sales_stock') }} AS b
    WHERE b."_sheet_name" = 'In-market sales'
),

bronze AS (
    SELECT OBJECT_CONSTRUCT_KEEP_NULL(b.*) AS r
    FROM {{ source('bronze', 'stock_in_market_sales_stock') }} AS b
    WHERE b."_sheet_name" = 'In-market sales'
),

unpivoted AS (
    SELECT
        r,
        REGEXP_SUBSTR(UPPER(TRIM(f.key::VARCHAR)), '^COL_([0-9]{4})$', 1, 1, 'e', 1) AS raw_month,
        TRY_TO_DOUBLE(NULLIF(TRIM(f.value::VARCHAR), ''))::NUMBER(18,2)              AS sales_amount
    FROM bronze,
         LATERAL FLATTEN(INPUT => r) AS f
    WHERE REGEXP_LIKE(UPPER(TRIM(f.key::VARCHAR)), '^COL_[0-9]{4}$')
),

transformed AS (
    SELECT
        TRY_TO_DATE('20' || raw_month || '01', 'YYYYMMDD')                  AS base_month,
        IFF(TRIM(GET_IGNORE_CASE(r, 'material_mch_main')::VARCHAR) = 'Enhertu',
            '2082 Enhertu',
            TRIM(GET_IGNORE_CASE(r, 'material_mch_main')::VARCHAR))         AS material_code,
        TRIM(GET_IGNORE_CASE(r, 'old_material_code')::VARCHAR)              AS material_code_old,
        TRIM(GET_IGNORE_CASE(r, 'english_description')::VARCHAR)            AS material_desc,
        TRIM(GET_IGNORE_CASE(r, 'korean_description')::VARCHAR)             AS material_desc_kr,
        TRIM(GET_IGNORE_CASE(r, 'brand_code')::VARCHAR)                     AS brand_code,
        TRIM(GET_IGNORE_CASE(r, 'product_name')::VARCHAR)                   AS brand_name,
        TRIM(GET_IGNORE_CASE(r, 'product_family')::VARCHAR)                 AS brand_group,
        TRIM(GET_IGNORE_CASE(r, 'ta')::VARCHAR)                             AS therapy_area,
        TRIM(GET_IGNORE_CASE(r, 'bu')::VARCHAR)                             AS business_unit,
        TRIM(GET_IGNORE_CASE(r, 'divested_partner')::VARCHAR)               AS material_biz_type_cd,
        sales_amount,
        CONVERT_TIMEZONE('UTC', 'Asia/Seoul', SYSDATE())::TIMESTAMP_NTZ(6)  AS _load_dtm
    FROM unpivoted
    WHERE sales_amount IS NOT NULL
      AND sales_amount <> 0
      AND TRIM(GET_IGNORE_CASE(r, 'material_mch_main')::VARCHAR) IS NOT NULL
)

SELECT
    t.base_month,
    t.material_code,
    t.material_code_old,
    t.material_desc,
    t.material_desc_kr,
    t.brand_code,
    t.brand_name,
    t.brand_group,
    t.therapy_area,
    t.business_unit,
    t.material_biz_type_cd,
    t.sales_amount,
    t._load_dtm,
    s._src_synced
FROM transformed AS t
CROSS JOIN src_synced AS s

