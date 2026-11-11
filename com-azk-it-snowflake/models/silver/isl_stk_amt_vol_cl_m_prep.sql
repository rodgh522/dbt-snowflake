{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='base_month'
) }}

WITH src_synced AS (
    SELECT MAX(b."_fivetran_synced"::TIMESTAMP_NTZ(6)) AS _src_synced
    FROM {{ source('bronze', 'stock_in_market_sales_stock') }} AS b
    WHERE b."_sheet_name" IN ('Stock value', 'Stock vol')
),

channel_map AS (
    SELECT
        column1 AS col_seq,
        column2 AS channel_type
    FROM VALUES
        ('0', 'Total'),
        ('1', 'Channel'),
        ('2', 'Partner')
),

amount_bronze AS (
    SELECT OBJECT_CONSTRUCT_KEEP_NULL(b.*) AS row_object
    FROM {{ source('bronze', 'stock_in_market_sales_stock') }} AS b
    WHERE b."_sheet_name" = 'Stock value'
),

amount_long AS (
    SELECT
        row_object,
        REGEXP_SUBSTR(UPPER(TRIM(f.key::VARCHAR)), '^COL_([0-9]{4})', 1, 1, 'e', 1)                     AS raw_month,
        COALESCE(REGEXP_SUBSTR(UPPER(TRIM(f.key::VARCHAR)), '^COL_[0-9]{4}_([0-9]+)$', 1, 1, 'e', 1), '0') AS col_seq,
        TRY_TO_DOUBLE(NULLIF(TRIM(f.value::VARCHAR), ''))::NUMBER(18,2)                                 AS stock_amount
    FROM amount_bronze,
         LATERAL FLATTEN(INPUT => row_object) AS f
    WHERE REGEXP_LIKE(UPPER(TRIM(f.key::VARCHAR)), '^COL_[0-9]{4}(_[0-9]+)?$')
),

amount_typed AS (
    SELECT
        TRY_TO_DATE('20' || l.raw_month || '01', 'YYYYMMDD')                AS base_month,
        TRIM(GET_IGNORE_CASE(l.row_object, 'material_mch_main')::VARCHAR)   AS material_code,
        TRIM(GET_IGNORE_CASE(l.row_object, 'old_material_code')::VARCHAR)   AS material_code_old,
        TRIM(GET_IGNORE_CASE(l.row_object, 'english_description')::VARCHAR) AS material_desc,
        TRIM(GET_IGNORE_CASE(l.row_object, 'korean_description')::VARCHAR)  AS material_desc_kr,
        TRIM(GET_IGNORE_CASE(l.row_object, 'brand_code')::VARCHAR)          AS brand_code,
        TRIM(GET_IGNORE_CASE(l.row_object, 'product_name')::VARCHAR)        AS brand_name,
        TRIM(GET_IGNORE_CASE(l.row_object, 'product_family')::VARCHAR)      AS brand_group,
        TRIM(GET_IGNORE_CASE(l.row_object, 'ta')::VARCHAR)                  AS therapy_area,
        TRIM(GET_IGNORE_CASE(l.row_object, 'bu')::VARCHAR)                  AS business_unit,
        TRIM(GET_IGNORE_CASE(l.row_object, 'divested_partner')::VARCHAR)    AS material_biz_type_cd,
        c.channel_type,
        l.stock_amount
    FROM amount_long AS l
    LEFT JOIN channel_map AS c
        ON l.col_seq = c.col_seq
),

volume_bronze AS (
    SELECT OBJECT_CONSTRUCT_KEEP_NULL(b.*) AS row_object
    FROM {{ source('bronze', 'stock_in_market_sales_stock') }} AS b
    WHERE b."_sheet_name" = 'Stock vol'
),

volume_long AS (
    SELECT
        row_object,
        REGEXP_SUBSTR(UPPER(TRIM(f.key::VARCHAR)), '^COL_([0-9]{4})', 1, 1, 'e', 1)                     AS raw_month,
        COALESCE(REGEXP_SUBSTR(UPPER(TRIM(f.key::VARCHAR)), '^COL_[0-9]{4}_([0-9]+)$', 1, 1, 'e', 1), '0') AS col_seq,
        TRY_TO_DOUBLE(NULLIF(TRIM(f.value::VARCHAR), ''))::NUMBER(18,2)                                 AS stock_volume
    FROM volume_bronze,
         LATERAL FLATTEN(INPUT => row_object) AS f
    WHERE REGEXP_LIKE(UPPER(TRIM(f.key::VARCHAR)), '^COL_[0-9]{4}(_[0-9]+)?$')
),

volume_typed AS (
    SELECT
        TRY_TO_DATE('20' || l.raw_month || '01', 'YYYYMMDD')                AS base_month,
        TRIM(GET_IGNORE_CASE(l.row_object, 'material_mch_main')::VARCHAR)   AS material_code,
        TRIM(GET_IGNORE_CASE(l.row_object, 'divested_partner')::VARCHAR)    AS material_biz_type_cd,
        c.channel_type,
        l.stock_volume
    FROM volume_long AS l
    LEFT JOIN channel_map AS c
        ON l.col_seq = c.col_seq
),

transformed AS (
    SELECT
        a.base_month,
        IFF(a.material_code = 'Enhertu', '2082 Enhertu', a.material_code)   AS material_code,
        a.material_code_old,
        a.material_desc,
        a.material_desc_kr,
        a.brand_code,
        a.brand_name,
        a.brand_group,
        a.therapy_area,
        a.business_unit,
        a.material_biz_type_cd,
        a.channel_type,
        a.stock_amount,
        v.stock_volume,
        CONVERT_TIMEZONE('UTC', 'Asia/Seoul', SYSDATE())::TIMESTAMP_NTZ(6)  AS _load_dtm
    FROM amount_typed AS a
    LEFT JOIN volume_typed AS v
        ON a.base_month           = v.base_month
       AND a.material_code        = v.material_code
       AND a.channel_type         = v.channel_type
       AND a.material_biz_type_cd = v.material_biz_type_cd
    WHERE a.material_code IS NOT NULL
      AND NOT (
          (a.stock_amount IS NULL OR a.stock_amount = 0)
          AND (v.stock_volume IS NULL OR v.stock_volume = 0)
      )
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
    t.channel_type,
    t.stock_amount,
    t.stock_volume,
    t._load_dtm,
    s._src_synced
FROM transformed AS t
CROSS JOIN src_synced AS s

