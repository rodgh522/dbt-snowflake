{{ config(
    materialized='table'
) }}

WITH src_synced AS (
    SELECT MAX(b."_fivetran_synced"::TIMESTAMP_NTZ(6)) AS _src_synced
    FROM {{ source('bronze', 'sku_mapping') }} AS b
),

bronze AS (
    SELECT OBJECT_CONSTRUCT_KEEP_NULL(b.*) AS r
    FROM {{ source('bronze', 'sku_mapping') }} AS b
),

transformed AS (
    SELECT
        GET_IGNORE_CASE(r, 'material')::VARCHAR                                             AS material_code,
        COALESCE(GET_IGNORE_CASE(r, 'material_description_new')::VARCHAR,
                 GET_IGNORE_CASE(r, 'material_description_new_')::VARCHAR)                  AS material_desc,
        COALESCE(GET_IGNORE_CASE(r, 'material_description_old')::VARCHAR,
                 GET_IGNORE_CASE(r, 'material_description_old_')::VARCHAR)                  AS material_desc_old,
        GET_IGNORE_CASE(r, 'product_code')::VARCHAR                                         AS brand_code,
        SUBSTR(GET_IGNORE_CASE(r, 'product_name')::VARCHAR, 6)                              AS brand_name,
        GET_IGNORE_CASE(r, 'product_group_1')::VARCHAR                                      AS brand_group,
        GET_IGNORE_CASE(r, 'ta')::VARCHAR                                                   AS therapy_area,
        GET_IGNORE_CASE(r, 'bu')::VARCHAR                                                   AS business_unit,
        TRUNC(TRY_TO_DOUBLE(GET_IGNORE_CASE(r, 'bu_order')::VARCHAR))::INTEGER              AS bu_order,
        TRUNC(TRY_TO_DOUBLE(GET_IGNORE_CASE(r, 'ta_order')::VARCHAR))::INTEGER              AS ta_order,
        TRUNC(TRY_TO_DOUBLE(GET_IGNORE_CASE(r, 'product_group_order')::VARCHAR))::INTEGER   AS product_group_order,
        TRUNC(TRY_TO_DOUBLE(GET_IGNORE_CASE(r, 'product_order')::VARCHAR))::INTEGER         AS product_order,
        CONVERT_TIMEZONE('UTC', 'Asia/Seoul', SYSDATE())::TIMESTAMP_NTZ(6)                  AS _load_dtm
    FROM bronze
    WHERE GET_IGNORE_CASE(r, 'material')::VARCHAR IS NOT NULL
)

SELECT
    t.material_code,
    t.material_desc,
    t.material_desc_old,
    t.brand_code,
    t.brand_name,
    t.brand_group,
    t.therapy_area,
    t.business_unit,
    t.bu_order,
    t.ta_order,
    t.product_group_order,
    t.product_order,
    t._load_dtm,
    s._src_synced
FROM transformed AS t
CROSS JOIN src_synced AS s

