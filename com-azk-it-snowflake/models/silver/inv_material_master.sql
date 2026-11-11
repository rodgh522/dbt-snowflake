{{ config(
    materialized='table'
) }}

WITH src_synced AS (
    SELECT MAX(b."_fivetran_synced"::TIMESTAMP_NTZ(6)) AS _src_synced
    FROM {{ source('bronze', 'kr_material_master') }} AS b
),

bronze AS (
    SELECT OBJECT_CONSTRUCT_KEEP_NULL(b.*) AS r
    FROM {{ source('bronze', 'kr_material_master') }} AS b
),

source_parsed AS (
    SELECT
        GET_IGNORE_CASE(r, 'product_number_global_code')::VARCHAR                                                 AS product_code_global,
        GET_IGNORE_CASE(r, 'local_code')::VARCHAR                                                                 AS material_code,
        REPLACE(GET_IGNORE_CASE(r, 'product_description')::VARCHAR, '''', '')                                     AS material_desc,
        GET_IGNORE_CASE(r, 'planning_technique')::VARCHAR                                                         AS planning_technique,
        TRUNC(TRY_TO_DOUBLE(GET_IGNORE_CASE(r,
            'stock_transfer_horizon_stock_replenishment_lead_time_calendar_day')::VARCHAR))::INTEGER              AS stock_transfer_horizon,
        TRUNC(TRY_TO_DOUBLE(GET_IGNORE_CASE(r, 'minimum_lot_quantity')::VARCHAR))::INTEGER                        AS minimum_lot_quantity,
        GET_IGNORE_CASE(r, 'gr_processing_time_import_testing_lead_time_working_day')::VARCHAR                    AS gr_processing_time,
        TRUNC(TRY_TO_DOUBLE(GET_IGNORE_CASE(r,
            'minimum_required_shelf_life_for_sale_month')::VARCHAR))::INTEGER                                     AS minimum_required_shelf_life_for_sale_month,
        GET_IGNORE_CASE(r, 'ta')::VARCHAR                                                                         AS therapy_area,
        GET_IGNORE_CASE(r, 'business_unit')::VARCHAR                                                              AS business_unit
    FROM bronze
    WHERE GET_IGNORE_CASE(r, 'local_code')::VARCHAR IS NOT NULL
),

brand AS (
    SELECT DISTINCT
        material_code,
        brand_code,
        brand_group
    FROM {{ ref('isl_material_master') }}
),

transformed AS (
    SELECT
        s.product_code_global,
        s.material_code,
        s.material_desc,
        s.planning_technique,
        s.stock_transfer_horizon,
        s.minimum_lot_quantity,
        s.gr_processing_time,
        s.minimum_required_shelf_life_for_sale_month,
        s.therapy_area,
        s.business_unit,
        b.brand_code,
        INITCAP(SPLIT_PART(s.material_desc, ' ', 1))                         AS brand_name,
        b.brand_group,
        CONVERT_TIMEZONE('UTC', 'Asia/Seoul', SYSDATE())::TIMESTAMP_NTZ(6)   AS _load_dtm
    FROM source_parsed AS s
    LEFT JOIN brand AS b
        USING (material_code)
)

SELECT
    t.product_code_global,
    t.material_code,
    t.material_desc,
    t.planning_technique,
    t.stock_transfer_horizon,
    t.minimum_lot_quantity,
    t.gr_processing_time,
    t.minimum_required_shelf_life_for_sale_month,
    t.therapy_area,
    t.business_unit,
    t.brand_code,
    t.brand_name,
    t.brand_group,
    t._load_dtm,
    s._src_synced
FROM transformed AS t
CROSS JOIN src_synced AS s

