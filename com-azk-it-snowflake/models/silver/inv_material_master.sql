{{ config(
    materialized='table'
) }}

WITH bronze AS (
    {% if target.name == 'dev' %}
    -- Dev 테스트 환경: 원천 테이블이 없을 경우를 대비한 안전한 샘플 인메모리 CTE
    SELECT
        OBJECT_CONSTRUCT(
            'product_number_global_code', 'GLOBAL_001',
            'local_code', 'MAT_001',
            'product_description', 'Cardio Medicine 100mg',
            'planning_technique', 'PD',
            'stock_transfer_horizon_stock_replenishment_lead_time_calendar_day', '14',
            'minimum_lot_quantity', '100',
            'gr_processing_time_import_testing_lead_time_working_day', '3',
            'minimum_required_shelf_life_for_sale_month', '12',
            'ta', 'Cardiovascular',
            'business_unit', 'BioPharma',
            '_fivetran_synced', CURRENT_TIMESTAMP()
        ) AS r
    {% else %}
    SELECT OBJECT_CONSTRUCT_KEEP_NULL(b.*) AS r
    FROM {{ source('bronze', 'kr_material_master') }} AS b
    {% endif %}
),

source_cleansed AS (
    SELECT
        GET_IGNORE_CASE(r, 'product_number_global_code')::VARCHAR                     AS product_code_global,
        GET_IGNORE_CASE(r, 'local_code')::VARCHAR                                     AS material_code,
        REPLACE(GET_IGNORE_CASE(r, 'product_description')::VARCHAR, '''', '')        AS material_desc,
        GET_IGNORE_CASE(r, 'planning_technique')::VARCHAR                             AS planning_technique,
        TRUNC(TRY_TO_DOUBLE(GET_IGNORE_CASE(r,
            'stock_transfer_horizon_stock_replenishment_lead_time_calendar_day')::VARCHAR))::INTEGER AS stock_transfer_horizon,
        TRUNC(TRY_TO_DOUBLE(GET_IGNORE_CASE(r, 'minimum_lot_quantity')::VARCHAR))::INTEGER        AS minimum_lot_quantity,
        GET_IGNORE_CASE(r, 'gr_processing_time_import_testing_lead_time_working_day')::VARCHAR    AS gr_processing_time,
        TRUNC(TRY_TO_DOUBLE(GET_IGNORE_CASE(r,
            'minimum_required_shelf_life_for_sale_month')::VARCHAR))::INTEGER                    AS minimum_required_shelf_life_for_sale_month,
        GET_IGNORE_CASE(r, 'ta')::VARCHAR                                             AS therapy_area,
        GET_IGNORE_CASE(r, 'business_unit')::VARCHAR                                  AS business_unit,
        GET_IGNORE_CASE(r, '_fivetran_synced')::TIMESTAMP_NTZ(6)                      AS _src_synced
    FROM bronze
    WHERE GET_IGNORE_CASE(r, 'local_code')::VARCHAR IS NOT NULL
),

brand AS (
    SELECT DISTINCT material_code, brand_code, brand_group
    FROM {{ ref('isl_material_master') }}
)

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
    INITCAP(SPLIT_PART(s.material_desc, ' ', 1))                        AS brand_name,
    b.brand_group,
    CONVERT_TIMEZONE('UTC', 'Asia/Seoul', SYSDATE())::TIMESTAMP_NTZ(6) AS _load_dtm,
    s._src_synced
FROM source_cleansed AS s
LEFT JOIN brand AS b USING (material_code)
