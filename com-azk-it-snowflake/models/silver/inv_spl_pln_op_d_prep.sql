{{ config(
    materialized='table'
) }}

WITH bronze AS (
    SELECT
        "_fivetran_synced",
        OBJECT_CONSTRUCT_KEEP_NULL(*) AS r
    FROM {{ source('bronze', 'inventory_supply_plan') }}
),

filtered_src AS (
    SELECT
        "_fivetran_synced",
        r
    FROM bronze
    WHERE REGEXP_REPLACE(
              LOWER(TRIM(REGEXP_SUBSTR(COALESCE(GET_IGNORE_CASE(r, '#_file'),
                                                GET_IGNORE_CASE(r, '_file'))::VARCHAR,
                                       '[^/]+$'))),
              '\\s+', ' ') = 'supply plan.xlsx'
),

src_synced AS (
    SELECT MAX("_fivetran_synced"::TIMESTAMP_NTZ(6)) AS _src_synced
    FROM filtered_src
),

raw AS (
    SELECT
        GET_IGNORE_CASE(r, 'availability_date_requirements_date')::VARCHAR                  AS avail_req_date_raw,
        GET_IGNORE_CASE(r, 'product_number')::VARCHAR                                       AS product_number,
        GET_IGNORE_CASE(r, 'product_short_description')::VARCHAR                            AS product_short_desc,
        GET_IGNORE_CASE(r, 'category_short_text')::VARCHAR                                  AS category_short_text,
        GET_IGNORE_CASE(r, 'receipt_element_requirements_element')::VARCHAR                 AS receipt_req_element,
        TRY_TO_DECIMAL(GET_IGNORE_CASE(r, 'receipt_quantity_requirements_quantity')::VARCHAR, 18, 3) AS receipt_req_qty,
        TRY_TO_DECIMAL(GET_IGNORE_CASE(r, 'confirmed_quantity_atp')::VARCHAR, 18, 3)                 AS confirmed_qty_atp,
        TRY_TO_DECIMAL(GET_IGNORE_CASE(r, 'requested_quantity')::VARCHAR, 18, 3)                     AS requested_qty,
        GET_IGNORE_CASE(r, 'source_location')::VARCHAR                                      AS source_location,
        GET_IGNORE_CASE(r, 'target_location')::VARCHAR                                      AS target_location,
        GET_IGNORE_CASE(r, 'local_product_code_source_location')::VARCHAR                   AS material_code_source_loc,
        GET_IGNORE_CASE(r, 'local_product_code_target_location')::VARCHAR                   AS material_code_target_loc,
        GET_IGNORE_CASE(r, 'supply_source')::VARCHAR                                        AS supply_source,
        TRY_TO_BOOLEAN(GET_IGNORE_CASE(r, 'pp_firmed')::VARCHAR)                            AS pp_firmed,
        GET_IGNORE_CASE(r, 'opening_date')::VARCHAR                                         AS opening_date_raw,
        GET_IGNORE_CASE(r, 'base_unit_of_measure')::VARCHAR                                 AS base_uom,
        GET_IGNORE_CASE(r, 'schedule_line_date')::VARCHAR                                   AS schedule_line_date_raw,
        GET_IGNORE_CASE(r, 'creation_date')::VARCHAR                                        AS creation_date_raw,
        GET_IGNORE_CASE(r, 'delivery_date')::VARCHAR                                        AS delivery_date_raw,
        GET_IGNORE_CASE(r, 'start_date')::VARCHAR                                           AS start_date_raw
    FROM filtered_src
    WHERE GET_IGNORE_CASE(r, 'product_number')::VARCHAR IS NOT NULL
),

transformed AS (
    SELECT
        COALESCE(TRY_TO_DATE(avail_req_date_raw, 'YYYYMMDD'),     TRY_TO_DATE(avail_req_date_raw))     AS avail_req_date,
        product_number,
        product_short_desc,
        category_short_text,
        receipt_req_element,
        receipt_req_qty,
        confirmed_qty_atp,
        requested_qty,
        source_location,
        target_location,
        material_code_source_loc,
        material_code_target_loc,
        supply_source,
        pp_firmed,
        COALESCE(TRY_TO_DATE(opening_date_raw, 'YYYYMMDD'),       TRY_TO_DATE(opening_date_raw))       AS opening_date,
        base_uom,
        COALESCE(TRY_TO_DATE(schedule_line_date_raw, 'YYYYMMDD'), TRY_TO_DATE(schedule_line_date_raw)) AS schedule_line_date,
        COALESCE(TRY_TO_DATE(creation_date_raw, 'YYYYMMDD'),      TRY_TO_DATE(creation_date_raw))      AS creation_date,
        COALESCE(TRY_TO_DATE(delivery_date_raw, 'YYYYMMDD'),      TRY_TO_DATE(delivery_date_raw))      AS delivery_date,
        COALESCE(TRY_TO_DATE(start_date_raw, 'YYYYMMDD'),         TRY_TO_DATE(start_date_raw))         AS start_date,
        CONVERT_TIMEZONE('UTC', 'Asia/Seoul', SYSDATE())::TIMESTAMP_NTZ(6)                             AS _load_dtm
    FROM raw
)

SELECT
    t.avail_req_date,
    t.product_number,
    t.product_short_desc,
    t.category_short_text,
    t.receipt_req_element,
    t.receipt_req_qty,
    t.confirmed_qty_atp,
    t.requested_qty,
    t.source_location,
    t.target_location,
    t.material_code_source_loc,
    t.material_code_target_loc,
    t.supply_source,
    t.pp_firmed,
    t.opening_date,
    t.base_uom,
    t.schedule_line_date,
    t.creation_date,
    t.delivery_date,
    t.start_date,
    t._load_dtm,
    s._src_synced
FROM transformed AS t
CROSS JOIN src_synced AS s

