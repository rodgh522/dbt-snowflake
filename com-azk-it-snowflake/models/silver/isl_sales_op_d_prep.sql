{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='base_date'
) }}

WITH src_synced AS (
    SELECT MAX(b."_fivetran_synced"::TIMESTAMP_NTZ(6)) AS _src_synced
    FROM {{ source('bronze', 'kr_10_mtd') }} AS b
),

bronze AS (
    SELECT OBJECT_CONSTRUCT_KEEP_NULL(b.*) AS r
    FROM {{ source('bronze', 'kr_10_mtd') }} AS b
),

transformed AS (
    SELECT
        COALESCE(TRY_TO_DATE(GET_IGNORE_CASE(r, 'doc_date')::VARCHAR, 'YYYYMMDD'),
                 TRY_TO_DATE(GET_IGNORE_CASE(r, 'doc_date')::VARCHAR))                          AS base_date,
        TRIM(GET_IGNORE_CASE(r, 'sold_to_pt')::VARCHAR)                                         AS sold_to_pt,
        TRIM(GET_IGNORE_CASE(r, 'item')::VARCHAR)                                               AS item_no,
        TRIM(GET_IGNORE_CASE(r, 'name_1')::VARCHAR)                                             AS customer_name,
        TRIM(GET_IGNORE_CASE(r, 'ship_to')::VARCHAR)                                            AS ship_to,
        TRIM(GET_IGNORE_CASE(r, 'material')::VARCHAR)                                           AS material_code,
        TRIM(GET_IGNORE_CASE(r, 'description')::VARCHAR)                                        AS material_desc,
        TRY_TO_DECIMAL(REPLACE(GET_IGNORE_CASE(r, 'actnetqty')::VARCHAR, ',', ''), 15, 2)       AS actual_net_qty,
        TRUNC(TRY_TO_DOUBLE(REPLACE(GET_IGNORE_CASE(r, 'net_price')::VARCHAR, ',', '')))::INTEGER AS net_price,
        TRY_TO_DECIMAL(REPLACE(GET_IGNORE_CASE(r, 'actnetval')::VARCHAR, ',', ''), 18, 2)       AS actual_net_value,
        TRY_TO_DECIMAL(REPLACE(GET_IGNORE_CASE(r, 'net_value')::VARCHAR, ',', ''), 18, 2)       AS net_value,
        TRIM(GET_IGNORE_CASE(r, 'po_number')::VARCHAR)                                          AS po_number,
        TRIM(GET_IGNORE_CASE(r, 'saty')::VARCHAR)                                               AS sales_doc_type,
        CONVERT_TIMEZONE('UTC', 'Asia/Seoul', SYSDATE())::TIMESTAMP_NTZ(6)                      AS _load_dtm
    FROM bronze
    WHERE TRIM(GET_IGNORE_CASE(r, 'material')::VARCHAR) IS NOT NULL
)

SELECT
    t.base_date,
    t.sold_to_pt,
    t.item_no,
    t.customer_name,
    t.ship_to,
    t.material_code,
    t.material_desc,
    t.actual_net_qty,
    t.net_price,
    t.actual_net_value,
    t.net_value,
    t.po_number,
    t.sales_doc_type,
    t._load_dtm,
    s._src_synced
FROM transformed AS t
CROSS JOIN src_synced AS s

