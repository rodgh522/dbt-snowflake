{{ config(
    materialized='table'
) }}

WITH src_synced AS (
    SELECT MAX(b."_fivetran_synced"::TIMESTAMP_NTZ(6)) AS _src_synced
    FROM {{ source('bronze', 'kr_stock_reconciliation') }} AS b
),

bronze AS (
    SELECT OBJECT_CONSTRUCT_KEEP_NULL(b.*) AS r
    FROM {{ source('bronze', 'kr_stock_reconciliation') }} AS b
),

parsed AS (
    SELECT
        TRIM(GET_IGNORE_CASE(r, 'plant')::VARCHAR)                                          AS plant,
        TRIM(GET_IGNORE_CASE(r, 'storage_location')::VARCHAR)                               AS storage_location,
        TRIM(GET_IGNORE_CASE(r, 'material')::VARCHAR)                                       AS material_code,
        TRIM(GET_IGNORE_CASE(r, 'description')::VARCHAR)                                    AS material_desc,
        TRIM(GET_IGNORE_CASE(r, 'batch')::VARCHAR)                                          AS batch,
        TRY_TO_DATE(TRIM(GET_IGNORE_CASE(r, 'expiry_date')::VARCHAR), 'YYYYMMDD')           AS expiry_date,
        TRIM(GET_IGNORE_CASE(r, 'status')::VARCHAR)                                         AS stock_status_cd,
        TRUNC(TRY_TO_DOUBLE(TRIM(GET_IGNORE_CASE(r, 'sap_quantity')::VARCHAR)))::INTEGER    AS sap_qty,
        TRUNC(TRY_TO_DOUBLE(TRIM(GET_IGNORE_CASE(r, 'dc_quantity')::VARCHAR)))::INTEGER     AS dc_qty,
        TRUNC(TRY_TO_DOUBLE(TRIM(GET_IGNORE_CASE(r, 'quantity_diff')::VARCHAR)))::INTEGER   AS qty_diff,
        TRIM(GET_IGNORE_CASE(r, 'geonet_wms_sled')::VARCHAR)                                AS geonet_wms_sled,
        TRIM(GET_IGNORE_CASE(r, 'remarks_qty')::VARCHAR)                                    AS remarks_qty,
        TRIM(GET_IGNORE_CASE(r, 'remarks_sled')::VARCHAR)                                   AS remarks_sled
    FROM bronze
    WHERE TRIM(GET_IGNORE_CASE(r, 'material')::VARCHAR) IS NOT NULL
      AND NULLIF(TRIM(GET_IGNORE_CASE(r, 'expiry_date')::VARCHAR), '') IS NOT NULL
      AND TRIM(GET_IGNORE_CASE(r, 'expiry_date')::VARCHAR) <> '0'
),

transformed AS (
    SELECT
        plant,
        storage_location,
        material_code,
        material_desc,
        batch,
        expiry_date,
        stock_status_cd,
        CASE
            WHEN NULLIF(stock_status_cd, '') IS NULL THEN 'Available'
            WHEN stock_status_cd = 'S'               THEN 'Blocked'
            WHEN stock_status_cd = 'X'               THEN 'Under Quarantine'
        END                                                                                 AS stock_status_desc,
        sap_qty,
        dc_qty,
        qty_diff,
        geonet_wms_sled,
        DATEDIFF('day', CONVERT_TIMEZONE('Asia/Seoul', CURRENT_TIMESTAMP())::DATE, expiry_date) AS remaining_shelf_life,
        remarks_qty,
        remarks_sled,
        CONVERT_TIMEZONE('UTC', 'Asia/Seoul', SYSDATE())::TIMESTAMP_NTZ(6)                  AS _load_dtm
    FROM parsed
)

SELECT
    t.plant,
    t.storage_location,
    t.material_code,
    t.material_desc,
    t.batch,
    t.expiry_date,
    t.stock_status_cd,
    t.stock_status_desc,
    t.sap_qty,
    t.dc_qty,
    t.qty_diff,
    t.geonet_wms_sled,
    t.remaining_shelf_life,
    t.remarks_qty,
    t.remarks_sled,
    t._load_dtm,
    s._src_synced
FROM transformed AS t
CROSS JOIN src_synced AS s

