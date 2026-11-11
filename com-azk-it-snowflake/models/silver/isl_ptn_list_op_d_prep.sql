{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='base_date'
) }}

WITH src_synced AS (
    SELECT MAX(b."_fivetran_synced"::TIMESTAMP_NTZ(6)) AS _src_synced
    FROM {{ source('bronze', 'patner_list') }} AS b
),

bronze AS (
    SELECT OBJECT_CONSTRUCT_KEEP_NULL(b.*) AS r
    FROM {{ source('bronze', 'patner_list') }} AS b
    QUALIFY TO_DATE(REGEXP_SUBSTR(b."#_file", '\\d{4}-\\d{2}-\\d{2}'))
          = MAX(TO_DATE(REGEXP_SUBSTR(b."#_file", '\\d{4}-\\d{2}-\\d{2}')))
              OVER (PARTITION BY DATE_TRUNC('MONTH', TO_DATE(REGEXP_SUBSTR(b."#_file", '\\d{4}-\\d{2}-\\d{2}'))))
),

transformed AS (
    SELECT
        GET_IGNORE_CASE(r, 'billing_date')::VARCHAR                                         AS base_date,
        GET_IGNORE_CASE(r, 'billing_document_number')::VARCHAR                              AS doc_no,
        GET_IGNORE_CASE(r, 'billing_document_item_number')::VARCHAR                         AS item_no,
        GET_IGNORE_CASE(r, 'customer_code')::VARCHAR                                        AS customer_code,
        GET_IGNORE_CASE(r, 'customer_name')::VARCHAR                                        AS customer_name,
        GET_IGNORE_CASE(r, 'secondary_customer_code')::VARCHAR                              AS indirect_cust_code,
        GET_IGNORE_CASE(r, 'secondary_customer_name')::VARCHAR                              AS indirect_cust_name,
        GET_IGNORE_CASE(r, 'ship_to_code')::VARCHAR                                         AS ship_to_code,
        GET_IGNORE_CASE(r, 'ship_to_name')::VARCHAR                                         AS ship_to_name,
        GET_IGNORE_CASE(r, 'product_code')::VARCHAR                                         AS product_code,
        GET_IGNORE_CASE(r, 'product_name')::VARCHAR                                         AS product_name,
        TRUNC(TRY_TO_DOUBLE(GET_IGNORE_CASE(r, 'sales_quantity')::VARCHAR))::INTEGER        AS sales_qty,
        GET_IGNORE_CASE(r, 'sales_unit')::VARCHAR                                           AS sales_uom,
        TRY_TO_DECIMAL(GET_IGNORE_CASE(r, 'unit_price_net')::VARCHAR, 18, 2)                AS unit_price_exvat,
        TRY_TO_DECIMAL(GET_IGNORE_CASE(r, 'supply_amount_net')::VARCHAR, 18, 2)             AS supply_amount_exvat,
        TRY_TO_DECIMAL(GET_IGNORE_CASE(r, 'vat_amount')::VARCHAR, 18, 2)                    AS vat_amount,
        TRY_TO_DECIMAL(GET_IGNORE_CASE(r, 'sales_amount')::VARCHAR, 18, 2)                  AS sales_amount,
        GET_IGNORE_CASE(r, 'sap_sales_doc_type_code')::VARCHAR                              AS sales_doc_type_code,
        GET_IGNORE_CASE(r, 'sap_sales_doc_type_name')::VARCHAR                              AS sales_doc_type_name,
        CONVERT_TIMEZONE('UTC', 'Asia/Seoul', SYSDATE())::TIMESTAMP_NTZ(6)                  AS _load_dtm
    FROM bronze
    WHERE GET_IGNORE_CASE(r, 'billing_document_number')::VARCHAR IS NOT NULL
)

SELECT
    t.base_date,
    t.doc_no,
    t.item_no,
    t.customer_code,
    t.customer_name,
    t.indirect_cust_code,
    t.indirect_cust_name,
    t.ship_to_code,
    t.ship_to_name,
    t.product_code,
    t.product_name,
    t.sales_qty,
    t.sales_uom,
    t.unit_price_exvat,
    t.supply_amount_exvat,
    t.vat_amount,
    t.sales_amount,
    t.sales_doc_type_code,
    t.sales_doc_type_name,
    t._load_dtm,
    s._src_synced
FROM transformed AS t
CROSS JOIN src_synced AS s

