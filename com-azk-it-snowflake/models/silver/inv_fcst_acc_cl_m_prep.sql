{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='base_month'
) }}

WITH src_synced AS (
    SELECT MAX(b."_fivetran_synced"::TIMESTAMP_NTZ(6)) AS _src_synced
    FROM {{ source('bronze', 'fa_data') }} AS b
    WHERE ENDSWITH(LOWER(TRIM(b."#_file")), '.xls')
       OR ENDSWITH(LOWER(TRIM(b."#_file")), '.xlsx')
),

bronze AS (
    SELECT OBJECT_CONSTRUCT_KEEP_NULL(b.*) AS r
    FROM {{ source('bronze', 'fa_data') }} AS b
    WHERE ENDSWITH(LOWER(TRIM(b."#_file")), '.xls')
       OR ENDSWITH(LOWER(TRIM(b."#_file")), '.xlsx')
),

parsed AS (
    SELECT
        TRY_TO_DATE(TRUNC(TRY_TO_DOUBLE(GET_IGNORE_CASE(r, 'month')::VARCHAR))::INTEGER::VARCHAR || '01', 'YYYYMMDD') AS base_month,
        GET_IGNORE_CASE(r, 'market')::VARCHAR                                                       AS market,
        COALESCE(GET_IGNORE_CASE(r, 'selling_sub_region')::VARCHAR,
                 GET_IGNORE_CASE(r, 'sub_region')::VARCHAR)                                         AS selling_sub_region,
        GET_IGNORE_CASE(r, 'main_local_code')::VARCHAR                                              AS material_code,
        GET_IGNORE_CASE(r, 'brand_name')::VARCHAR                                                   AS brand_name,
        GET_IGNORE_CASE(r, 'product_item_name')::VARCHAR                                            AS material_desc,
        COALESCE(GET_IGNORE_CASE(r, 'segmentation')::VARCHAR,
                 GET_IGNORE_CASE(r, 'segment')::VARCHAR)                                            AS segmentation,
        GET_IGNORE_CASE(r, 'pack_class')::VARCHAR                                                   AS pack_class,
        GET_IGNORE_CASE(r, 'category')::VARCHAR                                                     AS category,
        GET_IGNORE_CASE(r, 'usage_type')::VARCHAR                                                   AS usage_type,
        GET_IGNORE_CASE(r, 'lag')::VARCHAR                                                          AS lag,
        GET_IGNORE_CASE(r, 'uom')::VARCHAR                                                          AS uom,
        GET_IGNORE_CASE(r, 'col_3_m_bias_flag')::VARCHAR                                            AS bias_flag_3m,
        GET_IGNORE_CASE(r, 'col_3_m_bias_type')::VARCHAR                                            AS bias_type_3m,
        TRUNC(TRY_TO_DOUBLE(GET_IGNORE_CASE(r, 'col_3_m_bias_flag_age')::VARCHAR))::INTEGER         AS bias_flag_age_3m,
        TRUNC(TRY_TO_DOUBLE(GET_IGNORE_CASE(r, 'esf_volume')::VARCHAR))::INTEGER                    AS esf_volume,
        TRUNC(TRY_TO_DOUBLE(GET_IGNORE_CASE(r, 'sales_volume')::VARCHAR))::INTEGER                  AS sales_volume,
        TRUNC(TRY_TO_DOUBLE(GET_IGNORE_CASE(r, 'forecast_error')::VARCHAR))::INTEGER                AS forecast_error,
        TRY_TO_DECIMAL(GET_IGNORE_CASE(r, 'forecast_error_1')::VARCHAR, 9, 4)                       AS forecast_error_pct,
        TRY_TO_DECIMAL(GET_IGNORE_CASE(r, 'forecast_accuracy')::VARCHAR, 9, 4)                      AS forecast_accuracy_pct,
        TRY_TO_DECIMAL(GET_IGNORE_CASE(r, 'forecast_accuracy_unconst')::VARCHAR, 9, 4)              AS forecast_accuracy_unconst,
        TRUNC(TRY_TO_DOUBLE(GET_IGNORE_CASE(r, 'abs_forecast_error')::VARCHAR))::INTEGER            AS abs_forecast_error,
        TRY_TO_DECIMAL(GET_IGNORE_CASE(r, 'absolute_error_mape')::VARCHAR, 9, 4)                    AS mape,
        TRY_TO_DECIMAL(GET_IGNORE_CASE(r, 'bias_mape')::VARCHAR, 18, 4)                             AS bias_mape,
        TRY_TO_DECIMAL(GET_IGNORE_CASE(r, 'unit_cost_price')::VARCHAR, 18, 2)                       AS unit_cost_price,
        TRY_TO_DECIMAL(GET_IGNORE_CASE(r, 'forecast_error_value')::VARCHAR, 18, 2)                  AS forecast_error_value,
        TRY_TO_DECIMAL(GET_IGNORE_CASE(r, 'sales_value')::VARCHAR, 18, 2)                           AS sales_value_usd,
        TRIM(GET_IGNORE_CASE(r, '#_file')::VARCHAR)                                                 AS file_name
    FROM bronze
),

file_attr AS (
    SELECT
        p.*,
        LOWER(REGEXP_SUBSTR(file_name, '[^/]+$'))                                                   AS file_base,
        REGEXP_SUBSTR(file_name, '(20[0-9]{2})(0[1-9]|1[0-2])')                                     AS file_month,
        LOWER(REGEXP_SUBSTR(file_name, '\\.([[:alnum:]_]+)$', 1, 1, 'e', 1))                        AS file_ext
    FROM parsed AS p
),

candidates AS (
    SELECT
        *,
        CASE
            WHEN file_base = 'fa_data.xlsx'                         THEN 1
            WHEN file_month IS NOT NULL AND file_ext = 'xlsx'       THEN 2
            WHEN file_month IS NOT NULL AND file_ext = 'xls'        THEN 3
        END AS file_priority
    FROM file_attr
    WHERE base_month IS NOT NULL
),

winning_files AS (
    SELECT
        base_month,
        file_name
    FROM (
        SELECT DISTINCT base_month, file_name, file_priority
        FROM candidates
    )
    QUALIFY ROW_NUMBER() OVER (PARTITION BY base_month ORDER BY file_priority, file_name) = 1
),

transformed AS (
    SELECT
        c.base_month,
        c.market,
        c.selling_sub_region,
        c.material_code,
        c.brand_name,
        c.material_desc,
        c.segmentation,
        c.pack_class,
        c.category,
        c.usage_type,
        c.lag,
        c.uom,
        c.bias_flag_3m,
        c.bias_type_3m,
        c.bias_flag_age_3m,
        c.esf_volume,
        c.sales_volume,
        c.forecast_error,
        c.forecast_error_pct,
        c.forecast_accuracy_pct,
        c.forecast_accuracy_unconst,
        c.abs_forecast_error,
        c.mape,
        c.bias_mape,
        c.unit_cost_price,
        c.forecast_error_value,
        c.sales_value_usd,
        CONVERT_TIMEZONE('UTC', 'Asia/Seoul', SYSDATE())::TIMESTAMP_NTZ(6) AS _load_dtm
    FROM candidates AS c
    INNER JOIN winning_files AS w
        ON c.base_month = w.base_month
       AND c.file_name  = w.file_name
)

SELECT
    t.*,
    s._src_synced
FROM transformed AS t
CROSS JOIN src_synced AS s

