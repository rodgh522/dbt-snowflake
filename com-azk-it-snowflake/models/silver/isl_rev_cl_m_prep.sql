{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='base_month'
) }}

WITH src_synced AS (
    SELECT MAX(b."_fivetran_synced"::TIMESTAMP_NTZ(6)) AS _src_synced
    FROM {{ source('bronze', 'actual_total_revenue_and_volume') }} AS b
),

bronze AS (
    SELECT OBJECT_CONSTRUCT_KEEP_NULL(b.*) AS r
    FROM {{ source('bronze', 'actual_total_revenue_and_volume') }} AS b
),

unpivoted AS (
    SELECT
        r,
        IFF(ENDSWITH(LOWER(TRIM(f.key::VARCHAR)), '_1'), 'volume', 'revenue')       AS measure_name,
        DECODE(SPLIT_PART(LOWER(TRIM(f.key::VARCHAR)), '_', 1),
               'jan',1,'feb',2,'mar',3,'apr',4,'may',5,'jun',6,
               'jul',7,'aug',8,'sep',9,'oct',10,'nov',11,'dec',12)                  AS month_num,
        TRY_TO_DOUBLE(NULLIF(REPLACE(TRIM(f.value::VARCHAR), ',', ''), ''))         AS measure_value
    FROM bronze,
         LATERAL FLATTEN(INPUT => r) AS f
    WHERE REGEXP_LIKE(LOWER(TRIM(f.key::VARCHAR)),
                      '^(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)(_1)?$')
),

rev_long AS (
    SELECT
        TRIM(GET_IGNORE_CASE(r, 'material')::VARCHAR)                               AS material_code,
        TRIM(GET_IGNORE_CASE(r, 'material_description')::VARCHAR)                   AS material_desc,
        TRIM(GET_IGNORE_CASE(r, 'product')::VARCHAR)                                AS brand_name,
        IFF(REGEXP_LIKE(COALESCE(TRIM(GET_IGNORE_CASE(r, 'fy')::VARCHAR), ''), '^FY[0-9]{2}$'),
            TRIM(GET_IGNORE_CASE(r, 'fy')::VARCHAR),
            TRIM(GET_IGNORE_CASE(r, 'col_4')::VARCHAR))                             AS fiscal_year,
        IFF(REGEXP_LIKE(COALESCE(TRIM(GET_IGNORE_CASE(r, 'fy')::VARCHAR), ''), '^FY[0-9]{2}$'),
            TRIM(GET_IGNORE_CASE(r, 'col_6')::VARCHAR),
            TRIM(GET_IGNORE_CASE(r, 'col_5')::VARCHAR))                             AS value_type,
        IFF(REGEXP_LIKE(COALESCE(TRIM(GET_IGNORE_CASE(r, 'fy')::VARCHAR), ''), '^FY[0-9]{2}$'),
            TRIM(GET_IGNORE_CASE(r, 'col_7')::VARCHAR),
            TRIM(GET_IGNORE_CASE(r, 'col_6')::VARCHAR))                             AS pr_material_code,
        month_num,
        measure_name,
        measure_value
    FROM unpivoted
),

pivoted AS (
    SELECT
        material_code,
        material_desc,
        brand_name,
        fiscal_year,
        value_type,
        pr_material_code,
        month_num,
        MAX(IFF(measure_name = 'revenue', measure_value, NULL))::DECIMAL(20,4)      AS amount,
        MAX(IFF(measure_name = 'volume',  measure_value, NULL))::DECIMAL(18,2)      AS volume
    FROM rev_long
    GROUP BY
        material_code,
        material_desc,
        brand_name,
        fiscal_year,
        value_type,
        pr_material_code,
        month_num
),

typed AS (
    SELECT
        *,
        2000 + TRY_TO_NUMBER(REGEXP_SUBSTR(UPPER(TRIM(fiscal_year)), 'FY([0-9]{2})', 1, 1, 'e', 1)) AS year_num
    FROM pivoted
    WHERE amount IS NOT NULL
      AND (COALESCE(volume, 0) <> 0 OR amount <> 0)
      AND material_code IS NOT NULL
      AND COALESCE(pr_material_code, '') <> 'Total_Product'
),

transformed AS (
    SELECT
        DATE_FROM_PARTS(year_num, month_num, 1)                             AS base_month,
        material_code,
        material_desc,
        brand_name,
        fiscal_year,
        value_type,
        pr_material_code,
        year_num,
        amount,
        volume,
        CONVERT_TIMEZONE('UTC', 'Asia/Seoul', SYSDATE())::TIMESTAMP_NTZ(6)  AS _load_dtm
    FROM typed
    WHERE year_num IS NOT NULL
)

SELECT
    t.base_month,
    t.material_code,
    t.material_desc,
    t.brand_name,
    t.fiscal_year,
    t.value_type,
    t.pr_material_code,
    t.year_num,
    t.amount,
    t.volume,
    t._load_dtm,
    s._src_synced
FROM transformed AS t
CROSS JOIN src_synced AS s

