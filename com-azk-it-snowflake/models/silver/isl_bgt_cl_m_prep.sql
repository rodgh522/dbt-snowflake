{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='base_month'
) }}

WITH src_synced AS (
    SELECT MAX(b."_fivetran_synced"::TIMESTAMP_NTZ(6)) AS _src_synced
    FROM {{ source('bronze', 'budget_total_revenue_volume') }} AS b
),

bronze AS (
    SELECT OBJECT_CONSTRUCT_KEEP_NULL(b.*) AS r
    FROM {{ source('bronze', 'budget_total_revenue_volume') }} AS b
),

raw_base AS (
    SELECT
        r,
        TRIM(GET_IGNORE_CASE(r, 'material')::VARCHAR)             AS material_code,
        TRIM(GET_IGNORE_CASE(r, 'material_description')::VARCHAR) AS material_desc,
        TRIM(GET_IGNORE_CASE(r, 'product')::VARCHAR)              AS brand_name_raw,
        TRIM(GET_IGNORE_CASE(r, 'product_code')::VARCHAR)         AS brand_code_raw,
        TRIM(GET_IGNORE_CASE(r, 'col_5')::VARCHAR)                AS raw_col_5,
        TRIM(GET_IGNORE_CASE(r, 'col_6')::VARCHAR)                AS raw_col_6,
        TRIM(GET_IGNORE_CASE(r, 'col_7')::VARCHAR)                AS raw_col_7
    FROM bronze
    WHERE COALESCE(GET_IGNORE_CASE(r, '_fivetran_deleted')::BOOLEAN, FALSE) = FALSE
),

base AS (
    SELECT
        r,
        material_code,
        material_desc,
        brand_name_raw,
        brand_code_raw,
        IFF(REGEXP_LIKE(COALESCE(raw_col_5, ''), '^FY[0-9]{2}$'),
            raw_col_5, raw_col_6)                                 AS fiscal_year,
        IFF(REGEXP_LIKE(COALESCE(raw_col_5, ''), '^FY[0-9]{2}$'),
            raw_col_6, raw_col_7)                                 AS value_type,
        IFF(REGEXP_LIKE(COALESCE(raw_col_5, ''), '^FY[0-9]{2}$'),
            raw_col_7, NULL)                                      AS pr_material_code
    FROM raw_base
),

bgt_long AS (
    SELECT
        b.* EXCLUDE r,
        IFF(ENDSWITH(LOWER(TRIM(f.key::VARCHAR)), '_1'), 'volume', 'revenue')       AS measure_name,
        DECODE(LEFT(LOWER(TRIM(f.key::VARCHAR)), 3),
               'jan',1,'feb',2,'mar',3,'apr',4,'may',5,'jun',6,
               'jul',7,'aug',8,'sep',9,'oct',10,'nov',11,'dec',12)                  AS month_no,
        TRY_TO_DOUBLE(NULLIF(REPLACE(TRIM(f.value::VARCHAR), ',', ''), ''))         AS measure_value
    FROM base AS b,
         LATERAL FLATTEN(INPUT => b.r) AS f
    WHERE REGEXP_LIKE(LOWER(TRIM(f.key::VARCHAR)),
                      '^(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)(_1)?$')
),

pivoted AS (
    SELECT
        material_code,
        material_desc,
        brand_name_raw,
        brand_code_raw,
        fiscal_year,
        value_type,
        pr_material_code,
        month_no,
        MAX(IFF(measure_name = 'revenue', measure_value, NULL))::DECIMAL(20,4)      AS amount,
        MAX(IFF(measure_name = 'volume',  measure_value, NULL))::DECIMAL(18,2)      AS volume
    FROM bgt_long
    GROUP BY
        material_code,
        material_desc,
        brand_name_raw,
        brand_code_raw,
        fiscal_year,
        value_type,
        pr_material_code,
        month_no
),

typed AS (
    SELECT
        *,
        2000 + TRY_TO_NUMBER(REGEXP_SUBSTR(UPPER(fiscal_year), 'FY([0-9]{2})', 1, 1, 'e', 1)) AS year_num
    FROM pivoted
    WHERE amount IS NOT NULL
      AND (amount <> 0 OR COALESCE(volume, 0) <> 0)
      AND material_code IS NOT NULL
      AND (pr_material_code IS NULL OR pr_material_code <> 'Total_Product')
),

transformed AS (
    SELECT
        DATE_FROM_PARTS(year_num, month_no, 1)                                                AS base_month,
        material_code,
        material_desc,
        COALESCE(brand_code_raw, REGEXP_SUBSTR(brand_name_raw, '^([0-9]+)\\s+', 1, 1, 'e', 1)) AS brand_code,
        REGEXP_REPLACE(brand_name_raw, '^[0-9]+\\s+', '')                                     AS brand_name,
        fiscal_year,
        value_type,
        pr_material_code,
        year_num,
        amount,
        volume,
        CONVERT_TIMEZONE('UTC', 'Asia/Seoul', SYSDATE())::TIMESTAMP_NTZ(6)                    AS _load_dtm
    FROM typed
)

SELECT
    t.base_month,
    t.material_code,
    t.material_desc,
    t.brand_code,
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

