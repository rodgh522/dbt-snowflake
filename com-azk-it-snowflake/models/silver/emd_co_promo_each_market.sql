{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['channel_specialty', 'brand_name', 'year', 'mm']
) }}

WITH raw_bronze AS (
    SELECT
        "_sheet_name",
        "_modified",
        NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("specialty"::STRING, CHR(12288), ' '), CHR(160), ' '), ' +', ' ')), '')      AS channel_specialty,
        NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("product_name"::STRING, CHR(12288), ' '), CHR(160), ' '), ' +', ' ')), '')   AS brand_name,
        OBJECT_CONSTRUCT_KEEP_NULL(*)                                                                                           AS row_obj
    FROM {{ source('bronze', 'azk_co_promotion_company_report') }}
    WHERE "_sheet_name" IN ('Detail Frequency', 'SOV')
      AND COALESCE("_fivetran_deleted", FALSE) = FALSE
      AND NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("product_name"::STRING, CHR(12288), ' '), CHR(160), ' '), ' +', ' ')), '') IS NOT NULL
),

flattened AS (
    SELECT
        t.channel_specialty,
        t.brand_name,
        SUBSTR(f.key, 5, 4)                                                                                                     AS year,
        SUBSTR(f.key, 10, 2)                                                                                                    AS mm,
        MAX(IFF(t."_sheet_name" = 'Detail Frequency', TRY_TO_DOUBLE(TRIM(REPLACE(f.value::STRING, ',', ''))), NULL))            AS rx_count,
        MAX(IFF(t."_sheet_name" = 'SOV',              TRY_TO_DOUBLE(TRIM(REPLACE(f.value::STRING, ',', ''))), NULL))            AS rx_share,
        MAX(t."_modified"::TIMESTAMP_NTZ(6))                                                                                    AS src_modified
    FROM raw_bronze AS t,
         LATERAL FLATTEN(INPUT => t.row_obj) AS f
    WHERE REGEXP_LIKE(f.key, '^col_[0-9]{4}_[0-9]{2}_[0-9]{2}_00_00_00(_1)?$', 'i')
    GROUP BY 1, 2, 3, 4
),

with_month_flag AS (
    SELECT
        y.*,
        MAX(IFF(y.rx_count IS NOT NULL OR y.rx_share IS NOT NULL, 1, 0))
            OVER (PARTITION BY y.year, y.mm) AS month_has_data
    FROM flattened AS y
),

staging AS (
    SELECT
        x.channel_specialty,
        x.brand_name,
        x.year,
        x.mm,
        TO_DATE(x.year || '-' || x.mm || '-01') AS yyyymmdd,
        ROUND(x.rx_count, 2)::NUMBER(15,2)      AS rx_count,
        ROUND(x.rx_share, 6)::NUMBER(15,6)      AS rx_share
    FROM with_month_flag AS x
    WHERE x.month_has_data = 1
)

{% if is_incremental() %}
SELECT
    s.channel_specialty,
    s.brand_name,
    s.year,
    s.mm,
    s.yyyymmdd,
    COALESCE(s.rx_count, t.rx_count) AS rx_count,
    COALESCE(s.rx_share, t.rx_share) AS rx_share
FROM staging AS s
LEFT JOIN {{ this }} AS t
    ON  t.channel_specialty IS NOT DISTINCT FROM s.channel_specialty
    AND t.brand_name        IS NOT DISTINCT FROM s.brand_name
    AND t.year              IS NOT DISTINCT FROM s.year
    AND t.mm                IS NOT DISTINCT FROM s.mm
{% else %}
SELECT
    channel_specialty,
    brand_name,
    year,
    mm,
    yyyymmdd,
    rx_count,
    rx_share
FROM staging
{% endif %}

