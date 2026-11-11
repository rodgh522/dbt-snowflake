{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['brand_name', 'channel', 'competitors', 'year', 'mm']
) }}

WITH raw_bronze AS (
    SELECT
        "_sheet_name",
        "_table",
        "unit_trend_k_dot",
        "_line",
        "_modified",
        OBJECT_CONSTRUCT_KEEP_NULL(*) AS row_obj
    FROM {{ source('bronze', 'ubist_monthly_xigduo_sidapvia') }}
    WHERE COALESCE("_fivetran_deleted", FALSE) = FALSE
      AND "unit_trend_k_dot" IS NOT NULL
),

flattened AS (
    SELECT
        t."_sheet_name"                                             AS brand_name,
        t."_table"                                                  AS channel,
        t."unit_trend_k_dot"                                        AS competitors,
        SUBSTR(f.key, 5, 4)                                         AS year,
        SUBSTR(f.key, 10, 2)                                        AS mm,
        TRY_TO_DOUBLE(TRIM(REPLACE(f.value::STRING, ',', '')))      AS rx_dot_volume,
        t."_line"                                                   AS src_line,
        t."_modified"::TIMESTAMP_NTZ(6)                             AS src_modified
    FROM raw_bronze AS t,
         LATERAL FLATTEN(INPUT => t.row_obj) AS f
    WHERE REGEXP_LIKE(f.key, '^col_[0-9]{4}_[0-9]{2}_[0-9]{2}_00_00_00$', 'i')
),

with_month_flag AS (
    SELECT
        y.*,
        MAX(IFF(y.rx_dot_volume IS NOT NULL, 1, 0))
            OVER (PARTITION BY y.brand_name, y.year, y.mm) AS month_has_data
    FROM flattened AS y
),

staging AS (
    SELECT
        x.brand_name,
        x.channel,
        x.competitors,
        x.year,
        x.mm,
        TO_DATE(x.year || '-' || x.mm || '-01')     AS yyyymmdd,
        ROUND(x.rx_dot_volume, 2)::NUMBER(15,2)     AS rx_dot_volume,
        x.src_line,
        x.src_modified
    FROM with_month_flag AS x
    WHERE x.month_has_data = 1
),

deduped AS (
    SELECT
        brand_name,
        channel,
        competitors,
        year,
        mm,
        yyyymmdd,
        rx_dot_volume
    FROM staging
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY brand_name, channel, competitors, year, mm
        ORDER BY src_modified DESC NULLS LAST, src_line DESC
    ) = 1
)

SELECT *
FROM deduped

