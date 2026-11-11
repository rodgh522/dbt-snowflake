{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['corporation', 'channel', 'manufacturer', 'local_brand_name', 'year', 'mm']
) }}

WITH raw_bronze AS (
    SELECT
        "#_file",
        "_line",
        "_sheet_name",
        "priv_hosp",
        "rank",
        "manufacturer",
        "local_brand",
        "_modified",
        OBJECT_CONSTRUCT_KEEP_NULL(*) AS row_obj
    FROM {{ source('bronze', 'azk_co_promotion_company_report') }}
    WHERE UPPER(TRIM("_sheet_name")) IN (
        'ALVOGEN KOREA', 'BLUE PHARM KOREA', 'DAE WOONG', 'DKSH', 'DAIICHISANKYO', 'GC PHARMA',
        'HK INNO.N', 'HYUNDAI PH.', 'IL DONG', 'SK CHEMICALS', 'IL YANG'
    )
      AND COALESCE("_fivetran_deleted", FALSE) = FALSE
),

flattened AS (
    SELECT
        t."#_file"                                                      AS src_file,
        t."_line"                                                       AS src_line,
        UPPER(TRIM(t."_sheet_name"))                                    AS corporation,
        UPPER(TRIM(t."priv_hosp"))                                      AS channel,
        TRY_TO_NUMBER(t."rank"::STRING, 10, 0)                          AS rank,
        t."manufacturer"                                                AS manufacturer,
        t."local_brand"                                                 AS local_brand_name,
        SUBSTR(f.key, 5, 4)                                             AS year,
        SUBSTR(f.key, 10, 2)                                            AS mm,
        MAX(IFF(NOT ENDSWITH(LOWER(f.key), '_1'), f.value::FLOAT, NULL)) AS rx_count,
        MAX(IFF(    ENDSWITH(LOWER(f.key), '_1'), f.value::FLOAT, NULL)) AS rx_share,
        MAX(t."_modified"::TIMESTAMP_NTZ(6))                            AS src_modified
    FROM raw_bronze AS t,
         LATERAL FLATTEN(INPUT => t.row_obj) AS f
    WHERE REGEXP_LIKE(f.key, '^col_[0-9]{4}_[0-9]{2}_[0-9]{2}_00_00_00(_1)?$', 'i')
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
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
        x.corporation,
        x.channel,
        x.rank,
        x.manufacturer,
        x.local_brand_name,
        x.year,
        x.mm,
        TO_DATE(x.year || '-' || x.mm || '-01') AS yyyymmdd,
        ROUND(x.rx_count)::NUMBER(15,0)         AS rx_count,
        ROUND(x.rx_share, 6)::NUMBER(15,6)      AS rx_share,
        x.src_modified
    FROM with_month_flag AS x
    WHERE x.month_has_data = 1
),

deduped AS (
    SELECT
        corporation,
        channel,
        rank,
        manufacturer,
        local_brand_name,
        year,
        mm,
        yyyymmdd,
        rx_count,
        rx_share
    FROM staging
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY corporation, channel, manufacturer, local_brand_name, year, mm
        ORDER BY src_modified DESC NULLS LAST, rx_count DESC NULLS LAST
    ) = 1
)

SELECT
    corporation,
    channel,
    rank,
    manufacturer,
    local_brand_name,
    year,
    mm,
    yyyymmdd,
    rx_count,
    rx_share
FROM deduped

