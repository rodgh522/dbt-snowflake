{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['brand_name', 'product_name', 'molecule_desc', 'pack_desc', 'strength', 'nfc_2', 'year', 'mm']
) }}

WITH unioned_bronze AS (
    SELECT
        'Enhertu'                                                                                                               AS brand_name,
        NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("product_name"::STRING, CHR(12288), ' '), CHR(160), ' '), ' +', ' ')), '')   AS product_name,
        NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("molecule_desc"::STRING, CHR(12288), ' '), CHR(160), ' '), ' +', ' ')), '')  AS molecule_desc,
        CAST(NULL AS STRING)                                                                                                    AS pack_desc,
        CAST(NULL AS STRING)                                                                                                    AS strength,
        CAST(NULL AS STRING)                                                                                                    AS nfc_2,
        1                                                                                                                       AS units_ok,
        "_modified"::TIMESTAMP_NTZ(6)                                                                                           AS src_modified,
        OBJECT_CONSTRUCT_KEEP_NULL(*)                                                                                           AS row_obj
    FROM {{ source('bronze', 'kr_dashboard_oncology_brand_performance_enhertu_monthly') }}
    WHERE NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("product_name"::STRING, CHR(12288), ' '), CHR(160), ' '), ' +', ' ')), '') IS NOT NULL
      AND UPPER(COALESCE(NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("molecule_desc"::STRING, CHR(12288), ' '), CHR(160), ' '), ' +', ' ')), ''), '')) <> 'GRAND TOTAL'

    UNION ALL

    SELECT
        'Tagrisso'                                                                                                              AS brand_name,
        NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("product_name"::STRING, CHR(12288), ' '), CHR(160), ' '), ' +', ' ')), '')   AS product_name,
        NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("molecule_desc"::STRING, CHR(12288), ' '), CHR(160), ' '), ' +', ' ')), '')  AS molecule_desc,
        CAST(NULL AS STRING)                                                                                                    AS pack_desc,
        CAST(NULL AS STRING)                                                                                                    AS strength,
        CAST(NULL AS STRING)                                                                                                    AS nfc_2,
        1                                                                                                                       AS units_ok,
        "_modified"::TIMESTAMP_NTZ(6)                                                                                           AS src_modified,
        OBJECT_CONSTRUCT_KEEP_NULL(*)                                                                                           AS row_obj
    FROM {{ source('bronze', 'kr_dashboard_oncology_brand_performance_tagrisso_monthly_updated') }}
    WHERE NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("product_name"::STRING, CHR(12288), ' '), CHR(160), ' '), ' +', ' ')), '') IS NOT NULL
      AND UPPER(COALESCE(NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("molecule_desc"::STRING, CHR(12288), ' '), CHR(160), ' '), ' +', ' ')), ''), '')) <> 'GRAND TOTAL'

    UNION ALL

    SELECT
        'Imfinzi'                                                                                                               AS brand_name,
        NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("product_name"::STRING, CHR(12288), ' '), CHR(160), ' '), ' +', ' ')), '')   AS product_name,
        NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("molecule_desc"::STRING, CHR(12288), ' '), CHR(160), ' '), ' +', ' ')), '')  AS molecule_desc,
        CAST(NULL AS STRING)                                                                                                    AS pack_desc,
        CAST(NULL AS STRING)                                                                                                    AS strength,
        CAST(NULL AS STRING)                                                                                                    AS nfc_2,
        1                                                                                                                       AS units_ok,
        "_modified"::TIMESTAMP_NTZ(6)                                                                                           AS src_modified,
        OBJECT_CONSTRUCT_KEEP_NULL(*)                                                                                           AS row_obj
    FROM {{ source('bronze', 'kr_dashboard_oncology_brand_performance_lmfinzi_monthly') }}
    WHERE NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("product_name"::STRING, CHR(12288), ' '), CHR(160), ' '), ' +', ' ')), '') IS NOT NULL
      AND UPPER(COALESCE(NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("molecule_desc"::STRING, CHR(12288), ' '), CHR(160), ' '), ' +', ' ')), ''), '')) <> 'GRAND TOTAL'

    UNION ALL

    SELECT
        'Lynparza'                                                                                                              AS brand_name,
        NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("product_name"::STRING, CHR(12288), ' '), CHR(160), ' '), ' +', ' ')), '')   AS product_name,
        NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("molecule_desc"::STRING, CHR(12288), ' '), CHR(160), ' '), ' +', ' ')), '')  AS molecule_desc,
        CAST(NULL AS STRING)                                                                                                    AS pack_desc,
        NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("strength"::STRING, CHR(12288), ' '), CHR(160), ' '), ' +', ' ')), '')       AS strength,
        NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("nfc_2"::STRING, CHR(12288), ' '), CHR(160), ' '), ' +', ' ')), '')          AS nfc_2,
        1                                                                                                                       AS units_ok,
        "_modified"::TIMESTAMP_NTZ(6)                                                                                           AS src_modified,
        OBJECT_CONSTRUCT_KEEP_NULL(*)                                                                                           AS row_obj
    FROM {{ source('bronze', 'kr_dashboard_oncology_brand_performance_lynparza_monthly') }}
    WHERE NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("product_name"::STRING, CHR(12288), ' '), CHR(160), ' '), ' +', ' ')), '') IS NOT NULL
      AND UPPER(COALESCE(NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("molecule_desc"::STRING, CHR(12288), ' '), CHR(160), ' '), ' +', ' ')), ''), '')) <> 'GRAND TOTAL'

    UNION ALL

    SELECT
        'Zoladex'                                                                                                               AS brand_name,
        NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("product_name"::STRING, CHR(12288), ' '), CHR(160), ' '), ' +', ' ')), '')   AS product_name,
        NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("molecule_desc"::STRING, CHR(12288), ' '), CHR(160), ' '), ' +', ' ')), '')  AS molecule_desc,
        NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("pack_desc"::STRING, CHR(12288), ' '), CHR(160), ' '), ' +', ' ')), '')     AS pack_desc,
        CAST(NULL AS STRING)                                                                                                    AS strength,
        CAST(NULL AS STRING)                                                                                                    AS nfc_2,
        0                                                                                                                       AS units_ok,
        "_modified"::TIMESTAMP_NTZ(6)                                                                                           AS src_modified,
        OBJECT_CONSTRUCT_KEEP_NULL(*)                                                                                           AS row_obj
    FROM {{ source('bronze', 'kr_dashboard_oncology_brand_performance_zoldadex_monthly') }}
    WHERE NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("product_name"::STRING, CHR(12288), ' '), CHR(160), ' '), ' +', ' ')), '') IS NOT NULL
      AND UPPER(COALESCE(NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("molecule_desc"::STRING, CHR(12288), ' '), CHR(160), ' '), ' +', ' ')), ''), '')) <> 'GRAND TOTAL'

    UNION ALL

    SELECT
        'Zoladex'                                                                                                               AS brand_name,
        NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("product_name"::STRING, CHR(12288), ' '), CHR(160), ' '), ' +', ' ')), '')   AS product_name,
        NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("molecule_desc"::STRING, CHR(12288), ' '), CHR(160), ' '), ' +', ' ')), '')  AS molecule_desc,
        NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("pack_desc"::STRING, CHR(12288), ' '), CHR(160), ' '), ' +', ' ')), '')     AS pack_desc,
        CAST(NULL AS STRING)                                                                                                    AS strength,
        CAST(NULL AS STRING)                                                                                                    AS nfc_2,
        1                                                                                                                       AS units_ok,
        "_modified"::TIMESTAMP_NTZ(6)                                                                                           AS src_modified,
        OBJECT_CONSTRUCT_KEEP_NULL(*)                                                                                           AS row_obj
    FROM {{ source('bronze', 'kr_dashboard_oncology_brand_performance_zoladex_volume_updated') }}
    WHERE NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("product_name"::STRING, CHR(12288), ' '), CHR(160), ' '), ' +', ' ')), '') IS NOT NULL
      AND UPPER(COALESCE(NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("molecule_desc"::STRING, CHR(12288), ' '), CHR(160), ' '), ' +', ' ')), ''), '')) <> 'GRAND TOTAL'
),

flattened AS (
    SELECT
        s.brand_name,
        s.product_name,
        s.molecule_desc,
        s.pack_desc,
        s.strength,
        s.nfc_2,
        s.units_ok,
        s.src_modified,
        REGEXP_SUBSTR(f.key, '^(units|counting_units|values_usd|values_lc|units_growth_py|units_market_share|values_usd_growth_py|values_usd_market_share|values_lc_growth_py|values_lc_market_share)_([0-9]{1,2})_([0-9]{4})$', 1, 1, 'e', 1)                      AS metric,
        LPAD(REGEXP_SUBSTR(f.key, '^(units|counting_units|values_usd|values_lc|units_growth_py|units_market_share|values_usd_growth_py|values_usd_market_share|values_lc_growth_py|values_lc_market_share)_([0-9]{1,2})_([0-9]{4})$', 1, 1, 'e', 2), 2, '0')         AS mm,
        REGEXP_SUBSTR(f.key, '^(units|counting_units|values_usd|values_lc|units_growth_py|units_market_share|values_usd_growth_py|values_usd_market_share|values_lc_growth_py|values_lc_market_share)_([0-9]{1,2})_([0-9]{4})$', 1, 1, 'e', 3)                      AS year,
        TRY_TO_DOUBLE(TRIM(REPLACE(REPLACE(f.value::STRING, ',', ''), '%', '')))                                                                                                                                                                                    AS v
    FROM unioned_bronze AS s,
         LATERAL FLATTEN(INPUT => s.row_obj) AS f
    WHERE REGEXP_LIKE(f.key, '^(units|counting_units|values_usd|values_lc|units_growth_py|units_market_share|values_usd_growth_py|values_usd_market_share|values_lc_growth_py|values_lc_market_share)_([0-9]{1,2})_([0-9]{4})$', 'i')
),

pivoted AS (
    SELECT
        z.brand_name,
        z.product_name,
        z.molecule_desc,
        z.pack_desc,
        z.strength,
        z.nfc_2,
        z.year,
        z.mm,
        MAX(IFF(z.units_ok = 1 AND z.metric = 'units', z.v, NULL))                  AS units,
        MAX(IFF(z.units_ok = 1 AND z.metric = 'counting_units', z.v, NULL))         AS counting_units,
        MAX(IFF(z.metric = 'values_usd', z.v, NULL))                                AS values_usd,
        MAX(IFF(z.metric = 'values_lc', z.v, NULL))                                 AS values_lc,
        MAX(IFF(z.metric = 'units_growth_py', z.v, NULL))                           AS units_growth_py,
        MAX(IFF(z.metric = 'units_market_share', z.v, NULL))                        AS units_market_share,
        MAX(IFF(z.metric = 'values_usd_growth_py', z.v, NULL))                      AS values_usd_growth_py,
        MAX(IFF(z.metric = 'values_usd_market_share', z.v, NULL))                   AS values_usd_market_share,
        MAX(IFF(z.metric = 'values_lc_growth_py', z.v, NULL))                       AS values_lc_growth_py,
        MAX(IFF(z.metric = 'values_lc_market_share', z.v, NULL))                    AS values_lc_market_share
    FROM flattened AS z
    GROUP BY
        z.brand_name,
        z.product_name,
        z.molecule_desc,
        z.pack_desc,
        z.strength,
        z.nfc_2,
        z.year,
        z.mm
)

SELECT
    x.brand_name,
    x.product_name,
    x.molecule_desc,
    x.pack_desc,
    x.strength,
    x.nfc_2,
    x.year,
    x.mm,
    TO_DATE(x.year || '-' || x.mm || '-01')             AS yyyymmdd,
    ROUND(x.units)::NUMBER(18,0)                        AS units,
    ROUND(x.counting_units)::NUMBER(18,0)               AS counting_units,
    ROUND(x.values_usd)::NUMBER(18,0)                   AS values_usd,
    ROUND(x.values_lc)::NUMBER(18,0)                    AS values_lc,
    ROUND(x.units_growth_py, 6)::NUMBER(18,6)           AS units_growth_py,
    ROUND(x.units_market_share, 6)::NUMBER(18,6)        AS units_market_share,
    ROUND(x.values_usd_growth_py, 6)::NUMBER(18,6)      AS values_usd_growth_py,
    ROUND(x.values_usd_market_share, 6)::NUMBER(18,6)   AS values_usd_market_share,
    ROUND(x.values_lc_growth_py, 6)::NUMBER(18,6)       AS values_lc_growth_py,
    ROUND(x.values_lc_market_share, 6)::NUMBER(18,6)    AS values_lc_market_share
FROM pivoted AS x
WHERE x.units IS NOT NULL
   OR x.counting_units IS NOT NULL
   OR x.values_usd IS NOT NULL
   OR x.values_lc IS NOT NULL
   OR x.units_growth_py IS NOT NULL
   OR x.units_market_share IS NOT NULL
   OR x.values_usd_growth_py IS NOT NULL
   OR x.values_usd_market_share IS NOT NULL
   OR x.values_lc_growth_py IS NOT NULL
   OR x.values_lc_market_share IS NOT NULL

