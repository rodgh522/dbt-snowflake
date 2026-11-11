{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['brand_name', 'criteria', 'main_players', 'year', 'mm']
) }}

WITH unioned_bronze AS (
    SELECT
        'iOAD'                          AS brand_name,
        "criteria_m_krw_"               AS criteria,
        "main_players"                  AS main_players,
        "_modified"::TIMESTAMP_NTZ(6)   AS src_modified,
        OBJECT_CONSTRUCT_KEEP_NULL(*)   AS row_obj
    FROM {{ source('bronze', 'kr_dashboard_call_spending') }}

    UNION ALL

    SELECT
        'PPI'                           AS brand_name,
        "criteria_m_krw_"               AS criteria,
        NULL                            AS main_players,
        "_modified"::TIMESTAMP_NTZ(6)   AS src_modified,
        OBJECT_CONSTRUCT_KEEP_NULL(*)   AS row_obj
    FROM {{ source('bronze', 'kr_dashboard_call_spending_ppi_market_spending') }}

    UNION ALL

    SELECT
        'Respiratory_Spending'          AS brand_name,
        "criteria_m_krw_"               AS criteria,
        NULL                            AS main_players,
        "_modified"::TIMESTAMP_NTZ(6)   AS src_modified,
        OBJECT_CONSTRUCT_KEEP_NULL(*)   AS row_obj
    FROM {{ source('bronze', 'kr_dashboard_call_spending_respiratory_spending') }}

    UNION ALL

    SELECT
        'Statin'                        AS brand_name,
        "criteria_m_krw_"               AS criteria,
        NULL                            AS main_players,
        "_modified"::TIMESTAMP_NTZ(6)   AS src_modified,
        OBJECT_CONSTRUCT_KEEP_NULL(*)   AS row_obj
    FROM {{ source('bronze', 'kr_dashboard_call_spending_stain_market_spending') }}
),

staging AS (
    SELECT
        s.brand_name,
        s.criteria,
        s.main_players,
        SUBSTR(f.key, 2, 4)                                                 AS year,
        SUBSTR(f.key, 7, 2)                                                 AS mm,
        TO_DATE(SUBSTR(f.key, 2, 4) || '-' || SUBSTR(f.key, 7, 2) || '-01') AS yyyymmdd,
        ROUND(f.value::FLOAT, 2)::NUMBER(15,2)                              AS spending_amt,
        s.src_modified
    FROM unioned_bronze AS s,
         LATERAL FLATTEN(INPUT => s.row_obj) AS f
    WHERE REGEXP_LIKE(f.key, '^_[0-9]{4}_[0-9]{2}_[0-9]{2}_t_00_00_00_z$', 'i')
),

deduped AS (
    SELECT
        brand_name,
        criteria,
        main_players,
        year,
        mm,
        yyyymmdd,
        spending_amt
    FROM staging
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY brand_name, criteria, main_players, year, mm
        ORDER BY src_modified DESC NULLS LAST, spending_amt DESC NULLS LAST
    ) = 1
)

SELECT *
FROM deduped

