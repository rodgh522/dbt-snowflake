{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['brand_name', 'criteria', 'main_players', 'channel_specialty', 'year', 'mm']
) }}

WITH unioned_bronze AS (
    SELECT
        'iOAD'                          AS brand_name,
        "criteria_of_calls_"            AS criteria,
        "main_players"                  AS main_players,
        "channel_specialty"             AS channel_specialty,
        "_modified"::TIMESTAMP_NTZ(6)   AS src_modified,
        OBJECT_CONSTRUCT_KEEP_NULL(*)   AS row_obj
    FROM {{ source('bronze', 'kr_dashboard_call_spending_ioad_market_call_frequency') }}

    UNION ALL

    SELECT
        'PPI'                           AS brand_name,
        "criteria_of_calls_"            AS criteria,
        NULL                            AS main_players,
        "channel_specialty"             AS channel_specialty,
        "_modified"::TIMESTAMP_NTZ(6)   AS src_modified,
        OBJECT_CONSTRUCT_KEEP_NULL(*)   AS row_obj
    FROM {{ source('bronze', 'kr_dashboard_call_spending_ppi_market_call_frequency') }}

    UNION ALL

    SELECT
        'Respiratory'                   AS brand_name,
        "criteria_of_calls_"            AS criteria,
        NULL                            AS main_players,
        "channel_specialty"             AS channel_specialty,
        "_modified"::TIMESTAMP_NTZ(6)   AS src_modified,
        OBJECT_CONSTRUCT_KEEP_NULL(*)   AS row_obj
    FROM {{ source('bronze', 'kr_dashboard_call_spending_respiratory_call_frequency') }}

    UNION ALL

    SELECT
        'Statin'                        AS brand_name,
        "criteria_of_calls_"            AS criteria,
        NULL                            AS main_players,
        "channel_specialty"             AS channel_specialty,
        "_modified"::TIMESTAMP_NTZ(6)   AS src_modified,
        OBJECT_CONSTRUCT_KEEP_NULL(*)   AS row_obj
    FROM {{ source('bronze', 'kr_dashboard_call_spending_stain_market_call_freqeuncy') }}
),

staging AS (
    SELECT
        s.brand_name,
        s.criteria,
        s.main_players,
        s.channel_specialty,
        SUBSTR(f.key, 2, 4)                                                 AS year,
        SUBSTR(f.key, 7, 2)                                                 AS mm,
        TO_DATE(SUBSTR(f.key, 2, 4) || '-' || SUBSTR(f.key, 7, 2) || '-01') AS yyyymmdd,
        TRY_TO_NUMBER(f.value::STRING, 15, 2)                               AS visit_call,
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
        channel_specialty,
        year,
        mm,
        yyyymmdd,
        visit_call
    FROM staging
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY brand_name, criteria, main_players, channel_specialty, year, mm
        ORDER BY src_modified DESC NULLS LAST, visit_call DESC NULLS LAST
    ) = 1
)

SELECT
    brand_name,
    criteria,
    main_players,
    channel_specialty,
    year,
    mm,
    yyyymmdd,
    visit_call
FROM deduped

