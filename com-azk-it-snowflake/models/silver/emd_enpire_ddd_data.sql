{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['year_mm', 'brick_code', 'azk_brand', 'competitors', 'channel']
) }}

WITH staging AS (
    SELECT
        TO_VARCHAR("year_month")            AS year_mm,
        "brick_code"                        AS brick_code,
        "brick_name"                        AS brick_name,
        "ims_brick_code"::NUMBER(18,0)      AS ims_brick_code,
        "ims_brick_name"                    AS ims_brick_name,
        "azk_brand"                         AS azk_brand,
        "competitors"                       AS competitors,
        "channel"                           AS channel,
        "priority"                          AS priority,
        "team_name"                         AS team_name,
        "dso_name"                          AS dso_name,
        "dsm_name"                          AS dsm_name,
        "dsm_prid"                          AS dsm_prid,
        "user_prid"                         AS user_prid,
        "user_name"                         AS user_name,
        "value"::FLOAT                      AS ddd_value,
        "value_"::FLOAT                     AS value_ms,
        "volume"::FLOAT                     AS ddd_volume,
        "volume_"::FLOAT                    AS volume_ms,
        "_line"                             AS src_line,
        "_modified"::TIMESTAMP_NTZ(6)       AS src_modified
    FROM {{ source('bronze', 'enpire_kr_ddd_crestor') }}
),

deduped AS (
    SELECT
        year_mm,
        brick_code,
        brick_name,
        ims_brick_code,
        ims_brick_name,
        azk_brand,
        competitors,
        channel,
        priority,
        team_name,
        dso_name,
        dsm_name,
        dsm_prid,
        user_prid,
        user_name,
        ddd_value,
        value_ms,
        ddd_volume,
        volume_ms
    FROM staging
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY year_mm, brick_code, azk_brand, competitors, channel
        ORDER BY src_modified DESC NULLS LAST, src_line DESC
    ) = 1
)

SELECT *
FROM deduped

