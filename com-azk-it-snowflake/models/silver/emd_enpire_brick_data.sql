{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['year_mm', 'brand_code', 'brick_code', 'user_prid']
) }}

WITH staging AS (
    SELECT
        "year_month"                        AS year_mm,
        "year_qtr"                          AS year_qtr,
        "year_half"                         AS year_half,
        "brand"                             AS brand,
        "brand_code"                        AS brand_code,
        "brick_code"                        AS brick_code,
        "brick_name"                        AS brick_name,
        "channel"                           AS channel,
        "priority"                          AS priority,
        "bu"                                AS bu,
        "user_prid"                         AS user_prid,
        "user_name"                         AS user_name,
        "dsm_prid"                          AS dsm_prid,
        "dsm_name"                          AS dsm_name,
        "dso_name"                          AS dso_name,
        "current_user"                      AS cur_user,
        "current_user_prid"                 AS cur_user_prid,
        "current_dsm"                       AS cur_dsm,
        "current_dsm_prid"                  AS cur_dsm_prid,
        "current_dso"                       AS cur_dso,
        "current_team"                      AS cur_team,
        "sales_target"::FLOAT               AS sales_target,
        "sales_tracking"::FLOAT             AS sales_tracking,
        "sales_tracking_vol"::FLOAT         AS sales_tracking_vol,
        "achieve_rate_"::FLOAT              AS achieve_rate,
        "g_r_"::FLOAT                       AS growth_rate,
        "m_s_"::FLOAT                       AS m_s,
        "m_s_vol"::FLOAT                    AS m_s_vol,
        "ddd_market"::FLOAT                 AS ddd_market,
        "ddd_market_vol"::FLOAT             AS ddd_market_vol,
        "ddd_az_"::FLOAT                    AS ddd_az,
        "ddd_az_vol"::FLOAT                 AS ddd_az_vol,
        "invoice_projcetion"::FLOAT         AS invoice_projcetion,
        "invoice_projcetion_2014_"::FLOAT   AS invoice_projcetion_2014,
        "invoice_projcetion_2015_"::FLOAT   AS invoice_projcetion_2015,
        "_line"                             AS src_line,
        "_modified"::TIMESTAMP_NTZ(6)       AS src_modified
    FROM {{ source('bronze', 'enpire_kr_brick_data') }}
),

deduped AS (
    SELECT
        year_mm,
        year_qtr,
        year_half,
        brand,
        brand_code,
        brick_code,
        brick_name,
        channel,
        priority,
        bu,
        user_prid,
        user_name,
        dsm_prid,
        dsm_name,
        dso_name,
        cur_user,
        cur_user_prid,
        cur_dsm,
        cur_dsm_prid,
        cur_dso,
        cur_team,
        sales_target,
        sales_tracking,
        sales_tracking_vol,
        achieve_rate,
        growth_rate,
        m_s,
        m_s_vol,
        ddd_market,
        ddd_market_vol,
        ddd_az,
        ddd_az_vol,
        invoice_projcetion,
        invoice_projcetion_2014,
        invoice_projcetion_2015
    FROM staging
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY year_mm, brand_code, brick_code, user_prid
        ORDER BY src_modified DESC NULLS LAST, src_line DESC
    ) = 1
)

SELECT *
FROM deduped

