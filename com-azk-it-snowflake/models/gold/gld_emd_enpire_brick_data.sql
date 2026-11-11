{{ config(
    materialized='table',
    alias='emd_enpire_brick_data'
) }}

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
FROM {{ ref('emd_enpire_brick_data') }}

