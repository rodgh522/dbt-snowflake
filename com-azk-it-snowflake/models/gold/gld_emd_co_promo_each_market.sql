{{ config(
    materialized='table',
    alias='emd_co_promo_each_market'
) }}

SELECT
    channel_specialty,
    brand_name,
    year,
    mm,
    yyyymmdd,
    rx_count,
    rx_share
FROM {{ ref('emd_co_promo_each_market') }}

