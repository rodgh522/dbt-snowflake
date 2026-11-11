{{ config(
    materialized='table',
    alias='emd_co_promo_repr_company'
) }}

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
FROM {{ ref('emd_co_promo_repr_company') }}

