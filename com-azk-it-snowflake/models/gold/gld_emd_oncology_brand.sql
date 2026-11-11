{{ config(
    materialized='table',
    alias='emd_oncology_brand'
) }}

SELECT
    brand_name,
    product_name,
    molecule_desc,
    pack_desc,
    strength,
    nfc_2,
    year,
    mm,
    yyyymmdd,
    units,
    counting_units,
    values_usd,
    values_lc,
    units_growth_py,
    units_market_share,
    values_usd_growth_py,
    values_usd_market_share,
    values_lc_growth_py,
    values_lc_market_share
FROM {{ ref('emd_oncology_brand') }}

