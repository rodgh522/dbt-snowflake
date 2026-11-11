{{ config(
    materialized='table',
    alias='emd_ubist_monthly_volume'
) }}

SELECT
    brand_name,
    therapy_area,
    bu,
    atc4,
    definition,
    molecule_combination,
    product_name,
    mono_original_generic,
    competitor_list_for_chart,
    corporation,
    gx_original,
    bed_number,
    channel,
    year,
    mm,
    yyyymmdd,
    rx_volume,
    rx_dot_volume
FROM {{ ref('emd_ubist_monthly_volume') }}

