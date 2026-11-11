{{ config(
    materialized='table',
    alias='emd_ubist_sidapvia_xigduo'
) }}

SELECT
    brand_name,
    channel,
    competitors,
    year,
    mm,
    yyyymmdd,
    rx_dot_volume
FROM {{ ref('emd_ubist_sidapvia_xigduo') }}

