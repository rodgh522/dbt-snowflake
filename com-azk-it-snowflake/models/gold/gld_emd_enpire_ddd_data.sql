{{ config(
    materialized='table',
    alias='emd_enpire_ddd_data'
) }}

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
FROM {{ ref('emd_enpire_ddd_data') }}

