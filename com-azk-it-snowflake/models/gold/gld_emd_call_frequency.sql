{{ config(
    materialized='table',
    alias='emd_call_frequency'
) }}

SELECT
    brand_name,
    criteria,
    main_players,
    channel_specialty,
    year,
    mm,
    yyyymmdd,
    visit_call
FROM {{ ref('emd_call_frequency') }}

