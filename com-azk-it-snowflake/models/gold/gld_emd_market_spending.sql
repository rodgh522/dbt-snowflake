{{ config(
    materialized='table',
    alias='emd_market_spending'
) }}

SELECT
    brand_name,
    criteria,
    main_players,
    year,
    mm,
    yyyymmdd,
    spending_amt
FROM {{ ref('emd_market_spending') }}

