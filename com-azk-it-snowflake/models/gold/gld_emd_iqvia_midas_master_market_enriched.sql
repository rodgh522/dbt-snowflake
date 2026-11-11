{{ config(
    materialized='table',
    alias='emd_iqvia_midas_master_market_enriched'
) }}

SELECT
    therapy_area,
    atc1,
    atc2,
    atc3,
    atc4,
    mnc,
    hospital_retail_split,
    manufacturer,
    international_product_launch_date,
    corporation,
    international_product,
    molecule_combination,
    product_family,
    international_prescription_status,
    international_corporation_analysis_atc,
    innovation_insights,
    year,
    qq,
    value_amt,
    etc_market_detail,
    etc_definition,
    etc_definition2,
    etc_otc_market_detail,
    etc_otc_gmd,
    etc_otc_chart_name
FROM {{ ref('emd_iqvia_midas_master_market_enriched') }}

