{{ config(
    materialized='table',
    alias='emd_iqvia_midas_master'
) }}

SELECT
    therapy_area,
    gmd,
    gmd_sub_mkt,
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
    value_amt
FROM {{ ref('emd_iqvia_midas_master') }}

