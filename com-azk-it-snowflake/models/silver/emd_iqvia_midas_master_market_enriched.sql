{{ config(
    materialized='table'
) }}

WITH master AS (
    SELECT * FROM {{ ref('emd_iqvia_midas_master') }}
),

code_etc AS (
    SELECT * FROM {{ source('silver_ref', 'IQVIA_MARKET_CODE_ETC') }}
),

code_etc_otc AS (
    SELECT * FROM {{ source('silver_ref', 'IQVIA_MARKET_CODE_ETC_OTC') }}
)

SELECT
    m.therapy_area                              AS therapy_area,
    m.atc1                                      AS atc1,
    m.atc2                                      AS atc2,
    m.atc3                                      AS atc3,
    m.atc4                                      AS atc4,
    m.mnc                                       AS mnc,
    m.hospital_retail_split                     AS hospital_retail_split,
    m.manufacturer                              AS manufacturer,
    m.international_product_launch_date         AS international_product_launch_date,
    m.corporation                               AS corporation,
    m.international_product                     AS international_product,
    m.molecule_combination                      AS molecule_combination,
    m.product_family                            AS product_family,
    m.international_prescription_status         AS international_prescription_status,
    m.international_corporation_analysis_atc    AS international_corporation_analysis_atc,
    m.innovation_insights                       AS innovation_insights,
    m.year                                      AS year,
    m.qq                                        AS qq,
    m.value_amt                                 AS value_amt,
    e.market_detail                             AS etc_market_detail,
    e.definition                                AS etc_definition,
    e.definition2                               AS etc_definition2,
    o.market_detail                             AS etc_otc_market_detail,
    o.gmd                                       AS etc_otc_gmd,
    o.chart_name                                AS etc_otc_chart_name
FROM master AS m
LEFT JOIN code_etc AS e
    ON m.atc4 = e.atc4
   AND (e.international_product = '전체' OR e.international_product = m.international_product)
LEFT JOIN code_etc_otc AS o
    ON m.atc4 = o.atc4
WHERE e.market_detail IS NOT NULL
   OR o.market_detail IS NOT NULL

