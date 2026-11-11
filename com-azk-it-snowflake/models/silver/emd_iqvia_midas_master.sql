{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=[
        'therapy_area', 'gmd', 'gmd_sub_mkt', 'atc1', 'atc2', 'atc3', 'atc4',
        'mnc', 'hospital_retail_split', 'manufacturer', 'international_product_launch_date',
        'corporation', 'international_product', 'molecule_combination', 'product_family',
        'international_prescription_status', 'international_corporation_analysis_atc',
        'innovation_insights', 'year', 'qq'
    ]
) }}

WITH raw_bronze AS (
    SELECT
        "therapy_area",
        "atc_1",
        "atc_2",
        "atc_3",
        "atc_4",
        "mnc",
        "hospital_retail_split",
        "manufacturer",
        "international_product_launch_date",
        "corporation",
        "international_product",
        "molecule_combination",
        "international_prescription_status",
        "international_corporation_analysis_atc",
        "innovation_insights",
        "_line",
        "_modified",
        OBJECT_CONSTRUCT_KEEP_NULL(*) AS row_obj
    FROM {{ source('bronze', 'iqvia_midas_raw_master') }}
    WHERE "_sheet_name" = 'TOTAL'
      AND COALESCE("_fivetran_deleted", FALSE) = FALSE
),

staging AS (
    SELECT
        t."therapy_area"                                        AS therapy_area,
        CAST(NULL AS STRING)                                    AS gmd,
        CAST(NULL AS STRING)                                    AS gmd_sub_mkt,
        t."atc_1"                                               AS atc1,
        t."atc_2"                                               AS atc2,
        t."atc_3"                                               AS atc3,
        t."atc_4"                                               AS atc4,
        t."mnc"                                                 AS mnc,
        t."hospital_retail_split"                               AS hospital_retail_split,
        t."manufacturer"                                        AS manufacturer,
        t."international_product_launch_date"::STRING           AS international_product_launch_date,
        t."corporation"                                         AS corporation,
        t."international_product"                               AS international_product,
        t."molecule_combination"                                AS molecule_combination,
        CAST(NULL AS STRING)                                    AS product_family,
        t."international_prescription_status"                   AS international_prescription_status,
        t."international_corporation_analysis_atc"              AS international_corporation_analysis_atc,
        t."innovation_insights"                                 AS innovation_insights,
        SUBSTR(f.key, 5, 4)                                     AS year,
        'Q' || SUBSTR(f.key, 12, 1)                             AS qq,
        f.value::FLOAT                                          AS value_amt,
        t."_line"                                               AS src_line,
        t."_modified"::TIMESTAMP_NTZ(6)                         AS src_modified
    FROM raw_bronze AS t,
         LATERAL FLATTEN(INPUT => t.row_obj) AS f
    WHERE REGEXP_LIKE(f.key, '^col_[0-9]{4}_q_[1-4]$', 'i')
      AND f.value::FLOAT IS NOT NULL
),

deduped AS (
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
    FROM staging
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY
            therapy_area, gmd, gmd_sub_mkt, atc1, atc2, atc3, atc4,
            mnc, hospital_retail_split, manufacturer, international_product_launch_date,
            corporation, international_product, molecule_combination, product_family,
            international_prescription_status, international_corporation_analysis_atc,
            innovation_insights, year, qq
        ORDER BY src_modified DESC NULLS LAST, src_line DESC
    ) = 1
)

SELECT *
FROM deduped

