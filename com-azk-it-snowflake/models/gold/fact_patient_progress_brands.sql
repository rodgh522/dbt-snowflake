{{ config(
    materialized='table'
) }}

WITH silver_source AS (
    SELECT * FROM {{ ref('isl_patient_progress_brands') }}
)

SELECT
    base_month,
    brand_name,
    hca,
    -- 'others' 표기 통일 (대소문자·공백 차이 포함)
    IFF(LOWER(TRIM(tier)) = 'others', 'Others', tier)                   AS tier,
    indication,
    sub_indication,
    business_unit,
    therapy_area,
    TRUNC(new_pts)::INTEGER                                             AS cnt_new_patients,
    CONVERT_TIMEZONE('UTC', 'Asia/Seoul', SYSDATE())::TIMESTAMP_NTZ(6) AS _load_dtm
FROM silver_source
