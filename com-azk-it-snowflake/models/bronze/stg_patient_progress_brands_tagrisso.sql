{{ config(
    materialized='table'
) }}

WITH source_data AS (
    {% if target.name == 'dev' %}
    -- Dev 테스트 환경: 원천 테이블 연결 없이도 컴파일 가능한 안전한 샘플 인메모리 CTE
    SELECT
        1249::NUMBER                                        AS _line,
        '2026-10-19 06:07:37.251 +0900'::TIMESTAMP_LTZ      AS _fivetran_synced,
        'Tagrisso'::TEXT                                    AS brand,
        4::NUMBER                                           AS new_pts,
        'Feb'::TEXT                                         AS month,
        'Oncology'::TEXT                                    AS business_unit,
        25::NUMBER                                          AS year,
        'Top10'::TEXT                                       AS tier,
        'FLAURA'::TEXT                                      AS indication,
        '화순전남대병원'::TEXT                               AS hca,
        'Onco'::TEXT                                        AS ta,
        '25 Q1'::TEXT                                       AS qtr,
        CAST(NULL AS TEXT)                                  AS sub_indication,
        'tagrisso RAW'::TEXT                                AS _sheet_name,
        'General/Patient Progress All Brands.xlsx'::TEXT    AS file_name,
        '2026-10-19 03:12:05.000 +0900'::TIMESTAMP_LTZ      AS _modified
    {% else %}
    SELECT
        "_line"                                             AS _line,
        "_fivetran_synced"                                  AS _fivetran_synced,
        "brand"                                             AS brand,
        "new_pts"                                           AS new_pts,
        "month"                                             AS month,
        "business_unit"                                     AS business_unit,
        "year"                                              AS year,
        "tier"                                              AS tier,
        "indication"                                        AS indication,
        "hca"                                               AS hca,
        "ta"                                                AS ta,
        "qtr"                                               AS qtr,
        "sub_indication"                                    AS sub_indication,
        "_sheet_name"                                       AS _sheet_name,
        "#_file"                                            AS file_name,
        "_modified"                                         AS _modified
    FROM {{ source('bronze', 'patient_progress_brands_tagrisso') }}
    {% endif %}
)

SELECT
    _line::INTEGER                                                      AS _line,
    _fivetran_synced::TIMESTAMP_LTZ                                     AS _fivetran_synced,
    TRIM(brand)::VARCHAR                                                AS brand,
    COALESCE(new_pts, 0)::INTEGER                                       AS new_pts,
    TRIM(month)::VARCHAR                                                AS month,
    TRIM(business_unit)::VARCHAR                                        AS business_unit,
    year::INTEGER                                                       AS year_short,
    CASE 
        WHEN year IS NOT NULL THEN (2000 + year)::INTEGER 
        ELSE NULL 
    END                                                                 AS year_full,
    TRIM(tier)::VARCHAR                                                 AS tier,
    TRIM(indication)::VARCHAR                                           AS indication,
    TRIM(hca)::VARCHAR                                                  AS hospital_name,
    TRIM(ta)::VARCHAR                                                   AS therapy_area,
    TRIM(qtr)::VARCHAR                                                  AS quarter,
    NULLIF(TRIM(sub_indication), '')::VARCHAR                           AS sub_indication,
    TRIM(_sheet_name)::VARCHAR                                          AS sheet_name,
    TRIM(file_name)::VARCHAR                                            AS file_name,
    _modified::TIMESTAMP_LTZ                                            AS _modified,
    CONVERT_TIMEZONE('UTC', 'Asia/Seoul', SYSDATE())::TIMESTAMP_NTZ(6)  AS _load_dtm
FROM source_data
