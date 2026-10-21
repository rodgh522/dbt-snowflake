{{ config(
    materialized='table'
) }}

WITH enhertu AS (
    SELECT * FROM {{ ref('stg_patient_progress_brands_enhertu') }}
),

tagrisso AS (
    SELECT * FROM {{ ref('stg_patient_progress_brands_tagrisso') }}
),

unified AS (
    SELECT * FROM enhertu
    UNION ALL
    SELECT * FROM tagrisso
)

SELECT
    _line,
    _fivetran_synced,
    brand,
    new_pts,
    month,
    business_unit,
    year_short,
    year_full,
    tier,
    indication,
    hospital_name,
    therapy_area,
    quarter,
    sub_indication,
    sheet_name,
    file_name,
    _modified,
    _load_dtm
FROM unified
