{{ config(
    materialized='table'
) }}

-- Silver 브랜드 참조 테이블 모델
SELECT
    'MAT_001' AS material_code,
    'BRD_01'  AS brand_code,
    'GRP_A'   AS brand_group,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(6) AS _load_dtm
