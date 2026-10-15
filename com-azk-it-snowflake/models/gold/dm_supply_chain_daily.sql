{{ config(
    materialized='table'
) }}

WITH silver_inv AS (
    SELECT * FROM {{ ref('inv_material_master') }}
)

SELECT
    COALESCE(therapy_area, 'UNASSIGNED') AS therapy_area,
    COALESCE(business_unit, 'UNASSIGNED') AS business_unit,
    brand_name,
    COUNT(DISTINCT material_code) AS total_materials,
    AVG(minimum_lot_quantity) AS avg_min_lot_qty,
    MAX(_load_dtm) AS last_processed_at
FROM silver_inv
GROUP BY 1, 2, 3
