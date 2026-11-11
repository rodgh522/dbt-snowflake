{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=[
        'brand_name', 'therapy_area', 'bu', 'atc4', 'definition',
        'molecule_combination', 'product_name', 'mono_original_generic',
        'competitor_list_for_chart', 'corporation', 'gx_original',
        'bed_number', 'channel', 'year', 'mm'
    ]
) }}

WITH unioned_bronze AS (
    SELECT
        'Crestor'                       AS brand_name,
        "ta"                            AS therapy_area,
        "bu"                            AS bu,
        "atc_4"                         AS atc4,
        "definition_1"                  AS definition,
        "molecule_combination"          AS molecule_combination,
        "product"                       AS product_name,
        "mono_original_vs_mono_generic" AS mono_original_generic,
        "competitor_list_for_chart"     AS competitor_list_for_chart,
        "company"                       AS corporation,
        "gx_v_original"                 AS gx_original,
        "bed_number"                    AS bed_number,
        "channel"                       AS channel,
        "_line"                         AS src_line,
        "_modified"::TIMESTAMP_NTZ(6)   AS src_modified,
        OBJECT_CONSTRUCT_KEEP_NULL(*)   AS row_obj
    FROM {{ source('bronze', 'kr_dashboard_ubist_volume_monthly_bbu_crestor') }}

    UNION ALL

    SELECT
        'Nexium'                        AS brand_name,
        "ta"                            AS therapy_area,
        "bu"                            AS bu,
        "atc_4"                         AS atc4,
        NULL                            AS definition,
        "molecule_combination"          AS molecule_combination,
        "product"                       AS product_name,
        NULL                            AS mono_original_generic,
        "competitor_list_for_chart_1"   AS competitor_list_for_chart,
        "company"                       AS corporation,
        "gx_v_original"                 AS gx_original,
        NULL                            AS bed_number,
        "channel"                       AS channel,
        "_line"                         AS src_line,
        "_modified"::TIMESTAMP_NTZ(6)   AS src_modified,
        OBJECT_CONSTRUCT_KEEP_NULL(*)   AS row_obj
    FROM {{ source('bronze', 'kr_dashboard_ubist_volume_monthly_bbu_nexium') }}

    UNION ALL

    SELECT
        "do_t_volume_by_brand"          AS brand_name,
        NULL                            AS therapy_area,
        NULL                            AS bu,
        NULL                            AS atc4,
        NULL                            AS definition,
        NULL                            AS molecule_combination,
        NULL                            AS product_name,
        NULL                            AS mono_original_generic,
        NULL                            AS competitor_list_for_chart,
        NULL                            AS corporation,
        NULL                            AS gx_original,
        NULL                            AS bed_number,
        "channel"                       AS channel,
        "_line"                         AS src_line,
        "_modified"::TIMESTAMP_NTZ(6)   AS src_modified,
        OBJECT_CONSTRUCT_KEEP_NULL(*)   AS row_obj
    FROM {{ source('bronze', 'kr_dashboard_ubist_volume_monthly_bbu_symbicort_cl') }}

    UNION ALL

    SELECT
        "do_t_volume_by_brand"          AS brand_name,
        NULL                            AS therapy_area,
        NULL                            AS bu,
        NULL                            AS atc4,
        NULL                            AS definition,
        NULL                            AS molecule_combination,
        NULL                            AS product_name,
        NULL                            AS mono_original_generic,
        NULL                            AS competitor_list_for_chart,
        NULL                            AS corporation,
        NULL                            AS gx_original,
        NULL                            AS bed_number,
        "channel"                       AS channel,
        "_line"                         AS src_line,
        "_modified"::TIMESTAMP_NTZ(6)   AS src_modified,
        OBJECT_CONSTRUCT_KEEP_NULL(*)   AS row_obj
    FROM {{ source('bronze', 'kr_dashboard_ubist_volume_monthly_bbu_symbicort_gh') }}

    UNION ALL

    SELECT
        "do_t_volume_by_brand"          AS brand_name,
        NULL                            AS therapy_area,
        NULL                            AS bu,
        NULL                            AS atc4,
        NULL                            AS definition,
        NULL                            AS molecule_combination,
        NULL                            AS product_name,
        NULL                            AS mono_original_generic,
        NULL                            AS competitor_list_for_chart,
        NULL                            AS corporation,
        NULL                            AS gx_original,
        NULL                            AS bed_number,
        "channel"                       AS channel,
        "_line"                         AS src_line,
        "_modified"::TIMESTAMP_NTZ(6)   AS src_modified,
        OBJECT_CONSTRUCT_KEEP_NULL(*)   AS row_obj
    FROM {{ source('bronze', 'kr_dashboard_ubist_volume_monthly_bbu_symbicort_sh') }}
),

flattened AS (
    SELECT
        s.brand_name,
        s.therapy_area,
        s.bu,
        s.atc4,
        s.definition,
        s.molecule_combination,
        s.product_name,
        s.mono_original_generic,
        s.competitor_list_for_chart,
        s.corporation,
        s.gx_original,
        s.bed_number,
        s.channel,
        SPLIT_PART(f.key, '_', 2)                                                                                               AS year,
        DECODE(LOWER(SPLIT_PART(f.key, '_', 1)),
               'january','01','february','02','march','03','april','04','may','05','june','06',
               'july','07','august','08','september','09','october','10','november','11','december','12')                       AS mm,
        s.src_line,
        s.src_modified,
        MAX(IFF(ENDSWITH(LOWER(f.key), '_counting_units_volume_absolute'),   TRY_TO_DOUBLE(TRIM(REPLACE(f.value::STRING, ',', ''))), NULL)) AS rx_volume,
        MAX(IFF(ENDSWITH(LOWER(f.key), '_days_of_therapy_volume_absolute'), TRY_TO_DOUBLE(TRIM(REPLACE(f.value::STRING, ',', ''))), NULL))  AS rx_dot_volume
    FROM unioned_bronze AS s,
         LATERAL FLATTEN(INPUT => s.row_obj) AS f
    WHERE REGEXP_LIKE(f.key, '^(january|february|march|april|may|june|july|august|september|october|november|december)_[0-9]{4}_(counting_units|days_of_therapy)_volume_absolute$', 'i')
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17
),

with_month_flag AS (
    SELECT
        y.*,
        MAX(IFF(y.rx_volume IS NOT NULL OR y.rx_dot_volume IS NOT NULL, 1, 0))
            OVER (PARTITION BY y.brand_name, y.year, y.mm) AS month_has_data
    FROM flattened AS y
),

staging AS (
    SELECT
        x.brand_name,
        x.therapy_area,
        x.bu,
        x.atc4,
        x.definition,
        x.molecule_combination,
        x.product_name,
        x.mono_original_generic,
        x.competitor_list_for_chart,
        x.corporation,
        x.gx_original,
        x.bed_number,
        x.channel,
        x.year,
        x.mm,
        TO_DATE(x.year || '-' || x.mm || '-01') AS yyyymmdd,
        x.rx_volume,
        x.rx_dot_volume,
        x.src_line,
        x.src_modified
    FROM with_month_flag AS x
    WHERE x.month_has_data = 1
),

deduped AS (
    SELECT
        brand_name,
        therapy_area,
        bu,
        atc4,
        definition,
        molecule_combination,
        product_name,
        mono_original_generic,
        competitor_list_for_chart,
        corporation,
        gx_original,
        bed_number,
        channel,
        year,
        mm,
        yyyymmdd,
        rx_volume,
        rx_dot_volume
    FROM staging
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY
            brand_name, therapy_area, bu, atc4, definition,
            molecule_combination, product_name, mono_original_generic,
            competitor_list_for_chart, corporation, gx_original,
            bed_number, channel, year, mm
        ORDER BY src_modified DESC NULLS LAST, src_line DESC
    ) = 1
)

SELECT *
FROM deduped

