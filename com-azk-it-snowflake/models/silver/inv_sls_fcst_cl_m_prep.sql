{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='base_month'
) }}

WITH src_synced AS (
    SELECT MAX(b."_fivetran_synced"::TIMESTAMP_NTZ(6)) AS _src_synced
    FROM {{ source('bronze', 'submission_month') }} AS b
),

bronze AS (
    SELECT OBJECT_CONSTRUCT_KEEP_NULL(b.*) AS r
    FROM {{ source('bronze', 'submission_month') }} AS b
),

parsed_base AS (
    SELECT
        r,
        TRY_TO_DATE(TRUNC(TRY_TO_DOUBLE(GET_IGNORE_CASE(r, 'submission_month')::VARCHAR))::INTEGER::VARCHAR || '01',
                    'YYYYMMDD')                                                     AS base_month,
        TRIM(GET_IGNORE_CASE(r, '#_file')::VARCHAR)                                 AS file_name
    FROM bronze
    WHERE GET_IGNORE_CASE(r, 'sku_description')::VARCHAR IS NOT NULL
),

file_attr AS (
    SELECT
        p.*,
        REGEXP_REPLACE(LOWER(REGEXP_SUBSTR(file_name, '[^/]+$')), '\\s+', ' ')      AS file_base,
        REGEXP_SUBSTR(file_name, '(20[0-9]{2})(0[1-9]|1[0-2])')                     AS file_month,
        LOWER(REGEXP_SUBSTR(file_name, '\\.([[:alnum:]_]+)$', 1, 1, 'e', 1))        AS file_ext
    FROM parsed_base AS p
),

candidates AS (
    SELECT
        *,
        CASE
            WHEN file_base = 'submission month.xlsx'            THEN 1
            WHEN file_month IS NOT NULL AND file_ext = 'xlsx'   THEN 2
        END AS file_priority
    FROM file_attr
    WHERE base_month IS NOT NULL
),

winning_files AS (
    SELECT
        base_month,
        file_name
    FROM (
        SELECT DISTINCT base_month, file_name, file_priority
        FROM candidates
    )
    QUALIFY ROW_NUMBER() OVER (PARTITION BY base_month ORDER BY file_priority, file_name) = 1
),

selected AS (
    SELECT
        c.r,
        c.base_month
    FROM candidates AS c
    INNER JOIN winning_files AS w
        ON c.base_month = w.base_month
       AND c.file_name  = w.file_name
),

unpivoted AS (
    SELECT
        s.base_month,
        TRIM(GET_IGNORE_CASE(s.r, 'sku_description')::VARCHAR)                          AS sku_text,
        REGEXP_SUBSTR(UPPER(TRIM(f.key::VARCHAR)), '^COL_([0-9]{6})$', 1, 1, 'e', 1)    AS raw_month,
        f.value::VARCHAR                                                                AS raw_value
    FROM selected AS s,
         LATERAL FLATTEN(INPUT => s.r) AS f
    WHERE REGEXP_LIKE(UPPER(TRIM(f.key::VARCHAR)), '^COL_[0-9]{6}$')
),

parsed_unpivot AS (
    SELECT
        base_month,
        sku_text,
        TRY_TO_DATE(raw_month || '01', 'YYYYMMDD')                                      AS fcst_month,
        TRY_TO_DOUBLE(
            REGEXP_REPLACE(
                TRANSLATE(raw_value, ',' || CHR(160) || CHR(8203) || CHR(65279), ''),
                '\\s', ''))                                                             AS fcst_volume
    FROM unpivoted
),

split AS (
    SELECT
        base_month,
        SPLIT_PART(sku_text, ' ', 1)                                                    AS material_code,
        IFF(CONTAINS(sku_text, ' '),
            SUBSTR(sku_text, LENGTH(SPLIT_PART(sku_text, ' ', 1)) + 2),
            NULL)                                                                       AS material_desc,
        fcst_month,
        fcst_volume
    FROM parsed_unpivot
),

transformed AS (
    SELECT
        base_month,
        material_code,
        material_desc,
        fcst_month,
        fcst_volume,
        IFF(base_month > fcst_month, 0, DATEDIFF('month', base_month, fcst_month) + 1)::INTEGER AS fcst_month_seq,
        IFF(IFF(base_month > fcst_month, 0, DATEDIFF('month', base_month, fcst_month) + 1) >= 1,
            'FCST', 'ACT')                                                                      AS data_type_cd,
        CONVERT_TIMEZONE('UTC', 'Asia/Seoul', SYSDATE())::TIMESTAMP_NTZ(6)                     AS _load_dtm
    FROM split
    WHERE fcst_volume IS NOT NULL
      AND fcst_volume <> 0
)

SELECT
    t.base_month,
    t.material_code,
    t.material_desc,
    t.fcst_month,
    t.fcst_volume,
    t.fcst_month_seq,
    t.data_type_cd,
    t._load_dtm,
    s._src_synced
FROM transformed AS t
CROSS JOIN src_synced AS s

