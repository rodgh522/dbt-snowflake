### Stored Procedure
```sql
CREATE OR REPLACE PROCEDURE COM_AZK_AIBI_WRK.AIBI_SILVER.SP_ISL_PATIENT_PROGRES_BRANDS()
RETURNS VARCHAR
LANGUAGE SQL
COMMENT='Patient progress (new patients) full refresh. Bronze patient_progress_tagrisso + patient_progress_entertu -> Silver inv_patient_progres_brands. Skips when GREATEST of both Bronze _fivetran_synced equals Silver _src_synced.'
EXECUTE AS CALLER
AS '
DECLARE
    empty_source  EXCEPTION (-20005, ''One of patient_progress Bronze sources is empty; full refresh would drop that brand. Target is unchanged.'');
    invalid_month EXCEPTION (-20004, ''base_month could not be resolved from year/month; target is unchanged.'');
    v_inserted     INTEGER DEFAULT 0;
    v_src_synced   TIMESTAMP_NTZ(6);   -- 두 Bronze 소스 _fivetran_synced 중 최신값
    v_tgt_synced   TIMESTAMP_NTZ(6);   -- Silver에 마지막으로 적재된 값
BEGIN
    USE SCHEMA COM_AZK_AIBI_WRK.AIBI_SILVER ;
    ------------------------------------------------------------------
    -- 1. 소스별 존재 확인
    --    (UNION ALL 구조라 한쪽이 비어도 정상 실행 → 해당 브랜드가 통째로 사라지는 것 방지)
    ------------------------------------------------------------------
    IF (NOT EXISTS (SELECT 1 FROM COM_AZK_AIBI_RAW_MDLS."aibi_bronze"."patient_progress_brands_enhertu") 
        OR NOT EXISTS (SELECT 1 FROM COM_AZK_AIBI_RAW_MDLS."aibi_bronze"."patient_progress_brands_tagrisso")) THEN
        RAISE empty_source;
    END IF;
 
    ------------------------------------------------------------------
    -- 1-1. 변경 여부 체크 (두 소스 중 최신 _fivetran_synced vs Silver _src_synced)
    --      재동기화 시각은 항상 직전 Silver 적재 이후이므로,
    --      어느 한쪽만 재동기화되어도 GREATEST 값이 바뀌어 감지됨
    ------------------------------------------------------------------
    v_src_synced := (SELECT GREATEST(
             (SELECT MAX(b."_fivetran_synced"::TIMESTAMP_NTZ(6))
                FROM COM_AZK_AIBI_RAW_MDLS."aibi_bronze"."patient_progress_brands_tagrisso" AS b),
             (SELECT MAX(b."_fivetran_synced"::TIMESTAMP_NTZ(6))
                FROM COM_AZK_AIBI_RAW_MDLS."aibi_bronze"."patient_progress_brands_enhertu" AS b)));
 
    SELECT MAX(_src_synced)
      INTO :v_tgt_synced
      FROM COM_AZK_AIBI_WRK.AIBI_SILVER.isl_patient_progres_brands;
 
    IF (v_tgt_synced IS NOT NULL AND v_src_synced = v_tgt_synced) THEN
        RETURN ''SKIPPED | no change (_fivetran_synced='' || v_src_synced || '')'';
    END IF;
 
    ------------------------------------------------------------------
    -- 2. 파싱 → 배치 임시 테이블
    ------------------------------------------------------------------
    CREATE OR REPLACE TEMP TABLE batch_isl_patient_progress AS
    WITH bronze AS (
        SELECT OBJECT_CONSTRUCT_KEEP_NULL(b.*) AS r
        FROM COM_AZK_AIBI_RAW_MDLS."aibi_bronze"."patient_progress_brands_tagrisso" AS b
        UNION ALL
        SELECT OBJECT_CONSTRUCT_KEEP_NULL(b.*) AS r
        FROM COM_AZK_AIBI_RAW_MDLS."aibi_bronze"."patient_progress_brands_enhertu" AS b
    ), parsed AS (
        SELECT
            GET_IGNORE_CASE(r, ''brand'')::VARCHAR                                AS brand_name,
            GET_IGNORE_CASE(r, ''hca'')::VARCHAR                                  AS hca,
            GET_IGNORE_CASE(r, ''tier'')::VARCHAR                                 AS tier,
            GET_IGNORE_CASE(r, ''indication'')::VARCHAR                           AS indication,
            GET_IGNORE_CASE(r, ''sub_indication'')::VARCHAR                       AS sub_indication,
            TRY_TO_DECIMAL(GET_IGNORE_CASE(r, ''new_pts'')::VARCHAR, 18, 2)       AS new_pts,
            TRUNC(TRY_TO_DOUBLE(GET_IGNORE_CASE(r, ''year'')::VARCHAR))::INTEGER  AS year,
            GET_IGNORE_CASE(r, ''month'')::VARCHAR                                AS month,
            GET_IGNORE_CASE(r, ''qtr'')::VARCHAR                                  AS qtr,
            GET_IGNORE_CASE(r, ''business_unit'')::VARCHAR                        AS business_unit,
            GET_IGNORE_CASE(r, ''ta'')::VARCHAR                                   AS therapy_area,
            GET_IGNORE_CASE(r, ''#_file'')::VARCHAR                                AS _file
        FROM bronze
    )
    SELECT
        -- year: 2자리(25) / 4자리(2025) 모두 허용, month: 대소문자·전체 철자(Jan/JAN/January) 허용
        DATE_FROM_PARTS(
            IFF(year < 100, 2000 + year, year),
            DECODE(UPPER(LEFT(TRIM(month), 3)),
                   ''JAN'',1,''FEB'',2,''MAR'',3,''APR'',4,''MAY'',5,''JUN'',6,
                   ''JUL'',7,''AUG'',8,''SEP'',9,''OCT'',10,''NOV'',11,''DEC'',12),
            1)                                                               AS base_month,
        brand_name, hca, tier, indication, sub_indication, new_pts,
        year, month, qtr, business_unit, therapy_area, _file,
        CONVERT_TIMEZONE(''UTC'', ''Asia/Seoul'', SYSDATE())::TIMESTAMP_NTZ(6)  AS _load_dtm
    FROM parsed
    WHERE brand_name IS NOT NULL;      -- NULL brand만 제외 (빈 문자열은 유지)
 
    ------------------------------------------------------------------
    -- 3. 배치 검증 (Full refresh이므로 실패 시 덮어쓰기 차단)
    ------------------------------------------------------------------
    IF (EXISTS (SELECT 1 FROM batch_isl_patient_progress WHERE base_month IS NULL)) THEN
        RAISE invalid_month;
    END IF;
 
    ------------------------------------------------------------------
    -- 4. Full refresh (_src_synced 기록)
    ------------------------------------------------------------------
    INSERT OVERWRITE INTO COM_AZK_AIBI_WRK.AIBI_SILVER.isl_patient_progres_brands
        (base_month, brand_name, hca, tier, indication, sub_indication, new_pts,
         year, month, qtr, business_unit, therapy_area, _file, _load_dtm, _src_synced)
    SELECT base_month, brand_name, hca, tier, indication, sub_indication, new_pts,
           year, month, qtr, business_unit, therapy_area, _file, _load_dtm, :v_src_synced
    FROM batch_isl_patient_progress;
    v_inserted := SQLROWCOUNT;
 
    DROP TABLE IF EXISTS batch_isl_patient_progress;
 
    RETURN ''SUCCESS | overwritten, inserted='' || v_inserted
           || '' (_src_synced='' || v_src_synced || '')'';
 
EXCEPTION
    WHEN OTHER THEN
        RAISE;
END;
';