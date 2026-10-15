-- ==============================================================================
-- 1. Context Setting
-- ==============================================================================
USE DATABASE COM_AZK_AIBI_WRK;
USE SCHEMA AIBI_SILVER;

-- ==============================================================================
-- 2. Silver Iceberg Table DDL (Managed Iceberg Table on AWS S3 External Volume)
-- ==============================================================================
CREATE ICEBERG TABLE IF NOT EXISTS COM_AZK_AIBI_WRK.AIBI_SILVER.inv_material_master (
    product_code_global                        VARCHAR,
    material_code                              VARCHAR,
    material_desc                              VARCHAR,
    planning_technique                         VARCHAR,
    stock_transfer_horizon                     INTEGER,
    minimum_lot_quantity                       INTEGER,
    gr_processing_time                         VARCHAR,
    minimum_required_shelf_life_for_sale_month INTEGER,
    therapy_area                               VARCHAR,
    business_unit                              VARCHAR,
    brand_code                                 VARCHAR,
    brand_name                                 VARCHAR,
    brand_group                                VARCHAR,
    _load_dtm                                  TIMESTAMP_NTZ(6),
    _src_synced                                TIMESTAMP_NTZ(6)
)
EXTERNAL_VOLUME = 'EXT_VOL_SILVER_S3'
CATALOG = 'SNOWFLAKE'
BASE_LOCATION = 'inv_material_master/';

-- ==============================================================================
-- 3. Stored Procedure DDL (Bronze -> Silver Preprocessing & Quality Verification)
-- ==============================================================================
CREATE OR REPLACE PROCEDURE COM_AZK_AIBI_WRK.AIBI_SILVER.SP_INV_MATERIAL_MASTER()
RETURNS VARCHAR
LANGUAGE SQL
COMMENT='Supply-chain material master full refresh. Bronze kr_material_master + brand from isl_material_master -> Silver inv_material_master. Skips when Bronze _fivetran_synced is unchanged AND isl_material_master was not reloaded since the last load. Run after SP_ISL_MATERIAL_MASTER.'
EXECUTE AS CALLER
AS '
DECLARE
    ambiguous_brand EXCEPTION (-20002, ''Conflicting brand mappings for material_code in isl_material_master; resolve duplicates before load.'');
    empty_batch     EXCEPTION (-20005, ''No rows in kr_material_master batch; target is unchanged.'');
    duplicate_key   EXCEPTION (-20006, ''Duplicate material_code (local_code) in kr_material_master; master would fan out joins. Target is unchanged.'');

    v_inserted      INTEGER DEFAULT 0;
    v_src_synced    TIMESTAMP_NTZ(6);   -- Bronze 현재 _fivetran_synced
    v_tgt_synced    TIMESTAMP_NTZ(6);   -- Silver에 마지막으로 적재된 Bronze _fivetran_synced
    v_ref_load_dtm  TIMESTAMP_NTZ(6);   -- 브랜드 참조 Silver(isl_material_master) 최종 적재시각
    v_tgt_load_dtm  TIMESTAMP_NTZ(6);   -- inv_material_master 최종 적재시각
BEGIN
    USE SCHEMA COM_AZK_AIBI_WRK.AIBI_SILVER;

    ------------------------------------------------------------------
    -- 0. 변경 여부 체크 (Short-Circuit)
    ------------------------------------------------------------------
    SELECT MAX(b."_fivetran_synced"::TIMESTAMP_NTZ(6))
      INTO :v_src_synced
      FROM COM_AZK_AIBI_RAW_MDLS."aibi_bronze"."kr_material_master" AS b;

    SELECT MAX(_src_synced), MAX(_load_dtm)
      INTO :v_tgt_synced, :v_tgt_load_dtm
      FROM COM_AZK_AIBI_WRK.AIBI_SILVER.inv_material_master;

    SELECT MAX(_load_dtm)
      INTO :v_ref_load_dtm
      FROM COM_AZK_AIBI_WRK.AIBI_SILVER.isl_material_master;

    IF (v_src_synced IS NULL) THEN
        RAISE empty_batch;
    END IF;

    IF (v_tgt_synced IS NOT NULL AND v_src_synced = v_tgt_synced
        AND v_ref_load_dtm <= v_tgt_load_dtm) THEN
        RETURN ''SKIPPED | no change (_fivetran_synced='' || v_src_synced
               || '', isl_material_master _load_dtm='' || v_ref_load_dtm || '')'';
    END IF;

    ------------------------------------------------------------------
    -- 1. 브랜드 매핑 충돌 확인 (Exception Handling)
    ------------------------------------------------------------------
    IF (EXISTS (SELECT 1 FROM COM_AZK_AIBI_WRK.AIBI_SILVER.isl_material_master
                GROUP BY material_code
                HAVING COUNT(DISTINCT TO_JSON(OBJECT_CONSTRUCT_KEEP_NULL(''brand_code'', brand_code,
                                                                         ''brand_group'', brand_group))) > 1)) THEN
        RAISE ambiguous_brand;
    END IF;

    ------------------------------------------------------------------
    -- 2. Bronze 파싱 + 브랜드 결합 → 배치 임시 테이블 (격리된 임시 가공)
    ------------------------------------------------------------------
    CREATE OR REPLACE TEMP TABLE batch_inv_material_master AS
    WITH bronze AS (
        SELECT OBJECT_CONSTRUCT_KEEP_NULL(b.*) AS r
        FROM COM_AZK_AIBI_RAW_MDLS."aibi_bronze"."kr_material_master" AS b
    ), source AS (
        SELECT
            GET_IGNORE_CASE(r, ''product_number_global_code'')::VARCHAR                     AS product_code_global,
            GET_IGNORE_CASE(r, ''local_code'')::VARCHAR                                     AS material_code,
            REPLACE(GET_IGNORE_CASE(r, ''product_description'')::VARCHAR, '''''''', '''')        AS material_desc,
            GET_IGNORE_CASE(r, ''planning_technique'')::VARCHAR                             AS planning_technique,
            TRUNC(TRY_TO_DOUBLE(GET_IGNORE_CASE(r,
                ''stock_transfer_horizon_stock_replenishment_lead_time_calendar_day'')::VARCHAR))::INTEGER AS stock_transfer_horizon,
            TRUNC(TRY_TO_DOUBLE(GET_IGNORE_CASE(r, ''minimum_lot_quantity'')::VARCHAR))::INTEGER        AS minimum_lot_quantity,
            GET_IGNORE_CASE(r, ''gr_processing_time_import_testing_lead_time_working_day'')::VARCHAR    AS gr_processing_time,
            TRUNC(TRY_TO_DOUBLE(GET_IGNORE_CASE(r,
                ''minimum_required_shelf_life_for_sale_month'')::VARCHAR))::INTEGER                    AS minimum_required_shelf_life_for_sale_month,
            GET_IGNORE_CASE(r, ''ta'')::VARCHAR                                             AS therapy_area,
            GET_IGNORE_CASE(r, ''business_unit'')::VARCHAR                                  AS business_unit
        FROM bronze
        WHERE GET_IGNORE_CASE(r, ''local_code'')::VARCHAR IS NOT NULL
    ), brand AS (
        SELECT DISTINCT material_code, brand_code, brand_group
        FROM COM_AZK_AIBI_WRK.AIBI_SILVER.isl_material_master
    )
    SELECT
        s.product_code_global,
        s.material_code,
        s.material_desc,
        s.planning_technique,
        s.stock_transfer_horizon,
        s.minimum_lot_quantity,
        s.gr_processing_time,
        s.minimum_required_shelf_life_for_sale_month,
        s.therapy_area,
        s.business_unit,
        b.brand_code,
        INITCAP(SPLIT_PART(s.material_desc, '' '', 1))                        AS brand_name,
        b.brand_group,
        CONVERT_TIMEZONE(''UTC'', ''Asia/Seoul'', SYSDATE())::TIMESTAMP_NTZ(6) AS _load_dtm
    FROM source AS s
    LEFT JOIN brand AS b USING (material_code);

    ------------------------------------------------------------------
    -- 3. 배치 검증 (Full refresh 실패 시 타깃 덮어쓰기 차단)
    ------------------------------------------------------------------
    IF (NOT EXISTS (SELECT 1 FROM batch_inv_material_master)) THEN
        RAISE empty_batch;
    END IF;

    IF (EXISTS (SELECT 1 FROM batch_inv_material_master
                GROUP BY material_code HAVING COUNT(*) > 1)) THEN
        RAISE duplicate_key;
    END IF;

    ------------------------------------------------------------------
    -- 4. Full refresh (_src_synced 기록, 원자적 적재)
    ------------------------------------------------------------------
    INSERT OVERWRITE INTO COM_AZK_AIBI_WRK.AIBI_SILVER.inv_material_master
        (product_code_global, material_code, material_desc, planning_technique, stock_transfer_horizon,
         minimum_lot_quantity, gr_processing_time, minimum_required_shelf_life_for_sale_month,
         therapy_area, business_unit, brand_code, brand_name, brand_group, _load_dtm, _src_synced)
    SELECT product_code_global, material_code, material_desc, planning_technique, stock_transfer_horizon,
           minimum_lot_quantity, gr_processing_time, minimum_required_shelf_life_for_sale_month,
           therapy_area, business_unit, brand_code, brand_name, brand_group, _load_dtm, :v_src_synced
    FROM batch_inv_material_master;

    v_inserted := SQLROWCOUNT;

    DROP TABLE IF EXISTS batch_inv_material_master;

    RETURN ''SUCCESS | overwritten, inserted='' || v_inserted
           || '' (_src_synced='' || v_src_synced || '')'';

EXCEPTION
    WHEN OTHER THEN
        RAISE;
END;
';

-- ==============================================================================
-- 4. Procedure Execution & Verification Test
-- ==============================================================================
-- CALL COM_AZK_AIBI_WRK.AIBI_SILVER.SP_INV_MATERIAL_MASTER();
-- SELECT * FROM COM_AZK_AIBI_WRK.AIBI_SILVER.inv_material_master LIMIT 10;
