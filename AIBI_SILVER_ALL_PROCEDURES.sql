

-- [01] SP_AIBI_BATCH_CHECK
-- ----------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE "SP_AIBI_BATCH_CHECK"("P_RUN_ID" VARCHAR DEFAULT null)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT='AIBI daily batch finalizer: raise if any job FAILED in the run'
EXECUTE AS CALLER
AS '
DECLARE
    v_run_id  VARCHAR;
    v_failed  INTEGER DEFAULT 0;
    v_jobs    VARCHAR;
    batch_failed EXCEPTION (-20010, ''AIBI daily batch has FAILED jobs. See AIBI_SILVER.aibi_batch_log.'');
BEGIN
    v_run_id := P_RUN_ID;
    IF (v_run_id IS NULL) THEN
        BEGIN
            SELECT SYSTEM$TASK_RUNTIME_INFO(''CURRENT_TASK_GRAPH_RUN_GROUP_ID'') INTO :v_run_id;
        EXCEPTION
            WHEN OTHER THEN v_run_id := NULL;
        END;
    END IF;
    IF (v_run_id IS NULL) THEN
        SELECT run_id INTO :v_run_id
        FROM COM_AZK_AIBI_WRK.AIBI_SILVER.aibi_batch_log
        ORDER BY log_dtm DESC
        LIMIT 1;
    END IF;

    SELECT COUNT(*), LISTAGG(domain || '':'' || layer || '':'' || job_name, '', '')
      INTO :v_failed, :v_jobs
      FROM COM_AZK_AIBI_WRK.AIBI_SILVER.aibi_batch_log
     WHERE run_id = :v_run_id AND status = ''FAILED'';

    IF (v_failed > 0) THEN
        -- (선택) 메일 알림: Notification Integration 생성 후 사용
        -- CALL SYSTEM$SEND_EMAIL(''<integration>'', ''<to>'', ''[AIBI] 일배치 실패'',
        --      ''run_id='' || :v_run_id || '' / '' || :v_jobs);
        RAISE batch_failed;
    END IF;

    RETURN ''OK | no failed jobs, run_id='' || v_run_id;
END;
';


-- [02] SP_AIBI_BATCH_LOG
-- ----------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE "SP_AIBI_BATCH_LOG"("P_RUN_ID" VARCHAR, "P_BASE_DATE" DATE, "P_DOMAIN" VARCHAR, "P_LAYER" VARCHAR, "P_JOB" VARCHAR, "P_STATUS" VARCHAR, "P_MSG" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
BEGIN
    INSERT INTO COM_AZK_AIBI_WRK.AIBI_SILVER.aibi_batch_log
        (run_id, base_date, domain, layer, job_name, status, message, log_dtm)
    SELECT :P_RUN_ID, :P_BASE_DATE, :P_DOMAIN, :P_LAYER, :P_JOB, :P_STATUS, LEFT(:P_MSG, 4000),
           CONVERT_TIMEZONE(''UTC'', ''Asia/Seoul'', SYSDATE())::TIMESTAMP_NTZ(6);
    RETURN ''OK'';
END;
';


-- [03] SP_AIBI_DOMAIN_BATCH
-- ----------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE "SP_AIBI_DOMAIN_BATCH"("P_DOMAIN" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT='AIBI domain daily batch: run Silver jobs then Gold jobs (single transaction) for one domain (ISL/INV). Change detection is inside each procedure. Config: aibi_batch_job_config'
EXECUTE AS CALLER
AS '
DECLARE
    invalid_domain EXCEPTION (-20011, ''Unknown domain or no enabled jobs in aibi_batch_job_config (use ISL or INV).'');
    v_domain     VARCHAR;
    v_base       DATE;
    v_run_id     VARCHAR;
    v_job        VARCHAR DEFAULT ''(init)'';
    v_proc       VARCHAR;
    v_dep        VARCHAR;
    v_sql        VARCHAR;
    v_res        VARCHAR;
    v_err        VARCHAR;
    v_status     VARCHAR;
    v_cnt        INTEGER DEFAULT 0;
    v_dep_failed INTEGER DEFAULT 0;
    v_s_run      INTEGER DEFAULT 0;
    v_s_skip     INTEGER DEFAULT 0;
    v_s_fail     INTEGER DEFAULT 0;
    v_g_run      INTEGER DEFAULT 0;
    v_g_skip     INTEGER DEFAULT 0;
    v_g_result   VARCHAR DEFAULT ''OK'';
BEGIN
    v_domain := UPPER(TRIM(P_DOMAIN));
    v_base   := CONVERT_TIMEZONE(''Asia/Seoul'', CURRENT_TIMESTAMP())::DATE;   -- 실행일(KST)

    SELECT COUNT(*) INTO :v_cnt
    FROM COM_AZK_AIBI_WRK.AIBI_SILVER.aibi_batch_job_config
    WHERE domain = :v_domain AND enabled_yn = ''Y'';
    IF (v_cnt = 0) THEN
        RAISE invalid_domain;
    END IF;

    -- run_id: Task 그래프 실행 ID (ISL/INV/Finalizer 공유), 수동 실행이면 UUID
    BEGIN
        SELECT SYSTEM$TASK_RUNTIME_INFO(''CURRENT_TASK_GRAPH_RUN_GROUP_ID'') INTO :v_run_id;
    EXCEPTION
        WHEN OTHER THEN v_run_id := NULL;
    END;
    v_run_id := COALESCE(v_run_id, UUID_STRING());

    ------------------------------------------------------------------
    -- 1. Silver: 순차 실행 (개별 실패해도 계속 진행)
    ------------------------------------------------------------------
    LET rs_silver RESULTSET := (
        SELECT job_name, proc_name, depends_on
        FROM COM_AZK_AIBI_WRK.AIBI_SILVER.aibi_batch_job_config
        WHERE domain = :v_domain AND layer = ''SILVER'' AND enabled_yn = ''Y''
        ORDER BY run_seq);
    LET c_silver CURSOR FOR rs_silver;

    FOR job IN c_silver DO
        v_job  := job.job_name;
        v_proc := job.proc_name;
        v_dep  := job.depends_on;

        BEGIN
            -- (1) 선행 작업 실패 여부 (같은 run_id 안에서, 영역 무관)
            v_dep_failed := 0;
            IF (v_dep IS NOT NULL) THEN
                SELECT COUNT(*) INTO :v_dep_failed
                FROM COM_AZK_AIBI_WRK.AIBI_SILVER.aibi_batch_log
                WHERE run_id = :v_run_id AND job_name = :v_dep AND status = ''FAILED'';
            END IF;

            IF (v_dep_failed > 0) THEN
                CALL COM_AZK_AIBI_WRK.AIBI_SILVER.SP_AIBI_BATCH_LOG(
                    :v_run_id, :v_base, :v_domain, ''SILVER'', :v_job, ''SKIPPED'', ''Upstream job failed: '' || :v_dep);
                v_s_skip := v_s_skip + 1;
            ELSE
                -- (2) 실행: 변경 감지는 프로시저 내부에서 수행
                v_sql := ''CALL '' || v_proc || ''()'';
                EXECUTE IMMEDIATE :v_sql;
                SELECT $1 INTO :v_res FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

                IF (STARTSWITH(v_res, ''SKIPPED'')) THEN
                    v_status := ''SKIPPED'';
                    v_s_skip := v_s_skip + 1;
                ELSE
                    v_status := ''SUCCESS'';
                    v_s_run := v_s_run + 1;
                END IF;
                CALL COM_AZK_AIBI_WRK.AIBI_SILVER.SP_AIBI_BATCH_LOG(
                    :v_run_id, :v_base, :v_domain, ''SILVER'', :v_job, :v_status, :v_res);
            END IF;

        EXCEPTION
            WHEN OTHER THEN
                v_err := SQLERRM;
                CALL COM_AZK_AIBI_WRK.AIBI_SILVER.SP_AIBI_BATCH_LOG(
                    :v_run_id, :v_base, :v_domain, ''SILVER'', :v_job, ''FAILED'', :v_err);
                v_s_fail := v_s_fail + 1;
        END;
    END FOR;

    ------------------------------------------------------------------
    -- 2. Gold: 영역 단위 단일 트랜잭션 (항상 실행, SKIP 판단은 각 Gold 프로시저)
    --    ※ Silver 결과로 Gold 실행 여부를 막지 않음
    --      - Gold 자체가 _load_dtm 비교로 빠르게 SKIP
    --      - 재고 Gold는 SKIP 시에도 remaining_shelf_life를 매일 갱신해야 함
    --    작업별 로그도 트랜잭션에 포함 → 실패 시 함께 롤백되고 FAILED 1건만 남음
    ------------------------------------------------------------------
    LET rs_gold RESULTSET := (
        SELECT job_name, proc_name
        FROM COM_AZK_AIBI_WRK.AIBI_SILVER.aibi_batch_job_config
        WHERE domain = :v_domain AND layer = ''GOLD'' AND enabled_yn = ''Y''
        ORDER BY run_seq);
    LET c_gold CURSOR FOR rs_gold;

    v_job := ''(gold init)'';
    BEGIN
        BEGIN TRANSACTION;

        FOR job IN c_gold DO
            v_job  := job.job_name;
            v_proc := job.proc_name;
            v_sql  := ''CALL '' || v_proc || ''()'';
            EXECUTE IMMEDIATE :v_sql;
            SELECT $1 INTO :v_res FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

            IF (STARTSWITH(v_res, ''SKIPPED'')) THEN
                v_status := ''SKIPPED'';
                v_g_skip := v_g_skip + 1;
            ELSE
                v_status := ''SUCCESS'';
                v_g_run := v_g_run + 1;
            END IF;
            CALL COM_AZK_AIBI_WRK.AIBI_SILVER.SP_AIBI_BATCH_LOG(
                :v_run_id, :v_base, :v_domain, ''GOLD'', :v_job, :v_status, :v_res);
        END FOR;

        COMMIT;

    EXCEPTION
        WHEN OTHER THEN
            v_err := SQLERRM;
            ROLLBACK;
            CALL COM_AZK_AIBI_WRK.AIBI_SILVER.SP_AIBI_BATCH_LOG(
                :v_run_id, :v_base, :v_domain, ''GOLD'', :v_job, ''FAILED'',
                ''All '' || :v_domain || '' Gold changes rolled back. '' || :v_err);
            v_g_result := ''FAILED at '' || v_job || '' (rolled back)'';
            v_g_run  := 0;
            v_g_skip := 0;
    END;

    -- 실패가 있어도 RAISE 하지 않음 (다음 영역 Task 실행 보장). 최종 판정은 Finalizer
    RETURN v_domain || '' | SILVER executed='' || v_s_run || '', skipped='' || v_s_skip || '', failed='' || v_s_fail
        || '' | GOLD '' || v_g_result || '', executed='' || v_g_run || '', skipped='' || v_g_skip
        || '' | run_id='' || v_run_id;
END;
';


-- [04] SP_BATCH_EMD_CALL
-- ----------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE "SP_BATCH_EMD_CALL"()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
BEGIN

CALL COM_AZK_AIBI_WRK.AIBI_SILVER.SP_LOAD_ENPIRE_BRICK_DATA();
CALL COM_AZK_AIBI_WRK.AIBI_SILVER.SP_LOAD_CALL_FREQUENCY();
CALL COM_AZK_AIBI_WRK.AIBI_SILVER.SP_LOAD_ENPIRE_DDD_DATA();
CALL COM_AZK_AIBI_WRK.AIBI_SILVER.SP_LOAD_IQVIA_MIDAS_MASTER();
CALL COM_AZK_AIBI_WRK.AIBI_SILVER.SP_LOAD_IQVIA_MIDAS_MASTER_MARKET_ENRICHED();
CALL COM_AZK_AIBI_WRK.AIBI_SILVER.SP_LOAD_MARKET_SPENDING();
CALL COM_AZK_AIBI_WRK.AIBI_SILVER.SP_LOAD_UBIST_MONTHLY_VOLUME();
CALL COM_AZK_AIBI_WRK.AIBI_SILVER.SP_LOAD_CO_PROMO_REPR_COMPANY();
CALL COM_AZK_AIBI_WRK.AIBI_SILVER.SP_LOAD_ONCOLOGY_BRAND();
CALL COM_AZK_AIBI_WRK.AIBI_SILVER.SP_LOAD_NPP_RAMA_TRACKER();
CALL COM_AZK_AIBI_WRK.AIBI_SILVER.SP_LOAD_UBIST_SIDAPVIA_XIGDUO();
CALL COM_AZK_AIBI_WRK.AIBI_SILVER.SP_LOAD_CO_PROMO_EACH_MARKET();

END;
';


-- [05] SP_INV_FCST_ACC_CL_M_PREP
-- ----------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE "SP_INV_FCST_ACC_CL_M_PREP"()
RETURNS VARCHAR
LANGUAGE SQL
COMMENT='Forecast accuracy (FA) Bronze fa_data -> Silver inv_fcst_acc_cl_m_prep. Per base_month source priority: FA_Data.xlsx(1) > FA_YYYYMM.xlsx(2) > FA_YYYYMM.xls(3). Skips when Bronze _fivetran_synced (xls/xlsx files) equals Silver _src_synced.'
EXECUTE AS CALLER
AS '
DECLARE
    ambiguous_source  EXCEPTION (-20001, ''Multiple files with the same top priority for one base_month; resolve source ambiguity.'');
    invalid_file_name EXCEPTION (-20002, ''File name is neither FA_Data.xlsx nor FA_YYYYMM.(xlsx|xls); source priority cannot be resolved. Target is unchanged.'');
    invalid_month     EXCEPTION (-20004, ''base_month could not be resolved from month column; target is unchanged.'');
    empty_batch       EXCEPTION (-20005, ''No rows in forecast accuracy batch; target is unchanged.'');
    v_deleted      INTEGER DEFAULT 0;
    v_inserted     INTEGER DEFAULT 0;
    v_src_synced   TIMESTAMP_NTZ(6);   -- Bronze 현재 _fivetran_synced (xls/xlsx 파일)
    v_tgt_synced   TIMESTAMP_NTZ(6);   -- Silver에 마지막으로 적재된 Bronze _fivetran_synced
BEGIN
    USE SCHEMA COM_AZK_AIBI_WRK.AIBI_SILVER ;
    ------------------------------------------------------------------
    -- 0. 변경 여부 체크 (Bronze _fivetran_synced vs Silver _src_synced)
                                                                 
    ------------------------------------------------------------------
    SELECT MAX(b."_fivetran_synced"::TIMESTAMP_NTZ(6))
      INTO :v_src_synced
      FROM COM_AZK_AIBI_RAW_MDLS.aibi_bronze."fa_data" AS b
     WHERE ENDSWITH(LOWER(TRIM(b."#_file")), ''.xls'')
        OR ENDSWITH(LOWER(TRIM(b."#_file")), ''.xlsx'');

    SELECT MAX(_src_synced)
      INTO :v_tgt_synced
      FROM COM_AZK_AIBI_WRK.AIBI_SILVER.inv_fcst_acc_cl_m_prep;

    IF (v_src_synced IS NULL) THEN
        RAISE empty_batch;
    END IF;
    IF (v_tgt_synced IS NOT NULL AND v_src_synced = v_tgt_synced) THEN
        RETURN ''SKIPPED | no change (_fivetran_synced='' || v_src_synced || '')'';
    END IF;

    ------------------------------------------------------------------
    -- 1. 후보 스테이징: 파싱 + 파일 유형/우선순위 산출
    ------------------------------------------------------------------
    CREATE OR REPLACE TEMP TABLE sftran_inv_fcst_acc_candidates AS
    WITH bronze AS (
        SELECT OBJECT_CONSTRUCT_KEEP_NULL(b.*) AS r
        FROM COM_AZK_AIBI_RAW_MDLS.aibi_bronze."fa_data" AS b
        WHERE ENDSWITH(LOWER(TRIM(b."#_file")), ''.xls'')
           OR ENDSWITH(LOWER(TRIM(b."#_file")), ''.xlsx'')
    ), parsed AS (
        SELECT
                                                                                   
            TRY_TO_DATE(TRUNC(TRY_TO_DOUBLE(GET_IGNORE_CASE(r, ''month'')::VARCHAR))::INTEGER::VARCHAR || ''01'', ''YYYYMMDD'') AS base_month,
            GET_IGNORE_CASE(r, ''market'')::VARCHAR                       AS market,
            COALESCE(GET_IGNORE_CASE(r, ''selling_sub_region'')::VARCHAR,
                    GET_IGNORE_CASE(r, ''sub_region'')::VARCHAR)       AS selling_sub_region,            
            GET_IGNORE_CASE(r, ''main_local_code'')::VARCHAR              AS material_code,
            GET_IGNORE_CASE(r, ''brand_name'')::VARCHAR                   AS brand_name,
            GET_IGNORE_CASE(r, ''product_item_name'')::VARCHAR            AS material_desc,
            COALESCE(GET_IGNORE_CASE(r, ''segmentation'')::VARCHAR,
                    GET_IGNORE_CASE(r, ''segment'')::VARCHAR)       AS segmentation,                
            GET_IGNORE_CASE(r, ''pack_class'')::VARCHAR                   AS pack_class,
            GET_IGNORE_CASE(r, ''category'')::VARCHAR                     AS category,
            GET_IGNORE_CASE(r, ''usage_type'')::VARCHAR                   AS usage_type,
            GET_IGNORE_CASE(r, ''lag'')::VARCHAR                          AS lag,
            GET_IGNORE_CASE(r, ''uom'')::VARCHAR                          AS uom,
            GET_IGNORE_CASE(r, ''col_3_m_bias_flag'')::VARCHAR            AS bias_flag_3m,
            GET_IGNORE_CASE(r, ''col_3_m_bias_type'')::VARCHAR            AS bias_type_3m,
            TRUNC(TRY_TO_DOUBLE(GET_IGNORE_CASE(r, ''col_3_m_bias_flag_age'')::VARCHAR))::INTEGER AS bias_flag_age_3m,
            TRUNC(TRY_TO_DOUBLE(GET_IGNORE_CASE(r, ''esf_volume'')::VARCHAR))::INTEGER           AS esf_volume,
            TRUNC(TRY_TO_DOUBLE(GET_IGNORE_CASE(r, ''sales_volume'')::VARCHAR))::INTEGER         AS sales_volume,
            TRUNC(TRY_TO_DOUBLE(GET_IGNORE_CASE(r, ''forecast_error'')::VARCHAR))::INTEGER       AS forecast_error,
            TRY_TO_DECIMAL(GET_IGNORE_CASE(r, ''forecast_error_1'')::VARCHAR, 9, 4)              AS forecast_error_pct,
            TRY_TO_DECIMAL(GET_IGNORE_CASE(r, ''forecast_accuracy'')::VARCHAR, 9, 4)             AS forecast_accuracy_pct,
            TRY_TO_DECIMAL(GET_IGNORE_CASE(r, ''forecast_accuracy_unconst'')::VARCHAR, 9, 4)     AS forecast_accuracy_unconst,
            TRUNC(TRY_TO_DOUBLE(GET_IGNORE_CASE(r, ''abs_forecast_error'')::VARCHAR))::INTEGER   AS abs_forecast_error,
            TRY_TO_DECIMAL(GET_IGNORE_CASE(r, ''absolute_error_mape'')::VARCHAR, 9, 4)           AS mape,
            TRY_TO_DECIMAL(GET_IGNORE_CASE(r, ''bias_mape'')::VARCHAR, 18, 4)                    AS bias_mape,
            TRY_TO_DECIMAL(GET_IGNORE_CASE(r, ''unit_cost_price'')::VARCHAR, 18, 2)              AS unit_cost_price,
            TRY_TO_DECIMAL(GET_IGNORE_CASE(r, ''forecast_error_value'')::VARCHAR, 18, 2)         AS forecast_error_value,
            TRY_TO_DECIMAL(GET_IGNORE_CASE(r, ''sales_value'')::VARCHAR, 18, 2)                  AS sales_value_usd,
            TRIM(GET_IGNORE_CASE(r, ''#_file'')::VARCHAR)                                        AS file_name
        FROM bronze
    ), file_attr AS (
        SELECT p.*,
            LOWER(REGEXP_SUBSTR(file_name, ''[^/]+$''))                                      AS file_base,   -- 경로 제거한 파일명
            REGEXP_SUBSTR(file_name, ''(20[0-9]{2})(0[1-9]|1[0-2])'')                        AS file_month,  -- FA_Data는 NULL
            LOWER(REGEXP_SUBSTR(file_name, ''\\\\.([[:alnum:]_]+)$'', 1, 1, ''e'', 1))            AS file_ext
        FROM parsed AS p
    )
    SELECT *,
        CASE
            WHEN file_base = ''fa_data.xlsx''                          THEN 1   -- 고정명, 다월 파일
            WHEN file_month IS NOT NULL AND file_ext = ''xlsx''        THEN 2   -- FA_YYYYMM.xlsx
            WHEN file_month IS NOT NULL AND file_ext = ''xls''         THEN 3   -- FA_YYYYMM.xls
        END AS file_priority
    FROM file_attr
    WHERE base_month IS NOT NULL;

    ------------------------------------------------------------------
    -- 2. 후보 검증
    ------------------------------------------------------------------
    -- 규칙에 맞지 않는 파일명 → 우선순위 판단 불가
    IF (EXISTS (SELECT 1 FROM sftran_inv_fcst_acc_candidates
                WHERE file_priority IS NULL)) THEN
        RAISE invalid_file_name;
    END IF;

    -- 같은 base_month에서 최상위 우선순위 파일이 2개 이상이면 모호
    -- (예: FA_202608.xlsx와 FA_202609.xlsx 둘 다 base_month=202608 행을 포함)
    IF (EXISTS (
            SELECT 1
            FROM (
                SELECT base_month, file_name, file_priority,
                       MIN(file_priority) OVER (PARTITION BY base_month) AS top_priority
                FROM (SELECT DISTINCT base_month, file_name, file_priority
                      FROM sftran_inv_fcst_acc_candidates)
            )
            WHERE file_priority = top_priority
            GROUP BY base_month
            HAVING COUNT(DISTINCT file_name) > 1
        )) THEN
        RAISE ambiguous_source;
    END IF;

    ------------------------------------------------------------------
    -- 3. base_month별 최우선 파일 선택 → 배치 임시 테이블
    ------------------------------------------------------------------
    CREATE OR REPLACE TEMP TABLE batch_inv_fcst_acc AS
    WITH winning_files AS (
        SELECT base_month, file_name
        FROM (SELECT DISTINCT base_month, file_name, file_priority
              FROM sftran_inv_fcst_acc_candidates)
        QUALIFY ROW_NUMBER() OVER (PARTITION BY base_month
                                   ORDER BY file_priority, file_name) = 1
    )
    SELECT r.* EXCLUDE (file_name, file_base, file_month, file_ext, file_priority),
           CONVERT_TIMEZONE(''UTC'', ''Asia/Seoul'', SYSDATE())::TIMESTAMP_NTZ(6) AS _load_dtm
    FROM sftran_inv_fcst_acc_candidates AS r
    JOIN winning_files AS w
      ON r.base_month = w.base_month
     AND r.file_name  = w.file_name;

    ------------------------------------------------------------------
    -- 4. 배치 검증
    ------------------------------------------------------------------
    IF (NOT EXISTS (SELECT 1 FROM batch_inv_fcst_acc)) THEN
        RAISE empty_batch;
    END IF;
                                                                                                    
    IF (EXISTS (SELECT 1 FROM batch_inv_fcst_acc WHERE base_month IS NULL)) THEN
        RAISE invalid_month;
    END IF;

    ------------------------------------------------------------------
    -- 5. 월 단위 DELETE + INSERT (트랜잭션, _src_synced 기록)
    ------------------------------------------------------------------
    BEGIN TRANSACTION;

    DELETE FROM COM_AZK_AIBI_WRK.AIBI_SILVER.inv_fcst_acc_cl_m_prep AS t
    USING (SELECT DISTINCT base_month FROM batch_inv_fcst_acc) AS s
    WHERE t.base_month = s.base_month;
    v_deleted := SQLROWCOUNT;

    INSERT INTO COM_AZK_AIBI_WRK.AIBI_SILVER.inv_fcst_acc_cl_m_prep
    SELECT b.*, :v_src_synced FROM batch_inv_fcst_acc AS b;
    v_inserted := SQLROWCOUNT;

    COMMIT;

    DROP TABLE IF EXISTS sftran_inv_fcst_acc_candidates;
    DROP TABLE IF EXISTS batch_inv_fcst_acc;

    RETURN ''SUCCESS | deleted='' || v_deleted || '', inserted='' || v_inserted
           || '' (_src_synced='' || v_src_synced || '')'';

EXCEPTION
    WHEN OTHER THEN
        ROLLBACK;
        RAISE;
END;
';


-- [06] SP_INV_MATERIAL_MASTER
-- ----------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE "SP_INV_MATERIAL_MASTER"()
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
    USE SCHEMA COM_AZK_AIBI_WRK.AIBI_SILVER ;
    ------------------------------------------------------------------
    -- 0. 변경 여부 체크
    --    (a) Bronze kr_material_master _fivetran_synced vs Silver _src_synced
    --    (b) 브랜드 참조 테이블(isl_material_master)이 마지막 적재 이후 재적재되었는지
    --    → 둘 다 변경 없을 때만 SKIP (브랜드 매핑만 바뀌어도 재적재 필요)
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
    -- 1. 브랜드 매핑 충돌 확인 (원본 dropDuplicates에 안정적 tie-breaker가 없었음)
    ------------------------------------------------------------------
    IF (EXISTS (SELECT 1 FROM COM_AZK_AIBI_WRK.AIBI_SILVER.isl_material_master
                GROUP BY material_code
                HAVING COUNT(DISTINCT TO_JSON(OBJECT_CONSTRUCT_KEEP_NULL(''brand_code'', brand_code,
                                                                         ''brand_group'', brand_group))) > 1)) THEN
        RAISE ambiguous_brand;
    END IF;

    ------------------------------------------------------------------
    -- 2. Bronze 파싱 + 브랜드 결합 → 배치 임시 테이블
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
    -- 3. 배치 검증 (Full refresh이므로 실패 시 덮어쓰기 차단)
    ------------------------------------------------------------------
    IF (NOT EXISTS (SELECT 1 FROM batch_inv_material_master)) THEN
        RAISE empty_batch;
    END IF;
    IF (EXISTS (SELECT 1 FROM batch_inv_material_master
                GROUP BY material_code HAVING COUNT(*) > 1)) THEN
        RAISE duplicate_key;
    END IF;

    ------------------------------------------------------------------
    -- 4. Full refresh (_src_synced 기록)
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


-- [07] SP_INV_SLS_FCST_CL_M_PREP
-- ----------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE "SP_INV_SLS_FCST_CL_M_PREP"()
RETURNS VARCHAR
LANGUAGE SQL
COMMENT='Sales volume forecast snapshot. Bronze submission_month (wide col_YYYYMM) -> Silver inv_sls_fcst_cl_m_prep (long). base_month = submission(snapshot) month. Per base_month source priority: \"Submission Month.xlsx\"(1) > \"Submission Month YYYYMM.xlsx\"(2). Skips when Bronze _fivetran_synced equals Silver _src_synced.'
EXECUTE AS CALLER
AS '
DECLARE
    ambiguous_source     EXCEPTION (-20001, ''Multiple files with the same top priority for one base_month; resolve source ambiguity.'');
    invalid_file_name    EXCEPTION (-20002, ''File name is neither "Submission Month.xlsx" nor "Submission Month YYYYMM.xlsx"; source priority cannot be resolved. Target is unchanged.'');
    missing_date_columns EXCEPTION (-20003, ''No recognized month columns (col_YYYYMM) in Bronze submission_month; target is unchanged.'');
    invalid_month        EXCEPTION (-20004, ''base_month(submission_month) or fcst_month could not be resolved; target is unchanged.'');
    empty_batch          EXCEPTION (-20005, ''No rows in sales forecast batch; target is unchanged.'');
    duplicate_key        EXCEPTION (-20006, ''Duplicate (base_month, material_code, fcst_month) in source; target is unchanged.'');
    v_deleted      INTEGER DEFAULT 0;
    v_inserted     INTEGER DEFAULT 0;
    v_src_synced   TIMESTAMP_NTZ(6);   -- Bronze 현재 _fivetran_synced
    v_tgt_synced   TIMESTAMP_NTZ(6);   -- Silver에 마지막으로 적재된 Bronze _fivetran_synced
BEGIN
    USE SCHEMA COM_AZK_AIBI_WRK.AIBI_SILVER ;
    ------------------------------------------------------------------
    -- 0. 변경 여부 체크 (Bronze _fivetran_synced vs Silver _src_synced)
    ------------------------------------------------------------------
    SELECT MAX(b."_fivetran_synced"::TIMESTAMP_NTZ(6))
      INTO :v_src_synced
      FROM COM_AZK_AIBI_RAW_MDLS."aibi_bronze"."submission_month" AS b;

    SELECT MAX(_src_synced)
      INTO :v_tgt_synced
      FROM COM_AZK_AIBI_WRK.AIBI_SILVER.inv_sls_fcst_cl_m_prep;

    IF (v_src_synced IS NULL) THEN
        RAISE empty_batch;
    END IF;
    IF (v_tgt_synced IS NOT NULL AND v_src_synced = v_tgt_synced) THEN
        RETURN ''SKIPPED | no change (_fivetran_synced='' || v_src_synced || '')'';
    END IF;

    ------------------------------------------------------------------
    -- 1. Bronze 월 컬럼 존재 여부 확인 (col_YYYYMM)
    ------------------------------------------------------------------
    IF (NOT EXISTS (SELECT 1 FROM COM_AZK_AIBI_RAW_MDLS.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ''aibi_bronze'' AND TABLE_NAME = ''submission_month''
          AND REGEXP_LIKE(UPPER(TRIM(COLUMN_NAME)), ''^COL_[0-9]{6}$''))) THEN
        RAISE missing_date_columns;
    END IF;

    ------------------------------------------------------------------
    -- 2. 후보 스테이징 (wide 상태 유지): base_month + 파일 유형/우선순위
                                                                                     
    ------------------------------------------------------------------
    CREATE OR REPLACE TEMP TABLE sftran_inv_sls_fcst_candidates AS
    WITH bronze AS (
        SELECT OBJECT_CONSTRUCT_KEEP_NULL(b.*) AS r
        FROM COM_AZK_AIBI_RAW_MDLS."aibi_bronze"."submission_month" AS b
    ), parsed AS (
        SELECT
            r,
            -- submission_month (예: 202610) = 스냅샷 생성월
            TRY_TO_DATE(TRUNC(TRY_TO_DOUBLE(GET_IGNORE_CASE(r, ''submission_month'')::VARCHAR))::INTEGER::VARCHAR || ''01'',
                        ''YYYYMMDD'')                                                   AS base_month,
            TRIM(GET_IGNORE_CASE(r, ''#_file'')::VARCHAR)                              AS file_name
        FROM bronze
        WHERE GET_IGNORE_CASE(r, ''sku_description'')::VARCHAR IS NOT NULL
    ), file_attr AS (
        SELECT p.*,
            -- 경로 제거 + 소문자 + 연속 공백 정규화
            REGEXP_REPLACE(LOWER(REGEXP_SUBSTR(file_name, ''[^/]+$'')), ''\\\\s+'', '' '')  AS file_base,
            REGEXP_SUBSTR(file_name, ''(20[0-9]{2})(0[1-9]|1[0-2])'')                  AS file_month,  -- 고정명 파일은 NULL
            LOWER(REGEXP_SUBSTR(file_name, ''\\\\.([[:alnum:]_]+)$'', 1, 1, ''e'', 1))      AS file_ext
        FROM parsed AS p
    )
    SELECT *,
        CASE
            WHEN file_base = ''submission month.xlsx''           THEN 1   -- 고정명, 다월 파일
            WHEN file_month IS NOT NULL AND file_ext = ''xlsx''  THEN 2   -- Submission Month YYYYMM.xlsx
        END AS file_priority
    FROM file_attr;

    ------------------------------------------------------------------
    -- 3. 후보 검증
    ------------------------------------------------------------------
    IF (NOT EXISTS (SELECT 1 FROM sftran_inv_sls_fcst_candidates)) THEN
        RAISE empty_batch;
    END IF;
    -- 규칙에 맞지 않는 파일명 → 우선순위 판단 불가
    IF (EXISTS (SELECT 1 FROM sftran_inv_sls_fcst_candidates
                WHERE file_priority IS NULL)) THEN
        RAISE invalid_file_name;
    END IF;
    -- base_month NULL이면 파일 선택(JOIN)에서 조용히 탈락하므로 사전 차단
    IF (EXISTS (SELECT 1 FROM sftran_inv_sls_fcst_candidates
                WHERE base_month IS NULL)) THEN
        RAISE invalid_month;
    END IF;
    -- 같은 base_month에서 최상위 우선순위 파일이 2개 이상이면 모호
    IF (EXISTS (
            SELECT 1
            FROM (
                SELECT base_month, file_name, file_priority,
                       MIN(file_priority) OVER (PARTITION BY base_month) AS top_priority
                FROM (SELECT DISTINCT base_month, file_name, file_priority
                      FROM sftran_inv_sls_fcst_candidates)
            )
            WHERE file_priority = top_priority
            GROUP BY base_month
            HAVING COUNT(DISTINCT file_name) > 1
        )) THEN
        RAISE ambiguous_source;
    END IF;

    ------------------------------------------------------------------
    -- 4. base_month별 최우선 파일 선택 → Wide → Long 변환 → 배치 임시 테이블
    --    신규 col_YYYYMM 컬럼은 FLATTEN으로 자동 인식 (schema-drift-safe)
    ------------------------------------------------------------------
    CREATE OR REPLACE TEMP TABLE batch_inv_sls_fcst AS
    WITH winning_files AS (
        SELECT base_month, file_name
        FROM (SELECT DISTINCT base_month, file_name, file_priority
              FROM sftran_inv_sls_fcst_candidates)
        QUALIFY ROW_NUMBER() OVER (PARTITION BY base_month
                                   ORDER BY file_priority, file_name) = 1
    ), selected AS (
        SELECT c.r, c.base_month
        FROM sftran_inv_sls_fcst_candidates AS c
        JOIN winning_files AS w
          ON c.base_month = w.base_month
         AND c.file_name  = w.file_name
    ), unpivoted AS (
        SELECT
            s.base_month,
            TRIM(GET_IGNORE_CASE(s.r, ''sku_description'')::VARCHAR)                       AS sku_text,
            REGEXP_SUBSTR(UPPER(TRIM(f.key::VARCHAR)), ''^COL_([0-9]{6})$'', 1, 1, ''e'', 1) AS raw_month,
            f.value::VARCHAR                                                             AS raw_value
        FROM selected AS s,
             LATERAL FLATTEN(INPUT => s.r) AS f
        -- col_YYYYMM만 대상 (total, submission_month, sku_description, #_file 등 제외)
        WHERE REGEXP_LIKE(UPPER(TRIM(f.key::VARCHAR)), ''^COL_[0-9]{6}$'')
    ), parsed AS (
        SELECT
            base_month,
                                                                                                                        
                                                                                                    
                                                                                         
            sku_text,
            TRY_TO_DATE(raw_month || ''01'', ''YYYYMMDD'')                               AS fcst_month,
            -- 콤마, 공백, NBSP(160), ZWSP(8203), BOM(65279) 제거 후 숫자 변환
            TRY_TO_DOUBLE(
                REGEXP_REPLACE(
                    TRANSLATE(raw_value, '','' || CHR(160) || CHR(8203) || CHR(65279), ''''),
                    ''\\\\s'', ''''))                                                      AS fcst_volume
        FROM unpivoted
                                                                        
    ), split AS (
        SELECT
            base_month,
            SPLIT_PART(sku_text, '' '', 1)                                             AS material_code,
            IFF(CONTAINS(sku_text, '' ''),
                SUBSTR(sku_text, LENGTH(SPLIT_PART(sku_text, '' '', 1)) + 2),
                NULL)                                                                AS material_desc,
            fcst_month,
            fcst_volume
        FROM parsed
    )
    SELECT
        base_month,
        material_code,
        material_desc,
        fcst_month,
        fcst_volume,
        -- fcst_month < base_month → ACT(seq 0), fcst_month >= base_month → FCST(seq = 차이 + 1)
        IFF(base_month > fcst_month, 0, DATEDIFF(''month'', base_month, fcst_month) + 1)::INTEGER       AS fcst_month_seq,
        IFF(IFF(base_month > fcst_month, 0, DATEDIFF(''month'', base_month, fcst_month) + 1) >= 1,
            ''FCST'', ''ACT'')                                                                            AS data_type_cd,
        CONVERT_TIMEZONE(''UTC'', ''Asia/Seoul'', SYSDATE())::TIMESTAMP_NTZ(6)                           AS _load_dtm
    FROM split
    WHERE fcst_volume IS NOT NULL
      AND fcst_volume <> 0;

    ------------------------------------------------------------------
    -- 5. 배치 검증 (실패 시 타겟 변경 없음)
    ------------------------------------------------------------------
    IF (NOT EXISTS (SELECT 1 FROM batch_inv_sls_fcst)) THEN
        RAISE empty_batch;
    END IF;
                                                                                                    
    IF (EXISTS (SELECT 1 FROM batch_inv_sls_fcst
                WHERE base_month IS NULL OR fcst_month IS NULL)) THEN
        RAISE invalid_month;
    END IF;
    -- 파일 선택 후에도 중복이면 단일 파일 내부 중복 (자재코드 중복 행 등)
    IF (EXISTS (SELECT 1 FROM batch_inv_sls_fcst
                GROUP BY base_month, material_code, fcst_month HAVING COUNT(*) > 1)) THEN
        RAISE duplicate_key;
    END IF;

    ------------------------------------------------------------------
    -- 6. 스냅샷 월(base_month) 단위 DELETE + INSERT (트랜잭션, _src_synced 기록)
    ------------------------------------------------------------------
    BEGIN TRANSACTION;

    DELETE FROM COM_AZK_AIBI_WRK.AIBI_SILVER.inv_sls_fcst_cl_m_prep AS t
    USING (SELECT DISTINCT base_month FROM batch_inv_sls_fcst) AS s
    WHERE t.base_month = s.base_month;
    v_deleted := SQLROWCOUNT;

    INSERT INTO COM_AZK_AIBI_WRK.AIBI_SILVER.inv_sls_fcst_cl_m_prep
    SELECT b.*, :v_src_synced FROM batch_inv_sls_fcst AS b;
    v_inserted := SQLROWCOUNT;

    COMMIT;

    DROP TABLE IF EXISTS sftran_inv_sls_fcst_candidates;
    DROP TABLE IF EXISTS batch_inv_sls_fcst;

    RETURN ''SUCCESS | deleted='' || v_deleted || '', inserted='' || v_inserted
           || '' (_src_synced='' || v_src_synced || '')'';

EXCEPTION
    WHEN OTHER THEN
        ROLLBACK;
        RAISE;
END;
';


-- [08] SP_INV_SPL_PLN_OP_D_PREP
-- ----------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE "SP_INV_SPL_PLN_OP_D_PREP"()
RETURNS VARCHAR
LANGUAGE SQL
COMMENT='Supply plan full refresh from the fixed-name Fivetran file \"Supply plan.XLSX\" only (dated files are ignored). Skips when that file _fivetran_synced equals Silver _src_synced. Bronze inventory_supply_plan -> Silver inv_spl_pln_op_d_prep'
EXECUTE AS CALLER
AS '
DECLARE
    missing_source_file EXCEPTION (-20002, ''Fixed-name source file "Supply plan.XLSX" not found in Bronze inventory_supply_plan. Target is unchanged.'');
    empty_batch         EXCEPTION (-20005, ''No rows in "Supply plan.XLSX"; target is unchanged.'');
    v_target_file    VARCHAR DEFAULT ''supply plan.xlsx'';   -- 정규화된 고정 파일명 (소문자, 단일 공백)
    v_inserted       INTEGER DEFAULT 0;
    v_src_synced     TIMESTAMP_NTZ(6);   -- Bronze 고정명 파일의 _fivetran_synced
    v_tgt_synced     TIMESTAMP_NTZ(6);   -- Silver에 마지막으로 적재된 Bronze _fivetran_synced
BEGIN
    USE SCHEMA COM_AZK_AIBI_WRK.AIBI_SILVER ;
    ------------------------------------------------------------------
    -- 1. 고정명 파일 존재 확인 + 변경 여부 체크
    --    파일이 없으면 v_src_synced가 NULL → OVERWRITE로 타겟이 비워지지 않도록 사전 차단
    --    Iceberg 정밀도(마이크로초)에 맞춰 양쪽 모두 (6)으로 비교
    ------------------------------------------------------------------
    SELECT MAX(b."_fivetran_synced"::TIMESTAMP_NTZ(6))
      INTO :v_src_synced
      FROM (SELECT "_fivetran_synced", OBJECT_CONSTRUCT_KEEP_NULL(*) AS r
              FROM COM_AZK_AIBI_RAW_MDLS."aibi_bronze"."inventory_supply_plan") AS b
     WHERE REGEXP_REPLACE(
               LOWER(TRIM(REGEXP_SUBSTR(COALESCE(GET_IGNORE_CASE(b.r, ''#_file''),
                                                 GET_IGNORE_CASE(b.r, ''_file''))::VARCHAR,
                                        ''[^/]+$''))),
               ''\\\\s+'', '' '') = :v_target_file;

    IF (v_src_synced IS NULL) THEN
        RAISE missing_source_file;
    END IF;

    SELECT MAX(_src_synced)
      INTO :v_tgt_synced
      FROM COM_AZK_AIBI_WRK.AIBI_SILVER.inv_spl_pln_op_d_prep;

    IF (v_tgt_synced IS NOT NULL AND v_src_synced = v_tgt_synced) THEN
        RETURN ''SKIPPED | no change (file=Supply plan.XLSX''
               || '', _fivetran_synced='' || v_src_synced || '')'';
    END IF;

    ------------------------------------------------------------------
    -- 2. 고정명 파일만 파싱 → 배치 임시 테이블
    ------------------------------------------------------------------
    CREATE OR REPLACE TEMP TABLE batch_inv_spl_pln AS
    WITH bronze AS (
        SELECT OBJECT_CONSTRUCT_KEEP_NULL(*) AS r
        FROM COM_AZK_AIBI_RAW_MDLS."aibi_bronze"."inventory_supply_plan" AS b
    ), src AS (
        SELECT r
        FROM bronze
        WHERE REGEXP_REPLACE(
                  LOWER(TRIM(REGEXP_SUBSTR(COALESCE(GET_IGNORE_CASE(r, ''#_file''),
                                                    GET_IGNORE_CASE(r, ''_file''))::VARCHAR,
                                           ''[^/]+$''))),
                  ''\\\\s+'', '' '') = :v_target_file
    ), raw AS (
        SELECT
            GET_IGNORE_CASE(r, ''availability_date_requirements_date'')::VARCHAR    AS avail_req_date_raw,
            GET_IGNORE_CASE(r, ''product_number'')::VARCHAR                         AS product_number,
            GET_IGNORE_CASE(r, ''product_short_description'')::VARCHAR              AS product_short_desc,
            GET_IGNORE_CASE(r, ''category_short_text'')::VARCHAR                    AS category_short_text,
            GET_IGNORE_CASE(r, ''receipt_element_requirements_element'')::VARCHAR   AS receipt_req_element,
            TRY_TO_DECIMAL(GET_IGNORE_CASE(r, ''receipt_quantity_requirements_quantity'')::VARCHAR, 18, 3) AS receipt_req_qty,
            TRY_TO_DECIMAL(GET_IGNORE_CASE(r, ''confirmed_quantity_atp'')::VARCHAR, 18, 3)                 AS confirmed_qty_atp,
            TRY_TO_DECIMAL(GET_IGNORE_CASE(r, ''requested_quantity'')::VARCHAR, 18, 3)                     AS requested_qty,
            GET_IGNORE_CASE(r, ''source_location'')::VARCHAR                        AS source_location,
            GET_IGNORE_CASE(r, ''target_location'')::VARCHAR                        AS target_location,
            GET_IGNORE_CASE(r, ''local_product_code_source_location'')::VARCHAR     AS material_code_source_loc,
            GET_IGNORE_CASE(r, ''local_product_code_target_location'')::VARCHAR     AS material_code_target_loc,
            GET_IGNORE_CASE(r, ''supply_source'')::VARCHAR                          AS supply_source,
            TRY_TO_BOOLEAN(GET_IGNORE_CASE(r, ''pp_firmed'')::VARCHAR)              AS pp_firmed,           -- 구 PP-Firmed
            GET_IGNORE_CASE(r, ''opening_date'')::VARCHAR                           AS opening_date_raw,
            GET_IGNORE_CASE(r, ''base_unit_of_measure'')::VARCHAR                   AS base_uom,
            GET_IGNORE_CASE(r, ''schedule_line_date'')::VARCHAR                     AS schedule_line_date_raw,
            GET_IGNORE_CASE(r, ''creation_date'')::VARCHAR                          AS creation_date_raw,
            GET_IGNORE_CASE(r, ''delivery_date'')::VARCHAR                          AS delivery_date_raw,
            GET_IGNORE_CASE(r, ''start_date'')::VARCHAR                             AS start_date_raw
        FROM src
        WHERE GET_IGNORE_CASE(r, ''product_number'')::VARCHAR IS NOT NULL
    )
    SELECT
        -- 날짜: YYYYMMDD 우선, 실패 시 자동 형식 인식
        COALESCE(TRY_TO_DATE(avail_req_date_raw, ''YYYYMMDD''),     TRY_TO_DATE(avail_req_date_raw))     AS avail_req_date,
        product_number,
        product_short_desc,
        category_short_text,
        receipt_req_element,
        receipt_req_qty,
        confirmed_qty_atp,
        requested_qty,
        source_location,
        target_location,
        material_code_source_loc,
        material_code_target_loc,
        supply_source,
        pp_firmed,
        COALESCE(TRY_TO_DATE(opening_date_raw, ''YYYYMMDD''),       TRY_TO_DATE(opening_date_raw))       AS opening_date,
        base_uom,
        COALESCE(TRY_TO_DATE(schedule_line_date_raw, ''YYYYMMDD''), TRY_TO_DATE(schedule_line_date_raw)) AS schedule_line_date,
        COALESCE(TRY_TO_DATE(creation_date_raw, ''YYYYMMDD''),      TRY_TO_DATE(creation_date_raw))      AS creation_date,
        COALESCE(TRY_TO_DATE(delivery_date_raw, ''YYYYMMDD''),      TRY_TO_DATE(delivery_date_raw))      AS delivery_date,
        COALESCE(TRY_TO_DATE(start_date_raw, ''YYYYMMDD''),         TRY_TO_DATE(start_date_raw))         AS start_date,
        CONVERT_TIMEZONE(''UTC'', ''Asia/Seoul'', SYSDATE())::TIMESTAMP_NTZ(6)                            AS _load_dtm
    FROM raw;

    ------------------------------------------------------------------
    -- 3. 배치 검증 (Full refresh이므로 실패 시 덮어쓰기 차단)
    ------------------------------------------------------------------
    IF (NOT EXISTS (SELECT 1 FROM batch_inv_spl_pln)) THEN
        RAISE empty_batch;
    END IF;

    ------------------------------------------------------------------
    -- 4. Full refresh (_src_synced에 이번에 읽은 고정명 파일 버전 기록)
    ------------------------------------------------------------------
    INSERT OVERWRITE INTO COM_AZK_AIBI_WRK.AIBI_SILVER.inv_spl_pln_op_d_prep
        (avail_req_date, product_number, product_short_desc, category_short_text, receipt_req_element,
         receipt_req_qty, confirmed_qty_atp, requested_qty, source_location, target_location,
         material_code_source_loc, material_code_target_loc, supply_source, pp_firmed, opening_date,
         base_uom, schedule_line_date, creation_date, delivery_date, start_date, _load_dtm, _src_synced)
    SELECT avail_req_date, product_number, product_short_desc, category_short_text, receipt_req_element,
           receipt_req_qty, confirmed_qty_atp, requested_qty, source_location, target_location,
           material_code_source_loc, material_code_target_loc, supply_source, pp_firmed, opening_date,
           base_uom, schedule_line_date, creation_date, delivery_date, start_date, _load_dtm, :v_src_synced
    FROM batch_inv_spl_pln;
    v_inserted := SQLROWCOUNT;

    DROP TABLE IF EXISTS batch_inv_spl_pln;

    RETURN ''SUCCESS | overwritten from file=Supply plan.XLSX, inserted='' || v_inserted
           || '' (_src_synced='' || v_src_synced || '')'';

EXCEPTION
    WHEN OTHER THEN
        RAISE;
END;
';


-- [09] SP_INV_STATUS_OP_D_PREP
-- ----------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE "SP_INV_STATUS_OP_D_PREP"()
RETURNS VARCHAR
LANGUAGE SQL
COMMENT='Stock reconciliation (SAP vs DC) full refresh. Bronze kr_stock_reconciliation -> Silver inv_status_op_d_prep. When Bronze _fivetran_synced equals Silver _src_synced, skips reload and only refreshes remaining_shelf_life to KST today.'
EXECUTE AS CALLER
AS '
DECLARE
    empty_batch    EXCEPTION (-20005, ''No rows in kr_stock_reconciliation batch; target is unchanged.'');
    v_today        DATE;
    v_inserted     INTEGER DEFAULT 0;
    v_updated      INTEGER DEFAULT 0;
    v_src_synced   TIMESTAMP_NTZ(6);   -- Bronze 현재 _fivetran_synced
    v_tgt_synced   TIMESTAMP_NTZ(6);   -- Silver에 마지막으로 적재된 Bronze _fivetran_synced
BEGIN
    USE SCHEMA COM_AZK_AIBI_WRK.AIBI_SILVER ;
    -- 잔여 유효기간 기준일: 세션 타임존(UTC 등)과 무관하게 KST 오늘
    v_today := CONVERT_TIMEZONE(''Asia/Seoul'', CURRENT_TIMESTAMP())::DATE;

    ------------------------------------------------------------------
    -- 0. 변경 여부 체크 (Bronze _fivetran_synced vs Silver _src_synced)
    --    remaining_shelf_life는 실행일(KST 오늘) 기준 값이라 원천이 그대로여도 매일 바뀜
    --    → 원천 변경이 없으면 재적재 대신 remaining_shelf_life만 오늘 기준으로 갱신
    ------------------------------------------------------------------
    SELECT MAX(b."_fivetran_synced"::TIMESTAMP_NTZ(6))
      INTO :v_src_synced
      FROM COM_AZK_AIBI_RAW_MDLS."aibi_bronze"."kr_stock_reconciliation" AS b;

    SELECT MAX(_src_synced)
      INTO :v_tgt_synced
      FROM COM_AZK_AIBI_WRK.AIBI_SILVER.inv_status_op_d_prep;

    IF (v_src_synced IS NULL) THEN
        RAISE empty_batch;
    END IF;
    IF (v_tgt_synced IS NOT NULL AND v_src_synced = v_tgt_synced) THEN
        UPDATE COM_AZK_AIBI_WRK.AIBI_SILVER.inv_status_op_d_prep
           SET remaining_shelf_life = DATEDIFF(''day'', :v_today, expiry_date)
         WHERE remaining_shelf_life IS DISTINCT FROM DATEDIFF(''day'', :v_today, expiry_date);
        v_updated := SQLROWCOUNT;

        RETURN ''SKIPPED | no change (_fivetran_synced='' || v_src_synced
               || ''), remaining_shelf_life refreshed='' || v_updated || '' (base='' || v_today || '')'';
    END IF;

    ------------------------------------------------------------------
    -- 1. Bronze 파싱 → 배치 임시 테이블
    ------------------------------------------------------------------
    CREATE OR REPLACE TEMP TABLE batch_inv_status AS
    WITH bronze AS (
        SELECT OBJECT_CONSTRUCT_KEEP_NULL(b.*) AS r
        FROM COM_AZK_AIBI_RAW_MDLS."aibi_bronze"."kr_stock_reconciliation" AS b
    ), parsed AS (
        SELECT
            TRIM(GET_IGNORE_CASE(r, ''plant'')::VARCHAR)                                   AS plant,
            TRIM(GET_IGNORE_CASE(r, ''storage_location'')::VARCHAR)                        AS storage_location,
            TRIM(GET_IGNORE_CASE(r, ''material'')::VARCHAR)                                AS material_code,
            TRIM(GET_IGNORE_CASE(r, ''description'')::VARCHAR)                             AS material_desc,
            TRIM(GET_IGNORE_CASE(r, ''batch'')::VARCHAR)                                   AS batch,
            -- expiry_date: LONG(예: 20271231) → 문자열 → 날짜
            TRY_TO_DATE(TRIM(GET_IGNORE_CASE(r, ''expiry_date'')::VARCHAR), ''YYYYMMDD'')    AS expiry_date,
            TRIM(GET_IGNORE_CASE(r, ''status'')::VARCHAR)                                  AS stock_status_cd,
            TRUNC(TRY_TO_DOUBLE(TRIM(GET_IGNORE_CASE(r, ''sap_quantity'')::VARCHAR)))::INTEGER  AS sap_qty,   -- DOUBLE
            TRUNC(TRY_TO_DOUBLE(TRIM(GET_IGNORE_CASE(r, ''dc_quantity'')::VARCHAR)))::INTEGER   AS dc_qty,    -- DOUBLE
            TRUNC(TRY_TO_DOUBLE(TRIM(GET_IGNORE_CASE(r, ''quantity_diff'')::VARCHAR)))::INTEGER AS qty_diff,  -- LONG
            TRIM(GET_IGNORE_CASE(r, ''geonet_wms_sled'')::VARCHAR)                         AS geonet_wms_sled, -- LONG
            TRIM(GET_IGNORE_CASE(r, ''remarks_qty'')::VARCHAR)                             AS remarks_qty,
            TRIM(GET_IGNORE_CASE(r, ''remarks_sled'')::VARCHAR)                            AS remarks_sled
        FROM bronze
        WHERE TRIM(GET_IGNORE_CASE(r, ''material'')::VARCHAR) IS NOT NULL
          AND NULLIF(TRIM(GET_IGNORE_CASE(r, ''expiry_date'')::VARCHAR), '''') IS NOT NULL
          AND TRIM(GET_IGNORE_CASE(r, ''expiry_date'')::VARCHAR) <> ''0''
    )
    SELECT
        plant,
        storage_location,
        material_code,
        material_desc,
        batch,
        expiry_date,
        stock_status_cd,
        CASE WHEN NULLIF(stock_status_cd, '''') IS NULL THEN ''Available''
             WHEN stock_status_cd = ''S''               THEN ''Blocked''
             WHEN stock_status_cd = ''X''               THEN ''Under Quarantine''
        END                                                                 AS stock_status_desc,
        sap_qty,
        dc_qty,
        qty_diff,
        geonet_wms_sled,
        DATEDIFF(''day'', :v_today, expiry_date)                              AS remaining_shelf_life,
        remarks_qty,
        remarks_sled,
        CONVERT_TIMEZONE(''UTC'', ''Asia/Seoul'', SYSDATE())::TIMESTAMP_NTZ(6) AS _load_dtm
    FROM parsed;

    ------------------------------------------------------------------
    -- 2. 배치 검증 (Full refresh이므로 실패 시 덮어쓰기 차단)
    ------------------------------------------------------------------
    IF (NOT EXISTS (SELECT 1 FROM batch_inv_status)) THEN
        RAISE empty_batch;
    END IF;

    ------------------------------------------------------------------
    -- 3. Full refresh (_src_synced 기록)
    ------------------------------------------------------------------
    INSERT OVERWRITE INTO COM_AZK_AIBI_WRK.AIBI_SILVER.inv_status_op_d_prep
        (plant, storage_location, material_code, material_desc, batch, expiry_date, stock_status_cd,
         stock_status_desc, sap_qty, dc_qty, qty_diff, geonet_wms_sled, remaining_shelf_life,
         remarks_qty, remarks_sled, _load_dtm, _src_synced)
    SELECT plant, storage_location, material_code, material_desc, batch, expiry_date, stock_status_cd,
           stock_status_desc, sap_qty, dc_qty, qty_diff, geonet_wms_sled, remaining_shelf_life,
           remarks_qty, remarks_sled, _load_dtm, :v_src_synced
    FROM batch_inv_status;
    v_inserted := SQLROWCOUNT;

    DROP TABLE IF EXISTS batch_inv_status;

    RETURN ''SUCCESS | overwritten, inserted='' || v_inserted
           || '' (_src_synced='' || v_src_synced || '')'';

EXCEPTION
    WHEN OTHER THEN
        RAISE;
END;
';


-- [10] SP_ISL_BGT_CL_M_PREP
-- ----------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE "SP_ISL_BGT_CL_M_PREP"()
RETURNS VARCHAR
LANGUAGE SQL
COMMENT='Budget revenue/volume Bronze -> Silver prep. Month keys jan..dec = revenue, jan_1..dec_1 = volume (Fivetran suffix). Skips when Bronze _fivetran_synced equals Silver _src_synced.'
EXECUTE AS CALLER
AS '
DECLARE
    ambiguous_first      EXCEPTION (-20001, ''Conflicting duplicate pivot values: FIRST has no defined ordering. Resolve source duplicates.'');
    missing_date_columns EXCEPTION (-20003, ''No recognized month columns (jan..dec / jan_1..dec_1) in Bronze source; target is unchanged.'');
    unexpected_layout    EXCEPTION (-20004, ''fiscal_year/value_type could not be resolved (col_N shift?). Target is unchanged.'');
    empty_batch          EXCEPTION (-20005, ''No rows in budget batch; target is unchanged.'');
    v_deleted      INTEGER DEFAULT 0;
    v_inserted     INTEGER DEFAULT 0;
    v_src_synced   TIMESTAMP_NTZ(6);   -- Bronze 현재 _fivetran_synced
    v_tgt_synced   TIMESTAMP_NTZ(6);   -- Silver에 마지막으로 적재된 Bronze _fivetran_synced
BEGIN
    USE SCHEMA COM_AZK_AIBI_WRK.AIBI_SILVER ;
    ------------------------------------------------------------------
    -- 0. 변경 여부 체크 (Bronze _fivetran_synced vs Silver _src_synced)
    --    soft delete(_fivetran_deleted = TRUE) 행도 포함해서 MAX를 구함
    --    → 원천에서 행이 삭제된 경우도 변경으로 감지
    ------------------------------------------------------------------
    SELECT MAX(b."_fivetran_synced"::TIMESTAMP_NTZ(6))
      INTO :v_src_synced
      FROM COM_AZK_AIBI_RAW_MDLS.aibi_bronze."budget_total_revenue_volume" AS b;

    SELECT MAX(_src_synced)
      INTO :v_tgt_synced
      FROM COM_AZK_AIBI_WRK.AIBI_SILVER.isl_bgt_cl_m_prep;

    IF (v_src_synced IS NULL) THEN
        RAISE empty_batch;
    END IF;
    IF (v_tgt_synced IS NOT NULL AND v_src_synced = v_tgt_synced) THEN
        RETURN ''SKIPPED | no change (_fivetran_synced='' || v_src_synced || '')'';
    END IF;

    ------------------------------------------------------------------
    -- 1. Bronze 월 컬럼 존재 여부 확인 (jan..dec / jan_1..dec_1)
    ------------------------------------------------------------------
    IF (NOT EXISTS (
          SELECT 1 FROM COM_AZK_AIBI_RAW_MDLS.INFORMATION_SCHEMA.COLUMNS
          WHERE TABLE_SCHEMA = ''aibi_bronze''
            AND TABLE_NAME   = ''budget_total_revenue_volume''
            AND REGEXP_LIKE(UPPER(TRIM(COLUMN_NAME)),
                            ''^(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)(_1)?$''))) THEN
        RAISE missing_date_columns;
    END IF;

    ------------------------------------------------------------------
    -- 2. Stage source once (long format: revenue / volume)
    ------------------------------------------------------------------
    CREATE OR REPLACE TEMP TABLE sftran_bgt_long AS
    WITH bronze AS (
        SELECT OBJECT_CONSTRUCT_KEEP_NULL(b.*) AS r
        FROM COM_AZK_AIBI_RAW_MDLS.aibi_bronze."budget_total_revenue_volume" AS b
    ), raw_base AS (
        SELECT
            r,
            TRIM(GET_IGNORE_CASE(r, ''material'')::VARCHAR)             AS material_code,
            TRIM(GET_IGNORE_CASE(r, ''material_description'')::VARCHAR) AS material_desc,
            TRIM(GET_IGNORE_CASE(r, ''product'')::VARCHAR)              AS brand_name_raw,
            TRIM(GET_IGNORE_CASE(r, ''product_code'')::VARCHAR)         AS brand_code_raw,
            TRIM(GET_IGNORE_CASE(r, ''col_5'')::VARCHAR)                AS raw_col_5,
            TRIM(GET_IGNORE_CASE(r, ''col_6'')::VARCHAR)                AS raw_col_6,
            TRIM(GET_IGNORE_CASE(r, ''col_7'')::VARCHAR)                AS raw_col_7
        FROM bronze
        WHERE COALESCE(GET_IGNORE_CASE(r, ''_fivetran_deleted'')::BOOLEAN, FALSE) = FALSE
    ), base AS (
        SELECT
            r,
            material_code, material_desc, brand_name_raw, brand_code_raw,
            -- col_5 holds FY for FY25+; for FY23/FY24 it is NULL and FY sits in col_6
            IFF(REGEXP_LIKE(COALESCE(raw_col_5,''''), ''^FY[0-9]{2}$''),
                raw_col_5, raw_col_6)                                   AS fiscal_year,
            IFF(REGEXP_LIKE(COALESCE(raw_col_5,''''), ''^FY[0-9]{2}$''),
                raw_col_6, raw_col_7)                                   AS value_type,
            IFF(REGEXP_LIKE(COALESCE(raw_col_5,''''), ''^FY[0-9]{2}$''),
                raw_col_7, NULL)                                        AS pr_material_code
        FROM raw_base
    )
    SELECT
        b.* EXCLUDE r,
        IFF(ENDSWITH(LOWER(TRIM(f.key::VARCHAR)), ''_1''), ''volume'', ''revenue'') AS measure_name,
        DECODE(LEFT(LOWER(TRIM(f.key::VARCHAR)), 3),
               ''jan'',1,''feb'',2,''mar'',3,''apr'',4,''may'',5,''jun'',6,
               ''jul'',7,''aug'',8,''sep'',9,''oct'',10,''nov'',11,''dec'',12)     AS month_no,
        TRY_TO_DOUBLE(NULLIF(REPLACE(TRIM(f.value::VARCHAR), '','', ''''), '''')) AS measure_value
    FROM base AS b,
         LATERAL FLATTEN(INPUT => b.r) AS f
    WHERE REGEXP_LIKE(LOWER(TRIM(f.key::VARCHAR)),
                      ''^(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)(_1)?$'');

    ------------------------------------------------------------------
    -- 3. 피벗 grain 중복 충돌 검사 (MAX로 조용히 선택하지 않고 중단)
    ------------------------------------------------------------------
    IF (EXISTS (
          SELECT 1 FROM sftran_bgt_long
          GROUP BY material_code, material_desc, brand_name_raw, brand_code_raw,
                   fiscal_year, value_type, pr_material_code, month_no, measure_name
          HAVING COUNT(DISTINCT TO_JSON(OBJECT_CONSTRUCT_KEEP_NULL(''v'', measure_value))) > 1)) THEN
        RAISE ambiguous_first;
    END IF;

    ------------------------------------------------------------------
    -- 4. Pivot (revenue → amount, volume → volume) → 배치 임시 테이블
    ------------------------------------------------------------------
    CREATE OR REPLACE TEMP TABLE batch_isl_bgt AS
    WITH pivoted AS (
        SELECT material_code, material_desc, brand_name_raw, brand_code_raw,
               fiscal_year, value_type, pr_material_code, month_no,
               MAX(IFF(measure_name = ''revenue'', measure_value, NULL))::DECIMAL(20,4) AS amount,
               MAX(IFF(measure_name = ''volume'',  measure_value, NULL))::DECIMAL(18,2) AS volume
        FROM sftran_bgt_long
        GROUP BY material_code, material_desc, brand_name_raw, brand_code_raw,
                 fiscal_year, value_type, pr_material_code, month_no
    ), typed AS (
        SELECT *,
               2000 + TRY_TO_NUMBER(REGEXP_SUBSTR(UPPER(fiscal_year), ''FY([0-9]{2})'', 1, 1, ''e'', 1)) AS year_num
        FROM pivoted
        WHERE amount IS NOT NULL AND (amount <> 0 OR COALESCE(volume, 0) <> 0) AND material_code IS NOT NULL
          AND (pr_material_code IS NULL OR pr_material_code <> ''Total_Product'')
    )
    SELECT
        DATE_FROM_PARTS(year_num, month_no, 1)                                           AS base_month,
        material_code,
        material_desc,
        COALESCE(brand_code_raw, REGEXP_SUBSTR(brand_name_raw, ''^([0-9]+)\\\\s+'', 1, 1, ''e'', 1)) AS brand_code,
        REGEXP_REPLACE(brand_name_raw, ''^[0-9]+\\\\s+'', '''')                                AS brand_name,
        fiscal_year, value_type, pr_material_code, year_num, amount, volume,
        CONVERT_TIMEZONE(''UTC'', ''Asia/Seoul'', SYSDATE())::TIMESTAMP_NTZ(6)              AS _load_dtm
    FROM typed;

    ------------------------------------------------------------------
    -- 5. 배치 검증 (실패 시 타겟 변경 없음)
    ------------------------------------------------------------------
    IF (NOT EXISTS (SELECT 1 FROM batch_isl_bgt)) THEN
        RAISE empty_batch;
    END IF;
    IF (EXISTS (SELECT 1 FROM batch_isl_bgt
                WHERE base_month IS NULL OR value_type IS NULL)) THEN
        RAISE unexpected_layout;
    END IF;

    ------------------------------------------------------------------
    -- 6. 월 단위 DELETE + INSERT (트랜잭션, _src_synced 기록)
    ------------------------------------------------------------------
    BEGIN TRANSACTION;

    DELETE FROM COM_AZK_AIBI_WRK.AIBI_SILVER.isl_bgt_cl_m_prep AS t
    USING (SELECT DISTINCT base_month FROM batch_isl_bgt) AS s
    WHERE t.base_month = s.base_month;
    v_deleted := SQLROWCOUNT;

    INSERT INTO COM_AZK_AIBI_WRK.AIBI_SILVER.isl_bgt_cl_m_prep
        (base_month, material_code, material_desc, brand_code, brand_name, fiscal_year,
         value_type, pr_material_code, year_num, amount, volume, _load_dtm, _src_synced)
    SELECT base_month, material_code, material_desc, brand_code, brand_name, fiscal_year,
           value_type, pr_material_code, year_num, amount, volume, _load_dtm, :v_src_synced
    FROM batch_isl_bgt;
    v_inserted := SQLROWCOUNT;

    COMMIT;

    DROP TABLE IF EXISTS sftran_bgt_long;
    DROP TABLE IF EXISTS batch_isl_bgt;

    RETURN ''SUCCESS | deleted='' || v_deleted || '', inserted='' || v_inserted
           || '' (_src_synced='' || v_src_synced || '')'';

EXCEPTION
    WHEN OTHER THEN
        ROLLBACK;
        RAISE;
END;
';


-- [11] SP_ISL_FX_RATES
-- ----------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE "SP_ISL_FX_RATES"()
RETURNS VARCHAR
LANGUAGE SQL
COMMENT='FX master full refresh (Bronze fx_rates -> Silver isl_fx_rates). Skips when Bronze _fivetran_synced equals Silver _src_synced. Latest base year is the active rate (use_yn = Y).'
EXECUTE AS CALLER
AS '
DECLARE
    empty_batch      EXCEPTION (-20005, ''No rows in fx_rates Bronze source; target is unchanged.'');
    unexpected_value EXCEPTION (-20004, ''base_year or rate could not be parsed (col_2 / krw_usd layout changed?); target is unchanged.'');
    v_inserted       INTEGER DEFAULT 0;
    v_src_synced     TIMESTAMP_NTZ(6);   -- Bronze 현재 _fivetran_synced
    v_tgt_synced     TIMESTAMP_NTZ(6);   -- Silver에 마지막으로 적재된 Bronze _fivetran_synced
BEGIN
    USE SCHEMA COM_AZK_AIBI_WRK.AIBI_SILVER;

    ------------------------------------------------------------------
    -- 0. 변경 여부 체크 (Bronze _fivetran_synced vs Silver _src_synced)
    --    Iceberg 정밀도(마이크로초)에 맞춰 양쪽 모두 (6)으로 비교
    ------------------------------------------------------------------
    SELECT MAX(b."_fivetran_synced"::TIMESTAMP_NTZ(6))
      INTO :v_src_synced
      FROM COM_AZK_AIBI_RAW_MDLS.aibi_bronze."fx_rates" AS b;

    SELECT MAX(_src_synced)
      INTO :v_tgt_synced
      FROM COM_AZK_AIBI_WRK.AIBI_SILVER.isl_fx_rates;

    -- Bronze가 비어 있으면 차단
    IF (v_src_synced IS NULL) THEN
        RAISE empty_batch;
    END IF;

    -- Silver에 이미 같은 버전의 Bronze가 적재되어 있으면 종료
    IF (v_tgt_synced IS NOT NULL AND v_src_synced = v_tgt_synced) THEN
        RETURN ''SKIPPED | no change (_fivetran_synced='' || v_src_synced || '')'';
    END IF;

    ------------------------------------------------------------------
    -- 1. Bronze 파싱 → 배치 임시 테이블
    ------------------------------------------------------------------
    CREATE OR REPLACE TEMP TABLE batch_isl_fx_rates AS
    WITH bronze AS (
        SELECT OBJECT_CONSTRUCT_KEEP_NULL(b.*) AS r
        FROM COM_AZK_AIBI_RAW_MDLS.aibi_bronze."fx_rates" AS b
    ), parsed AS (
        SELECT
            TRIM(GET_IGNORE_CASE(r, ''col_2'')::VARCHAR)                                                    AS br_year,
            TRUNC(TRY_TO_DOUBLE(GET_IGNORE_CASE(r, ''krw_usd'')::VARCHAR))::INTEGER                         AS rate,
            TRUNC(TRY_TO_DOUBLE(SPLIT_PART(TRIM(GET_IGNORE_CASE(r, ''col_2'')::VARCHAR), '' '', 1)))::INTEGER AS base_year
        FROM bronze
        WHERE TRIM(GET_IGNORE_CASE(r, ''col_2'')::VARCHAR) IS NOT NULL
    )
    SELECT
        br_year,
        rate                                                                AS rt_exchange_usd_to_krw,
        base_year,
        IFF(base_year = MAX(base_year) OVER (), ''Y'', ''N'')                   AS use_yn,
        CONVERT_TIMEZONE(''UTC'', ''Asia/Seoul'', SYSDATE())::TIMESTAMP_NTZ(6) AS _load_dtm
    FROM parsed;

    ------------------------------------------------------------------
    -- 2. 배치 검증
    ------------------------------------------------------------------
    IF (NOT EXISTS (SELECT 1 FROM batch_isl_fx_rates)) THEN
        RAISE empty_batch;
    END IF;
    IF (EXISTS (SELECT 1 FROM batch_isl_fx_rates
                WHERE base_year IS NULL OR rt_exchange_usd_to_krw IS NULL)) THEN
        RAISE unexpected_value;
    END IF;

    ------------------------------------------------------------------
    -- 3. Full refresh (_src_synced에 이번에 읽은 Bronze 버전 기록)
    ------------------------------------------------------------------
    INSERT OVERWRITE INTO COM_AZK_AIBI_WRK.AIBI_SILVER.isl_fx_rates
        (br_year, rt_exchange_usd_to_krw, base_year, use_yn, _load_dtm, _src_synced)
    SELECT br_year, rt_exchange_usd_to_krw, base_year, use_yn, _load_dtm, :v_src_synced
    FROM batch_isl_fx_rates;
    v_inserted := SQLROWCOUNT;

    DROP TABLE IF EXISTS batch_isl_fx_rates;

    RETURN ''SUCCESS | overwritten, inserted='' || v_inserted
           || '' (_src_synced='' || v_src_synced || '')'';

EXCEPTION
    WHEN OTHER THEN
        RAISE;
END;
';


-- [12] SP_ISL_MATERIAL_MASTER
-- ----------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE "SP_ISL_MATERIAL_MASTER"()
RETURNS VARCHAR
LANGUAGE SQL
COMMENT='Conformed product mapping full refresh (Bronze sku_mapping -> Silver isl_material_master). Skips when Bronze _fivetran_synced equals Silver _src_synced.'
EXECUTE AS CALLER
AS '
DECLARE
    empty_batch       EXCEPTION (-20005, ''No rows in sku_mapping Bronze source; target is unchanged.'');
    duplicate_key     EXCEPTION (-20006, ''Duplicate material_code in sku_mapping; master PK would fan out joins. Target is unchanged.'');
    v_inserted     INTEGER DEFAULT 0;
    v_src_synced   TIMESTAMP_NTZ(6);   -- Bronze 현재 _fivetran_synced
    v_tgt_synced   TIMESTAMP_NTZ(6);   -- Silver에 마지막으로 적재된 Bronze _fivetran_synced
BEGIN
    USE SCHEMA COM_AZK_AIBI_WRK.AIBI_SILVER ;
    ------------------------------------------------------------------
    -- 0. 변경 여부 체크 (Bronze _fivetran_synced vs Silver _src_synced)
    ------------------------------------------------------------------
    SELECT MAX(b."_fivetran_synced"::TIMESTAMP_NTZ(6))
      INTO :v_src_synced
      FROM COM_AZK_AIBI_RAW_MDLS.aibi_bronze."sku_mapping" AS b;

    SELECT MAX(_src_synced)
      INTO :v_tgt_synced
      FROM COM_AZK_AIBI_WRK.AIBI_SILVER.isl_material_master;

    IF (v_src_synced IS NULL) THEN
        RAISE empty_batch;
    END IF;
    IF (v_tgt_synced IS NOT NULL AND v_src_synced = v_tgt_synced) THEN
        RETURN ''SKIPPED | no change (_fivetran_synced='' || v_src_synced || '')'';
    END IF;

    ------------------------------------------------------------------
    -- 1. Bronze 파싱 → 배치 임시 테이블
    ------------------------------------------------------------------
    CREATE OR REPLACE TEMP TABLE batch_isl_material_master AS
    WITH bronze AS (
        SELECT OBJECT_CONSTRUCT_KEEP_NULL(b.*) AS r
        FROM COM_AZK_AIBI_RAW_MDLS.aibi_bronze."sku_mapping" AS b
    )
    SELECT
        GET_IGNORE_CASE(r, ''material'')::VARCHAR                                   AS material_code,
        COALESCE(GET_IGNORE_CASE(r, ''material_description_new'')::VARCHAR,
                 GET_IGNORE_CASE(r, ''material_description_new_'')::VARCHAR)       AS material_desc,
        COALESCE(GET_IGNORE_CASE(r, ''material_description_old'')::VARCHAR,
                 GET_IGNORE_CASE(r, ''material_description_old_'')::VARCHAR)       AS material_desc_old,
        GET_IGNORE_CASE(r, ''product_code'')::VARCHAR                               AS brand_code,
        SUBSTR(GET_IGNORE_CASE(r, ''product_name'')::VARCHAR, 6)                    AS brand_name,
        GET_IGNORE_CASE(r, ''product_group_1'')::VARCHAR                            AS brand_group,
        GET_IGNORE_CASE(r, ''ta'')::VARCHAR                                         AS therapy_area,
        GET_IGNORE_CASE(r, ''bu'')::VARCHAR                                         AS business_unit,
        TRUNC(TRY_TO_DOUBLE(GET_IGNORE_CASE(r, ''bu_order'')::VARCHAR))::INTEGER            AS bu_order,
        TRUNC(TRY_TO_DOUBLE(GET_IGNORE_CASE(r, ''ta_order'')::VARCHAR))::INTEGER            AS ta_order,
        TRUNC(TRY_TO_DOUBLE(GET_IGNORE_CASE(r, ''product_group_order'')::VARCHAR))::INTEGER AS product_group_order,
        TRUNC(TRY_TO_DOUBLE(GET_IGNORE_CASE(r, ''product_order'')::VARCHAR))::INTEGER       AS product_order,
        CONVERT_TIMEZONE(''UTC'', ''Asia/Seoul'', SYSDATE())::TIMESTAMP_NTZ(6)       AS _load_dtm
    FROM bronze
    WHERE GET_IGNORE_CASE(r, ''material'')::VARCHAR IS NOT NULL;

    ------------------------------------------------------------------
    -- 2. 배치 검증 (Full refresh이므로 실패 시 덮어쓰기 차단)
    ------------------------------------------------------------------
    IF (NOT EXISTS (SELECT 1 FROM batch_isl_material_master)) THEN
        RAISE empty_batch;
    END IF;
    IF (EXISTS (SELECT 1 FROM batch_isl_material_master
                GROUP BY material_code HAVING COUNT(*) > 1)) THEN
        RAISE duplicate_key;
    END IF;

    ------------------------------------------------------------------
    -- 3. Full refresh (_src_synced 기록)
    ------------------------------------------------------------------
    INSERT OVERWRITE INTO COM_AZK_AIBI_WRK.AIBI_SILVER.isl_material_master
        (material_code, material_desc, material_desc_old, brand_code, brand_name, brand_group,
         therapy_area, business_unit, bu_order, ta_order, product_group_order, product_order, _load_dtm, _src_synced)
    SELECT material_code, material_desc, material_desc_old, brand_code, brand_name, brand_group,
           therapy_area, business_unit, bu_order, ta_order, product_group_order, product_order, _load_dtm, :v_src_synced
    FROM batch_isl_material_master;
    v_inserted := SQLROWCOUNT;

    DROP TABLE IF EXISTS batch_isl_material_master;

    RETURN ''SUCCESS | overwritten, inserted='' || v_inserted
           || '' (_src_synced='' || v_src_synced || '')'';

EXCEPTION
    WHEN OTHER THEN
        RAISE;
END;
';


-- [13] SP_ISL_MMF_OP_M_PREP
-- ----------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE "SP_ISL_MMF_OP_M_PREP"()
RETURNS VARCHAR
LANGUAGE SQL
COMMENT='MMF (mid-month forecast) full refresh. Bronze mmf (USD mn) -> Silver isl_mmf_op_m_prep with USD/KRW conversion via active FX rate. Skips when Bronze _fivetran_synced is unchanged AND isl_fx_rates was not reloaded since the last MMF load.'
EXECUTE AS CALLER
AS '
DECLARE
    missing_date_columns EXCEPTION (-20003, ''No recognized date columns in Bronze mmf; target is unchanged.'');
    missing_fx_rate      EXCEPTION (-20007, ''No active FX rate (isl_fx_rates.use_yn = Y); target is unchanged.'');
    empty_batch          EXCEPTION (-20005, ''No rows in MMF batch; target is unchanged.'');
    invalid_month        EXCEPTION (-20004, ''base_month could not be resolved from month column; target is unchanged.'');
    v_inserted     INTEGER DEFAULT 0;
    v_src_synced   TIMESTAMP_NTZ(6);   -- Bronze 현재 _fivetran_synced
    v_tgt_synced   TIMESTAMP_NTZ(6);   -- Silver에 마지막으로 적재된 Bronze _fivetran_synced
    v_fx_load_dtm  TIMESTAMP_NTZ(6);   -- 환율 Silver(isl_fx_rates) 최종 적재시각
    v_tgt_load_dtm TIMESTAMP_NTZ(6);   -- MMF Silver 최종 적재시각
BEGIN
    USE SCHEMA COM_AZK_AIBI_WRK.AIBI_SILVER ;
    ------------------------------------------------------------------
    -- 0. 변경 여부 체크
    --    (a) Bronze mmf _fivetran_synced vs Silver _src_synced
    --    (b) 환율 테이블이 마지막 MMF 적재 이후 재적재되었는지 (_load_dtm 둘 다 KST)
    --    → 둘 다 변경 없을 때만 SKIP (환율만 바뀌어도 KRW 금액 재계산 필요)
    ------------------------------------------------------------------
    SELECT MAX(b."_fivetran_synced"::TIMESTAMP_NTZ(6))
      INTO :v_src_synced
      FROM COM_AZK_AIBI_RAW_MDLS.aibi_bronze."mmf" AS b;

    SELECT MAX(_src_synced), MAX(_load_dtm)
      INTO :v_tgt_synced, :v_tgt_load_dtm
      FROM COM_AZK_AIBI_WRK.AIBI_SILVER.isl_mmf_op_m_prep;

    SELECT MAX(_load_dtm)
      INTO :v_fx_load_dtm
      FROM COM_AZK_AIBI_WRK.AIBI_SILVER.isl_fx_rates;

    IF (v_src_synced IS NULL) THEN
        RAISE empty_batch;
    END IF;
    IF (v_tgt_synced IS NOT NULL AND v_src_synced = v_tgt_synced
        AND v_fx_load_dtm <= v_tgt_load_dtm) THEN
        RETURN ''SKIPPED | no change (_fivetran_synced='' || v_src_synced
               || '', fx _load_dtm='' || v_fx_load_dtm || '')'';
    END IF;

    ------------------------------------------------------------------
    -- 1. Bronze 월 컬럼 존재 여부 확인
    ------------------------------------------------------------------
    IF (NOT EXISTS (SELECT 1 FROM COM_AZK_AIBI_RAW_MDLS.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ''aibi_bronze'' AND TABLE_NAME = ''mmf''
          AND REGEXP_LIKE(UPPER(TRIM(COLUMN_NAME)),
                          ''^([0-9]{4}|[0-9]{6}|JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)$''))) THEN
        RAISE missing_date_columns;
    END IF;

    ------------------------------------------------------------------
    -- 2. 적용 환율 존재 여부 확인
    --    (없으면 CROSS JOIN 결과 0건 → OVERWRITE로 타겟이 비워지는 것을 방지)
    ------------------------------------------------------------------
    IF (NOT EXISTS (SELECT 1 FROM COM_AZK_AIBI_WRK.AIBI_SILVER.isl_fx_rates
                    WHERE use_yn = ''Y'' AND rt_exchange_usd_to_krw IS NOT NULL)) THEN
        RAISE missing_fx_rate;
    END IF;

    ------------------------------------------------------------------
    -- 3. Unpivot + 환산 → 배치 임시 테이블
    ------------------------------------------------------------------
    CREATE OR REPLACE TEMP TABLE batch_isl_mmf AS
    WITH bronze AS (
        SELECT OBJECT_CONSTRUCT_KEEP_NULL(b.*) AS r
        FROM COM_AZK_AIBI_RAW_MDLS.aibi_bronze."mmf" AS b
    ), u AS (
        SELECT r, TRIM(f.key::VARCHAR) AS raw_month, f.value::VARCHAR AS raw_value
        FROM bronze,
             LATERAL FLATTEN(INPUT => r) AS f
        WHERE REGEXP_LIKE(UPPER(TRIM(f.key::VARCHAR)),
                          ''^([0-9]{4}|[0-9]{6}|JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)$'')
    ), parsed AS (
        SELECT
            CASE WHEN REGEXP_LIKE(raw_month, ''^[0-9]{6}$'') THEN TRY_TO_DATE(raw_month || ''01'', ''YYYYMMDD'')
                 WHEN REGEXP_LIKE(raw_month, ''^[0-9]{4}$'') THEN TRY_TO_DATE(''20'' || raw_month || ''01'', ''YYYYMMDD'')
                 ELSE DATE_FROM_PARTS(YEAR(CURRENT_DATE()),
                          DECODE(UPPER(raw_month), ''JAN'',1,''FEB'',2,''MAR'',3,''APR'',4,''MAY'',5,''JUN'',6,
                                 ''JUL'',7,''AUG'',8,''SEP'',9,''OCT'',10,''NOV'',11,''DEC'',12), 1)
            END                                                              AS base_month,
            GET_IGNORE_CASE(r, ''material'')::VARCHAR                          AS material_code,
            COALESCE(GET_IGNORE_CASE(r, ''material_description_new'')::VARCHAR,
                     GET_IGNORE_CASE(r, ''material_description_new_'')::VARCHAR) AS material_desc,
            GET_IGNORE_CASE(r, ''product_code'')::VARCHAR                      AS brand_code,
            GET_IGNORE_CASE(r, ''product'')::VARCHAR                           AS brand_name,
            GET_IGNORE_CASE(r, ''month_submission'')::VARCHAR                  AS brand_group,
            TRY_TO_DOUBLE(raw_value)                                         AS amount
        FROM u
    ), fx AS (
        SELECT rt_exchange_usd_to_krw
        FROM COM_AZK_AIBI_WRK.AIBI_SILVER.isl_fx_rates
        WHERE use_yn = ''Y''
        ORDER BY base_year DESC, _load_dtm DESC
        LIMIT 1
    )
    SELECT
        p.base_month,
        p.material_code,
        p.material_desc,
        p.brand_code,
        p.brand_name,
        p.brand_group,
        p.amount,
        (p.amount * 1000000)::DECIMAL(20,4)                                       AS amount_usd,
        ((p.amount * 1000000)::DECIMAL(20,4) * fx.rt_exchange_usd_to_krw)::DECIMAL(20,4) AS amount_krw,
        CONVERT_TIMEZONE(''UTC'', ''Asia/Seoul'', SYSDATE())::TIMESTAMP_NTZ(6)       AS _load_dtm
    FROM parsed AS p
    CROSS JOIN fx
    WHERE p.amount IS NOT NULL
      AND p.amount <> 0
      AND p.material_code IS NOT NULL
      AND COALESCE(LOWER(p.brand_code), '''') <> ''subtotal'';

    ------------------------------------------------------------------
    -- 4. 배치 검증 (Full refresh이므로 실패 시 덮어쓰기 차단)
    ------------------------------------------------------------------
    IF (NOT EXISTS (SELECT 1 FROM batch_isl_mmf)) THEN
        RAISE empty_batch;
    END IF;
    IF (EXISTS (SELECT 1 FROM batch_isl_mmf WHERE base_month IS NULL)) THEN
        RAISE invalid_month;
    END IF;

    ------------------------------------------------------------------
    -- 5. Full refresh (_src_synced 기록)
    ------------------------------------------------------------------
    INSERT OVERWRITE INTO COM_AZK_AIBI_WRK.AIBI_SILVER.isl_mmf_op_m_prep
        (base_month, material_code, material_desc, brand_code, brand_name, brand_group,
         amount, amount_usd, amount_krw, _load_dtm, _src_synced)
    SELECT base_month, material_code, material_desc, brand_code, brand_name, brand_group,
           amount, amount_usd, amount_krw, _load_dtm, :v_src_synced
    FROM batch_isl_mmf;
    v_inserted := SQLROWCOUNT;

    DROP TABLE IF EXISTS batch_isl_mmf;

    RETURN ''SUCCESS | overwritten, inserted='' || v_inserted
           || '' (_src_synced='' || v_src_synced || '')'';

EXCEPTION
    WHEN OTHER THEN
        RAISE;
END;
';


-- [14] SP_ISL_PATIENT_PROGRESS_TOP_HCP
-- ----------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE "SP_ISL_PATIENT_PROGRESS_TOP_HCP"()
RETURNS VARCHAR
LANGUAGE SQL
COMMENT='Top HCP tier mapping full refresh. Bronze patient_progress_tag_tophcp -> Silver isl_patient_progress_top_hcp. Skips when Bronze _fivetran_synced equals Silver _src_synced.'
EXECUTE AS CALLER
AS '
DECLARE
    missing_columns EXCEPTION (-20003, ''Source header contract broken: brick_name / top column not found in Bronze; target is unchanged.'');
    empty_batch     EXCEPTION (-20005, ''No rows in patient_progress_tag_tophcp; target is unchanged.'');
    duplicate_key   EXCEPTION (-20006, ''Duplicate hcp (brick_name) in source; joins would fan out. Target is unchanged.'');
    v_inserted     INTEGER DEFAULT 0;
    v_src_synced   TIMESTAMP_NTZ(6);   -- Bronze 현재 _fivetran_synced
    v_tgt_synced   TIMESTAMP_NTZ(6);   -- Silver에 마지막으로 적재된 Bronze _fivetran_synced
BEGIN
    USE SCHEMA COM_AZK_AIBI_WRK.AIBI_SILVER ;
    ------------------------------------------------------------------
    -- 0. 변경 여부 체크 (Bronze _fivetran_synced vs Silver _src_synced)
    ------------------------------------------------------------------
    SELECT MAX(b."_fivetran_synced"::TIMESTAMP_NTZ(6))
      INTO :v_src_synced
      FROM COM_AZK_AIBI_RAW_MDLS."aibi_bronze"."patient_progress_brands_tophca" AS b;

    SELECT MAX(_src_synced)
      INTO :v_tgt_synced
      FROM COM_AZK_AIBI_WRK.AIBI_SILVER.isl_patient_progress_top_hcp;

    IF (v_src_synced IS NULL) THEN
        RAISE empty_batch;
    END IF;
    IF (v_tgt_synced IS NOT NULL AND v_src_synced = v_tgt_synced) THEN
        RETURN ''SKIPPED | no change (_fivetran_synced='' || v_src_synced || '')'';
    END IF;

    ------------------------------------------------------------------
    -- 1. 소스 헤더 계약 확인 (brick_name, top)
    --    컬럼명이 바뀌면 GET_IGNORE_CASE가 조용히 NULL을 반환하므로 사전 차단
    ------------------------------------------------------------------
    IF ((SELECT COUNT(DISTINCT LOWER(TRIM(COLUMN_NAME)))
         FROM COM_AZK_AIBI_RAW_MDLS.INFORMATION_SCHEMA.COLUMNS
         WHERE TABLE_SCHEMA = ''aibi_bronze''
           AND TABLE_NAME   = ''patient_progress_brands_tophca'' 
           AND LOWER(TRIM(COLUMN_NAME)) IN (''brick_name'', ''top'')) < 2) THEN
        RAISE missing_columns;
    END IF;

    ------------------------------------------------------------------
    -- 2. 파싱 → 배치 임시 테이블
    ------------------------------------------------------------------
    CREATE OR REPLACE TEMP TABLE batch_isl_top_hcp AS
    WITH bronze AS (
        SELECT OBJECT_CONSTRUCT_KEEP_NULL(b.*) AS r
        FROM COM_AZK_AIBI_RAW_MDLS."aibi_bronze"."patient_progress_brands_tophca" AS b
    )
    SELECT
        GET_IGNORE_CASE(r, ''brick_name'')::VARCHAR                           AS hcp,
        GET_IGNORE_CASE(r, ''top'')::VARCHAR                                  AS tier,
        CONVERT_TIMEZONE(''UTC'', ''Asia/Seoul'', SYSDATE())::TIMESTAMP_NTZ(6) AS _load_dtm
    FROM bronze
    WHERE GET_IGNORE_CASE(r, ''brick_name'')::VARCHAR IS NOT NULL;

    ------------------------------------------------------------------
    -- 3. 배치 검증 (Full refresh이므로 실패 시 덮어쓰기 차단)
    ------------------------------------------------------------------
    IF (NOT EXISTS (SELECT 1 FROM batch_isl_top_hcp)) THEN
        RAISE empty_batch;
    END IF;
    IF (EXISTS (SELECT 1 FROM batch_isl_top_hcp GROUP BY hcp HAVING COUNT(*) > 1)) THEN
        RAISE duplicate_key;
    END IF;

    ------------------------------------------------------------------
    -- 4. Full refresh (_src_synced 기록)
    ------------------------------------------------------------------
    INSERT OVERWRITE INTO COM_AZK_AIBI_WRK.AIBI_SILVER.isl_patient_progress_top_hcp
        (hcp, tier, _load_dtm, _src_synced)
    SELECT hcp, tier, _load_dtm, :v_src_synced
    FROM batch_isl_top_hcp;
    v_inserted := SQLROWCOUNT;

    DROP TABLE IF EXISTS batch_isl_top_hcp;

    RETURN ''SUCCESS | overwritten, inserted='' || v_inserted
           || '' (_src_synced='' || v_src_synced || '')'';

EXCEPTION
    WHEN OTHER THEN
        RAISE;
END;
';


-- [15] SP_ISL_PATIENT_PROGRES_BRANDS
-- ----------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE "SP_ISL_PATIENT_PROGRES_BRANDS"()
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


-- [16] SP_ISL_PNR_LIST_OP_D_PREP
-- ----------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE "SP_ISL_PNR_LIST_OP_D_PREP"()
RETURNS VARCHAR
LANGUAGE SQL
COMMENT='Partner list (month-end file per month) partition replacement by base_date. Skips when Bronze _fivetran_synced equals Silver _src_synced.'
EXECUTE AS CALLER
AS '
DECLARE
  rows_inserted  INTEGER DEFAULT 0;
  v_src_synced   TIMESTAMP_NTZ(6);   -- Bronze 현재 _fivetran_synced
  v_tgt_synced   TIMESTAMP_NTZ(6);   -- Silver에 마지막으로 적재된 Bronze _fivetran_synced
BEGIN
    USE SCHEMA COM_AZK_AIBI_WRK.AIBI_SILVER ;
  -- 0. 변경 여부 체크 (Bronze _fivetran_synced vs Silver _src_synced)
  --    파일 누적 테이블: 새 파일 적재/기존 파일 재동기화 시 MAX가 바뀌어 감지됨
  SELECT MAX(b."_fivetran_synced"::TIMESTAMP_NTZ(6))
    INTO :v_src_synced
    FROM COM_AZK_AIBI_RAW_MDLS.aibi_bronze."patner_list" AS b;

  SELECT MAX(_src_synced)
    INTO :v_tgt_synced
    FROM COM_AZK_AIBI_WRK.AIBI_SILVER.isl_ptn_list_op_d_prep;

  IF (v_src_synced IS NULL) THEN
    RETURN ''SKIPPED | bronze source is empty'';
  END IF;
  IF (v_tgt_synced IS NOT NULL AND v_src_synced = v_tgt_synced) THEN
    RETURN ''SKIPPED | no change (_fivetran_synced='' || v_src_synced || '')'';
  END IF;

  -- 1. Bronze → 정제
  CREATE OR REPLACE TEMP TABLE batch_isl_pnr_list_op_d AS
  WITH bronze AS (
    SELECT OBJECT_CONSTRUCT_KEEP_NULL(b.*) AS r
    FROM COM_AZK_AIBI_RAW_MDLS.aibi_bronze."patner_list" b 
    QUALIFY TO_DATE(REGEXP_SUBSTR(b."#_file", ''\\\\d{4}-\\\\d{2}-\\\\d{2}''))
          = MAX(TO_DATE(REGEXP_SUBSTR(b."#_file", ''\\\\d{4}-\\\\d{2}-\\\\d{2}'')))
              OVER (PARTITION BY DATE_TRUNC(''MONTH'', TO_DATE(REGEXP_SUBSTR(b."#_file", ''\\\\d{4}-\\\\d{2}-\\\\d{2}''))))
  )
  SELECT
    GET_IGNORE_CASE(r,''billing_date'')::VARCHAR                 AS base_date,
    GET_IGNORE_CASE(r,''billing_document_number'')::VARCHAR      AS doc_no,
    GET_IGNORE_CASE(r,''billing_document_item_number'')::VARCHAR AS item_no,
    GET_IGNORE_CASE(r,''customer_code'')::VARCHAR                AS customer_code,
    GET_IGNORE_CASE(r,''customer_name'')::VARCHAR                AS customer_name,
    GET_IGNORE_CASE(r,''secondary_customer_code'')::VARCHAR      AS indirect_cust_code,
    GET_IGNORE_CASE(r,''secondary_customer_name'')::VARCHAR      AS indirect_cust_name,
    GET_IGNORE_CASE(r,''ship_to_code'')::VARCHAR                 AS ship_to_code,
    GET_IGNORE_CASE(r,''ship_to_name'')::VARCHAR                 AS ship_to_name,
    GET_IGNORE_CASE(r,''product_code'')::VARCHAR                 AS product_code,
    GET_IGNORE_CASE(r,''product_name'')::VARCHAR                 AS product_name,
    TRUNC(TRY_TO_DOUBLE(GET_IGNORE_CASE(r,''sales_quantity'')::VARCHAR))::INTEGER AS sales_qty,
    GET_IGNORE_CASE(r,''sales_unit'')::VARCHAR                   AS sales_uom,
    TRY_TO_DECIMAL(GET_IGNORE_CASE(r,''unit_price_net'')::VARCHAR,18,2)    AS unit_price_exvat,
    TRY_TO_DECIMAL(GET_IGNORE_CASE(r,''supply_amount_net'')::VARCHAR,18,2) AS supply_amount_exvat,
    TRY_TO_DECIMAL(GET_IGNORE_CASE(r,''vat_amount'')::VARCHAR,18,2)        AS vat_amount,
    TRY_TO_DECIMAL(GET_IGNORE_CASE(r,''sales_amount'')::VARCHAR,18,2)      AS sales_amount,
    GET_IGNORE_CASE(r,''sap_sales_doc_type_code'')::VARCHAR          AS sales_doc_type_code,
    GET_IGNORE_CASE(r,''sap_sales_doc_type_name'')::VARCHAR          AS sales_doc_type_name,
    CONVERT_TIMEZONE(''UTC'',''Asia/Seoul'',SYSDATE())::TIMESTAMP_NTZ(6) AS _load_dtm
  FROM bronze
  WHERE GET_IGNORE_CASE(r,''billing_document_number'')::VARCHAR IS NOT NULL;

  -- 2. 대상 일자 교체 (트랜잭션, _src_synced 기록)
  BEGIN
    BEGIN TRANSACTION;
    DELETE FROM COM_AZK_AIBI_WRK.AIBI_SILVER.isl_ptn_list_op_d_prep t
      USING (SELECT DISTINCT base_date FROM batch_isl_pnr_list_op_d) s
      WHERE t.base_date = s.base_date;
    INSERT INTO COM_AZK_AIBI_WRK.AIBI_SILVER.isl_ptn_list_op_d_prep
      (base_date, doc_no, item_no, customer_code, customer_name, indirect_cust_code, indirect_cust_name,
       ship_to_code, ship_to_name, product_code, product_name, sales_qty, sales_uom,
       unit_price_exvat, supply_amount_exvat, vat_amount, sales_amount,
       sales_doc_type_code, sales_doc_type_name, _load_dtm, _src_synced)
      SELECT b.*, :v_src_synced FROM batch_isl_pnr_list_op_d AS b;
    rows_inserted := SQLROWCOUNT;
    COMMIT;
  EXCEPTION
    WHEN OTHER THEN
      ROLLBACK;
      RAISE;
  END;

  RETURN ''isl_pnr_list_op_d_prep loaded: '' || rows_inserted || '' rows''
         || '' (_src_synced='' || v_src_synced || '')'';
END;
';


-- [17] SP_ISL_RDR_OP_M_PREP
-- ----------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE "SP_ISL_RDR_OP_M_PREP"()
RETURNS VARCHAR
LANGUAGE SQL
COMMENT='Revenue deduction rate full refresh. Bronze revenue_deduction_rates -> Silver isl_rdr_op_m_prep (material x month). Skips when Bronze _fivetran_synced equals Silver _src_synced.'
EXECUTE AS CALLER
AS '
DECLARE
    missing_date_columns EXCEPTION (-20003, ''No recognized date columns in Bronze revenue_deduction_rates; target is unchanged.'');
    empty_batch          EXCEPTION (-20005, ''No rows in deduction rate batch; target is unchanged.'');
    invalid_month        EXCEPTION (-20004, ''base_month could not be resolved from month column; target is unchanged.'');
    duplicate_key        EXCEPTION (-20006, ''Duplicate (base_month, material_code) in deduction rates; net sales join would fan out. Target is unchanged.'');
    v_inserted     INTEGER DEFAULT 0;
    v_src_synced   TIMESTAMP_NTZ(6);   -- Bronze 현재 _fivetran_synced
    v_tgt_synced   TIMESTAMP_NTZ(6);   -- Silver에 마지막으로 적재된 Bronze _fivetran_synced
BEGIN
    USE SCHEMA COM_AZK_AIBI_WRK.AIBI_SILVER ;
    ------------------------------------------------------------------
    -- 0. 변경 여부 체크 (Bronze _fivetran_synced vs Silver _src_synced)
    ------------------------------------------------------------------
    SELECT MAX(b."_fivetran_synced"::TIMESTAMP_NTZ(6))
      INTO :v_src_synced
      FROM COM_AZK_AIBI_RAW_MDLS.aibi_bronze."revenue_deduction_rates" AS b;

    SELECT MAX(_src_synced)
      INTO :v_tgt_synced
      FROM COM_AZK_AIBI_WRK.AIBI_SILVER.isl_rdr_op_m_prep;

    IF (v_src_synced IS NULL) THEN
        RAISE empty_batch;
    END IF;
    IF (v_tgt_synced IS NOT NULL AND v_src_synced = v_tgt_synced) THEN
        RETURN ''SKIPPED | no change (_fivetran_synced='' || v_src_synced || '')'';
    END IF;

    ------------------------------------------------------------------
    -- 1. Bronze 월 컬럼 존재 여부 확인
    ------------------------------------------------------------------
    IF (NOT EXISTS (SELECT 1 FROM COM_AZK_AIBI_RAW_MDLS.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ''aibi_bronze'' AND TABLE_NAME = ''revenue_deduction_rates''
          AND REGEXP_LIKE(UPPER(TRIM(COLUMN_NAME)),
                          ''^([0-9]{4}|[0-9]{6}|JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)$''))) THEN
        RAISE missing_date_columns;
    END IF;

    ------------------------------------------------------------------
    -- 2. Unpivot → 배치 임시 테이블
    ------------------------------------------------------------------
    CREATE OR REPLACE TEMP TABLE batch_isl_rdr AS
    WITH bronze AS (
        SELECT OBJECT_CONSTRUCT_KEEP_NULL(b.*) AS r
        FROM COM_AZK_AIBI_RAW_MDLS.aibi_bronze."revenue_deduction_rates" AS b
    ), u AS (
        SELECT r,
               TRIM(f.key::VARCHAR)                  AS raw_month,
               TRY_TO_DOUBLE(f.value::VARCHAR)       AS deduction_rate
        FROM bronze,
             LATERAL FLATTEN(INPUT => r) AS f
        WHERE REGEXP_LIKE(UPPER(TRIM(f.key::VARCHAR)),
                          ''^([0-9]{4}|[0-9]{6}|JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)$'')
    )
    SELECT
        CASE WHEN REGEXP_LIKE(raw_month, ''^[0-9]{6}$'') THEN TRY_TO_DATE(raw_month || ''01'', ''YYYYMMDD'')
             WHEN REGEXP_LIKE(raw_month, ''^[0-9]{4}$'') THEN TRY_TO_DATE(''20'' || raw_month || ''01'', ''YYYYMMDD'')
             ELSE DATE_FROM_PARTS(YEAR(CURRENT_DATE()),
                      DECODE(UPPER(raw_month), ''JAN'',1,''FEB'',2,''MAR'',3,''APR'',4,''MAY'',5,''JUN'',6,
                             ''JUL'',7,''AUG'',8,''SEP'',9,''OCT'',10,''NOV'',11,''DEC'',12), 1)
        END                                                                  AS base_month,
        GET_IGNORE_CASE(r, ''sku'')::VARCHAR                                   AS material_code,
        GET_IGNORE_CASE(r, ''sku_description'')::VARCHAR                       AS material_desc,
        GET_IGNORE_CASE(r, ''brand'')::VARCHAR                                 AS brand_name,
        deduction_rate,
        CONVERT_TIMEZONE(''UTC'', ''Asia/Seoul'', SYSDATE())::TIMESTAMP_NTZ(6)  AS _load_dtm
    FROM u
    WHERE deduction_rate IS NOT NULL
      AND deduction_rate <> 0
      AND GET_IGNORE_CASE(r, ''sku'')::VARCHAR IS NOT NULL;

    ------------------------------------------------------------------
    -- 3. 배치 검증 (Full refresh이므로 실패 시 덮어쓰기 차단)
    ------------------------------------------------------------------
    IF (NOT EXISTS (SELECT 1 FROM batch_isl_rdr)) THEN
        RAISE empty_batch;
    END IF;
    IF (EXISTS (SELECT 1 FROM batch_isl_rdr WHERE base_month IS NULL)) THEN
        RAISE invalid_month;
    END IF;
    IF (EXISTS (SELECT 1 FROM batch_isl_rdr
                GROUP BY base_month, material_code HAVING COUNT(*) > 1)) THEN
        RAISE duplicate_key;
    END IF;

    ------------------------------------------------------------------
    -- 4. Full refresh (_src_synced 기록)
    ------------------------------------------------------------------
    INSERT OVERWRITE INTO COM_AZK_AIBI_WRK.AIBI_SILVER.isl_rdr_op_m_prep
        (base_month, material_code, material_desc, brand_name, deduction_rate, _load_dtm, _src_synced)
    SELECT base_month, material_code, material_desc, brand_name, deduction_rate, _load_dtm, :v_src_synced
    FROM batch_isl_rdr;
    v_inserted := SQLROWCOUNT;

    DROP TABLE IF EXISTS batch_isl_rdr;

    RETURN ''SUCCESS | overwritten, inserted='' || v_inserted
           || '' (_src_synced='' || v_src_synced || '')'';

EXCEPTION
    WHEN OTHER THEN
        RAISE;
END;
';


-- [18] SP_ISL_REV_CL_M_PREP
-- ----------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE "SP_ISL_REV_CL_M_PREP"()
RETURNS VARCHAR
LANGUAGE SQL
COMMENT='Actual revenue/volume (CL) partition replacement by base_month. Skips when Bronze _fivetran_synced equals Silver _src_synced.'
EXECUTE AS CALLER
AS '
DECLARE
  missing_date_columns EXCEPTION (-20003,''No recognized month columns (jan~dec, jan_1~dec_1) in Bronze source; target is unchanged.'');
  ambiguous_first      EXCEPTION (-20001,''Conflicting duplicate pivot values. Resolve source duplicates.'');
  rows_inserted        INTEGER DEFAULT 0;
  v_src_synced         TIMESTAMP_NTZ(6);   -- Bronze 현재 _fivetran_synced
  v_tgt_synced         TIMESTAMP_NTZ(6);   -- Silver에 마지막으로 적재된 Bronze _fivetran_synced
BEGIN
    USE SCHEMA COM_AZK_AIBI_WRK.AIBI_SILVER ;
  -- 0. 변경 여부 체크 (Bronze _fivetran_synced vs Silver _src_synced)
  SELECT MAX(b."_fivetran_synced"::TIMESTAMP_NTZ(6))
    INTO :v_src_synced
    FROM COM_AZK_AIBI_RAW_MDLS.aibi_bronze."actual_total_revenue_and_volume" AS b;

  SELECT MAX(_src_synced)
    INTO :v_tgt_synced
    FROM COM_AZK_AIBI_WRK.AIBI_SILVER.isl_rev_cl_m_prep;

  IF (v_src_synced IS NULL) THEN
    RETURN ''SKIPPED | bronze source is empty'';
  END IF;
  IF (v_tgt_synced IS NOT NULL AND v_src_synced = v_tgt_synced) THEN
    RETURN ''SKIPPED | no change (_fivetran_synced='' || v_src_synced || '')'';
  END IF;

  -- 0-1. 원천 컬럼 검증
  IF (NOT EXISTS(SELECT 1 FROM COM_AZK_AIBI_RAW_MDLS.INFORMATION_SCHEMA.COLUMNS
      WHERE UPPER(TABLE_SCHEMA)=''AIBI_BRONZE'' AND LOWER(TABLE_NAME)=''actual_total_revenue_and_volume''
        AND REGEXP_LIKE(LOWER(TRIM(COLUMN_NAME)),''^(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)_1$''))) THEN
    RAISE missing_date_columns;
  END IF;

  -- 1. Unpivot (jan~dec = revenue, jan_1~dec_1 = volume)
  CREATE OR REPLACE TEMP TABLE sftran_rev_long AS
  WITH bronze AS (
    SELECT OBJECT_CONSTRUCT_KEEP_NULL(b.*) r
    FROM COM_AZK_AIBI_RAW_MDLS.aibi_bronze."actual_total_revenue_and_volume" b
  ), unpivoted AS (
    SELECT r,
      IFF(ENDSWITH(LOWER(TRIM(f.key::VARCHAR)),''_1''),''volume'',''revenue'') measure_name,
      DECODE(SPLIT_PART(LOWER(TRIM(f.key::VARCHAR)),''_'',1),
        ''jan'',1,''feb'',2,''mar'',3,''apr'',4,''may'',5,''jun'',6,''jul'',7,''aug'',8,''sep'',9,''oct'',10,''nov'',11,''dec'',12) month_num,
      TRY_TO_DOUBLE(NULLIF(REPLACE(TRIM(f.value::VARCHAR),'','',''''),'''')) measure_value
    FROM bronze, LATERAL FLATTEN(INPUT=>r) f
    WHERE REGEXP_LIKE(LOWER(TRIM(f.key::VARCHAR)),''^(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)(_1)?$'')
  )
  SELECT TRIM(GET_IGNORE_CASE(r,''material'')::VARCHAR) material_code,
    TRIM(GET_IGNORE_CASE(r,''material_description'')::VARCHAR) material_desc,
    TRIM(GET_IGNORE_CASE(r,''product'')::VARCHAR) brand_name,
    -- fy column holds FY for most rows; col_4 holds FY for FY26+
    IFF(REGEXP_LIKE(COALESCE(TRIM(GET_IGNORE_CASE(r,''fy'')::VARCHAR),''''), ''^FY[0-9]{2}$''),
        TRIM(GET_IGNORE_CASE(r,''fy'')::VARCHAR),
        TRIM(GET_IGNORE_CASE(r,''col_4'')::VARCHAR))                     AS fiscal_year,
    IFF(REGEXP_LIKE(COALESCE(TRIM(GET_IGNORE_CASE(r,''fy'')::VARCHAR),''''), ''^FY[0-9]{2}$''),
        TRIM(GET_IGNORE_CASE(r,''col_6'')::VARCHAR),
        TRIM(GET_IGNORE_CASE(r,''col_5'')::VARCHAR))                     AS value_type,
    IFF(REGEXP_LIKE(COALESCE(TRIM(GET_IGNORE_CASE(r,''fy'')::VARCHAR),''''), ''^FY[0-9]{2}$''),
        TRIM(GET_IGNORE_CASE(r,''col_7'')::VARCHAR),
        TRIM(GET_IGNORE_CASE(r,''col_6'')::VARCHAR))                     AS pr_material_code,
    month_num, measure_name, measure_value
  FROM unpivoted;

  -- 2. 중복 충돌 검증
  IF (EXISTS(SELECT 1 FROM sftran_rev_long
      GROUP BY material_code,material_desc,brand_name,fiscal_year,value_type,pr_material_code,month_num,measure_name
      HAVING COUNT(DISTINCT TO_JSON(OBJECT_CONSTRUCT_KEEP_NULL(''v'',measure_value)))>1)) THEN
    RAISE ambiguous_first;
  END IF;

  -- 3. Pivot + 정제
  CREATE OR REPLACE TEMP TABLE batch_isl_rev AS
  WITH pivoted AS (
    SELECT material_code,material_desc,brand_name,fiscal_year,value_type,pr_material_code,month_num,
      MAX(IFF(measure_name=''revenue'',measure_value,NULL))::DECIMAL(20,4) amount,
      MAX(IFF(measure_name=''volume'',measure_value,NULL))::DECIMAL(18,2) volume
    FROM sftran_rev_long
    GROUP BY material_code,material_desc,brand_name,fiscal_year,value_type,pr_material_code,month_num
  ), typed AS (
    SELECT *, 2000+TRY_TO_NUMBER(REGEXP_SUBSTR(UPPER(TRIM(fiscal_year)),''FY([0-9]{2})'',1,1,''e'',1)) year_num
    FROM pivoted
    WHERE amount IS NOT NULL
      AND (COALESCE(volume,0)<>0 OR amount<>0)
      AND material_code IS NOT NULL
      AND COALESCE(pr_material_code,'''')<>''Total_Product''
  )
  SELECT DATE_FROM_PARTS(year_num,month_num,1) base_month, material_code, material_desc, brand_name,
    fiscal_year, value_type, pr_material_code, year_num, amount, volume,
    CONVERT_TIMEZONE(''UTC'',''Asia/Seoul'',SYSDATE())::TIMESTAMP_NTZ(6) _load_dtm
  FROM typed
  WHERE year_num IS NOT NULL;

  -- 4. 대상 월 교체 (트랜잭션, _src_synced 기록)
  BEGIN
    BEGIN TRANSACTION;
    DELETE FROM COM_AZK_AIBI_WRK.AIBI_SILVER.isl_rev_cl_m_prep t
      USING (SELECT DISTINCT base_month FROM batch_isl_rev) s
      WHERE t.base_month = s.base_month;
    INSERT INTO COM_AZK_AIBI_WRK.AIBI_SILVER.isl_rev_cl_m_prep
      (base_month,material_code,material_desc,brand_name,fiscal_year,value_type,pr_material_code,year_num,amount,volume,_load_dtm,_src_synced)
      SELECT b.*, :v_src_synced FROM batch_isl_rev AS b;
    rows_inserted := SQLROWCOUNT;
    COMMIT;
  EXCEPTION
    WHEN OTHER THEN
      ROLLBACK;
      RAISE;
  END;

  RETURN ''isl_rev_cl_m_prep loaded: '' || rows_inserted || '' rows''
         || '' (_src_synced='' || v_src_synced || '')'';
END;
';


-- [19] SP_ISL_SALES_ENH_OP_D_PREP
-- ----------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE "SP_ISL_SALES_ENH_OP_D_PREP"()
RETURNS VARCHAR
LANGUAGE SQL
COMMENT='Enhertu daily sales partition replacement by base_date. Skips when Bronze _fivetran_synced equals Silver _src_synced.'
EXECUTE AS CALLER
AS '
DECLARE
  rows_inserted  INTEGER DEFAULT 0;
  v_src_synced   TIMESTAMP_NTZ(6);   -- Bronze 현재 _fivetran_synced
  v_tgt_synced   TIMESTAMP_NTZ(6);   -- Silver에 마지막으로 적재된 Bronze _fivetran_synced
BEGIN
    USE SCHEMA COM_AZK_AIBI_WRK.AIBI_SILVER ;
  -- 0. 변경 여부 체크 (Bronze _fivetran_synced vs Silver _src_synced)
  SELECT MAX(b."_fivetran_synced"::TIMESTAMP_NTZ(6))
    INTO :v_src_synced
    FROM COM_AZK_AIBI_RAW_MDLS.aibi_bronze."enhertu_daily_sales_order_list" AS b;

  SELECT MAX(_src_synced)
    INTO :v_tgt_synced
    FROM COM_AZK_AIBI_WRK.AIBI_SILVER.isl_sales_enh_op_d_prep;

  IF (v_src_synced IS NULL) THEN
    RETURN ''SKIPPED | bronze source is empty'';
  END IF;
  IF (v_tgt_synced IS NOT NULL AND v_src_synced = v_tgt_synced) THEN
    RETURN ''SKIPPED | no change (_fivetran_synced='' || v_src_synced || '')'';
  END IF;

  -- 1. Bronze → 정제
  CREATE OR REPLACE TEMP TABLE batch_isl_sales_enh_op_d AS
  WITH bronze AS (
    SELECT OBJECT_CONSTRUCT_KEEP_NULL(b.*) AS r
    FROM COM_AZK_AIBI_RAW_MDLS.aibi_bronze."enhertu_daily_sales_order_list" b
  )
  SELECT
    GET_IGNORE_CASE(r,''billing_date'')::VARCHAR                 AS base_date,
    GET_IGNORE_CASE(r,''billing_document_number'')::VARCHAR      AS doc_no,
    GET_IGNORE_CASE(r,''billing_document_item_number'')::VARCHAR AS item_no,
    GET_IGNORE_CASE(r,''customer_code'')::VARCHAR                AS customer_code,
    GET_IGNORE_CASE(r,''customer_name'')::VARCHAR                AS customer_name,
    GET_IGNORE_CASE(r,''secondary_customer_code'')::VARCHAR      AS indirect_cust_code,
    GET_IGNORE_CASE(r,''secondary_customer_name'')::VARCHAR      AS indirect_cust_name,
    GET_IGNORE_CASE(r,''ship_to_code'')::VARCHAR                 AS ship_to_code,
    GET_IGNORE_CASE(r,''ship_to_name'')::VARCHAR                 AS ship_to_name,
    GET_IGNORE_CASE(r,''product_code'')::VARCHAR                 AS product_code,
    GET_IGNORE_CASE(r,''product_name'')::VARCHAR                 AS product_name,
    TRUNC(TRY_TO_DOUBLE(GET_IGNORE_CASE(r,''sales_quantity'')::VARCHAR))::INTEGER AS sales_qty,
    GET_IGNORE_CASE(r,''sales_unit'')::VARCHAR                   AS sales_uom,
    TRY_TO_DECIMAL(GET_IGNORE_CASE(r,''unit_price_net'')::VARCHAR,18,2)    AS unit_price_exvat,
    TRY_TO_DECIMAL(GET_IGNORE_CASE(r,''supply_amount_net'')::VARCHAR,18,2) AS supply_amount_exvat,
    TRY_TO_DECIMAL(GET_IGNORE_CASE(r,''vat_amount'')::VARCHAR,18,2)        AS vat_amount,
    TRY_TO_DECIMAL(GET_IGNORE_CASE(r,''sales_amount'')::VARCHAR,18,2)      AS sales_amount,
    GET_IGNORE_CASE(r,''sap_sales_doc_type_code'')::VARCHAR          AS sales_doc_type_code,
    GET_IGNORE_CASE(r,''sap_sales_doc_type_name'')::VARCHAR          AS sales_doc_type_name,
    CONVERT_TIMEZONE(''UTC'',''Asia/Seoul'',SYSDATE())::TIMESTAMP_NTZ(6) AS _load_dtm
  FROM bronze
  WHERE GET_IGNORE_CASE(r,''billing_document_number'')::VARCHAR IS NOT NULL;

  -- 2. 대상 일자 교체 (트랜잭션, _src_synced 기록)
  BEGIN
    BEGIN TRANSACTION;
    DELETE FROM COM_AZK_AIBI_WRK.AIBI_SILVER.isl_sales_enh_op_d_prep t
      USING (SELECT DISTINCT base_date FROM batch_isl_sales_enh_op_d) s
      WHERE t.base_date = s.base_date;
    INSERT INTO COM_AZK_AIBI_WRK.AIBI_SILVER.isl_sales_enh_op_d_prep
      (base_date, doc_no, item_no, customer_code, customer_name, indirect_cust_code, indirect_cust_name,
       ship_to_code, ship_to_name, product_code, product_name, sales_qty, sales_uom,
       unit_price_exvat, supply_amount_exvat, vat_amount, sales_amount,
       sales_doc_type_code, sales_doc_type_name, _load_dtm, _src_synced)
      SELECT b.*, :v_src_synced FROM batch_isl_sales_enh_op_d AS b;
    rows_inserted := SQLROWCOUNT;
    COMMIT;
  EXCEPTION
    WHEN OTHER THEN
      ROLLBACK;
      RAISE;
  END;

  RETURN ''isl_sales_enh_op_d_prep loaded: '' || rows_inserted || '' rows''
         || '' (_src_synced='' || v_src_synced || '')'';
END;
';


-- [20] SP_ISL_SALES_OP_D_PREP
-- ----------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE "SP_ISL_SALES_OP_D_PREP"()
RETURNS VARCHAR
LANGUAGE SQL
COMMENT='Daily sales (KR10 MTD) partition replacement by base_date. Skips when Bronze _fivetran_synced equals Silver _src_synced. Preserves untouched dates, removes stale rows in loaded dates.'
EXECUTE AS CALLER
AS '
DECLARE
    empty_batch  EXCEPTION (-20005, ''No rows in kr10_mtd batch; target is unchanged.'');
    invalid_date EXCEPTION (-20004, ''base_date could not be parsed from doc_date; NULL-date rows would never be replaced. Target is unchanged.'');
    v_deleted      INTEGER DEFAULT 0;
    v_inserted     INTEGER DEFAULT 0;
    v_src_synced   TIMESTAMP_NTZ(6);   -- Bronze 현재 _fivetran_synced
    v_tgt_synced   TIMESTAMP_NTZ(6);   -- Silver에 마지막으로 적재된 Bronze _fivetran_synced
BEGIN
    USE SCHEMA COM_AZK_AIBI_WRK.AIBI_SILVER ;
    ------------------------------------------------------------------
    -- 0. 변경 여부 체크 (Bronze _fivetran_synced vs Silver _src_synced)
    ------------------------------------------------------------------
    SELECT MAX(b."_fivetran_synced"::TIMESTAMP_NTZ(6))
      INTO :v_src_synced
      FROM COM_AZK_AIBI_RAW_MDLS.aibi_bronze."kr_10_mtd" AS b;

    SELECT MAX(_src_synced)
      INTO :v_tgt_synced
      FROM COM_AZK_AIBI_WRK.AIBI_SILVER.isl_sales_op_d_prep;

    IF (v_src_synced IS NULL) THEN
        RAISE empty_batch;
    END IF;
    IF (v_tgt_synced IS NOT NULL AND v_src_synced = v_tgt_synced) THEN
        RETURN ''SKIPPED | no change (_fivetran_synced='' || v_src_synced || '')'';
    END IF;

    ------------------------------------------------------------------
    -- 1. Bronze 파싱 → 배치 임시 테이블
    ------------------------------------------------------------------
    CREATE OR REPLACE TEMP TABLE batch_isl_sales_op_d AS
    WITH bronze AS (
        SELECT OBJECT_CONSTRUCT_KEEP_NULL(b.*) AS r
        FROM COM_AZK_AIBI_RAW_MDLS.aibi_bronze."kr_10_mtd" AS b
    )
    SELECT
        -- YYYYMMDD 우선, 실패 시 자동 형식 인식(YYYY-MM-DD 등)으로 재시도
        COALESCE(TRY_TO_DATE(GET_IGNORE_CASE(r, ''doc_date'')::VARCHAR, ''YYYYMMDD''),
                 TRY_TO_DATE(GET_IGNORE_CASE(r, ''doc_date'')::VARCHAR))                        AS base_date,
        TRIM(GET_IGNORE_CASE(r, ''sold_to_pt'')::VARCHAR)                                        AS sold_to_pt,
        TRIM(GET_IGNORE_CASE(r, ''item'')::VARCHAR)                                              AS item_no,
        TRIM(GET_IGNORE_CASE(r, ''name_1'')::VARCHAR)                                            AS customer_name,
        TRIM(GET_IGNORE_CASE(r, ''ship_to'')::VARCHAR)                                           AS ship_to,
        TRIM(GET_IGNORE_CASE(r, ''material'')::VARCHAR)                                          AS material_code,
        TRIM(GET_IGNORE_CASE(r, ''description'')::VARCHAR)                                       AS material_desc,
        TRY_TO_DECIMAL(REPLACE(GET_IGNORE_CASE(r, ''actnetqty'')::VARCHAR, '','', ''''), 15, 2)      AS actual_net_qty,
        TRUNC(TRY_TO_DOUBLE(REPLACE(GET_IGNORE_CASE(r, ''net_price'')::VARCHAR, '','', '''')))::INTEGER AS net_price,
        TRY_TO_DECIMAL(REPLACE(GET_IGNORE_CASE(r, ''actnetval'')::VARCHAR, '','', ''''), 18, 2)      AS actual_net_value,
        TRY_TO_DECIMAL(REPLACE(GET_IGNORE_CASE(r, ''net_value'')::VARCHAR, '','', ''''), 18, 2)      AS net_value,
        TRIM(GET_IGNORE_CASE(r, ''po_number'')::VARCHAR)                                         AS po_number,
        TRIM(GET_IGNORE_CASE(r, ''saty'')::VARCHAR)                                              AS sales_doc_type,
        CONVERT_TIMEZONE(''UTC'', ''Asia/Seoul'', SYSDATE())::TIMESTAMP_NTZ(6)                    AS _load_dtm
    FROM bronze
    WHERE TRIM(GET_IGNORE_CASE(r, ''material'')::VARCHAR) IS NOT NULL;

    ------------------------------------------------------------------
    -- 2. 배치 검증 (실패 시 타겟 변경 없음)
    ------------------------------------------------------------------
    IF (NOT EXISTS (SELECT 1 FROM batch_isl_sales_op_d)) THEN
        RAISE empty_batch;
    END IF;
    -- NULL 날짜 행은 DELETE 조건(t.base_date = s.base_date)에 절대 매칭되지 않아
    -- 실행할 때마다 누적 중복됨 → 사전 차단
    IF (EXISTS (SELECT 1 FROM batch_isl_sales_op_d WHERE base_date IS NULL)) THEN
        RAISE invalid_date;
    END IF;

    ------------------------------------------------------------------
    -- 3. 일자 단위 DELETE + INSERT (트랜잭션, _src_synced 기록)
    ------------------------------------------------------------------
    BEGIN TRANSACTION;

    DELETE FROM COM_AZK_AIBI_WRK.AIBI_SILVER.isl_sales_op_d_prep AS t
    USING (SELECT DISTINCT base_date FROM batch_isl_sales_op_d) AS s
    WHERE t.base_date = s.base_date;
    v_deleted := SQLROWCOUNT;

    INSERT INTO COM_AZK_AIBI_WRK.AIBI_SILVER.isl_sales_op_d_prep
    SELECT b.*, :v_src_synced FROM batch_isl_sales_op_d AS b;
    v_inserted := SQLROWCOUNT;

    COMMIT;

    DROP TABLE IF EXISTS batch_isl_sales_op_d;

    RETURN ''SUCCESS | deleted='' || v_deleted || '', inserted='' || v_inserted
           || '' (_src_synced='' || v_src_synced || '')'';

EXCEPTION
    WHEN OTHER THEN
        ROLLBACK;
        RAISE;
END;
';


-- [21] SP_ISL_STK_AMT_VOL_CL_M_PREP
-- ----------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE "SP_ISL_STK_AMT_VOL_CL_M_PREP"()
RETURNS VARCHAR
LANGUAGE SQL
COMMENT='In-market stock amount/volume (CL) Bronze -> Silver prep. Sheet: Stock value(amount) / Stock vol(volume). Skips when Bronze _fivetran_synced (these sheets) equals Silver _src_synced.'
EXECUTE AS CALLER
AS '
DECLARE
    missing_date_columns EXCEPTION (-20003, ''No recognized date columns (col_YYMM[_n]) in Bronze source; target is unchanged.'');
    unmapped_channel     EXCEPTION (-20004, ''Unmapped column group suffix or invalid month; check channel_map. Target is unchanged.'');
    empty_batch          EXCEPTION (-20005, ''No rows for sheet filter (Stock value / Stock vol); target is unchanged.'');
    v_deleted      INTEGER DEFAULT 0;
    v_inserted     INTEGER DEFAULT 0;
    v_src_synced   TIMESTAMP_NTZ(6);   -- Bronze 현재 _fivetran_synced (Stock value / Stock vol 시트)
    v_tgt_synced   TIMESTAMP_NTZ(6);   -- Silver에 마지막으로 적재된 Bronze _fivetran_synced
BEGIN
    USE SCHEMA COM_AZK_AIBI_WRK.AIBI_SILVER ;
    ------------------------------------------------------------------
    -- 0. 변경 여부 체크 (Bronze _fivetran_synced vs Silver _src_synced)
    --    같은 Bronze 테이블을 In-market sales SP와 공유하므로 대상 시트로 한정
    ------------------------------------------------------------------
    SELECT MAX(b."_fivetran_synced"::TIMESTAMP_NTZ(6))
      INTO :v_src_synced
      FROM COM_AZK_AIBI_RAW_MDLS.aibi_bronze."stock_in_market_sales_stock" AS b
     WHERE b."_sheet_name" IN (''Stock value'', ''Stock vol'');

    SELECT MAX(_src_synced)
      INTO :v_tgt_synced
      FROM COM_AZK_AIBI_WRK.AIBI_SILVER.isl_stk_amt_vol_cl_m_prep;

    IF (v_src_synced IS NULL) THEN
        RAISE empty_batch;
    END IF;
    IF (v_tgt_synced IS NOT NULL AND v_src_synced = v_tgt_synced) THEN
        RETURN ''SKIPPED | no change (_fivetran_synced='' || v_src_synced || '')'';
    END IF;

    ------------------------------------------------------------------
    -- 1. Bronze 년월 컬럼 존재 여부 확인 (col_YYMM, col_YYMM_1, col_YYMM_2)
    ------------------------------------------------------------------
    IF (NOT EXISTS(SELECT 1 FROM COM_AZK_AIBI_RAW_MDLS.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ''aibi_bronze'' AND TABLE_NAME = ''stock_in_market_sales_stock''
          AND REGEXP_LIKE(UPPER(TRIM(COLUMN_NAME)), ''^COL_[0-9]{4}(_[0-9]+)?$''))) THEN
        RAISE missing_date_columns;
    END IF;

    ------------------------------------------------------------------
    -- 2. Unpivot + amount/volume 결합 → 배치 임시 테이블
    ------------------------------------------------------------------
    CREATE OR REPLACE TEMP TABLE batch_isl_stock_amount_volume AS
    WITH channel_map AS (
        -- 날짜 그룹 순서: TOTAL(col_YYMM), Channel(_1), Partner(_2)
        -- ※ TOTAL = Channel + Partner 합계 → 집계 시 TOTAL과 채널을 함께 SUM 하지 말 것
        SELECT column1 AS col_seq, column2 AS channel_type
        FROM VALUES
            (''0'', ''Total''),     -- col_YYMM
            (''1'', ''Channel''),   -- col_YYMM_1
            (''2'', ''Partner'')    -- col_YYMM_2
    ), amount_bronze AS (
        SELECT OBJECT_CONSTRUCT_KEEP_NULL(b.*) AS row_object
        FROM COM_AZK_AIBI_RAW_MDLS.aibi_bronze."stock_in_market_sales_stock" AS b
        WHERE b."_sheet_name" = ''Stock value''          -- 금액 데이터
    ), amount_long AS (
        SELECT
            row_object,
            REGEXP_SUBSTR(UPPER(TRIM(f.key::VARCHAR)), ''^COL_([0-9]{4})'', 1, 1, ''e'', 1) AS raw_month,
            COALESCE(REGEXP_SUBSTR(UPPER(TRIM(f.key::VARCHAR)), ''^COL_[0-9]{4}_([0-9]+)$'', 1, 1, ''e'', 1), ''0'') AS col_seq,
            TRY_TO_DOUBLE(NULLIF(TRIM(f.value::VARCHAR), ''''))::NUMBER(18,2) AS stock_amount
        FROM amount_bronze,
             LATERAL FLATTEN(INPUT => row_object) AS f
        WHERE REGEXP_LIKE(UPPER(TRIM(f.key::VARCHAR)), ''^COL_[0-9]{4}(_[0-9]+)?$'')   -- TOTAL(col_YYMM) + Channel/Partner(_n)
    ), amount_typed AS (
        SELECT
            TRY_TO_DATE(''20'' || l.raw_month || ''01'', ''YYYYMMDD'') AS base_month,
            TRIM(GET_IGNORE_CASE(l.row_object, ''material_mch_main'')::VARCHAR)   AS material_code,
            TRIM(GET_IGNORE_CASE(l.row_object, ''old_material_code'')::VARCHAR)   AS material_code_old,
            TRIM(GET_IGNORE_CASE(l.row_object, ''english_description'')::VARCHAR) AS material_desc,
            TRIM(GET_IGNORE_CASE(l.row_object, ''korean_description'')::VARCHAR)  AS material_desc_kr,
            TRIM(GET_IGNORE_CASE(l.row_object, ''brand_code'')::VARCHAR)          AS brand_code,
            TRIM(GET_IGNORE_CASE(l.row_object, ''product_name'')::VARCHAR)        AS brand_name,
            TRIM(GET_IGNORE_CASE(l.row_object, ''product_family'')::VARCHAR)      AS brand_group,
            TRIM(GET_IGNORE_CASE(l.row_object, ''ta'')::VARCHAR)                  AS therapy_area,
            TRIM(GET_IGNORE_CASE(l.row_object, ''bu'')::VARCHAR)                  AS business_unit,
            TRIM(GET_IGNORE_CASE(l.row_object, ''divested_partner'')::VARCHAR)    AS material_biz_type_cd,
            c.channel_type,
            l.stock_amount
        FROM amount_long AS l
        LEFT JOIN channel_map AS c ON l.col_seq = c.col_seq
    ), volume_bronze AS (
        SELECT OBJECT_CONSTRUCT_KEEP_NULL(b.*) AS row_object
        FROM COM_AZK_AIBI_RAW_MDLS.aibi_bronze."stock_in_market_sales_stock" AS b
        WHERE b."_sheet_name" = ''Stock vol''            -- 수량 데이터
    ), volume_long AS (
        SELECT
            row_object,
            REGEXP_SUBSTR(UPPER(TRIM(f.key::VARCHAR)), ''^COL_([0-9]{4})'', 1, 1, ''e'', 1) AS raw_month,
            COALESCE(REGEXP_SUBSTR(UPPER(TRIM(f.key::VARCHAR)), ''^COL_[0-9]{4}_([0-9]+)$'', 1, 1, ''e'', 1), ''0'') AS col_seq,
            TRY_TO_DOUBLE(NULLIF(TRIM(f.value::VARCHAR), ''''))::NUMBER(18,2) AS stock_volume
        FROM volume_bronze,
             LATERAL FLATTEN(INPUT => row_object) AS f
        WHERE REGEXP_LIKE(UPPER(TRIM(f.key::VARCHAR)), ''^COL_[0-9]{4}(_[0-9]+)?$'')   -- TOTAL(col_YYMM) + Channel/Partner(_n)
    ), volume_typed AS (
        SELECT
            TRY_TO_DATE(''20'' || l.raw_month || ''01'', ''YYYYMMDD'') AS base_month,
            TRIM(GET_IGNORE_CASE(l.row_object, ''material_mch_main'')::VARCHAR) AS material_code,
            TRIM(GET_IGNORE_CASE(l.row_object, ''divested_partner'')::VARCHAR)  AS material_biz_type_cd,
            c.channel_type,
            l.stock_volume
        FROM volume_long AS l
        LEFT JOIN channel_map AS c ON l.col_seq = c.col_seq
    )
    SELECT
        a.base_month,
        IFF(a.material_code = ''Enhertu'', ''2082 Enhertu'', a.material_code) AS material_code,
        a.material_code_old,
        a.material_desc,
        a.material_desc_kr,
        a.brand_code,
        a.brand_name,
        a.brand_group,
        a.therapy_area,
        a.business_unit,
        a.material_biz_type_cd,
        a.channel_type,
        a.stock_amount,
        v.stock_volume,
        CONVERT_TIMEZONE(''UTC'', ''Asia/Seoul'', SYSDATE())::TIMESTAMP_NTZ(6) AS _load_dtm
    FROM amount_typed AS a
    LEFT JOIN volume_typed AS v
      ON a.base_month = v.base_month
     AND a.material_code = v.material_code
     AND a.channel_type = v.channel_type
     AND a.material_biz_type_cd = v.material_biz_type_cd
    WHERE a.material_code IS NOT NULL
      AND NOT (
          (a.stock_amount IS NULL OR a.stock_amount = 0)
          AND (v.stock_volume IS NULL OR v.stock_volume = 0)
      );

    ------------------------------------------------------------------
    -- 3. 배치 검증 (실패 시 타겟 변경 없음)
    ------------------------------------------------------------------
    IF (NOT EXISTS(SELECT 1 FROM batch_isl_stock_amount_volume)) THEN
        RAISE empty_batch;
    END IF;
    IF (EXISTS(SELECT 1 FROM batch_isl_stock_amount_volume
        WHERE channel_type IS NULL OR base_month IS NULL)) THEN
        RAISE unmapped_channel;
    END IF;

    ------------------------------------------------------------------
    -- 4. 월 단위 DELETE + INSERT (트랜잭션, _src_synced 기록)
    ------------------------------------------------------------------
    BEGIN TRANSACTION;

    DELETE FROM COM_AZK_AIBI_WRK.AIBI_SILVER.isl_stk_amt_vol_cl_m_prep AS target
    USING (SELECT DISTINCT base_month FROM batch_isl_stock_amount_volume) AS batch
    WHERE target.base_month = batch.base_month;
    v_deleted := SQLROWCOUNT;

    INSERT INTO COM_AZK_AIBI_WRK.AIBI_SILVER.isl_stk_amt_vol_cl_m_prep
    SELECT b.*, :v_src_synced FROM batch_isl_stock_amount_volume AS b;
    v_inserted := SQLROWCOUNT;

    COMMIT;

    DROP TABLE IF EXISTS batch_isl_stock_amount_volume;

    RETURN ''SUCCESS | deleted='' || v_deleted || '', inserted='' || v_inserted
           || '' (_src_synced='' || v_src_synced || '')'';

EXCEPTION
    WHEN OTHER THEN
        ROLLBACK;
        RAISE;
END;
';


-- [22] SP_ISL_STK_INM_SALES_CL_M_PREP
-- ----------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE "SP_ISL_STK_INM_SALES_CL_M_PREP"()
RETURNS VARCHAR
LANGUAGE SQL
COMMENT='In-market sales (CL) Bronze -> Silver prep. Sheet: In-market sales (no channel split, col_YYMM only). Skips when Bronze _fivetran_synced (this sheet) equals Silver _src_synced.'
EXECUTE AS CALLER
AS '
DECLARE
    missing_date_columns EXCEPTION (-20003, ''No recognized date columns (col_YYMM) in Bronze source; target is unchanged.'');
    invalid_month        EXCEPTION (-20004, ''Invalid month in col_YYMM column name; target is unchanged.'');
    empty_batch          EXCEPTION (-20005, ''No rows for sheet filter (In-market sales); target is unchanged.'');
    v_deleted      INTEGER DEFAULT 0;
    v_inserted     INTEGER DEFAULT 0;
    v_src_synced   TIMESTAMP_NTZ(6);   -- Bronze 현재 _fivetran_synced (In-market sales 시트)
    v_tgt_synced   TIMESTAMP_NTZ(6);   -- Silver에 마지막으로 적재된 Bronze _fivetran_synced
BEGIN
    USE SCHEMA COM_AZK_AIBI_WRK.AIBI_SILVER ;
    ------------------------------------------------------------------
    -- 0. 변경 여부 체크 (Bronze _fivetran_synced vs Silver _src_synced)
    --    같은 Bronze 테이블을 Stock amount/volume SP와 공유하므로 대상 시트로 한정
    ------------------------------------------------------------------
    SELECT MAX(b."_fivetran_synced"::TIMESTAMP_NTZ(6))
      INTO :v_src_synced
      FROM COM_AZK_AIBI_RAW_MDLS.aibi_bronze."stock_in_market_sales_stock" AS b
     WHERE b."_sheet_name" = ''In-market sales'';

    SELECT MAX(_src_synced)
      INTO :v_tgt_synced
      FROM COM_AZK_AIBI_WRK.AIBI_SILVER.isl_stk_inm_sales_cl_m_prep;

    IF (v_src_synced IS NULL) THEN
        RAISE empty_batch;
    END IF;
    IF (v_tgt_synced IS NOT NULL AND v_src_synced = v_tgt_synced) THEN
        RETURN ''SKIPPED | no change (_fivetran_synced='' || v_src_synced || '')'';
    END IF;

    ------------------------------------------------------------------
    -- 1. Bronze 년월 컬럼 존재 여부 확인 (col_YYMM, 채널 구분 없음)
    ------------------------------------------------------------------
    IF (NOT EXISTS(SELECT 1 FROM COM_AZK_AIBI_RAW_MDLS.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ''aibi_bronze''
          AND TABLE_NAME   = ''stock_in_market_sales_stock''
          AND REGEXP_LIKE(UPPER(TRIM(COLUMN_NAME)), ''^COL_[0-9]{4}$''))) THEN
        RAISE missing_date_columns;
    END IF;

    ------------------------------------------------------------------
    -- 2. Unpivot → 배치 임시 테이블
    ------------------------------------------------------------------
    CREATE OR REPLACE TEMP TABLE batch_isl_inmarket AS
    WITH bronze AS (
        SELECT OBJECT_CONSTRUCT_KEEP_NULL(b.*) AS r
        FROM COM_AZK_AIBI_RAW_MDLS.aibi_bronze."stock_in_market_sales_stock" AS b
        WHERE b."_sheet_name" = ''In-market sales''          -- In-market sales 데이터
    ), u AS (
        SELECT
            r,
            REGEXP_SUBSTR(UPPER(TRIM(f.key::VARCHAR)), ''^COL_([0-9]{4})$'', 1, 1, ''e'', 1) AS raw_month,
            TRY_TO_DOUBLE(NULLIF(TRIM(f.value::VARCHAR), ''''))::NUMBER(18,2)          AS sales_amount
        FROM bronze,
             LATERAL FLATTEN(INPUT => r) AS f
        -- 채널 구분 없음: col_YYMM만 사용 (col_YYMM_1, col_YYMM_2 제외)
        WHERE REGEXP_LIKE(UPPER(TRIM(f.key::VARCHAR)), ''^COL_[0-9]{4}$'')
    )
    SELECT
        TRY_TO_DATE(''20'' || raw_month || ''01'', ''YYYYMMDD'') AS base_month,
        IFF(TRIM(GET_IGNORE_CASE(r, ''material_mch_main'')::VARCHAR) = ''Enhertu'', ''2082 Enhertu'',
            TRIM(GET_IGNORE_CASE(r, ''material_mch_main'')::VARCHAR))      AS material_code,
        TRIM(GET_IGNORE_CASE(r, ''old_material_code'')::VARCHAR)           AS material_code_old,
        TRIM(GET_IGNORE_CASE(r, ''english_description'')::VARCHAR)         AS material_desc,
        TRIM(GET_IGNORE_CASE(r, ''korean_description'')::VARCHAR)          AS material_desc_kr,
        TRIM(GET_IGNORE_CASE(r, ''brand_code'')::VARCHAR)                  AS brand_code,
        TRIM(GET_IGNORE_CASE(r, ''product_name'')::VARCHAR)                AS brand_name,
        TRIM(GET_IGNORE_CASE(r, ''product_family'')::VARCHAR)              AS brand_group,
        TRIM(GET_IGNORE_CASE(r, ''ta'')::VARCHAR)                          AS therapy_area,
        TRIM(GET_IGNORE_CASE(r, ''bu'')::VARCHAR)                          AS business_unit,
        TRIM(GET_IGNORE_CASE(r, ''divested_partner'')::VARCHAR)            AS material_biz_type_cd,
        sales_amount,
        CONVERT_TIMEZONE(''UTC'', ''Asia/Seoul'', SYSDATE())::TIMESTAMP_NTZ(6) AS _load_dtm
    FROM u
    WHERE sales_amount IS NOT NULL
      AND sales_amount <> 0
      AND TRIM(GET_IGNORE_CASE(r, ''material_mch_main'')::VARCHAR) IS NOT NULL;

    ------------------------------------------------------------------
    -- 3. 배치 검증 (실패 시 타겟 변경 없음)
    ------------------------------------------------------------------
    IF (NOT EXISTS(SELECT 1 FROM batch_isl_inmarket)) THEN
        RAISE empty_batch;
    END IF;
    IF (EXISTS(SELECT 1 FROM batch_isl_inmarket WHERE base_month IS NULL)) THEN
        RAISE invalid_month;
    END IF;

    ------------------------------------------------------------------
    -- 4. 월 단위 DELETE + INSERT (트랜잭션, _src_synced 기록)
    ------------------------------------------------------------------
    BEGIN TRANSACTION;

    DELETE FROM COM_AZK_AIBI_WRK.AIBI_SILVER.isl_stk_inm_sales_cl_m_prep AS t
    USING (SELECT DISTINCT base_month FROM batch_isl_inmarket) AS s
    WHERE t.base_month = s.base_month;
    v_deleted := SQLROWCOUNT;

    INSERT INTO COM_AZK_AIBI_WRK.AIBI_SILVER.isl_stk_inm_sales_cl_m_prep
    SELECT b.*, :v_src_synced FROM batch_isl_inmarket AS b;
    v_inserted := SQLROWCOUNT;

    COMMIT;

    DROP TABLE IF EXISTS batch_isl_inmarket;

    RETURN ''SUCCESS | deleted='' || v_deleted || '', inserted='' || v_inserted
           || '' (_src_synced='' || v_src_synced || '')'';

EXCEPTION
    WHEN OTHER THEN
        ROLLBACK;
        RAISE;
END;
';


-- [23] SP_LOAD_CALL_FREQUENCY
-- ----------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE "SP_LOAD_CALL_FREQUENCY"()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
BEGIN

  -- (1) STAGING : 4개 소스 UNION ALL -> 월 컬럼 FLATTEN
  INSERT OVERWRITE INTO COM_AZK_AIBI_WRK.AIBI_SILVER.EMD_CALL_FREQUENCY_STAGING
  SELECT
    s.BRAND_NAME,
    s.CRITERIA,
    s.MAIN_PLAYERS,
    s.CHANNEL_SPECIALTY,
    SUBSTR(f.key, 2, 4),
    SUBSTR(f.key, 7, 2),
    TO_DATE(SUBSTR(f.key, 2, 4) || ''-'' || SUBSTR(f.key, 7, 2) || ''-01''),
    TRY_TO_NUMBER(f.value::STRING, 15, 2),
    s.SRC_MODIFIED
  FROM (
    SELECT ''iOAD'' AS BRAND_NAME,                    -- TODO: Databricks brand_name 값과 동일하게
           "criteria_of_calls_" AS CRITERIA,
           "main_players"       AS MAIN_PLAYERS,
           "channel_specialty"  AS CHANNEL_SPECIALTY,
           "_modified"::TIMESTAMP_NTZ(6) AS SRC_MODIFIED,
           OBJECT_CONSTRUCT_KEEP_NULL(*) AS ROW_OBJ
    FROM COM_AZK_AIBI_RAW_MDLS."aibi_bronze"."kr_dashboard_call_spending_ioad_market_call_frequency"
    UNION ALL
    SELECT ''PPI'',
           "criteria_of_calls_", NULL, "channel_specialty", "_modified"::TIMESTAMP_NTZ(6),
           OBJECT_CONSTRUCT_KEEP_NULL(*)
    FROM COM_AZK_AIBI_RAW_MDLS."aibi_bronze"."kr_dashboard_call_spending_ppi_market_call_frequency"
    UNION ALL
    SELECT ''Respiratory'',                           -- TODO: Databricks brand_name 값과 동일하게
           "criteria_of_calls_", NULL, "channel_specialty", "_modified"::TIMESTAMP_NTZ(6),
           OBJECT_CONSTRUCT_KEEP_NULL(*)
    FROM COM_AZK_AIBI_RAW_MDLS."aibi_bronze"."kr_dashboard_call_spending_respiratory_call_frequency"
    UNION ALL
    SELECT ''Statin'',
           "criteria_of_calls_", NULL, "channel_specialty", "_modified"::TIMESTAMP_NTZ(6),
           OBJECT_CONSTRUCT_KEEP_NULL(*)
    FROM COM_AZK_AIBI_RAW_MDLS."aibi_bronze"."kr_dashboard_call_spending_stain_market_call_freqeuncy"
  ) s,
  LATERAL FLATTEN(input => s.ROW_OBJ) f
  WHERE REGEXP_LIKE(f.key, ''^_[0-9]{4}_[0-9]{2}_[0-9]{2}_t_00_00_00_z$'', ''i'');
 
  -- (2) HISTORY MERGE : 같은 키가 여러 행이면 _modified 가 최신인 행 채택
  MERGE INTO COM_AZK_AIBI_WRK.AIBI_SILVER.EMD_CALL_FREQUENCY AS t
  USING (
    SELECT *
    FROM COM_AZK_AIBI_WRK.AIBI_SILVER.EMD_CALL_FREQUENCY_STAGING
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY BRAND_NAME, CRITERIA, MAIN_PLAYERS, CHANNEL_SPECIALTY, YEAR, MM
      ORDER BY SRC_MODIFIED DESC NULLS LAST, VISIT_CALL DESC NULLS LAST
    ) = 1
  ) AS s
  ON  t.BRAND_NAME        IS NOT DISTINCT FROM s.BRAND_NAME
  AND t.CRITERIA          IS NOT DISTINCT FROM s.CRITERIA
  AND t.MAIN_PLAYERS      IS NOT DISTINCT FROM s.MAIN_PLAYERS
  AND t.CHANNEL_SPECIALTY IS NOT DISTINCT FROM s.CHANNEL_SPECIALTY
  AND t.YEAR              IS NOT DISTINCT FROM s.YEAR
  AND t.MM                IS NOT DISTINCT FROM s.MM
  WHEN MATCHED THEN UPDATE SET
    t.YYYYMMDD   = s.YYYYMMDD,
    t.VISIT_CALL = s.VISIT_CALL
  WHEN NOT MATCHED THEN INSERT
    (BRAND_NAME, CRITERIA, MAIN_PLAYERS, CHANNEL_SPECIALTY, YEAR, MM, YYYYMMDD, VISIT_CALL)
  VALUES
    (s.BRAND_NAME, s.CRITERIA, s.MAIN_PLAYERS, s.CHANNEL_SPECIALTY, s.YEAR, s.MM, s.YYYYMMDD, s.VISIT_CALL);
 
  -- (3) GOLD : history 전체를 그대로 복사 (Databricks 의 curated INSERT OVERWRITE 대응)
  INSERT OVERWRITE INTO COM_AZK_AIBI_PUB.AIBI_GOLD.EMD_CALL_FREQUENCY
  SELECT BRAND_NAME, CRITERIA, MAIN_PLAYERS, CHANNEL_SPECIALTY, YEAR, MM, YYYYMMDD, VISIT_CALL
  FROM COM_AZK_AIBI_WRK.AIBI_SILVER.EMD_CALL_FREQUENCY;
 
  RETURN ''OK'';
END;
';


-- [24] SP_LOAD_CO_PROMO_EACH_MARKET
-- ----------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE "SP_LOAD_CO_PROMO_EACH_MARKET"()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
BEGIN
  -- (1) STAGING : 두 시트를 한 번에 FLATTEN 하여 같은 (채널/전문과, 제품, 연, 월) 한 행에 합침
  --     Detail Frequency -> RX_COUNT,  SOV -> RX_SHARE
  --     값이 NULL 인 행은 유지하고, 모든 행이 NULL 인 월(미래의 빈 월 컬럼)만 제외 (MONTH_HAS_DATA)
  INSERT OVERWRITE INTO COM_AZK_AIBI_WRK.AIBI_SILVER.EMD_CO_PROMO_EACH_MARKET_STAGING
  SELECT
    x.CHANNEL_SPECIALTY,
    x.BRAND_NAME,
    x.YEAR,
    x.MM,
    TO_DATE(x.YEAR || ''-'' || x.MM || ''-01''),
    ROUND(x.RX_COUNT, 2)::NUMBER(15,2),
    ROUND(x.RX_SHARE, 6)::NUMBER(15,6),
    x.SRC_MODIFIED
  FROM (
    SELECT y.*,
           MAX(IFF(y.RX_COUNT IS NOT NULL OR y.RX_SHARE IS NOT NULL, 1, 0))
             OVER (PARTITION BY y.YEAR, y.MM) AS MONTH_HAS_DATA
    FROM (
      SELECT
        t.CHANNEL_SPECIALTY,
        t.BRAND_NAME,
        SUBSTR(f.key, 5, 4)  AS YEAR,
        SUBSTR(f.key, 10, 2) AS MM,
        MAX(IFF(t."_sheet_name" = ''Detail Frequency'', TRY_TO_DOUBLE(TRIM(REPLACE(f.value::STRING, '','', ''''))), NULL)) AS RX_COUNT,
        MAX(IFF(t."_sheet_name" = ''SOV'',              TRY_TO_DOUBLE(TRIM(REPLACE(f.value::STRING, '','', ''''))), NULL)) AS RX_SHARE,
        MAX(t."_modified"::TIMESTAMP_NTZ(6)) AS SRC_MODIFIED
      FROM (
        -- OBJECT_CONSTRUCT_KEEP_NULL(*) 는 SELECT 절에서만 쓸 수 있으므로 서브쿼리에서 먼저 만든다
        SELECT "_sheet_name", "_modified",
               NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("specialty"::STRING, CHR(12288), '' ''), CHR(160), '' ''), '' +'', '' '')), '''') AS CHANNEL_SPECIALTY,
               NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("product_name"::STRING, CHR(12288), '' ''), CHR(160), '' ''), '' +'', '' '')), '''') AS BRAND_NAME,
               OBJECT_CONSTRUCT_KEEP_NULL(*) AS ROW_OBJ
        FROM COM_AZK_AIBI_RAW_MDLS."aibi_bronze"."azk_co_promotion_company_report"
        WHERE "_sheet_name" IN (''Detail Frequency'', ''SOV'')
          AND COALESCE("_fivetran_deleted", FALSE) = FALSE
          AND NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("product_name"::STRING, CHR(12288), '' ''), CHR(160), '' ''), '' +'', '' '')), '''') IS NOT NULL
      ) t,
      LATERAL FLATTEN(input => t.ROW_OBJ) f
      WHERE REGEXP_LIKE(f.key, ''^col_[0-9]{4}_[0-9]{2}_[0-9]{2}_00_00_00(_1)?$'', ''i'')
      GROUP BY 1, 2, 3, 4
    ) y
  ) x
  WHERE x.MONTH_HAS_DATA = 1;
 
  -- (2) HISTORY MERGE : staging 은 이미 키별 1행.  새 값이 NULL 이면 기존 값을 유지
  MERGE INTO COM_AZK_AIBI_WRK.AIBI_SILVER.EMD_CO_PROMO_EACH_MARKET AS t
  USING COM_AZK_AIBI_WRK.AIBI_SILVER.EMD_CO_PROMO_EACH_MARKET_STAGING AS s
  ON  t.CHANNEL_SPECIALTY IS NOT DISTINCT FROM s.CHANNEL_SPECIALTY
  AND t.BRAND_NAME        IS NOT DISTINCT FROM s.BRAND_NAME
  AND t.YEAR              IS NOT DISTINCT FROM s.YEAR
  AND t.MM                IS NOT DISTINCT FROM s.MM
  WHEN MATCHED THEN UPDATE SET
    t.YYYYMMDD = s.YYYYMMDD,
    t.RX_COUNT = COALESCE(s.RX_COUNT, t.RX_COUNT),
    t.RX_SHARE = COALESCE(s.RX_SHARE, t.RX_SHARE)
  WHEN NOT MATCHED THEN INSERT
    (CHANNEL_SPECIALTY, BRAND_NAME, YEAR, MM, YYYYMMDD, RX_COUNT, RX_SHARE)
  VALUES
    (s.CHANNEL_SPECIALTY, s.BRAND_NAME, s.YEAR, s.MM, s.YYYYMMDD, s.RX_COUNT, s.RX_SHARE);
 
  -- (3) GOLD : history 전체를 그대로 복사
  INSERT OVERWRITE INTO COM_AZK_AIBI_PUB.AIBI_GOLD.EMD_CO_PROMO_EACH_MARKET
  SELECT CHANNEL_SPECIALTY, BRAND_NAME, YEAR, MM, YYYYMMDD, RX_COUNT, RX_SHARE
  FROM COM_AZK_AIBI_WRK.AIBI_SILVER.EMD_CO_PROMO_EACH_MARKET;
 
  RETURN ''OK'';
END;
';


-- [25] SP_LOAD_CO_PROMO_REPR_COMPANY
-- ----------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE "SP_LOAD_CO_PROMO_REPR_COMPANY"()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
BEGIN
  -- (1) STAGING : 월 컬럼을 FLATTEN 하고, 키 이름 끝이 ''_1'' 이면 백분율(RX_SHARE), 아니면 value(RX_COUNT)
  --     같은 원본 행(파일, 줄) + 연 + 월 로 묶어 두 값을 한 행의 두 컬럼으로 합침
  --     값이 NULL 인 행도 유지하되, 모든 행이 NULL 인 월(미래의 빈 월 컬럼)만 제외 (MONTH_HAS_DATA)
  INSERT OVERWRITE INTO COM_AZK_AIBI_WRK.AIBI_SILVER.EMD_CO_PROMO_REPR_COMPANY_STAGING
  SELECT
    x.CORPORATION,
    x.CHANNEL,
    x.RANK_NO,
    x.MANUFACTURER,
    x.LOCAL_BRAND_NAME,
    x.YEAR,
    x.MM,
    TO_DATE(x.YEAR || ''-'' || x.MM || ''-01''),
    ROUND(x.RX_COUNT)::NUMBER(15,0),
    ROUND(x.RX_SHARE, 6)::NUMBER(15,6),
    x.SRC_MODIFIED
  FROM (
    SELECT y.*,
           MAX(IFF(y.RX_COUNT IS NOT NULL OR y.RX_SHARE IS NOT NULL, 1, 0))
             OVER (PARTITION BY y.YEAR, y.MM) AS MONTH_HAS_DATA
    FROM (
      SELECT
        t."#_file"                         AS SRC_FILE,
        t."_line"                          AS SRC_LINE,
        UPPER(TRIM(t."_sheet_name"))       AS CORPORATION,
        UPPER(TRIM(t."priv_hosp"))         AS CHANNEL,            -- TODO: 0)(b) 확인 후 필요하면 수정
        TRY_TO_NUMBER(t."rank"::STRING, 10, 0) AS RANK_NO,   -- rank 에 숫자가 아닌 값이 섞여 있어도 에러 대신 NULL
        t."manufacturer"                   AS MANUFACTURER,
        t."local_brand"                    AS LOCAL_BRAND_NAME,
        SUBSTR(f.key, 5, 4)                AS YEAR,
        SUBSTR(f.key, 10, 2)               AS MM,
        MAX(IFF(NOT ENDSWITH(LOWER(f.key), ''_1''), f.value::FLOAT, NULL)) AS RX_COUNT,
        MAX(IFF(    ENDSWITH(LOWER(f.key), ''_1''), f.value::FLOAT, NULL)) AS RX_SHARE,
        MAX(t."_modified"::TIMESTAMP_NTZ(6)) AS SRC_MODIFIED
      FROM (
        -- OBJECT_CONSTRUCT_KEEP_NULL(*) 는 SELECT 절에서만 쓸 수 있으므로 서브쿼리에서 먼저 만든다
        SELECT "#_file", "_line", "_sheet_name", "priv_hosp", "rank", "manufacturer", "local_brand", "_modified",
               OBJECT_CONSTRUCT_KEEP_NULL(*) AS ROW_OBJ
        FROM COM_AZK_AIBI_RAW_MDLS."aibi_bronze"."azk_co_promotion_company_report"
        WHERE UPPER(TRIM("_sheet_name")) IN
                (''ALVOGEN KOREA'',''BLUE PHARM KOREA'',''DAE WOONG'',''DKSH'',''DAIICHISANKYO'',''GC PHARMA'',
                 ''HK INNO.N'',''HYUNDAI PH.'',''IL DONG'',''SK CHEMICALS'',''IL YANG'')
          AND COALESCE("_fivetran_deleted", FALSE) = FALSE
      ) t,
      LATERAL FLATTEN(input => t.ROW_OBJ) f
      WHERE REGEXP_LIKE(f.key, ''^col_[0-9]{4}_[0-9]{2}_[0-9]{2}_00_00_00(_1)?$'', ''i'')
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
    ) y
  ) x
  WHERE x.MONTH_HAS_DATA = 1;
 
  -- (2) HISTORY MERGE : 같은 키가 여러 행이면 _modified 가 최신인 행 채택
  MERGE INTO COM_AZK_AIBI_WRK.AIBI_SILVER.EMD_CO_PROMO_REPR_COMPANY AS t
  USING (
    SELECT *
    FROM COM_AZK_AIBI_WRK.AIBI_SILVER.EMD_CO_PROMO_REPR_COMPANY_STAGING
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY CORPORATION, CHANNEL, MANUFACTURER, LOCAL_BRAND_NAME, YEAR, MM
      ORDER BY SRC_MODIFIED DESC NULLS LAST, RX_COUNT DESC NULLS LAST
    ) = 1
  ) AS s
  ON  t.CORPORATION      IS NOT DISTINCT FROM s.CORPORATION
  AND t.CHANNEL          IS NOT DISTINCT FROM s.CHANNEL
  AND t.MANUFACTURER     IS NOT DISTINCT FROM s.MANUFACTURER
  AND t.LOCAL_BRAND_NAME IS NOT DISTINCT FROM s.LOCAL_BRAND_NAME
  AND t.YEAR             IS NOT DISTINCT FROM s.YEAR
  AND t.MM               IS NOT DISTINCT FROM s.MM
  WHEN MATCHED THEN UPDATE SET
    t.RANK     = s.RANK,
    t.YYYYMMDD = s.YYYYMMDD,
    t.RX_COUNT = s.RX_COUNT,
    t.RX_SHARE = s.RX_SHARE
  WHEN NOT MATCHED THEN INSERT
    (CORPORATION, CHANNEL, RANK, MANUFACTURER, LOCAL_BRAND_NAME, YEAR, MM, YYYYMMDD, RX_COUNT, RX_SHARE)
  VALUES
    (s.CORPORATION, s.CHANNEL, s.RANK, s.MANUFACTURER, s.LOCAL_BRAND_NAME, s.YEAR, s.MM, s.YYYYMMDD, s.RX_COUNT, s.RX_SHARE);
 
  -- (3) GOLD : history 전체를 그대로 복사
  INSERT OVERWRITE INTO COM_AZK_AIBI_PUB.AIBI_GOLD.EMD_CO_PROMO_REPR_COMPANY
  SELECT CORPORATION, CHANNEL, RANK, MANUFACTURER, LOCAL_BRAND_NAME, YEAR, MM, YYYYMMDD, RX_COUNT, RX_SHARE
  FROM COM_AZK_AIBI_WRK.AIBI_SILVER.EMD_CO_PROMO_REPR_COMPANY;
 
  RETURN ''OK'';
END;
';


-- [26] SP_LOAD_ENPIRE_BRICK_DATA
-- ----------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE "SP_LOAD_ENPIRE_BRICK_DATA"()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
BEGIN
  -- (1) STAGING : bronze 컬럼명을 대상 컬럼명으로 바꾸고 타입 정리
  INSERT OVERWRITE INTO COM_AZK_AIBI_WRK.AIBI_SILVER.EMD_ENPIRE_BRICK_DATA_STAGING
  SELECT
    "year_month"  AS YEAR_MM,
    "year_qtr"  AS YEAR_QTR,
    "year_half"  AS YEAR_HALF,
    "brand"  AS BRAND,
    "brand_code"  AS BRAND_CODE,
    "brick_code"  AS BRICK_CODE,
    "brick_name"  AS BRICK_NAME,
    "channel"  AS CHANNEL,
    "priority"  AS PRIORITY,
    "bu"  AS BU,
    "user_prid"  AS USER_PRID,
    "user_name"  AS USER_NAME,
    "dsm_prid"  AS DSM_PRID,
    "dsm_name"  AS DSM_NAME,
    "dso_name"  AS DSO_NAME,
    "current_user"  AS CUR_USER,
    "current_user_prid"  AS CUR_USER_PRID,
    "current_dsm"  AS CUR_DSM,
    "current_dsm_prid"  AS CUR_DSM_PRID,
    "current_dso"  AS CUR_DSO,
    "current_team"  AS CUR_TEAM,
    "sales_target"::FLOAT  AS SALES_TARGET,
    "sales_tracking"::FLOAT  AS SALES_TRACKING,
    "sales_tracking_vol"::FLOAT  AS SALES_TRACKING_VOL,
    "achieve_rate_"::FLOAT  AS ACHIEVE_RATE,
    "g_r_"::FLOAT  AS GROWTH_RATE,
    "m_s_"::FLOAT  AS M_S,
    "m_s_vol"::FLOAT  AS M_S_VOL,
    "ddd_market"::FLOAT  AS DDD_MARKET,
    "ddd_market_vol"::FLOAT  AS DDD_MARKET_VOL,
    "ddd_az_"::FLOAT  AS DDD_AZ,
    "ddd_az_vol"::FLOAT  AS DDD_AZ_VOL,
    "invoice_projcetion"::FLOAT  AS INVOICE_PROJCETION,
    "invoice_projcetion_2014_"::FLOAT  AS INVOICE_PROJCETION_2014,
    "invoice_projcetion_2015_"::FLOAT  AS INVOICE_PROJCETION_2015,
    "_line"                                AS SRC_LINE,
    "_modified"::TIMESTAMP_NTZ(6)          AS SRC_MODIFIED
  FROM COM_AZK_AIBI_RAW_MDLS."aibi_bronze"."enpire_kr_brick_data";
 
  -- (2) HISTORY MERGE : 같은 키가 여러 행이면 _modified 가 최신인 행 채택
  MERGE INTO COM_AZK_AIBI_WRK.AIBI_SILVER.EMD_ENPIRE_BRICK_DATA AS t
  USING (
    SELECT *
    FROM COM_AZK_AIBI_WRK.AIBI_SILVER.EMD_ENPIRE_BRICK_DATA_STAGING
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY YEAR_MM, BRAND_CODE, BRICK_CODE, USER_PRID
      ORDER BY SRC_MODIFIED DESC NULLS LAST, SRC_LINE DESC
    ) = 1
  ) AS s
  ON  t.YEAR_MM      IS NOT DISTINCT FROM s.YEAR_MM
  AND t.BRAND_CODE   IS NOT DISTINCT FROM s.BRAND_CODE
  AND t.BRICK_CODE   IS NOT DISTINCT FROM s.BRICK_CODE
  AND t.USER_PRID    IS NOT DISTINCT FROM s.USER_PRID
  WHEN MATCHED THEN UPDATE SET
    t.YEAR_QTR = s.YEAR_QTR,
    t.YEAR_HALF = s.YEAR_HALF,
    t.BRAND = s.BRAND,
    t.BRICK_NAME = s.BRICK_NAME,
    t.CHANNEL = s.CHANNEL,
    t.PRIORITY = s.PRIORITY,
    t.BU = s.BU,
    t.USER_NAME = s.USER_NAME,
    t.DSM_PRID = s.DSM_PRID,
    t.DSM_NAME = s.DSM_NAME,
    t.DSO_NAME = s.DSO_NAME,
    t.CUR_USER = s.CUR_USER,
    t.CUR_USER_PRID = s.CUR_USER_PRID,
    t.CUR_DSM = s.CUR_DSM,
    t.CUR_DSM_PRID = s.CUR_DSM_PRID,
    t.CUR_DSO = s.CUR_DSO,
    t.CUR_TEAM = s.CUR_TEAM,
    t.SALES_TARGET = s.SALES_TARGET,
    t.SALES_TRACKING = s.SALES_TRACKING,
    t.SALES_TRACKING_VOL = s.SALES_TRACKING_VOL,
    t.ACHIEVE_RATE = s.ACHIEVE_RATE,
    t.GROWTH_RATE = s.GROWTH_RATE,
    t.M_S = s.M_S,
    t.M_S_VOL = s.M_S_VOL,
    t.DDD_MARKET = s.DDD_MARKET,
    t.DDD_MARKET_VOL = s.DDD_MARKET_VOL,
    t.DDD_AZ = s.DDD_AZ,
    t.DDD_AZ_VOL = s.DDD_AZ_VOL,
    t.INVOICE_PROJCETION = s.INVOICE_PROJCETION,
    t.INVOICE_PROJCETION_2014 = s.INVOICE_PROJCETION_2014,
    t.INVOICE_PROJCETION_2015 = s.INVOICE_PROJCETION_2015
  WHEN NOT MATCHED THEN INSERT
    (YEAR_MM, YEAR_QTR, YEAR_HALF, BRAND, BRAND_CODE, BRICK_CODE, BRICK_NAME, CHANNEL, PRIORITY, BU, USER_PRID, USER_NAME, DSM_PRID, DSM_NAME, DSO_NAME, CUR_USER, CUR_USER_PRID, CUR_DSM, CUR_DSM_PRID, CUR_DSO, CUR_TEAM, SALES_TARGET, SALES_TRACKING, SALES_TRACKING_VOL, ACHIEVE_RATE, GROWTH_RATE, M_S, M_S_VOL, DDD_MARKET, DDD_MARKET_VOL, DDD_AZ, DDD_AZ_VOL, INVOICE_PROJCETION, INVOICE_PROJCETION_2014, INVOICE_PROJCETION_2015)
  VALUES
    (s.YEAR_MM, s.YEAR_QTR, s.YEAR_HALF, s.BRAND, s.BRAND_CODE, s.BRICK_CODE, s.BRICK_NAME, s.CHANNEL, s.PRIORITY, s.BU, s.USER_PRID, s.USER_NAME, s.DSM_PRID, s.DSM_NAME, s.DSO_NAME, s.CUR_USER, s.CUR_USER_PRID, s.CUR_DSM, s.CUR_DSM_PRID, s.CUR_DSO, s.CUR_TEAM, s.SALES_TARGET, s.SALES_TRACKING, s.SALES_TRACKING_VOL, s.ACHIEVE_RATE, s.GROWTH_RATE, s.M_S, s.M_S_VOL, s.DDD_MARKET, s.DDD_MARKET_VOL, s.DDD_AZ, s.DDD_AZ_VOL, s.INVOICE_PROJCETION, s.INVOICE_PROJCETION_2014, s.INVOICE_PROJCETION_2015);
 
  -- (3) GOLD : history 전체를 그대로 복사
  INSERT OVERWRITE INTO COM_AZK_AIBI_PUB.AIBI_GOLD.EMD_ENPIRE_BRICK_DATA
  SELECT YEAR_MM, YEAR_QTR, YEAR_HALF, BRAND, BRAND_CODE, BRICK_CODE, BRICK_NAME, CHANNEL, PRIORITY, BU, USER_PRID, USER_NAME, DSM_PRID, DSM_NAME, DSO_NAME, CUR_USER, CUR_USER_PRID, CUR_DSM, CUR_DSM_PRID, CUR_DSO, CUR_TEAM, SALES_TARGET, SALES_TRACKING, SALES_TRACKING_VOL, ACHIEVE_RATE, GROWTH_RATE, M_S, M_S_VOL, DDD_MARKET, DDD_MARKET_VOL, DDD_AZ, DDD_AZ_VOL, INVOICE_PROJCETION, INVOICE_PROJCETION_2014, INVOICE_PROJCETION_2015
  FROM COM_AZK_AIBI_WRK.AIBI_SILVER.EMD_ENPIRE_BRICK_DATA;
 
  RETURN ''OK'';
END;
';


-- [27] SP_LOAD_ENPIRE_DDD_DATA
-- ----------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE "SP_LOAD_ENPIRE_DDD_DATA"()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
BEGIN
  -- (1) STAGING : bronze 컬럼명을 대상 컬럼명으로 바꾸고 타입 정리
  INSERT OVERWRITE INTO COM_AZK_AIBI_WRK.AIBI_SILVER.EMD_ENPIRE_DDD_DATA_STAGING
  SELECT
    TO_VARCHAR("year_month")  AS YEAR_MM,
    "brick_code"  AS BRICK_CODE,
    "brick_name"  AS BRICK_NAME,
    "ims_brick_code"::NUMBER(18,0)  AS IMS_BRICK_CODE,
    "ims_brick_name"  AS IMS_BRICK_NAME,
    "azk_brand"  AS AZK_BRAND,
    "competitors"  AS COMPETITORS,
    "channel"  AS CHANNEL,
    "priority"  AS PRIORITY,
    "team_name"  AS TEAM_NAME,
    "dso_name"  AS DSO_NAME,
    "dsm_name"  AS DSM_NAME,
    "dsm_prid"  AS DSM_PRID,
    "user_prid"  AS USER_PRID,
    "user_name"  AS USER_NAME,
    "value"::FLOAT  AS DDD_VALUE,
    "value_"::FLOAT  AS VALUE_MS,
    "volume"::FLOAT  AS DDD_VOLUME,
    "volume_"::FLOAT  AS VOLUME_MS,
    "_line"                                AS SRC_LINE,
    "_modified"::TIMESTAMP_NTZ(6)          AS SRC_MODIFIED
  FROM COM_AZK_AIBI_RAW_MDLS."aibi_bronze"."enpire_kr_ddd_crestor";
 
  -- (2) HISTORY MERGE : 같은 키가 여러 행이면 _modified 가 최신인 행 채택
  MERGE INTO COM_AZK_AIBI_WRK.AIBI_SILVER.EMD_ENPIRE_DDD_DATA AS t
  USING (
    SELECT *
    FROM COM_AZK_AIBI_WRK.AIBI_SILVER.EMD_ENPIRE_DDD_DATA_STAGING
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY YEAR_MM, BRICK_CODE, AZK_BRAND, COMPETITORS, CHANNEL
      ORDER BY SRC_MODIFIED DESC NULLS LAST, SRC_LINE DESC
    ) = 1
  ) AS s
  ON  t.YEAR_MM      IS NOT DISTINCT FROM s.YEAR_MM
  AND t.BRICK_CODE   IS NOT DISTINCT FROM s.BRICK_CODE
  AND t.AZK_BRAND    IS NOT DISTINCT FROM s.AZK_BRAND
  AND t.COMPETITORS  IS NOT DISTINCT FROM s.COMPETITORS
  AND t.CHANNEL      IS NOT DISTINCT FROM s.CHANNEL
  WHEN MATCHED THEN UPDATE SET
    t.BRICK_NAME = s.BRICK_NAME,
    t.IMS_BRICK_CODE = s.IMS_BRICK_CODE,
    t.IMS_BRICK_NAME = s.IMS_BRICK_NAME,
    t.PRIORITY = s.PRIORITY,
    t.TEAM_NAME = s.TEAM_NAME,
    t.DSO_NAME = s.DSO_NAME,
    t.DSM_NAME = s.DSM_NAME,
    t.DSM_PRID = s.DSM_PRID,
    t.USER_PRID = s.USER_PRID,
    t.USER_NAME = s.USER_NAME,
    t.DDD_VALUE = s.DDD_VALUE,
    t.VALUE_MS = s.VALUE_MS,
    t.DDD_VOLUME = s.DDD_VOLUME,
    t.VOLUME_MS = s.VOLUME_MS
  WHEN NOT MATCHED THEN INSERT
    (YEAR_MM, BRICK_CODE, BRICK_NAME, IMS_BRICK_CODE, IMS_BRICK_NAME, AZK_BRAND, COMPETITORS, CHANNEL, PRIORITY, TEAM_NAME, DSO_NAME, DSM_NAME, DSM_PRID, USER_PRID, USER_NAME, DDD_VALUE, VALUE_MS, DDD_VOLUME, VOLUME_MS)
  VALUES
    (s.YEAR_MM, s.BRICK_CODE, s.BRICK_NAME, s.IMS_BRICK_CODE, s.IMS_BRICK_NAME, s.AZK_BRAND, s.COMPETITORS, s.CHANNEL, s.PRIORITY, s.TEAM_NAME, s.DSO_NAME, s.DSM_NAME, s.DSM_PRID, s.USER_PRID, s.USER_NAME, s.DDD_VALUE, s.VALUE_MS, s.DDD_VOLUME, s.VOLUME_MS);
 
  -- (3) GOLD : history 전체를 그대로 복사
  INSERT OVERWRITE INTO COM_AZK_AIBI_PUB.AIBI_GOLD.EMD_ENPIRE_DDD_DATA
  SELECT YEAR_MM, BRICK_CODE, BRICK_NAME, IMS_BRICK_CODE, IMS_BRICK_NAME, AZK_BRAND, COMPETITORS, CHANNEL, PRIORITY, TEAM_NAME, DSO_NAME, DSM_NAME, DSM_PRID, USER_PRID, USER_NAME, DDD_VALUE, VALUE_MS, DDD_VOLUME, VOLUME_MS
  FROM COM_AZK_AIBI_WRK.AIBI_SILVER.EMD_ENPIRE_DDD_DATA;
 
  RETURN ''OK'';
END;
';


-- [28] SP_LOAD_IQVIA_MIDAS_MASTER
-- ----------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE "SP_LOAD_IQVIA_MIDAS_MASTER"()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
BEGIN
  -- (1) STAGING : 분기 컬럼(col_YYYY_q_N)을 YEAR / QQ / VALUE_AMT 로 변환
  INSERT OVERWRITE INTO COM_AZK_AIBI_WRK.AIBI_SILVER.EMD_IQVIA_MIDAS_MASTER_STAGING
  SELECT
    x.THERAPY_AREA,
    x.GMD,
    x.GMD_SUB_MKT,
    x.ATC1,
    x.ATC2,
    x.ATC3,
    x.ATC4,
    x.MNC,
    x.HOSPITAL_RETAIL_SPLIT,
    x.MANUFACTURER,
    x.INTERNATIONAL_PRODUCT_LAUNCH_DATE,
    x.CORPORATION,
    x.INTERNATIONAL_PRODUCT,
    x.MOLECULE_COMBINATION,
    x.PRODUCT_FAMILY,
    x.INTERNATIONAL_PRESCRIPTION_STATUS,
    x.INTERNATIONAL_CORPORATION_ANALYSIS_ATC,
    x.INNOVATION_INSIGHTS,
    x.YEAR,
    x.QQ,
    x.VALUE_AMT,
    x.SRC_LINE,
    x.SRC_MODIFIED
  FROM (
    SELECT
      t."therapy_area" AS THERAPY_AREA,
      CAST(NULL AS STRING) AS GMD,
      CAST(NULL AS STRING) AS GMD_SUB_MKT,
      t."atc_1" AS ATC1,
      t."atc_2" AS ATC2,
      t."atc_3" AS ATC3,
      t."atc_4" AS ATC4,
      t."mnc" AS MNC,
      t."hospital_retail_split" AS HOSPITAL_RETAIL_SPLIT,
      t."manufacturer" AS MANUFACTURER,
      t."international_product_launch_date"::STRING AS INTERNATIONAL_PRODUCT_LAUNCH_DATE,
      t."corporation" AS CORPORATION,
      t."international_product" AS INTERNATIONAL_PRODUCT,
      t."molecule_combination" AS MOLECULE_COMBINATION,
      CAST(NULL AS STRING) AS PRODUCT_FAMILY,
      t."international_prescription_status" AS INTERNATIONAL_PRESCRIPTION_STATUS,
      t."international_corporation_analysis_atc" AS INTERNATIONAL_CORPORATION_ANALYSIS_ATC,
      t."innovation_insights" AS INNOVATION_INSIGHTS,
      SUBSTR(f.key, 5, 4)                     AS YEAR,
      ''Q'' || SUBSTR(f.key, 12, 1)             AS QQ,
      f.value::FLOAT                          AS VALUE_AMT,
      t."_line"                               AS SRC_LINE,
      t."_modified"::TIMESTAMP_NTZ(6)         AS SRC_MODIFIED
    FROM (
      SELECT "therapy_area", "atc_1", "atc_2", "atc_3", "atc_4", "mnc", "hospital_retail_split", "manufacturer", "international_product_launch_date", "corporation", "international_product", "molecule_combination", "international_prescription_status", "international_corporation_analysis_atc", "innovation_insights", "_line", "_modified",
             OBJECT_CONSTRUCT_KEEP_NULL(*) AS ROW_OBJ
      FROM COM_AZK_AIBI_RAW_MDLS."aibi_bronze"."iqvia_midas_raw_master"
      WHERE "_sheet_name" = ''TOTAL''                       -- 시트 선택은 여기 한 곳
        AND COALESCE("_fivetran_deleted", FALSE) = FALSE
    ) t,
    LATERAL FLATTEN(input => t.ROW_OBJ) f
    WHERE REGEXP_LIKE(f.key, ''^col_[0-9]{4}_q_[1-4]$'', ''i'')
  ) x
  WHERE x.VALUE_AMT IS NOT NULL;
  -- (2) HISTORY MERGE : 같은 키가 여러 행이면 _modified 가 최신인 행 채택
  MERGE INTO COM_AZK_AIBI_WRK.AIBI_SILVER.EMD_IQVIA_MIDAS_MASTER AS t
  USING (
    SELECT *
    FROM COM_AZK_AIBI_WRK.AIBI_SILVER.EMD_IQVIA_MIDAS_MASTER_STAGING
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY THERAPY_AREA, GMD, GMD_SUB_MKT, ATC1, ATC2, ATC3, ATC4, MNC, HOSPITAL_RETAIL_SPLIT, MANUFACTURER, INTERNATIONAL_PRODUCT_LAUNCH_DATE, CORPORATION, INTERNATIONAL_PRODUCT, MOLECULE_COMBINATION, PRODUCT_FAMILY, INTERNATIONAL_PRESCRIPTION_STATUS, INTERNATIONAL_CORPORATION_ANALYSIS_ATC, INNOVATION_INSIGHTS, YEAR, QQ
      ORDER BY SRC_MODIFIED DESC NULLS LAST, SRC_LINE DESC
    ) = 1
  ) AS s
  ON  t.THERAPY_AREA                           IS NOT DISTINCT FROM s.THERAPY_AREA
  AND t.GMD                                    IS NOT DISTINCT FROM s.GMD
  AND t.GMD_SUB_MKT                            IS NOT DISTINCT FROM s.GMD_SUB_MKT
  AND t.ATC1                                   IS NOT DISTINCT FROM s.ATC1
  AND t.ATC2                                   IS NOT DISTINCT FROM s.ATC2
  AND t.ATC3                                   IS NOT DISTINCT FROM s.ATC3
  AND t.ATC4                                   IS NOT DISTINCT FROM s.ATC4
  AND t.MNC                                    IS NOT DISTINCT FROM s.MNC
  AND t.HOSPITAL_RETAIL_SPLIT                  IS NOT DISTINCT FROM s.HOSPITAL_RETAIL_SPLIT
  AND t.MANUFACTURER                           IS NOT DISTINCT FROM s.MANUFACTURER
  AND t.INTERNATIONAL_PRODUCT_LAUNCH_DATE      IS NOT DISTINCT FROM s.INTERNATIONAL_PRODUCT_LAUNCH_DATE
  AND t.CORPORATION                            IS NOT DISTINCT FROM s.CORPORATION
  AND t.INTERNATIONAL_PRODUCT                  IS NOT DISTINCT FROM s.INTERNATIONAL_PRODUCT
  AND t.MOLECULE_COMBINATION                   IS NOT DISTINCT FROM s.MOLECULE_COMBINATION
  AND t.PRODUCT_FAMILY                         IS NOT DISTINCT FROM s.PRODUCT_FAMILY
  AND t.INTERNATIONAL_PRESCRIPTION_STATUS      IS NOT DISTINCT FROM s.INTERNATIONAL_PRESCRIPTION_STATUS
  AND t.INTERNATIONAL_CORPORATION_ANALYSIS_ATC IS NOT DISTINCT FROM s.INTERNATIONAL_CORPORATION_ANALYSIS_ATC
  AND t.INNOVATION_INSIGHTS                    IS NOT DISTINCT FROM s.INNOVATION_INSIGHTS
  AND t.YEAR                                   IS NOT DISTINCT FROM s.YEAR
  AND t.QQ                                     IS NOT DISTINCT FROM s.QQ
  WHEN MATCHED THEN UPDATE SET
    t.VALUE_AMT = s.VALUE_AMT
  WHEN NOT MATCHED THEN INSERT
    (THERAPY_AREA, GMD, GMD_SUB_MKT, ATC1, ATC2, ATC3, ATC4, MNC, HOSPITAL_RETAIL_SPLIT, MANUFACTURER, INTERNATIONAL_PRODUCT_LAUNCH_DATE, CORPORATION, INTERNATIONAL_PRODUCT, MOLECULE_COMBINATION, PRODUCT_FAMILY, INTERNATIONAL_PRESCRIPTION_STATUS, INTERNATIONAL_CORPORATION_ANALYSIS_ATC, INNOVATION_INSIGHTS, YEAR, QQ, VALUE_AMT)
  VALUES
    (s.THERAPY_AREA, s.GMD, s.GMD_SUB_MKT, s.ATC1, s.ATC2, s.ATC3, s.ATC4, s.MNC, s.HOSPITAL_RETAIL_SPLIT, s.MANUFACTURER, s.INTERNATIONAL_PRODUCT_LAUNCH_DATE, s.CORPORATION, s.INTERNATIONAL_PRODUCT, s.MOLECULE_COMBINATION, s.PRODUCT_FAMILY, s.INTERNATIONAL_PRESCRIPTION_STATUS, s.INTERNATIONAL_CORPORATION_ANALYSIS_ATC, s.INNOVATION_INSIGHTS, s.YEAR, s.QQ, s.VALUE_AMT);
  -- (3) GOLD : history 전체를 그대로 복사
  INSERT OVERWRITE INTO COM_AZK_AIBI_PUB.AIBI_GOLD.EMD_IQVIA_MIDAS_MASTER
  SELECT THERAPY_AREA, GMD, GMD_SUB_MKT, ATC1, ATC2, ATC3, ATC4, MNC, HOSPITAL_RETAIL_SPLIT, MANUFACTURER, INTERNATIONAL_PRODUCT_LAUNCH_DATE, CORPORATION, INTERNATIONAL_PRODUCT, MOLECULE_COMBINATION, PRODUCT_FAMILY, INTERNATIONAL_PRESCRIPTION_STATUS, INTERNATIONAL_CORPORATION_ANALYSIS_ATC, INNOVATION_INSIGHTS, YEAR, QQ, VALUE_AMT
  FROM COM_AZK_AIBI_WRK.AIBI_SILVER.EMD_IQVIA_MIDAS_MASTER;
  RETURN ''OK'';
END;
';


-- [29] SP_LOAD_IQVIA_MIDAS_MASTER_MARKET_ENRICHED
-- ----------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE "SP_LOAD_IQVIA_MIDAS_MASTER_MARKET_ENRICHED"()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
BEGIN
  -- (1) STAGING : master + 코드표 2개 조인 (전체 재계산)
  INSERT OVERWRITE INTO COM_AZK_AIBI_WRK.AIBI_SILVER.EMD_IQVIA_MIDAS_MASTER_MARKET_ENRICHED_STAGING
  SELECT
    m.THERAPY_AREA                             AS THERAPY_AREA,
    m.ATC1                                     AS ATC1,
    m.ATC2                                     AS ATC2,
    m.ATC3                                     AS ATC3,
    m.ATC4                                     AS ATC4,
    m.MNC                                      AS MNC,
    m.HOSPITAL_RETAIL_SPLIT                    AS HOSPITAL_RETAIL_SPLIT,
    m.MANUFACTURER                             AS MANUFACTURER,
    m.INTERNATIONAL_PRODUCT_LAUNCH_DATE        AS INTERNATIONAL_PRODUCT_LAUNCH_DATE,
    m.CORPORATION                              AS CORPORATION,
    m.INTERNATIONAL_PRODUCT                    AS INTERNATIONAL_PRODUCT,
    m.MOLECULE_COMBINATION                     AS MOLECULE_COMBINATION,
    m.PRODUCT_FAMILY                           AS PRODUCT_FAMILY,
    m.INTERNATIONAL_PRESCRIPTION_STATUS        AS INTERNATIONAL_PRESCRIPTION_STATUS,
    m.INTERNATIONAL_CORPORATION_ANALYSIS_ATC   AS INTERNATIONAL_CORPORATION_ANALYSIS_ATC,
    m.INNOVATION_INSIGHTS                      AS INNOVATION_INSIGHTS,
    m.YEAR                                     AS YEAR,
    m.QQ                                       AS QQ,
    m.VALUE_AMT                                AS VALUE_AMT,
    e.MARKET_DETAIL                            AS ETC_MARKET_DETAIL,
    e.DEFINITION                               AS ETC_DEFINITION,
    e.DEFINITION2                              AS ETC_DEFINITION2,
    o.MARKET_DETAIL                            AS ETC_OTC_MARKET_DETAIL,
    o.GMD                                      AS ETC_OTC_GMD,
    o.CHART_NAME                               AS ETC_OTC_CHART_NAME
  FROM COM_AZK_AIBI_WRK.AIBI_SILVER.EMD_IQVIA_MIDAS_MASTER m
  LEFT JOIN COM_AZK_AIBI_WRK.AIBI_SILVER.IQVIA_MARKET_CODE_ETC e
         ON m.ATC4 = e.ATC4
        AND (e.INTERNATIONAL_PRODUCT = ''전체'' OR e.INTERNATIONAL_PRODUCT = m.INTERNATIONAL_PRODUCT)
  LEFT JOIN COM_AZK_AIBI_WRK.AIBI_SILVER.IQVIA_MARKET_CODE_ETC_OTC o
         ON m.ATC4 = o.ATC4
  WHERE e.MARKET_DETAIL IS NOT NULL OR o.MARKET_DETAIL IS NOT NULL;
  -- (2) SILVER : staging 의 현재 스냅샷으로 덮어쓰기
  INSERT OVERWRITE INTO COM_AZK_AIBI_WRK.AIBI_SILVER.EMD_IQVIA_MIDAS_MASTER_MARKET_ENRICHED
  SELECT THERAPY_AREA, ATC1, ATC2, ATC3, ATC4, MNC, HOSPITAL_RETAIL_SPLIT, MANUFACTURER, INTERNATIONAL_PRODUCT_LAUNCH_DATE, CORPORATION, INTERNATIONAL_PRODUCT, MOLECULE_COMBINATION, PRODUCT_FAMILY, INTERNATIONAL_PRESCRIPTION_STATUS, INTERNATIONAL_CORPORATION_ANALYSIS_ATC, INNOVATION_INSIGHTS, YEAR, QQ, VALUE_AMT, ETC_MARKET_DETAIL, ETC_DEFINITION, ETC_DEFINITION2, ETC_OTC_MARKET_DETAIL, ETC_OTC_GMD, ETC_OTC_CHART_NAME
  FROM COM_AZK_AIBI_WRK.AIBI_SILVER.EMD_IQVIA_MIDAS_MASTER_MARKET_ENRICHED_STAGING;
  -- (3) GOLD : silver 를 그대로 복사
  INSERT OVERWRITE INTO COM_AZK_AIBI_PUB.AIBI_GOLD.EMD_IQVIA_MIDAS_MASTER_MARKET_ENRICHED
  SELECT THERAPY_AREA, ATC1, ATC2, ATC3, ATC4, MNC, HOSPITAL_RETAIL_SPLIT, MANUFACTURER, INTERNATIONAL_PRODUCT_LAUNCH_DATE, CORPORATION, INTERNATIONAL_PRODUCT, MOLECULE_COMBINATION, PRODUCT_FAMILY, INTERNATIONAL_PRESCRIPTION_STATUS, INTERNATIONAL_CORPORATION_ANALYSIS_ATC, INNOVATION_INSIGHTS, YEAR, QQ, VALUE_AMT, ETC_MARKET_DETAIL, ETC_DEFINITION, ETC_DEFINITION2, ETC_OTC_MARKET_DETAIL, ETC_OTC_GMD, ETC_OTC_CHART_NAME
  FROM COM_AZK_AIBI_WRK.AIBI_SILVER.EMD_IQVIA_MIDAS_MASTER_MARKET_ENRICHED;
  RETURN ''OK'';
END;
';


-- [30] SP_LOAD_ISL_SALES_ENH_OP_D_PREP
-- ----------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE "SP_LOAD_ISL_SALES_ENH_OP_D_PREP"()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
  rows_inserted INTEGER DEFAULT 0;
BEGIN
    USE SCHEMA COM_AZK_AIBI_WRK.AIBI_SILVER ;
  -- 1. Bronze → 정제
  CREATE OR REPLACE TEMP TABLE batch_isl_sales_enh_op_d AS
  WITH bronze AS (
    SELECT OBJECT_CONSTRUCT_KEEP_NULL(b.*) AS r
    FROM COM_AZK_AIBI_RAW_MDLS.aibi_bronze."enhertu_daily_sales_order_list" b
  )
  SELECT
    GET_IGNORE_CASE(r,''billing_date'')::VARCHAR                 AS base_date,
    GET_IGNORE_CASE(r,''billing_document_number'')::VARCHAR      AS doc_no,
    GET_IGNORE_CASE(r,''billing_document_item_number'')::VARCHAR AS item_no,
    GET_IGNORE_CASE(r,''customer_code'')::VARCHAR                AS customer_code,
    GET_IGNORE_CASE(r,''customer_name'')::VARCHAR                AS customer_name,
    GET_IGNORE_CASE(r,''secondary_customer_code'')::VARCHAR      AS indirect_cust_code,
    GET_IGNORE_CASE(r,''secondary_customer_name'')::VARCHAR      AS indirect_cust_name,
    GET_IGNORE_CASE(r,''ship_to_code'')::VARCHAR                 AS ship_to_code,
    GET_IGNORE_CASE(r,''ship_to_name'')::VARCHAR                 AS ship_to_name,
    GET_IGNORE_CASE(r,''product_code'')::VARCHAR                 AS product_code,
    GET_IGNORE_CASE(r,''product_name'')::VARCHAR                 AS product_name,
    TRUNC(TRY_TO_DOUBLE(GET_IGNORE_CASE(r,''sales_quantity'')::VARCHAR))::INTEGER AS sales_qty,
    GET_IGNORE_CASE(r,''sales_unit'')::VARCHAR                   AS sales_uom,
    TRY_TO_DECIMAL(GET_IGNORE_CASE(r,''unit_price_net'')::VARCHAR,18,2)    AS unit_price_exvat,
    TRY_TO_DECIMAL(GET_IGNORE_CASE(r,''supply_amount_net'')::VARCHAR,18,2) AS supply_amount_exvat,
    TRY_TO_DECIMAL(GET_IGNORE_CASE(r,''vat_amount'')::VARCHAR,18,2)        AS vat_amount,
    TRY_TO_DECIMAL(GET_IGNORE_CASE(r,''sales_amount'')::VARCHAR,18,2)      AS sales_amount,
    GET_IGNORE_CASE(r,''sales_doc_type_code'')::VARCHAR          AS sales_doc_type_code,
    GET_IGNORE_CASE(r,''sales_doc_type_name'')::VARCHAR          AS sales_doc_type_name,
    CONVERT_TIMEZONE(''UTC'',''Asia/Seoul'',SYSDATE())::TIMESTAMP_NTZ(6) AS _load_dtm
  FROM bronze
  WHERE GET_IGNORE_CASE(r,''billing_document_number'')::VARCHAR IS NOT NULL;

  -- 2. 대상 일자 교체 (트랜잭션)
  BEGIN
    BEGIN TRANSACTION;
    DELETE FROM COM_AZK_AIBI_WRK.AIBI_SILVER.isl_sales_enh_op_d_prep t
      USING (SELECT DISTINCT base_date FROM batch_isl_sales_enh_op_d) s
      WHERE t.base_date = s.base_date;
    INSERT INTO COM_AZK_AIBI_WRK.AIBI_SILVER.isl_sales_enh_op_d_prep
      (base_date, doc_no, item_no, customer_code, customer_name, indirect_cust_code, indirect_cust_name,
       ship_to_code, ship_to_name, product_code, product_name, sales_qty, sales_uom,
       unit_price_exvat, supply_amount_exvat, vat_amount, sales_amount,
       sales_doc_type_code, sales_doc_type_name, _load_dtm)
      SELECT * FROM batch_isl_sales_enh_op_d;
    rows_inserted := SQLROWCOUNT;
    COMMIT;
  EXCEPTION
    WHEN OTHER THEN
      ROLLBACK;
      RAISE;
  END;

  RETURN ''isl_sales_enh_op_d_prep loaded: '' || rows_inserted || '' rows'';
END;
';


-- [31] SP_LOAD_MARKET_SPENDING
-- ----------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE "SP_LOAD_MARKET_SPENDING"()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
BEGIN
  -- (1) STAGING : 4개 소스 UNION ALL -> 월 컬럼 FLATTEN
  --     월 컬럼은 FLOAT 이므로 소수 2자리로 반올림해 NUMBER(15,2) 로 저장
  INSERT OVERWRITE INTO COM_AZK_AIBI_WRK.AIBI_SILVER.EMD_MARKET_SPENDING_STAGING
  SELECT
    s.BRAND_NAME,
    s.CRITERIA,
    s.MAIN_PLAYERS,
    SUBSTR(f.key, 2, 4),
    SUBSTR(f.key, 7, 2),
    TO_DATE(SUBSTR(f.key, 2, 4) || ''-'' || SUBSTR(f.key, 7, 2) || ''-01''),
    ROUND(f.value::FLOAT, 2)::NUMBER(15,2),
    s.SRC_MODIFIED
  FROM (
    SELECT ''iOAD'' AS BRAND_NAME,                          -- TODO: Databricks brand_name 값과 동일하게
           "criteria_m_krw_" AS CRITERIA,
           "main_players"    AS MAIN_PLAYERS,
           "_modified"::TIMESTAMP_NTZ(6) AS SRC_MODIFIED,
           OBJECT_CONSTRUCT_KEEP_NULL(*) AS ROW_OBJ
    FROM COM_AZK_AIBI_RAW_MDLS."aibi_bronze"."kr_dashboard_call_spending"
    UNION ALL
    SELECT ''PPI'',                                         -- TODO: Databricks brand_name 값과 동일하게
           "criteria_m_krw_", NULL, "_modified"::TIMESTAMP_NTZ(6),
           OBJECT_CONSTRUCT_KEEP_NULL(*)
    FROM COM_AZK_AIBI_RAW_MDLS."aibi_bronze"."kr_dashboard_call_spending_ppi_market_spending"
    UNION ALL
    SELECT ''Respiratory_Spending'',                        -- TODO: Databricks brand_name 값과 동일하게
           "criteria_m_krw_", NULL, "_modified"::TIMESTAMP_NTZ(6),
           OBJECT_CONSTRUCT_KEEP_NULL(*)
    FROM COM_AZK_AIBI_RAW_MDLS."aibi_bronze"."kr_dashboard_call_spending_respiratory_spending"
    UNION ALL
    SELECT ''Statin'',
           "criteria_m_krw_", NULL, "_modified"::TIMESTAMP_NTZ(6),
           OBJECT_CONSTRUCT_KEEP_NULL(*)
    FROM COM_AZK_AIBI_RAW_MDLS."aibi_bronze"."kr_dashboard_call_spending_stain_market_spending"
  ) s,
  LATERAL FLATTEN(input => s.ROW_OBJ) f
  WHERE REGEXP_LIKE(f.key, ''^_[0-9]{4}_[0-9]{2}_[0-9]{2}_t_00_00_00_z$'', ''i'');
 
  -- (2) HISTORY MERGE : 같은 키가 여러 행이면 _modified 가 최신인 행 채택
  MERGE INTO COM_AZK_AIBI_WRK.AIBI_SILVER.EMD_MARKET_SPENDING AS t
  USING (
    SELECT *
    FROM COM_AZK_AIBI_WRK.AIBI_SILVER.EMD_MARKET_SPENDING_STAGING
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY BRAND_NAME, CRITERIA, MAIN_PLAYERS, YEAR, MM
      ORDER BY SRC_MODIFIED DESC NULLS LAST, SPENDING_AMT DESC NULLS LAST
    ) = 1
  ) AS s
  ON  t.BRAND_NAME   IS NOT DISTINCT FROM s.BRAND_NAME
  AND t.CRITERIA     IS NOT DISTINCT FROM s.CRITERIA
  AND t.MAIN_PLAYERS IS NOT DISTINCT FROM s.MAIN_PLAYERS
  AND t.YEAR         IS NOT DISTINCT FROM s.YEAR
  AND t.MM           IS NOT DISTINCT FROM s.MM
  WHEN MATCHED THEN UPDATE SET
    t.YYYYMMDD     = s.YYYYMMDD,
    t.SPENDING_AMT = s.SPENDING_AMT
  WHEN NOT MATCHED THEN INSERT
    (BRAND_NAME, CRITERIA, MAIN_PLAYERS, YEAR, MM, YYYYMMDD, SPENDING_AMT)
  VALUES
    (s.BRAND_NAME, s.CRITERIA, s.MAIN_PLAYERS, s.YEAR, s.MM, s.YYYYMMDD, s.SPENDING_AMT);
 
  -- (3) GOLD : history 전체를 그대로 복사
  INSERT OVERWRITE INTO COM_AZK_AIBI_PUB.AIBI_GOLD.EMD_MARKET_SPENDING
  SELECT BRAND_NAME, CRITERIA, MAIN_PLAYERS, YEAR, MM, YYYYMMDD, SPENDING_AMT
  FROM COM_AZK_AIBI_WRK.AIBI_SILVER.EMD_MARKET_SPENDING;
 
  RETURN ''OK'';
END;
';


-- [32] SP_LOAD_NPP_RAMA_TRACKER
-- ----------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE "SP_LOAD_NPP_RAMA_TRACKER"()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
BEGIN
 
  -- ================= OBU  (npp_obu_rama_tracker) =================
  -- (1) STAGING : 삭제된 행(_fivetran_deleted)과 구분 컬럼이 모두 빈 행은 제외, 같은 (TA, BRAND, STUDY_NAME, INDICATION) 안에서 원본 순서(_line)대로 DEDUP_SEQ 부여
  INSERT OVERWRITE INTO COM_AZK_AIBI_WRK.AIBI_SILVER.NPP_RAMA_TRACKER_OBU_STAGING
  SELECT
    x.TA,
    x.BRAND,
    x.STUDY_NAME,
    x.INDICATION,
    x.CURRENT_STATUS,
    x.RA_CATEGORY,
    x.GIFT_ODD,
    x.PNR_TRACK,
    x.RA_SUBMISSION_BASE,
    x.RA_APPROVAL_BASE,
    x.PNR_SUBMISSION_BASE,
    x.REIM_LAUNCH_BEST,
    x.REIM_LAUNCH_BASE,
    x.RA_REMARKS,
    x.PNR_REMARKS,
    x.RA_LEAD,
    x.PNR_LEAD,
    x.COUNTRY_SUBMISSION_1ST,
    x.COUNTRY_APPROVAL_1ST,
    x.PIVOTAL_STUDY,
    x.NEED_FOR_KOREAN_BRIDGING_PURPOSE,
    x.REQUIRED_FOR_REG_SUBMISSION,
    ROW_NUMBER() OVER (PARTITION BY x.TA, x.BRAND, x.STUDY_NAME, x.INDICATION ORDER BY x.SRC_LINE) AS DEDUP_SEQ,
    x.SRC_LINE,
    x.SRC_MODIFIED
  FROM (
    SELECT
        NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("_sheet_name"::STRING, CHR(12288), '' ''), CHR(160), '' ''), '' +'', '' '')), '''') AS TA,
        NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("brand"::STRING, CHR(12288), '' ''), CHR(160), '' ''), '' +'', '' '')), '''') AS BRAND,
        NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("study_name"::STRING, CHR(12288), '' ''), CHR(160), '' ''), '' +'', '' '')), '''') AS STUDY_NAME,
        NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("indication"::STRING, CHR(12288), '' ''), CHR(160), '' ''), '' +'', '' '')), '''') AS INDICATION,
        NULLIF(TRIM(REPLACE(REPLACE("current_status"::STRING, CHR(12288), '' ''), CHR(160), '' '')), '''') AS CURRENT_STATUS,
        NULLIF(TRIM(REPLACE(REPLACE("ra_category"::STRING, CHR(12288), '' ''), CHR(160), '' '')), '''') AS RA_CATEGORY,
        NULLIF(TRIM(REPLACE(REPLACE("gift_odd"::STRING, CHR(12288), '' ''), CHR(160), '' '')), '''') AS GIFT_ODD,
        NULLIF(TRIM(REPLACE(REPLACE("p_r_track"::STRING, CHR(12288), '' ''), CHR(160), '' '')), '''') AS PNR_TRACK,
        IFF(REGEXP_LIKE(NULLIF(TRIM(REPLACE(REPLACE("ra_submission_base"::STRING, CHR(12288), '' ''), CHR(160), '' '')), ''''), ''^[0-9]{4}-[0-9]{2}-[0-9]{2}.*''), LEFT(NULLIF(TRIM(REPLACE(REPLACE("ra_submission_base"::STRING, CHR(12288), '' ''), CHR(160), '' '')), ''''), 10), NULLIF(TRIM(REPLACE(REPLACE("ra_submission_base"::STRING, CHR(12288), '' ''), CHR(160), '' '')), '''')) AS RA_SUBMISSION_BASE,
        IFF(REGEXP_LIKE(NULLIF(TRIM(REPLACE(REPLACE("ra_approval_base"::STRING, CHR(12288), '' ''), CHR(160), '' '')), ''''), ''^[0-9]{4}-[0-9]{2}-[0-9]{2}.*''), LEFT(NULLIF(TRIM(REPLACE(REPLACE("ra_approval_base"::STRING, CHR(12288), '' ''), CHR(160), '' '')), ''''), 10), NULLIF(TRIM(REPLACE(REPLACE("ra_approval_base"::STRING, CHR(12288), '' ''), CHR(160), '' '')), '''')) AS RA_APPROVAL_BASE,
        IFF(REGEXP_LIKE(NULLIF(TRIM(REPLACE(REPLACE("p_r_submission_base"::STRING, CHR(12288), '' ''), CHR(160), '' '')), ''''), ''^[0-9]{4}-[0-9]{2}-[0-9]{2}.*''), LEFT(NULLIF(TRIM(REPLACE(REPLACE("p_r_submission_base"::STRING, CHR(12288), '' ''), CHR(160), '' '')), ''''), 10), NULLIF(TRIM(REPLACE(REPLACE("p_r_submission_base"::STRING, CHR(12288), '' ''), CHR(160), '' '')), '''')) AS PNR_SUBMISSION_BASE,
        IFF(REGEXP_LIKE(NULLIF(TRIM(REPLACE(REPLACE("reim_launch_best"::STRING, CHR(12288), '' ''), CHR(160), '' '')), ''''), ''^[0-9]{4}-[0-9]{2}-[0-9]{2}.*''), LEFT(NULLIF(TRIM(REPLACE(REPLACE("reim_launch_best"::STRING, CHR(12288), '' ''), CHR(160), '' '')), ''''), 10), NULLIF(TRIM(REPLACE(REPLACE("reim_launch_best"::STRING, CHR(12288), '' ''), CHR(160), '' '')), '''')) AS REIM_LAUNCH_BEST,
        IFF(REGEXP_LIKE(NULLIF(TRIM(REPLACE(REPLACE("reim_launch_base"::STRING, CHR(12288), '' ''), CHR(160), '' '')), ''''), ''^[0-9]{4}-[0-9]{2}-[0-9]{2}.*''), LEFT(NULLIF(TRIM(REPLACE(REPLACE("reim_launch_base"::STRING, CHR(12288), '' ''), CHR(160), '' '')), ''''), 10), NULLIF(TRIM(REPLACE(REPLACE("reim_launch_base"::STRING, CHR(12288), '' ''), CHR(160), '' '')), '''')) AS REIM_LAUNCH_BASE,
        NULLIF(TRIM(REPLACE(REPLACE("ra_remarks_best_approval_ra_key_milestone_etc"::STRING, CHR(12288), '' ''), CHR(160), '' '')), '''') AS RA_REMARKS,
        NULLIF(TRIM(REPLACE(REPLACE("p_r_remarks_mtp_timelines_p_r_key_milestone_etc"::STRING, CHR(12288), '' ''), CHR(160), '' '')), '''') AS PNR_REMARKS,
        NULLIF(TRIM(REPLACE(REPLACE("ra_lead"::STRING, CHR(12288), '' ''), CHR(160), '' '')), '''') AS RA_LEAD,
        NULLIF(TRIM(REPLACE(REPLACE("p_r_lead"::STRING, CHR(12288), '' ''), CHR(160), '' '')), '''') AS PNR_LEAD,
        NULLIF(TRIM(REPLACE(REPLACE("col_1_st_country_submission"::STRING, CHR(12288), '' ''), CHR(160), '' '')), '''') AS COUNTRY_SUBMISSION_1ST,
        NULLIF(TRIM(REPLACE(REPLACE("col_1_st_country_approval"::STRING, CHR(12288), '' ''), CHR(160), '' '')), '''') AS COUNTRY_APPROVAL_1ST,
        NULLIF(TRIM(REPLACE(REPLACE("pivotal_study_y_n"::STRING, CHR(12288), '' ''), CHR(160), '' '')), '''') AS PIVOTAL_STUDY,
        NULLIF(TRIM(REPLACE(REPLACE("need_for_korean_bridging_purpose_y_n_tbd"::STRING, CHR(12288), '' ''), CHR(160), '' '')), '''') AS NEED_FOR_KOREAN_BRIDGING_PURPOSE,
        NULLIF(TRIM(REPLACE(REPLACE("required_for_reg_submission_full_analysis_set"::STRING, CHR(12288), '' ''), CHR(160), '' '')), '''') AS REQUIRED_FOR_REG_SUBMISSION,
        "_line"                         AS SRC_LINE,
        "_modified"::TIMESTAMP_NTZ(6)   AS SRC_MODIFIED
    FROM COM_AZK_AIBI_RAW_MDLS."aibi_bronze"."npp_obu_rama_tracker"
    WHERE COALESCE("_fivetran_deleted", FALSE) = FALSE
  ) x
  WHERE x.BRAND IS NOT NULL OR x.STUDY_NAME IS NOT NULL OR x.INDICATION IS NOT NULL;
 
  -- (2) HISTORY MERGE : 키 = TA, BRAND, STUDY_NAME, INDICATION, DEDUP_SEQ
  MERGE INTO COM_AZK_AIBI_WRK.AIBI_SILVER.NPP_RAMA_TRACKER_OBU AS t
  USING COM_AZK_AIBI_WRK.AIBI_SILVER.NPP_RAMA_TRACKER_OBU_STAGING AS s
  ON  t.TA            IS NOT DISTINCT FROM s.TA
    AND t.BRAND         IS NOT DISTINCT FROM s.BRAND
    AND t.STUDY_NAME    IS NOT DISTINCT FROM s.STUDY_NAME
    AND t.INDICATION    IS NOT DISTINCT FROM s.INDICATION
    AND t.DEDUP_SEQ     IS NOT DISTINCT FROM s.DEDUP_SEQ
  WHEN MATCHED THEN UPDATE SET
      t.CURRENT_STATUS = s.CURRENT_STATUS,
      t.RA_CATEGORY = s.RA_CATEGORY,
      t.GIFT_ODD = s.GIFT_ODD,
      t.PNR_TRACK = s.PNR_TRACK,
      t.RA_SUBMISSION_BASE = s.RA_SUBMISSION_BASE,
      t.RA_APPROVAL_BASE = s.RA_APPROVAL_BASE,
      t.PNR_SUBMISSION_BASE = s.PNR_SUBMISSION_BASE,
      t.REIM_LAUNCH_BEST = s.REIM_LAUNCH_BEST,
      t.REIM_LAUNCH_BASE = s.REIM_LAUNCH_BASE,
      t.RA_REMARKS = s.RA_REMARKS,
      t.PNR_REMARKS = s.PNR_REMARKS,
      t.RA_LEAD = s.RA_LEAD,
      t.PNR_LEAD = s.PNR_LEAD,
      t.COUNTRY_SUBMISSION_1ST = s.COUNTRY_SUBMISSION_1ST,
      t.COUNTRY_APPROVAL_1ST = s.COUNTRY_APPROVAL_1ST,
      t.PIVOTAL_STUDY = s.PIVOTAL_STUDY,
      t.NEED_FOR_KOREAN_BRIDGING_PURPOSE = s.NEED_FOR_KOREAN_BRIDGING_PURPOSE,
      t.REQUIRED_FOR_REG_SUBMISSION = s.REQUIRED_FOR_REG_SUBMISSION
  WHEN NOT MATCHED THEN INSERT
    (TA, BRAND, STUDY_NAME, INDICATION, CURRENT_STATUS, RA_CATEGORY, GIFT_ODD, PNR_TRACK, RA_SUBMISSION_BASE, RA_APPROVAL_BASE, PNR_SUBMISSION_BASE, REIM_LAUNCH_BEST, REIM_LAUNCH_BASE, RA_REMARKS, PNR_REMARKS, RA_LEAD, PNR_LEAD, COUNTRY_SUBMISSION_1ST, COUNTRY_APPROVAL_1ST, PIVOTAL_STUDY, NEED_FOR_KOREAN_BRIDGING_PURPOSE, REQUIRED_FOR_REG_SUBMISSION, DEDUP_SEQ)
  VALUES
    (s.TA, s.BRAND, s.STUDY_NAME, s.INDICATION, s.CURRENT_STATUS, s.RA_CATEGORY, s.GIFT_ODD, s.PNR_TRACK, s.RA_SUBMISSION_BASE, s.RA_APPROVAL_BASE, s.PNR_SUBMISSION_BASE, s.REIM_LAUNCH_BEST, s.REIM_LAUNCH_BASE, s.RA_REMARKS, s.PNR_REMARKS, s.RA_LEAD, s.PNR_LEAD, s.COUNTRY_SUBMISSION_1ST, s.COUNTRY_APPROVAL_1ST, s.PIVOTAL_STUDY, s.NEED_FOR_KOREAN_BRIDGING_PURPOSE, s.REQUIRED_FOR_REG_SUBMISSION, s.DEDUP_SEQ);
 
  -- (3) GOLD : history 전체를 그대로 복사
  INSERT OVERWRITE INTO COM_AZK_AIBI_PUB.AIBI_GOLD.NPP_RAMA_TRACKER_OBU
  SELECT TA, BRAND, STUDY_NAME, INDICATION, CURRENT_STATUS, RA_CATEGORY, GIFT_ODD, PNR_TRACK, RA_SUBMISSION_BASE, RA_APPROVAL_BASE, PNR_SUBMISSION_BASE, REIM_LAUNCH_BEST, REIM_LAUNCH_BASE, RA_REMARKS, PNR_REMARKS, RA_LEAD, PNR_LEAD, COUNTRY_SUBMISSION_1ST, COUNTRY_APPROVAL_1ST, PIVOTAL_STUDY, NEED_FOR_KOREAN_BRIDGING_PURPOSE, REQUIRED_FOR_REG_SUBMISSION, DEDUP_SEQ
  FROM COM_AZK_AIBI_WRK.AIBI_SILVER.NPP_RAMA_TRACKER_OBU;
 
  -- ================= BBU  (npp_bbu_rama_tracker) =================
  -- (1) STAGING : 삭제된 행(_fivetran_deleted)과 구분 컬럼이 모두 빈 행은 제외, 같은 (TA, BRAND, STUDY_NAME, INDICATION) 안에서 원본 순서(_line)대로 DEDUP_SEQ 부여
  INSERT OVERWRITE INTO COM_AZK_AIBI_WRK.AIBI_SILVER.NPP_RAMA_TRACKER_BBU_STAGING
  SELECT
    x.TA,
    x.BRAND,
    x.STUDY_NAME,
    x.INDICATION,
    x.CURRENT_STATUS,
    x.RA_CATEGORY,
    x.GIFT_ODD,
    x.PNR_TRACK,
    x.RA_SUBMISSION_BASE,
    x.RA_APPROVAL_BASE,
    x.PNR_SUBMISSION_BASE,
    x.REIM_LAUNCH_BEST,
    x.REIM_LAUNCH_BASE,
    x.RA_REMARKS,
    x.PNR_REMARKS,
    x.RA_LEAD,
    x.PNR_LEAD,
    x.COUNTRY_SUBMISSION_1ST,
    x.COUNTRY_APPROVAL_1ST,
    x.PIVOTAL_STUDY,
    x.NEED_FOR_KOREAN_BRIDGING_PURPOSE,
    x.REQUIRED_FOR_REG_SUBMISSION,
    ROW_NUMBER() OVER (PARTITION BY x.TA, x.BRAND, x.STUDY_NAME, x.INDICATION ORDER BY x.SRC_LINE) AS DEDUP_SEQ,
    x.SRC_LINE,
    x.SRC_MODIFIED
  FROM (
    SELECT
        NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("_sheet_name"::STRING, CHR(12288), '' ''), CHR(160), '' ''), '' +'', '' '')), '''') AS TA,
        NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("brand"::STRING, CHR(12288), '' ''), CHR(160), '' ''), '' +'', '' '')), '''') AS BRAND,
        NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("study_name"::STRING, CHR(12288), '' ''), CHR(160), '' ''), '' +'', '' '')), '''') AS STUDY_NAME,
        NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("indication"::STRING, CHR(12288), '' ''), CHR(160), '' ''), '' +'', '' '')), '''') AS INDICATION,
        NULLIF(TRIM(REPLACE(REPLACE("current_status"::STRING, CHR(12288), '' ''), CHR(160), '' '')), '''') AS CURRENT_STATUS,
        NULLIF(TRIM(REPLACE(REPLACE("ra_category"::STRING, CHR(12288), '' ''), CHR(160), '' '')), '''') AS RA_CATEGORY,
        NULLIF(TRIM(REPLACE(REPLACE("gift_odd"::STRING, CHR(12288), '' ''), CHR(160), '' '')), '''') AS GIFT_ODD,
        NULLIF(TRIM(REPLACE(REPLACE("p_r_track"::STRING, CHR(12288), '' ''), CHR(160), '' '')), '''') AS PNR_TRACK,
        IFF(REGEXP_LIKE(NULLIF(TRIM(REPLACE(REPLACE("ra_submission_base"::STRING, CHR(12288), '' ''), CHR(160), '' '')), ''''), ''^[0-9]{4}-[0-9]{2}-[0-9]{2}.*''), LEFT(NULLIF(TRIM(REPLACE(REPLACE("ra_submission_base"::STRING, CHR(12288), '' ''), CHR(160), '' '')), ''''), 10), NULLIF(TRIM(REPLACE(REPLACE("ra_submission_base"::STRING, CHR(12288), '' ''), CHR(160), '' '')), '''')) AS RA_SUBMISSION_BASE,
        IFF(REGEXP_LIKE(NULLIF(TRIM(REPLACE(REPLACE("ra_approval_base"::STRING, CHR(12288), '' ''), CHR(160), '' '')), ''''), ''^[0-9]{4}-[0-9]{2}-[0-9]{2}.*''), LEFT(NULLIF(TRIM(REPLACE(REPLACE("ra_approval_base"::STRING, CHR(12288), '' ''), CHR(160), '' '')), ''''), 10), NULLIF(TRIM(REPLACE(REPLACE("ra_approval_base"::STRING, CHR(12288), '' ''), CHR(160), '' '')), '''')) AS RA_APPROVAL_BASE,
        IFF(REGEXP_LIKE(NULLIF(TRIM(REPLACE(REPLACE("p_r_submission_base"::STRING, CHR(12288), '' ''), CHR(160), '' '')), ''''), ''^[0-9]{4}-[0-9]{2}-[0-9]{2}.*''), LEFT(NULLIF(TRIM(REPLACE(REPLACE("p_r_submission_base"::STRING, CHR(12288), '' ''), CHR(160), '' '')), ''''), 10), NULLIF(TRIM(REPLACE(REPLACE("p_r_submission_base"::STRING, CHR(12288), '' ''), CHR(160), '' '')), '''')) AS PNR_SUBMISSION_BASE,
        IFF(REGEXP_LIKE(NULLIF(TRIM(REPLACE(REPLACE("reim_launch_best"::STRING, CHR(12288), '' ''), CHR(160), '' '')), ''''), ''^[0-9]{4}-[0-9]{2}-[0-9]{2}.*''), LEFT(NULLIF(TRIM(REPLACE(REPLACE("reim_launch_best"::STRING, CHR(12288), '' ''), CHR(160), '' '')), ''''), 10), NULLIF(TRIM(REPLACE(REPLACE("reim_launch_best"::STRING, CHR(12288), '' ''), CHR(160), '' '')), '''')) AS REIM_LAUNCH_BEST,
        IFF(REGEXP_LIKE(NULLIF(TRIM(REPLACE(REPLACE("reim_launch_base"::STRING, CHR(12288), '' ''), CHR(160), '' '')), ''''), ''^[0-9]{4}-[0-9]{2}-[0-9]{2}.*''), LEFT(NULLIF(TRIM(REPLACE(REPLACE("reim_launch_base"::STRING, CHR(12288), '' ''), CHR(160), '' '')), ''''), 10), NULLIF(TRIM(REPLACE(REPLACE("reim_launch_base"::STRING, CHR(12288), '' ''), CHR(160), '' '')), '''')) AS REIM_LAUNCH_BASE,
        NULLIF(TRIM(REPLACE(REPLACE("ra_remarks_best_approval_ra_key_milestone_etc"::STRING, CHR(12288), '' ''), CHR(160), '' '')), '''') AS RA_REMARKS,
        NULLIF(TRIM(REPLACE(REPLACE("p_r_remarks_mtp_timelines_p_r_key_milestone_etc"::STRING, CHR(12288), '' ''), CHR(160), '' '')), '''') AS PNR_REMARKS,
        NULLIF(TRIM(REPLACE(REPLACE("ra_lead"::STRING, CHR(12288), '' ''), CHR(160), '' '')), '''') AS RA_LEAD,
        NULLIF(TRIM(REPLACE(REPLACE("p_r_lead"::STRING, CHR(12288), '' ''), CHR(160), '' '')), '''') AS PNR_LEAD,
        NULLIF(TRIM(REPLACE(REPLACE("col_1_st_country_submission"::STRING, CHR(12288), '' ''), CHR(160), '' '')), '''') AS COUNTRY_SUBMISSION_1ST,
        NULLIF(TRIM(REPLACE(REPLACE("col_1_st_country_approval"::STRING, CHR(12288), '' ''), CHR(160), '' '')), '''') AS COUNTRY_APPROVAL_1ST,
        NULLIF(TRIM(REPLACE(REPLACE("pivotal_study_y_n"::STRING, CHR(12288), '' ''), CHR(160), '' '')), '''') AS PIVOTAL_STUDY,
        NULLIF(TRIM(REPLACE(REPLACE("need_for_korean_bridging_purpose_y_n_unk"::STRING, CHR(12288), '' ''), CHR(160), '' '')), '''') AS NEED_FOR_KOREAN_BRIDGING_PURPOSE,
        NULLIF(TRIM(REPLACE(REPLACE("required_for_reg_submission_full_analysis_set"::STRING, CHR(12288), '' ''), CHR(160), '' '')), '''') AS REQUIRED_FOR_REG_SUBMISSION,
        "_line"                         AS SRC_LINE,
        "_modified"::TIMESTAMP_NTZ(6)   AS SRC_MODIFIED
    FROM COM_AZK_AIBI_RAW_MDLS."aibi_bronze"."npp_bbu_rama_tracker"
    WHERE COALESCE("_fivetran_deleted", FALSE) = FALSE
  ) x
  WHERE x.BRAND IS NOT NULL OR x.STUDY_NAME IS NOT NULL OR x.INDICATION IS NOT NULL;
 
  -- (2) HISTORY MERGE : 키 = TA, BRAND, STUDY_NAME, INDICATION, DEDUP_SEQ
  MERGE INTO COM_AZK_AIBI_WRK.AIBI_SILVER.NPP_RAMA_TRACKER_BBU AS t
  USING COM_AZK_AIBI_WRK.AIBI_SILVER.NPP_RAMA_TRACKER_BBU_STAGING AS s
  ON  t.TA            IS NOT DISTINCT FROM s.TA
    AND t.BRAND         IS NOT DISTINCT FROM s.BRAND
    AND t.STUDY_NAME    IS NOT DISTINCT FROM s.STUDY_NAME
    AND t.INDICATION    IS NOT DISTINCT FROM s.INDICATION
    AND t.DEDUP_SEQ     IS NOT DISTINCT FROM s.DEDUP_SEQ
  WHEN MATCHED THEN UPDATE SET
      t.CURRENT_STATUS = s.CURRENT_STATUS,
      t.RA_CATEGORY = s.RA_CATEGORY,
      t.GIFT_ODD = s.GIFT_ODD,
      t.PNR_TRACK = s.PNR_TRACK,
      t.RA_SUBMISSION_BASE = s.RA_SUBMISSION_BASE,
      t.RA_APPROVAL_BASE = s.RA_APPROVAL_BASE,
      t.PNR_SUBMISSION_BASE = s.PNR_SUBMISSION_BASE,
      t.REIM_LAUNCH_BEST = s.REIM_LAUNCH_BEST,
      t.REIM_LAUNCH_BASE = s.REIM_LAUNCH_BASE,
      t.RA_REMARKS = s.RA_REMARKS,
      t.PNR_REMARKS = s.PNR_REMARKS,
      t.RA_LEAD = s.RA_LEAD,
      t.PNR_LEAD = s.PNR_LEAD,
      t.COUNTRY_SUBMISSION_1ST = s.COUNTRY_SUBMISSION_1ST,
      t.COUNTRY_APPROVAL_1ST = s.COUNTRY_APPROVAL_1ST,
      t.PIVOTAL_STUDY = s.PIVOTAL_STUDY,
      t.NEED_FOR_KOREAN_BRIDGING_PURPOSE = s.NEED_FOR_KOREAN_BRIDGING_PURPOSE,
      t.REQUIRED_FOR_REG_SUBMISSION = s.REQUIRED_FOR_REG_SUBMISSION
  WHEN NOT MATCHED THEN INSERT
    (TA, BRAND, STUDY_NAME, INDICATION, CURRENT_STATUS, RA_CATEGORY, GIFT_ODD, PNR_TRACK, RA_SUBMISSION_BASE, RA_APPROVAL_BASE, PNR_SUBMISSION_BASE, REIM_LAUNCH_BEST, REIM_LAUNCH_BASE, RA_REMARKS, PNR_REMARKS, RA_LEAD, PNR_LEAD, COUNTRY_SUBMISSION_1ST, COUNTRY_APPROVAL_1ST, PIVOTAL_STUDY, NEED_FOR_KOREAN_BRIDGING_PURPOSE, REQUIRED_FOR_REG_SUBMISSION, DEDUP_SEQ)
  VALUES
    (s.TA, s.BRAND, s.STUDY_NAME, s.INDICATION, s.CURRENT_STATUS, s.RA_CATEGORY, s.GIFT_ODD, s.PNR_TRACK, s.RA_SUBMISSION_BASE, s.RA_APPROVAL_BASE, s.PNR_SUBMISSION_BASE, s.REIM_LAUNCH_BEST, s.REIM_LAUNCH_BASE, s.RA_REMARKS, s.PNR_REMARKS, s.RA_LEAD, s.PNR_LEAD, s.COUNTRY_SUBMISSION_1ST, s.COUNTRY_APPROVAL_1ST, s.PIVOTAL_STUDY, s.NEED_FOR_KOREAN_BRIDGING_PURPOSE, s.REQUIRED_FOR_REG_SUBMISSION, s.DEDUP_SEQ);
 
  -- (3) GOLD : history 전체를 그대로 복사
  INSERT OVERWRITE INTO COM_AZK_AIBI_PUB.AIBI_GOLD.NPP_RAMA_TRACKER_BBU
  SELECT TA, BRAND, STUDY_NAME, INDICATION, CURRENT_STATUS, RA_CATEGORY, GIFT_ODD, PNR_TRACK, RA_SUBMISSION_BASE, RA_APPROVAL_BASE, PNR_SUBMISSION_BASE, REIM_LAUNCH_BEST, REIM_LAUNCH_BASE, RA_REMARKS, PNR_REMARKS, RA_LEAD, PNR_LEAD, COUNTRY_SUBMISSION_1ST, COUNTRY_APPROVAL_1ST, PIVOTAL_STUDY, NEED_FOR_KOREAN_BRIDGING_PURPOSE, REQUIRED_FOR_REG_SUBMISSION, DEDUP_SEQ
  FROM COM_AZK_AIBI_WRK.AIBI_SILVER.NPP_RAMA_TRACKER_BBU;
 
  -- ================= RDU  (npp_rd_rama_tracker) =================
  -- (1) STAGING : 삭제된 행(_fivetran_deleted)과 구분 컬럼이 모두 빈 행은 제외, 같은 (TA, BRAND, STUDY_NAME, INDICATION) 안에서 원본 순서(_line)대로 DEDUP_SEQ 부여
  INSERT OVERWRITE INTO COM_AZK_AIBI_WRK.AIBI_SILVER.NPP_RAMA_TRACKER_RDU_STAGING
  SELECT
    x.TA,
    x.BRAND,
    x.STUDY_NAME,
    x.INDICATION,
    x.CURRENT_STATUS,
    x.RA_CATEGORY,
    x.GIFT_ODD,
    x.PNR_TRACK,
    x.RA_SUBMISSION_BASE,
    x.RA_APPROVAL_BASE,
    x.PNR_SUBMISSION_BASE,
    x.REIM_LAUNCH_BEST,
    x.REIM_LAUNCH_BASE,
    x.RA_REMARKS,
    x.PNR_REMARKS,
    x.RA_LEAD,
    x.PNR_LEAD,
    x.COUNTRY_SUBMISSION_1ST,
    x.COUNTRY_APPROVAL_1ST,
    x.PIVOTAL_STUDY,
    x.NEED_FOR_KOREAN_BRIDGING_PURPOSE,
    x.REQUIRED_FOR_REG_SUBMISSION,
    ROW_NUMBER() OVER (PARTITION BY x.TA, x.BRAND, x.STUDY_NAME, x.INDICATION ORDER BY x.SRC_LINE) AS DEDUP_SEQ,
    x.SRC_LINE,
    x.SRC_MODIFIED
  FROM (
    SELECT
        NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("_sheet_name"::STRING, CHR(12288), '' ''), CHR(160), '' ''), '' +'', '' '')), '''') AS TA,
        NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("brand"::STRING, CHR(12288), '' ''), CHR(160), '' ''), '' +'', '' '')), '''') AS BRAND,
        NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("study_name"::STRING, CHR(12288), '' ''), CHR(160), '' ''), '' +'', '' '')), '''') AS STUDY_NAME,
        NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("indication"::STRING, CHR(12288), '' ''), CHR(160), '' ''), '' +'', '' '')), '''') AS INDICATION,
        NULLIF(TRIM(REPLACE(REPLACE("current_status"::STRING, CHR(12288), '' ''), CHR(160), '' '')), '''') AS CURRENT_STATUS,
        NULLIF(TRIM(REPLACE(REPLACE("ra_category"::STRING, CHR(12288), '' ''), CHR(160), '' '')), '''') AS RA_CATEGORY,
        NULLIF(TRIM(REPLACE(REPLACE("gift_odd"::STRING, CHR(12288), '' ''), CHR(160), '' '')), '''') AS GIFT_ODD,
        NULLIF(TRIM(REPLACE(REPLACE("p_r_track"::STRING, CHR(12288), '' ''), CHR(160), '' '')), '''') AS PNR_TRACK,
        IFF(REGEXP_LIKE(NULLIF(TRIM(REPLACE(REPLACE("ra_submission_base"::STRING, CHR(12288), '' ''), CHR(160), '' '')), ''''), ''^[0-9]{4}-[0-9]{2}-[0-9]{2}.*''), LEFT(NULLIF(TRIM(REPLACE(REPLACE("ra_submission_base"::STRING, CHR(12288), '' ''), CHR(160), '' '')), ''''), 10), NULLIF(TRIM(REPLACE(REPLACE("ra_submission_base"::STRING, CHR(12288), '' ''), CHR(160), '' '')), '''')) AS RA_SUBMISSION_BASE,
        IFF(REGEXP_LIKE(NULLIF(TRIM(REPLACE(REPLACE("ra_approval_base"::STRING, CHR(12288), '' ''), CHR(160), '' '')), ''''), ''^[0-9]{4}-[0-9]{2}-[0-9]{2}.*''), LEFT(NULLIF(TRIM(REPLACE(REPLACE("ra_approval_base"::STRING, CHR(12288), '' ''), CHR(160), '' '')), ''''), 10), NULLIF(TRIM(REPLACE(REPLACE("ra_approval_base"::STRING, CHR(12288), '' ''), CHR(160), '' '')), '''')) AS RA_APPROVAL_BASE,
        IFF(REGEXP_LIKE(NULLIF(TRIM(REPLACE(REPLACE("p_r_submission_base"::STRING, CHR(12288), '' ''), CHR(160), '' '')), ''''), ''^[0-9]{4}-[0-9]{2}-[0-9]{2}.*''), LEFT(NULLIF(TRIM(REPLACE(REPLACE("p_r_submission_base"::STRING, CHR(12288), '' ''), CHR(160), '' '')), ''''), 10), NULLIF(TRIM(REPLACE(REPLACE("p_r_submission_base"::STRING, CHR(12288), '' ''), CHR(160), '' '')), '''')) AS PNR_SUBMISSION_BASE,
        IFF(REGEXP_LIKE(NULLIF(TRIM(REPLACE(REPLACE("reim_launch_best"::STRING, CHR(12288), '' ''), CHR(160), '' '')), ''''), ''^[0-9]{4}-[0-9]{2}-[0-9]{2}.*''), LEFT(NULLIF(TRIM(REPLACE(REPLACE("reim_launch_best"::STRING, CHR(12288), '' ''), CHR(160), '' '')), ''''), 10), NULLIF(TRIM(REPLACE(REPLACE("reim_launch_best"::STRING, CHR(12288), '' ''), CHR(160), '' '')), '''')) AS REIM_LAUNCH_BEST,
        IFF(REGEXP_LIKE(NULLIF(TRIM(REPLACE(REPLACE("reim_launch_base"::STRING, CHR(12288), '' ''), CHR(160), '' '')), ''''), ''^[0-9]{4}-[0-9]{2}-[0-9]{2}.*''), LEFT(NULLIF(TRIM(REPLACE(REPLACE("reim_launch_base"::STRING, CHR(12288), '' ''), CHR(160), '' '')), ''''), 10), NULLIF(TRIM(REPLACE(REPLACE("reim_launch_base"::STRING, CHR(12288), '' ''), CHR(160), '' '')), '''')) AS REIM_LAUNCH_BASE,
        NULLIF(TRIM(REPLACE(REPLACE("ra_remarks_best_approval_ra_key_milestone_etc"::STRING, CHR(12288), '' ''), CHR(160), '' '')), '''') AS RA_REMARKS,
        NULLIF(TRIM(REPLACE(REPLACE("p_r_remarks_mtp_timelines_p_r_key_milestone_etc"::STRING, CHR(12288), '' ''), CHR(160), '' '')), '''') AS PNR_REMARKS,
        NULLIF(TRIM(REPLACE(REPLACE("ra_lead"::STRING, CHR(12288), '' ''), CHR(160), '' '')), '''') AS RA_LEAD,
        NULLIF(TRIM(REPLACE(REPLACE("p_r_lead"::STRING, CHR(12288), '' ''), CHR(160), '' '')), '''') AS PNR_LEAD,
        NULLIF(TRIM(REPLACE(REPLACE("col_1_st_country_submission"::STRING, CHR(12288), '' ''), CHR(160), '' '')), '''') AS COUNTRY_SUBMISSION_1ST,
        NULLIF(TRIM(REPLACE(REPLACE("col_1_st_country_approval"::STRING, CHR(12288), '' ''), CHR(160), '' '')), '''') AS COUNTRY_APPROVAL_1ST,
        NULLIF(TRIM(REPLACE(REPLACE("pivotal_study_y_n"::STRING, CHR(12288), '' ''), CHR(160), '' '')), '''') AS PIVOTAL_STUDY,
        NULLIF(TRIM(REPLACE(REPLACE("need_for_korean_bridging_purpose_y_n_tbd"::STRING, CHR(12288), '' ''), CHR(160), '' '')), '''') AS NEED_FOR_KOREAN_BRIDGING_PURPOSE,
        NULLIF(TRIM(REPLACE(REPLACE("required_for_reg_submission_full_analysis_set"::STRING, CHR(12288), '' ''), CHR(160), '' '')), '''') AS REQUIRED_FOR_REG_SUBMISSION,
        "_line"                         AS SRC_LINE,
        "_modified"::TIMESTAMP_NTZ(6)   AS SRC_MODIFIED
    FROM COM_AZK_AIBI_RAW_MDLS."aibi_bronze"."npp_rd_rama_tracker"
    WHERE COALESCE("_fivetran_deleted", FALSE) = FALSE
  ) x
  WHERE x.BRAND IS NOT NULL OR x.STUDY_NAME IS NOT NULL OR x.INDICATION IS NOT NULL;
 
  -- (2) HISTORY MERGE : 키 = TA, BRAND, STUDY_NAME, INDICATION, DEDUP_SEQ
  MERGE INTO COM_AZK_AIBI_WRK.AIBI_SILVER.NPP_RAMA_TRACKER_RDU AS t
  USING COM_AZK_AIBI_WRK.AIBI_SILVER.NPP_RAMA_TRACKER_RDU_STAGING AS s
  ON  t.TA            IS NOT DISTINCT FROM s.TA
    AND t.BRAND         IS NOT DISTINCT FROM s.BRAND
    AND t.STUDY_NAME    IS NOT DISTINCT FROM s.STUDY_NAME
    AND t.INDICATION    IS NOT DISTINCT FROM s.INDICATION
    AND t.DEDUP_SEQ     IS NOT DISTINCT FROM s.DEDUP_SEQ
  WHEN MATCHED THEN UPDATE SET
      t.CURRENT_STATUS = s.CURRENT_STATUS,
      t.RA_CATEGORY = s.RA_CATEGORY,
      t.GIFT_ODD = s.GIFT_ODD,
      t.PNR_TRACK = s.PNR_TRACK,
      t.RA_SUBMISSION_BASE = s.RA_SUBMISSION_BASE,
      t.RA_APPROVAL_BASE = s.RA_APPROVAL_BASE,
      t.PNR_SUBMISSION_BASE = s.PNR_SUBMISSION_BASE,
      t.REIM_LAUNCH_BEST = s.REIM_LAUNCH_BEST,
      t.REIM_LAUNCH_BASE = s.REIM_LAUNCH_BASE,
      t.RA_REMARKS = s.RA_REMARKS,
      t.PNR_REMARKS = s.PNR_REMARKS,
      t.RA_LEAD = s.RA_LEAD,
      t.PNR_LEAD = s.PNR_LEAD,
      t.COUNTRY_SUBMISSION_1ST = s.COUNTRY_SUBMISSION_1ST,
      t.COUNTRY_APPROVAL_1ST = s.COUNTRY_APPROVAL_1ST,
      t.PIVOTAL_STUDY = s.PIVOTAL_STUDY,
      t.NEED_FOR_KOREAN_BRIDGING_PURPOSE = s.NEED_FOR_KOREAN_BRIDGING_PURPOSE,
      t.REQUIRED_FOR_REG_SUBMISSION = s.REQUIRED_FOR_REG_SUBMISSION
  WHEN NOT MATCHED THEN INSERT
    (TA, BRAND, STUDY_NAME, INDICATION, CURRENT_STATUS, RA_CATEGORY, GIFT_ODD, PNR_TRACK, RA_SUBMISSION_BASE, RA_APPROVAL_BASE, PNR_SUBMISSION_BASE, REIM_LAUNCH_BEST, REIM_LAUNCH_BASE, RA_REMARKS, PNR_REMARKS, RA_LEAD, PNR_LEAD, COUNTRY_SUBMISSION_1ST, COUNTRY_APPROVAL_1ST, PIVOTAL_STUDY, NEED_FOR_KOREAN_BRIDGING_PURPOSE, REQUIRED_FOR_REG_SUBMISSION, DEDUP_SEQ)
  VALUES
    (s.TA, s.BRAND, s.STUDY_NAME, s.INDICATION, s.CURRENT_STATUS, s.RA_CATEGORY, s.GIFT_ODD, s.PNR_TRACK, s.RA_SUBMISSION_BASE, s.RA_APPROVAL_BASE, s.PNR_SUBMISSION_BASE, s.REIM_LAUNCH_BEST, s.REIM_LAUNCH_BASE, s.RA_REMARKS, s.PNR_REMARKS, s.RA_LEAD, s.PNR_LEAD, s.COUNTRY_SUBMISSION_1ST, s.COUNTRY_APPROVAL_1ST, s.PIVOTAL_STUDY, s.NEED_FOR_KOREAN_BRIDGING_PURPOSE, s.REQUIRED_FOR_REG_SUBMISSION, s.DEDUP_SEQ);
 
  -- (3) GOLD : history 전체를 그대로 복사
  INSERT OVERWRITE INTO COM_AZK_AIBI_PUB.AIBI_GOLD.NPP_RAMA_TRACKER_RDU
  SELECT TA, BRAND, STUDY_NAME, INDICATION, CURRENT_STATUS, RA_CATEGORY, GIFT_ODD, PNR_TRACK, RA_SUBMISSION_BASE, RA_APPROVAL_BASE, PNR_SUBMISSION_BASE, REIM_LAUNCH_BEST, REIM_LAUNCH_BASE, RA_REMARKS, PNR_REMARKS, RA_LEAD, PNR_LEAD, COUNTRY_SUBMISSION_1ST, COUNTRY_APPROVAL_1ST, PIVOTAL_STUDY, NEED_FOR_KOREAN_BRIDGING_PURPOSE, REQUIRED_FOR_REG_SUBMISSION, DEDUP_SEQ
  FROM COM_AZK_AIBI_WRK.AIBI_SILVER.NPP_RAMA_TRACKER_RDU;
 
  RETURN ''OK'';
END;
';


-- [33] SP_LOAD_ONCOLOGY_BRAND
-- ----------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE "SP_LOAD_ONCOLOGY_BRAND"()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
BEGIN
  -- (1) STAGING : 6개 소스를 합쳐 월 컬럼을 FLATTEN -> (브랜드, 구분 컬럼, 연, 월) 한 행에 지표를 컬럼으로 모음
  --     units / counting_units 는 UNITS_OK = 1 인 소스에서만 사용 (Zoladex 는 updated 볼륨 시트, 월간 시트의 units 는 제외)
  --     점유율 / 성장률: 원천 값 그대로 (예: 0.4231).  지표가 하나도 없는 행은 만들지 않음
  INSERT OVERWRITE INTO COM_AZK_AIBI_WRK.AIBI_SILVER.EMD_ONCOLOGY_BRAND_STAGING
  SELECT
    x.BRAND_NAME,
    x.PRODUCT_NAME,
    x.MOLECULE_DESC,
    x.PACK_DESC,
    x.STRENGTH,
    x.NFC_2,
    x.YEAR,
    x.MM,
    TO_DATE(x.YEAR || ''-'' || x.MM || ''-01''),
    ROUND(x.UNITS)::NUMBER(18,0),
    ROUND(x.COUNTING_UNITS)::NUMBER(18,0),
    ROUND(x.VALUES_USD)::NUMBER(18,0),
    ROUND(x.VALUES_LC)::NUMBER(18,0),
    ROUND(x.UNITS_GROWTH_PY, 6)::NUMBER(18,6),
    ROUND(x.UNITS_MARKET_SHARE, 6)::NUMBER(18,6),
    ROUND(x.VALUES_USD_GROWTH_PY, 6)::NUMBER(18,6),
    ROUND(x.VALUES_USD_MARKET_SHARE, 6)::NUMBER(18,6),
    ROUND(x.VALUES_LC_GROWTH_PY, 6)::NUMBER(18,6),
    ROUND(x.VALUES_LC_MARKET_SHARE, 6)::NUMBER(18,6),
    x.SRC_MODIFIED
  FROM (
    SELECT z.BRAND_NAME, z.PRODUCT_NAME, z.MOLECULE_DESC, z.PACK_DESC, z.STRENGTH, z.NFC_2, z.YEAR, z.MM,
             MAX(IFF(z.UNITS_OK = 1 AND z.METRIC = ''units'', z.V, NULL)) AS UNITS,
             MAX(IFF(z.UNITS_OK = 1 AND z.METRIC = ''counting_units'', z.V, NULL)) AS COUNTING_UNITS,
             MAX(IFF(z.METRIC = ''values_usd'', z.V, NULL)) AS VALUES_USD,
             MAX(IFF(z.METRIC = ''values_lc'', z.V, NULL)) AS VALUES_LC,
             MAX(IFF(z.METRIC = ''units_growth_py'', z.V, NULL)) AS UNITS_GROWTH_PY,
             MAX(IFF(z.METRIC = ''units_market_share'', z.V, NULL)) AS UNITS_MARKET_SHARE,
             MAX(IFF(z.METRIC = ''values_usd_growth_py'', z.V, NULL)) AS VALUES_USD_GROWTH_PY,
             MAX(IFF(z.METRIC = ''values_usd_market_share'', z.V, NULL)) AS VALUES_USD_MARKET_SHARE,
             MAX(IFF(z.METRIC = ''values_lc_growth_py'', z.V, NULL)) AS VALUES_LC_GROWTH_PY,
             MAX(IFF(z.METRIC = ''values_lc_market_share'', z.V, NULL)) AS VALUES_LC_MARKET_SHARE,
             MAX(z.SRC_MODIFIED) AS SRC_MODIFIED
    FROM (
      SELECT s.BRAND_NAME, s.PRODUCT_NAME, s.MOLECULE_DESC, s.PACK_DESC, s.STRENGTH, s.NFC_2, s.UNITS_OK, s.SRC_MODIFIED,
             REGEXP_SUBSTR(f.key, ''^(units|counting_units|values_usd|values_lc|units_growth_py|units_market_share|values_usd_growth_py|values_usd_market_share|values_lc_growth_py|values_lc_market_share)_([0-9]{1,2})_([0-9]{4})$'', 1, 1, ''e'', 1)                       AS METRIC,
             LPAD(REGEXP_SUBSTR(f.key, ''^(units|counting_units|values_usd|values_lc|units_growth_py|units_market_share|values_usd_growth_py|values_usd_market_share|values_lc_growth_py|values_lc_market_share)_([0-9]{1,2})_([0-9]{4})$'', 1, 1, ''e'', 2), 2, ''0'')          AS MM,
             REGEXP_SUBSTR(f.key, ''^(units|counting_units|values_usd|values_lc|units_growth_py|units_market_share|values_usd_growth_py|values_usd_market_share|values_lc_growth_py|values_lc_market_share)_([0-9]{1,2})_([0-9]{4})$'', 1, 1, ''e'', 3)                       AS YEAR,
             TRY_TO_DOUBLE(TRIM(REPLACE(REPLACE(f.value::STRING, '','', ''''), ''%'', ''''))) AS V
      FROM (
      SELECT ''Enhertu'' AS BRAND_NAME,
             NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("product_name"::STRING, CHR(12288), '' ''), CHR(160), '' ''), '' +'', '' '')), '''') AS PRODUCT_NAME,
             NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("molecule_desc"::STRING, CHR(12288), '' ''), CHR(160), '' ''), '' +'', '' '')), '''') AS MOLECULE_DESC,
             CAST(NULL AS STRING) AS PACK_DESC,
             CAST(NULL AS STRING) AS STRENGTH,
             CAST(NULL AS STRING) AS NFC_2,
             1 AS UNITS_OK,
             "_modified"::TIMESTAMP_NTZ(6) AS SRC_MODIFIED,
             OBJECT_CONSTRUCT_KEEP_NULL(*) AS ROW_OBJ
      FROM COM_AZK_AIBI_RAW_MDLS."aibi_bronze"."kr_dashboard_oncology_brand_performance_enhertu_monthly"
      WHERE NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("product_name"::STRING, CHR(12288), '' ''), CHR(160), '' ''), '' +'', '' '')), '''') IS NOT NULL
        AND UPPER(COALESCE(NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("molecule_desc"::STRING, CHR(12288), '' ''), CHR(160), '' ''), '' +'', '' '')), ''''), '''')) <> ''GRAND TOTAL''
      UNION ALL
      SELECT ''Tagrisso'',
             NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("product_name"::STRING, CHR(12288), '' ''), CHR(160), '' ''), '' +'', '' '')), ''''),
             NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("molecule_desc"::STRING, CHR(12288), '' ''), CHR(160), '' ''), '' +'', '' '')), ''''),
             CAST(NULL AS STRING),
             CAST(NULL AS STRING),
             CAST(NULL AS STRING),
             1,
             "_modified"::TIMESTAMP_NTZ(6),
             OBJECT_CONSTRUCT_KEEP_NULL(*)
      FROM COM_AZK_AIBI_RAW_MDLS."aibi_bronze"."kr_dashboard_oncology_brand_performance_tagrisso_monthly_updated"
      WHERE NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("product_name"::STRING, CHR(12288), '' ''), CHR(160), '' ''), '' +'', '' '')), '''') IS NOT NULL
        AND UPPER(COALESCE(NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("molecule_desc"::STRING, CHR(12288), '' ''), CHR(160), '' ''), '' +'', '' '')), ''''), '''')) <> ''GRAND TOTAL''
      UNION ALL
      SELECT ''Imfinzi'',
             NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("product_name"::STRING, CHR(12288), '' ''), CHR(160), '' ''), '' +'', '' '')), ''''),
             NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("molecule_desc"::STRING, CHR(12288), '' ''), CHR(160), '' ''), '' +'', '' '')), ''''),
             CAST(NULL AS STRING),
             CAST(NULL AS STRING),
             CAST(NULL AS STRING),
             1,
             "_modified"::TIMESTAMP_NTZ(6),
             OBJECT_CONSTRUCT_KEEP_NULL(*)
      FROM COM_AZK_AIBI_RAW_MDLS."aibi_bronze"."kr_dashboard_oncology_brand_performance_lmfinzi_monthly"
      WHERE NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("product_name"::STRING, CHR(12288), '' ''), CHR(160), '' ''), '' +'', '' '')), '''') IS NOT NULL
        AND UPPER(COALESCE(NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("molecule_desc"::STRING, CHR(12288), '' ''), CHR(160), '' ''), '' +'', '' '')), ''''), '''')) <> ''GRAND TOTAL''
      UNION ALL
      SELECT ''Lynparza'',
             NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("product_name"::STRING, CHR(12288), '' ''), CHR(160), '' ''), '' +'', '' '')), ''''),
             NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("molecule_desc"::STRING, CHR(12288), '' ''), CHR(160), '' ''), '' +'', '' '')), ''''),
             CAST(NULL AS STRING),
             NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("strength"::STRING, CHR(12288), '' ''), CHR(160), '' ''), '' +'', '' '')), ''''),
             NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("nfc_2"::STRING, CHR(12288), '' ''), CHR(160), '' ''), '' +'', '' '')), ''''),
             1,
             "_modified"::TIMESTAMP_NTZ(6),
             OBJECT_CONSTRUCT_KEEP_NULL(*)
      FROM COM_AZK_AIBI_RAW_MDLS."aibi_bronze"."kr_dashboard_oncology_brand_performance_lynparza_monthly"
      WHERE NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("product_name"::STRING, CHR(12288), '' ''), CHR(160), '' ''), '' +'', '' '')), '''') IS NOT NULL
        AND UPPER(COALESCE(NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("molecule_desc"::STRING, CHR(12288), '' ''), CHR(160), '' ''), '' +'', '' '')), ''''), '''')) <> ''GRAND TOTAL''
      UNION ALL
      SELECT ''Zoladex'',
             NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("product_name"::STRING, CHR(12288), '' ''), CHR(160), '' ''), '' +'', '' '')), ''''),
             NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("molecule_desc"::STRING, CHR(12288), '' ''), CHR(160), '' ''), '' +'', '' '')), ''''),
             NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("pack_desc"::STRING, CHR(12288), '' ''), CHR(160), '' ''), '' +'', '' '')), ''''),
             CAST(NULL AS STRING),
             CAST(NULL AS STRING),
             0,
             "_modified"::TIMESTAMP_NTZ(6),
             OBJECT_CONSTRUCT_KEEP_NULL(*)
      FROM COM_AZK_AIBI_RAW_MDLS."aibi_bronze"."kr_dashboard_oncology_brand_performance_zoldadex_monthly"
      WHERE NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("product_name"::STRING, CHR(12288), '' ''), CHR(160), '' ''), '' +'', '' '')), '''') IS NOT NULL
        AND UPPER(COALESCE(NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("molecule_desc"::STRING, CHR(12288), '' ''), CHR(160), '' ''), '' +'', '' '')), ''''), '''')) <> ''GRAND TOTAL''
      UNION ALL
      SELECT ''Zoladex'',
             NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("product_name"::STRING, CHR(12288), '' ''), CHR(160), '' ''), '' +'', '' '')), ''''),
             NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("molecule_desc"::STRING, CHR(12288), '' ''), CHR(160), '' ''), '' +'', '' '')), ''''),
             NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("pack_desc"::STRING, CHR(12288), '' ''), CHR(160), '' ''), '' +'', '' '')), ''''),
             CAST(NULL AS STRING),
             CAST(NULL AS STRING),
             1,
             "_modified"::TIMESTAMP_NTZ(6),
             OBJECT_CONSTRUCT_KEEP_NULL(*)
      FROM COM_AZK_AIBI_RAW_MDLS."aibi_bronze"."kr_dashboard_oncology_brand_performance_zoladex_volume_updated"
      WHERE NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("product_name"::STRING, CHR(12288), '' ''), CHR(160), '' ''), '' +'', '' '')), '''') IS NOT NULL
        AND UPPER(COALESCE(NULLIF(TRIM(REGEXP_REPLACE(REPLACE(REPLACE("molecule_desc"::STRING, CHR(12288), '' ''), CHR(160), '' ''), '' +'', '' '')), ''''), '''')) <> ''GRAND TOTAL''
      ) s,
      LATERAL FLATTEN(input => s.ROW_OBJ) f
      WHERE REGEXP_LIKE(f.key, ''^(units|counting_units|values_usd|values_lc|units_growth_py|units_market_share|values_usd_growth_py|values_usd_market_share|values_lc_growth_py|values_lc_market_share)_([0-9]{1,2})_([0-9]{4})$'', ''i'')
    ) z
    GROUP BY z.BRAND_NAME, z.PRODUCT_NAME, z.MOLECULE_DESC, z.PACK_DESC, z.STRENGTH, z.NFC_2, z.YEAR, z.MM
  ) x
  WHERE x.UNITS IS NOT NULL
     OR x.COUNTING_UNITS IS NOT NULL
     OR x.VALUES_USD IS NOT NULL
     OR x.VALUES_LC IS NOT NULL
     OR x.UNITS_GROWTH_PY IS NOT NULL
     OR x.UNITS_MARKET_SHARE IS NOT NULL
     OR x.VALUES_USD_GROWTH_PY IS NOT NULL
     OR x.VALUES_USD_MARKET_SHARE IS NOT NULL
     OR x.VALUES_LC_GROWTH_PY IS NOT NULL
     OR x.VALUES_LC_MARKET_SHARE IS NOT NULL;
  -- (2) HISTORY MERGE : staging 은 이미 키별 1행
  MERGE INTO COM_AZK_AIBI_WRK.AIBI_SILVER.EMD_ONCOLOGY_BRAND AS t
  USING COM_AZK_AIBI_WRK.AIBI_SILVER.EMD_ONCOLOGY_BRAND_STAGING AS s
  ON  t.BRAND_NAME     IS NOT DISTINCT FROM s.BRAND_NAME
  AND t.PRODUCT_NAME   IS NOT DISTINCT FROM s.PRODUCT_NAME
  AND t.MOLECULE_DESC  IS NOT DISTINCT FROM s.MOLECULE_DESC
  AND t.PACK_DESC      IS NOT DISTINCT FROM s.PACK_DESC
  AND t.STRENGTH       IS NOT DISTINCT FROM s.STRENGTH
  AND t.NFC_2          IS NOT DISTINCT FROM s.NFC_2
  AND t.YEAR           IS NOT DISTINCT FROM s.YEAR
  AND t.MM             IS NOT DISTINCT FROM s.MM
  WHEN MATCHED THEN UPDATE SET
    t.YYYYMMDD = s.YYYYMMDD,
    t.UNITS = s.UNITS,
    t.COUNTING_UNITS = s.COUNTING_UNITS,
    t.VALUES_USD = s.VALUES_USD,
    t.VALUES_LC = s.VALUES_LC,
    t.UNITS_GROWTH_PY = s.UNITS_GROWTH_PY,
    t.UNITS_MARKET_SHARE = s.UNITS_MARKET_SHARE,
    t.VALUES_USD_GROWTH_PY = s.VALUES_USD_GROWTH_PY,
    t.VALUES_USD_MARKET_SHARE = s.VALUES_USD_MARKET_SHARE,
    t.VALUES_LC_GROWTH_PY = s.VALUES_LC_GROWTH_PY,
    t.VALUES_LC_MARKET_SHARE = s.VALUES_LC_MARKET_SHARE
  WHEN NOT MATCHED THEN INSERT
    (BRAND_NAME, PRODUCT_NAME, MOLECULE_DESC, PACK_DESC, STRENGTH, NFC_2, YEAR, MM, YYYYMMDD, UNITS, COUNTING_UNITS, VALUES_USD, VALUES_LC, UNITS_GROWTH_PY, UNITS_MARKET_SHARE, VALUES_USD_GROWTH_PY, VALUES_USD_MARKET_SHARE, VALUES_LC_GROWTH_PY, VALUES_LC_MARKET_SHARE)
  VALUES
    (s.BRAND_NAME, s.PRODUCT_NAME, s.MOLECULE_DESC, s.PACK_DESC, s.STRENGTH, s.NFC_2, s.YEAR, s.MM, s.YYYYMMDD, s.UNITS, s.COUNTING_UNITS, s.VALUES_USD, s.VALUES_LC, s.UNITS_GROWTH_PY, s.UNITS_MARKET_SHARE, s.VALUES_USD_GROWTH_PY, s.VALUES_USD_MARKET_SHARE, s.VALUES_LC_GROWTH_PY, s.VALUES_LC_MARKET_SHARE);
  -- (3) GOLD : history 전체를 그대로 복사
  INSERT OVERWRITE INTO COM_AZK_AIBI_PUB.AIBI_GOLD.EMD_ONCOLOGY_BRAND
  SELECT BRAND_NAME, PRODUCT_NAME, MOLECULE_DESC, PACK_DESC, STRENGTH, NFC_2, YEAR, MM, YYYYMMDD, UNITS, COUNTING_UNITS, VALUES_USD, VALUES_LC, UNITS_GROWTH_PY, UNITS_MARKET_SHARE, VALUES_USD_GROWTH_PY, VALUES_USD_MARKET_SHARE, VALUES_LC_GROWTH_PY, VALUES_LC_MARKET_SHARE
  FROM COM_AZK_AIBI_WRK.AIBI_SILVER.EMD_ONCOLOGY_BRAND;
  RETURN ''OK'';
END;
';


-- [34] SP_LOAD_UBIST_MONTHLY_VOLUME
-- ----------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE "SP_LOAD_UBIST_MONTHLY_VOLUME"()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
BEGIN
  -- (1) STAGING : 5개 소스 UNION ALL -> 월 컬럼 FLATTEN -> 월 이름을 YEAR / MM 으로 분리
  INSERT OVERWRITE INTO COM_AZK_AIBI_WRK.AIBI_SILVER.EMD_UBIST_MONTHLY_VOLUME_STAGING
  SELECT
    x.BRAND_NAME,
    x.THERAPY_AREA,
    x.BU,
    x.ATC4,
    x.DEFINITION,
    x.MOLECULE_COMBINATION,
    x.PRODUCT_NAME,
    x.MONO_ORIGINAL_GENERIC,
    x.COMPETITOR_LIST_FOR_CHART,
    x.CORPORATION,
    x.GX_ORIGINAL,
    x.BED_NUMBER,
    x.CHANNEL,
    x.YEAR,
    x.MM,
    TO_DATE(x.YEAR || ''-'' || x.MM || ''-01''),
    x.RX_VOLUME,
    x.RX_DOT_VOLUME,
    x.SRC_LINE,
    x.SRC_MODIFIED
  FROM (
    SELECT y.*,
           MAX(IFF(y.RX_VOLUME IS NOT NULL OR y.RX_DOT_VOLUME IS NOT NULL, 1, 0))
             OVER (PARTITION BY y.BRAND_NAME, y.YEAR, y.MM) AS MONTH_HAS_DATA
    FROM (
      SELECT s.BRAND_NAME, s.THERAPY_AREA, s.BU, s.ATC4, s.DEFINITION, s.MOLECULE_COMBINATION, s.PRODUCT_NAME, s.MONO_ORIGINAL_GENERIC, s.COMPETITOR_LIST_FOR_CHART, s.CORPORATION, s.GX_ORIGINAL, s.BED_NUMBER, s.CHANNEL,
             SPLIT_PART(f.key, ''_'', 2)                                       AS YEAR,
             DECODE(LOWER(SPLIT_PART(f.key, ''_'', 1)), ''january'',''01'',''february'',''02'',''march'',''03'',''april'',''04'',''may'',''05'',''june'',''06'',''july'',''07'',''august'',''08'',''september'',''09'',''october'',''10'',''november'',''11'',''december'',''12'')  AS MM,
             s.SRC_LINE,
             s.SRC_MODIFIED,
             MAX(IFF(ENDSWITH(LOWER(f.key), ''_counting_units_volume_absolute''),   TRY_TO_DOUBLE(TRIM(REPLACE(f.value::STRING, '','', ''''))), NULL)) AS RX_VOLUME,
             MAX(IFF(ENDSWITH(LOWER(f.key), ''_days_of_therapy_volume_absolute''), TRY_TO_DOUBLE(TRIM(REPLACE(f.value::STRING, '','', ''''))), NULL)) AS RX_DOT_VOLUME
      FROM (
    SELECT ''Crestor'' AS BRAND_NAME,
           "ta" AS THERAPY_AREA,
           "bu" AS BU,
           "atc_4" AS ATC4,
           "definition_1" AS DEFINITION,
           "molecule_combination" AS MOLECULE_COMBINATION,
           "product" AS PRODUCT_NAME,
           "mono_original_vs_mono_generic" AS MONO_ORIGINAL_GENERIC,
           "competitor_list_for_chart" AS COMPETITOR_LIST_FOR_CHART,
           "company" AS CORPORATION,
           "gx_v_original" AS GX_ORIGINAL,
           "bed_number" AS BED_NUMBER,
           "channel" AS CHANNEL,
           "_line" AS SRC_LINE,
           "_modified"::TIMESTAMP_NTZ(6) AS SRC_MODIFIED,
           OBJECT_CONSTRUCT_KEEP_NULL(*) AS ROW_OBJ
    FROM COM_AZK_AIBI_RAW_MDLS."aibi_bronze"."kr_dashboard_ubist_volume_monthly_bbu_crestor"
        UNION ALL
    SELECT ''Nexium'',
           "ta",
           "bu",
           "atc_4",
           NULL,
           "molecule_combination",
           "product",
           NULL,
           "competitor_list_for_chart_1",
           "company",
           "gx_v_original",
           NULL,
           "channel",
           "_line" AS SRC_LINE,
           "_modified"::TIMESTAMP_NTZ(6) AS SRC_MODIFIED,
           OBJECT_CONSTRUCT_KEEP_NULL(*) AS ROW_OBJ
    FROM COM_AZK_AIBI_RAW_MDLS."aibi_bronze"."kr_dashboard_ubist_volume_monthly_bbu_nexium"
        UNION ALL
    SELECT "do_t_volume_by_brand",
           NULL,
           NULL,
           NULL,
           NULL,
           NULL,
           NULL,
           NULL,
           NULL,
           NULL,
           NULL,
           NULL,
           "channel",
           "_line" AS SRC_LINE,
           "_modified"::TIMESTAMP_NTZ(6) AS SRC_MODIFIED,
           OBJECT_CONSTRUCT_KEEP_NULL(*) AS ROW_OBJ
    FROM COM_AZK_AIBI_RAW_MDLS."aibi_bronze"."kr_dashboard_ubist_volume_monthly_bbu_symbicort_cl"
        UNION ALL
    SELECT "do_t_volume_by_brand",
           NULL,
           NULL,
           NULL,
           NULL,
           NULL,
           NULL,
           NULL,
           NULL,
           NULL,
           NULL,
           NULL,
           "channel",
           "_line" AS SRC_LINE,
           "_modified"::TIMESTAMP_NTZ(6) AS SRC_MODIFIED,
           OBJECT_CONSTRUCT_KEEP_NULL(*) AS ROW_OBJ
    FROM COM_AZK_AIBI_RAW_MDLS."aibi_bronze"."kr_dashboard_ubist_volume_monthly_bbu_symbicort_gh"
        UNION ALL
    SELECT "do_t_volume_by_brand",
           NULL,
           NULL,
           NULL,
           NULL,
           NULL,
           NULL,
           NULL,
           NULL,
           NULL,
           NULL,
           NULL,
           "channel",
           "_line" AS SRC_LINE,
           "_modified"::TIMESTAMP_NTZ(6) AS SRC_MODIFIED,
           OBJECT_CONSTRUCT_KEEP_NULL(*) AS ROW_OBJ
    FROM COM_AZK_AIBI_RAW_MDLS."aibi_bronze"."kr_dashboard_ubist_volume_monthly_bbu_symbicort_sh"
      ) s,
      LATERAL FLATTEN(input => s.ROW_OBJ) f
      WHERE REGEXP_LIKE(f.key, ''^(january|february|march|april|may|june|july|august|september|october|november|december)_[0-9]{4}_(counting_units|days_of_therapy)_volume_absolute$'', ''i'')
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17
    ) y
  ) x
  WHERE x.MONTH_HAS_DATA = 1;
 
  -- (2) HISTORY MERGE : 같은 키가 여러 행이면 _modified 가 최신인 행 채택
  MERGE INTO COM_AZK_AIBI_WRK.AIBI_SILVER.EMD_UBIST_MONTHLY_VOLUME AS t
  USING (
    SELECT *
    FROM COM_AZK_AIBI_WRK.AIBI_SILVER.EMD_UBIST_MONTHLY_VOLUME_STAGING
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY BRAND_NAME, THERAPY_AREA, BU, ATC4, DEFINITION, MOLECULE_COMBINATION, PRODUCT_NAME, MONO_ORIGINAL_GENERIC, COMPETITOR_LIST_FOR_CHART, CORPORATION, GX_ORIGINAL, BED_NUMBER, CHANNEL, YEAR, MM
      ORDER BY SRC_MODIFIED DESC NULLS LAST, SRC_LINE DESC
    ) = 1
  ) AS s
  ON  t.BRAND_NAME                 IS NOT DISTINCT FROM s.BRAND_NAME
  AND t.THERAPY_AREA               IS NOT DISTINCT FROM s.THERAPY_AREA
  AND t.BU                         IS NOT DISTINCT FROM s.BU
  AND t.ATC4                       IS NOT DISTINCT FROM s.ATC4
  AND t.DEFINITION                 IS NOT DISTINCT FROM s.DEFINITION
  AND t.MOLECULE_COMBINATION       IS NOT DISTINCT FROM s.MOLECULE_COMBINATION
  AND t.PRODUCT_NAME               IS NOT DISTINCT FROM s.PRODUCT_NAME
  AND t.MONO_ORIGINAL_GENERIC      IS NOT DISTINCT FROM s.MONO_ORIGINAL_GENERIC
  AND t.COMPETITOR_LIST_FOR_CHART  IS NOT DISTINCT FROM s.COMPETITOR_LIST_FOR_CHART
  AND t.CORPORATION                IS NOT DISTINCT FROM s.CORPORATION
  AND t.GX_ORIGINAL                IS NOT DISTINCT FROM s.GX_ORIGINAL
  AND t.BED_NUMBER                 IS NOT DISTINCT FROM s.BED_NUMBER
  AND t.CHANNEL                    IS NOT DISTINCT FROM s.CHANNEL
  AND t.YEAR                       IS NOT DISTINCT FROM s.YEAR
  AND t.MM                         IS NOT DISTINCT FROM s.MM
  WHEN MATCHED THEN UPDATE SET
    t.YYYYMMDD = s.YYYYMMDD,
    t.RX_VOLUME = s.RX_VOLUME,
    t.RX_DOT_VOLUME = s.RX_DOT_VOLUME
  WHEN NOT MATCHED THEN INSERT
    (BRAND_NAME, THERAPY_AREA, BU, ATC4, DEFINITION, MOLECULE_COMBINATION, PRODUCT_NAME, MONO_ORIGINAL_GENERIC, COMPETITOR_LIST_FOR_CHART, CORPORATION, GX_ORIGINAL, BED_NUMBER, CHANNEL, YEAR, MM, YYYYMMDD, RX_VOLUME, RX_DOT_VOLUME)
  VALUES
    (s.BRAND_NAME, s.THERAPY_AREA, s.BU, s.ATC4, s.DEFINITION, s.MOLECULE_COMBINATION, s.PRODUCT_NAME, s.MONO_ORIGINAL_GENERIC, s.COMPETITOR_LIST_FOR_CHART, s.CORPORATION, s.GX_ORIGINAL, s.BED_NUMBER, s.CHANNEL, s.YEAR, s.MM, s.YYYYMMDD, s.RX_VOLUME, s.RX_DOT_VOLUME);
 
  -- (3) GOLD : history 전체를 그대로 복사
  INSERT OVERWRITE INTO COM_AZK_AIBI_PUB.AIBI_GOLD.EMD_UBIST_MONTHLY_VOLUME
  SELECT BRAND_NAME, THERAPY_AREA, BU, ATC4, DEFINITION, MOLECULE_COMBINATION, PRODUCT_NAME, MONO_ORIGINAL_GENERIC, COMPETITOR_LIST_FOR_CHART, CORPORATION, GX_ORIGINAL, BED_NUMBER, CHANNEL, YEAR, MM, YYYYMMDD, RX_VOLUME, RX_DOT_VOLUME
  FROM COM_AZK_AIBI_WRK.AIBI_SILVER.EMD_UBIST_MONTHLY_VOLUME;
 
  RETURN ''OK'';
END;
';


-- [35] SP_LOAD_UBIST_SIDAPVIA_XIGDUO
-- ----------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE "SP_LOAD_UBIST_SIDAPVIA_XIGDUO"()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
BEGIN
  -- (1) STAGING : 월 컬럼을 FLATTEN 하여 행으로 변환
  --     값이 NULL 인 행은 유지하고, 모든 행이 NULL 인 월(미래의 빈 월 컬럼)만 제외 (MONTH_HAS_DATA)
  INSERT OVERWRITE INTO COM_AZK_AIBI_WRK.AIBI_SILVER.EMD_UBIST_SIDAPVIA_XIGDUO_STAGING
  SELECT
    x.BRAND_NAME,
    x.CHANNEL,
    x.COMPETITORS,
    x.YEAR,
    x.MM,
    TO_DATE(x.YEAR || ''-'' || x.MM || ''-01''),
    ROUND(x.RX_DOT_VOLUME, 2)::NUMBER(15,2),
    x.SRC_LINE,
    x.SRC_MODIFIED
  FROM (
    SELECT y.*,
           MAX(IFF(y.RX_DOT_VOLUME IS NOT NULL, 1, 0)) OVER (PARTITION BY y.BRAND_NAME, y.YEAR, y.MM) AS MONTH_HAS_DATA
    FROM (
      SELECT
        t."_sheet_name"                   AS BRAND_NAME,        -- TODO: 0)(a) 확인 후 필요하면 수정
        t."_table"                        AS CHANNEL,           -- TODO: 0)(a) 확인 후 필요하면 수정
        t."unit_trend_k_dot"              AS COMPETITORS,
        SUBSTR(f.key, 5, 4)               AS YEAR,
        SUBSTR(f.key, 10, 2)              AS MM,
        TRY_TO_DOUBLE(TRIM(REPLACE(f.value::STRING, '','', ''''))) AS RX_DOT_VOLUME,
        t."_line"                         AS SRC_LINE,
        t."_modified"::TIMESTAMP_NTZ(6)   AS SRC_MODIFIED
      FROM (
        SELECT "_sheet_name", "_table", "unit_trend_k_dot", "_line", "_modified",
               OBJECT_CONSTRUCT_KEEP_NULL(*) AS ROW_OBJ
        FROM COM_AZK_AIBI_RAW_MDLS."aibi_bronze"."ubist_monthly_xigduo_sidapvia"
        WHERE COALESCE("_fivetran_deleted", FALSE) = FALSE
          AND "unit_trend_k_dot" IS NOT NULL
      ) t,
      LATERAL FLATTEN(input => t.ROW_OBJ) f
      WHERE REGEXP_LIKE(f.key, ''^col_[0-9]{4}_[0-9]{2}_[0-9]{2}_00_00_00$'', ''i'')
    ) y
  ) x
  WHERE x.MONTH_HAS_DATA = 1;
 
  -- (2) HISTORY MERGE : 같은 키가 여러 행이면 _modified 가 최신인 행 채택
  MERGE INTO COM_AZK_AIBI_WRK.AIBI_SILVER.EMD_UBIST_SIDAPVIA_XIGDUO AS t
  USING (
    SELECT *
    FROM COM_AZK_AIBI_WRK.AIBI_SILVER.EMD_UBIST_SIDAPVIA_XIGDUO_STAGING
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY BRAND_NAME, CHANNEL, COMPETITORS, YEAR, MM
      ORDER BY SRC_MODIFIED DESC NULLS LAST, SRC_LINE DESC
    ) = 1
  ) AS s
  ON  t.BRAND_NAME  IS NOT DISTINCT FROM s.BRAND_NAME
  AND t.CHANNEL     IS NOT DISTINCT FROM s.CHANNEL
  AND t.COMPETITORS IS NOT DISTINCT FROM s.COMPETITORS
  AND t.YEAR        IS NOT DISTINCT FROM s.YEAR
  AND t.MM          IS NOT DISTINCT FROM s.MM
  WHEN MATCHED THEN UPDATE SET
    t.YYYYMMDD      = s.YYYYMMDD,
    t.RX_DOT_VOLUME = s.RX_DOT_VOLUME
  WHEN NOT MATCHED THEN INSERT
    (BRAND_NAME, CHANNEL, COMPETITORS, YEAR, MM, YYYYMMDD, RX_DOT_VOLUME)
  VALUES
    (s.BRAND_NAME, s.CHANNEL, s.COMPETITORS, s.YEAR, s.MM, s.YYYYMMDD, s.RX_DOT_VOLUME);
 
  -- (3) GOLD : history 전체를 그대로 복사
  INSERT OVERWRITE INTO COM_AZK_AIBI_PUB.AIBI_GOLD.EMD_UBIST_SIDAPVIA_XIGDUO
  SELECT BRAND_NAME, CHANNEL, COMPETITORS, YEAR, MM, YYYYMMDD, RX_DOT_VOLUME
  FROM COM_AZK_AIBI_WRK.AIBI_SILVER.EMD_UBIST_SIDAPVIA_XIGDUO;
 
  RETURN ''OK'';
END;
';
