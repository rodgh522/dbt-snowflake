-- ==============================================================================
-- Snowflake Tasks Orchestration for com-azk-it-snowflake dbt project
-- ==============================================================================

-- 1. 기존 실행 중인 태스크 일시 정지 (SUSPEND)
ALTER TASK IF EXISTS TSK_RUN_COM_AZK_LAKEHOUSE SUSPEND;

-- 2. dbt 프로젝트 전체 모델 빌드 및 테스트 태스크
CREATE OR ALTER TASK TSK_RUN_COM_AZK_LAKEHOUSE
  WAREHOUSE = SNOWFLAKE_LEARNING_WH
  SCHEDULE = 'USING CRON 0 4 * * * Asia/Seoul' -- 매일 04:00 KST
  AS
    EXECUTE DBT PROJECT com_azk_it_snowflake_object ARGS='build --target prod';

-- 3. 태스크 활성화 (RESUME)
ALTER TASK IF EXISTS TSK_RUN_COM_AZK_LAKEHOUSE RESUME;
