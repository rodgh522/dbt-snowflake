-- ==============================================================================
-- Silver Layer Task Definitions (Root & Child Tasks)
-- ==============================================================================
USE DATABASE COM_AZK_AIBI_WRK;
USE SCHEMA AIBI_SILVER;

-- 1. Silver 사전 참조 테이블 적재 태스크 (Root Task)
CREATE OR ALTER TASK COM_AZK_AIBI_WRK.AIBI_SILVER.TSK_SILVER_ISL_MATERIAL_MASTER
  WAREHOUSE = EDP_COM_AZK_AIBI_INGESTION_WH
  SCHEDULE = 'USING CRON 0 4 * * * Asia/Seoul' -- 매일 04:00 KST
  AS
    CALL COM_AZK_AIBI_WRK.AIBI_SILVER.SP_ISL_MATERIAL_MASTER();

-- 2. Silver 자재 마스터 적재 태스크 (Child Task - Root 완료 후 실행)
CREATE OR ALTER TASK COM_AZK_AIBI_WRK.AIBI_SILVER.TSK_SILVER_INV_MATERIAL_MASTER
  WAREHOUSE = EDP_COM_AZK_AIBI_INGESTION_WH
  AFTER COM_AZK_AIBI_WRK.AIBI_SILVER.TSK_SILVER_ISL_MATERIAL_MASTER
  AS
    CALL COM_AZK_AIBI_WRK.AIBI_SILVER.SP_INV_MATERIAL_MASTER();
