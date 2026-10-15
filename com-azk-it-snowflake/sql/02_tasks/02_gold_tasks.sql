-- ==============================================================================
-- Gold Layer Task Definitions (Child Task of Silver)
-- ==============================================================================
USE DATABASE COM_AZK_AIBI_PUB;
USE SCHEMA AIBI_GOLD;

-- Gold 계층 집계 태스크 (Child Task - Silver 완료 후 실행)
CREATE OR ALTER TASK COM_AZK_AIBI_PUB.AIBI_GOLD.TSK_GOLD_SUPPLY_CHAIN_DAILY
  WAREHOUSE = EDP_COM_AZK_AIBI_INGESTION_WH
  AFTER COM_AZK_AIBI_WRK.AIBI_SILVER.TSK_SILVER_INV_MATERIAL_MASTER
  AS
    CALL COM_AZK_AIBI_PUB.AIBI_GOLD.SP_GOLD_SUPPLY_CHAIN_DAILY();
