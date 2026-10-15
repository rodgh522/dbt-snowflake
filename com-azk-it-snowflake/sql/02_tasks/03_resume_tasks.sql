-- ==============================================================================
-- Resume Tasks DAG (MUST resume in reverse order: Child -> Root)
-- ==============================================================================

-- 1. Leaf / Child Tasks 먼저 활성화
ALTER TASK IF EXISTS COM_AZK_AIBI_PUB.AIBI_GOLD.TSK_GOLD_SUPPLY_CHAIN_DAILY RESUME;
ALTER TASK IF EXISTS COM_AZK_AIBI_WRK.AIBI_SILVER.TSK_SILVER_INV_MATERIAL_MASTER RESUME;

-- 2. Root Task 마지막에 활성화
ALTER TASK IF EXISTS COM_AZK_AIBI_WRK.AIBI_SILVER.TSK_SILVER_ISL_MATERIAL_MASTER RESUME;
