# Snowflake Iceberg Medallion Lakehouse 프로젝트 명세서 (PROJECT.md)

## 1. 프로젝트 개요 (Overview)

본 프로젝트는 **Snowflake**를 코어 데이터 플랫폼으로 활용하여, **Apache Iceberg Open Table Format**과 **AWS S3 External Volume**을 결합한 엔터프라이즈급 **Medallion Architecture (Bronze → Silver → Gold)** 데이터 레이크하우스를 구축하는 것을 목표로 합니다.

### 핵심 설계 원칙
1. **스토리지와 컴퓨팅의 완전한 분리**: 데이터는 고객 소유의 AWS S3 버킷에 Parquet/Iceberg 형식으로 영구 보관되며, Snowflake는 고성능 쿼리 및 데이터 가공 컴퓨팅 엔진으로 동작합니다.
2. **오픈 테이블 포맷 (Apache Iceberg)**: 벤더 락인을 방지하고, ACID 트랜잭션, 타임 트래블, 스키마 에볼루션을 보장하며, 외부 분석 엔진(Athena, Spark 등)과의 상호 운용성을 확보합니다.
3. **네이티브 서버리스 파이프라인**: 복잡한 외부 오케스트레이터(Airflow 등) 의존성을 최소화하고, Snowflake Stored Procedure(Snowflake Scripting)와 Snowflake Tasks를 활용하여 멱등성(Idempotency)과 무결성이 보장되는 파이프라인을 운영합니다.

---

## 2. Snowflake 환경 및 인프라 명세

### 2.1 클라우드 및 에디션/리전 구성
- **Snowflake Edition / Tier**: **Business Critical Edition** (AWS PrivateLink 사설 통신망 연동, Tri-Secret Secure, 전송/저장 데이터 고수준 암호화 지원 필수 티어).
- **클라우드 제공자**: AWS
- **배치 원칙**: Snowflake 계정과 AWS S3 버킷(Bronze, Silver, Gold)을 **동일한 AWS 리전(예: ap-northeast-2)**에 구성하여 데이터 전송 지연시간을 단축하고 불필요한 Data Egress 비용을 제거합니다.

### 2.2 가상 웨어하우스(Virtual Warehouse) 설계
워크로드의 성격에 따라 웨어하우스를 물리적으로 격리하여 리소스 경합을 방지하고 비용을 최적화합니다.

| 웨어하우스명 | 크기 | Auto-Suspend | 멀티 클러스터 | 주요 워크로드 |
| :--- | :---: | :---: | :---: | :--- |
| `WH_INGEST_XS` | X-Small | 60초 | No | Bronze 카탈로그 동기화 및 메타데이터 갱신 |
| `WH_TRANSFORM_SILVER` | Medium | 60초 | No | Silver 계층 Stored Procedure 실행 (정제/유효성검증) |
| `WH_TRANSFORM_GOLD` | Medium | 60초 | No | Gold 계층 Stored Procedure 실행 (마트 집계) |
| `WH_ANALYTICS_BI` | Medium / Large | 120초 | Yes (1~3) | BI 대시보드(Tableau, PowerBI 등) 및 분석가 쿼리 |

### 2.3 데이터베이스 및 스키마 명명 체계
계층별로 전용 데이터베이스와 스키마를 논리적·물리적으로 분리합니다.

| 레이어 | 데이터베이스명 | 스키마명 | External Volume | 설명 |
| :--- | :--- | :--- | :--- | :--- |
| **Bronze** | `COM_AZK_AIBI_RAW_MDLS` | `aibi_bronze` | `EXT_VOL_BRONZE_S3` | Fivetran/S3에서 동기화된 원천 Iceberg 테이블 (Read-Only) |
| **Silver** | `COM_AZK_AIBI_WRK` | `AIBI_SILVER` | `EXT_VOL_SILVER_S3` | 비즈니스 룰 검증, 정제 및 표준화된 테이블 (Read/Write) |
| **Gold** | `COM_AZK_AIBI_PUB` | `AIBI_GOLD` | `EXT_VOL_GOLD_S3` | 최종 비즈니스 KPI 집계 마트 테이블 (Read/Write) |
| **Admin** | `COM_AZK_MGMT_DB` | `MONITORING`, `TASKS` | - | 파이프라인 감사 로그, Event Table, 알림 연동 |

### 2.4 Snowflake Admin Group 중앙 관리 요소 (dbt 불포함 인프라 영역)
데이터 파이프라인(dbt, 일반 배치)의 권한 범위를 제한하고, 거버넌스 및 비용 통제를 위해 다음 인프라 요소는 **Snowflake Admin Group (`ACCOUNTADMIN` / `SYSADMIN`)**에서 전담 관리합니다.

1. **Warehouse (가상 웨어하우스 전담 관리)**:
   - **관리 주체**: Snowflake Admin Group.
   - **통제 범위**: 웨어하우스 생성(`CREATE WAREHOUSE`), 크기 조정(Resizing), Auto-Suspend / Auto-Resume 정책, Multi-Cluster 설정, Resource Monitor(월간 크레딧 한도 및 강제 서스펜드 정책) 설정.
   - **파이프라인/개발자 권한**: dbt 및 일반 파이프라인 역할은 웨어하우스 생성/수정 권한이 없으며, 오직 지정된 웨어하우스에 대한 `USAGE` 권한만 부여받아 쿼리를 실행합니다.
2. **Catalog & Storage (데이터 거버넌스 및 카탈로그 전담 관리)**:
   - **관리 주체**: Snowflake Admin Group.
   - **통제 범위**: Database, Schema, Catalog Integration(AWS Glue, Snowflake Polaris, Snowflake Internal Catalog), External Volume, Storage Integration 생성 및 AWS IAM 보안 연동.
   - **파이프라인/개발자 권한**: dbt 및 파이프라인은 Admin이 사전에 프로비저닝한 Catalog 및 External Volume 환경 내에서 테이블 생성/구체화(`CREATE ICEBERG TABLE`, `DML`)만 수행합니다.

---

## 3. 보안, 인증 및 인가 체계

### 3.1 사용자 및 시스템 인증 (Authentication)
1. **일반 사용자 & 데이터 분석가 (SSO Only 접근 강제)**:
   - **접속 방식**: **IdP SSO(Single Sign-On)로만 접속 가능** (아이디/패스워드 직접 로그인 차단).
   - **인증 연동**: 사내 IdP (Okta, Azure AD 등)와 연동된 **SAML 2.0 기반 SSO** 및 **SCIM** 자동 프로비저닝 적용.
   - **MFA 의무화**: IdP 차원의 조건부 액세스 및 MFA(다중 요소 인증) 필수 통과.
   - **보안 정책**: Snowflake `AUTHENTICATION POLICY`를 통해 일반 유저의 패스워드 인증 방식을 원천 비활성화.
2. **CI/CD 파이프라인 (GitHub Actions)**:
   - 비밀번호/PAT 없이 **GitHub Actions OIDC (Workload Identity Federation)** 기반 인증 연동.
3. **배치 및 시스템 연동 계정**:
   - **RSA 2048-bit Key-pair 인증** (`JWT Token`)을 사용하며 비밀번호 로그인 차단.

### 3.2 네트워크 인가 및 엔터프라이즈 티어 통제 (AWS PrivateLink & VPN 단말 제한)
- **Snowflake Tier & 연결 구조 (AWS PrivateLink)**:
  - Snowflake **Business Critical Edition**의 **AWS PrivateLink** 기능을 적용하여 공용 인터넷(Public Internet) 노출을 완전히 차단.
  - 전용 사설 엔드포인트 URL(`https://<account_name>.<region>.privatelink.snowflakecomputing.com`)을 통해서만 통신.
- **VPN 인가 특정 PC(단말기)만 접속 허용 (3중 방어 체계)**:
  1. **네트워크 레벨 (Snowflake Network Policy)**:
     - Snowflake `NETWORK POLICY`의 `ALLOWED_IP_LIST`에 사내 승인된 **전용 VPN 게이트웨이의 고정 IP 대역만 등록**하여, VPN 터널을 경유하지 않은 모든 접근 차단.
  2. **단말기 레벨 (Device Trust & Corporate VPN)**:
     - 사내 자산 관리 시스템(MDM / Intune / Jamf)에 등록되어 인증서(mTLS)가 설치된 **인가된 특정 업무용 PC**에만 사내 VPN 클라이언트 접속 프로파일을 배포.
     - 비인가 개인 단말(BYOD)이나 승인되지 않은 PC에서의 VPN 접속 원천 차단.
  3. **인증 레벨 (IdP Conditional Access & SSO)**:
     - 사내 IdP(Azure AD, Okta 등)의 조건부 액세스 정책(Conditional Access Policy)을 구성하여, **"인가된 특정 PC(Compliant Device) + 사내 VPN IP 대역"** 조건이 모두 충족될 때만 Snowflake SSO 로그인이 성공하도록 통제.

### 3.3 Snowflake 내 권한 관리 체계 (RBAC: 2-Tier Architecture)

접근 권한을 오브젝트 중심의 **Access Role (접근 역할)**과 업무 중심의 **Functional Role (직무 역할)**로 분리합니다.

```mermaid
graph TD
    subgraph Functional_Roles["직무 역할 (Functional Roles)"]
        FR_ADMIN["FR_DATA_ENGINEER_ADMIN"]
        FR_PIPELINE["FR_PIPELINE_SERVICE"]
        FR_ANALYST["FR_DATA_ANALYST"]
        FR_BI["FR_BI_SERVICE"]
    end

    subgraph Access_Roles["접근 역할 (Access Roles)"]
        AR_BRONZE_RO["AR_BRONZE_RO\n(USAGE DB/Schema, SELECT Iceberg)"]
        AR_SILVER_RW["AR_SILVER_RW\n(USAGE, CREATE, DML, EXECUTE PROC)"]
        AR_SILVER_RO["AR_SILVER_RO\n(USAGE, SELECT Iceberg)"]
        AR_GOLD_RW["AR_GOLD_RW\n(USAGE, CREATE, DML, EXECUTE PROC)"]
        AR_GOLD_RO["AR_GOLD_RO\n(USAGE, SELECT Iceberg)"]
        AR_TASK_OP["AR_TASK_OPERATOR\n(OPERATE, EXECUTE TASK)"]
        AR_WH_USAGE["AR_WH_*_USAGE\n(USAGE ON WAREHOUSE)"]
    end

    FR_ADMIN --> AR_BRONZE_RO
    FR_ADMIN --> AR_SILVER_RW
    FR_ADMIN --> AR_GOLD_RW
    FR_ADMIN --> AR_TASK_OP

    FR_PIPELINE --> AR_BRONZE_RO
    FR_PIPELINE --> AR_SILVER_RW
    FR_PIPELINE --> AR_GOLD_RW
    FR_PIPELINE --> AR_TASK_OP
    FR_PIPELINE --> AR_WH_USAGE

    FR_ANALYST --> AR_SILVER_RO
    FR_ANALYST --> AR_GOLD_RO
    FR_ANALYST --> AR_WH_USAGE

    FR_BI --> AR_GOLD_RO
    FR_BI --> AR_WH_USAGE
```

#### 세부 권한 설정
- **Snowflake Admin Group (`ACCOUNTADMIN` / `SYSADMIN`)**:
  - `CREATE WAREHOUSE`, `MODIFY WAREHOUSE`, `CREATE RESOURCE MONITOR` 등 모든 웨어하우스 인프라 제어 전담.
  - `CREATE DATABASE`, `CREATE EXTERNAL VOLUME`, `CREATE CATALOG INTEGRATION` 등 카탈로그 및 스토리지 통합 전담.
  - 일반 개발자/파이프라인 역할에는 상기 생성 권한을 일체 위임하지 않음.
- **Bronze (`AR_BRONZE_RO`)**:
  - `USAGE ON DATABASE COM_AZK_AIBI_RAW_MDLS`
  - `USAGE ON SCHEMA COM_AZK_AIBI_RAW_MDLS."aibi_bronze"`
  - `SELECT ON ALL/FUTURE ICEBERG TABLES IN SCHEMA COM_AZK_AIBI_RAW_MDLS."aibi_bronze"`
  - **DML(INSERT/UPDATE/DELETE/ALTER) 권한 부여 원천 차단 (Read-Only 보장)**
- **Silver (`AR_SILVER_RW`)**:
  - `USAGE ON DATABASE COM_AZK_AIBI_WRK`
  - `USAGE, CREATE ICEBERG TABLE, CREATE PROCEDURE, CREATE TASK ON SCHEMA COM_AZK_AIBI_WRK.AIBI_SILVER`
  - `ALL PRIVILEGES ON ALL/FUTURE ICEBERG TABLES IN SCHEMA COM_AZK_AIBI_WRK.AIBI_SILVER`
- **Gold (`AR_GOLD_RW`)**:
  - `USAGE ON DATABASE COM_AZK_AIBI_PUB`
  - `USAGE, CREATE ICEBERG TABLE, CREATE PROCEDURE, CREATE TASK ON SCHEMA COM_AZK_AIBI_PUB.AIBI_GOLD`
  - `ALL PRIVILEGES ON ALL/FUTURE ICEBERG TABLES IN SCHEMA COM_AZK_AIBI_PUB.AIBI_GOLD`

---

## 4. Snowflake 작업 환경 및 데이터 파이프라인

### 4.1 전체 아키텍처 개요

```mermaid
flowchart LR
    subgraph Storage_Tier["AWS S3 Storage (Multi-Bucket)"]
        S3_B[("s3://azk-aibi-bronze-raw\n(Bucket 1)")]
        S3_S[("s3://azk-aibi-silver-wrk\n(Bucket 2)")]
        S3_G[("s3://azk-aibi-gold-pub\n(Bucket 3)")]
    end

    subgraph Bronze_Tier["1. Bronze Tier (Raw / Unmanaged)"]
        EV_B["External Volume\n(EXT_VOL_BRONZE_S3)\n[READ_ONLY]"]
        T_B["Iceberg Table\n(kr_material_master)\n자동 생성 / Read-Only"]
    end

    subgraph Silver_Tier["2. Silver Tier (Cleansed / Work)"]
        EV_S["External Volume\n(EXT_VOL_SILVER_S3)\n[READ_WRITE]"]
        SP_S["Stored Procedure\n(SP_INV_MATERIAL_MASTER)"]
        T_S["Iceberg Table\n(inv_material_master)"]
        T_S_REF["Iceberg Table\n(isl_material_master)"]
    end

    subgraph Gold_Tier["3. Gold Tier (Aggregated / Mart)"]
        EV_G["External Volume\n(EXT_VOL_GOLD_S3)\n[READ_WRITE]"]
        SP_G["Stored Procedure\n(SP_GOLD_SUPPLY_CHAIN_DAILY)"]
        T_G["Iceberg Table\n(dm_supply_chain_daily)"]
    end

    S3_B <-->|External Volume Sync| EV_B --> T_B
    T_B -->|Read & Validate| SP_S
    T_S_REF -->|Dimension Join| SP_S
    SP_S -->|INSERT OVERWRITE| T_S
    T_S <--> EV_S <--> S3_S

    T_S -->|Business Aggregation| SP_G
    SP_G -->|INSERT OVERWRITE| T_G
    T_G <--> EV_G <--> S3_G
```

### 4.2 AWS S3 External Volume 구성
각 계층별 독립된 S3 버킷과 IAM Role을 연동합니다.

```sql
-- 1. Bronze External Volume (Read-Only)
CREATE OR REPLACE EXTERNAL VOLUME EXT_VOL_BRONZE_S3
   STORAGE_LOCATIONS =
      (
         (
            NAME = 'ext-vol-bronze-s3'
            STORAGE_PROVIDER = 'S3'
            STORAGE_BASE_URL = 's3://azk-aibi-bronze-raw/'
            STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/snowflake-bronze-s3-role'
         )
      );

-- 2. Silver External Volume (Read & Write)
CREATE OR REPLACE EXTERNAL VOLUME EXT_VOL_SILVER_S3
   STORAGE_LOCATIONS =
      (
         (
            NAME = 'ext-vol-silver-s3'
            STORAGE_PROVIDER = 'S3'
            STORAGE_BASE_URL = 's3://azk-aibi-silver-wrk/'
            STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/snowflake-silver-s3-role'
         )
      );

-- 3. Gold External Volume (Read & Write)
CREATE OR REPLACE EXTERNAL VOLUME EXT_VOL_GOLD_S3
   STORAGE_LOCATIONS =
      (
         (
            NAME = 'ext-vol-gold-s3'
            STORAGE_PROVIDER = 'S3'
            STORAGE_BASE_URL = 's3://azk-aibi-gold-pub/'
            STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/snowflake-gold-s3-role'
         )
      );
```

* **AWS IAM Role 보안 신뢰 관계**:
  * `DESCRIBE EXTERNAL VOLUME <name>` 결과의 `STORAGE_AWS_IAM_USER_ARN` 및 `STORAGE_AWS_EXTERNAL_ID`를 IAM Role Trust Policy에 등록.
  * Bronze Role은 `s3:GetObject`, `s3:ListBucket`만 허용(쓰기 차단).
  * Silver/Gold Role은 `s3:PutObject`, `s3:DeleteObject`, `s3:AbortMultipartUpload` 추가 허용.

### 4.3 Apache Iceberg Table 구성 전략
- **Bronze 계층**:
  - External Volume 메타데이터 기반으로 테이블이 자동 생성/연동되는 Unmanaged Iceberg Table.
  - 사용자는 직접 DML/DDL 조작을 하지 않음 (Read-Only).
- **Silver 및 Gold 계층**:
  - Snowflake가 Iceberg 메타데이터 라이프사이클을 관리하는 **Snowflake Managed Iceberg Table**.
  - `CATALOG = 'SNOWFLAKE'`, `EXTERNAL_VOLUME = 'EXT_VOL_SILVER_S3'`, `BASE_LOCATION = '<table_name>/'` 명시.

```sql
-- Silver Iceberg Table 생성 예시
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
```

### 4.4 Stored Procedure 기반 데이터 전처리 표준 프레임워크

파이프라인의 모든 전처리 프로시저는 다음의 **5대 신뢰성 원칙**을 준수합니다.

1. **Short-Circuit / 변경 감지 (Skip Logic)**:
   - Bronze 원천의 변경 시각(`MAX(_fivetran_synced)`)과 타깃 테이블의 `_src_synced`, 참조 테이블의 `_load_dtm`을 비교하여 변동이 없을 경우 즉시 `SKIPPED` 반환. 불필요한 웨어하우스 비용 절감.
2. **사전 데이터 품질 게이트 (Exception Handling)**:
   - `empty_batch (-20005)`: 원천 데이터 0건 여부.
   - `ambiguous_brand (-20002)`: 1:N 조인 매핑 충돌 방지.
   - `duplicate_key (-20006)`: 자연키/기본키 중복 검증.
3. **격리된 임시 가공 (`TEMP TABLE`)**:
   - `CREATE OR REPLACE TEMP TABLE`을 통해 대소문자 무시 파싱(`GET_IGNORE_CASE`), 타입 캐스팅(`TRY_TO_DOUBLE`), 포맷팅, 참조 테이블 조인을 격리 수행.
4. **원자적 적재 (`INSERT OVERWRITE INTO`)**:
   - 전처리 및 검증이 100% 완료된 데이터만 대상 테이블에 원자적으로 덮어쓰기(All-or-Nothing).
5. **감사 추적성 확보 (Audit Trail)**:
   - 적재 시각(`_load_dtm`)과 원천 동기화 시점(`_src_synced`)을 기록하여 데이터 계보(Lineage) 확보.

#### [참조 프로시저 구현 코드]
```sql
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
    -- 0. 변경 여부 체크
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
    -- 1. 브랜드 매핑 충돌 확인
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
```

### 4.5 Snowflake Tasks를 활용한 DAG 오케스트레이션

선행 태스크 완료 후 후행 태스크가 순차 실행되는 Serverless DAG 구조를 구축합니다.

```sql
USE SCHEMA COM_AZK_AIBI_WRK.AIBI_SILVER;

-- 1. Silver 사전 참조 테이블 적재 태스크 (Root Task)
CREATE OR REPLACE TASK TSK_SILVER_ISL_MATERIAL_MASTER
  WAREHOUSE = WH_TRANSFORM_SILVER
  SCHEDULE = 'USING CRON 0 4 * * * Asia/Seoul' -- 매일 04:00 KST
  AS
    CALL SP_ISL_MATERIAL_MASTER();

-- 2. Silver 자재 마스터 적재 태스크 (Child Task - Root 완료 후 실행)
CREATE OR REPLACE TASK TSK_SILVER_INV_MATERIAL_MASTER
  WAREHOUSE = WH_TRANSFORM_SILVER
  AFTER TSK_SILVER_ISL_MATERIAL_MASTER
  AS
    CALL SP_INV_MATERIAL_MASTER();

-- 3. Gold 계층 집계 태스크 (Child Task - Silver 완료 후 실행)
CREATE OR REPLACE TASK COM_AZK_AIBI_PUB.AIBI_GOLD.TSK_GOLD_SUPPLY_CHAIN_DAILY
  WAREHOUSE = WH_TRANSFORM_GOLD
  AFTER COM_AZK_AIBI_WRK.AIBI_SILVER.TSK_SILVER_INV_MATERIAL_MASTER
  AS
    CALL COM_AZK_AIBI_PUB.AIBI_GOLD.SP_GOLD_SUPPLY_CHAIN_DAILY();

-- 4. 태스크 활성화 (반드시 하위 Child부터 상위 Root 순서로 RESUME)
ALTER TASK COM_AZK_AIBI_PUB.AIBI_GOLD.TSK_GOLD_SUPPLY_CHAIN_DAILY RESUME;
ALTER TASK COM_AZK_AIBI_WRK.AIBI_SILVER.TSK_SILVER_INV_MATERIAL_MASTER RESUME;
ALTER TASK COM_AZK_AIBI_WRK.AIBI_SILVER.TSK_SILVER_ISL_MATERIAL_MASTER RESUME;
```

---

## 5. 운영, 모니터링 및 거버넌스

1. **Resource Monitor (크레딧 한도 보호)**:
   - `WH_TRANSFORM_SILVER`, `WH_TRANSFORM_GOLD` 등 각 배치 웨어하우스에 크레딧 할당량을 지정하여 예산 초과 방지.
2. **Event Table 관측성 (Logging & Tracing)**:
   - 프로시저 실행 스키마에 `ALTER SCHEMA ... SET LOG_LEVEL = 'INFO', TRACE_LEVEL = 'ALWAYS'`를 적용하여 모든 세부 실행 이력 및 예외를 중앙 테이블에 수집.
3. **태스크 오류 알림 (Notification Integration)**:
   - `CREATE NOTIFICATION INTEGRATION`을 통해 AWS SNS 또는 Webhook과 연동하여 Task 실패 시 실시간 장애 알림 발송.
