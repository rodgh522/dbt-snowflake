# Task 2: Stored Procedure to dbt 변환 및 구현 상세

본 문서는 `task_2.md`에 정의된 Snowflake Stored Procedure(`SP_ISL_PATIENT_PROGRES_BRANDS`)를 dbt 아키텍처 및 모범 사례에 맞추어 변환한 내용과 구현 상세를 정리한 문서입니다.

---

## 1. 개요 (Overview)

- **목적**: 기존 명령형/절차적 Snowflake Stored Procedure를 dbt의 선언적 SQL 모델 및 데이터 품질 테스트 구조로 전환
- **대상 원천 데이터 (Bronze)**:
  - `DBT_TEST.AIBI_BRONZE."patient_progress_brands_tagrisso"`
  - `DBT_TEST.AIBI_BRONZE."patient_progress_brands_enhertu"`
- **대상 적재 데이터 (Silver)**:
  - `DBT_TEST.AIBI_SILVER.isl_patient_progress_brands` (Iceberg Table 형식)

---

## 2. 주요 변환 비교 (Procedure vs. dbt)

| 항목 | 기존 Stored Procedure (`task_2.md`) | dbt 변환 구현 (`models/silver/`) | 비고 / 개선 효과 |
| :--- | :--- | :--- | :--- |
| **원천 참조 방식** | `COM_AZK_AIBI_RAW_MDLS."aibi_bronze".*` 직접 하드코딩 | `{{ source('bronze', 'patient_progress_brands_tagrisso') }}`, `{{ source('bronze', 'patient_progress_brands_enhertu') }}` | `sources.yml`을 통해 환경(dev/prod)별 동적 바인딩 및 데이터 리니지(DAG) 추적 가능 |
| **테이블 관리 방식** | `CREATE TEMP TABLE` 후 `INSERT OVERWRITE` 수행 | `dbt_project.yml`의 Silver 설정을 상속받아 `materialized='table'` (Iceberg Table) 자동 생성 | DDL/DML 절차 없이 선언적 SELECT 쿼리로 Snowflake Iceberg Table 관리 |
| **동기화 시점 (`_src_synced`)** | 프로시저 변수 `v_src_synced`에 두 소스의 최신 `_fivetran_synced`를 구하여 컬럼으로 적재 | `src_synced` CTE에서 `GREATEST(MAX(tagrisso), MAX(enhertu))` 계산 후 `CROSS JOIN` | 단일 쿼리 파이프라인 내에서 메타데이터를 원자적으로 일괄 부여 |
| **대소문자 무관 JSON 파싱** | `OBJECT_CONSTRUCT_KEEP_NULL` + `GET_IGNORE_CASE` 활용 | 동일한 패턴을 `bronze` 및 `parsed` CTE로 분리 구현 | Iceberg/Bronze 원천의 따옴표, 대소문자 차이에 유연하게 대응 |
| **기준월 계산 (`base_month`)** | `DATE_FROM_PARTS` + `DECODE` (2/4자리 연도 및 영문 월 약어 대응) | `transformed` CTE에서 동일 변환 로직 적용 및 `WHERE brand_name IS NOT NULL` 필터링 | 1일자 기준 표준 DATE 타입 컬럼 생성 |
| **데이터 검증 및 예외 처리** | `RAISE invalid_month`, `RAISE empty_source` 등 절차적 예외 발생 | `isl_patient_progress_brands.yml`에 `not_null` 테스트 슈트 구성 | 데이터 적재와 품질 검증의 분리(Separation of Concerns), CI/CD 자동화 지원 |

---

## 3. 파일별 구현 상세

### 3.1. dbt SQL 모델: `com-azk-it-snowflake/models/silver/isl_patient_progress_brands.sql`

#### 구조 및 CTE 흐름
1. **`src_synced`**: 두 Bronze 원천 테이블의 `_fivetran_synced` 최신값 중 가장 큰 값(`GREATEST`)을 단일 행으로 추출
2. **`bronze`**: `patient_progress_brands_tagrisso`와 `patient_progress_brands_enhertu`를 `OBJECT_CONSTRUCT_KEEP_NULL`을 통해 Variant 객체로 결합 (`UNION ALL`)
3. **`parsed`**: `GET_IGNORE_CASE`를 사용하여 대소문자 구분 없이 필드 추출 및 적절한 데이터 타입(`VARCHAR`, `DECIMAL`, `INTEGER`)으로 캐스팅
4. **`transformed`**: 연도(2자리/4자리) 및 월 텍스트(`Jan`, `January` 등)를 파싱하여 `base_month` 생성, 시스템 컬럼(`_load_dtm`) 생성, `brand_name IS NOT NULL` 조건 적용
5. **최종 `SELECT`**: `src_synced`와 `CROSS JOIN`하여 원본 프로시저의 목적 테이블 스키마 15개 컬럼을 완벽하게 투영

```sql
{{ config(
    materialized='table'
) }}

WITH src_synced AS (
    SELECT GREATEST(
        (SELECT MAX(b."_fivetran_synced"::TIMESTAMP_NTZ(6)) FROM {{ source('bronze', 'patient_progress_brands_tagrisso') }} AS b),
        (SELECT MAX(b."_fivetran_synced"::TIMESTAMP_NTZ(6)) FROM {{ source('bronze', 'patient_progress_brands_enhertu') }} AS b)
    ) AS _src_synced
),

bronze AS (
    SELECT OBJECT_CONSTRUCT_KEEP_NULL(b.*) AS r
    FROM {{ source('bronze', 'patient_progress_brands_tagrisso') }} AS b
    UNION ALL
    SELECT OBJECT_CONSTRUCT_KEEP_NULL(b.*) AS r
    FROM {{ source('bronze', 'patient_progress_brands_enhertu') }} AS b
),

parsed AS (
    SELECT
        GET_IGNORE_CASE(r, 'brand')::VARCHAR                                AS brand_name,
        GET_IGNORE_CASE(r, 'hca')::VARCHAR                                  AS hca,
        GET_IGNORE_CASE(r, 'tier')::VARCHAR                                 AS tier,
        GET_IGNORE_CASE(r, 'indication')::VARCHAR                           AS indication,
        GET_IGNORE_CASE(r, 'sub_indication')::VARCHAR                       AS sub_indication,
        TRY_TO_DECIMAL(GET_IGNORE_CASE(r, 'new_pts')::VARCHAR, 18, 2)       AS new_pts,
        TRUNC(TRY_TO_DOUBLE(GET_IGNORE_CASE(r, 'year')::VARCHAR))::INTEGER  AS year,
        GET_IGNORE_CASE(r, 'month')::VARCHAR                                AS month,
        GET_IGNORE_CASE(r, 'qtr')::VARCHAR                                  AS qtr,
        GET_IGNORE_CASE(r, 'business_unit')::VARCHAR                        AS business_unit,
        GET_IGNORE_CASE(r, 'ta')::VARCHAR                                   AS therapy_area,
        GET_IGNORE_CASE(r, '#_file')::VARCHAR                                AS _file
    FROM bronze
),

transformed AS (
    SELECT
        -- year: 2자리(25) / 4자리(2025) 모두 허용, month: 대소문자·전체 철자(Jan/JAN/January) 허용
        DATE_FROM_PARTS(
            IFF(year < 100, 2000 + year, year),
            DECODE(UPPER(LEFT(TRIM(month), 3)),
                   'JAN',1,'FEB',2,'MAR',3,'APR',4,'MAY',5,'JUN',6,
                   'JUL',7,'AUG',8,'SEP',9,'OCT',10,'NOV',11,'DEC',12),
            1)                                                               AS base_month,
        brand_name,
        hca,
        tier,
        indication,
        sub_indication,
        new_pts,
        year,
        month,
        qtr,
        business_unit,
        therapy_area,
        _file,
        CONVERT_TIMEZONE('UTC', 'Asia/Seoul', SYSDATE())::TIMESTAMP_NTZ(6)   AS _load_dtm
    FROM parsed
    WHERE brand_name IS NOT NULL
)

SELECT
    t.base_month,
    t.brand_name,
    t.hca,
    t.tier,
    t.indication,
    t.sub_indication,
    t.new_pts,
    t.year,
    t.month,
    t.qtr,
    t.business_unit,
    t.therapy_area,
    t._file,
    t._load_dtm,
    s._src_synced
FROM transformed AS t
CROSS JOIN src_synced AS s
```

---

### 3.2. dbt 스키마 및 테스트: `com-azk-it-snowflake/models/silver/isl_patient_progress_brands.yml`

- **문서화**: 테이블 및 모든 컬럼에 대해 상세 국문 설명을 정의하여 데이터 카탈로그 및 dbt docs 가독성 확보
- **품질 검증 테스트**:
  - `base_month`: `not_null` (스토어드 프로시저의 `invalid_month` 예외 차단 로직 대응)
  - `brand_name`: `not_null` (필수 브랜드 식별값 검증)
  - `_load_dtm`, `_src_synced`: `not_null` (적재 메타데이터 무결성 검증)

```yaml
version: 2

models:
  - name: isl_patient_progress_brands
    description: "Tagrisso 및 Enhertu 브랜드 신환 진행 현황(Patient Progress) 통합 Silver 테이블"
    columns:
      - name: base_month
        description: "연도와 월을 파싱하여 생성된 기준월 (해당 월의 1일자)"
        tests:
          - not_null
      - name: brand_name
        description: "의약품 브랜드명 (Enhertu, Tagrisso 등)"
        tests:
          - not_null
      - name: hca
        description: "병원명 (Healthcare Agency / Hospital)"
      - name: tier
        description: "병원 티어 등급 (Top5, Top10, Top20, Top30, Others 등)"
      - name: indication
        description: "적응증"
      - name: sub_indication
        description: "세부 적응증"
      - name: new_pts
        description: "신환 수 (New Patients)"
      - name: year
        description: "원천 연도 값 (2자리 또는 4자리)"
      - name: month
        description: "원천 월 텍스트 (영문 약어 등)"
      - name: qtr
        description: "분기 (Quarter)"
      - name: business_unit
        description: "사업부 (예: Oncology)"
      - name: therapy_area
        description: "치료 영역 (Therapy Area)"
      - name: _file
        description: "원천 파일명"
      - name: _load_dtm
        description: "Silver 테이블 적재 일시 (KST/Asia/Seoul 기준)"
        tests:
          - not_null
      - name: _src_synced
        description: "참조된 Bronze 원천 소스들의 최신 _fivetran_synced 일시"
        tests:
          - not_null
```

---

## 4. 연계 및 후속 작업 (Next Steps)

- **Gold 레이어 연계 (`task_3.md`)**:
  - 후속 작업인 `task_3.md`의 Gold 모델(`fact_patient_progres_brands`) 작성 시, `{{ ref('isl_patient_progress_brands') }}`를 통해 Silver 모델을 직접 참조할 수 있습니다.
- **CI/CD 파이프라인**:
  - GitHub Actions 워크플로우(`.github/workflows/incoming_pr.yml`)에서 `snow dbt execute` 빌드 시 자동으로 본 Silver 모델 빌드 및 테스트(`not_null`)가 실행됩니다.
