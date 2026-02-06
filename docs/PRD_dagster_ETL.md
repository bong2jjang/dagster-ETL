# Dagster 기반 ETL 프로젝트 PRD 및 개발 설계

## 1. 프로젝트 개요 (개발 착수 기준)

### 1.1 목적
본 프로젝트는 Dagster 기반의 **운영 가능한 ETL 파이프라인**을 구축하는 것을 목표로 한다. 개발 착수 즉시 구현이 가능하도록 코드 구조, 실행 단위, 재처리 기준, 운영 포인트를 명확히 정의한다.

### 1.2 개발 산출물
- Dagster 프로젝트 코드 저장소
- RDB → Transform → Trino → PostgreSQL ETL 파이프라인
- S3 Parquet 기반 Input / Output 데이터 레이크
- 로컬/개발/운영 환경 분리 설정

### 1.3 개발 전제 조건
- Python 3.10 이상
- Dagster OSS 사용
- Docker 기반 실행 환경
- AWS S3 사용 (MinIO로 로컬 대체 가능)

---

## 2. 전체 아키텍처 (구현 기준)

### 2.1 처리 흐름
```
[Source RDB]
   │  (SQL Extract)
   ▼
[extract_asset]
   │  └─ s3://.../extract/
   ▼
[transform_asset]
   │  └─ s3://.../transform/
   ▼
[load_asset]
   │  (Trino INSERT)
   ▼
[Target PostgreSQL]
```

### 2.2 실행 단위 정의
- 최소 실행 단위: **Dagster Asset**
- 재처리 단위: Asset + Partition
- 전체 실행: Dagster Job

---

## 3. 기능 요구사항 (PRD)

### 3.1 데이터 소스 관리
- RDB 커넥션 정보 관리 (환경변수/Secret)
- 테이블/쿼리 단위 Extract 지원
- 증분/전체 로딩 옵션

### 3.2 ETL 파이프라인
- Dagster Asset 기반 파이프라인 구성
- Asset 간 의존성 명확화
- 실패 Asset 단위 재실행 지원

### 3.3 Transform 로직
- Python 코드 기반 데이터 가공
- 공통 유틸/비즈니스 로직 모듈화
- 데이터 검증 (Null, Type, Range)

### 3.4 데이터 적재 (Load)
- Trino를 통한 PostgreSQL Insert/Upsert
- Batch 단위 적재
- 스키마 사전 검증

### 3.5 Parquet & S3 저장
- Extract 결과 Parquet 저장
- Transform 결과 Parquet 저장
- 파티션 전략 (date, job_name)

### 3.6 운영 및 모니터링
- Dagster UI 기반 실행 상태 확인
- 로그/메트릭 수집
- 실패 알림 (Slack/Email – 선택)

---

## 4. 비기능 요구사항

| 항목 | 요구사항 |
|---|---|
| 성능 | 1M rows 기준 10분 이내 처리 |
| 확장성 | 데이터 소스 추가 시 Asset 추가로 대응 |
| 안정성 | 재시도 및 Idempotent 처리 |
| 보안 | DB/S3 Credential 외부 노출 금지 |
| 유지보수 | 코드 기반 파이프라인 관리 |

---

## 5. Dagster 상세 설계 (개발자 기준)

### 5.1 프로젝트 디렉토리 구조
```
etl_project/
 ├─ etl/
 │   ├─ assets/
 │   │   ├─ extract.py
 │   │   ├─ transform.py
 │   │   └─ load.py
 │   ├─ resources/
 │   │   ├─ rdb.py
 │   │   ├─ trino.py
 │   │   └─ s3.py
 │   ├─ jobs/
 │   │   └─ daily_job.py
 │   ├─ partitions/
 │   │   └─ daily.py
 │   └─ utils/
 │       ├─ parquet.py
 │       ├─ validation.py
 │       └─ logging.py
 ├─ docker/
 ├─ pyproject.toml
 └─ dagster.yaml
```

### 5.2 Asset 상세 정의

#### extract_asset
- 입력: 없음 (Partition = 날짜)
- 처리:
  - Source RDB SQL 실행
  - Pandas DataFrame 생성
  - S3 Parquet 저장
- 출력 메타데이터:
  - row_count
  - parquet_path

#### transform_asset
- 입력: extract_asset
- 처리:
  - Parquet 로드
  - 비즈니스 로직 처리
  - 데이터 검증
  - 결과 Parquet 저장

#### load_asset
- 입력: transform_asset
- 처리:
  - Trino SQL INSERT 실행
  - Target PostgreSQL 적재

### 5.3 Partition 전략
- Daily Partition (YYYY-MM-DD)
- 모든 Asset 동일 Partition 사용

---

## 6. 데이터 저장 설계

### 6.1 S3 디렉토리 구조 예시
```
s3://data-lake/
 ├─ extract/
 │   └─ job_name=xxx/date=YYYYMMDD/*.parquet
 ├─ transform/
 │   └─ job_name=xxx/date=YYYYMMDD/*.parquet
```

### 6.2 Parquet Schema 관리
- 명시적 Schema 정의
- Schema Drift 감지 로직

---

## 7. Trino 연계 설계

- Trino Catalog
  - Source RDB Catalog
  - Target PostgreSQL Catalog

- Insert 방식
  - `INSERT INTO target_table SELECT ...`

- 성능 고려
  - Batch Size 조절
  - Worker 확장

---

## 8. 오류 처리 및 재처리 전략 (운영 기준)

### 8.1 오류 유형
| 유형 | 대응 |
|---|---|
| Extract 실패 | 재실행 가능 |
| Transform 오류 | 데이터 검증 로그 후 중단 |
| Load 실패 | Trino SQL 재실행 |

### 8.2 재처리 원칙
- Extract Parquet은 **불변(Immutable)**
- Transform / Load는 overwrite 허용
- Dagster UI에서 Partition 단위 재실행

---

## 9. 개발 WBS (착수 기준)

### Week 1: 환경 및 기본 구조
- Dagster 프로젝트 생성
- Docker / 로컬 실행 환경 구성
- S3 / RDB / Trino Resource 구현

### Week 2: Extract / Transform 구현
- RDB Extract Asset 개발
- Parquet 저장 유틸 구현
- 기본 Transform 로직 적용

### Week 3: Load / 운영 기능
- Trino Insert 로직 구현
- PostgreSQL 적재 검증
- Dagster Job / Schedule 구성

### Week 4: 안정화
- 실패/재처리 시나리오 테스트
- 로그/메타데이터 정리
- 운영 문서 정리

---|---|
| 설계 | 1주 |
| ETL 기본 구조 | 2주 |
| Trino 연계 | 1주 |
| 운영/모니터링 | 1주 |

---

## 10. APS · 제조 데이터용 ETL 시나리오 예제

본 ETL 파이프라인은 APS(Advanced Planning & Scheduling) 및 제조 데이터 환경에 바로 적용 가능하도록 설계된다. 아래는 **실제 제조 현장에서 바로 쓰이는 대표 시나리오** 기준 예제이다.

---

### 10.1 시나리오 1: 공정 실적 수집 → 계획 입력 데이터 생성

#### 목적
- MES/POP 시스템의 공정 실적 데이터를 수집하여
- APS 계획 엔진의 **입력 데이터 스냅샷**을 생성

#### Source (RDB)
- `lot_history`
- `equipment_event`
- `process_result`

#### Extract
- 기준일(D-1) 완료 Lot 실적 조회
- 공정 단계별 투입/완료 수량 집계
- S3 저장
```
s3://data-lake/extract/aps_input/date=YYYY-MM-DD/
```

#### Transform
- 공정별 WIP 계산
- Lead Time 재산출
- 비정상 Lot 필터링 (Hold / Scrap)
- APS Input Schema로 정규화

#### Load
- Trino를 통해 APS 운영 DB 적재
- 테이블 예:
  - `aps_input_wip`
  - `aps_input_cycle_time`

---

### 10.2 시나리오 2: 생산 계획 결과 이력 관리 (Plan Snapshot)

#### 목적
- APS 엔진 실행 결과를 **변경 불가능한 계획 스냅샷**으로 보존
- 계획 변경 비교 및 Audit 대응

#### Source
- APS 결과 테이블
  - `plan_production`
  - `plan_equipment`

#### Extract
- 계획 실행 ID 기준 결과 조회
- 전체 결과를 그대로 Parquet 저장
```
s3://data-lake/extract/aps_plan/run_id=XXXX/
```

#### Transform
- 최소 정합성 검증만 수행
- 컬럼 타입/범위 체크

#### Load
- PostgreSQL (운영 DB) 적재
- 동시에 S3에 **Immutable Plan Snapshot** 유지

---

### 10.3 시나리오 3: 계획 vs 실적 비교 데이터 생성

#### 목적
- 계획 정확도 평가
- 병목 공정/설비 식별

#### Transform 중심 시나리오
- Plan Snapshot Parquet 로드
- 실제 실적 Parquet 로드
- 공정/설비/일자 기준 Join

#### 산출 데이터
- 계획 대비 실적 편차
- 설비 가동률 차이
- 공정 병목 구간

```
s3://data-lake/transform/aps_kpi/date=YYYY-MM-DD/
```

#### 활용
- APS 튜닝 입력
- 경영 KPI 리포트
- AI 학습 데이터

---

### 10.4 시나리오 4: EVEN / 병렬 공정 밸런스 분석

#### 목적
- 특정 공정 쏠림 생산 방지
- 공정 간 WIP 불균형 사전 감지

#### Transform 로직 예
- 공정별 생산량 / 처리능력 집계
- 병렬 설비 그룹 기준 생산 편차 계산
- Threshold 초과 시 Flag 생성

#### Output
- APS 보정 입력 데이터
- 공정 밸런스 모니터링 데이터

---

### 10.5 APS ETL 설계 핵심 포인트

| 항목 | 설계 원칙 |
|---|---|
| 계획 데이터 | Immutable Snapshot 유지 |
| 실적 데이터 | 일자 단위 Partition |
| 재처리 | Parquet 기준 재생성 |
| 추적성 | Plan Run ID / 기준일 메타데이터 |

---

## 11. 성공 기준 (KPI)


- ETL 성공률 99% 이상
- 재처리 소요 시간 50% 이상 단축
- 신규 데이터 소스 추가 시 1일 이내 적용

