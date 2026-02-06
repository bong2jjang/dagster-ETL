# Dagster ETL 프로젝트 가이드

## 프로젝트 개요

Dagster 기반 멀티테넌트 ETL 파이프라인 프로젝트.
RDB(PostgreSQL)에서 데이터를 추출하여 S3(Parquet)에 저장하고, dbt로 변환하며, Trino(Iceberg)에 적재합니다.

## 프로젝트 구조

```
etl/
├── __init__.py              # build_tenant_definitions(), get_all_definitions()
├── all.py                   # 통합 Code Location (모든 테넌트)
├── project_01.py            # Project 01 Code Location
├── project_02.py            # Project 02 Code Location
├── default.py               # Default Code Location
├── config/                  # 설정 관리
│   ├── tenant_config.py     # Pydantic 기반 TenantConfig 스키마
│   ├── config_loader.py     # YAML → TenantConfig 로드
│   └── tenant_loader.py     # 테넌트별 커스텀 코드 로더
├── resources/               # Dagster Resources
│   ├── rdb.py               # SQLAlchemy RDB 연결 (RDBResource)
│   ├── trino.py             # Trino 연결 (TrinoResource)
│   └── s3.py                # S3/MinIO 연결 (S3Resource)
├── factories/               # 팩토리 패턴 (핵심)
│   ├── asset_factory.py     # PipelineAssetConfig → Dagster Assets
│   ├── job_factory.py       # 테넌트별 Job 생성
│   ├── schedule_factory.py  # 테넌트별 Schedule 생성
│   ├── resource_factory.py  # 테넌트별 Resource 생성
│   └── dbt_factory.py       # dbt Assets/Resource 생성
├── common/assets/           # 재사용 공통 Asset 함수
│   ├── extract.py           # RDB → DataFrame 추출
│   ├── transfer.py          # DataFrame 변환 (Python)
│   └── load.py              # S3/Trino 적재
├── tenants/                 # 멀티테넌트 설정 및 코드
│   ├── _template/           # 신규 테넌트 템플릿
│   ├── project_01/          # Project 01 (config.yaml, assets/, dbt/)
│   ├── project_02/          # Project 02
│   └── default/             # Default 테넌트
├── utils/                   # 유틸리티
│   ├── parquet.py           # Parquet 변환
│   ├── validation.py        # 데이터 검증
│   └── logging.py           # 구조화 로깅
├── jobs/                    # Job 정의
└── partitions/              # Partition 정의
```

## 핵심 아키텍처

### 멀티테넌트 팩토리 패턴

모든 테넌트는 동일한 팩토리를 통해 Dagster 리소스를 생성합니다:

```python
# etl/__init__.py
def build_tenant_definitions(tenant_id: str) -> Definitions:
    config = ConfigLoader.load_tenant(tenant_id)
    assets = AssetFactory(config).create_all_etl_assets()
    jobs = JobFactory(config).create_all_jobs()
    schedules = ScheduleFactory(config).create_all_schedules()
    resources = ResourceFactory(config).create_all_resources()
    return Definitions(assets=assets, jobs=jobs, schedules=schedules, resources=resources)
```

### Asset Key 네이밍 규칙

```
[tenant_id]/[stage]/[pipeline_name]
예: project_01/extract/lot_history
    project_01/transfer/lot_history
    project_01/load/lot_history
    project_01/dbt/stg_cfg_item_master
```

### 파이프라인 설정 (PipelineAssetConfig)

각 파이프라인은 `config.yaml`에서 선언적으로 정의됩니다:

```yaml
assets:
  pipelines:
    lot_history:
      source_table: lot_history
      date_column: created_at
      has_transfer: true       # Python transfer
      has_dbt_transform: false # dbt transform
      save_to_s3: true
      save_to_trino: true
      trino_output:
        target_table: aps_input_wip
        target_schema: aps
```

### dbt 통합

테넌트별 dbt 프로젝트가 `etl/tenants/{tenant_id}/dbt/`에 위치합니다:
- `TenantDbtTranslator`: dbt 모델 → Dagster Asset Key 매핑
- dbt source → `[tenant_id, "extract", source_name]` (extract asset에 의존성 연결)
- dbt model → `[tenant_id, "dbt", model_name]`

---

## 코딩 컨벤션

### Python 버전

- **Python 3.10 이상** 타겟
- 빌트인 제네릭 타입 사용: `list[str]`, `dict[str, Any]`, `X | Y` 유니온

### 타입 어노테이션

- 모든 함수에 타입 힌트 필수
- `typing.Dict`, `typing.List`, `typing.Set`, `typing.Tuple` **사용 금지** → 빌트인 타입 사용
- `dict[str, Any]`, `list[str]`, `set[int]`, `tuple[str, ...]` 형태 사용

```python
# 올바른 예시
def process_data(items: list[str], config: dict[str, Any]) -> tuple[int, str]:
    ...

# 잘못된 예시
from typing import Dict, List
def process_data(items: List[str], config: Dict[str, Any]):
    ...
```

### Import 규칙

- **항상 모듈 최상위에서 import** (함수 내부 import 지양)
- 허용되는 예외: TYPE_CHECKING 블록, 순환 import 해결, 선택적 의존성
- isort 규칙 준수 (ruff가 자동 정렬)

### 데이터 모델

- **Pydantic BaseModel** 사용 (설정, DTO, 결과 객체 등)
- `Field(default_factory=...)` 패턴으로 가변 기본값 처리
- `model_dump()` / `model_validate()` 활용

```python
from pydantic import BaseModel, Field

class PipelineResult(BaseModel):
    """파이프라인 실행 결과"""
    row_count: int
    columns: list[str] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
```

### 예외 처리

- 예외를 제어 흐름으로 사용 **금지**
- 예외를 묵시적으로 삼키지 않기 (`except: pass` 금지)
- 가능하면 조건 검사를 먼저 수행 (proactive checking)
- 필요시 의미 있는 컨텍스트를 추가하여 re-raise

```python
# 올바른 예시: 조건 검사 우선
if asset_key in metadata:
    value = metadata[asset_key]
else:
    handle_missing()

# 잘못된 예시: 예외를 제어 흐름으로 사용
try:
    value = metadata[asset_key]
except KeyError:
    handle_missing()
```

### Context Manager

- 중간 변수 할당 없이 `with` 문에서 직접 사용

```python
# 올바른 예시
with build_context(sensor_name="test") as ctx:
    ctx.do_something()

# 잘못된 예시
ctx = build_context(sensor_name="test")
with ctx:
    ctx.do_something()
```

### Dagster 리소스

- Dagster Resource는 `ConfigurableResource` 상속하여 정의
- `@asset`, `@multi_asset` 데코레이터 사용
- `key_prefix=[tenant_id, stage]` 패턴으로 Asset Key 구성

### 로깅

- `print()` 사용 금지
- Dagster context logger 사용: `context.log.info(...)`
- 유틸리티에서는 `get_dagster_logger()` 사용

---

## 개발 워크플로우

### Ruff 포매팅 및 린트

**중요**: Python 파일 수정 후 반드시 ruff를 실행합니다.

```bash
# 포매팅 + 린트
ruff check --fix .
ruff format .

# 또는 개별 파일
ruff check --fix path/to/file.py
ruff format path/to/file.py
```

ruff가 자동으로 처리하지 못하는 문제는 수동으로 수정합니다:
- 줄 길이 초과 (88자)
- 사용하지 않는 import/변수
- 타입 힌트 누락

### 테스트

```bash
# 전체 테스트
pytest tests/ -v

# 특정 파일
pytest tests/test_specific.py -v

# 커버리지 포함
pytest tests/ -v --cov=etl --cov-report=term-missing
```

### Dagster 로컬 개발

```bash
# 테넌트 분리 실행 (권장)
dagster dev -m etl.project_01 -m etl.project_02 -m etl.default -p 3000

# 통합 실행
dagster dev -m etl.all -p 3000
```

### dbt 관련

```bash
# dbt 프로젝트 검증
cd etl/tenants/project_01/dbt
dbt debug --profiles-dir .
dbt parse --profiles-dir .

# Dagster definitions 검증
dagster definitions validate -m etl.project_01
```

### Git 커밋

- 의미 있는 단위로 커밋
- 시크릿 파일(.env, credentials) 커밋 금지
- Co-Authored-By 헤더 포함 (Claude 작업 시)

---

## 환경 변수 규칙

테넌트별 환경변수는 다음 패턴을 따릅니다:

```
{TENANT_ID}_DB_HOST      # 대문자 테넌트 ID
{TENANT_ID}_DB_PORT
{TENANT_ID}_DB_NAME
{TENANT_ID}_DB_USER
{TENANT_ID}_DB_PASSWORD
```

예: `PROJECT_01_DB_HOST`, `PROJECT_02_DB_PORT`

공통 환경변수:
- `DAGSTER_ENVIRONMENT` (dev/staging/prod)
- `DAGSTER_HOME` (Dagster 홈 디렉토리)
- `TRINO_HOST`, `TRINO_PORT`, `TRINO_USER`, `TRINO_CATALOG`
- `S3_ENDPOINT_URL`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `S3_BUCKET_NAME`

---

## 작업 이력 관리

모든 작업 이력은 `docs/WORK_LOG_{YYYYMMDD}.md`에 기록합니다:
- 작업 시작 전 계획 기록
- 작업 완료 후 결과 기록
- 이슈 발생 시 참고사항 기록
