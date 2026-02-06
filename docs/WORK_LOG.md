# Dagster ETL 프로젝트 작업 이력

## 개요
이 문서는 프로젝트의 모든 작업 계획 및 이력을 기록합니다.

---

## 작업 이력: 파이프라인 구조 변경 (2026-02-06)

### 변경 내용
기존 `extract → transform → load` 3단계를 새로운 파이프라인 기반 구조로 변경.

**새 파이프라인 흐름:**
- `extract` (input_load): RDB에서 추출 → S3 Parquet 저장 (항상)
- `transfer`: 입력 데이터 가공 (선택, `has_transfer=True`)
- `load` (output_save): Trino에 적재 (선택, `save_to_trino=True`)

**핵심 변경 사항:**
1. `PipelineAssetConfig` Pydantic 모델 추가 - 파이프라인별 설정 (파티션/비파티션, transfer 여부, Trino 적재 여부)
2. `AssetFactory` 전면 재작성 - PipelineAssetConfig 기반 동적 Asset 생성
3. 비파티션 데이터 지원 (`date_column=None` → `partitions_def=None`, S3 경로 `latest/`)
4. `JobFactory` 업데이트 - 파티션/비파티션 Job 분리 (`daily_etl_job` + `master_sync_job`)
5. `transform.py` → `transfer.py` 리네임 (공통, 테넌트, 템플릿 모두)
6. 테넌트 YAML에 `assets.pipelines` 설정 추가

**CFG_ITEM_MASTER 샘플 (project_01):**
- 비파티션 마스터 데이터 (파티션 없음)
- extract → load (transfer 없음, 직접 Trino 적재)
- `project_01/extract/cfg_item_master` → `project_01/load/cfg_item_master`

**검증 결과:**
- project_01: 11 assets, 5 jobs, 1 schedule
- default: 9 assets, 4 jobs, 1 schedule

---

## 현재 프로젝트 상태 (2026-02-06 기준)

### 아키텍처 개요

```
etl/
├── __init__.py              # Dagster Definitions 빌드 함수
├── project_01.py            # Project 01 테넌트 Code Location
├── default.py               # Default 테넌트 Code Location
├── all.py                   # 전체 통합 Code Location
├── common/                  # 공용 코드 (Single Source of Truth)
│   └── assets/
│       ├── extract.py       # 기본 Extract(Input Load) 쿼리/설정
│       ├── transfer.py      # 기본 Transfer 로직
│       └── load.py          # 기본 Load(Output Save) 설정
├── assets/                  # 레거시 호환성 레이어
│   ├── extract.py           # → common 사용
│   ├── transform.py         # → common 사용
│   └── load.py              # → common 사용
├── tenants/                 # 멀티테넌트 지원
│   ├── _template/           # 신규 테넌트 템플릿
│   │   ├── __init__.py
│   │   ├── config.yaml
│   │   └── assets/
│   │       └── transfer.py
│   ├── default/             # 기본 테넌트 (common만 사용)
│   └── project_01/          # Project 01 테넌트 (커스텀 Transform)
├── config/                  # 설정 관리
│   ├── tenant_config.py     # Pydantic 설정 모델
│   ├── config_loader.py     # YAML 설정 로더
│   └── tenant_loader.py     # 테넌트별 코드 로더
├── factories/               # 동적 생성 팩토리
│   ├── asset_factory.py     # Asset 동적 생성
│   ├── job_factory.py       # Job 동적 생성
│   ├── schedule_factory.py  # Schedule 동적 생성
│   └── resource_factory.py  # Resource 동적 생성
├── resources/               # Dagster Resources
├── jobs/                    # Job 정의
├── partitions/              # Partition 정의
└── utils/                   # 유틸리티
```

### 구현 완료된 모듈

#### Resources
- [x] `etl/resources/rdb.py` - Source RDB 연결 Resource
- [x] `etl/resources/trino.py` - Trino 연결 Resource
- [x] `etl/resources/s3.py` - S3 스토리지 Resource

#### Common Assets (Single Source of Truth)
- [x] `etl/common/assets/extract.py` - 기본 Extract 쿼리/설정
- [x] `etl/common/assets/transform.py` - 기본 Transform 로직
- [x] `etl/common/assets/load.py` - 기본 Load 설정

#### Legacy Assets (호환성 레이어)
- [x] `etl/assets/extract.py` - common 사용으로 리팩토링
- [x] `etl/assets/transform.py` - common 사용으로 리팩토링
- [x] `etl/assets/load.py` - common 사용으로 리팩토링

#### Partitions
- [x] `etl/partitions/daily.py` - Daily Partition 정의

#### Utils
- [x] `etl/utils/parquet.py` - Parquet 저장/로드 유틸
- [x] `etl/utils/validation.py` - 데이터 검증 유틸
- [x] `etl/utils/logging.py` - 로깅 유틸

#### Jobs
- [x] `etl/jobs/daily_job.py` - Daily Job 정의

#### Config & Factories (Multi-tenant 지원)
- [x] `etl/config/tenant_config.py` - Tenant 설정 Pydantic 모델
- [x] `etl/config/tenant_loader.py` - 테넌트별 커스텀 코드 로더
- [x] `etl/config/config_loader.py` - YAML 설정 로더
- [x] `etl/factories/asset_factory.py` - Asset 동적 생성
- [x] `etl/factories/job_factory.py` - Job 동적 생성
- [x] `etl/factories/schedule_factory.py` - Schedule 동적 생성
- [x] `etl/factories/resource_factory.py` - Resource 동적 생성

#### Tenants
- [x] `etl/tenants/_template/` - 신규 테넌트 생성 템플릿
- [x] `etl/tenants/default/` - 기본 테넌트 (공용 코드만 사용)
- [x] `etl/tenants/project_01/` - Project 01 테넌트 (커스텀 Transform 예시)

---

## 작업 이력

### 2026-02-05

#### 테넌트 명칭 및 모듈 경로 개선
- **시간**: 18:00 ~ 18:15
- **목적**: 테넌트 명칭 정리 및 모듈 import 경로 단순화
- **작업 내용**:
  1. **acme → project_01 명칭 변경**
     - `etl/tenants/acme/` → `etl/tenants/project_01/`
     - 모든 설정 파일 내 참조 업데이트
     - 환경변수 접두사 변경 (ACME_* → PROJECT_01_*)
  2. **모듈 경로 단순화**
     - 기존: `etl.definitions.acme`, `etl.definitions.default`
     - 변경: `etl.project_01`, `etl.default`
     - `etl/definitions/` 폴더 삭제
     - `etl/project_01.py`, `etl/default.py`, `etl/all.py` 생성
  3. **설정 파일 업데이트**
     - `.vscode/launch.json` - 새 모듈 경로 반영
     - `workspace.yaml` - 새 모듈 경로 반영
     - `.env` - PROJECT_01_* 환경변수
  4. **레거시 파일 정리**
     - `etl/definitions/` 폴더 삭제
     - `tenant_acme.yaml` 삭제 (폴더 기반 config.yaml 사용)
- **관련 파일**:
  - `etl/project_01.py` (신규)
  - `etl/default.py` (이동)
  - `etl/all.py` (이동)
  - `etl/__init__.py` (수정)
  - `etl/tenants/project_01/` (명칭 변경)
  - `.vscode/launch.json`
  - `workspace.yaml`
  - `.env`
- **상태**: 완료
- **실행 방법**:
  - 테넌트 분리: `dagster dev -m etl.project_01 -m etl.default -p 3000`
  - 통합 모드: `dagster dev -m etl.all -p 3000`

#### Common/Tenant 분리 구조 개선
- **시간**: 17:00 ~ 17:30
- **목적**: ETL 코드를 common 영역과 테넌트별 영역으로 명확히 분리
- **작업 내용**:
  1. **레거시 코드 정리**
     - `etl/assets/` 모듈을 common 사용하는 호환성 레이어로 변환
     - 중복 로직 제거, common 모듈 import로 대체
  2. **테넌트 템플릿 완성**
     - `_template/assets/` 폴더 구조 추가
     - 커스텀 Transform 예시 코드 추가
     - `__init__.py` 문서화 강화
  3. **__init__.py 정비**
     - `etl/common/__init__.py` export 정리
     - `etl/common/assets/__init__.py` 문서화
     - `etl/assets/__init__.py` 호환성 레이어 명시
- **관련 파일**:
  - `etl/assets/__init__.py`
  - `etl/assets/extract.py`
  - `etl/assets/transform.py`
  - `etl/assets/load.py`
  - `etl/common/__init__.py`
  - `etl/common/assets/__init__.py`
  - `etl/tenants/_template/__init__.py`
  - `etl/tenants/_template/assets/__init__.py`
  - `etl/tenants/_template/assets/transform.py`
- **상태**: 완료
- **이슈/참고사항**:
  - 레거시 Assets는 단일 테넌트 모드 호환성을 위해 유지
  - 새로운 로직 추가 시 반드시 `common/assets/`에 먼저 추가
  - 테넌트별 커스텀은 `tenants/{tenant_id}/assets/`에 추가

#### 테넌트별 Code Location 분리
- **시간**: 17:40 ~ 17:50
- **목적**: Dagster UI에서 Jobs/Assets를 테넌트별 폴더로 분리 표시
- **작업 내용**:
  1. **etl/__init__.py 리팩토링**
     - `build_tenant_definitions(tenant_id)` - 단일 테넌트 빌드 함수 추가
     - `build_all_definitions()` - 전체 테넌트 통합 빌드 함수 분리
  2. **Code Location 모듈 생성**
     - `etl/project_01.py` - Project 01 테넌트 Code Location
     - `etl/default.py` - Default 테넌트 Code Location
     - `etl/all.py` - 전체 통합 Code Location
  3. **launch.json 업데이트**
     - "Dagster Dev (테넌트 분리)" - 테넌트별 폴더 분리 모드
     - "Dagster Dev (통합)" - 기존 단일 폴더 모드
- **관련 파일**:
  - `etl/__init__.py`
  - `etl/project_01.py`
  - `etl/default.py`
  - `etl/all.py`
  - `.vscode/launch.json`
  - `workspace.yaml`
- **상태**: 완료

#### 실행 및 디버그 환경 설정
- **시간**: 17:30 ~ 17:40
- **목적**: VSCode에서 Dagster 실행 및 디버그 가능하도록 환경 구성
- **작업 내용**:
  1. **환경변수 설정**
     - `.env` 파일 생성 (로컬 개발용)
     - 환경변수 자동 로드 확인
  2. **VSCode 설정**
     - `.vscode/launch.json` - 디버그 설정
     - `.vscode/settings.json` - Python, 포맷팅, 테스트 설정
  3. **검증 완료**
     - Dagster 모듈 로드 테스트 성공
     - `dagster definitions validate` 성공
     - `dagster dev` 서버 시작 성공
- **관련 파일**:
  - `.env` (gitignore에 포함됨)
  - `.vscode/launch.json`
  - `.vscode/settings.json`
  - `.gitignore` 수정 (.vscode/ 공유 설정 허용)
- **상태**: 완료

#### 작업 관리 체계 수립
- **시간**: 17:00
- **작업 내용**: 작업 이력 관리를 위한 문서 구조 생성
- **산출물**: `docs/WORK_LOG.md` 생성
- **상태**: 완료

---

## 향후 작업 (Backlog)

| 우선순위 | 작업 | 상태 | 비고 |
|---------|------|------|------|
| 높음 | 테스트 코드 작성 | 대기 | common/assets 단위 테스트 |
| 중간 | CI/CD 파이프라인 구축 | 대기 | GitHub Actions |
| 중간 | 운영 문서화 | 대기 | 신규 테넌트 온보딩 가이드 |

---

## 개발 가이드

### 실행 방법

#### VSCode에서 실행 (권장)
1. F5 키 또는 "Run and Debug" (Ctrl+Shift+D)
2. 모드 선택:
   - **"Dagster Dev (테넌트 분리)"** - UI에서 테넌트별 폴더 분리
   - **"Dagster Dev (통합)"** - 모든 테넌트 하나의 폴더
3. http://localhost:3000 접속

#### 터미널에서 실행
```bash
# 가상환경 활성화
.venv\Scripts\activate  # Windows
source .venv/bin/activate  # Linux/Mac

# 테넌트별 분리 모드 (권장)
dagster dev -m etl.project_01 -m etl.default -p 3000

# 통합 모드
dagster dev -m etl.all -p 3000
```

#### 디버그 설정 (launch.json)
| 설정 | 설명 |
|-----|------|
| Dagster Dev (테넌트 분리) | 테넌트별 Code Location 분리 (포트 3000) |
| Dagster Dev (통합) | 모든 테넌트 통합 (포트 3000) |
| Dagster Dev (Port 3001) | 대체 포트 사용 |
| Run Single Asset | 특정 Asset 실행 |
| Python: Current File | 현재 파일 실행 |
| Pytest: All Tests | 전체 테스트 실행 |
| Pytest: Current File | 현재 테스트 파일 실행 |

### 신규 테넌트 추가 방법

1. `_template` 폴더 복사
   ```bash
   cp -r etl/tenants/_template etl/tenants/{new_tenant_id}
   ```

2. `__init__.py` 수정
   - `TENANT_ID`, `TENANT_NAME` 변경
   - 필요시 `CUSTOM_*` 변수 설정

3. `config.yaml` 수정
   - DB 연결 정보
   - 스케줄 설정
   - 태그 정보

4. Code Location 모듈 생성
   ```python
   # etl/{new_tenant_id}.py
   from etl import build_tenant_definitions
   defs = build_tenant_definitions("{new_tenant_id}")
   ```

5. (선택) 커스텀 로직 추가
   - `assets/transform.py`에 커스텀 함수 구현
   - `__init__.py`의 `CUSTOM_TRANSFORM_FUNCTIONS`에 등록

### 공용 로직 수정 시 주의사항

1. `etl/common/assets/`에서 로직 수정
2. 모든 테넌트에 영향을 미치므로 신중하게 검토
3. 하위 호환성 유지 필요

---

## 작업 템플릿

새 작업 기록 시 아래 템플릿을 사용합니다:

```markdown
#### [작업 제목]
- **시간**: YYYY-MM-DD HH:MM
- **목적**:
- **작업 내용**:
- **관련 파일**:
- **산출물**:
- **상태**: 진행중 / 완료 / 보류
- **이슈/참고사항**:
```
