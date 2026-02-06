# 신규 테넌트 추가

새로운 테넌트를 프로젝트에 추가합니다. `_template/` 디렉토리를 기반으로 테넌트 구조를 생성합니다.

## 사용법

```bash
/add_tenant <tenant_id>
```

## 실행 단계

1. **테넌트 ID 검증**:
   - 소문자 영숫자 + 언더스코어만 허용 (`^[a-z][a-z0-9_]*$`)
   - 기존 테넌트와 중복 확인 (`etl/tenants/` 디렉토리 검사)

2. **디렉토리 구조 생성**: `etl/tenants/_template/`을 복사하여 신규 테넌트 생성
   ```
   etl/tenants/{tenant_id}/
   ├── config.yaml          # 테넌트 설정 (YAML)
   ├── __init__.py           # 커스텀 코드 등록
   ├── assets/
   │   ├── __init__.py
   │   └── transfer.py       # 커스텀 transfer 함수
   └── dbt/                  # dbt 프로젝트 (선택)
       ├── dbt_project.yml
       ├── profiles.yml
       └── models/
   ```

3. **config.yaml 생성**: 템플릿의 `{TENANT_ID}` 플레이스홀더를 실제 값으로 치환
   - `tenant.id`: tenant_id
   - `tenant.name`: 사용자 입력 또는 기본값
   - `source_database`: 환경변수 패턴 설정 (`{TENANT_ID_UPPER}_DB_*`)
   - `storage.base_path`: `project_id={tenant_id}`

4. **Code Location 모듈 생성**: `etl/{tenant_id}.py`
   ```python
   from etl import build_tenant_definitions
   defs = build_tenant_definitions("{tenant_id}")
   ```

5. **`etl/__init__.py` 업데이트**: `get_{tenant_id}_definitions()` 함수 추가

6. **환경변수 추가**: `.env` 파일에 테넌트 DB 연결 정보 추가
   ```
   {TENANT_ID_UPPER}_DB_HOST=localhost
   {TENANT_ID_UPPER}_DB_PORT=5432
   {TENANT_ID_UPPER}_DB_NAME=
   {TENANT_ID_UPPER}_DB_USER=
   {TENANT_ID_UPPER}_DB_PASSWORD=
   ```

7. **VSCode launch.json 업데이트**: 테넌트 분리 실행 설정에 `-m etl.{tenant_id}` 추가

8. **검증**:
   ```bash
   dagster definitions validate -m etl.{tenant_id}
   ```

## 생성 후 안내

사용자에게 다음을 안내합니다:
- `.env`의 DB 연결 정보를 실제 값으로 수정
- `config.yaml`의 파이프라인 정의를 프로젝트 요구사항에 맞게 수정
- dbt 사용 시 `dbt/` 디렉토리에 모델 추가
- 커스텀 transfer 로직 필요 시 `assets/transfer.py` 구현
