# 파이프라인 추가

기존 테넌트에 새로운 ETL 파이프라인을 추가합니다.

## 사용법

```bash
/add_pipeline <tenant_id> <pipeline_name>
```

## 실행 단계

1. **테넌트 확인**: `etl/tenants/{tenant_id}/config.yaml` 존재 여부 확인

2. **파이프라인 이름 검증**:
   - 소문자 영숫자 + 언더스코어만 허용
   - 기존 파이프라인과 중복 확인

3. **사용자 입력 수집** (AskUserQuestion 사용):
   - 소스 테이블명
   - 날짜 컬럼 (파티셔닝용, 없으면 마스터 데이터)
   - Python transfer 필요 여부
   - dbt transform 필요 여부
   - S3 저장 여부
   - Trino 적재 여부 (적재 시 target_table, target_schema, key_columns)

4. **config.yaml 업데이트**: `assets.pipelines` 섹션에 새 파이프라인 추가
   ```yaml
   assets:
     pipelines:
       {pipeline_name}:
         source_table: {source_table}
         date_column: {date_column}  # null이면 생략
         has_transfer: {true/false}
         has_dbt_transform: {true/false}
         save_to_s3: {true/false}
         save_to_trino: {true/false}
         trino_output:  # save_to_trino가 true인 경우만
           target_table: {target_table}
           target_schema: {target_schema}
           key_columns: [{key_columns}]
   ```

5. **커스텀 transfer 추가** (has_transfer인 경우):
   - `etl/tenants/{tenant_id}/assets/transfer.py`에 함수 추가
   - `__init__.py`의 `CUSTOM_TRANSFER_FUNCTIONS`에 등록

6. **dbt 모델 추가** (has_dbt_transform인 경우):
   - `etl/tenants/{tenant_id}/dbt/models/staging/stg_{pipeline_name}.sql` 생성
   - `etl/tenants/{tenant_id}/dbt/models/staging/_staging__sources.yml` 업데이트
   - `etl/tenants/{tenant_id}/dbt/models/transform/tfm_{pipeline_name}.sql` 생성 (필요 시)

7. **검증**:
   ```bash
   dagster definitions validate -m etl.{tenant_id}
   ```

## 파이프라인 유형 가이드

### 마스터 데이터 (date_column 없음)
- 전체 테이블을 매번 추출
- 예: `cfg_item_master`, `cfg_process_master`

### 트랜잭션 데이터 (date_column 있음)
- 날짜 기반 파티셔닝
- 예: `lot_history` (date_column: `created_at`)

### Python transfer 파이프라인
- 복잡한 데이터 변환이 필요한 경우
- DataFrame 조인, 집계, 피벗 등

### dbt transform 파이프라인
- SQL 기반 변환이 적합한 경우
- staging → transform 패턴 사용
