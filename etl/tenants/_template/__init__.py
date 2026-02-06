"""
Template Tenant - 새 테넌트 생성 템플릿

이 폴더를 복사하여 새 테넌트를 생성합니다.

사용법:
1. 이 폴더를 tenants/{new_tenant_id}/ 로 복사
2. 이 파일에서 TENANT_ID, TENANT_NAME 수정
3. config.yaml 수정 (DB 연결정보, 스케줄 등)
4. 필요시 assets/ 폴더에 커스텀 로직 추가

구조:
    tenants/{tenant_id}/
    ├── __init__.py          # 테넌트 정의 및 커스텀 설정
    ├── config.yaml          # 테넌트 설정 (DB, Storage, Jobs)
    └── assets/              # 커스텀 Asset 로직 (선택)
        ├── __init__.py
        └── transform.py     # 커스텀 Transform 로직

커스텀 코드 우선순위:
1. tenants/{tenant_id}/assets/ 에 정의된 로직
2. common/assets/ 에 정의된 공용 로직
"""

TENANT_ID = "template"  # 변경 필요: 소문자 영숫자, 언더스코어만 사용
TENANT_NAME = "Template Tenant"  # 변경 필요: 표시용 이름


# =============================================================================
# 커스텀 Extract 쿼리
# =============================================================================
# 특정 테이블만 오버라이드 가능, None이면 common 사용
CUSTOM_EXTRACT_QUERIES = None

# 예시: 특정 테이블의 쿼리 오버라이드
# CUSTOM_EXTRACT_QUERIES = {
#     "lot_history": """
#         SELECT
#             lot_id,
#             product_code,
#             process_step,
#             equipment_id,
#             start_time,
#             end_time,
#             quantity,
#             status,
#             custom_field,  -- 테넌트 고유 필드
#             created_at
#         FROM lot_history
#     """,
# }


# =============================================================================
# 커스텀 Transform 함수
# =============================================================================
# None이면 common 사용, 함수를 지정하면 해당 함수 사용
CUSTOM_TRANSFORM_FUNCTIONS = None

# 예시: 커스텀 Transform 함수 등록
# from etl.tenants.{tenant_id}.assets.transform import (
#     transform_aps_wip_logic as custom_wip_logic,
# )
#
# CUSTOM_TRANSFORM_FUNCTIONS = {
#     "aps_wip": custom_wip_logic,  # 커스텀 함수 사용
#     "cycle_time": None,           # common 사용
#     "equipment_utilization": None, # common 사용
# }


# =============================================================================
# 커스텀 Load 설정
# =============================================================================
# None이면 common 사용, 설정을 지정하면 해당 설정 사용
CUSTOM_LOAD_CONFIGS = None

# 예시: 커스텀 Load 설정
# CUSTOM_LOAD_CONFIGS = {
#     "aps_wip": {
#         "target_table": "custom_wip_table",  # 다른 테이블명 사용
#         "target_schema": "custom_schema",    # 다른 스키마 사용
#         "key_columns": ["project_id", "snapshot_date", "custom_key"],
#     },
# }
