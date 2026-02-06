"""
Project 01 테넌트 - 커스텀 Transfer 로직 사용 예시
"""

TENANT_ID = "project_01"
TENANT_NAME = "Project 01"

# 커스텀 Extract 쿼리 (특정 쿼리만 오버라이드)
CUSTOM_EXTRACT_QUERIES = {
    # project_01은 lot_history에 추가 컬럼이 있음
    "lot_history": """
        SELECT
            lot_id,
            product_code,
            process_step,
            equipment_id,
            start_time,
            end_time,
            quantity,
            status,
            priority,
            customer_order_id,
            created_at
        FROM lot_history
    """,
}

# 커스텀 Transfer 함수 등록
# None이면 common 사용, 함수를 지정하면 해당 함수 사용
from etl.tenants.project_01.assets.transfer import (
    transform_aps_wip_logic as project_01_wip_logic,
)

CUSTOM_TRANSFER_FUNCTIONS = {
    "aps_wip": project_01_wip_logic,
    # "cycle_time": None,  # common 사용
    # "equipment_utilization": None,  # common 사용
}

# 커스텀 Load 설정
CUSTOM_LOAD_CONFIGS = {
    "aps_wip": {
        "target_table": "aps_input_wip",
        "target_schema": "aps",
        "key_columns": ["project_id", "snapshot_date", "process_step", "product_code"],
    },
}
