"""
Common Extract Logic - 공용 추출 로직
테넌트별 커스텀이 필요한 경우 tenants/{tenant}/assets/extract.py에서 오버라이드
"""

from typing import Any

import pandas as pd

from etl.utils.logging import ETLLogger


logger = ETLLogger("common.extract")


# 기본 Extract SQL 쿼리 템플릿
DEFAULT_EXTRACT_QUERIES = {
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
            created_at
        FROM lot_history
    """,
    "equipment_event": """
        SELECT
            event_id,
            equipment_id,
            event_type,
            event_time,
            duration_minutes,
            description,
            created_at
        FROM equipment_event
    """,
    "process_result": """
        SELECT
            result_id,
            lot_id,
            process_step,
            measurement_value,
            spec_lower,
            spec_upper,
            pass_fail,
            measured_at
        FROM process_result
    """,
    "cfg_item_master": """
        SELECT
            project_id,
            item_id,
            item_type,
            item_name,
            item_group_id,
            description,
            item_priority,
            procurement_type,
            prod_type,
            item_size_type,
            item_spec,
            create_datetime,
            update_datetime
        FROM cfg_item_master
        WHERE 
            project_id = 'EED70012-E49D-4BA5-AD05-870C338DF39A'
    """,
}

# 기본 Extract 설정
DEFAULT_EXTRACT_CONFIGS = {

    "lot_history": {
        "source_table": "lot_history",
        "date_column": "DATE(created_at)",
    },
    "equipment_event": {
        "source_table": "equipment_event",
        "date_column": "DATE(event_time)",
    },
    "process_result": {
        "source_table": "process_result",
        "date_column": "DATE(measured_at)",
    },
    "cfg_item_master": {
        "source_table": "cfg_item_master",
        "date_column": None,  # 파티션 없음 (마스터 데이터)
    },
}


def get_extract_query(asset_name: str, custom_queries: dict | None = None) -> str:
    """
    Extract 쿼리 조회 (커스텀 쿼리 우선)

    Args:
        asset_name: Asset 이름
        custom_queries: 테넌트별 커스텀 쿼리

    Returns:
        SQL 쿼리 문자열
    """
    if custom_queries and asset_name in custom_queries:
        return custom_queries[asset_name]
    return DEFAULT_EXTRACT_QUERIES.get(asset_name, f"SELECT * FROM {asset_name}")


def get_extract_config(asset_name: str, custom_configs: dict | None = None) -> dict:
    """
    Extract 설정 조회 (커스텀 설정 우선)

    Args:
        asset_name: Asset 이름
        custom_configs: 테넌트별 커스텀 설정

    Returns:
        설정 딕셔너리
    """
    default_config = DEFAULT_EXTRACT_CONFIGS.get(asset_name, {
        "source_table": asset_name,
        "date_column": "DATE(created_at)",
    })

    if custom_configs and asset_name in custom_configs:
        return {**default_config, **custom_configs[asset_name]}

    return default_config
