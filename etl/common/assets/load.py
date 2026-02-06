"""
Common Load Logic - 공용 적재 로직
테넌트별 커스텀이 필요한 경우 tenants/{tenant}/assets/load.py에서 오버라이드
"""

from typing import Any

from etl.utils.logging import ETLLogger


logger = ETLLogger("common.load")


# 기본 Load 설정
DEFAULT_LOAD_CONFIGS = {
    "aps_wip": {
        "target_table": "aps_input_wip",
        "target_schema": "aps",
        "key_columns": ["project_id", "snapshot_date", "process_step", "product_code"],
    },
    "cycle_time": {
        "target_table": "aps_input_cycle_time",
        "target_schema": "aps",
        "key_columns": ["project_id", "snapshot_date", "process_step", "product_code"],
    },
    "equipment_utilization": {
        "target_table": "equipment_utilization",
        "target_schema": "monitoring",
        "key_columns": ["project_id", "snapshot_date", "equipment_id"],
    },
}


def get_load_config(asset_name: str, custom_configs: dict | None = None) -> dict:
    """
    Load 설정 조회 (커스텀 설정 우선)

    Args:
        asset_name: Asset 이름
        custom_configs: 테넌트별 커스텀 설정

    Returns:
        설정 딕셔너리
    """
    default_config = DEFAULT_LOAD_CONFIGS.get(asset_name, {
        "target_table": asset_name,
        "target_schema": "public",
        "key_columns": ["project_id", "snapshot_date"],
    })

    if custom_configs and asset_name in custom_configs:
        return {**default_config, **custom_configs[asset_name]}

    return default_config
