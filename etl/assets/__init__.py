"""
Assets Module - Legacy Compatibility Layer

이 모듈은 기존 단일 테넌트 모드와의 호환성을 위해 유지됩니다.
실제 Asset 로직은 etl/common/assets/에 정의되어 있으며,
멀티테넌트 환경에서는 AssetFactory를 통해 동적으로 생성됩니다.

권장 사용 방식:
- 멀티테넌트: etl/__init__.py의 build_definitions() 사용
- 단일테넌트(레거시): 이 모듈의 Assets 직접 사용 (호환성 유지)

참고:
- 새로운 로직 추가 시 etl/common/assets/에 추가
- 테넌트별 커스텀 로직은 etl/tenants/{tenant_id}/assets/에 추가
"""

# 레거시 호환성을 위해 기존 Asset 유지
# 실제 로직은 common/assets/에서 import
from etl.common.assets.extract import (
    DEFAULT_EXTRACT_CONFIGS,
    DEFAULT_EXTRACT_QUERIES,
    get_extract_config,
    get_extract_query,
)
from etl.common.assets.load import (
    DEFAULT_LOAD_CONFIGS,
    get_load_config,
)
from etl.common.assets.transfer import (
    transform_aps_wip_logic,
    transform_cycle_time_logic,
    transform_equipment_utilization_logic,
)

__all__ = [
    # Extract
    "DEFAULT_EXTRACT_QUERIES",
    "DEFAULT_EXTRACT_CONFIGS",
    "get_extract_query",
    "get_extract_config",
    # Transform
    "transform_aps_wip_logic",
    "transform_cycle_time_logic",
    "transform_equipment_utilization_logic",
    # Load
    "DEFAULT_LOAD_CONFIGS",
    "get_load_config",
]
