"""
ETL Common Module - 공용 코드

모든 테넌트에서 공유하는 기본 로직을 정의합니다.
테넌트별 커스텀이 필요한 경우 tenants/{tenant_id}/assets/에서 오버라이드합니다.

구조:
    common/
    └── assets/
        ├── extract.py   - 기본 Extract(Input Load) 쿼리 및 설정
        ├── transfer.py  - 기본 Transfer 로직
        └── load.py      - 기본 Load(Output Save) 설정
"""

from etl.common.assets import extract, transfer, load

# Extract
from etl.common.assets.extract import (
    DEFAULT_EXTRACT_QUERIES,
    DEFAULT_EXTRACT_CONFIGS,
    get_extract_query,
    get_extract_config,
)

# Transfer
from etl.common.assets.transfer import (
    transform_aps_wip_logic,
    transform_cycle_time_logic,
    transform_equipment_utilization_logic,
)

# Load
from etl.common.assets.load import (
    DEFAULT_LOAD_CONFIGS,
    get_load_config,
)

__all__ = [
    # Modules
    "extract",
    "transfer",
    "load",
    # Extract
    "DEFAULT_EXTRACT_QUERIES",
    "DEFAULT_EXTRACT_CONFIGS",
    "get_extract_query",
    "get_extract_config",
    # Transfer
    "transform_aps_wip_logic",
    "transform_cycle_time_logic",
    "transform_equipment_utilization_logic",
    # Load
    "DEFAULT_LOAD_CONFIGS",
    "get_load_config",
]
