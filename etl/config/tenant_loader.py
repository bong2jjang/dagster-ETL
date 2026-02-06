"""
Tenant Loader - 테넌트별 커스텀 코드 로더
테넌트 폴더에서 커스텀 Extract/Transform/Load 코드를 로드
"""

import importlib
import sys
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from etl.common.assets.extract import (
    DEFAULT_EXTRACT_QUERIES,
    DEFAULT_EXTRACT_CONFIGS,
    get_extract_query,
    get_extract_config,
)
from etl.common.assets.transform import (
    transform_aps_wip_logic,
    transform_cycle_time_logic,
    transform_equipment_utilization_logic,
)
from etl.common.assets.load import (
    DEFAULT_LOAD_CONFIGS,
    get_load_config,
)


class TenantLoader:
    """테넌트별 커스텀 코드 로더"""

    # 공용 Transform 함수 매핑
    COMMON_TRANSFORM_FUNCTIONS = {
        "aps_wip": transform_aps_wip_logic,
        "cycle_time": transform_cycle_time_logic,
        "equipment_utilization": transform_equipment_utilization_logic,
    }

    def __init__(self, tenant_id: str, tenants_dir: Path | None = None):
        """
        Args:
            tenant_id: 테넌트 ID
            tenants_dir: 테넌트 폴더 디렉토리
        """
        self.tenant_id = tenant_id
        self.tenants_dir = tenants_dir or Path(__file__).parent.parent / "tenants"
        self.tenant_dir = self.tenants_dir / tenant_id

        self._custom_module = None
        self._loaded = False

    def _load_tenant_module(self) -> Any | None:
        """테넌트 모듈 로드"""
        if self._loaded:
            return self._custom_module

        self._loaded = True

        if not self.tenant_dir.exists():
            print(f"Tenant directory not found: {self.tenant_dir}")
            return None

        init_file = self.tenant_dir / "__init__.py"
        if not init_file.exists():
            print(f"Tenant __init__.py not found: {init_file}")
            return None

        try:
            module_name = f"etl.tenants.{self.tenant_id}"

            # 이미 로드된 경우 재로드
            if module_name in sys.modules:
                self._custom_module = importlib.reload(sys.modules[module_name])
            else:
                self._custom_module = importlib.import_module(module_name)

            return self._custom_module

        except Exception as e:
            print(f"Failed to load tenant module {self.tenant_id}: {e}")
            return None

    def get_extract_queries(self) -> dict[str, str]:
        """
        Extract 쿼리 조회 (커스텀 + 공용)

        Returns:
            {asset_name: sql_query} 딕셔너리
        """
        module = self._load_tenant_module()

        custom_queries = None
        if module and hasattr(module, "CUSTOM_EXTRACT_QUERIES"):
            custom_queries = module.CUSTOM_EXTRACT_QUERIES

        # 공용 쿼리를 기본으로, 커스텀이 있으면 오버라이드
        queries = dict(DEFAULT_EXTRACT_QUERIES)
        if custom_queries:
            queries.update(custom_queries)

        return queries

    def get_extract_config(self, asset_name: str) -> dict:
        """
        특정 Asset의 Extract 설정 조회

        Args:
            asset_name: Asset 이름

        Returns:
            설정 딕셔너리
        """
        return get_extract_config(asset_name)

    def get_transform_function(
        self, asset_name: str
    ) -> Callable[[dict[str, pd.DataFrame], str, str], pd.DataFrame]:
        """
        Transform 함수 조회 (커스텀 우선, 없으면 공용)

        Args:
            asset_name: Asset 이름 (e.g., "aps_wip")

        Returns:
            Transform 함수
        """
        module = self._load_tenant_module()

        # 커스텀 함수 확인
        if module and hasattr(module, "CUSTOM_TRANSFORM_FUNCTIONS"):
            custom_functions = module.CUSTOM_TRANSFORM_FUNCTIONS
            if custom_functions and asset_name in custom_functions:
                custom_fn = custom_functions[asset_name]
                if custom_fn is not None:
                    print(f"[{self.tenant_id}] Using custom transform for {asset_name}")
                    return custom_fn

        # 공용 함수 반환
        return self.COMMON_TRANSFORM_FUNCTIONS.get(asset_name, transform_aps_wip_logic)

    def get_load_config(self, asset_name: str) -> dict:
        """
        Load 설정 조회 (커스텀 우선, 없으면 공용)

        Args:
            asset_name: Asset 이름

        Returns:
            설정 딕셔너리
        """
        module = self._load_tenant_module()

        custom_configs = None
        if module and hasattr(module, "CUSTOM_LOAD_CONFIGS"):
            custom_configs = module.CUSTOM_LOAD_CONFIGS

        return get_load_config(asset_name, custom_configs)

    def get_all_transform_functions(
        self,
    ) -> dict[str, Callable[[dict[str, pd.DataFrame], str, str], pd.DataFrame]]:
        """
        모든 Transform 함수 조회

        Returns:
            {asset_name: transform_function} 딕셔너리
        """
        return {
            name: self.get_transform_function(name)
            for name in self.COMMON_TRANSFORM_FUNCTIONS.keys()
        }

    def has_custom_code(self) -> bool:
        """커스텀 코드 존재 여부"""
        module = self._load_tenant_module()
        if not module:
            return False

        has_custom = any([
            getattr(module, "CUSTOM_EXTRACT_QUERIES", None),
            getattr(module, "CUSTOM_TRANSFORM_FUNCTIONS", None),
            getattr(module, "CUSTOM_LOAD_CONFIGS", None),
        ])

        return has_custom
