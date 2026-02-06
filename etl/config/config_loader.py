"""
Configuration Loader
테넌트 폴더에서 설정을 로드하고 환경변수를 해석

지원하는 구조:
1. 폴더 기반: tenants/{tenant_id}/config.yaml
2. 파일 기반 (레거시): tenants/tenant_{id}.yaml
"""

import logging
import os
import re
from pathlib import Path
from typing import Any

import yaml

from etl.config.tenant_config import TenantConfig

logger = logging.getLogger(__name__)


class ConfigLoader:
    """테넌트 설정 로더"""

    def __init__(self, config_dir: Path | None = None):
        """
        Args:
            config_dir: 테넌트 설정 폴더가 있는 디렉토리
        """
        self.config_dir = config_dir or Path(__file__).parent.parent / "tenants"
        self._tenants: dict[str, TenantConfig] = {}

    def _resolve_env_vars(self, value: Any) -> Any:
        """
        설정값에서 ${VAR:default} 패턴의 환경변수 해석

        Examples:
            ${DB_HOST:localhost} -> 환경변수 DB_HOST 값 또는 "localhost"
            ${DB_PASSWORD} -> 환경변수 DB_PASSWORD 값 또는 빈 문자열
        """
        if isinstance(value, str):
            pattern = r"\$\{([^}:]+)(?::([^}]*))?\}"

            def replacer(match: re.Match) -> str:
                var_name = match.group(1)
                default = match.group(2) if match.group(2) is not None else ""
                return os.environ.get(var_name, default)

            return re.sub(pattern, replacer, value)

        elif isinstance(value, dict):
            return {k: self._resolve_env_vars(v) for k, v in value.items()}

        elif isinstance(value, list):
            return [self._resolve_env_vars(item) for item in value]

        return value

    def load_tenant(self, config_path: Path) -> TenantConfig:
        """
        단일 테넌트 설정 파일 로드

        Args:
            config_path: YAML 설정 파일 경로

        Returns:
            TenantConfig 인스턴스
        """
        with open(config_path, encoding="utf-8") as f:
            raw_config = yaml.safe_load(f)

        # "tenant" 키 아래의 설정 추출
        tenant_data = raw_config.get("tenant", raw_config)

        # 환경변수 해석
        resolved_config = self._resolve_env_vars(tenant_data)

        return TenantConfig(**resolved_config)

    def _find_tenant_configs(self) -> list[tuple[str, Path]]:
        """
        테넌트 설정 파일 탐색

        Returns:
            [(tenant_id, config_path), ...] 리스트
        """
        configs = []

        if not self.config_dir.exists():
            return configs

        # 1. 폴더 기반 탐색: tenants/{tenant_id}/config.yaml
        for tenant_dir in self.config_dir.iterdir():
            if not tenant_dir.is_dir():
                continue

            # 템플릿, 숨김 폴더 제외
            if tenant_dir.name.startswith("_") or tenant_dir.name.startswith("."):
                continue

            config_file = tenant_dir / "config.yaml"
            if config_file.exists():
                configs.append((tenant_dir.name, config_file))

        # 2. 파일 기반 탐색 (레거시): tenants/tenant_*.yaml
        for config_file in self.config_dir.glob("tenant_*.yaml"):
            if config_file.name.startswith("_"):
                continue

            # 이미 폴더 기반으로 로드된 경우 스킵
            tenant_id = config_file.stem.replace("tenant_", "")
            if any(tid == tenant_id for tid, _ in configs):
                continue

            configs.append((tenant_id, config_file))

        return configs

    def load_all_tenants(self, environment: str = "prod") -> dict[str, TenantConfig]:
        """
        모든 테넌트 설정 로드 (환경별 필터링)

        Args:
            environment: 대상 환경 (dev, staging, prod)

        Returns:
            {tenant_id: TenantConfig} 딕셔너리
        """
        tenants: dict[str, TenantConfig] = {}

        if not self.config_dir.exists():
            logger.warning("Config directory not found: %s", self.config_dir)
            return tenants

        for tenant_id, config_path in self._find_tenant_configs():
            try:
                tenant = self.load_tenant(config_path)

                # 환경별 활성화 확인
                if tenant.is_enabled_for_environment(environment):
                    tenants[tenant.id] = tenant
                    has_custom = self._check_custom_code(tenant_id)
                    custom_flag = " [custom]" if has_custom else ""
                    logger.info(
                        "Loaded tenant: %s (%s)%s", tenant.id, tenant.name, custom_flag
                    )
                else:
                    logger.info(
                        "Skipped tenant: %s (disabled for %s)", tenant.id, environment
                    )

            except Exception as e:
                logger.warning("Failed to load tenant config %s: %s", config_path, e)

        self._tenants = tenants
        return tenants

    def _check_custom_code(self, tenant_id: str) -> bool:
        """테넌트에 커스텀 코드가 있는지 확인"""
        tenant_dir = self.config_dir / tenant_id

        if not tenant_dir.is_dir():
            return False

        # assets 폴더가 있고 파이썬 파일이 있으면 커스텀 코드 있음
        assets_dir = tenant_dir / "assets"
        if assets_dir.exists():
            py_files = list(assets_dir.glob("*.py"))
            # __init__.py 외에 다른 파일이 있으면 커스텀
            non_init = [f for f in py_files if f.name != "__init__.py"]
            if non_init:
                return True

        # __init__.py에서 CUSTOM_ 변수 확인
        init_file = tenant_dir / "__init__.py"
        if init_file.exists():
            content = init_file.read_text(encoding="utf-8")
            if (
                "CUSTOM_" in content
                and "= None" not in content.split("CUSTOM_")[1][:50]
            ):
                return True

        return False

    def get_tenant(self, tenant_id: str) -> TenantConfig | None:
        """특정 테넌트 설정 조회"""
        return self._tenants.get(tenant_id)

    def get_tenant_dir(self, tenant_id: str) -> Path | None:
        """테넌트 폴더 경로 조회"""
        tenant_dir = self.config_dir / tenant_id
        if tenant_dir.is_dir():
            return tenant_dir
        return None

    def reload(self, environment: str = "prod") -> dict[str, TenantConfig]:
        """설정 다시 로드"""
        self._tenants = {}
        return self.load_all_tenants(environment)
