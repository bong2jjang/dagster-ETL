"""
Dagster ETL Project - Multi-Tenant Architecture
RDB → Transform → Trino → PostgreSQL ETL Pipeline

멀티테넌시 지원:
- YAML 파일로 테넌트 설정 관리
- 테넌트별 독립 Source DB
- 공유 Target DB (project_id 컬럼으로 분리)
- 테넌트별 S3 경로 격리
- 테넌트별 Code Location 분리 (Dagster UI에서 폴더 구분)
"""

import logging
import os
from pathlib import Path

from dagster import Definitions

from etl.config.config_loader import ConfigLoader
from etl.config.tenant_config import TenantConfig as TenantConfig
from etl.factories.asset_factory import AssetFactory
from etl.factories.job_factory import JobFactory
from etl.factories.resource_factory import ResourceFactory, get_shared_s3_config
from etl.factories.schedule_factory import ScheduleFactory

logger = logging.getLogger(__name__)


def build_tenant_definitions(tenant_id: str) -> Definitions:
    """
    특정 테넌트의 Dagster Definitions 빌드

    Args:
        tenant_id: 테넌트 ID

    Returns:
        해당 테넌트의 Definitions
    """
    environment = os.getenv("DAGSTER_ENVIRONMENT", "dev")

    # 테넌트 설정 로드
    config_dir = Path(__file__).parent / "tenants"
    loader = ConfigLoader(config_dir)
    tenants = loader.load_all_tenants(environment)

    if tenant_id not in tenants:
        logger.warning(
            "Tenant '%s' not found or disabled for %s", tenant_id, environment
        )
        return Definitions(assets=[], resources={}, jobs=[], schedules=[])

    tenant = tenants[tenant_id]
    logger.info("Building definitions for tenant: %s (%s)", tenant_id, tenant.name)

    # 공유 S3 설정
    shared_s3_config = get_shared_s3_config()

    # 1. Resource 생성
    resource_factory = ResourceFactory(tenant, shared_s3_config)
    resources = resource_factory.create_all_resources()

    # 2. Asset 생성
    asset_factory = AssetFactory(tenant, environment)
    assets = asset_factory.create_all_etl_assets()

    # 3. Job 생성
    job_factory = JobFactory(tenant, environment)
    jobs = job_factory.create_all_jobs()

    # 4. Schedule 생성
    jobs_dict = {job.name: job for job in jobs}
    schedule_factory = ScheduleFactory(tenant, jobs_dict)
    schedules = schedule_factory.create_all_schedules()

    logger.info(
        "[%s] Loaded: %d assets, %d jobs, %d schedules (env=%s)",
        tenant_id,
        len(assets),
        len(jobs),
        len(schedules),
        environment,
    )

    return Definitions(
        assets=assets,
        resources=resources,
        jobs=jobs,
        schedules=schedules,
    )


def build_all_definitions() -> Definitions:
    """
    모든 테넌트의 Dagster Definitions 빌드 (단일 Code Location)

    환경변수:
        DAGSTER_ENVIRONMENT: 실행 환경 (dev, staging, prod)
    """
    environment = os.getenv("DAGSTER_ENVIRONMENT", "dev")
    logger.info("Building all definitions for environment: %s", environment)

    # 테넌트 설정 로드
    config_dir = Path(__file__).parent / "tenants"
    loader = ConfigLoader(config_dir)
    tenants = loader.load_all_tenants(environment)

    if not tenants:
        logger.warning("No tenants loaded. Check tenant configuration files.")
        return Definitions(assets=[], resources={}, jobs=[], schedules=[])

    all_assets = []
    all_resources = {}
    all_jobs = []
    all_schedules = []

    # 공유 S3 설정
    shared_s3_config = get_shared_s3_config()

    for tenant_id, tenant in tenants.items():
        logger.info("Building assets for tenant: %s", tenant_id)

        # 1. Resource 생성
        resource_factory = ResourceFactory(tenant, shared_s3_config)
        tenant_resources = resource_factory.create_all_resources()
        all_resources.update(tenant_resources)

        # 2. Asset 생성 (TenantLoader를 통해 커스텀 코드 자동 적용)
        asset_factory = AssetFactory(tenant, environment)
        tenant_assets = asset_factory.create_all_etl_assets()
        all_assets.extend(tenant_assets)

        # 3. Job 생성
        job_factory = JobFactory(tenant, environment)
        tenant_jobs = job_factory.create_all_jobs()
        all_jobs.extend(tenant_jobs)

        # 4. Schedule 생성
        jobs_dict = {job.name: job for job in tenant_jobs}
        schedule_factory = ScheduleFactory(tenant, jobs_dict)
        tenant_schedules = schedule_factory.create_all_schedules()
        all_schedules.extend(tenant_schedules)

    logger.info(
        "Total: %d tenants, %d assets, %d jobs",
        len(tenants),
        len(all_assets),
        len(all_jobs),
    )

    return Definitions(
        assets=all_assets,
        resources=all_resources,
        jobs=all_jobs,
        schedules=all_schedules,
    )


def get_available_tenants() -> list[str]:
    """활성화된 테넌트 ID 목록 반환"""
    environment = os.getenv("DAGSTER_ENVIRONMENT", "dev")
    config_dir = Path(__file__).parent / "tenants"
    loader = ConfigLoader(config_dir)
    tenants = loader.load_all_tenants(environment)
    return list(tenants.keys())


# =============================================================================
# 테넌트별 Definitions (Code Location 분리용)
# workspace.yaml에서 각각 로드하여 UI에서 테넌트별 폴더로 표시
# =============================================================================


def get_project_01_definitions() -> Definitions:
    """Project 01 테넌트 Definitions"""
    return build_tenant_definitions("project_01")


def get_project_02_definitions() -> Definitions:
    """Project 02 테넌트 Definitions"""
    return build_tenant_definitions("project_02")


def get_default_definitions() -> Definitions:
    """Default 테넌트 Definitions"""
    return build_tenant_definitions("default")


# 기존 호환성 유지 (단일 Code Location 모드)
# build_definitions = build_all_definitions을 사용하면 모든 테넌트가 하나의 폴더에 표시
# defs = build_all_definitions()

# 테넌트별 분리 모드에서는 workspace.yaml에서 각 테넌트 함수를 개별 로드
