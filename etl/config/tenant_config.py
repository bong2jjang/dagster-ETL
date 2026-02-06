"""
Tenant Configuration Models
Pydantic 기반 테넌트 설정 스키마 정의
"""

import re
from typing import Any

from pydantic import BaseModel, Field, field_validator


class SourceDatabaseConfig(BaseModel):
    """Source 데이터베이스 연결 설정"""

    host: str = "localhost"
    port: int = 5432
    database: str
    username: str
    password: str = ""
    schema_name: str = Field(default="public", alias="schema")

    class Config:
        populate_by_name = True


class StorageConfig(BaseModel):
    """S3 스토리지 설정"""

    bucket: str = "data-lake"
    base_path: str  # e.g., "project_id=acme"

    def build_s3_key(
        self, stage: str, job_name: str, partition_date: str, filename: str = "data.parquet"
    ) -> str:
        """테넌트 S3 경로 생성"""
        date_formatted = partition_date.replace("-", "")
        return f"{self.base_path}/{stage}/job_name={job_name}/date={date_formatted}/{filename}"


class TargetDatabaseConfig(BaseModel):
    """Target 데이터베이스 설정 (Trino 경유)"""

    trino_host: str = "localhost"
    trino_port: int = 8080
    trino_user: str = "trino"
    trino_password: str = ""
    target_catalog: str = "postgresql"
    target_schema: str = "aps"  # 공유 스키마 (project_id 컬럼으로 구분)


class ScheduleConfig(BaseModel):
    """Job 스케줄 설정"""

    cron: str = "0 1 * * *"
    timezone: str = "Asia/Seoul"
    auto_start: bool = False


class JobConfig(BaseModel):
    """개별 Job 설정"""

    enabled: bool = True
    schedule: ScheduleConfig | None = None


class JobsConfig(BaseModel):
    """전체 Job 설정"""

    daily_etl: JobConfig = Field(default_factory=JobConfig)
    extract_only: JobConfig = Field(default_factory=lambda: JobConfig(enabled=False))
    transform_only: JobConfig = Field(default_factory=lambda: JobConfig(enabled=False))
    load_only: JobConfig = Field(default_factory=lambda: JobConfig(enabled=False))
    wip_pipeline: JobConfig = Field(default_factory=JobConfig)
    cycle_time_pipeline: JobConfig = Field(default_factory=JobConfig)
    equipment_pipeline: JobConfig = Field(default_factory=JobConfig)


class ExtractAssetConfig(BaseModel):
    """Extract Asset 설정"""

    enabled: bool = True
    source_table: str
    date_column: str
    query: str | None = None  # Custom SQL query (optional)


class TransformAssetConfig(BaseModel):
    """Transform Asset 설정"""

    enabled: bool = True


class LoadAssetConfig(BaseModel):
    """Load Asset 설정"""

    enabled: bool = True
    target_table: str
    target_schema: str = "aps"
    key_columns: list[str] = Field(default_factory=list)


class AssetsConfig(BaseModel):
    """Asset 설정"""

    extract: dict[str, ExtractAssetConfig] = Field(default_factory=dict)
    transform: dict[str, TransformAssetConfig] = Field(default_factory=dict)
    load: dict[str, LoadAssetConfig] = Field(default_factory=dict)


class EnvironmentConfig(BaseModel):
    """환경별 활성화 설정"""

    enabled: bool = True


class EnvironmentsConfig(BaseModel):
    """전체 환경 설정"""

    dev: EnvironmentConfig = Field(default_factory=EnvironmentConfig)
    staging: EnvironmentConfig = Field(default_factory=EnvironmentConfig)
    prod: EnvironmentConfig = Field(default_factory=EnvironmentConfig)


class TenantConfig(BaseModel):
    """테넌트 전체 설정"""

    id: str
    name: str
    enabled: bool = True
    environments: EnvironmentsConfig = Field(default_factory=EnvironmentsConfig)
    source_database: SourceDatabaseConfig
    storage: StorageConfig
    target_database: TargetDatabaseConfig = Field(default_factory=TargetDatabaseConfig)
    jobs: JobsConfig = Field(default_factory=JobsConfig)
    assets: AssetsConfig = Field(default_factory=AssetsConfig)
    tags: dict[str, str] = Field(default_factory=dict)

    @field_validator("id")
    @classmethod
    def validate_tenant_id(cls, v: str) -> str:
        """테넌트 ID 형식 검증 (소문자 영숫자, 언더스코어)"""
        if not re.match(r"^[a-z][a-z0-9_]*$", v):
            raise ValueError(
                "Tenant ID must be lowercase alphanumeric starting with a letter"
            )
        return v

    def is_enabled_for_environment(self, environment: str) -> bool:
        """특정 환경에서 활성화 여부 확인"""
        if not self.enabled:
            return False

        env_config = getattr(self.environments, environment, None)
        if env_config is None:
            return True  # 환경 설정이 없으면 기본 활성화

        return env_config.enabled

    def get_default_assets_config(self) -> AssetsConfig:
        """기본 Asset 설정 반환"""
        return AssetsConfig(
            extract={
                "lot_history": ExtractAssetConfig(
                    source_table="lot_history",
                    date_column="DATE(created_at)",
                ),
                "equipment_event": ExtractAssetConfig(
                    source_table="equipment_event",
                    date_column="DATE(event_time)",
                ),
                "process_result": ExtractAssetConfig(
                    source_table="process_result",
                    date_column="DATE(measured_at)",
                ),
            },
            transform={
                "aps_wip": TransformAssetConfig(),
                "cycle_time": TransformAssetConfig(),
                "equipment_utilization": TransformAssetConfig(),
            },
            load={
                "aps_wip": LoadAssetConfig(
                    target_table="aps_input_wip",
                    target_schema="aps",
                    key_columns=["project_id", "snapshot_date", "process_step", "product_code"],
                ),
                "cycle_time": LoadAssetConfig(
                    target_table="aps_input_cycle_time",
                    target_schema="aps",
                    key_columns=["project_id", "snapshot_date", "process_step", "product_code"],
                ),
                "equipment_utilization": LoadAssetConfig(
                    target_table="equipment_utilization",
                    target_schema="monitoring",
                    key_columns=["project_id", "snapshot_date", "equipment_id"],
                ),
            },
        )
