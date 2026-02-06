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
    trino_http_scheme: str = "http"
    target_catalog: str = "iceberg"
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
    master_sync: JobConfig = Field(default_factory=JobConfig)
    wip_pipeline: JobConfig = Field(default_factory=JobConfig)
    cycle_time_pipeline: JobConfig = Field(default_factory=JobConfig)
    equipment_pipeline: JobConfig = Field(default_factory=JobConfig)


class TrinoOutputConfig(BaseModel):
    """Trino 출력 설정"""

    target_table: str
    target_schema: str = "aps"
    key_columns: list[str] = Field(default_factory=list)


class PipelineEnvOverride(BaseModel):
    """환경별 파이프라인 옵션 오버라이드"""

    save_to_s3: bool | None = None
    save_to_trino: bool | None = None


class PipelineAssetConfig(BaseModel):
    """개별 파이프라인 Asset 설정

    파이프라인 흐름: input_load → (optional) transfer → (optional) output_save

    environments 필드로 환경별 save_to_s3/save_to_trino를 오버라이드할 수 있음.
    예) dev에서는 save_to_s3: false, prod에서는 save_to_s3: true
    """

    source_table: str
    query: str | None = None
    date_column: str | None = None  # None이면 파티션 없음 (마스터 데이터)
    save_to_s3: bool = True  # Extract 결과 S3 Parquet 저장 여부
    has_transfer: bool = False  # transfer 단계 포함 여부
    transfer_inputs: list[str] | None = None  # transfer의 입력 asset 목록 (None이면 자기 자신)
    save_to_trino: bool = False  # Trino 적재 여부
    trino_output: TrinoOutputConfig | None = None
    environments: dict[str, PipelineEnvOverride] | None = None

    def resolve_for_env(self, env: str) -> "PipelineAssetConfig":
        """환경에 맞게 옵션을 오버라이드한 설정 반환"""
        if not self.environments or env not in self.environments:
            return self

        override = self.environments[env]
        data = self.model_dump()
        data.pop("environments", None)

        if override.save_to_s3 is not None:
            data["save_to_s3"] = override.save_to_s3
        if override.save_to_trino is not None:
            data["save_to_trino"] = override.save_to_trino

        return PipelineAssetConfig(**data)


class AssetsConfig(BaseModel):
    """Asset 설정 (파이프라인 기반)"""

    pipelines: dict[str, PipelineAssetConfig] = Field(default_factory=dict)


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
            pipelines={
                "lot_history": PipelineAssetConfig(
                    source_table="lot_history",
                    date_column="created_at",
                    has_transfer=True,
                    save_to_trino=True,
                    trino_output=TrinoOutputConfig(
                        target_table="aps_input_wip",
                        target_schema="aps",
                        key_columns=["project_id", "snapshot_date", "process_step", "product_code"],
                    ),
                ),
                "equipment_event": PipelineAssetConfig(
                    source_table="equipment_event",
                    date_column="event_time",
                    has_transfer=True,
                    save_to_trino=True,
                    trino_output=TrinoOutputConfig(
                        target_table="equipment_utilization",
                        target_schema="monitoring",
                        key_columns=["project_id", "snapshot_date", "equipment_id"],
                    ),
                ),
                "process_result": PipelineAssetConfig(
                    source_table="process_result",
                    date_column="measured_at",
                    has_transfer=True,
                    save_to_trino=True,
                    trino_output=TrinoOutputConfig(
                        target_table="aps_input_cycle_time",
                        target_schema="aps",
                        key_columns=["project_id", "snapshot_date", "process_step", "product_code"],
                    ),
                ),
            },
        )
