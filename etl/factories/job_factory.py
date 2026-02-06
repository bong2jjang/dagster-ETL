"""
Job Factory - 테넌트별 Job 동적 생성
PipelineAssetConfig 기반으로 파티션/비파티션 Job 분리
"""

from dagster import AssetKey, AssetSelection, define_asset_job

from etl.config.tenant_config import PipelineAssetConfig, TenantConfig
from etl.partitions.daily import daily_partitions_def


class JobFactory:
    """테넌트별 Job 생성 팩토리"""

    def __init__(self, tenant: TenantConfig, environment: str = "dev"):
        self.tenant = tenant
        self.tenant_id = tenant.id
        self.environment = environment

    def _asset_key(self, stage: str, name: str) -> AssetKey:
        """계층형 Asset Key 생성"""
        return AssetKey([self.tenant_id, stage, name])

    def _get_pipeline_configs(self) -> dict[str, PipelineAssetConfig]:
        """테넌트의 파이프라인 설정 조회 (환경별 오버라이드 적용)"""
        if self.tenant.assets.pipelines:
            pipelines = self.tenant.assets.pipelines
        else:
            pipelines = self.tenant.get_default_assets_config().pipelines

        return {
            name: config.resolve_for_env(self.environment)
            for name, config in pipelines.items()
        }

    def _collect_pipeline_assets(
        self, config: PipelineAssetConfig, name: str
    ) -> list[AssetKey]:
        """파이프라인의 모든 Asset Key 수집"""
        keys = [self._asset_key("extract", name)]
        if config.has_transfer:
            keys.append(self._asset_key("transfer", name))
        if config.save_to_trino and config.trino_output:
            keys.append(self._asset_key("load", name))
        return keys

    def create_all_jobs(self) -> list:
        """테넌트의 모든 활성화된 Job 생성"""
        jobs = []
        jobs_config = self.tenant.jobs
        pipelines = self._get_pipeline_configs()

        # 파티션별 파이프라인 분류
        partitioned = {k: v for k, v in pipelines.items() if v.date_column}
        non_partitioned = {k: v for k, v in pipelines.items() if not v.date_column}

        # Daily ETL Job (파티션 있는 asset만)
        if jobs_config.daily_etl.enabled and partitioned:
            jobs.append(self._create_daily_etl_job(partitioned))

        # Master Sync Job (비파티션 asset만)
        if jobs_config.master_sync.enabled and non_partitioned:
            jobs.append(self._create_master_sync_job(non_partitioned))

        # WIP Pipeline Job
        if jobs_config.wip_pipeline.enabled:
            job = self._create_pipeline_job(
                pipelines, "wip_pipeline", "WIP Data Pipeline",
                extract_names=["lot_history"],
                transfer_names=["aps_wip"],
                load_names=["aps_wip"],
            )
            if job:
                jobs.append(job)

        # Cycle Time Pipeline Job
        if jobs_config.cycle_time_pipeline.enabled:
            job = self._create_pipeline_job(
                pipelines, "cycle_time_pipeline", "Cycle Time Pipeline",
                extract_names=["lot_history", "process_result"],
                transfer_names=["cycle_time"],
                load_names=["cycle_time"],
            )
            if job:
                jobs.append(job)

        # Equipment Pipeline Job
        if jobs_config.equipment_pipeline.enabled:
            job = self._create_pipeline_job(
                pipelines, "equipment_pipeline", "Equipment Utilization Pipeline",
                extract_names=["equipment_event"],
                transfer_names=["equipment_utilization"],
                load_names=["equipment_utilization"],
            )
            if job:
                jobs.append(job)

        # dbt Transform Job
        if jobs_config.dbt_transform.enabled:
            job = self._create_dbt_transform_job()
            if job:
                jobs.append(job)

        return jobs

    def _create_daily_etl_job(self, partitioned_pipelines: dict[str, PipelineAssetConfig]):
        """Daily ETL 파이프라인 Job (파티션 있는 asset만)"""
        asset_keys = []
        for name, config in partitioned_pipelines.items():
            asset_keys.extend(self._collect_pipeline_assets(config, name))

        return define_asset_job(
            name=f"{self.tenant_id}_daily_etl_job",
            description=f"[{self.tenant.name}] Daily ETL Pipeline (Extract → Transfer → Load)",
            selection=AssetSelection.assets(*asset_keys),
            partitions_def=daily_partitions_def,
            tags={
                "tenant_id": self.tenant_id,
                "pipeline": "daily-etl",
                **self.tenant.tags,
            },
        )

    def _create_master_sync_job(self, non_partitioned_pipelines: dict[str, PipelineAssetConfig]):
        """마스터 데이터 동기화 Job (비파티션 asset)"""
        asset_keys = []
        for name, config in non_partitioned_pipelines.items():
            asset_keys.extend(self._collect_pipeline_assets(config, name))

        return define_asset_job(
            name=f"{self.tenant_id}_master_sync_job",
            description=f"[{self.tenant.name}] Master Data Sync (Non-partitioned)",
            selection=AssetSelection.assets(*asset_keys),
            tags={
                "tenant_id": self.tenant_id,
                "pipeline": "master-sync",
                **self.tenant.tags,
            },
        )

    def _create_pipeline_job(
        self,
        pipelines: dict[str, PipelineAssetConfig],
        job_name_suffix: str,
        description: str,
        extract_names: list[str],
        transfer_names: list[str],
        load_names: list[str],
    ):
        """개별 파이프라인 Job 생성 (지정된 asset이 존재하는 경우만)"""
        asset_keys = []

        for name in extract_names:
            if name in pipelines:
                asset_keys.append(self._asset_key("extract", name))

        for name in transfer_names:
            if name in pipelines and pipelines.get(name, PipelineAssetConfig(source_table="")).has_transfer:
                asset_keys.append(self._asset_key("transfer", name))

        for name in load_names:
            config = pipelines.get(name)
            if config and config.save_to_trino and config.trino_output:
                asset_keys.append(self._asset_key("load", name))

        if not asset_keys:
            return None

        # 파티션 여부 결정 (모든 관련 asset이 파티션인지 확인)
        has_partition = any(
            pipelines.get(name, PipelineAssetConfig(source_table="")).date_column
            for name in extract_names
            if name in pipelines
        )

        return define_asset_job(
            name=f"{self.tenant_id}_{job_name_suffix}_job",
            description=f"[{self.tenant.name}] {description}",
            selection=AssetSelection.assets(*asset_keys),
            partitions_def=daily_partitions_def if has_partition else None,
            tags={
                "tenant_id": self.tenant_id,
                "pipeline": job_name_suffix.replace("_", "-"),
                **self.tenant.tags,
            },
        )

    def _create_dbt_transform_job(self):
        """dbt Transform Job 생성 (tenant_id/dbt/* 프리픽스 기반)"""
        return define_asset_job(
            name=f"{self.tenant_id}_dbt_transform_job",
            description=f"[{self.tenant.name}] dbt Transform Pipeline",
            selection=AssetSelection.key_prefixes([self.tenant_id, "dbt"]),
            tags={
                "tenant_id": self.tenant_id,
                "pipeline": "dbt-transform",
                **self.tenant.tags,
            },
        )
