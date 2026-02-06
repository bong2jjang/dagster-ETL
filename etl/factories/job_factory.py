"""
Job Factory - 테넌트별 Job 동적 생성
"""

from dagster import AssetKey, AssetSelection, define_asset_job

from etl.config.tenant_config import TenantConfig
from etl.partitions.daily import daily_partitions_def


class JobFactory:
    """테넌트별 Job 생성 팩토리"""

    def __init__(self, tenant: TenantConfig):
        self.tenant = tenant
        self.tenant_id = tenant.id

    def _asset_key(self, stage: str, name: str) -> AssetKey:
        """계층형 Asset Key 생성"""
        return AssetKey([self.tenant_id, stage, name])

    def create_all_jobs(self) -> list:
        """테넌트의 모든 활성화된 Job 생성"""
        jobs = []
        jobs_config = self.tenant.jobs

        # Daily ETL Job (전체 파이프라인)
        if jobs_config.daily_etl.enabled:
            jobs.append(self._create_daily_etl_job())

        # Extract Only Job
        if jobs_config.extract_only.enabled:
            jobs.append(self._create_extract_job())

        # Transform Only Job
        if jobs_config.transform_only.enabled:
            jobs.append(self._create_transform_job())

        # Load Only Job
        if jobs_config.load_only.enabled:
            jobs.append(self._create_load_job())

        # WIP Pipeline Job
        if jobs_config.wip_pipeline.enabled:
            jobs.append(self._create_wip_pipeline_job())

        # Cycle Time Pipeline Job
        if jobs_config.cycle_time_pipeline.enabled:
            jobs.append(self._create_cycle_time_pipeline_job())

        # Equipment Pipeline Job
        if jobs_config.equipment_pipeline.enabled:
            jobs.append(self._create_equipment_pipeline_job())

        return jobs

    def _create_daily_etl_job(self):
        """Daily ETL 전체 파이프라인 Job"""
        return define_asset_job(
            name=f"{self.tenant_id}_daily_etl_job",
            description=f"[{self.tenant.name}] Daily ETL Pipeline (Extract → Transform → Load)",
            selection=AssetSelection.groups(self.tenant_id),
            partitions_def=daily_partitions_def,
            tags={
                "tenant_id": self.tenant_id,
                "pipeline": "daily-etl",
                **self.tenant.tags,
            },
        )

    def _create_extract_job(self):
        """Extract 단계만 실행하는 Job"""
        return define_asset_job(
            name=f"{self.tenant_id}_extract_job",
            description=f"[{self.tenant.name}] Extract Only",
            selection=AssetSelection.assets(
                self._asset_key("extract", "lot_history"),
                self._asset_key("extract", "equipment_event"),
                self._asset_key("extract", "process_result"),
            ),
            partitions_def=daily_partitions_def,
            tags={
                "tenant_id": self.tenant_id,
                "stage": "extract",
                **self.tenant.tags,
            },
        )

    def _create_transform_job(self):
        """Transform 단계만 실행하는 Job"""
        return define_asset_job(
            name=f"{self.tenant_id}_transform_job",
            description=f"[{self.tenant.name}] Transform Only",
            selection=AssetSelection.assets(
                self._asset_key("transform", "aps_wip"),
                self._asset_key("transform", "cycle_time"),
                self._asset_key("transform", "equipment_utilization"),
            ),
            partitions_def=daily_partitions_def,
            tags={
                "tenant_id": self.tenant_id,
                "stage": "transform",
                **self.tenant.tags,
            },
        )

    def _create_load_job(self):
        """Load 단계만 실행하는 Job"""
        return define_asset_job(
            name=f"{self.tenant_id}_load_job",
            description=f"[{self.tenant.name}] Load Only",
            selection=AssetSelection.assets(
                self._asset_key("load", "aps_wip"),
                self._asset_key("load", "cycle_time"),
                self._asset_key("load", "equipment_utilization"),
            ),
            partitions_def=daily_partitions_def,
            tags={
                "tenant_id": self.tenant_id,
                "stage": "load",
                **self.tenant.tags,
            },
        )

    def _create_wip_pipeline_job(self):
        """WIP 데이터 파이프라인 Job"""
        return define_asset_job(
            name=f"{self.tenant_id}_wip_pipeline_job",
            description=f"[{self.tenant.name}] WIP Data Pipeline",
            selection=AssetSelection.assets(
                self._asset_key("extract", "lot_history"),
                self._asset_key("transform", "aps_wip"),
                self._asset_key("load", "aps_wip"),
            ),
            partitions_def=daily_partitions_def,
            tags={
                "tenant_id": self.tenant_id,
                "pipeline": "wip",
                **self.tenant.tags,
            },
        )

    def _create_cycle_time_pipeline_job(self):
        """Cycle Time 데이터 파이프라인 Job"""
        return define_asset_job(
            name=f"{self.tenant_id}_cycle_time_pipeline_job",
            description=f"[{self.tenant.name}] Cycle Time Pipeline",
            selection=AssetSelection.assets(
                self._asset_key("extract", "lot_history"),
                self._asset_key("extract", "process_result"),
                self._asset_key("transform", "cycle_time"),
                self._asset_key("load", "cycle_time"),
            ),
            partitions_def=daily_partitions_def,
            tags={
                "tenant_id": self.tenant_id,
                "pipeline": "cycle-time",
                **self.tenant.tags,
            },
        )

    def _create_equipment_pipeline_job(self):
        """설비 가동률 데이터 파이프라인 Job"""
        return define_asset_job(
            name=f"{self.tenant_id}_equipment_pipeline_job",
            description=f"[{self.tenant.name}] Equipment Utilization Pipeline",
            selection=AssetSelection.assets(
                self._asset_key("extract", "equipment_event"),
                self._asset_key("transform", "equipment_utilization"),
                self._asset_key("load", "equipment_utilization"),
            ),
            partitions_def=daily_partitions_def,
            tags={
                "tenant_id": self.tenant_id,
                "pipeline": "equipment",
                **self.tenant.tags,
            },
        )
