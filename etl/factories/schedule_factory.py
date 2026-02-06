"""
Schedule Factory - 테넌트별 Schedule 동적 생성
"""

from dagster import DefaultScheduleStatus, ScheduleDefinition

from etl.config.tenant_config import TenantConfig


class ScheduleFactory:
    """테넌트별 Schedule 생성 팩토리"""

    def __init__(self, tenant: TenantConfig, jobs: dict):
        """
        Args:
            tenant: 테넌트 설정
            jobs: {job_name: job_definition} 딕셔너리
        """
        self.tenant = tenant
        self.tenant_id = tenant.id
        self.jobs = jobs

    def create_all_schedules(self) -> list:
        """테넌트의 모든 활성화된 Schedule 생성"""
        schedules = []
        jobs_config = self.tenant.jobs

        # Daily ETL Schedule
        if jobs_config.daily_etl.enabled and jobs_config.daily_etl.schedule:
            schedule = self._create_schedule(
                job_suffix="daily_etl_job",
                schedule_config=jobs_config.daily_etl.schedule,
                schedule_suffix="daily_etl_schedule",
            )
            if schedule:
                schedules.append(schedule)

        # Master Sync Schedule
        if jobs_config.master_sync.enabled and jobs_config.master_sync.schedule:
            schedule = self._create_schedule(
                job_suffix="master_sync_job",
                schedule_config=jobs_config.master_sync.schedule,
                schedule_suffix="master_sync_schedule",
            )
            if schedule:
                schedules.append(schedule)

        # WIP Pipeline Schedule
        if jobs_config.wip_pipeline.enabled and jobs_config.wip_pipeline.schedule:
            schedule = self._create_schedule(
                job_suffix="wip_pipeline_job",
                schedule_config=jobs_config.wip_pipeline.schedule,
                schedule_suffix="wip_pipeline_schedule",
            )
            if schedule:
                schedules.append(schedule)

        # Cycle Time Pipeline Schedule
        if jobs_config.cycle_time_pipeline.enabled and jobs_config.cycle_time_pipeline.schedule:
            schedule = self._create_schedule(
                job_suffix="cycle_time_pipeline_job",
                schedule_config=jobs_config.cycle_time_pipeline.schedule,
                schedule_suffix="cycle_time_pipeline_schedule",
            )
            if schedule:
                schedules.append(schedule)

        # Equipment Pipeline Schedule
        if jobs_config.equipment_pipeline.enabled and jobs_config.equipment_pipeline.schedule:
            schedule = self._create_schedule(
                job_suffix="equipment_pipeline_job",
                schedule_config=jobs_config.equipment_pipeline.schedule,
                schedule_suffix="equipment_pipeline_schedule",
            )
            if schedule:
                schedules.append(schedule)

        return schedules

    def _create_schedule(
        self,
        job_suffix: str,
        schedule_config,
        schedule_suffix: str,
    ) -> ScheduleDefinition | None:
        """
        개별 Schedule 생성

        Args:
            job_suffix: Job 이름 접미사 (e.g., "daily_etl_job")
            schedule_config: ScheduleConfig 인스턴스
            schedule_suffix: Schedule 이름 접미사
        """
        job_name = f"{self.tenant_id}_{job_suffix}"
        job = self.jobs.get(job_name)

        if not job:
            print(f"Warning: Job not found for schedule: {job_name}")
            return None

        return ScheduleDefinition(
            job=job,
            cron_schedule=schedule_config.cron,
            execution_timezone=schedule_config.timezone,
            default_status=(
                DefaultScheduleStatus.RUNNING
                if schedule_config.auto_start
                else DefaultScheduleStatus.STOPPED
            ),
            name=f"{self.tenant_id}_{schedule_suffix}",
            tags={
                "tenant_id": self.tenant_id,
                **self.tenant.tags,
            },
        )
