"""
Daily ETL Job Definition
"""

from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    ScheduleDefinition,
    define_asset_job,
)

from etl.partitions.daily import daily_partitions_def

# Extract → Transform → Load 전체 파이프라인 Job
daily_etl_job = define_asset_job(
    name="daily_etl_job",
    description="일별 ETL 전체 파이프라인 (Extract → Transform → Load)",
    selection=AssetSelection.groups("extract", "transform", "load"),
    partitions_def=daily_partitions_def,
    tags={
        "team": "data-engineering",
        "pipeline": "aps-etl",
    },
)

# Extract만 실행하는 Job
extract_job = define_asset_job(
    name="extract_job",
    description="Extract 단계만 실행",
    selection=AssetSelection.groups("extract"),
    partitions_def=daily_partitions_def,
    tags={"stage": "extract"},
)

# Transform만 실행하는 Job
transform_job = define_asset_job(
    name="transform_job",
    description="Transform 단계만 실행",
    selection=AssetSelection.groups("transform"),
    partitions_def=daily_partitions_def,
    tags={"stage": "transform"},
)

# Load만 실행하는 Job
load_job = define_asset_job(
    name="load_job",
    description="Load 단계만 실행",
    selection=AssetSelection.groups("load"),
    partitions_def=daily_partitions_def,
    tags={"stage": "load"},
)

# WIP 파이프라인 (lot_history → aps_wip → load_aps_wip)
wip_pipeline_job = define_asset_job(
    name="wip_pipeline_job",
    description="WIP 데이터 파이프라인",
    selection=AssetSelection.assets(
        "extract_lot_history", "transform_aps_wip", "load_aps_wip"
    ),
    partitions_def=daily_partitions_def,
    tags={"pipeline": "wip"},
)

# Cycle Time 파이프라인
cycle_time_pipeline_job = define_asset_job(
    name="cycle_time_pipeline_job",
    description="Cycle Time 데이터 파이프라인",
    selection=AssetSelection.assets(
        "extract_lot_history",
        "extract_process_result",
        "transform_cycle_time",
        "load_cycle_time",
    ),
    partitions_def=daily_partitions_def,
    tags={"pipeline": "cycle-time"},
)

# Equipment Utilization 파이프라인
equipment_pipeline_job = define_asset_job(
    name="equipment_pipeline_job",
    description="설비 가동률 데이터 파이프라인",
    selection=AssetSelection.assets(
        "extract_equipment_event",
        "transform_equipment_utilization",
        "load_equipment_utilization",
    ),
    partitions_def=daily_partitions_def,
    tags={"pipeline": "equipment"},
)


# 스케줄 정의: 매일 새벽 1시 (KST)에 전일 파티션 실행
daily_etl_schedule = ScheduleDefinition(
    job=daily_etl_job,
    cron_schedule="0 1 * * *",  # 매일 01:00 KST
    execution_timezone="Asia/Seoul",
    default_status=DefaultScheduleStatus.STOPPED,  # 기본 비활성화 (수동 활성화 필요)
)
