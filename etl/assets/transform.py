"""
Transform Assets - Legacy Compatibility Layer

이 모듈은 기존 단일 테넌트 모드와의 호환성을 위해 유지됩니다.
새로운 Transform 로직은 etl/common/assets/transform.py에 추가하세요.

멀티테넌트 환경에서는 AssetFactory를 통해 동적으로 Asset이 생성됩니다.
"""

from typing import Any

from dagster import (
    AssetExecutionContext,
    AssetIn,
    MetadataValue,
    Output,
    asset,
)

from etl.common.assets.transfer import (
    transform_aps_wip_logic,
    transform_cycle_time_logic,
    transform_equipment_utilization_logic,
)
from etl.partitions.daily import daily_partitions_def
from etl.resources.s3 import S3Resource
from etl.utils.logging import ETLLogger


logger = ETLLogger("transform")


# =============================================================================
# 레거시 단일 테넌트용 Assets (호환성 유지)
# =============================================================================


@asset(
    partitions_def=daily_partitions_def,
    ins={"extract_lot_history": AssetIn(key="extract_lot_history")},
    group_name="transform",
    description="APS 입력용 WIP 데이터 생성 [Legacy]",
    compute_kind="pandas",
)
def transform_aps_wip(
    context: AssetExecutionContext,
    s3: S3Resource,
    extract_lot_history: dict[str, Any],
) -> Output[dict[str, Any]]:
    """
    lot_history에서 APS 입력용 WIP 데이터 생성

    - 공정별 WIP 계산
    - Hold/Scrap Lot 필터링
    - APS Input Schema로 정규화
    """
    partition_date = context.partition_key
    job_name = "aps_wip"
    input_path = extract_lot_history["s3_path"]

    logger.log_transform_start(job_name, partition_date, input_path)

    # Extract 결과 로드
    df = s3.read_parquet_from_path(input_path)
    input_rows = len(df)

    # 공용 변환 로직 호출
    wip_df = transform_aps_wip_logic(
        input_dfs={"lot_history": df},
        partition_date=partition_date,
        tenant_id="default",
    )

    # S3에 저장
    s3_path = s3.write_parquet(
        df=wip_df,
        stage="transform",
        job_name=job_name,
        partition_date=partition_date,
    )

    output_rows = len(wip_df)
    logger.log_transform_complete(
        job_name, partition_date, input_rows, output_rows, s3_path
    )

    return Output(
        value={"s3_path": s3_path, "row_count": output_rows},
        metadata={
            "input_rows": MetadataValue.int(input_rows),
            "output_rows": MetadataValue.int(output_rows),
            "s3_path": MetadataValue.path(s3_path),
            "partition_date": MetadataValue.text(partition_date),
        },
    )


@asset(
    partitions_def=daily_partitions_def,
    ins={
        "extract_lot_history": AssetIn(key="extract_lot_history"),
        "extract_process_result": AssetIn(key="extract_process_result"),
    },
    group_name="transform",
    description="공정별 Cycle Time 산출 [Legacy]",
    compute_kind="pandas",
)
def transform_cycle_time(
    context: AssetExecutionContext,
    s3: S3Resource,
    extract_lot_history: dict[str, Any],
    extract_process_result: dict[str, Any],
) -> Output[dict[str, Any]]:
    """
    lot_history와 process_result를 조인하여 Cycle Time 산출
    """
    partition_date = context.partition_key
    job_name = "cycle_time"

    logger.log_transform_start(
        job_name, partition_date, extract_lot_history["s3_path"]
    )

    # 데이터 로드
    lot_df = s3.read_parquet_from_path(extract_lot_history["s3_path"])
    result_df = s3.read_parquet_from_path(extract_process_result["s3_path"])

    input_rows = len(lot_df)

    # 공용 변환 로직 호출
    cycle_time_df = transform_cycle_time_logic(
        input_dfs={"lot_history": lot_df, "process_result": result_df},
        partition_date=partition_date,
        tenant_id="default",
    )

    # S3 저장
    s3_path = s3.write_parquet(
        df=cycle_time_df,
        stage="transform",
        job_name=job_name,
        partition_date=partition_date,
    )

    output_rows = len(cycle_time_df)
    logger.log_transform_complete(
        job_name, partition_date, input_rows, output_rows, s3_path
    )

    return Output(
        value={"s3_path": s3_path, "row_count": output_rows},
        metadata={
            "input_rows": MetadataValue.int(input_rows),
            "output_rows": MetadataValue.int(output_rows),
            "s3_path": MetadataValue.path(s3_path),
            "partition_date": MetadataValue.text(partition_date),
        },
    )


@asset(
    partitions_def=daily_partitions_def,
    ins={"extract_equipment_event": AssetIn(key="extract_equipment_event")},
    group_name="transform",
    description="설비 가동률 산출 [Legacy]",
    compute_kind="pandas",
)
def transform_equipment_utilization(
    context: AssetExecutionContext,
    s3: S3Resource,
    extract_equipment_event: dict[str, Any],
) -> Output[dict[str, Any]]:
    """
    equipment_event에서 설비 가동률 산출
    """
    partition_date = context.partition_key
    job_name = "equipment_utilization"
    input_path = extract_equipment_event["s3_path"]

    logger.log_transform_start(job_name, partition_date, input_path)

    df = s3.read_parquet_from_path(input_path)
    input_rows = len(df)

    # 공용 변환 로직 호출
    result_df = transform_equipment_utilization_logic(
        input_dfs={"equipment_event": df},
        partition_date=partition_date,
        tenant_id="default",
    )

    # S3 저장
    s3_path = s3.write_parquet(
        df=result_df,
        stage="transform",
        job_name=job_name,
        partition_date=partition_date,
    )

    output_rows = len(result_df)
    logger.log_transform_complete(
        job_name, partition_date, input_rows, output_rows, s3_path
    )

    return Output(
        value={"s3_path": s3_path, "row_count": output_rows},
        metadata={
            "input_rows": MetadataValue.int(input_rows),
            "output_rows": MetadataValue.int(output_rows),
            "s3_path": MetadataValue.path(s3_path),
            "partition_date": MetadataValue.text(partition_date),
        },
    )
