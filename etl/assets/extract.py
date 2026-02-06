"""
Extract Assets - Legacy Compatibility Layer

이 모듈은 기존 단일 테넌트 모드와의 호환성을 위해 유지됩니다.
새로운 Extract 로직은 etl/common/assets/extract.py에 추가하세요.

멀티테넌트 환경에서는 AssetFactory를 통해 동적으로 Asset이 생성됩니다.
"""

from typing import Any

from dagster import (
    AssetExecutionContext,
    MetadataValue,
    Output,
    asset,
)

from etl.common.assets.extract import (
    get_extract_query,
)
from etl.partitions.daily import daily_partitions_def
from etl.resources.rdb import RDBResource
from etl.resources.s3 import S3Resource
from etl.utils.logging import ETLLogger

logger = ETLLogger("extract")


# =============================================================================
# 레거시 단일 테넌트용 Assets (호환성 유지)
# =============================================================================


@asset(
    partitions_def=daily_partitions_def,
    group_name="extract",
    description="공정 실적 데이터 추출 (lot_history) [Legacy]",
    compute_kind="sql",
)
def extract_lot_history(
    context: AssetExecutionContext,
    rdb: RDBResource,
    s3: S3Resource,
) -> Output[dict[str, Any]]:
    """
    Source RDB에서 lot_history 테이블 추출

    - 파티션 날짜 기준 데이터 조회
    - Parquet 형식으로 S3 저장
    """
    partition_date = context.partition_key
    job_name = "lot_history"

    logger.log_extract_start(job_name, partition_date, "lot_history")

    # 공용 쿼리 사용
    query = get_extract_query("lot_history")

    df = rdb.execute_query_with_date_filter(
        query=query,
        date_column="DATE(created_at)",
        partition_date=partition_date,
    )

    # S3에 Parquet 저장
    s3_path = s3.write_parquet(
        df=df,
        stage="extract",
        job_name=job_name,
        partition_date=partition_date,
    )

    row_count = len(df)
    logger.log_extract_complete(job_name, partition_date, row_count, s3_path)

    return Output(
        value={"s3_path": s3_path, "row_count": row_count},
        metadata={
            "row_count": MetadataValue.int(row_count),
            "s3_path": MetadataValue.path(s3_path),
            "partition_date": MetadataValue.text(partition_date),
            "columns": MetadataValue.json(df.columns.tolist()),
        },
    )


@asset(
    partitions_def=daily_partitions_def,
    group_name="extract",
    description="설비 이벤트 데이터 추출 (equipment_event) [Legacy]",
    compute_kind="sql",
)
def extract_equipment_event(
    context: AssetExecutionContext,
    rdb: RDBResource,
    s3: S3Resource,
) -> Output[dict[str, Any]]:
    """
    Source RDB에서 equipment_event 테이블 추출
    """
    partition_date = context.partition_key
    job_name = "equipment_event"

    logger.log_extract_start(job_name, partition_date, "equipment_event")

    query = get_extract_query("equipment_event")

    df = rdb.execute_query_with_date_filter(
        query=query,
        date_column="DATE(event_time)",
        partition_date=partition_date,
    )

    s3_path = s3.write_parquet(
        df=df,
        stage="extract",
        job_name=job_name,
        partition_date=partition_date,
    )

    row_count = len(df)
    logger.log_extract_complete(job_name, partition_date, row_count, s3_path)

    return Output(
        value={"s3_path": s3_path, "row_count": row_count},
        metadata={
            "row_count": MetadataValue.int(row_count),
            "s3_path": MetadataValue.path(s3_path),
            "partition_date": MetadataValue.text(partition_date),
        },
    )


@asset(
    partitions_def=daily_partitions_def,
    group_name="extract",
    description="공정 결과 데이터 추출 (process_result) [Legacy]",
    compute_kind="sql",
)
def extract_process_result(
    context: AssetExecutionContext,
    rdb: RDBResource,
    s3: S3Resource,
) -> Output[dict[str, Any]]:
    """
    Source RDB에서 process_result 테이블 추출
    """
    partition_date = context.partition_key
    job_name = "process_result"

    logger.log_extract_start(job_name, partition_date, "process_result")

    query = get_extract_query("process_result")

    df = rdb.execute_query_with_date_filter(
        query=query,
        date_column="DATE(measured_at)",
        partition_date=partition_date,
    )

    s3_path = s3.write_parquet(
        df=df,
        stage="extract",
        job_name=job_name,
        partition_date=partition_date,
    )

    row_count = len(df)
    logger.log_extract_complete(job_name, partition_date, row_count, s3_path)

    return Output(
        value={"s3_path": s3_path, "row_count": row_count},
        metadata={
            "row_count": MetadataValue.int(row_count),
            "s3_path": MetadataValue.path(s3_path),
            "partition_date": MetadataValue.text(partition_date),
        },
    )
