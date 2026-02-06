"""
Load Assets - Legacy Compatibility Layer

이 모듈은 기존 단일 테넌트 모드와의 호환성을 위해 유지됩니다.
새로운 Load 로직은 etl/common/assets/load.py에 추가하세요.

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

from etl.common.assets.load import get_load_config
from etl.partitions.daily import daily_partitions_def
from etl.resources.s3 import S3Resource
from etl.resources.trino import TrinoResource
from etl.utils.logging import ETLLogger

logger = ETLLogger("load")


# =============================================================================
# 레거시 단일 테넌트용 Assets (호환성 유지)
# =============================================================================


@asset(
    partitions_def=daily_partitions_def,
    ins={"transform_aps_wip": AssetIn(key="transform_aps_wip")},
    group_name="load",
    description="APS WIP 데이터 PostgreSQL 적재 [Legacy]",
    compute_kind="trino",
)
def load_aps_wip(
    context: AssetExecutionContext,
    s3: S3Resource,
    trino: TrinoResource,
    transform_aps_wip: dict[str, Any],
) -> Output[dict[str, Any]]:
    """
    transform_aps_wip 결과를 Trino를 통해 PostgreSQL에 적재
    """
    partition_date = context.partition_key
    job_name = "aps_wip"
    input_path = transform_aps_wip["s3_path"]

    # 공용 설정 사용
    load_config = get_load_config("aps_wip")
    target_table = load_config["target_table"]
    target_schema = load_config["target_schema"]
    key_columns = load_config["key_columns"]

    logger.log_load_start(job_name, partition_date, target_table)

    # Transform 결과 로드
    df = s3.read_parquet_from_path(input_path)

    # Trino를 통해 PostgreSQL에 적재 (Upsert)
    result = trino.upsert_dataframe(
        df=df,
        target_catalog="postgresql",
        target_schema=target_schema,
        target_table=target_table,
        key_columns=key_columns,
        batch_size=1000,
    )

    row_count = result["inserted"]
    logger.log_load_complete(job_name, partition_date, row_count, target_table)

    return Output(
        value={
            "target_table": f"postgresql.{target_schema}.{target_table}",
            "row_count": row_count,
            "deleted": result["deleted"],
            "inserted": result["inserted"],
        },
        metadata={
            "target_table": MetadataValue.text(
                f"postgresql.{target_schema}.{target_table}"
            ),
            "deleted_rows": MetadataValue.int(result["deleted"]),
            "inserted_rows": MetadataValue.int(result["inserted"]),
            "partition_date": MetadataValue.text(partition_date),
        },
    )


@asset(
    partitions_def=daily_partitions_def,
    ins={"transform_cycle_time": AssetIn(key="transform_cycle_time")},
    group_name="load",
    description="Cycle Time 데이터 PostgreSQL 적재 [Legacy]",
    compute_kind="trino",
)
def load_cycle_time(
    context: AssetExecutionContext,
    s3: S3Resource,
    trino: TrinoResource,
    transform_cycle_time: dict[str, Any],
) -> Output[dict[str, Any]]:
    """
    transform_cycle_time 결과를 PostgreSQL에 적재
    """
    partition_date = context.partition_key
    job_name = "cycle_time"
    input_path = transform_cycle_time["s3_path"]

    # 공용 설정 사용
    load_config = get_load_config("cycle_time")
    target_table = load_config["target_table"]
    target_schema = load_config["target_schema"]
    key_columns = load_config["key_columns"]

    logger.log_load_start(job_name, partition_date, target_table)

    df = s3.read_parquet_from_path(input_path)

    result = trino.upsert_dataframe(
        df=df,
        target_catalog="postgresql",
        target_schema=target_schema,
        target_table=target_table,
        key_columns=key_columns,
        batch_size=1000,
    )

    row_count = result["inserted"]
    logger.log_load_complete(job_name, partition_date, row_count, target_table)

    return Output(
        value={
            "target_table": f"postgresql.{target_schema}.{target_table}",
            "row_count": row_count,
        },
        metadata={
            "target_table": MetadataValue.text(
                f"postgresql.{target_schema}.{target_table}"
            ),
            "deleted_rows": MetadataValue.int(result["deleted"]),
            "inserted_rows": MetadataValue.int(result["inserted"]),
            "partition_date": MetadataValue.text(partition_date),
        },
    )


@asset(
    partitions_def=daily_partitions_def,
    ins={
        "transform_equipment_utilization": AssetIn(
            key="transform_equipment_utilization"
        )
    },
    group_name="load",
    description="설비 가동률 데이터 PostgreSQL 적재 [Legacy]",
    compute_kind="trino",
)
def load_equipment_utilization(
    context: AssetExecutionContext,
    s3: S3Resource,
    trino: TrinoResource,
    transform_equipment_utilization: dict[str, Any],
) -> Output[dict[str, Any]]:
    """
    transform_equipment_utilization 결과를 PostgreSQL에 적재
    """
    partition_date = context.partition_key
    job_name = "equipment_utilization"
    input_path = transform_equipment_utilization["s3_path"]

    # 공용 설정 사용
    load_config = get_load_config("equipment_utilization")
    target_table = load_config["target_table"]
    target_schema = load_config["target_schema"]
    key_columns = load_config["key_columns"]

    logger.log_load_start(job_name, partition_date, target_table)

    df = s3.read_parquet_from_path(input_path)

    result = trino.upsert_dataframe(
        df=df,
        target_catalog="postgresql",
        target_schema=target_schema,
        target_table=target_table,
        key_columns=key_columns,
        batch_size=1000,
    )

    row_count = result["inserted"]
    logger.log_load_complete(job_name, partition_date, row_count, target_table)

    return Output(
        value={
            "target_table": f"postgresql.{target_schema}.{target_table}",
            "row_count": row_count,
        },
        metadata={
            "target_table": MetadataValue.text(
                f"postgresql.{target_schema}.{target_table}"
            ),
            "deleted_rows": MetadataValue.int(result["deleted"]),
            "inserted_rows": MetadataValue.int(result["inserted"]),
            "partition_date": MetadataValue.text(partition_date),
        },
    )
