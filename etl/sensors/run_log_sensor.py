"""
Run Log Export Sensor
Run 완료 시 event log를 S3에 Parquet로 내보내는 sensor

S3 경로: project_id={tenant_id}/logs/run_events/date={YYYYMMDD}/{run_id}.parquet

Dagster Event Log → Python Collector → S3 (Parquet) → Trino
"""

import io
import logging
import os
from datetime import datetime, timezone

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dagster import (
    DagsterRunStatus,
    RunStatusSensorContext,
    RunStatusSensorDefinition,
    run_status_sensor,
)

from etl.utils.observability import RunAnalyzer

logger = logging.getLogger(__name__)


def _get_s3_client():
    """환경변수에서 standalone S3 클라이언트 생성

    Sensor는 daemon 프로세스에서 실행되므로
    Dagster Resource 대신 환경변수로 직접 생성합니다.
    """
    import boto3

    kwargs: dict = {
        "service_name": "s3",
        "region_name": os.getenv("AWS_REGION", "ap-northeast-2"),
    }
    access_key = os.getenv("AWS_ACCESS_KEY_ID", "")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "")
    endpoint_url = os.getenv("S3_ENDPOINT_URL", "")

    if access_key and secret_key:
        kwargs["aws_access_key_id"] = access_key
        kwargs["aws_secret_access_key"] = secret_key
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url

    return boto3.client(**kwargs)


def _write_parquet_to_s3(df: pd.DataFrame, bucket: str, s3_key: str) -> str:
    """DataFrame을 S3에 Parquet으로 저장"""
    client = _get_s3_client()
    buffer = io.BytesIO()
    table = pa.Table.from_pandas(df)
    pq.write_table(table, buffer, compression="snappy")
    buffer.seek(0)

    client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=buffer.getvalue(),
    )
    return f"s3://{bucket}/{s3_key}"


def _build_run_log_sensor(
    status: DagsterRunStatus,
    name_suffix: str,
) -> RunStatusSensorDefinition:
    """Run status sensor 팩토리"""

    @run_status_sensor(
        run_status=status,
        name=f"etl_run_log_{name_suffix}_sensor",
        description=f"Run {name_suffix} 시 event log를 S3에 Parquet으로 export",
        minimum_interval_seconds=30,
    )
    def _sensor(context: RunStatusSensorContext) -> None:
        dagster_run = context.dagster_run
        run_id = dagster_run.run_id
        job_name = dagster_run.job_name
        tags = dict(dagster_run.tags)
        tenant_id = tags.get("tenant_id", "unknown")
        instance = context.instance

        logger.info(
            "Exporting run log: run_id=%s, job=%s, tenant=%s, status=%s",
            run_id,
            job_name,
            tenant_id,
            status.value,
        )

        # Event log 분석
        analyzer = RunAnalyzer(instance, run_id)
        df = analyzer.to_dataframe("all")

        if df.empty:
            logger.info("No events to export for run %s", run_id)
            return

        # 보강 컬럼 추가
        df["job_name"] = job_name
        df["tenant_id"] = tenant_id
        df["run_status"] = status.value
        df["partition_key"] = tags.get("dagster/partition", "")

        # S3 경로 생성
        today = datetime.now(tz=timezone.utc).strftime("%Y%m%d")
        bucket = os.getenv("S3_BUCKET_NAME", "data-lake")
        s3_key = (
            f"project_id={tenant_id}/logs/run_events"
            f"/date={today}/{run_id}.parquet"
        )

        try:
            s3_path = _write_parquet_to_s3(df, bucket, s3_key)
            logger.info("Exported run log: %s (%d rows)", s3_path, len(df))
        except Exception:
            logger.exception("Failed to export run log for run %s", run_id)

    return _sensor


# Sensor 인스턴스
run_success_log_sensor = _build_run_log_sensor(
    DagsterRunStatus.SUCCESS, "success"
)
run_failure_log_sensor = _build_run_log_sensor(
    DagsterRunStatus.FAILURE, "failure"
)
