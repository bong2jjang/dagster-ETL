"""
ETL Hooks - Step-level success/failure 로깅 hooks

Job에 연결하여 각 step 완료/실패 시 구조화된 로그를 기록합니다.

사용법:
    define_asset_job(..., hooks={etl_success_hook, etl_failure_hook})
"""

import logging
from datetime import datetime, timezone

from dagster import HookContext, failure_hook, success_hook
from dagster._core.events import DagsterEventType

logger = logging.getLogger(__name__)


@success_hook
def etl_success_hook(context: HookContext) -> None:
    """Step 성공 시 duration과 함께 구조화된 로그 기록"""
    run_id = context.run_id
    step_key = context.step_key
    job_name = context.job_name
    instance = context.instance

    duration_sec = _get_step_duration(instance, run_id, step_key)

    dagster_run = instance.get_run_by_id(run_id)
    tenant_id = dagster_run.tags.get("tenant_id", "unknown") if dagster_run else "unknown"
    partition_key = (
        dagster_run.tags.get("dagster/partition", "") if dagster_run else ""
    )

    context.log.info(
        "Step succeeded: %s (%.3fs)",
        step_key,
        duration_sec or 0,
        extra={
            "tenant_id": tenant_id,
            "job_name": job_name,
            "step_key": step_key,
            "duration_sec": duration_sec,
            "partition_key": partition_key,
            "status": "success",
        },
    )


@failure_hook
def etl_failure_hook(context: HookContext) -> None:
    """Step 실패 시 에러 정보와 함께 구조화된 로그 기록"""
    run_id = context.run_id
    step_key = context.step_key
    job_name = context.job_name
    instance = context.instance

    duration_sec = _get_step_duration(instance, run_id, step_key)

    dagster_run = instance.get_run_by_id(run_id)
    tenant_id = dagster_run.tags.get("tenant_id", "unknown") if dagster_run else "unknown"
    partition_key = (
        dagster_run.tags.get("dagster/partition", "") if dagster_run else ""
    )

    # 실패 이벤트에서 에러 메시지 추출
    error_message = _get_step_error(instance, run_id, step_key)

    context.log.error(
        "Step failed: %s (%s)",
        step_key,
        error_message[:200] if error_message else "unknown error",
        extra={
            "tenant_id": tenant_id,
            "job_name": job_name,
            "step_key": step_key,
            "duration_sec": duration_sec,
            "partition_key": partition_key,
            "status": "failure",
            "error_message": error_message,
        },
    )


def _get_step_duration(
    instance: object, run_id: str, step_key: str
) -> float | None:
    """STEP_START 이벤트로부터 step 실행 시간 계산"""
    start_logs = list(
        instance.all_logs(run_id, of_type={DagsterEventType.STEP_START})
    )
    for record in start_logs:
        entry = record.event_log_entry
        if entry.step_key == step_key:
            now = datetime.now(tz=timezone.utc).timestamp()
            return round(now - entry.timestamp, 3)
    return None


def _get_step_error(instance: object, run_id: str, step_key: str) -> str:
    """STEP_FAILURE 이벤트에서 에러 메시지 추출"""
    failure_logs = list(
        instance.all_logs(run_id, of_type={DagsterEventType.STEP_FAILURE})
    )
    for record in failure_logs:
        entry = record.event_log_entry
        if entry.step_key == step_key and entry.error_info:
            return entry.error_info.message[:500]
    return ""
