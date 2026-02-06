"""
Run Observability Utilities
Dagster event log 분석 및 구조화 데이터 추출

사용 예시:
    analyzer = RunAnalyzer(instance, run_id)
    failures = analyzer.get_failures()
    durations = analyzer.get_step_durations()
    df = analyzer.to_dataframe("all")
"""

import logging
from datetime import datetime, timezone
from typing import Any

import pandas as pd
from dagster import DagsterInstance
from dagster._core.events import DagsterEventType

logger = logging.getLogger(__name__)


class RunAnalyzer:
    """Dagster run event log 분석기

    완료된 run의 event log에서 구조화된 데이터를 추출:
    - Step failures (에러 타입, 메시지)
    - Asset materializations (metadata 포함)
    - Step durations (시작 → 종료 시간 차이)
    """

    def __init__(self, instance: DagsterInstance, run_id: str):
        self._instance = instance
        self._run_id = run_id

    def get_run_metadata(self) -> dict[str, Any]:
        """Run 기본 메타데이터 조회 (job_name, status, tags)"""
        dagster_run = self._instance.get_run_by_id(self._run_id)
        if not dagster_run:
            return {}
        return {
            "run_id": self._run_id,
            "job_name": dagster_run.job_name,
            "status": dagster_run.status.value,
            "tags": dict(dagster_run.tags),
        }

    def get_failures(self) -> list[dict[str, Any]]:
        """STEP_FAILURE 이벤트 추출"""
        logs = self._instance.all_logs(
            self._run_id,
            of_type={DagsterEventType.STEP_FAILURE},
        )
        failures = []
        for record in logs:
            entry = record.event_log_entry
            error_info = entry.error_info
            failures.append(
                {
                    "run_id": self._run_id,
                    "step_key": entry.step_key,
                    "timestamp": _ts_to_iso(entry.timestamp),
                    "error_type": (
                        error_info.cls_name if error_info else "Unknown"
                    ),
                    "error_message": (
                        error_info.message if error_info else entry.user_message
                    ),
                    "event_type": DagsterEventType.STEP_FAILURE.value,
                }
            )
        return failures

    def get_materializations(self) -> list[dict[str, Any]]:
        """ASSET_MATERIALIZATION 이벤트 추출 (metadata 포함)"""
        logs = self._instance.all_logs(
            self._run_id,
            of_type={DagsterEventType.ASSET_MATERIALIZATION},
        )
        materializations = []
        for record in logs:
            entry = record.event_log_entry
            event = entry.dagster_event
            if not event or not event.event_specific_data:
                continue

            mat_data = event.event_specific_data
            metadata: dict[str, str] = {}
            asset_key_str = ""

            if hasattr(mat_data, "materialization"):
                mat = mat_data.materialization
                if mat.asset_key:
                    asset_key_str = ".".join(mat.asset_key.path)
                if hasattr(mat, "metadata") and mat.metadata:
                    for key, val in mat.metadata.items():
                        metadata[key] = (
                            str(val.value) if hasattr(val, "value") else str(val)
                        )

            materializations.append(
                {
                    "run_id": self._run_id,
                    "step_key": entry.step_key,
                    "timestamp": _ts_to_iso(entry.timestamp),
                    "asset_key": asset_key_str,
                    "event_type": DagsterEventType.ASSET_MATERIALIZATION.value,
                    **metadata,
                }
            )
        return materializations

    def get_step_durations(self) -> list[dict[str, Any]]:
        """Step 실행 시간 계산 (STEP_START → STEP_SUCCESS/FAILURE)"""
        event_types = {
            DagsterEventType.STEP_START,
            DagsterEventType.STEP_SUCCESS,
            DagsterEventType.STEP_FAILURE,
        }
        logs = self._instance.all_logs(self._run_id, of_type=event_types)

        starts: dict[str, float] = {}
        durations: list[dict[str, Any]] = []

        for record in logs:
            entry = record.event_log_entry
            event = entry.dagster_event
            if not event or not entry.step_key:
                continue

            step_key = entry.step_key

            if event.event_type == DagsterEventType.STEP_START:
                starts[step_key] = entry.timestamp
            elif event.event_type in (
                DagsterEventType.STEP_SUCCESS,
                DagsterEventType.STEP_FAILURE,
            ):
                start_ts = starts.get(step_key)
                duration_sec = (
                    round(entry.timestamp - start_ts, 3) if start_ts else None
                )
                durations.append(
                    {
                        "run_id": self._run_id,
                        "step_key": step_key,
                        "start_timestamp": (
                            _ts_to_iso(start_ts) if start_ts else None
                        ),
                        "end_timestamp": _ts_to_iso(entry.timestamp),
                        "duration_sec": duration_sec,
                        "status": (
                            "success"
                            if event.event_type == DagsterEventType.STEP_SUCCESS
                            else "failure"
                        ),
                    }
                )
        return durations

    def to_dataframe(self, event_kind: str = "all") -> pd.DataFrame:
        """분석 결과를 DataFrame으로 변환

        Args:
            event_kind: "failures", "materializations", "durations", "all"
        """
        if event_kind == "failures":
            return pd.DataFrame(self.get_failures())
        elif event_kind == "materializations":
            return pd.DataFrame(self.get_materializations())
        elif event_kind == "durations":
            return pd.DataFrame(self.get_step_durations())
        else:
            rows: list[dict[str, Any]] = []
            for f in self.get_failures():
                f["category"] = "failure"
                rows.append(f)
            for m in self.get_materializations():
                m["category"] = "materialization"
                rows.append(m)
            for d in self.get_step_durations():
                d["category"] = "duration"
                rows.append(d)
            return pd.DataFrame(rows)


def _ts_to_iso(timestamp: float) -> str:
    """Unix timestamp → ISO 8601 문자열"""
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()
