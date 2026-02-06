"""
Common Transform Logic - 공용 변환 로직
테넌트별 커스텀이 필요한 경우 tenants/{tenant}/assets/transform.py에서 오버라이드
"""

import pandas as pd

from etl.utils.logging import ETLLogger
from etl.utils.validation import DataValidator


logger = ETLLogger("common.transform")


def transform_aps_wip_logic(
    input_dfs: dict[str, pd.DataFrame],
    partition_date: str,
    tenant_id: str = "default",
) -> pd.DataFrame:
    """
    APS 입력용 WIP 데이터 생성 로직 (공용)

    Args:
        input_dfs: {"lot_history": DataFrame} 입력 데이터
        partition_date: 파티션 날짜 (YYYY-MM-DD)
        tenant_id: 테넌트 ID (로깅용)

    Returns:
        WIP 집계 DataFrame
    """
    df = input_dfs["lot_history"]

    # 1. Hold/Scrap Lot 필터링
    df_active = df[df["status"].isin(["IN_PROGRESS", "COMPLETED"])].copy()

    # 2. 공정별 WIP 집계
    wip_df = (
        df_active.groupby(["process_step", "product_code"])
        .agg(
            wip_qty=("quantity", "sum"),
            lot_count=("lot_id", "nunique"),
            avg_qty_per_lot=("quantity", "mean"),
        )
        .reset_index()
    )

    # 3. 메타데이터 추가
    wip_df["snapshot_date"] = partition_date

    # 4. 데이터 검증
    validator = DataValidator(wip_df)
    validation = (
        validator.check_not_null("process_step")
        .check_not_null("wip_qty")
        .check_range("wip_qty", min_val=0)
        .validate()
    )

    if not validation.passed:
        logger.warning(
            f"[{tenant_id}] WIP validation warnings",
            partition_date=partition_date,
            failed_rules=len(validation.failed_rules),
        )

    return wip_df


def transform_cycle_time_logic(
    input_dfs: dict[str, pd.DataFrame],
    partition_date: str,
    tenant_id: str = "default",
) -> pd.DataFrame:
    """
    Cycle Time 산출 로직 (공용)

    Args:
        input_dfs: {"lot_history": DataFrame, "process_result": DataFrame} 입력 데이터
        partition_date: 파티션 날짜 (YYYY-MM-DD)
        tenant_id: 테넌트 ID (로깅용)

    Returns:
        Cycle Time 집계 DataFrame
    """
    lot_df = input_dfs["lot_history"]

    # 1. 완료된 Lot만 필터
    completed_lots = lot_df[lot_df["status"] == "COMPLETED"].copy()

    # 2. Cycle Time 계산 (end_time - start_time)
    completed_lots["start_time"] = pd.to_datetime(completed_lots["start_time"])
    completed_lots["end_time"] = pd.to_datetime(completed_lots["end_time"])
    completed_lots["cycle_time_minutes"] = (
        (completed_lots["end_time"] - completed_lots["start_time"]).dt.total_seconds()
        / 60
    )

    # 3. 공정/제품별 평균 Cycle Time
    cycle_time_df = (
        completed_lots.groupby(["process_step", "product_code"])
        .agg(
            avg_cycle_time=("cycle_time_minutes", "mean"),
            min_cycle_time=("cycle_time_minutes", "min"),
            max_cycle_time=("cycle_time_minutes", "max"),
            lot_count=("lot_id", "count"),
        )
        .reset_index()
    )

    # 4. 메타데이터 추가
    cycle_time_df["snapshot_date"] = partition_date

    # 5. 검증
    validator = DataValidator(cycle_time_df)
    validation = (
        validator.check_not_null("avg_cycle_time")
        .check_range("avg_cycle_time", min_val=0)
        .validate()
    )

    if not validation.passed:
        logger.warning(
            f"[{tenant_id}] Cycle time validation warnings",
            partition_date=partition_date,
            failed_rules=len(validation.failed_rules),
        )

    return cycle_time_df


def transform_equipment_utilization_logic(
    input_dfs: dict[str, pd.DataFrame],
    partition_date: str,
    tenant_id: str = "default",
) -> pd.DataFrame:
    """
    설비 가동률 산출 로직 (공용)

    Args:
        input_dfs: {"equipment_event": DataFrame} 입력 데이터
        partition_date: 파티션 날짜 (YYYY-MM-DD)
        tenant_id: 테넌트 ID (로깅용)

    Returns:
        설비 가동률 DataFrame
    """
    df = input_dfs["equipment_event"]

    # 1. 이벤트 타입별 시간 집계
    event_summary = (
        df.groupby(["equipment_id", "event_type"])
        .agg(total_duration=("duration_minutes", "sum"))
        .reset_index()
    )

    # 2. 피벗하여 이벤트 타입별 컬럼 생성
    pivot_df = event_summary.pivot(
        index="equipment_id", columns="event_type", values="total_duration"
    ).fillna(0)

    pivot_df = pivot_df.reset_index()

    # 3. 가동률 계산 (24시간 기준)
    total_minutes_per_day = 24 * 60

    if "RUN" in pivot_df.columns:
        pivot_df["utilization_rate"] = (
            pivot_df["RUN"] / total_minutes_per_day * 100
        ).round(2)
    else:
        pivot_df["utilization_rate"] = 0.0

    # 4. 메타데이터 추가
    pivot_df["snapshot_date"] = partition_date

    return pivot_df
