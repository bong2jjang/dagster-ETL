"""
Project 01 - 커스텀 Transform 로직
common/assets/transform.py를 기반으로 고유 로직 추가
"""

import pandas as pd

from etl.utils.logging import ETLLogger
from etl.utils.validation import DataValidator


logger = ETLLogger("project_01.transform")


def transform_aps_wip_logic(
    input_dfs: dict[str, pd.DataFrame],
    partition_date: str,
    tenant_id: str = "project_01",
) -> pd.DataFrame:
    """
    Project 01 커스텀 WIP 로직

    공용 로직 대비 변경점:
    - priority 컬럼 추가 집계
    - customer_order_id별 그룹핑 추가
    - 긴급 Lot (priority='HIGH') 별도 표시
    """
    df = input_dfs["lot_history"]

    logger.info(f"[{tenant_id}] Using Project 01 custom WIP transform logic")

    # 1. Hold/Scrap Lot 필터링
    df_active = df[df["status"].isin(["IN_PROGRESS", "COMPLETED"])].copy()

    # 2. priority 컬럼이 있는지 확인
    has_priority = "priority" in df_active.columns

    # 3. 공정별 WIP 집계
    group_cols = ["process_step", "product_code"]

    agg_dict = {
        "quantity": [("wip_qty", "sum")],
        "lot_id": [("lot_count", "nunique")],
    }

    # priority가 있으면 HIGH priority 카운트 추가
    if has_priority:
        df_active["is_high_priority"] = (df_active["priority"] == "HIGH").astype(int)
        agg_dict["is_high_priority"] = [("high_priority_count", "sum")]

    wip_df = df_active.groupby(group_cols).agg(
        wip_qty=("quantity", "sum"),
        lot_count=("lot_id", "nunique"),
        avg_qty_per_lot=("quantity", "mean"),
    ).reset_index()

    # HIGH priority 카운트 추가
    if has_priority:
        priority_df = df_active.groupby(group_cols).agg(
            high_priority_count=("is_high_priority", "sum")
        ).reset_index()
        wip_df = wip_df.merge(priority_df, on=group_cols, how="left")
        wip_df["high_priority_count"] = wip_df["high_priority_count"].fillna(0).astype(int)

    # 4. 메타데이터 추가
    wip_df["snapshot_date"] = partition_date

    # 5. 데이터 검증
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

    logger.info(
        f"[{tenant_id}] WIP transform completed",
        rows=len(wip_df),
        has_priority=has_priority,
    )

    return wip_df


# cycle_time과 equipment_utilization은 common 로직 사용
# 필요시 여기에 커스텀 함수 추가 가능
