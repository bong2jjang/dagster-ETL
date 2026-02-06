"""
Template Tenant - Custom Transfer Logic

이 파일을 복사하여 테넌트별 커스텀 Transfer 로직을 구현하세요.

사용법:
1. 이 파일을 tenants/{tenant_id}/assets/transfer.py로 복사
2. 필요한 함수만 구현 (나머지는 common 사용)
3. __init__.py의 CUSTOM_TRANSFER_FUNCTIONS에 등록

참고:
- 함수 시그니처는 common/assets/transfer.py와 동일하게 유지
- 입력: input_dfs (dict[str, DataFrame]), partition_date (str), tenant_id (str)
- 출력: DataFrame
"""

import pandas as pd

from etl.utils.logging import ETLLogger
from etl.utils.validation import DataValidator


logger = ETLLogger("template.transfer")


def transform_aps_wip_logic(
    input_dfs: dict[str, pd.DataFrame],
    partition_date: str,
    tenant_id: str = "template",
) -> pd.DataFrame:
    """
    커스텀 WIP Transfer 로직 예시

    common/assets/transfer.py의 transform_aps_wip_logic을 기반으로
    테넌트별 특수 로직을 추가합니다.

    변경점 예시:
    - 추가 컬럼 집계
    - 필터링 조건 변경
    - 비즈니스 로직 커스터마이징
    """
    df = input_dfs["lot_history"]

    logger.info(f"[{tenant_id}] Using custom WIP transfer logic")

    # 1. Hold/Scrap Lot 필터링 (기본 로직)
    df_active = df[df["status"].isin(["IN_PROGRESS", "COMPLETED"])].copy()

    # 2. 공정별 WIP 집계 (기본 로직)
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


# =============================================================================
# 아래는 필요시 구현하세요 (구현하지 않으면 common 로직 사용)
# =============================================================================

# def transform_cycle_time_logic(
#     input_dfs: dict[str, pd.DataFrame],
#     partition_date: str,
#     tenant_id: str = "template",
# ) -> pd.DataFrame:
#     """커스텀 Cycle Time 로직"""
#     pass


# def transform_equipment_utilization_logic(
#     input_dfs: dict[str, pd.DataFrame],
#     partition_date: str,
#     tenant_id: str = "template",
# ) -> pd.DataFrame:
#     """커스텀 설비 가동률 로직"""
#     pass
