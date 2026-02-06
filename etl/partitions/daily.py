"""
Daily Partition Definition
모든 Asset에서 공통으로 사용하는 일별 파티션
"""

from datetime import datetime

from dagster import DailyPartitionsDefinition


# Daily Partition 정의 (YYYY-MM-DD 형식)
daily_partitions_def = DailyPartitionsDefinition(
    start_date="2024-01-01",  # 시작일 (프로젝트에 맞게 조정)
    timezone="Asia/Seoul",
    fmt="%Y-%m-%d",
    end_offset=1,  # 오늘 날짜까지 파티션 생성
)


def get_partition_date_str(partition_key: str) -> str:
    """
    파티션 키를 문자열로 반환

    Args:
        partition_key: Dagster 파티션 키 (YYYY-MM-DD)

    Returns:
        YYYY-MM-DD 형식 문자열
    """
    return partition_key


def get_partition_date_formatted(partition_key: str, fmt: str = "%Y%m%d") -> str:
    """
    파티션 키를 지정 포맷으로 변환

    Args:
        partition_key: Dagster 파티션 키 (YYYY-MM-DD)
        fmt: 출력 포맷

    Returns:
        포맷팅된 날짜 문자열
    """
    dt = datetime.strptime(partition_key, "%Y-%m-%d")
    return dt.strftime(fmt)


def parse_partition_key(partition_key: str) -> datetime:
    """
    파티션 키를 datetime 객체로 변환

    Args:
        partition_key: Dagster 파티션 키 (YYYY-MM-DD)

    Returns:
        datetime 객체
    """
    return datetime.strptime(partition_key, "%Y-%m-%d")
