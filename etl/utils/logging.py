"""
ETL Logging Utilities
"""

import logging
import sys
from typing import Any

from dagster import get_dagster_logger


class ETLLogger:
    """ETL 로깅 유틸리티"""

    def __init__(self, name: str = "etl"):
        self._name = name
        self._dagster_logger = None
        self._fallback_logger = None

    @property
    def logger(self) -> logging.Logger:
        """Dagster 로거 또는 폴백 로거 반환"""
        try:
            if self._dagster_logger is None:
                self._dagster_logger = get_dagster_logger()
            return self._dagster_logger
        except Exception:
            # Dagster 컨텍스트 외부에서 실행 시 폴백
            if self._fallback_logger is None:
                self._fallback_logger = self._create_fallback_logger()
            return self._fallback_logger

    def _create_fallback_logger(self) -> logging.Logger:
        """폴백 로거 생성"""
        logger = logging.getLogger(self._name)
        if not logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            handler.setFormatter(
                logging.Formatter(
                    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
                )
            )
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger

    def info(self, message: str, **kwargs: Any) -> None:
        """INFO 레벨 로그"""
        self.logger.info(self._format_message(message, kwargs))

    def warning(self, message: str, **kwargs: Any) -> None:
        """WARNING 레벨 로그"""
        self.logger.warning(self._format_message(message, kwargs))

    def error(self, message: str, **kwargs: Any) -> None:
        """ERROR 레벨 로그"""
        self.logger.error(self._format_message(message, kwargs))

    def debug(self, message: str, **kwargs: Any) -> None:
        """DEBUG 레벨 로그"""
        self.logger.debug(self._format_message(message, kwargs))

    def _format_message(self, message: str, context: dict[str, Any]) -> str:
        """컨텍스트 정보를 포함한 메시지 포맷팅"""
        if not context:
            return message
        context_str = " | ".join(f"{k}={v}" for k, v in context.items())
        return f"{message} | {context_str}"

    def log_extract_start(
        self, job_name: str, partition_date: str, source_table: str
    ) -> None:
        """Extract 시작 로그"""
        self.info(
            "Extract started",
            job_name=job_name,
            partition_date=partition_date,
            source_table=source_table,
        )

    def log_extract_complete(
        self, job_name: str, partition_date: str, row_count: int, s3_path: str
    ) -> None:
        """Extract 완료 로그"""
        self.info(
            "Extract completed",
            job_name=job_name,
            partition_date=partition_date,
            row_count=row_count,
            s3_path=s3_path,
        )

    def log_transform_start(
        self, job_name: str, partition_date: str, input_path: str
    ) -> None:
        """Transform 시작 로그"""
        self.info(
            "Transform started",
            job_name=job_name,
            partition_date=partition_date,
            input_path=input_path,
        )

    def log_transform_complete(
        self,
        job_name: str,
        partition_date: str,
        input_rows: int,
        output_rows: int,
        s3_path: str,
    ) -> None:
        """Transform 완료 로그"""
        self.info(
            "Transform completed",
            job_name=job_name,
            partition_date=partition_date,
            input_rows=input_rows,
            output_rows=output_rows,
            s3_path=s3_path,
        )

    def log_load_start(
        self, job_name: str, partition_date: str, target_table: str
    ) -> None:
        """Load 시작 로그"""
        self.info(
            "Load started",
            job_name=job_name,
            partition_date=partition_date,
            target_table=target_table,
        )

    def log_load_complete(
        self, job_name: str, partition_date: str, row_count: int, target_table: str
    ) -> None:
        """Load 완료 로그"""
        self.info(
            "Load completed",
            job_name=job_name,
            partition_date=partition_date,
            row_count=row_count,
            target_table=target_table,
        )

    def log_validation_result(
        self, job_name: str, partition_date: str, passed: bool, summary: dict[str, Any]
    ) -> None:
        """검증 결과 로그"""
        level = "info" if passed else "warning"
        getattr(self, level)(
            "Validation completed",
            job_name=job_name,
            partition_date=partition_date,
            passed=passed,
            **summary,
        )

    def log_error(
        self,
        job_name: str,
        partition_date: str,
        stage: str,
        error: Exception,
    ) -> None:
        """에러 로그"""
        self.error(
            f"ETL error in {stage}",
            job_name=job_name,
            partition_date=partition_date,
            error_type=type(error).__name__,
            error_message=str(error),
        )


# 전역 로거 인스턴스
etl_logger = ETLLogger()
