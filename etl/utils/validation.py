"""
Data Validation Utilities
"""

from dataclasses import dataclass, field
from typing import Any, Callable

import pandas as pd


@dataclass
class ValidationResult:
    """검증 결과"""

    is_valid: bool
    rule_name: str
    column: str | None = None
    message: str = ""
    failed_count: int = 0
    failed_samples: list[Any] = field(default_factory=list)


@dataclass
class ValidationReport:
    """전체 검증 리포트"""

    total_rows: int
    passed: bool
    results: list[ValidationResult] = field(default_factory=list)

    @property
    def failed_rules(self) -> list[ValidationResult]:
        return [r for r in self.results if not r.is_valid]

    @property
    def summary(self) -> dict[str, Any]:
        return {
            "total_rows": self.total_rows,
            "passed": self.passed,
            "total_rules": len(self.results),
            "passed_rules": len([r for r in self.results if r.is_valid]),
            "failed_rules": len(self.failed_rules),
        }


class DataValidator:
    """데이터 검증 클래스"""

    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.results: list[ValidationResult] = []

    def check_not_null(self, column: str) -> "DataValidator":
        """Null 값 체크"""
        null_count = self.df[column].isna().sum()
        is_valid = null_count == 0

        self.results.append(
            ValidationResult(
                is_valid=is_valid,
                rule_name="not_null",
                column=column,
                message=f"Found {null_count} null values" if not is_valid else "",
                failed_count=int(null_count),
            )
        )
        return self

    def check_unique(self, column: str) -> "DataValidator":
        """고유값 체크"""
        duplicate_count = self.df[column].duplicated().sum()
        is_valid = duplicate_count == 0

        self.results.append(
            ValidationResult(
                is_valid=is_valid,
                rule_name="unique",
                column=column,
                message=f"Found {duplicate_count} duplicates" if not is_valid else "",
                failed_count=int(duplicate_count),
            )
        )
        return self

    def check_range(
        self, column: str, min_val: float | None = None, max_val: float | None = None
    ) -> "DataValidator":
        """범위 체크"""
        col_data = self.df[column].dropna()
        failed_mask = pd.Series([False] * len(col_data), index=col_data.index)

        if min_val is not None:
            failed_mask |= col_data < min_val
        if max_val is not None:
            failed_mask |= col_data > max_val

        failed_count = failed_mask.sum()
        is_valid = failed_count == 0

        self.results.append(
            ValidationResult(
                is_valid=is_valid,
                rule_name="range",
                column=column,
                message=f"Found {failed_count} out of range values" if not is_valid else "",
                failed_count=int(failed_count),
                failed_samples=col_data[failed_mask].head(5).tolist(),
            )
        )
        return self

    def check_type(self, column: str, expected_type: str) -> "DataValidator":
        """타입 체크"""
        type_mapping = {
            "int": ["int64", "int32", "Int64", "Int32"],
            "float": ["float64", "float32", "Float64", "Float32"],
            "string": ["object", "string", "str"],
            "datetime": ["datetime64[ns]", "datetime64[ns, UTC]"],
            "bool": ["bool", "boolean"],
        }

        actual_type = str(self.df[column].dtype)
        expected_types = type_mapping.get(expected_type, [expected_type])
        is_valid = actual_type in expected_types

        self.results.append(
            ValidationResult(
                is_valid=is_valid,
                rule_name="type",
                column=column,
                message=f"Expected {expected_type}, got {actual_type}" if not is_valid else "",
            )
        )
        return self

    def check_values_in(self, column: str, allowed_values: list[Any]) -> "DataValidator":
        """허용 값 체크"""
        invalid_mask = ~self.df[column].isin(allowed_values) & self.df[column].notna()
        failed_count = invalid_mask.sum()
        is_valid = failed_count == 0

        self.results.append(
            ValidationResult(
                is_valid=is_valid,
                rule_name="values_in",
                column=column,
                message=f"Found {failed_count} invalid values" if not is_valid else "",
                failed_count=int(failed_count),
                failed_samples=self.df.loc[invalid_mask, column].unique()[:5].tolist(),
            )
        )
        return self

    def check_regex(self, column: str, pattern: str) -> "DataValidator":
        """정규식 패턴 체크"""
        str_col = self.df[column].astype(str)
        match_mask = str_col.str.match(pattern, na=False)
        failed_mask = ~match_mask & self.df[column].notna()
        failed_count = failed_mask.sum()
        is_valid = failed_count == 0

        self.results.append(
            ValidationResult(
                is_valid=is_valid,
                rule_name="regex",
                column=column,
                message=f"Found {failed_count} values not matching pattern" if not is_valid else "",
                failed_count=int(failed_count),
                failed_samples=self.df.loc[failed_mask, column].head(5).tolist(),
            )
        )
        return self

    def check_custom(
        self,
        rule_name: str,
        check_fn: Callable[[pd.DataFrame], tuple[bool, str]],
    ) -> "DataValidator":
        """커스텀 검증 함수"""
        is_valid, message = check_fn(self.df)

        self.results.append(
            ValidationResult(
                is_valid=is_valid,
                rule_name=rule_name,
                message=message if not is_valid else "",
            )
        )
        return self

    def check_row_count(
        self, min_count: int | None = None, max_count: int | None = None
    ) -> "DataValidator":
        """행 수 체크"""
        row_count = len(self.df)
        is_valid = True
        message = ""

        if min_count is not None and row_count < min_count:
            is_valid = False
            message = f"Row count {row_count} is less than minimum {min_count}"
        if max_count is not None and row_count > max_count:
            is_valid = False
            message = f"Row count {row_count} exceeds maximum {max_count}"

        self.results.append(
            ValidationResult(
                is_valid=is_valid,
                rule_name="row_count",
                message=message,
            )
        )
        return self

    def validate(self, fail_fast: bool = False) -> ValidationReport:
        """
        전체 검증 실행

        Args:
            fail_fast: True면 첫 실패 시 중단

        Returns:
            ValidationReport
        """
        passed = all(r.is_valid for r in self.results)

        return ValidationReport(
            total_rows=len(self.df),
            passed=passed,
            results=self.results,
        )

    def reset(self) -> "DataValidator":
        """검증 결과 초기화"""
        self.results = []
        return self
