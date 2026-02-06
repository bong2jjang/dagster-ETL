"""
Parquet Utilities - Schema 관리 및 변환 유틸리티
"""

from typing import Any

import pandas as pd
import pyarrow as pa


class ParquetUtils:
    """Parquet 관련 유틸리티 클래스"""

    # 공통 데이터 타입 매핑 (Pandas → PyArrow)
    DTYPE_MAPPING: dict[str, pa.DataType] = {
        "int64": pa.int64(),
        "int32": pa.int32(),
        "float64": pa.float64(),
        "float32": pa.float32(),
        "object": pa.string(),
        "string": pa.string(),
        "bool": pa.bool_(),
        "datetime64[ns]": pa.timestamp("ns"),
        "datetime64[ns, UTC]": pa.timestamp("ns", tz="UTC"),
        "datetime64[ns, Asia/Seoul]": pa.timestamp("ns", tz="Asia/Seoul"),
    }

    @classmethod
    def infer_schema(cls, df: pd.DataFrame) -> pa.Schema:
        """
        DataFrame에서 PyArrow 스키마 추론

        Args:
            df: 소스 DataFrame

        Returns:
            pa.Schema: 추론된 스키마
        """
        fields = []
        for col in df.columns:
            dtype_str = str(df[col].dtype)
            pa_type = cls.DTYPE_MAPPING.get(dtype_str, pa.string())
            fields.append(pa.field(col, pa_type))

        return pa.schema(fields)

    @classmethod
    def create_schema(cls, columns: list[tuple[str, str]]) -> pa.Schema:
        """
        명시적 스키마 생성

        Args:
            columns: [(컬럼명, 타입)] 리스트
                     타입: "int64", "string", "float64", "timestamp", "bool"

        Returns:
            pa.Schema: 생성된 스키마
        """
        type_mapping = {
            "int64": pa.int64(),
            "int32": pa.int32(),
            "float64": pa.float64(),
            "float32": pa.float32(),
            "string": pa.string(),
            "bool": pa.bool_(),
            "timestamp": pa.timestamp("ns"),
            "timestamp_utc": pa.timestamp("ns", tz="UTC"),
            "timestamp_kst": pa.timestamp("ns", tz="Asia/Seoul"),
            "date": pa.date32(),
        }

        fields = []
        for col_name, col_type in columns:
            pa_type = type_mapping.get(col_type, pa.string())
            fields.append(pa.field(col_name, pa_type))

        return pa.schema(fields)

    @classmethod
    def validate_schema(
        cls, df: pd.DataFrame, expected_schema: pa.Schema
    ) -> tuple[bool, list[str]]:
        """
        DataFrame이 예상 스키마와 일치하는지 검증

        Args:
            df: 검증할 DataFrame
            expected_schema: 예상 스키마

        Returns:
            (일치 여부, 불일치 항목 리스트)
        """
        errors = []
        expected_columns = {field.name for field in expected_schema}
        actual_columns = set(df.columns)

        # 누락된 컬럼 확인
        missing = expected_columns - actual_columns
        if missing:
            errors.append(f"Missing columns: {missing}")

        # 추가된 컬럼 확인
        extra = actual_columns - expected_columns
        if extra:
            errors.append(f"Extra columns: {extra}")

        return len(errors) == 0, errors

    @classmethod
    def detect_schema_drift(
        cls, current_schema: pa.Schema, new_schema: pa.Schema
    ) -> dict[str, Any]:
        """
        스키마 드리프트 감지

        Args:
            current_schema: 현재 스키마
            new_schema: 새로운 스키마

        Returns:
            드리프트 정보 dict
        """
        current_fields = {f.name: f.type for f in current_schema}
        new_fields = {f.name: f.type for f in new_schema}

        added = set(new_fields.keys()) - set(current_fields.keys())
        removed = set(current_fields.keys()) - set(new_fields.keys())

        type_changed = {}
        for col in set(current_fields.keys()) & set(new_fields.keys()):
            if current_fields[col] != new_fields[col]:
                type_changed[col] = {
                    "from": str(current_fields[col]),
                    "to": str(new_fields[col]),
                }

        return {
            "has_drift": bool(added or removed or type_changed),
            "added_columns": list(added),
            "removed_columns": list(removed),
            "type_changes": type_changed,
        }

    @classmethod
    def apply_schema(cls, df: pd.DataFrame, schema: pa.Schema) -> pd.DataFrame:
        """
        DataFrame에 스키마 적용 (타입 변환)

        Args:
            df: 소스 DataFrame
            schema: 적용할 스키마

        Returns:
            타입이 변환된 DataFrame
        """
        result = df.copy()

        for field in schema:
            if field.name not in result.columns:
                continue

            if pa.types.is_integer(field.type):
                result[field.name] = pd.to_numeric(
                    result[field.name], errors="coerce"
                ).astype("Int64")
            elif pa.types.is_floating(field.type):
                result[field.name] = pd.to_numeric(result[field.name], errors="coerce")
            elif pa.types.is_string(field.type):
                result[field.name] = result[field.name].astype(str)
            elif pa.types.is_boolean(field.type):
                result[field.name] = result[field.name].astype(bool)
            elif pa.types.is_timestamp(field.type):
                result[field.name] = pd.to_datetime(result[field.name], errors="coerce")

        return result
