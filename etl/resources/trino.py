"""
Trino Resource - Query Engine for Data Loading
"""

from contextlib import contextmanager
from typing import Any, Generator

import pandas as pd
from dagster import ConfigurableResource, InitResourceContext
from pydantic import Field
from trino.dbapi import connect
from trino.auth import BasicAuthentication


class TrinoResource(ConfigurableResource):
    """Trino 연결을 위한 Dagster Resource"""

    host: str = Field(default="localhost", description="Trino host")
    port: int = Field(default=8080, description="Trino port")
    user: str = Field(default="trino", description="Trino user")
    password: str = Field(default="", description="Trino password (optional)")
    catalog: str = Field(default="postgresql", description="Default catalog")
    schema_name: str = Field(default="public", description="Default schema")
    http_scheme: str = Field(default="http", description="HTTP scheme (http/https)")

    _connection: Any = None

    def setup_for_execution(self, context: InitResourceContext) -> None:
        """Resource 초기화"""
        pass  # Connection은 필요시 lazy 생성

    def teardown_after_execution(self, context: InitResourceContext) -> None:
        """Resource 정리"""
        if self._connection:
            self._connection.close()
            self._connection = None

    def _create_connection(self) -> Any:
        """Trino 연결 생성"""
        auth = None
        if self.password:
            auth = BasicAuthentication(self.user, self.password)

        return connect(
            host=self.host,
            port=self.port,
            user=self.user,
            catalog=self.catalog,
            schema=self.schema_name,
            http_scheme=self.http_scheme,
            auth=auth,
        )

    @contextmanager
    def get_connection(self) -> Generator[Any, None, None]:
        """데이터베이스 연결 컨텍스트 매니저"""
        conn = self._create_connection()
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def get_cursor(self) -> Generator[Any, None, None]:
        """커서 컨텍스트 매니저"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                yield cursor
            finally:
                cursor.close()

    def execute_query(self, query: str) -> pd.DataFrame:
        """
        쿼리 실행 및 DataFrame 반환

        Args:
            query: 실행할 SQL 쿼리

        Returns:
            pd.DataFrame: 쿼리 결과
        """
        with self.get_cursor() as cursor:
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            return pd.DataFrame(rows, columns=columns)

    def execute_statement(self, statement: str) -> int:
        """
        DML 문 실행 (INSERT, UPDATE, DELETE)

        Args:
            statement: 실행할 SQL 문

        Returns:
            영향 받은 행 수
        """
        with self.get_cursor() as cursor:
            cursor.execute(statement)
            return cursor.rowcount if cursor.rowcount else 0

    def insert_dataframe(
        self,
        df: pd.DataFrame,
        target_catalog: str,
        target_schema: str,
        target_table: str,
        batch_size: int = 1000,
    ) -> int:
        """
        DataFrame을 Trino를 통해 Target DB에 적재

        Args:
            df: 적재할 DataFrame
            target_catalog: 타겟 카탈로그
            target_schema: 타겟 스키마
            target_table: 타겟 테이블
            batch_size: 배치 크기

        Returns:
            적재된 행 수
        """
        if df.empty:
            return 0

        total_rows = 0
        columns = df.columns.tolist()
        col_str = ", ".join(columns)

        with self.get_cursor() as cursor:
            for i in range(0, len(df), batch_size):
                batch = df.iloc[i : i + batch_size]

                # VALUES 절 생성
                values_list = []
                for _, row in batch.iterrows():
                    values = []
                    for val in row:
                        if pd.isna(val):
                            values.append("NULL")
                        elif isinstance(val, str):
                            # SQL Injection 방지를 위한 이스케이프
                            escaped = val.replace("'", "''")
                            values.append(f"'{escaped}'")
                        elif isinstance(val, (int, float)):
                            values.append(str(val))
                        else:
                            escaped = str(val).replace("'", "''")
                            values.append(f"'{escaped}'")
                    values_list.append(f"({', '.join(values)})")

                values_str = ", ".join(values_list)
                insert_sql = (
                    f"INSERT INTO {target_catalog}.{target_schema}.{target_table} "
                    f"({col_str}) VALUES {values_str}"
                )

                cursor.execute(insert_sql)
                total_rows += len(batch)

        return total_rows

    def upsert_dataframe(
        self,
        df: pd.DataFrame,
        target_catalog: str,
        target_schema: str,
        target_table: str,
        key_columns: list[str],
        batch_size: int = 1000,
    ) -> dict[str, int]:
        """
        DataFrame을 Trino를 통해 Upsert (MERGE)

        Note: Trino의 MERGE 지원 여부에 따라 DELETE + INSERT로 구현

        Args:
            df: 적재할 DataFrame
            target_catalog: 타겟 카탈로그
            target_schema: 타겟 스키마
            target_table: 타겟 테이블
            key_columns: PK 컬럼 목록
            batch_size: 배치 크기

        Returns:
            {"deleted": int, "inserted": int}
        """
        if df.empty:
            return {"deleted": 0, "inserted": 0}

        deleted = 0
        inserted = 0
        full_table = f"{target_catalog}.{target_schema}.{target_table}"

        with self.get_cursor() as cursor:
            for i in range(0, len(df), batch_size):
                batch = df.iloc[i : i + batch_size]

                # 기존 데이터 삭제 (Key 기준)
                for _, row in batch.iterrows():
                    conditions = []
                    for key_col in key_columns:
                        val = row[key_col]
                        if pd.isna(val):
                            conditions.append(f"{key_col} IS NULL")
                        elif isinstance(val, str):
                            escaped = val.replace("'", "''")
                            conditions.append(f"{key_col} = '{escaped}'")
                        else:
                            conditions.append(f"{key_col} = {val}")

                    delete_sql = f"DELETE FROM {full_table} WHERE {' AND '.join(conditions)}"
                    cursor.execute(delete_sql)
                    deleted += cursor.rowcount if cursor.rowcount else 0

        # INSERT
        inserted = self.insert_dataframe(
            df, target_catalog, target_schema, target_table, batch_size
        )

        return {"deleted": deleted, "inserted": inserted}

    def truncate_table(
        self, target_catalog: str, target_schema: str, target_table: str
    ) -> None:
        """테이블 전체 삭제 (Trino는 TRUNCATE 미지원, DELETE 사용)"""
        full_table = f"{target_catalog}.{target_schema}.{target_table}"
        with self.get_cursor() as cursor:
            cursor.execute(f"DELETE FROM {full_table}")

    def table_exists(
        self, catalog: str, schema: str, table: str
    ) -> bool:
        """테이블 존재 여부 확인"""
        query = f"""
            SELECT COUNT(*)
            FROM {catalog}.information_schema.tables
            WHERE table_schema = '{schema}' AND table_name = '{table}'
        """
        with self.get_cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchone()
            return result[0] > 0 if result else False
