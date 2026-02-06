"""
RDB Resource - Source Database Connection
"""

from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

import pandas as pd
from dagster import ConfigurableResource, InitResourceContext
from pydantic import Field
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


class RDBResource(ConfigurableResource):
    """Source RDB 연결을 위한 Dagster Resource"""

    host: str = Field(default="localhost", description="Database host")
    port: int = Field(default=5432, description="Database port")
    database: str = Field(default="source_db", description="Database name")
    username: str = Field(default="postgres", description="Database username")
    password: str = Field(default="", description="Database password")
    driver: str = Field(
        default="postgresql+psycopg2", description="SQLAlchemy driver string"
    )

    _engine: Engine | None = None

    def setup_for_execution(self, context: InitResourceContext) -> None:
        """Resource 초기화 시 엔진 생성"""
        self._engine = self._create_engine()

    def teardown_after_execution(self, context: InitResourceContext) -> None:
        """Resource 정리 시 엔진 dispose"""
        if self._engine:
            self._engine.dispose()
            self._engine = None

    def _create_engine(self) -> Engine:
        """SQLAlchemy Engine 생성"""
        connection_string = (
            f"{self.driver}://{self.username}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )
        return create_engine(connection_string, pool_pre_ping=True, pool_size=5)

    @property
    def engine(self) -> Engine:
        """Engine 인스턴스 반환"""
        if self._engine is None:
            self._engine = self._create_engine()
        return self._engine

    @contextmanager
    def get_connection(self) -> Generator[Any, None, None]:
        """데이터베이스 연결 컨텍스트 매니저"""
        conn = self.engine.connect()
        try:
            yield conn
        finally:
            conn.close()

    def execute_query(self, query: str, params: dict | None = None) -> pd.DataFrame:
        """
        SQL 쿼리 실행 및 DataFrame 반환

        Args:
            query: 실행할 SQL 쿼리
            params: 쿼리 파라미터 (optional)

        Returns:
            pd.DataFrame: 쿼리 결과
        """
        with self.get_connection() as conn:
            return pd.read_sql(text(query), conn, params=params)

    def execute_query_with_date_filter(
        self,
        query: str,
        date_column: str,
        partition_date: str,
        params: dict | None = None,
    ) -> pd.DataFrame:
        """
        날짜 필터가 포함된 쿼리 실행

        Args:
            query: 기본 SQL 쿼리 (WHERE 절 없이)
            date_column: 필터링할 날짜 컬럼명
            partition_date: 파티션 날짜 (YYYY-MM-DD)
            params: 추가 쿼리 파라미터

        Returns:
            pd.DataFrame: 쿼리 결과
        """
        filtered_query = f"{query} WHERE {date_column} = :partition_date"
        all_params = {"partition_date": partition_date}
        if params:
            all_params.update(params)

        with self.get_connection() as conn:
            return pd.read_sql(text(filtered_query), conn, params=all_params)

    def get_table_row_count(self, table_name: str, schema: str = "public") -> int:
        """테이블 행 수 조회"""
        query = f"SELECT COUNT(*) as cnt FROM {schema}.{table_name}"
        with self.get_connection() as conn:
            result = conn.execute(text(query))
            return result.scalar() or 0
