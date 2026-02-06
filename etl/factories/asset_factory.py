"""
Asset Factory - 테넌트별 Asset 동적 생성
TenantLoader를 사용하여 테넌트별 커스텀 코드 지원
"""

from typing import Any, Callable

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetsDefinition,
    MetadataValue,
    Output,
    asset,
)

from etl.config.tenant_config import TenantConfig
from etl.config.tenant_loader import TenantLoader
from etl.partitions.daily import daily_partitions_def
from etl.resources.rdb import RDBResource
from etl.resources.s3 import S3Resource
from etl.resources.trino import TrinoResource
from etl.utils.logging import ETLLogger


class AssetFactory:
    """테넌트별 Asset 생성 팩토리 (TenantLoader 연동)"""

    def __init__(self, tenant: TenantConfig):
        self.tenant = tenant
        self.tenant_id = tenant.id
        self.logger = ETLLogger(f"factory.{self.tenant_id}")
        self.tenant_loader = TenantLoader(tenant.id)

    def create_extract_asset(
        self,
        asset_name: str,
        source_table: str,
        date_column: str,
        query: str | None = None,
    ) -> AssetsDefinition:
        """
        Extract Asset 생성

        Args:
            asset_name: Asset 이름 (e.g., "lot_history")
            source_table: 소스 테이블명
            date_column: 날짜 필터 컬럼
            query: SQL 쿼리 (없으면 기본 쿼리 사용)
        """
        tenant_id = self.tenant_id
        tenant_name = self.tenant.name
        full_asset_name = f"{tenant_id}/extract/{asset_name}"

        # TenantLoader에서 커스텀 쿼리 조회 (커스텀 > 공용)
        extract_queries = self.tenant_loader.get_extract_queries()
        sql_query = query or extract_queries.get(asset_name, f"SELECT * FROM {source_table}")

        @asset(
            name=asset_name,
            key_prefix=[tenant_id, "extract"],
            partitions_def=daily_partitions_def,
            group_name=tenant_id,
            description=f"[{tenant_name}] Extract {asset_name}",
            compute_kind="sql",
            tags={
                "tenant_id": tenant_id,
                "stage": "extract",
                "source_table": source_table,
                **self.tenant.tags,
            },
        )
        def _extract_asset(
            context: AssetExecutionContext,
            rdb: RDBResource,
            s3: S3Resource,
        ) -> Output[dict[str, Any]]:
            partition_date = context.partition_key
            logger = ETLLogger(f"extract.{tenant_id}")

            logger.log_extract_start(full_asset_name, partition_date, source_table)

            # 데이터 추출
            df = rdb.execute_query_with_date_filter(
                query=sql_query,
                date_column=date_column,
                partition_date=partition_date,
            )

            # S3에 저장 (테넌트 경로)
            s3_path = s3.write_parquet_for_tenant(
                df=df,
                tenant_id=tenant_id,
                stage="extract",
                job_name=asset_name,
                partition_date=partition_date,
            )

            row_count = len(df)
            logger.log_extract_complete(full_asset_name, partition_date, row_count, s3_path)

            return Output(
                value={
                    "s3_path": s3_path,
                    "row_count": row_count,
                    "tenant_id": tenant_id,
                },
                metadata={
                    "row_count": MetadataValue.int(row_count),
                    "s3_path": MetadataValue.path(s3_path),
                    "partition_date": MetadataValue.text(partition_date),
                    "tenant_id": MetadataValue.text(tenant_id),
                    "columns": MetadataValue.json(df.columns.tolist()),
                },
            )

        return _extract_asset

    def create_transform_asset(
        self,
        asset_name: str,
        input_assets: list[str],
        transform_fn: Callable[[dict[str, pd.DataFrame], str, str], pd.DataFrame],
    ) -> AssetsDefinition:
        """
        Transform Asset 생성

        Args:
            asset_name: Asset 이름 (e.g., "aps_wip")
            input_assets: 입력 Asset 이름 리스트 (tenant prefix 없이)
            transform_fn: 변환 함수 (input_dfs, partition_date, tenant_id) -> output_df
        """
        tenant_id = self.tenant_id
        tenant_name = self.tenant.name
        full_asset_name = f"{tenant_id}/transform/{asset_name}"

        # 입력 의존성 설정 (계층형 key 사용)
        ins = {
            name: AssetIn(key=[tenant_id, "extract", name])
            for name in input_assets
        }

        @asset(
            name=asset_name,
            key_prefix=[tenant_id, "transform"],
            partitions_def=daily_partitions_def,
            ins=ins,
            group_name=tenant_id,
            description=f"[{tenant_name}] Transform {asset_name}",
            compute_kind="pandas",
            tags={
                "tenant_id": tenant_id,
                "stage": "transform",
                **self.tenant.tags,
            },
        )
        def _transform_asset(
            context: AssetExecutionContext,
            s3: S3Resource,
            **inputs: dict[str, Any],
        ) -> Output[dict[str, Any]]:
            partition_date = context.partition_key
            logger = ETLLogger(f"transform.{tenant_id}")

            # 입력 데이터 로드
            input_dfs: dict[str, pd.DataFrame] = {}
            total_input_rows = 0

            for asset_key, asset_data in inputs.items():
                # asset_key가 이제 원래 이름 그대로 (e.g., "lot_history")
                df = s3.read_parquet_from_path(asset_data["s3_path"])
                input_dfs[asset_key] = df
                total_input_rows += len(df)

            first_input = list(inputs.values())[0]
            logger.log_transform_start(full_asset_name, partition_date, first_input["s3_path"])

            # 변환 실행
            output_df = transform_fn(input_dfs, partition_date, tenant_id)

            # S3에 저장
            s3_path = s3.write_parquet_for_tenant(
                df=output_df,
                tenant_id=tenant_id,
                stage="transform",
                job_name=asset_name,
                partition_date=partition_date,
            )

            output_rows = len(output_df)
            logger.log_transform_complete(
                full_asset_name, partition_date, total_input_rows, output_rows, s3_path
            )

            return Output(
                value={
                    "s3_path": s3_path,
                    "row_count": output_rows,
                    "tenant_id": tenant_id,
                },
                metadata={
                    "input_rows": MetadataValue.int(total_input_rows),
                    "output_rows": MetadataValue.int(output_rows),
                    "s3_path": MetadataValue.path(s3_path),
                    "partition_date": MetadataValue.text(partition_date),
                    "tenant_id": MetadataValue.text(tenant_id),
                },
            )

        return _transform_asset

    def create_load_asset(
        self,
        asset_name: str,
        input_asset: str,
        target_table: str,
        target_schema: str,
        key_columns: list[str],
    ) -> AssetsDefinition:
        """
        Load Asset 생성

        Args:
            asset_name: Asset 이름 (e.g., "aps_wip")
            input_asset: 입력 Transform Asset 이름 (tenant prefix 없이)
            target_table: 타겟 테이블명
            target_schema: 타겟 스키마명
            key_columns: Upsert 키 컬럼 (project_id 포함)
        """
        tenant_id = self.tenant_id
        tenant_name = self.tenant.name
        full_asset_name = f"{tenant_id}/load/{asset_name}"

        @asset(
            name=asset_name,
            key_prefix=[tenant_id, "load"],
            partitions_def=daily_partitions_def,
            ins={input_asset: AssetIn(key=[tenant_id, "transform", input_asset])},
            group_name=tenant_id,
            description=f"[{tenant_name}] Load {asset_name} to {target_schema}.{target_table}",
            compute_kind="trino",
            tags={
                "tenant_id": tenant_id,
                "stage": "load",
                "target_table": f"{target_schema}.{target_table}",
                **self.tenant.tags,
            },
        )
        def _load_asset(
            context: AssetExecutionContext,
            s3: S3Resource,
            trino: TrinoResource,
            **inputs: dict[str, Any],
        ) -> Output[dict[str, Any]]:
            partition_date = context.partition_key
            logger = ETLLogger(f"load.{tenant_id}")
            input_data = inputs[input_asset]

            logger.log_load_start(full_asset_name, partition_date, target_table)

            # 데이터 로드
            df = s3.read_parquet_from_path(input_data["s3_path"])

            # project_id 컬럼 추가 (공유 DB에서 테넌트 구분)
            df["project_id"] = tenant_id

            # Trino를 통해 적재
            result = trino.upsert_dataframe(
                df=df,
                target_catalog=self.tenant.target_database.target_catalog,
                target_schema=target_schema,
                target_table=target_table,
                key_columns=key_columns,
            )

            row_count = result["inserted"]
            logger.log_load_complete(full_asset_name, partition_date, row_count, target_table)

            return Output(
                value={
                    "target_table": f"{target_schema}.{target_table}",
                    "row_count": row_count,
                    "tenant_id": tenant_id,
                    "deleted": result["deleted"],
                    "inserted": result["inserted"],
                },
                metadata={
                    "target_table": MetadataValue.text(f"{target_schema}.{target_table}"),
                    "deleted_rows": MetadataValue.int(result["deleted"]),
                    "inserted_rows": MetadataValue.int(result["inserted"]),
                    "partition_date": MetadataValue.text(partition_date),
                    "tenant_id": MetadataValue.text(tenant_id),
                },
            )

        return _load_asset

    def create_all_etl_assets(self) -> list[AssetsDefinition]:
        """
        테넌트의 모든 ETL Asset 생성 (TenantLoader 기반)

        Extract/Transform/Load 각 단계별 Asset을 생성하며,
        TenantLoader를 통해 테넌트별 커스텀 코드가 자동 적용됨.

        Returns:
            AssetsDefinition 리스트
        """
        assets = []

        # Extract Asset 설정 (공용 + 커스텀)
        extract_configs = [
            {"name": "lot_history", "source_table": "lot_history", "date_column": "created_at"},
            {"name": "equipment_event", "source_table": "equipment_event", "date_column": "event_time"},
            {"name": "process_result", "source_table": "process_result", "date_column": "measured_at"},
        ]

        # Extract Assets 생성
        for config in extract_configs:
            asset = self.create_extract_asset(
                asset_name=config["name"],
                source_table=config["source_table"],
                date_column=config["date_column"],
            )
            assets.append(asset)

        # Transform Asset 설정 (TenantLoader에서 함수 조회)
        transform_configs = [
            {
                "name": "aps_wip",
                "input_assets": ["lot_history"],
            },
            {
                "name": "cycle_time",
                "input_assets": ["lot_history", "process_result"],
            },
            {
                "name": "equipment_utilization",
                "input_assets": ["equipment_event"],
            },
        ]

        # Transform Assets 생성 (TenantLoader에서 커스텀/공용 함수 자동 선택)
        for config in transform_configs:
            transform_fn = self.tenant_loader.get_transform_function(config["name"])
            asset = self.create_transform_asset(
                asset_name=config["name"],
                input_assets=config["input_assets"],
                transform_fn=transform_fn,
            )
            assets.append(asset)

        # Load Asset 설정 (TenantLoader에서 설정 조회)
        load_asset_names = ["aps_wip", "cycle_time", "equipment_utilization"]

        # Load Assets 생성 (TenantLoader에서 커스텀/공용 설정 자동 선택)
        for asset_name in load_asset_names:
            load_config = self.tenant_loader.get_load_config(asset_name)
            asset = self.create_load_asset(
                asset_name=asset_name,
                input_asset=asset_name,  # Transform과 Load는 같은 이름
                target_table=load_config["target_table"],
                target_schema=load_config["target_schema"],
                key_columns=load_config["key_columns"],
            )
            assets.append(asset)

        return assets
