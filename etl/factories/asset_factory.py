"""
Asset Factory - 테넌트별 Asset 동적 생성
PipelineAssetConfig 기반으로 extract → transfer → load 파이프라인 구성

파이프라인 흐름:
- extract (input_load): RDB에서 추출 → S3 Parquet 저장 (항상)
- transfer: 입력 데이터 가공 (선택, has_transfer=True)
- load (output_save): Trino에 적재 (선택, save_to_trino=True)
"""

from typing import Any, Callable

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetsDefinition,
    MetadataValue,
    Output,
    TableColumn,
    TableSchema,
    asset,
)

from etl.config.tenant_config import PipelineAssetConfig, TenantConfig
from etl.config.tenant_loader import TenantLoader
from etl.factories.dbt_factory import DbtFactory
from etl.partitions.daily import daily_partitions_def
from etl.resources.rdb import RDBResource
from etl.resources.s3 import S3Resource
from etl.resources.trino import TrinoResource
from etl.utils.logging import ETLLogger


class AssetFactory:
    """테넌트별 Asset 생성 팩토리 (PipelineAssetConfig 기반)"""

    def __init__(self, tenant: TenantConfig, environment: str = "dev"):
        self.tenant = tenant
        self.tenant_id = tenant.id
        self.environment = environment
        self.logger = ETLLogger(f"factory.{self.tenant_id}")
        self.tenant_loader = TenantLoader(tenant.id)
        self.dbt_factory = DbtFactory(tenant) if tenant.dbt.enabled else None

    def _get_pipeline_configs(self) -> dict[str, PipelineAssetConfig]:
        """테넌트의 파이프라인 설정 조회 (YAML > 기본값), 환경별 오버라이드 적용"""
        if self.tenant.assets.pipelines:
            pipelines = self.tenant.assets.pipelines
        else:
            pipelines = self.tenant.get_default_assets_config().pipelines

        return {
            name: config.resolve_for_env(self.environment)
            for name, config in pipelines.items()
        }

    def _get_partitions_def(self, config: PipelineAssetConfig):
        """파티션 정의 반환 (date_column이 있으면 daily, 없으면 None)"""
        return daily_partitions_def if config.date_column else None

    def create_extract_asset(
        self,
        asset_name: str,
        config: PipelineAssetConfig,
    ) -> AssetsDefinition:
        """
        Extract Asset 생성 (input_load)
        RDB에서 데이터 추출 후 항상 S3에 Parquet 저장

        Args:
            asset_name: Asset 이름 (e.g., "cfg_item_master")
            config: 파이프라인 설정
        """
        tenant_id = self.tenant_id
        tenant_name = self.tenant.name
        full_asset_name = f"{tenant_id}/extract/{asset_name}"
        source_table = config.source_table
        date_column = config.date_column
        save_to_s3 = config.save_to_s3
        partitions_def = self._get_partitions_def(config)

        # TenantLoader에서 커스텀 쿼리 조회 (커스텀 > 공용)
        extract_queries = self.tenant_loader.get_extract_queries()
        sql_query = config.query or extract_queries.get(
            asset_name, f"SELECT * FROM {source_table}"
        )

        @asset(
            name=asset_name,
            key_prefix=[tenant_id, "extract"],
            partitions_def=partitions_def,
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
            logger = ETLLogger(f"extract.{tenant_id}")

            # 파티션 날짜 결정
            partition_date = context.partition_key if partitions_def else None

            logger.log_extract_start(full_asset_name, partition_date or "latest", source_table)

            # 데이터 추출
            if date_column and partition_date:
                df = rdb.execute_query_with_date_filter(
                    query=sql_query,
                    date_column=date_column,
                    partition_date=partition_date,
                )
            else:
                df = rdb.execute_query(sql_query)

            row_count = len(df)

            # S3 저장 (save_to_s3 옵션에 따라)
            if save_to_s3:
                s3_path = s3.write_parquet_for_tenant(
                    df=df,
                    tenant_id=tenant_id,
                    stage="extract",
                    job_name=asset_name,
                    partition_date=partition_date,
                )
            else:
                s3_path = None

            logger.log_extract_complete(full_asset_name, partition_date or "latest", row_count, s3_path or "N/A (S3 save disabled)")

            # 메타데이터 구성
            metadata = {
                "row_count": MetadataValue.int(row_count),
                "s3_path": MetadataValue.text(s3_path or "disabled"),
                "partition_date": MetadataValue.text(partition_date or "latest"),
                "tenant_id": MetadataValue.text(tenant_id),
                # 컬럼 스키마 (UI의 "Columns" 탭에 표시)
                "dagster/column_schema": TableSchema(
                    columns=[
                        TableColumn(name=col, type=str(dtype))
                        for col, dtype in df.dtypes.items()
                    ]
                ),
            }

            # 데이터 프리뷰 (상위 20행, markdown 테이블)
            if row_count > 0:
                preview_df = df.head(20)
                metadata["preview"] = MetadataValue.md(
                    preview_df.to_markdown(index=False)
                )

                # Null 현황
                null_counts = df.isnull().sum()
                nulls = {col: int(cnt) for col, cnt in null_counts.items() if cnt > 0}
                if nulls:
                    metadata["null_counts"] = MetadataValue.json(nulls)

            return Output(
                value={
                    "s3_path": s3_path,
                    "row_count": row_count,
                    "tenant_id": tenant_id,
                },
                metadata=metadata,
            )

        return _extract_asset

    def create_transfer_asset(
        self,
        asset_name: str,
        config: PipelineAssetConfig,
        transfer_fn: Callable[[dict[str, pd.DataFrame], str, str], pd.DataFrame],
    ) -> AssetsDefinition:
        """
        Transfer Asset 생성
        입력 데이터를 가공하여 출력 데이터 생성

        Args:
            asset_name: Asset 이름 (e.g., "aps_wip")
            config: 파이프라인 설정
            transfer_fn: 변환 함수 (input_dfs, partition_date, tenant_id) -> output_df
        """
        tenant_id = self.tenant_id
        tenant_name = self.tenant.name
        full_asset_name = f"{tenant_id}/transfer/{asset_name}"
        partitions_def = self._get_partitions_def(config)

        # 입력 의존성 설정
        input_assets = config.transfer_inputs or [asset_name]
        ins = {
            name: AssetIn(key=[tenant_id, "extract", name])
            for name in input_assets
        }

        @asset(
            name=asset_name,
            key_prefix=[tenant_id, "transfer"],
            partitions_def=partitions_def,
            ins=ins,
            group_name=tenant_id,
            description=f"[{tenant_name}] Transfer {asset_name}",
            compute_kind="pandas",
            tags={
                "tenant_id": tenant_id,
                "stage": "transfer",
                **self.tenant.tags,
            },
        )
        def _transfer_asset(
            context: AssetExecutionContext,
            s3: S3Resource,
            **inputs: dict[str, Any],
        ) -> Output[dict[str, Any]]:
            logger = ETLLogger(f"transfer.{tenant_id}")
            partition_date = context.partition_key if partitions_def else "latest"

            # 입력 데이터 로드
            input_dfs: dict[str, pd.DataFrame] = {}
            total_input_rows = 0

            for asset_key, asset_data in inputs.items():
                df = s3.read_parquet_from_path(asset_data["s3_path"])
                input_dfs[asset_key] = df
                total_input_rows += len(df)

            first_input = list(inputs.values())[0]
            logger.log_transform_start(full_asset_name, partition_date, first_input["s3_path"])

            # 변환 실행
            output_df = transfer_fn(input_dfs, partition_date, tenant_id)

            # S3에 저장
            s3_path = s3.write_parquet_for_tenant(
                df=output_df,
                tenant_id=tenant_id,
                stage="transfer",
                job_name=asset_name,
                partition_date=partition_date if partition_date != "latest" else None,
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

        return _transfer_asset

    def create_load_asset(
        self,
        asset_name: str,
        config: PipelineAssetConfig,
    ) -> AssetsDefinition:
        """
        Load Asset 생성 (output_save)
        Trino를 통해 데이터 적재

        Args:
            asset_name: Asset 이름 (e.g., "cfg_item_master")
            config: 파이프라인 설정
        """
        tenant_id = self.tenant_id
        tenant_name = self.tenant.name
        full_asset_name = f"{tenant_id}/load/{asset_name}"
        partitions_def = self._get_partitions_def(config)
        trino_config = config.trino_output

        # 입력 단계 결정: transfer가 있으면 transfer, 없으면 extract
        if config.has_transfer:
            input_stage = "transfer"
        else:
            input_stage = "extract"

        input_asset_name = asset_name
        target_table = trino_config.target_table
        target_schema = trino_config.target_schema
        key_columns = trino_config.key_columns

        @asset(
            name=asset_name,
            key_prefix=[tenant_id, "load"],
            partitions_def=partitions_def,
            ins={input_asset_name: AssetIn(key=[tenant_id, input_stage, input_asset_name])},
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
            logger = ETLLogger(f"load.{tenant_id}")
            partition_date = context.partition_key if partitions_def else "latest"
            input_data = inputs[input_asset_name]

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
        테넌트의 모든 ETL Asset 생성 (PipelineAssetConfig 기반)

        파이프라인 설정에 따라 각 단계별 Asset을 동적으로 생성:
        - extract: 항상 생성 (RDB → S3)
        - transfer: has_transfer=True일 때만 생성
        - load: save_to_trino=True일 때만 생성

        Returns:
            AssetsDefinition 리스트
        """
        assets = []
        pipelines = self._get_pipeline_configs()

        for name, config in pipelines.items():
            # 1. Extract (항상 생성)
            assets.append(self.create_extract_asset(name, config))

            # 2. Transfer (선택)
            if config.has_transfer:
                transfer_fn = self.tenant_loader.get_transfer_function(name)
                assets.append(self.create_transfer_asset(name, config, transfer_fn))

            # 3. Load (선택)
            if config.save_to_trino and config.trino_output:
                assets.append(self.create_load_asset(name, config))

        # 4. dbt Assets (dbt enabled인 경우)
        if self.dbt_factory:
            dbt_assets = self.dbt_factory.create_dbt_assets()
            assets.extend(dbt_assets)

        return assets
