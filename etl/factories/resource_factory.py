"""
Resource Factory - 테넌트별 Resource 동적 생성
"""

import os

from etl.config.tenant_config import TenantConfig
from etl.resources.rdb import RDBResource
from etl.resources.s3 import S3Resource
from etl.resources.trino import TrinoResource


class ResourceFactory:
    """테넌트별 Resource 생성 팩토리"""

    def __init__(self, tenant: TenantConfig, shared_s3_config: dict | None = None):
        """
        Args:
            tenant: 테넌트 설정
            shared_s3_config: 공유 S3 설정 (endpoint, credentials 등)
        """
        self.tenant = tenant
        self.tenant_id = tenant.id
        self.shared_s3_config = shared_s3_config or {}

    def create_all_resources(self) -> dict:
        """테넌트의 모든 Resource 생성"""
        return {
            "rdb": self._create_rdb_resource(),
            "s3": self._create_s3_resource(),
            "trino": self._create_trino_resource(),
        }

    def _create_rdb_resource(self) -> RDBResource:
        """RDB Resource 생성 (테넌트별 독립 Source DB)"""
        db_config = self.tenant.source_database

        return RDBResource(
            host=db_config.host,
            port=db_config.port,
            database=db_config.database,
            username=db_config.username,
            password=db_config.password,
        )

    def _create_s3_resource(self) -> S3Resource:
        """S3 Resource 생성 (테넌트 경로 포함)"""
        storage_config = self.tenant.storage

        return S3Resource(
            endpoint_url=self.shared_s3_config.get("endpoint_url", ""),
            aws_access_key_id=self.shared_s3_config.get("aws_access_key_id", ""),
            aws_secret_access_key=self.shared_s3_config.get("aws_secret_access_key", ""),
            region_name=self.shared_s3_config.get("region_name", "ap-northeast-2"),
            bucket_name=storage_config.bucket,
            tenant_base_path=storage_config.base_path,
        )

    def _create_trino_resource(self) -> TrinoResource:
        """Trino Resource 생성 (공유 Target DB)"""
        target_config = self.tenant.target_database

        return TrinoResource(
            host=target_config.trino_host,
            port=target_config.trino_port,
            user=target_config.trino_user,
            password=target_config.trino_password,
            http_scheme=target_config.trino_http_scheme,
            catalog=target_config.target_catalog,
            schema_name=target_config.target_schema,
        )


def get_shared_s3_config() -> dict:
    """환경변수에서 공유 S3 설정 로드"""
    return {
        "endpoint_url": os.getenv("S3_ENDPOINT_URL", ""),
        "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID", ""),
        "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY", ""),
        "region_name": os.getenv("AWS_REGION", "ap-northeast-2"),
    }
