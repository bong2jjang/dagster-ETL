"""
S3 Resource - Cloud Storage for Parquet files
"""

import io
from typing import Any

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError
from dagster import ConfigurableResource, InitResourceContext
from pydantic import Field


class S3Resource(ConfigurableResource):
    """S3 스토리지 연결을 위한 Dagster Resource (MinIO 호환, 멀티테넌시 지원)"""

    endpoint_url: str = Field(
        default="", description="S3 endpoint URL (MinIO의 경우 필수)"
    )
    aws_access_key_id: str = Field(default="", description="AWS Access Key ID")
    aws_secret_access_key: str = Field(default="", description="AWS Secret Access Key")
    region_name: str = Field(default="ap-northeast-2", description="AWS Region")
    bucket_name: str = Field(default="data-lake", description="Default bucket name")
    tenant_base_path: str = Field(
        default="", description="테넌트별 기본 경로 (e.g., project_id=acme)"
    )

    _client: Any = None

    def setup_for_execution(self, context: InitResourceContext) -> None:
        """Resource 초기화"""
        self._client = self._create_client()

    def _create_client(self) -> Any:
        """S3 클라이언트 생성"""
        client_kwargs: dict[str, Any] = {
            "service_name": "s3",
            "region_name": self.region_name,
        }

        if self.aws_access_key_id and self.aws_secret_access_key:
            client_kwargs["aws_access_key_id"] = self.aws_access_key_id
            client_kwargs["aws_secret_access_key"] = self.aws_secret_access_key

        if self.endpoint_url:
            client_kwargs["endpoint_url"] = self.endpoint_url

        return boto3.client(**client_kwargs)

    @property
    def client(self) -> Any:
        """S3 클라이언트 반환"""
        if self._client is None:
            self._client = self._create_client()
        return self._client

    def _build_s3_path(
        self, stage: str, job_name: str, partition_date: str, filename: str
    ) -> str:
        """
        S3 경로 생성

        Args:
            stage: extract / transform
            job_name: Job 이름
            partition_date: 파티션 날짜 (YYYY-MM-DD)
            filename: 파일명

        Returns:
            S3 key 경로
        """
        date_formatted = partition_date.replace("-", "")
        return f"{stage}/job_name={job_name}/date={date_formatted}/{filename}"

    def write_parquet(
        self,
        df: pd.DataFrame,
        stage: str,
        job_name: str,
        partition_date: str,
        filename: str = "data.parquet",
        schema: pa.Schema | None = None,
    ) -> str:
        """
        DataFrame을 Parquet으로 S3에 저장

        Args:
            df: 저장할 DataFrame
            stage: extract / transform
            job_name: Job 이름
            partition_date: 파티션 날짜
            filename: 파일명
            schema: PyArrow 스키마 (optional)

        Returns:
            저장된 S3 경로 (s3://bucket/key)
        """
        s3_key = self._build_s3_path(stage, job_name, partition_date, filename)

        # DataFrame을 Parquet 바이트로 변환
        buffer = io.BytesIO()
        table = pa.Table.from_pandas(df, schema=schema)
        pq.write_table(table, buffer, compression="snappy")
        buffer.seek(0)

        # S3에 업로드
        self.client.put_object(
            Bucket=self.bucket_name,
            Key=s3_key,
            Body=buffer.getvalue(),
        )

        return f"s3://{self.bucket_name}/{s3_key}"

    def _build_tenant_s3_path(
        self,
        tenant_id: str,
        stage: str,
        job_name: str,
        partition_date: str | None,
        filename: str,
    ) -> str:
        """
        테넌트별 S3 경로 생성

        Args:
            tenant_id: 테넌트 ID
            stage: extract / transfer / load
            job_name: Job 이름
            partition_date: 파티션 날짜 (YYYY-MM-DD), None이면 latest
            filename: 파일명

        Returns:
            S3 key 경로
        """
        base = self.tenant_base_path if self.tenant_base_path else f"project_id={tenant_id}"

        if partition_date:
            date_formatted = partition_date.replace("-", "")
            return f"{base}/{stage}/job_name={job_name}/date={date_formatted}/{filename}"
        else:
            return f"{base}/{stage}/job_name={job_name}/latest/{filename}"

    def write_parquet_for_tenant(
        self,
        df: pd.DataFrame,
        tenant_id: str,
        stage: str,
        job_name: str,
        partition_date: str | None = None,
        filename: str = "data.parquet",
        schema: pa.Schema | None = None,
    ) -> str:
        """
        DataFrame을 테넌트별 경로에 Parquet으로 저장

        Args:
            df: 저장할 DataFrame
            tenant_id: 테넌트 ID
            stage: extract / transfer / load
            job_name: Job 이름
            partition_date: 파티션 날짜 (None이면 latest)
            filename: 파일명
            schema: PyArrow 스키마 (optional)

        Returns:
            저장된 S3 경로 (s3://bucket/project_id={tenant_id}/...)
        """
        s3_key = self._build_tenant_s3_path(
            tenant_id, stage, job_name, partition_date, filename
        )

        # DataFrame을 Parquet 바이트로 변환
        buffer = io.BytesIO()
        table = pa.Table.from_pandas(df, schema=schema)
        pq.write_table(table, buffer, compression="snappy")
        buffer.seek(0)

        # S3에 업로드
        self.client.put_object(
            Bucket=self.bucket_name,
            Key=s3_key,
            Body=buffer.getvalue(),
        )

        return f"s3://{self.bucket_name}/{s3_key}"

    def read_parquet_for_tenant(
        self,
        tenant_id: str,
        stage: str,
        job_name: str,
        partition_date: str,
        filename: str = "data.parquet",
    ) -> pd.DataFrame:
        """
        테넌트별 경로에서 Parquet 파일 읽기

        Args:
            tenant_id: 테넌트 ID
            stage: extract / transform
            job_name: Job 이름
            partition_date: 파티션 날짜
            filename: 파일명

        Returns:
            pd.DataFrame: 로드된 데이터
        """
        s3_key = self._build_tenant_s3_path(
            tenant_id, stage, job_name, partition_date, filename
        )

        response = self.client.get_object(Bucket=self.bucket_name, Key=s3_key)
        buffer = io.BytesIO(response["Body"].read())

        table = pq.read_table(buffer)
        return table.to_pandas()

    def list_tenant_partitions(
        self, tenant_id: str, stage: str, job_name: str
    ) -> list[str]:
        """
        특정 테넌트/stage/job의 파티션 목록 조회

        Returns:
            파티션 날짜 목록 (YYYYMMDD 형식)
        """
        if self.tenant_base_path:
            prefix = f"{self.tenant_base_path}/{stage}/job_name={job_name}/"
        else:
            prefix = f"project_id={tenant_id}/{stage}/job_name={job_name}/"

        paginator = self.client.get_paginator("list_objects_v2")
        partitions = set()

        for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                parts = key.split("/")
                for part in parts:
                    if part.startswith("date="):
                        partitions.add(part.replace("date=", ""))

        return sorted(list(partitions))

    def read_parquet(
        self,
        stage: str,
        job_name: str,
        partition_date: str,
        filename: str = "data.parquet",
    ) -> pd.DataFrame:
        """
        S3에서 Parquet 파일 읽기

        Args:
            stage: extract / transform
            job_name: Job 이름
            partition_date: 파티션 날짜
            filename: 파일명

        Returns:
            pd.DataFrame: 로드된 데이터
        """
        s3_key = self._build_s3_path(stage, job_name, partition_date, filename)

        # S3에서 다운로드
        response = self.client.get_object(Bucket=self.bucket_name, Key=s3_key)
        buffer = io.BytesIO(response["Body"].read())

        # Parquet 읽기
        table = pq.read_table(buffer)
        return table.to_pandas()

    def read_parquet_from_path(self, s3_path: str) -> pd.DataFrame:
        """
        전체 S3 경로로 Parquet 파일 읽기

        Args:
            s3_path: s3://bucket/key 형식의 경로

        Returns:
            pd.DataFrame: 로드된 데이터
        """
        # s3://bucket/key 파싱
        path = s3_path.replace("s3://", "")
        bucket = path.split("/")[0]
        key = "/".join(path.split("/")[1:])

        response = self.client.get_object(Bucket=bucket, Key=key)
        buffer = io.BytesIO(response["Body"].read())

        table = pq.read_table(buffer)
        return table.to_pandas()

    def check_exists(
        self,
        stage: str,
        job_name: str,
        partition_date: str,
        filename: str = "data.parquet",
    ) -> bool:
        """파일 존재 여부 확인"""
        s3_key = self._build_s3_path(stage, job_name, partition_date, filename)

        try:
            self.client.head_object(Bucket=self.bucket_name, Key=s3_key)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            raise

    def delete_parquet(
        self,
        stage: str,
        job_name: str,
        partition_date: str,
        filename: str = "data.parquet",
    ) -> None:
        """Parquet 파일 삭제"""
        s3_key = self._build_s3_path(stage, job_name, partition_date, filename)
        self.client.delete_object(Bucket=self.bucket_name, Key=s3_key)

    def list_partitions(self, stage: str, job_name: str) -> list[str]:
        """
        특정 stage/job의 파티션 목록 조회

        Returns:
            파티션 날짜 목록 (YYYYMMDD 형식)
        """
        prefix = f"{stage}/job_name={job_name}/"

        paginator = self.client.get_paginator("list_objects_v2")
        partitions = set()

        for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                # date=YYYYMMDD 추출
                parts = key.split("/")
                for part in parts:
                    if part.startswith("date="):
                        partitions.add(part.replace("date=", ""))

        return sorted(list(partitions))
