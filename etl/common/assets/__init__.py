"""
Common Assets Module - 공용 Asset 로직

이 모듈은 모든 테넌트에서 공유하는 기본 ETL 로직을 포함합니다.

모듈 구성:
- extract.py: 기본 Extract(Input Load) SQL 쿼리 및 설정
- transfer.py: 기본 Transfer 로직 함수
- load.py: 기본 Load(Output Save) 설정

사용법:
- AssetFactory가 TenantLoader를 통해 이 로직을 사용
- 테넌트별 커스텀이 필요하면 tenants/{tenant_id}/assets/에서 오버라이드
"""

from etl.common.assets import extract, transfer, load

__all__ = ["extract", "transfer", "load"]
