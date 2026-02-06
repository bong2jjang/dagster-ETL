"""
Default Tenant - 기본 테넌트
공용 코드(common/)만 사용, 커스텀 없음
"""

TENANT_ID = "default"
TENANT_NAME = "Default Tenant"

# 커스텀 Extract 쿼리 (없으면 common 사용)
CUSTOM_EXTRACT_QUERIES = None

# 커스텀 Transform 함수 (없으면 common 사용)
CUSTOM_TRANSFORM_FUNCTIONS = None

# 커스텀 Load 설정 (없으면 common 사용)
CUSTOM_LOAD_CONFIGS = None
