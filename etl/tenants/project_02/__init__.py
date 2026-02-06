"""
Project 02 테넌트 - 공용 로직 사용 (커스텀 없음)
커스텀 로직이 필요하면 assets/transfer.py에 함수를 추가하고
CUSTOM_TRANSFER_FUNCTIONS에 등록하세요.
"""

TENANT_ID = "project_02"
TENANT_NAME = "Project 02"

# 커스텀 Extract 쿼리 (필요시 오버라이드)
CUSTOM_EXTRACT_QUERIES = None

# 커스텀 Transfer 함수 (필요시 등록)
# from etl.tenants.project_02.assets.transfer import (
#     transform_aps_wip_logic as project_02_wip_logic,
# )
CUSTOM_TRANSFER_FUNCTIONS = None

# 커스텀 Load 설정 (필요시 등록)
CUSTOM_LOAD_CONFIGS = None
