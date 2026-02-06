"""
모든 테넌트 통합 Definitions
단일 Code Location 모드에서 사용

workspace.yaml에서 아래와 같이 설정:
load_from:
  - python_module:
      module_name: etl.all
"""

from etl import build_all_definitions

# 모든 테넌트의 Definitions 빌드 (단일 Code Location)
defs = build_all_definitions()
