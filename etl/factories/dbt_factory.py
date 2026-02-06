"""
dbt Factory - 테넌트별 dbt Asset/Resource 동적 생성

dagster-dbt 통합:
- TenantDbtTranslator: dbt 모델을 테넌트 Asset Key 체계로 매핑
- DbtFactory: dbt 프로젝트 경로 관리, DbtCliResource/Asset 생성
"""

from pathlib import Path
from typing import Any

from dagster import AssetKey, AssetsDefinition
from dagster_dbt import (
    DagsterDbtTranslator,
    DbtCliResource,
    DbtProject,
    dbt_assets,
)

from etl.config.tenant_config import TenantConfig


class TenantDbtTranslator(DagsterDbtTranslator):
    """테넌트별 dbt → Dagster Asset Key 매핑

    매핑 규칙:
    - dbt model → [tenant_id, "dbt", model_name]
    - dbt source → [tenant_id, "extract", source_name]  (extract asset 의존성 연결)
    - group_name → tenant_id
    """

    def __init__(self, tenant_id: str):
        super().__init__()
        self._tenant_id = tenant_id

    def get_asset_key(self, dbt_resource_props: dict[str, Any]) -> AssetKey:
        resource_type = dbt_resource_props.get("resource_type", "")

        if resource_type == "source":
            # dbt source → extract asset에 매핑
            source_name = dbt_resource_props.get("name", "unknown")
            return AssetKey([self._tenant_id, "extract", source_name])

        # dbt model/seed/snapshot → dbt 네임스페이스
        model_name = dbt_resource_props.get("name", "unknown")
        return AssetKey([self._tenant_id, "dbt", model_name])

    def get_group_name(self, dbt_resource_props: dict[str, Any]) -> str:
        return self._tenant_id

    def get_description(self, dbt_resource_props: dict[str, Any]) -> str:
        resource_type = dbt_resource_props.get("resource_type", "model")
        name = dbt_resource_props.get("name", "unknown")
        description = dbt_resource_props.get("description", "")
        base = f"[{self._tenant_id}] dbt {resource_type}: {name}"
        if description:
            base += f"\n\n{description}"
        return base


class DbtFactory:
    """테넌트별 dbt 프로젝트 관리 및 Asset/Resource 생성"""

    def __init__(self, tenant: TenantConfig, tenants_dir: Path | None = None):
        self.tenant = tenant
        self.tenant_id = tenant.id
        self.dbt_config = tenant.dbt
        self.tenants_dir = tenants_dir or Path(__file__).parent.parent / "tenants"

    def get_dbt_project_dir(self) -> Path:
        """dbt 프로젝트 디렉토리 반환"""
        if self.dbt_config.project_dir:
            return Path(self.dbt_config.project_dir)
        return self.tenants_dir / self.tenant_id / "dbt"

    def has_dbt_project(self) -> bool:
        """dbt_project.yml 존재 여부 확인"""
        project_dir = self.get_dbt_project_dir()
        return (project_dir / "dbt_project.yml").exists()

    def create_dbt_project(self) -> DbtProject:
        """DbtProject 인스턴스 생성"""
        project_dir = self.get_dbt_project_dir()
        return DbtProject(
            project_dir=project_dir,
            profiles_dir=project_dir,
            target=self.dbt_config.target,
        )

    def create_dbt_cli_resource(self) -> DbtCliResource:
        """테넌트용 DbtCliResource 생성"""
        project_dir = self.get_dbt_project_dir()
        return DbtCliResource(
            project_dir=project_dir,
            profiles_dir=project_dir,
            target=self.dbt_config.target,
        )

    def create_dbt_assets(self) -> list[AssetsDefinition]:
        """테넌트의 dbt Asset 생성

        dbt manifest.json을 기반으로 Dagster Asset을 생성합니다.
        manifest가 없으면 dbt parse를 실행하여 생성합니다.

        Returns:
            dbt AssetsDefinition 리스트
        """
        if not self.dbt_config.enabled or not self.has_dbt_project():
            return []

        project_dir = self.get_dbt_project_dir()
        dbt_project = self.create_dbt_project()

        # manifest.json 준비 (없으면 개발 모드에서 자동 생성)
        dbt_project.prepare_if_dev()
        manifest_path = dbt_project.manifest_path

        if not manifest_path.exists():
            print(
                f"[{self.tenant_id}] dbt manifest not found at {manifest_path}. "
                f"Run 'dbt parse --profiles-dir {project_dir}' to generate it."
            )
            return []

        translator = TenantDbtTranslator(self.tenant_id)
        tenant_id = self.tenant_id
        tenant_tags = self.tenant.tags

        # dbt select/exclude 설정 수집
        select, exclude = self._build_dbt_selection()

        @dbt_assets(
            manifest=manifest_path,
            select=select,
            exclude=exclude or None,
            name=f"{tenant_id}_dbt_assets",
            dagster_dbt_translator=translator,
            project=dbt_project,
        )
        def _tenant_dbt_assets(context, dbt_cli: DbtCliResource):
            yield from dbt_cli.cli(["build"], context=context).stream()

        return [_tenant_dbt_assets]

    def _build_dbt_selection(self) -> tuple[str, str]:
        """파이프라인 설정에서 dbt select/exclude 문자열 수집

        Returns:
            (select, exclude) 튜플
        """
        selects = []
        excludes = []

        for name, pipeline_config in self.tenant.assets.pipelines.items():
            if pipeline_config.has_dbt_transform and pipeline_config.dbt_transform:
                dbt_tf = pipeline_config.dbt_transform
                if dbt_tf.dbt_select:
                    selects.append(dbt_tf.dbt_select)
                if dbt_tf.dbt_exclude:
                    excludes.append(dbt_tf.dbt_exclude)

        select = " ".join(selects) if selects else "fqn:*"
        exclude = " ".join(excludes)

        return select, exclude
