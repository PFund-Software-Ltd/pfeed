from __future__ import annotations

import datetime
from typing import Any, ClassVar, Literal

from pydantic import Field, field_validator

from pfeed.data_models.base_data_model import BaseDataModel
from pfeed.sources.pfund.component_data_handler import ComponentDataHandler
from pfund.datas.resolution import Resolution
from pfund.enums import ArtifactType, ComponentType, Environment, RunMode, RunStage
from pfund.typing import ComponentName


class PFundComponentDataModel(BaseDataModel):
    data_handler_class: ClassVar[type[ComponentDataHandler]] = ComponentDataHandler

    artifact_type: ArtifactType | str
    extension: Literal[".joblib", ".safetensors", ".py", ".delta"]

    env: Environment | str
    run_stage: RunStage | str
    project_name: str
    run_id: str
    # fields from pfund component.to_dict()
    class_name: str
    component_name: str
    data_start: datetime.date
    data_end: datetime.date
    resolution: Resolution | str
    df_form: Literal["wide", "long"]
    component_type: ComponentType | str
    signal_cols: list[str]
    # fields for metadata
    run_mode: RunMode | str
    signature: tuple[tuple[Any, ...], dict[str, Any]]  # (args, kwargs)
    config: dict[str, Any]
    params: dict[str, Any]
    settings: dict[str, Any]
    datas: list[dict[str, Any]]
    strategies: list[ComponentName] = Field(default_factory=list)
    models: list[ComponentName] = Field(default_factory=list)
    features: list[ComponentName] = Field(default_factory=list)
    indicators: list[ComponentName] = Field(default_factory=list)

    @field_validator("artifact_type", mode="before")
    @classmethod
    def _validate_artifact_type(cls, v: str | ArtifactType) -> ArtifactType:
        if isinstance(v, str):
            return ArtifactType[v.lower()]
        return v

    @field_validator("env", mode="before")
    @classmethod
    def _validate_env(cls, v: str | Environment) -> Environment:
        if isinstance(v, str):
            return Environment[v.upper()]
        return v

    @field_validator("run_mode", mode="before")
    @classmethod
    def _validate_run_mode(cls, v: str | RunMode) -> RunMode:
        if isinstance(v, str):
            return RunMode[v.upper()]
        return v

    @field_validator("run_stage", mode="before")
    @classmethod
    def _validate_run_stage(cls, v: str | RunStage) -> RunStage:
        if isinstance(v, str):
            return RunStage[v.lower()]
        return v

    @field_validator("resolution", mode="before")
    @classmethod
    def _validate_resolution(cls, v: str | Resolution) -> Resolution:
        if isinstance(v, str):
            return Resolution(v)
        return v

    @field_validator("component_type", mode="before")
    @classmethod
    def _validate_component_type(cls, v: str | ComponentType) -> ComponentType:
        if isinstance(v, str):
            return ComponentType[v.lower()]
        return v


class SourceArtifact(PFundComponentDataModel):
    artifact_type: Literal[ArtifactType.source] = ArtifactType.source
    extension: Literal[".py"] = ".py"
    filename: str  # e.g. "my_alpha.py"


class ModelArtifact(PFundComponentDataModel):
    artifact_type: Literal[ArtifactType.model] = ArtifactType.model
    extension: Literal[".joblib", ".safetensors"]


class DataArtifact(PFundComponentDataModel):
    """The output frame of a component (strategy / model / feature / indicator)."""

    artifact_type: Literal[ArtifactType.data] = ArtifactType.data
    # .delta is just the convention for the deltalake folder
    extension: Literal[".delta"] = ".delta"
