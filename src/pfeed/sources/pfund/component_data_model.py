from __future__ import annotations

import datetime
from typing import Any, ClassVar, Literal

from pydantic import Field, field_validator

from pfeed.data_models.base_data_model import BaseDataModel
from pfeed.sources.pfund.component_data_handler import PFundComponentDataHandler
from pfund.datas.resolution import Resolution
from pfund.enums import ArtifactType, ComponentType, Environment, RunMode
from pfund.typing import ComponentName


# REVIEW: too many fields?
class PFundComponentDataModel(BaseDataModel):
    DataHandler: ClassVar[type[PFundComponentDataHandler]] = PFundComponentDataHandler

    artifact_type: ArtifactType
    extension: Literal[
        ".joblib",  # sklearn model
        ".safetensors",  # torch/jax model
        ".py",  # python source code
        ".delta",  # trading_df delta table
        ".pth",  # pytorch checkpoint
        ".pkl",  # jax checkpoint
    ]

    env: Environment | str
    project_name: str
    run_name: str = Field(
        description="""
        mtflow\'s run_id (e.g. "run_001") if mtflow is used, otherwise it is "default_run" by default in pfund
        """
    )
    # fields from pfund component.to_dict()
    class_name: str
    component_name: str
    data_start: datetime.date
    data_end: datetime.date
    resolution: Resolution | str
    df_form: Literal["wide", "long"]
    component_type: ComponentType | str
    signal_cols: list[str]
    model: str | None = None  # underlying model class, for model components
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


class CheckpointArtifact(PFundComponentDataModel):
    artifact_type: Literal[ArtifactType.checkpoint] = ArtifactType.checkpoint
    extension: Literal[".pth", ".pkl"]
    step: int = Field(ge=0)


class DataArtifact(PFundComponentDataModel):
    """The output frame of a component (strategy / model / feature)."""

    artifact_type: Literal[ArtifactType.data] = ArtifactType.data
    # .delta is just the convention for the deltalake folder
    extension: Literal[".delta"] = ".delta"
