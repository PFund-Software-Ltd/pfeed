from __future__ import annotations

import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field


def _utc_now() -> datetime.datetime:
    return datetime.datetime.now(datetime.UTC)


class ComponentMetadata(BaseModel):
    """A persisted snapshot of a pfund component's definition."""

    class_name: str
    source_sha256: str
    component_name: str
    resolution: str
    df_form: Literal["wide", "long"]
    # NOTE: signal_cols is always empty when metadata is being written, skip it for now
    # signal_cols: list[str]
    model: str | None = None
    signature: tuple[tuple[Any, ...], dict[str, Any]]
    config: dict[str, Any]
    params: dict[str, Any]
    datas: list[dict[str, Any]]
    strategies: list[str] = Field(default_factory=list)
    models: list[str] = Field(default_factory=list)
    features: list[str] = Field(default_factory=list)


class RunMetadata(BaseModel):
    """The run context in which the component snapshot was produced."""

    env: str
    project_name: str
    run_id: str
    run_mode: str
    data_start: datetime.date
    data_end: datetime.date
    settings: dict[str, Any]


class PFundComponentDataMetadata(BaseModel):
    """The component-level manifest persisted as ``metadata.json``."""

    schema_version: Literal[1] = 1
    created_at: datetime.datetime = Field(default_factory=_utc_now, frozen=True)
    component_id: str
    component_type: str
    component: ComponentMetadata
    run: RunMetadata
