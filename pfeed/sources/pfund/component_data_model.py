from __future__ import annotations
from typing import TYPE_CHECKING, Any, ClassVar, Literal
if TYPE_CHECKING:
    from pfund.typing import Component

import datetime
import hashlib
import inspect
from pathlib import Path

from pydantic import Field, field_validator, BaseModel, ConfigDict

from pfund.entities.products.product_base import BaseProduct
from pfund.datas.resolution import Resolution
from pfund.enums import ArtifactType, ComponentType, Environment, RunMode
from pfeed.data_models.base_data_model import BaseDataModel, BaseMetadataModel
from pfeed.sources.pfund.component_data_handler import ComponentDataHandler
from pfeed.sources.pfund.source import PFundSource


class ArtifactMetadata(BaseModel):
    model_config = ConfigDict(extra="allow")  # forward-compat

    # known top-level keys (typed lightly)
    signature: tuple[list[Any], dict[str, Any]]  # (args, kwargs)
    config: dict[str, Any]
    params: dict[str, Any]
    settings: dict[str, Any]
    datas: list[dict[str, Any]]
    models: list[dict[str, Any]]
    features: list[dict[str, Any]]
    indicators: list[dict[str, Any]]

    # extrinsic audit (not queried)
    env: str
    run_mode: str
    name: str
    

class BaseArtifact(BaseModel):
    '''
    Data model for pfund to store its components data, e.g. computed features, model predictions, etc.
    '''
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid")

    data_handler_class: ClassVar[type[ComponentDataHandler]] = ComponentDataHandler
    metadata_class: ClassVar[type[ArtifactMetadata]] = ArtifactMetadata

    artifact_type: ArtifactType
    extension: Literal['.joblib', '.pth', '.py', '.delta']
    size_bytes: int | None = None

    component_type: ComponentType
    class_name: str


class SourceArtifact(BaseArtifact):
    artifact_type: Literal[ArtifactType.source] = ArtifactType.source
    extension: Literal['.py'] = '.py'
    # the .py bytes ARE the identity — sha256 of file contents
    source_hash: str

    @classmethod
    def _compute_hash(cls, component: Component) -> str:
        path = Path(inspect.getfile(type(component)))
        return hashlib.sha256(path.read_bytes()).hexdigest()[:16]


class ModelArtifact(BaseArtifact):
    artifact_type: Literal[ArtifactType.model] = ArtifactType.model
    extension: Literal['.joblib', '.pth']
    model_hash: str
    source_hash: str
    data_hash: str
    
    resolution: Resolution
    df_form: Literal['wide', 'long']
    data_start_date: datetime.date
    data_end_date: datetime.date


class DataArtifact(BaseArtifact):
    artifact_type: Literal[ArtifactType.data] = ArtifactType.data
    # NOTE: .delta is just a convention for the deltalake folder
    extension: Literal['.delta'] = '.delta'  # deltalake's folder
    source_hash: str
    data_hash: str
    model_hash: str | None = None
    
    resolution: Resolution
    df_form: Literal['wide', 'long']
    data_start_date: datetime.date
    data_end_date: datetime.date
    
    
# TEMP: data hash draft
'''
1. Canonicalize the input so equivalent inputs serialize identically:
Sort dict keys recursively.
Decide list-order policy: if order is semantically meaningless (sets, tags, subscribed datas), sort lists by their canonical form too. If order matters (sequence of operations), don't.
2. Hash the canonical form with sha256 (stdlib, deterministic across processes/machines/Python versions).

import hashlib
import json
from typing import Any

def _canonical(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: _canonical(obj[k]) for k in sorted(obj)}
    if isinstance(obj, (list, tuple)):
        items = [_canonical(x) for x in obj]
        # order-insensitive: sort by serialized form
        return sorted(items, key=lambda x: json.dumps(x, sort_keys=True, default=str))
    return obj

def stable_hash(obj: Any, length: int = 12) -> str:
    canonical = json.dumps(_canonical(obj), sort_keys=True, default=str, separators=(',', ':'))
    return hashlib.sha256(canonical.encode()).hexdigest()[:length]
    
data_hash = stable_hash([data.to_dict() for data in self.get_datas()])
'''