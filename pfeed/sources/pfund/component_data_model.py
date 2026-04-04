from __future__ import annotations
from typing import ClassVar

from pydantic import field_validator

from pfund.enums import ComponentType, Environment
from pfeed.data_models.base_data_model import BaseDataModel, BaseMetadataModel
from pfeed.sources.pfund.component_data_handler import ComponentDataHandler


class ComponentMetadataModel(BaseMetadataModel):
    pass


class ComponentDataModel(BaseDataModel):
    '''
    Data model for pfund to store its components data, e.g. computed features, model predictions, etc.
    '''
    data_handler_class: ClassVar[type[ComponentDataHandler]] = ComponentDataHandler
    metadata_class: ClassVar[type[ComponentMetadataModel]] = ComponentMetadataModel

    env: Environment
    component_type: ComponentType
    component_name: str
    
    @field_validator('component_name', mode='before')
    @classmethod
    def create_component_name(cls, v):
        from pfund_kit.utils.text import to_snake_case
        return to_snake_case(v)
    