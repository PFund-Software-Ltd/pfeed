from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfeed._typing import GenericData
    from pfeed._typing import tDataTool
    from pfeed.data_models.base_data_model import BaseDataModel
    from pfeed.enums import DataLayer

from abc import ABC, abstractmethod


class BaseDataHandler(ABC):
    def __init__(
        self, 
        data_model: BaseDataModel, 
        data_layer: DataLayer, 
        data_path: str, 
        use_deltalake: bool,
        storage_options: dict | None=None,
    ):
        self._data_model = data_model
        self._data_layer = data_layer
        self._data_path = data_path
        self._use_deltalake = use_deltalake
        self._storage_options = storage_options or {}
        
    @abstractmethod
    def write(self, data: GenericData, *args, **kwargs):
        pass

    @abstractmethod
    def read(self, data_tool: tDataTool='polars', **kwargs) -> GenericData | None:
        pass

    @abstractmethod
    def _validate_schema(self, data: GenericData) -> GenericData:
        pass

    @abstractmethod
    def _create_file_paths(self, data_model: BaseDataModel | None=None) -> list[str]:
        pass