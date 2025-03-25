from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfeed.typing import GenericData
    from pfeed.typing import tDATA_TOOL
    from pfeed.data_models.base_data_model import BaseDataModel

from abc import ABC, abstractmethod


class BaseDataHandler(ABC):
    def __init__(self, data_model: BaseDataModel, data_path: str):
        self._data_model = data_model
        self._data_path = data_path

    @abstractmethod
    def write(self, data: GenericData, *args, **kwargs):
        pass

    @abstractmethod
    def read(self, data_tool: tDATA_TOOL='polars', **kwargs) -> GenericData | None:
        pass

    @abstractmethod
    def _validate_schema(self, data: GenericData) -> GenericData:
        pass

    def _create_file_paths(self, data_model: BaseDataModel | None=None) -> str:
        data_model = data_model or self._data_model
        return self._data_path + '/' + str(data_model.storage_path) + '/' + data_model.filename
