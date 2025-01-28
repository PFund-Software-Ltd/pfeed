from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    import pyarrow.fs as pa_fs
    from pfeed.data_handlers.base_data_handler import BaseDataHandler
    from pfeed.data_models.base_data_model import BaseDataModel
    from pfeed.typing.literals import tSTORAGE, tDATA_TOOL, tDATA_LAYER
    from pfeed.typing.core import tData

import logging
from abc import ABC, abstractmethod
from pathlib import Path

from pfeed.const.enums import DataStorage, DataLayer
from pfeed.data_models.market_data_model import MarketDataModel


class BaseStorage(ABC):
    def __init__(
        self,
        name: tSTORAGE,
        data_layer: tDATA_LAYER='cleaned',
        data_domain: str='general_data',
        use_deltalake: bool=False,
        **kwargs
    ):
        self.name = DataStorage[name.upper()]
        self.data_layer = DataLayer[data_layer.upper()]
        self.data_domain = data_domain
        self.use_deltalake = use_deltalake
        self._logger: logging.Logger | None = None
        self._data_model: BaseDataModel | None = None
        self._data_handler: BaseDataHandler | None = None
        self._kwargs = kwargs

    @classmethod
    def from_data_model(
        cls,
        data_model: BaseDataModel, 
        data_layer: tDATA_LAYER,
        data_domain: str,
        use_deltalake: bool=False, 
        **kwargs
    ) -> BaseStorage:
        instance = cls(
            data_layer=data_layer,
            data_domain=data_domain,
            use_deltalake=use_deltalake, 
            **kwargs
        )
        instance.attach_data_model(data_model)
        instance.initialize_logger()
        instance.initialize_data_handler()
        return instance
    
    @abstractmethod
    def get_file_system(self) -> pa_fs.FileSystem:
        pass
    
    def get_storage_options(self) -> dict:
        return {}
        
    def attach_data_model(self, data_model: BaseDataModel) -> None:
        '''
        Attach a data model to the storage and update related attributes.

        Args:
            data_model: data model is a detailed description of the data stored.
                if not provided, storage is just an empty vessel with basic properties, e.g. data_path,
                since it is not pointing to any data.
        '''
        self._data_model = data_model
    
    def initialize_data_handler(self):
        from pfeed.data_handlers import MarketDataHandler
        data_handler_configs = {
            'data_path': str(self.data_path),
            'file_system': self.get_file_system(),
            'storage_options': self.get_storage_options(),
        }
        if isinstance(self.data_model, MarketDataModel):
            data_handler_configs['use_deltalake'] = self.use_deltalake
            self._data_handler = MarketDataHandler(self.data_model, **data_handler_configs)
        else:
            raise NotImplementedError(f'No data handler is available for {type(self.data_model)}')
    
    def initialize_logger(self):
        name = self._data_model.source.name.lower()
        self._logger = logging.getLogger(f"{name}_data")
    
    @property
    def data_model(self) -> BaseDataModel:
        if not self._data_model:
            raise ValueError("No data model has been attached to this storage instance")
        return self._data_model
    
    @property
    def data_handler(self) -> BaseDataHandler:
        if not self._data_handler:
            raise ValueError("No data handler has been initialized for this storage instance")
        return self._data_handler

    @property
    def filename(self) -> str | None:
        return None if self.use_deltalake else self.data_model.full_filename
    
    @property
    def file_path(self) -> Path | str | None:
        if isinstance(self.data_path, Path):
            return self.data_path / self.storage_path / self.filename
        elif isinstance(self.data_path, str):
            return self.data_path + '/' + str(self.storage_path) + '/' + self.filename
        else:
            raise ValueError(f'{type(self.data_path)} is not supported')

    @property
    def storage_path(self) -> Path:
        return self.data_model.storage_path
    
    @property
    def data_path(self) -> Path:
        from pfeed.config import get_config
        config = get_config()
        return Path(config.data_path) / self.data_layer.name.lower() / self.data_domain
    
    def __str__(self):
        if self._data_model:
            return f'{self.name}:{self._data_model}'
        else:
            return f'{self.name}'
    
    def write_data(self, data: tData) -> bool:
        try:
            self.data_handler.write(data)
            return True
        except Exception:
            self._logger.exception(f'Failed to write data (type={type(data)}) to {self.name}')
            return False

    def read_data(self, data_tool: tDATA_TOOL='pandas', delta_version: int | None=None) -> tuple[tData | None, dict]:
        '''
        Args:
            delta_version: version of the deltalake table to read, if None, read the latest version.
        '''
        try:
            data, metadata = self.data_handler.read(data_tool=data_tool, delta_version=delta_version)
            return data, metadata
        except Exception:
            self._logger.exception(f'Failed to read data ({data_tool=}, {delta_version=}) from {self.name}')
            return None, {}
