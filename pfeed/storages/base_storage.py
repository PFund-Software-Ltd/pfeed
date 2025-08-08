from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    import pyarrow.fs as pa_fs
    from pfeed.data_handlers.base_data_handler import BaseDataHandler
    from pfeed.data_models.base_data_model import BaseDataModel
    from pfeed._typing import tStorage, tDataLayer, GenericData, StorageMetadata
    from pfeed.messaging.streaming_message import StreamingMessage

from abc import ABC, abstractmethod
from pathlib import Path

from pfeed.enums import StreamMode


class BaseStorage(ABC):
    def __new__(cls, *args, use_deltalake: bool=False, **kwargs):
        if use_deltalake:
            # Create a new class that inherits from both BaseStorage and DeltaLakeStorageMixin
            from pfeed.storages.deltalake_storage_mixin import DeltaLakeStorageMixin
            new_cls = type(
                f'DeltaLake{cls.__name__}',
                (cls, DeltaLakeStorageMixin),
                {}
            )
            return super(BaseStorage, new_cls).__new__(new_cls)
        return super().__new__(cls)

    def __init__(
        self,
        name: tStorage,
        data_layer: tDataLayer='CLEANED',
        data_domain: str='general_data',
        use_deltalake: bool=False,
        storage_options: dict | None=None,
        **storage_kwargs,
    ):
        from pfeed.enums import DataStorage, DataLayer
        self.name = DataStorage[name.upper()]
        self.data_layer = DataLayer[data_layer.upper()] if not isinstance(data_layer, DataLayer) else data_layer
        self.data_domain = data_domain.lower()
        self.use_deltalake = use_deltalake
        self._data_model: BaseDataModel | None = None
        self._data_handler: BaseDataHandler | None = None
        self._storage_options = storage_options or {}
        self._storage_kwargs = storage_kwargs

    @classmethod
    def from_data_model(
        cls,
        data_model: BaseDataModel, 
        data_layer: tDataLayer,
        data_domain: str,
        use_deltalake: bool=False, 
        storage_options: dict | None=None,
        stream_mode: StreamMode=StreamMode.FAST,
        delta_flush_interval: int=100,
        **storage_kwargs,
    ) -> BaseStorage:
        instance = cls(
            data_layer=data_layer,
            data_domain=data_domain,
            use_deltalake=use_deltalake, 
            storage_options=storage_options,
            **storage_kwargs,
        )
        instance.attach_data_model(data_model)
        instance.initialize_data_handler(stream_mode=stream_mode, delta_flush_interval=delta_flush_interval)
        return instance
    
    @abstractmethod
    def get_filesystem(self) -> pa_fs.FileSystem:
        pass
    
    def attach_data_model(self, data_model: BaseDataModel) -> None:
        '''
        Attach a data model to the storage and update related attributes.

        Args:
            data_model: data model is a detailed description of the data stored.
                if not provided, storage is just an empty vessel with basic properties, e.g. data_path,
                since it is not pointing to any data.
        '''
        self._data_model = data_model
    
    def initialize_data_handler(self, stream_mode: StreamMode, delta_flush_interval: int):
        data_handler_configs = {
            'data_layer': self.data_layer,
            'data_path': str(self.data_path),
            'filesystem': self.get_filesystem(),
            'storage_options': self._storage_options,
            'use_deltalake': self.use_deltalake,
            'stream_mode': stream_mode,
            'delta_flush_interval': delta_flush_interval,
        }
        self._data_handler = self.data_model.create_data_handler(**data_handler_configs)
    
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
    def data_path(self) -> Path:
        from pfeed.config import get_config
        config = get_config()
        return (
            Path(config.data_path)
            / f'data_layer={self.data_layer.name.lower()}'
            / f'data_domain={self.data_domain}'
        )
    
    def __str__(self):
        if self._data_model:
            return f'{self.name}:{self._data_model}'
        else:
            return f'{self.name}'
    
    def write_data(self, data: GenericData | StreamingMessage, streaming: bool=False):
        self.data_handler.write(data, streaming=streaming)
            
    def read_data(self, delta_version: int | None=None) -> tuple[GenericData | None, StorageMetadata]:
        '''
        Args:
            delta_version: version of the deltalake table to read, if None, read the latest version.
        '''
        data, metadata = self.data_handler.read(delta_version=delta_version)
        return data, metadata
