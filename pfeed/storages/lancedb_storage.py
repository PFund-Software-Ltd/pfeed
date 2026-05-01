from __future__ import annotations
from typing import Any

from pfeed.enums import IOFormat, DataLayer, DataStorage
from pfeed.enums.data_storage import FileBasedDataStorage
from pfeed.storages.database_storage import DatabaseStorage


class LanceDBStorage(DatabaseStorage):
    SUPPORTED_IO_FORMATS = [IOFormat.LANCEDB]

    def __new__(cls, *args: Any, file_backend: FileBasedDataStorage | str = FileBasedDataStorage.LOCAL, **kwargs: Any):
        FileBasedStorage = DataStorage[file_backend.upper()].storage_class
        new_cls = type(cls.__name__, (cls, FileBasedStorage), {'__module__': cls.__module__})
        return object.__new__(new_cls)
    
    def __init__(
        self, 
        data_path: str | None = None,
        data_layer: DataLayer=DataLayer.CLEANED,
        data_domain: str = 'MARKET_DATA',
        storage_options: dict[str, Any] | None = None,
        file_backend: FileBasedDataStorage | str = FileBasedDataStorage.LOCAL,
        **kwargs: Any,  # additional kwargs for compatibility with other storages
    ):
        super().__init__(
            data_path=data_path,
            data_layer=data_layer,
            data_domain=data_domain,
            storage_options=storage_options,
            **kwargs,
        )
    
    def _create_uri(self) -> str:
        return ''
