from __future__ import annotations
from typing import TYPE_CHECKING, Literal
if TYPE_CHECKING:
    from pathlib import Path

from pfeed.enums import IOFormat, DataLayer, DataStorage
from pfeed.enums.data_storage import FileBasedDataStorage
from pfeed.storages.database_storage import DatabaseStorage
from pfeed._io.lancedb_io import LanceDBIO


class LanceDBStorage(DatabaseStorage):
    SUPPORTED_IO_FORMATS = [IOFormat.LANCEDB]

    def __new__(cls, *args, data_storage: FileBasedDataStorage | str = FileBasedDataStorage.LOCAL, **kwargs):
        FileBasedStorage = DataStorage[data_storage.upper()].storage_class
        new_cls = type(cls.__name__, (cls, FileBasedStorage), {'__module__': cls.__module__})
        return object.__new__(new_cls)
    
    def __init__(
        self, 
        data_path: Path | str | None = None,
        data_layer: DataLayer=DataLayer.CLEANED,
        data_domain: str | Literal['MARKET_DATA', 'NEWS_DATA'] = 'MARKET_DATA',
        data_storage: FileBasedDataStorage | str = FileBasedDataStorage.LOCAL,
        storage_options: dict | None = None,
    ):
        super().__init__(
            data_path=data_path,
            data_layer=data_layer,
            data_domain=data_domain,
            storage_options=storage_options,
        )
    
    def _create_uri(self) -> str:
        return ''
    
    def with_io(self, io_options: dict | None = None, **kwargs) -> LanceDBIO:
        '''
        Args:
            kwargs: Unused parameters accepted for compatibility with other storage backends
        '''
        return super().with_io(io_options=io_options)
    
    def _create_io(self, io_options: dict | None = None, **kwargs) -> LanceDBIO:
        '''
        Args:
            kwargs: Unused parameters accepted for compatibility with other storage backends
        '''
        return LanceDBIO(
            filesystem=self.get_filesystem(),
            storage_options=self.storage_options,
            io_options=io_options,
        )
