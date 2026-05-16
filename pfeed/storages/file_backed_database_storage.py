from __future__ import annotations
from typing import TYPE_CHECKING, Any, Self

if TYPE_CHECKING:
    from pfeed.storages.storage_config import StorageConfig
    from pfeed._io.database_io import DatabaseIO
    from pfeed._io.io_config import IOConfig

from abc import ABC

from pfeed.enums import DataLayer, DataStorage
from pfeed.enums.data_storage import FileBasedDataStorage
from pfeed.storages.database_storage import DatabaseStorage


class FileBackedDatabaseStorage(DatabaseStorage, ABC):
    """Database storage that sits on top of a file-based backend (e.g. DuckDB, LanceDB)."""

    def __new__(
        cls,
        data_path: str | None = None,
        data_layer: DataLayer = DataLayer.CLEANED,
        data_domain: str = 'MARKET_DATA',
        storage_options: dict[str, Any] | None = None,
        file_backend: FileBasedDataStorage | str = FileBasedDataStorage.LOCAL,
    ):
        FileBasedStorage = DataStorage[str(file_backend).upper()].storage_class
        new_cls = type(cls.__name__, (cls, FileBasedStorage), {'__module__': cls.__module__})
        return object.__new__(new_cls)

    def __init__(
        self,
        data_path: str | None = None,
        data_layer: DataLayer = DataLayer.CLEANED,
        data_domain: str = 'MARKET_DATA',
        storage_options: dict[str, Any] | None = None,
        file_backend: FileBasedDataStorage | str = FileBasedDataStorage.LOCAL,
    ):
        # file_backend is consumed by __new__; it does not propagate to super().__init__.
        super().__init__(
            data_path=data_path,
            data_layer=data_layer,
            data_domain=data_domain,
            storage_options=storage_options,
        )

    @classmethod
    def from_storage_config(cls, storage_config: StorageConfig) -> Self:
        return cls(**storage_config.model_dump(exclude={'storage'}))

    def with_io(self, io_config: IOConfig) -> DatabaseIO:
        supports_only_one_io_format = len(self.SUPPORTED_IO_FORMATS) == 1
        # e.g. DuckDB only supports DuckDBIO, always use it, regardless of the input io_config
        if supports_only_one_io_format:
            io_config = io_config.model_copy(update={'io_format': self.SUPPORTED_IO_FORMATS[0]})
        else:
            raise NotImplementedError(f"Multiple IO formats are not supported for {self.__class__.__name__}")
        return super().with_io(io_config=io_config)  # pyright: ignore[reportReturnType]
