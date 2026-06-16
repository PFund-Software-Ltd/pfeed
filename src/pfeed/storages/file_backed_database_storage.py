from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pathlib import Path

    from pfeed.utils.file_path import FilePath

from abc import ABC

from pfeed.enums import DataLayer, DataStorage
from pfeed.enums.data_storage import FileBasedDataStorage
from pfeed.storages.database_storage import DatabaseStorage


class FileBackedDatabaseStorage(DatabaseStorage, ABC):
    """Database storage that sits on top of a file-based backend (e.g. DuckDB, LanceDB).

    The file backend is deduced from ``data_path`` rather than configured explicitly:
    ``hf://…`` -> HUGGINGFACE, a local path containing "cache" -> CACHE, otherwise LOCAL.
    """

    @staticmethod
    def _deduce_file_backend(
        data_path: FilePath | Path | str | None,
    ) -> FileBasedDataStorage:
        if data_path is None:
            return FileBasedDataStorage.LOCAL

        from pfeed.utils.file_path import FilePath

        file_path = (
            data_path if isinstance(data_path, FilePath) else FilePath(data_path)
        )
        if file_path.is_hf:
            return FileBasedDataStorage.HUGGINGFACE
        if file_path.is_cloud:
            raise NotImplementedError(f"no file backend for cloud path: {data_path}")
        # local filesystem: a cache directory is the only behavioural variant of LOCAL
        if "cache" in str(file_path).lower():
            return FileBasedDataStorage.CACHE
        return FileBasedDataStorage.LOCAL

    def __new__(
        cls,
        data_path: str | None,
        data_layer: DataLayer = DataLayer.CLEANED,
        data_domain: str = "MARKET_DATA",
        storage_options: dict[str, Any] | None = None,
    ):
        file_backend = cls._deduce_file_backend(data_path)
        FileBasedStorage = DataStorage[str(file_backend).upper()].storage_class
        new_cls = type(
            cls.__name__, (cls, FileBasedStorage), {"__module__": cls.__module__}
        )
        return object.__new__(new_cls)
