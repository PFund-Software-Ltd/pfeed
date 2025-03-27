from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfeed.storages.base_storage import BaseStorage

from enum import StrEnum


class LocalDataStorage(StrEnum):
    CACHE = 'CACHE'
    LOCAL = 'LOCAL'
    MINIO = 'MINIO'
    DUCKDB = 'DUCKDB'

    
class DataStorage(StrEnum):
    CACHE = LocalDataStorage.CACHE
    LOCAL = LocalDataStorage.LOCAL
    MINIO = LocalDataStorage.MINIO
    DUCKDB = LocalDataStorage.DUCKDB
    # TODO:
    # HUGGINGFACE = HF = 'HUGGINGFACE'
    # S3 = 'S3'
    # AZURE = 'AZURE'
    # GCP = 'GCP'

    @property
    def storage_class(self) -> type[BaseStorage]:
        from pfeed.storages import (
            LocalStorage,
            CacheStorage,
            MinioStorage,
            DuckDBStorage,
        )
        return {
            DataStorage.LOCAL: LocalStorage,
            DataStorage.CACHE: CacheStorage,
            DataStorage.MINIO: MinioStorage,
            DataStorage.DUCKDB: DuckDBStorage,
        }[self]
