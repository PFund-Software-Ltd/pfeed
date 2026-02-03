from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfeed.storages.base_storage import BaseStorage

from enum import StrEnum



class FileBasedDataStorage(StrEnum):
    CACHE = 'CACHE'
    LOCAL = 'LOCAL'
    HUGGINGFACE = 'HUGGINGFACE'
    # MINIO = 'MINIO'


class DatabaseDataStorage(StrEnum):
    DUCKDB = 'DUCKDB'
    LANCEDB = 'LANCEDB'
    # POSTGRESQL = 'POSTGRESQL'

    
class DataStorage(StrEnum):
    CACHE = FileBasedDataStorage.CACHE
    LOCAL = FileBasedDataStorage.LOCAL
    DUCKDB = DatabaseDataStorage.DUCKDB
    LANCEDB = DatabaseDataStorage.LANCEDB
    HUGGINGFACE = HF = FileBasedDataStorage.HUGGINGFACE
    # TODO:
    # POSTGRESQL = DatabaseDataStorage.POSTGRESQL
    # MINIO = FileBasedDataStorage.MINIO
    # S3 = 'S3'
    # AZURE = 'AZURE'
    # GCP = 'GCP'

    @property
    def storage_class(self) -> type[BaseStorage]:
        if self == DataStorage.LOCAL:
            from pfeed.storages.local_storage import LocalStorage
            return LocalStorage
        elif self == DataStorage.CACHE:
            from pfeed.storages.cache_storage import CacheStorage
            return CacheStorage
        elif self == DataStorage.DUCKDB:
            from pfeed.storages.duckdb_storage import DuckDBStorage
            return DuckDBStorage
        elif self == DataStorage.LANCEDB:
            from pfeed.storages.lancedb_storage import LanceDBStorage
            return LanceDBStorage
        elif self == DataStorage.HUGGINGFACE:
            from pfeed.storages.huggingface_storage import HuggingFaceStorage
            return HuggingFaceStorage
        # elif self == DataStorage.POSTGRESQL:
        #     from pfeed.storages.postgresql_storage import PostgreSQLStorage
        #     return PostgreSQLStorage
        else:
            raise ValueError(f'{self=} is not supported')
