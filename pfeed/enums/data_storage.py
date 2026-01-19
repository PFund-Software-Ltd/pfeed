from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfeed.storages.base_storage import BaseStorage
    from pfeed.enums import IOFormat

from enum import StrEnum



class LocalDataStorage(StrEnum):
    CACHE = 'CACHE'
    LOCAL = 'LOCAL'
    # MINIO = 'MINIO'


class DatabaseStorage(StrEnum):
    DUCKDB = 'DUCKDB'
    LANCEDB = 'LANCEDB'
    POSTGRESQL = 'POSTGRESQL'

    
class DataStorage(StrEnum):
    CACHE = LocalDataStorage.CACHE
    LOCAL = LocalDataStorage.LOCAL
    DUCKDB = DatabaseStorage.DUCKDB
    LANCEDB = DatabaseStorage.LANCEDB
    POSTGRESQL = DatabaseStorage.POSTGRESQL
    # TODO:
    # MINIO = LocalDataStorage.MINIO
    # HUGGINGFACE = HF = 'HUGGINGFACE'
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
        # elif self == DataStorage.POSTGRESQL:
        #     from pfeed.storages.postgresql_storage import PostgreSQLStorage
        #     return PostgreSQLStorage
        else:
            raise ValueError(f'{self=} is not supported')

    @property
    def default_io_format(self) -> IOFormat:
        from pfeed.enums import IOFormat
        if self == DataStorage.CACHE:
            return IOFormat.PARQUET
        elif self == DataStorage.LOCAL:
            return IOFormat.DELTALAKE
        elif self == DataStorage.DUCKDB:
            return IOFormat.DUCKDB
        elif self == DataStorage.LANCEDB:
            return IOFormat.LANCEDB
        elif self == DataStorage.POSTGRESQL:
            return IOFormat.POSTGRESQL
        else:
            raise ValueError(f'{self=} is not supported')