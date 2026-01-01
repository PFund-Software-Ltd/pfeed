from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfeed.storages.base_storage import BaseStorage

from enum import StrEnum


class LocalDataStorage(StrEnum):
    CACHE = 'CACHE'
    LOCAL = 'LOCAL'
    MINIO = 'MINIO'

    
class DataStorage(StrEnum):
    CACHE = LocalDataStorage.CACHE
    LOCAL = LocalDataStorage.LOCAL
    MINIO = LocalDataStorage.MINIO
    # TODO:
    # HUGGINGFACE = HF = 'HUGGINGFACE'
    # S3 = 'S3'
    # AZURE = 'AZURE'
    # GCP = 'GCP'

    @property
    def storage_class(self) -> type[BaseStorage]:
        from pfeed.storages.local_storage import LocalStorage
        from pfeed.storages.minio_storage import MinioStorage
        from pfeed.storages.cache_storage import CacheStorage
        return {
            DataStorage.LOCAL: LocalStorage,
            DataStorage.CACHE: CacheStorage,
            DataStorage.MINIO: MinioStorage,
        }[self]
