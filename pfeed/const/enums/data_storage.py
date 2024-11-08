from enum import StrEnum


class DataStorage(StrEnum):
    CACHE = 'CACHE'
    LOCAL = 'LOCAL'
    MINIO = 'MINIO'