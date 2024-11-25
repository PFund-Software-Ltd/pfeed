from enum import StrEnum


class DataStorage(StrEnum):
    CACHE = 'CACHE'
    LOCAL = 'LOCAL'
    MINIO = 'MINIO'
    S3 = 'S3'
    AZURE = 'AZURE'
    GCP = 'GCP'
