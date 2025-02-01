import os
import datetime

import pyarrow.fs as pa_fs
from minio import Minio

from pfeed.storages.minio_storage import MinioStorage


# TODO: pseudo code only
class S3Storage(MinioStorage):
    @staticmethod
    def create_endpoint() -> str:
        if endpoint := os.getenv('S3_ENDPOINT'):
            return endpoint
        else:
            raise Exception("S3_ENDPOINT is not set in environment variables")
    
    def get_filesystem(self) -> pa_fs.S3FileSystem:
        return pa_fs.S3FileSystem(
            endpoint_override=os.getenv('S3_ENDPOINT'),
            access_key=os.getenv("S3_ACCESS_KEY"),
            secret_key=os.getenv("S3_SECRET_KEY"),
        )

    # TODO
    def _create_minio(self) -> Minio:
        access_key, secret_key = os.getenv('S3_ACCESS_KEY'), os.getenv('S3_SECRET_KEY')
        assert access_key and secret_key, "S3_ACCESS_KEY and S3_SECRET_KEY are not set in environment variables"
        return Minio(
            endpoint=self.endpoint.replace('http://', '').replace('https://', ''),
            access_key=access_key,
            secret_key=secret_key,
            secure=self.endpoint.startswith('https://'),  # turn off TLS, i.e. not using HTTPS
            **self.kwargs,
        )

    # TODO: boto3? or just minio.bucket_exists()? ideally you want ServerError raised
    @staticmethod
    def _check_if_server_running(cache_time: datetime.datetime, endpoint: str) -> bool:
        pass
