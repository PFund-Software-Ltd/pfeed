import os
import datetime

import pyarrow.fs as pa_fs

from pfeed.storages.file_based_storage import FileBasedStorage


# TODO: pseudo code only
class S3Storage(FileBasedStorage):
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

    # TODO: boto3? or just minio.bucket_exists()? ideally you want ServerError raised
    @staticmethod
    def _check_if_server_running(cache_time: datetime.datetime, endpoint: str) -> bool:
        pass
