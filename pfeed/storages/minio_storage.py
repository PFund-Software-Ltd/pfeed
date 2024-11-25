from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    try:
        from minio.api import ObjectWriteResult, Tags
    except ImportError:
        pass
    from typing import Generator

import os
import io

from minio import S3Error, ServerError, Minio

from pfeed.storages.base_storage import BaseStorage


class MinioStorage(BaseStorage):
    # DATA_PART_SIZE = 5 * (1024 ** 2)  # part size for S3, 5 MB
    BUCKET_NAME: str = 'pfeed'
    
    def __post_init__(self):
        super().__post_init__()
        self.endpoint = self.create_endpoint()
        if not self.check_if_server_running():
            raise ServerError(f"{self.name} is not running", 503)
        self.minio = self._create_minio()
        if not self.minio.bucket_exists(self.BUCKET_NAME):
            self.minio.make_bucket(self.BUCKET_NAME)
        
    @staticmethod
    def create_endpoint() -> str:
        '''Creates endpoint, e.g. http://localhost:9000'''
        endpoint = os.getenv('MINIO_HOST', 'localhost')+':'+os.getenv('MINIO_PORT', '9000')
        if not endpoint.startswith('http'):
            endpoint = 'http://' + endpoint if 'localhost' in endpoint or '127.0.0.1' in endpoint else 'https://' + endpoint
        return endpoint
    
    def _create_minio(self) -> Minio:
        return Minio(
            endpoint=self.endpoint.replace('http://', '').replace('https://', ''),
            access_key=os.getenv('MINIO_ROOT_USER', 'pfunder'),
            secret_key=os.getenv('MINIO_ROOT_PASSWORD', 'password'),
            secure=self.endpoint.startswith('https://'),  # turn off TLS, i.e. not using HTTPS
            **self.kwargs,
        )
    
    def check_if_server_running(self) -> bool:
        import requests
        from requests.exceptions import RequestException, ReadTimeout
        
        try:
            response = requests.get(f'{self.endpoint}/minio/health/live', timeout=3)
            if response.status_code != 200:
                print(f"Unhandled response from MinIO: {response.status_code=} {response.content} {response}")
                return False
        except (ReadTimeout, RequestException) as err:
            return False
        return True
    
    # s3:// doesn't work with pathlib, so we need to return a string
    def _create_data_path(self) -> str:
        return "s3://" + self.BUCKET_NAME

    def exists(self) -> bool:
        object_name = str(self.storage_path)
        return self.exist_object(object_name)
    
    def get_object(self, object_name: str) -> bytes | None:
        try:
            res = self.minio.get_object(self.BUCKET_NAME, object_name)
            if res.status == 200:
                return res.data
        except S3Error as err:
            return None

    def exist_object(self, object_name: str) -> bool:
        try:
            res: Tags | None = self.minio.get_object_tags(self.BUCKET_NAME, object_name)
            return True
        except S3Error as err:
            return False

    def list_objects(self, prefix) -> list | None:
        '''
            Args:
                prefix: e.g. live/bybit/historical/raw/BTC_USDT_PERP/
        '''
        objects: Generator = self.minio.list_objects(self.BUCKET_NAME, prefix=prefix)
        return list(objects)
    
    def put_object(self, object_name: str, data: bytes) -> ObjectWriteResult:
        return self.minio.put_object(
            self.BUCKET_NAME,
            object_name,
            data=io.BytesIO(data),
            # part_size=self.DATA_PART_SIZE,
            length=len(data),
            content_type='application/parquet',
        )
