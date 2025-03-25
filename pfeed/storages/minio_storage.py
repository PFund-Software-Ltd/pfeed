from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    try:
        from minio.api import ObjectWriteResult, Tags
    except ImportError:
        pass
    from typing import Generator
    from pfeed.typing import tDATA_LAYER

import os
import io
import datetime
from functools import lru_cache

import pyarrow.fs as pa_fs
from minio import S3Error, ServerError, Minio

from pfeed.storages.base_storage import BaseStorage


class MinioStorage(BaseStorage):
    # DATA_PART_SIZE = 5 * (1024 ** 2)  # part size for S3, 5 MB
    BUCKET_NAME: str = 'pfeed'
    
    def __init__(
        self,
        data_layer: tDATA_LAYER='cleaned',
        data_domain: str='general_data',
        use_deltalake: bool=False, 
        **kwargs
    ):
        '''
        Args:
            kwargs: kwargs specific to minio client
        '''
        super().__init__(name='minio', data_layer=data_layer, data_domain=data_domain, use_deltalake=use_deltalake, **kwargs)
        self.endpoint: str = self.create_endpoint()
        cache_time = datetime.datetime.now().replace(second=0, microsecond=0) + datetime.timedelta(minutes=1)
        if not MinioStorage._check_if_server_running(cache_time, self.endpoint):
            raise ServerError(f"{self.name} is not running", 503)
        self.minio: Minio = self._create_minio()
        if not self.minio.bucket_exists(self.BUCKET_NAME):
            self.minio.make_bucket(self.BUCKET_NAME)
    
    def get_filesystem(self) -> pa_fs.S3FileSystem:
        return pa_fs.S3FileSystem(
            endpoint_override=self.endpoint,
            access_key=os.getenv('MINIO_ROOT_USER', 'pfunder'),
            secret_key=os.getenv('MINIO_ROOT_PASSWORD', 'password'),
        )
    
    def get_storage_options(self) -> dict:
        storage_options = {
            "endpoint_url": self.endpoint,
            "access_key_id": os.getenv('MINIO_ROOT_USER', 'pfunder'),
            "secret_access_key": os.getenv('MINIO_ROOT_PASSWORD', 'password'),
            "region": "us-east-1",
        }
        if self.use_deltalake:
            storage_options['allow_http'] = 'true' if self.endpoint.startswith('http://') else 'false'
            storage_options['conditional_put'] = 'etag'
        return storage_options
    
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
            **self._kwargs,
        )
    
    @staticmethod
    @lru_cache(maxsize=1)  # cache the result to avoid repeated checking
    def _check_if_server_running(cache_time: datetime.datetime, endpoint: str) -> bool:
        '''
        Args:
            cache_time: datetime.datetime (seconds removed), used to make the function signature unique
                to make lru_cache's ttl (time to live) to be 1 minute
        '''
        import requests
        from requests.exceptions import RequestException, ReadTimeout
        
        try:
            response = requests.get(f'{endpoint}/minio/health/live', timeout=1)
            success = response.status_code == 200
            if not success:
                print(f"Unhandled response from MinIO: {response.status_code=} {response.content} {response}")
            return success
        except (ReadTimeout, RequestException) as err:
            return False
    
    # s3:// doesn't work with pathlib, so we need to return a string
    @property
    def data_path(self) -> str:
        data_path = "s3://" + self.BUCKET_NAME
        return '/'.join([
            data_path,
            f'data_layer={self.data_layer.name.lower()}',
            f'data_domain={self.data_domain}'
        ])
    
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
