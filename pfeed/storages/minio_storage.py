from __future__ import annotations
from typing_extensions import TypedDict
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    import urllib3
    try:
        from minio.api import ObjectWriteResult, Tags
    except ImportError:
        pass
    from minio.credentials.providers import Provider
    from typing import Generator
    from pfeed.typing import tDATA_LAYER

import os
import io
import datetime
from functools import lru_cache

import pyarrow.fs as pa_fs
from minio import S3Error, ServerError, Minio

from pfeed.storages.base_storage import BaseStorage


class MinioStorageOptions(TypedDict):
    access_key: str
    secret_key: str
    session_token: str
    region: str
    http_client: urllib3.PoolManager
    credentials: Provider
    cert_check: bool


class MinioStorage(BaseStorage):
    # DATA_PART_SIZE = 5 * (1024 ** 2)  # part size for S3, 5 MB
    BUCKET_NAME: str = 'pfeed'
    
    def __init__(
        self,
        data_layer: tDATA_LAYER='cleaned',
        data_domain: str='general_data',
        use_deltalake: bool=False, 
        storage_options: MinioStorageOptions | None=None,
    ):
        '''
        Args:
            storage_options: kwargs specific to minio client
        '''
        self.endpoint = self._create_endpoint()
        super().__init__(
            name='minio', 
            data_layer=data_layer, 
            data_domain=data_domain, 
            use_deltalake=use_deltalake, 
            storage_options=self._normalize_storage_options(storage_options),
        )
        cache_time = datetime.datetime.now().replace(second=0, microsecond=0) + datetime.timedelta(minutes=1)
        if not MinioStorage._check_if_server_running(cache_time, self.endpoint):
            raise ServerError(f"{self.name} is not running", 503)
        self.minio: Minio = self._create_minio(storage_options)
        if not self.minio.bucket_exists(self.BUCKET_NAME):
            self.minio.make_bucket(self.BUCKET_NAME)
    
    @staticmethod
    def _create_endpoint() -> str:
        '''Creates endpoint, e.g. http://localhost:9000'''
        endpoint = os.getenv('MINIO_HOST', 'localhost')+':'+os.getenv('MINIO_PORT', '9000')
        if not endpoint.startswith('http'):
            endpoint = 'http://' + endpoint if 'localhost' in endpoint or '127.0.0.1' in endpoint else 'https://' + endpoint
        return endpoint

    def _normalize_storage_options(self, minio_options: MinioStorageOptions | None) -> dict:
        """
        Normalize minio's storage options to S3 compatible options
        """
        default_access_key = os.getenv('MINIO_ROOT_USER', 'pfunder')
        default_secret_key = os.getenv('MINIO_ROOT_PASSWORD', 'password')
        if minio_options is None:
            storage_options = {
                "endpoint_url": self.endpoint,
                "access_key_id": default_access_key,
                "secret_access_key": default_secret_key,
            }
        else:
            storage_options = {
                "endpoint_url": self.endpoint,
                "access_key_id": minio_options.pop("access_key", default_access_key),
                "secret_access_key": minio_options.pop("secret_key", default_secret_key),
            }

        # REVIEW
        if self.use_deltalake:
            if 'allow_http' not in storage_options:
                storage_options["allow_http"] = 'true' if self.endpoint.startswith("http://") else 'false'
            if 'conditional_put' not in storage_options:
                storage_options["conditional_put"] = 'etag'
            if 'region' not in storage_options:
                storage_options["region"] = 'us-east-1'  # REVIEW

        return storage_options
    
    def _create_minio(self, minio_options: MinioStorageOptions | None) -> Minio:
        return Minio(
            endpoint=self.endpoint.replace('http://', '').replace('https://', ''),
            access_key=self._storage_options['access_key_id'],
            secret_key=self._storage_options['secret_access_key'],
            secure=self.endpoint.startswith('https://'),  # turn off TLS, i.e. not using HTTPS
            **(minio_options or {}),
        )
    
    def get_filesystem(self) -> pa_fs.S3FileSystem:
        return pa_fs.S3FileSystem(
            endpoint_override=self._storage_options['endpoint_url'],
            access_key=self._storage_options['access_key_id'],
            secret_key=self._storage_options['secret_access_key'],
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
