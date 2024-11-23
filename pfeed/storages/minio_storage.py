from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    try:
        from minio.api import ObjectWriteResult, Tags
    except ImportError:
        pass
    from typing import Generator
    from pfeed.types.core import tData

import os
import io
from pathlib import Path

from minio import S3Error, Minio

from pfeed.types.core import is_dataframe
from pfeed.storages.base_storage import BaseStorage


def check_if_minio_running():
    import requests
    from requests.exceptions import RequestException, ReadTimeout

    endpoint = os.getenv('MINIO_HOST', 'localhost')+':'+os.getenv('MINIO_PORT', '9000')
    if not endpoint.startswith('http'):
        if any(local in endpoint for local in ['localhost:', '127.0.0.1:']):
            endpoint = f'http://{endpoint}'
        else:
            endpoint = f'https://{endpoint}'
    try:
        response = requests.get(f'{endpoint}/minio/health/live', timeout=3)
        if response.status_code != 200:
            print(f"Unhandled response from MinIO: {response.status_code=} {response.content} {response}")
            return False
    except (ReadTimeout, RequestException) as e:
        return False
    return True


class MinioStorage(BaseStorage):
    # DATA_PART_SIZE = 5 * (1024 ** 2)  # part size for S3, 5 MB
    BUCKET_NAME: str = 'pfeed'
    
    def __post_init__(self):
        super().__post_init__()
        endpoint = os.getenv('MINIO_HOST', 'localhost')+':'+os.getenv('MINIO_PORT', '9000')
        self.minio = Minio(
            endpoint=endpoint,
            access_key=os.getenv('MINIO_ROOT_USER', 'pfunder'),
            secret_key=os.getenv('MINIO_ROOT_PASSWORD', 'password'),
            # turn off TLS, i.e. not using HTTPS
            secure=True if os.getenv('MINIO_HOST', 'localhost') not in ['localhost', '127.0.0.1'] else False,
            **self.kwargs,
        )
        if not self.minio.bucket_exists(self.BUCKET_NAME):
            self.minio.make_bucket(self.BUCKET_NAME)
    
    def _create_data_path(self) -> Path:
        return Path("s3://" + self.BUCKET_NAME)

    def exists(self) -> bool:
        object_name = str(self.storage_path)
        return self.exist_object(object_name)
    
    def load(self, data: tData):
        if is_dataframe(data):
            from pfeed.etl import convert_to_pandas_df
            df = convert_to_pandas_df(data)
            data: bytes = df.to_parquet(compression='zstd')
        elif isinstance(data, bytes):
            pass
        else:
            raise NotImplementedError(f'{type(data)=}')
        object_name = str(self.storage_path)
        self.put_object(object_name, data)

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
