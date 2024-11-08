from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    import pandas as pd
    try:
        from minio.api import ObjectWriteResult, Tags
    except ImportError:
        pass
    from typing import Generator
    from pfeed.types.core import tData

import os
import io

from minio import S3Error

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
    
    # TODO: how to pass in kwargs? move minio to class variable? how long does it take to initialize minio?
    minio_kwargs: dict = {}

    def __post_init__(self):
        super().__post_init__()
        from minio import Minio
        endpoint = os.getenv('MINIO_HOST', 'localhost')+':'+os.getenv('MINIO_PORT', '9000')
        self.minio = Minio(
            endpoint=endpoint,
            access_key=os.getenv('MINIO_ROOT_USER', 'pfunder'),
            secret_key=os.getenv('MINIO_ROOT_PASSWORD', 'password'),
            # turn off TLS, i.e. not using HTTPS
            secure=True if os.getenv('MINIO_HOST', 'localhost') not in ['localhost', '127.0.0.1'] else False,
            **self.minio_kwargs,
        )
        if not self.minio.bucket_exists(self.BUCKET_NAME):
            self.minio.make_bucket(self.BUCKET_NAME)
    
    # TODO
    def exists(self) -> bool:
        return self.exist_object(self.storage_path)
    
    # TODO: should change file_path
    def read(self, *args, **kwargs) -> pd.DataFrame | None:
        from pfeed.data_tools.data_tool_pandas import read_parquet
        object_name = self.storage_path
        if self.exist_object(object_name):
            path = "s3://" + self.BUCKET_NAME + "/" + object_name
            df: pd.DataFrame = read_parquet(path, storage='minio')
            return df

    def load(self, data: tData, **kwargs):
        from pfeed.etl import convert_to_pandas_df
        object_name = self.storage_path
        if is_dataframe(data):
            df = convert_to_pandas_df(data)
            data: bytes = df.to_parquet(compression='zstd')
        elif isinstance(data, bytes):
            pass
        else:
            raise NotImplementedError(f'{type(data)=}')
        self.put_object(object_name, data, **kwargs)

    def get_object(self, object_name: str) -> bytes | None:
        try:
            res = self.minio.get_object(self.BUCKET_NAME, object_name)
            if res.status == 200:
                return res.data
            else:
                self.logger.error(f'Unhandled MinIO response status {res.status}')
        except S3Error as err:
            # logger.warning(f'MinIO S3Error {object_name=} {err=}')
            return None

    def exist_object(self, object_name: str) -> bool:
        try:
            res: Tags | None = self.minio.get_object_tags(self.BUCKET_NAME, object_name)
            return True
        except S3Error as err:
            # self.logger.warning(f'MinIO S3Error {object_name=} {err=}')
            return False

    def list_objects(self, prefix) -> list | None:
        '''
            Args:
                prefix: e.g. live/bybit/historical/raw/BTC_USDT_PERP/
        '''
        objects: Generator = self.minio.list_objects(self.BUCKET_NAME, prefix=prefix)
        return list(objects)
    
    def put_object(self, object_name: str, data: bytes, **kwargs) -> ObjectWriteResult:
        return self.minio.put_object(
            self.BUCKET_NAME,
            object_name,
            data=io.BytesIO(data),
            # part_size=self.DATA_PART_SIZE,
            length=len(data),
            content_type='application/parquet',
            **kwargs
        )