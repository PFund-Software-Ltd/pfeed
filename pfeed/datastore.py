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
import logging


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


class Datastore:
    # DATA_PART_SIZE = 5 * (1024 ** 2)  # part size for S3, 5 MB
    BUCKET_NAME = 'pfeed'

    # EXTEND, currently only consider using MinIO
    @classmethod
    def initialize_store(cls, name: str, **kwargs):
        if name == 'minio':
            from minio import Minio
            endpoint = os.getenv('MINIO_HOST', 'localhost')+':'+os.getenv('MINIO_PORT', '9000')
            cls.minio = Minio(
                endpoint=endpoint,
                access_key=os.getenv('MINIO_ROOT_USER', 'pfunder'),
                secret_key=os.getenv('MINIO_ROOT_PASSWORD', 'password'),
                # turn off TLS, i.e. not using HTTPS
                secure=True if os.getenv('MINIO_HOST', 'localhost') not in ['localhost', '127.0.0.1'] else False,
                **kwargs,
            )
            if not cls.minio.bucket_exists(cls.BUCKET_NAME):
                cls.minio.make_bucket(cls.BUCKET_NAME)
        else:
            raise NotImplementedError(f'Unsupported datastore {name}')
    
    def __init__(self, name: str, **kwargs):
        self.name = name.lower()
        self.logger = logging.getLogger(self.name)
        if not hasattr(self.__class__, self.name):
            self.__class__.initialize_store(self.name, **kwargs)
        
    def __getattr__(self, attr):
        '''gets triggered only when the attribute is not found'''
        return getattr(self.minio, attr)
    
    def get_object(self, object_name: str) -> bytes | None:
        from minio import S3Error
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
        from minio import S3Error
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
        
            
if __name__ == '__main__':
    datastore = Datastore('minio')
    # list buckets
    # buckets = datastore.list_buckets()
    # for bucket in buckets:
    #     print(bucket.name, bucket.creation_date)

    # list objects
    objects = datastore.list_objects()
    for obj in objects:
        print(obj.object_name)
    
    # get object
    # data = datastore.get_object(
    #     object_name="live/bybit/historical/raw/BTC_USDT_PERP/BTC_USDT_PERP_2023-11-01.csv.gz"
    # )
    
    # put object
    # datastore.put_object(
    #     bucket_name='test',
    #     object_name='test_prefix/test',
    #     data=b'test',
    #     part_size=1024**2 * 5,
    # )
    
    # upload a file
    # datastore.fput_object(
    #     bucket_name="dev",
    #     object_name="test_prefix/test",
    #     file_path=f"{PROJ_PATH}/test_data/test.txt"
    # )

    # get object info
    # res = datastore.stat_object(
    #     bucket_name="dev",
    #     object_name="test_prefix/test"
    # )


    # copy an object from one prefix to another
    # res = datastore.copy_object(
    #     bucket_name="dev",
    #     object_name="new_prefix/test",
    #     source=CopySource(
    #         bucket_name='dev',
    #         object_name='test_prefix/test'
    #     )
    # )