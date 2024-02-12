import os
import io
import requests
import logging
from requests.exceptions import RequestException, ReadTimeout

from typing import Generator

from minio import Minio, S3Error
from minio.api import ObjectWriteResult


logger = logging.getLogger('minio')


def check_if_minio_running():
    endpoint = os.getenv('MINIO_HOST', 'localhost')+':'+os.getenv('MINIO_PORT', '9000')
    if 'http' not in endpoint:
        if 'localhost' in endpoint:
            endpoint = f'http://{endpoint}'
        else:
            endpoint = f'https://{endpoint}'
    try:
        response = requests.get(f'{endpoint}/minio/health/live', timeout=3)
        if response.status_code == 200:
            print(f"MinIO is running on {endpoint}")
            return True
        else:
            raise Exception(f"Unhandled response: {response.status_code=} {response.content} {response}")
    except (ReadTimeout, RequestException) as e:
        print(f"MinIO is not running on {endpoint}: {e}")
    return False


def assert_access_key_and_secret_key_exists():
    console_endpoint = os.getenv('MINIO_HOST', 'localhost')+':'+os.getenv('MINIO_CONSOLE_PORT', '9001')
    assert os.getenv('MINIO_ACCESS_KEY') and os.getenv('MINIO_SECRET_KEY'), \
        f'MINIO_ACCESS_KEY and MINIO_SECRET_KEY are required in environment variables,\nPlease create them using MinIO Console on {console_endpoint}.\n' \
        'For details, please refer to https://min.io/docs/minio/container/administration/console/security-and-access.html'


# EXTEND, currently only consider using MinIO
class Datastore:
    DATA_PART_SIZE = 5 * (1024 ** 2)  # part size for S3, 5 MB
    BUCKET_NAME = 'pfeed' + '-' + os.getenv('PFEED_ENV', 'DEV').lower()
    
    def __init__(self, **kwargs):
        assert_access_key_and_secret_key_exists()
        self.minio = Minio(
            endpoint=os.getenv('MINIO_HOST', 'localhost')+':'+os.getenv('MINIO_PORT', '9000'),
            access_key=os.getenv('MINIO_ACCESS_KEY'),
            secret_key=os.getenv('MINIO_SECRET_KEY'),
            # turn off TLS, i.e. not using HTTPS
            secure=True if os.getenv('PFEED_ENV', 'DEV').upper() == 'PRD' else False,
            **kwargs,
        )

    def __getattr__(self, attr):
        '''gets triggered only when the attribute is not found'''
        return getattr(self.minio, attr)
    
    def get_object(self, object_name: str) -> bytes | None:
        try:
            bucket_name = self.BUCKET_NAME
            res = self.minio.get_object(bucket_name, object_name)
            if res.status == 200:
                return res.data
            else:
                logger.error(f'Unhandled MinIO response status {res.status}')
        except S3Error as err:
            # logger.warning(f'MinIO S3Error {object_name=} {err=}')
            pass

    def list_objects(self, prefix) -> list | None:
        '''
            Args:
                prefix: e.g. live/bybit/historical/raw/BTC_USDT_PERP/
        '''
        bucket_name = self.BUCKET_NAME
        objects: Generator = self.minio.list_objects(bucket_name, prefix=prefix)
        return list(objects)
    
    def put_object(self, object_name: str, data: bytes, **kwargs) -> ObjectWriteResult:
        bucket_name = self.BUCKET_NAME
        if not self.minio.bucket_exists(bucket_name):
            self.minio.make_bucket(bucket_name)
        return self.minio.put_object(
            bucket_name,
            object_name,
            data=io.BytesIO(data),
            part_size=self.DATA_PART_SIZE,
            length=-1,
            **kwargs
        )
        
            
if __name__ == '__main__':
    datastore = Datastore()
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