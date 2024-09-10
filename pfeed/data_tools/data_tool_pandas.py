from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfeed.resolution import ExtendedResolution
    from pfeed.types.common_literals import tSUPPORTED_STORAGES

import os
import io

import s3fs
import pandas as pd

from pfeed.const.common import SUPPORTED_STORAGES


name = 'pandas'


def read_parquet(path_or_obj: str | bytes, *args, storage: tSUPPORTED_STORAGES='local', **kwargs) -> pd.DataFrame:
    assert storage in SUPPORTED_STORAGES, f'{storage=} not in {SUPPORTED_STORAGES}'
    if isinstance(path_or_obj, bytes):
        obj = io.BytesIO(path_or_obj)
        return pd.read_parquet(obj, *args, **kwargs)
    else:
        path = path_or_obj
        if storage == 'local':
            return pd.read_parquet(path, *args, **kwargs)
        elif storage == 'minio':
            fs = s3fs.S3FileSystem(
                endpoint_url="http://"+os.getenv('MINIO_HOST', 'localhost')+':'+os.getenv('MINIO_PORT', '9000'),
                key=os.getenv('MINIO_ROOT_USER', 'pfunder'),
                secret=os.getenv('MINIO_ROOT_PASSWORD', 'password'),
            )
            return pd.read_parquet(path, *args, filesystem=fs, **kwargs)
        else:
            raise NotImplementedError(f'{storage=}')


def concat(dfs: list[pd.DataFrame], *args, **kwargs) -> pd.DataFrame:
    return pd.concat(dfs, *args, **kwargs)


def estimate_memory_usage(df: pd.DataFrame) -> float:
    """Estimate the memory usage of a pandas DataFrame in GB."""
    return df.memory_usage(deep=True).sum() / (1024 ** 3)

    
def organize_time_series_columns(pdt: str, resolution: str | ExtendedResolution, df: pd.DataFrame) -> pd.DataFrame:
    """Organize the columns of a pandas DataFrame.
    Moving 'ts', 'product', 'resolution' to the leftmost side.
    """
    from pfeed.resolution import ExtendedResolution
    assert 'ts' in df.columns, "'ts' column not found"
    assert 'product' not in df.columns, "'product' column already exists"
    assert 'resolution' not in df.columns, "'resolution' column already exists"
    if isinstance(resolution, str):
        resolution = ExtendedResolution(resolution)
    left_cols = ['ts', 'product', 'resolution']
    df['product'] = pdt
    df['resolution'] = repr(resolution)
    df = df.reindex(left_cols + [col for col in df.columns if col not in left_cols], axis=1)
    return df