from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfeed.resolution import ExtendedResolution
    from pfeed.types.common_literals import tSUPPORTED_STORAGES
    
import os

import polars as pl

from pfeed.const.common import SUPPORTED_STORAGES


name = 'polars'


def read_parquet(path_or_obj: str | bytes, *args, storage: tSUPPORTED_STORAGES='local', **kwargs) -> pl.DataFrame | pl.LazyFrame:
    assert storage in SUPPORTED_STORAGES, f'{storage=} not in {SUPPORTED_STORAGES}'
    if isinstance(path_or_obj, bytes):
        obj = path_or_obj
        return pl.read_parquet(obj, *args, **kwargs)
    else:
        path = path_or_obj
        if storage == 'local':
            return pl.scan_parquet(path, *args, **kwargs)
        elif storage == 'minio':
            storage_options = {
                "endpoint_url": "http://"+os.getenv('MINIO_HOST', 'localhost')+':'+os.getenv('MINIO_PORT', '9000'),
                "access_key_id": os.getenv('MINIO_ROOT_USER', 'pfunder'),
                "secret_access_key": os.getenv('MINIO_ROOT_PASSWORD', 'password'),
            }
            return pl.scan_parquet(path, *args, storage_options=storage_options, **kwargs)
        else:
            raise NotImplementedError(f'{storage=}')


def concat(dfs: list[pl.DataFrame | pl.LazyFrame], *args, **kwargs) -> pl.DataFrame | pl.LazyFrame:
    return pl.concat(dfs, *args, **kwargs)

        
def estimate_memory_usage(df: pl.DataFrame | pl.LazyFrame) -> float:
    """Estimate the memory usage of a polars DataFrame in GB."""
    if isinstance(df, pl.LazyFrame):
        df = df.collect()
    return df.estimated_size(unit='gb')


def organize_time_series_columns(pdt: str, resolution: str | ExtendedResolution, df: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame | pl.LazyFrame:
    from pfeed.resolution import ExtendedResolution
    if isinstance(df, pl.LazyFrame):
        cols = df.collect_schema().names()
    else:
        cols = df.columns
    assert 'ts' in cols, "'ts' column not found"
    assert 'product' not in cols, "'product' column already exists"
    assert 'resolution' not in cols, "'resolution' column already exists"
    if isinstance(resolution, str):
        resolution = ExtendedResolution(resolution)
    df = df.with_columns(
        pl.lit(pdt).alias('product'),
        pl.lit(repr(resolution)).alias('resolution')
    )
    left_cols = ['ts', 'product', 'resolution']
    df = df.select(left_cols + [col for col in df.collect_schema().names() if col not in left_cols])
    return df