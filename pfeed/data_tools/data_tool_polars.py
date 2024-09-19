from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfeed.resolution import ExtendedResolution
    from pfeed.types.common_literals import tSUPPORTED_STORAGES
    
import os

import polars as pl

from pfeed.const.common import SUPPORTED_STORAGES


name = 'polars'


def read_parquet(paths_or_obj: list[str] | str | bytes, *args, storage: tSUPPORTED_STORAGES='local', **kwargs) -> pl.DataFrame | pl.LazyFrame:
    assert storage in SUPPORTED_STORAGES, f'{storage=} not in {SUPPORTED_STORAGES}'
    if isinstance(paths_or_obj, bytes):
        obj = paths_or_obj
        return pl.read_parquet(obj, *args, **kwargs)
    else:
        paths = paths_or_obj if isinstance(paths_or_obj, list) else [paths_or_obj]
        if storage == 'local':
            return pl.scan_parquet(paths, *args, **kwargs)
        elif storage == 'minio':
            storage_options = {
                "endpoint_url": "http://"+os.getenv('MINIO_HOST', 'localhost')+':'+os.getenv('MINIO_PORT', '9000'),
                "access_key_id": os.getenv('MINIO_ROOT_USER', 'pfunder'),
                "secret_access_key": os.getenv('MINIO_ROOT_PASSWORD', 'password'),
            }
            return pl.scan_parquet(paths, *args, storage_options=storage_options, **kwargs)
        else:
            raise NotImplementedError(f'{storage=}')


def estimate_memory_usage(df: pl.DataFrame | pl.LazyFrame) -> float:
    """Estimate the memory usage of a polars DataFrame in GB."""
    if isinstance(df, pl.LazyFrame):
        df = df.collect()
    return df.estimated_size(unit='gb')


def organize_time_series_columns(
    pdt: str, 
    resolution: str | ExtendedResolution, 
    df: pl.DataFrame | pl.LazyFramem,
    override_resolution: bool=False,
) -> pl.DataFrame | pl.LazyFrame:
    from pfeed.resolution import ExtendedResolution
    if isinstance(df, pl.LazyFrame):
        cols = df.collect_schema().names()
    else:
        cols = df.columns
    assert 'ts' in cols, "'ts' column not found"
    if isinstance(resolution, str):
        resolution = ExtendedResolution(resolution)
    if 'product' not in cols:
        df = df.with_columns(
            pl.lit(pdt).alias('product'),
        )
    if 'resolution' not in cols or override_resolution:
        df = df.with_columns(
            pl.lit(repr(resolution)).alias('resolution')
        )
    left_cols = ['ts', 'product', 'resolution']
    df = df.select(left_cols + [col for col in cols if col not in left_cols])
    return df