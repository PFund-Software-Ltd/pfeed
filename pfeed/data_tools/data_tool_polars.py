from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfund.datas.resolution import Resolution
    from pfeed.types.literals import tSTORAGE
    
import os

import polars as pl

from pfeed.const.enums import DataStorage, DataTool


name = DataTool.POLARS


def read_parquet(paths_or_obj: list[str] | str | bytes, *args, storage: tSTORAGE, **kwargs) -> pl.DataFrame | pl.LazyFrame:
    if isinstance(paths_or_obj, bytes):
        obj = paths_or_obj
        return pl.read_parquet(obj, *args, **kwargs)
    else:
        paths = paths_or_obj if isinstance(paths_or_obj, list) else [paths_or_obj]
        storage = DataStorage[storage.upper()]
        if storage not in [DataStorage.LOCAL, DataStorage.CACHE] and 'storage_options' not in kwargs:
            if storage == DataStorage.MINIO:
                from pfeed.storages.minio_storage import MinioStorage
                kwargs['storage_options'] = {
                    "endpoint_url": MinioStorage.create_endpoint(),
                    "access_key_id": os.getenv('MINIO_ROOT_USER', 'pfunder'),
                    "secret_access_key": os.getenv('MINIO_ROOT_PASSWORD', 'password'),
                }
            else:
                raise NotImplementedError(f"read_parquet() for storage {storage} is not implemented")
        return pl.scan_parquet(paths, *args, **kwargs)


def concat(dfs: list[pl.DataFrame | pl.LazyFrame]) -> pl.DataFrame | pl.LazyFrame:
    return pl.concat(dfs)


def sort_by_ts(df: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame | pl.LazyFrame:
    return df.sort(by='ts', descending=False)


def estimate_memory_usage(df: pl.DataFrame | pl.LazyFrame) -> float:
    """Estimate the memory usage of a polars DataFrame in GB."""
    if isinstance(df, pl.LazyFrame):
        df = df.collect()
    return df.estimated_size(unit='gb')


def organize_time_series_columns(
    pdt: str, 
    resolution: str | Resolution, 
    df: pl.DataFrame | pl.LazyFrame,
    override_resolution: bool=False,
) -> pl.DataFrame | pl.LazyFrame:
    from pfund.datas.resolution import Resolution
    if isinstance(df, pl.LazyFrame):
        cols = df.collect_schema().names()
    else:
        cols = df.columns
    assert 'ts' in cols, "'ts' column not found"
    if isinstance(resolution, str):
        resolution = Resolution(resolution)
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