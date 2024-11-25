from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfund.datas.resolution import Resolution
    from pfeed.types.literals import tSTORAGE

import io

import pandas as pd

from pfeed.const.enums import DataStorage, DataTool


name = DataTool.PANDAS


def read_parquet(paths_or_obj: list[str] | str | bytes, *args, storage: tSTORAGE, **kwargs) -> pd.DataFrame:
    if isinstance(paths_or_obj, bytes):
        obj = io.BytesIO(paths_or_obj)
        return pd.read_parquet(obj, *args, **kwargs)
    else:
        from pfeed.etl import get_filesystem
        paths = paths_or_obj if isinstance(paths_or_obj, list) else [paths_or_obj]
        storage = DataStorage[storage.upper()]
        if storage not in [DataStorage.LOCAL, DataStorage.CACHE] and 'filesystem' not in kwargs:
            kwargs['filesystem'] = get_filesystem(storage)
        return pd.read_parquet(paths, *args, **kwargs)


def concat(dfs: list[pd.DataFrame], ignore_index: bool=True) -> pd.DataFrame:
    return pd.concat(dfs, ignore_index=ignore_index)


def sort_by_ts(df: pd.DataFrame) -> pd.DataFrame:
    return df.sort_values(by='ts', ignore_index=True, ascending=True)


def estimate_memory_usage(df: pd.DataFrame) -> float:
    """Estimate the memory usage of a pandas DataFrame in GB."""
    return df.memory_usage(deep=True).sum() / (1024 ** 3)

    
def organize_time_series_columns(
    pdt: str, 
    resolution: str | Resolution, 
    df: pd.DataFrame,
    override_resolution: bool=False,
) -> pd.DataFrame:
    """Standardize the columns of a pandas DataFrame.
    Moving 'ts', 'product', 'resolution' to the leftmost side.
    """
    from pfund.datas.resolution import Resolution
    assert 'ts' in df.columns, "'ts' column not found"
    if isinstance(resolution, str):
        resolution = Resolution(resolution)
    if 'product' not in df.columns:
        df['product'] = pdt
    if 'resolution' not in df.columns or override_resolution:
        df['resolution'] = repr(resolution)
    left_cols = ['ts', 'product', 'resolution']
    df = df.reindex(left_cols + [col for col in df.columns if col not in left_cols], axis=1)
    return df