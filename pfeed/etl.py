from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfeed.types.literals import tSTORAGE
    from pfeed.types.core import tDataFrame, tData, tDataModel
    from pfeed.storages.base_storage import BaseStorage

import os
from pathlib import Path

import pandas as pd

try:
    import polars as pl
except ImportError:
    pl = None

try:
    import dask.dataframe as dd
except ImportError:
    dd = None

try:
    os.environ['PYARROW_IGNORE_TIMEZONE'] = '1'  # used to suppress warning
    import pyspark.pandas as ps
except ImportError:
    ps = None
    
        
from pfund.datas.resolution import Resolution

from pfeed.const.enums import DataStorage, DataTool
from pfeed.storages.minio_storage import check_if_minio_running


def extract_data(
    data_model: tDataModel,
    storage: tSTORAGE | None=None,
) -> Path | None:
    '''
    Args:
        storage: if specified, only search for data in the specified storage.
    '''
    storages = [_storage for _storage in DataStorage] if storage is None else [DataStorage[storage.upper()]]
    for storage in storages:
        if storage == DataStorage.MINIO:
            if not check_if_minio_running():
                continue
        storage: BaseStorage = _get_storage(data_model, storage)
        return storage.file_path if storage.exists() else None

    
def filter_non_standard_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Filter out unnecessary columns from raw data."""
    assert 'ts' in df.columns, '"ts" column not found'
    is_tick_data = 'price' in df.columns
    if is_tick_data:
        df = df.loc[:, ['ts', 'product', 'resolution', 'side', 'volume', 'price']]
    else:
        df = df.loc[:, ['ts', 'product', 'resolution', 'open', 'high', 'low', 'close', 'volume']]
    return df


def organize_columns(df: pd.DataFrame, pdt: str, resolution: Resolution) -> pd.DataFrame:
    """Organizes the columns of a DataFrame.
    Moving 'ts', 'product', 'resolution' to the leftmost side.
    """
    df['product'] = pdt
    df['resolution'] = repr(resolution)
    left_cols = ['ts', 'product', 'resolution']
    return df.reindex(left_cols + [col for col in df.columns if col not in left_cols], axis=1)
        

def resample_data(
    df: pd.DataFrame, 
    resolution: Resolution, 
) -> pd.DataFrame:
    '''
    Resamples the input data based on the specified resolution and returns the resampled data in Parquet format.
    '''
    # converts to pandas's resolution format
    eresolution = repr(resolution)
        
    # 'min' means minute in pandas, please refer to https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#dateoffset-objects
    eresolution = eresolution.replace('m', 'min')
    eresolution = eresolution.replace('d', 'D')
    
    is_tick_data = 'price' in df.columns
    assert not df.empty, 'data is empty'
    df.set_index('ts', inplace=True)
    
    if is_tick_data:
        resample_logic = {
            'price': 'ohlc',
            'volume': 'sum',
        }
    else:
        resample_logic = {
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum',
        }

    if 'dividends' in df.columns:
        resample_logic['dividends'] = 'sum'
    if 'splits' in df.columns:
        resample_logic['splits'] = 'prod'
            
    resampled_df = (
        df
        .resample(eresolution)
        .apply(resample_logic)
    )
    
    if is_tick_data:
        # drop an unnecessary level created by 'ohlc' in the resample_logic
        resampled_df = resampled_df.droplevel(0, axis=1)

    resampled_df.dropna(inplace=True)
    resampled_df.reset_index(inplace=True)
    
    return resampled_df


def _get_storage(data_model: tDataModel, storage: DataStorage, **kwargs) -> BaseStorage:
    from pfeed.storages import LocalStorage, MinioStorage, CacheStorage
    if storage == DataStorage.LOCAL:
        return LocalStorage(data_model=data_model)
    elif storage == DataStorage.MINIO:
        return MinioStorage(data_model=data_model, kwargs=kwargs)
    elif storage == DataStorage.CACHE:
        cache_storage = CacheStorage(data_model=data_model)
        cache_storage.clear_caches()
        return cache_storage
    else:
        raise NotImplementedError(f'{storage=}')


def load_data(
    data_model: tDataModel,
    data: tData,
    storage: tSTORAGE,
    **kwargs,
) -> BaseStorage:
    """
    Loads data into the specified data destination.
    Args:
        storage: The destination where the data will be loaded. 
    Returns:
        None
    """
    try:
        from pfeed.utils.monitor import print_disk_usage
    except ImportError:
        print_disk_usage = None
    storage = DataStorage[storage.upper()]
    if storage == DataStorage.MINIO:
        if not check_if_minio_running():
            raise Exception("MinIO is not running")
    storage = _get_storage(data_model, storage, **kwargs)
    storage.load(data)
    if print_disk_usage:
        print_disk_usage(storage.data_path)
    return storage


def convert_to_pandas_df(data: tData) -> pd.DataFrame:
    from pfeed.utils.file_formats import convert_raw_data_to_pandas_df
    if isinstance(data, bytes):
        return convert_raw_data_to_pandas_df(data)
    elif isinstance(data, pd.DataFrame):
        return data
    elif pl and isinstance(data, pl.DataFrame):
        return data.to_pandas()
    elif pl and isinstance(data, pl.LazyFrame):
        return data.collect().to_pandas()
    elif dd and isinstance(data, dd.DataFrame):
        return data.compute()
    elif ps and isinstance(data, ps.DataFrame):
        return data.to_pandas()
    else:
        raise ValueError(f'{type(data)=}')


def convert_to_user_df(df: pd.DataFrame, data_tool: DataTool) -> tDataFrame:
    if data_tool == DataTool.PANDAS:
        return df
    elif data_tool == DataTool.POLARS:
        return pl.from_pandas(df)
    elif data_tool == DataTool.DASK:
        return dd.from_pandas(df, npartitions=1)
    elif data_tool == DataTool.SPARK:
        return ps.from_pandas(df)
    else:
        raise ValueError(f'{data_tool=}')
