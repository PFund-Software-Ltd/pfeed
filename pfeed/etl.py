from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    import pyarrow as pa
    import pyarrow.parquet as pq
    from pfeed.types.literals import tSTORAGE, tPRODUCT_TYPE
    from pfeed.types.core import tDataFrame, tData, tDataModel
    from pfeed.storages.base_storage import BaseStorage

import os

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
    from pyspark.sql import DataFrame as SparkDataFrame, SparkSession
except ImportError:
    ps = None
    SparkDataFrame = None
    SparkSession = None
    
from pfund.datas.resolution import Resolution        
from pfeed.const.enums import DataStorage, DataTool, DataRawLevel


def write_data(data: bytes | pd.DataFrame, storage: BaseStorage, metadata: dict | None = None, compression: str = 'zstd'):
    import pyarrow as pa
    import pyarrow.parquet as pq
    from pfeed.utils.file_formats import compression_methods
    
    metadata = metadata or {}
    if storage.name in [DataStorage.LOCAL, DataStorage.CACHE]:
        file_path = str(storage.file_path)
    elif storage.name == DataStorage.MINIO:
        file_path = str(storage.file_path).replace('s3://', '')
    else:
        raise NotImplementedError(f'{storage.name=}')
    fs = get_filesystem(storage.name)

    # Write data based on its type
    if isinstance(data, pd.DataFrame):
        table = pa.Table.from_pandas(data)
        schema = table.schema.with_metadata(metadata)
        table = table.replace_schema_metadata(schema.metadata)
        with fs.open_output_stream(file_path) as f:
            pq.write_table(table, f, compression=compression)
    elif isinstance(data, (bytes, str)):
        if isinstance(data, str):
            data: bytes = data.encode('utf-8')
        data: bytes = compression_methods[compression](data)
        with fs.open_output_stream(file_path) as f:
            f.write(data)
    else:
        raise NotImplementedError(f'{type(data)=}')
    

def get_filesystem(storage: DataStorage) -> pa.fs.FileSystem:
    from pyarrow import fs as pa_fs
    if storage == DataStorage.MINIO:
        from pfeed.storages.minio_storage import MinioStorage
        fs = pa_fs.S3FileSystem(
            endpoint_override=MinioStorage.create_endpoint(),
            access_key=os.getenv('MINIO_ROOT_USER', 'pfunder'),
            secret_key=os.getenv('MINIO_ROOT_PASSWORD', 'password'),
        )
    elif storage == DataStorage.S3:
        fs = pa_fs.S3FileSystem(
            endpoint_override=os.getenv('S3_ENDPOINT'),
            access_key=os.getenv("S3_ACCESS_KEY"),
            secret_key=os.getenv("S3_SECRET_KEY"),
        )
    elif storage == DataStorage.AZURE:
        fs = pa_fs.AzureFileSystem(
            account_name=os.getenv("AZURE_ACCOUNT_NAME"),
            account_key=os.getenv("AZURE_ACCOUNT_KEY"),
        )
    elif storage == DataStorage.GCP:
        fs = pa_fs.GcsFileSystem(
            access_token=os.getenv("GCP_ACCESS_TOKEN")  # For GCP OAuth tokens
        )
    else:
        fs = pa_fs.LocalFileSystem()
    return fs


def read_data(storage: BaseStorage, compression: str = 'zstd') -> tuple[pq.ParquetFile | bytes | str, dict | None]:
    import pyarrow.parquet as pq
    from pfeed.utils.file_formats import decompression_methods
    
    if storage.name in [DataStorage.LOCAL, DataStorage.CACHE]:
        file_path = str(storage.file_path)
    elif storage.name == DataStorage.MINIO:
        file_path = str(storage.file_path).replace('s3://', '')
    else:
        raise NotImplementedError(f'{storage.name=}')
    fs = get_filesystem(storage.name)
    with fs.open_input_file(file_path) as f:
        if file_path.endswith('.parquet'):
            parquet_file = pq.ParquetFile(f)
            metadata = parquet_file.schema.to_arrow_schema().metadata
            metadata = {k.decode(): v.decode() for k, v in metadata.items()}
            return parquet_file, metadata
        else:
            raw_data = f.read()
            raw_data = decompression_methods[compression](raw_data)

            # Attempt to decode as UTF-8 to return text
            try:
                text_data = raw_data.decode('utf-8')
                return text_data, None
            except UnicodeDecodeError:
                # Return as raw bytes if not UTF-8 decodable
                return raw_data, None


def extract_data(data_model: tDataModel, storage: tSTORAGE | None = None) -> BaseStorage | None:
    from pfeed.data_models.market_data_model import MarketDataModel
    local_storages = ['cache', 'local', 'minio']
    storages = local_storages if storage is None else [storage]  # search through all local storages if not specified
    for storage in storages:
        storage: BaseStorage | None = get_storage(data_model, storage)
        if storage and storage.exists():
            _, metadata_from_storage = read_data(storage, compression=data_model.compression)
            if isinstance(storage.data_model, MarketDataModel):
                raw_level_from_storage = DataRawLevel[metadata_from_storage['raw_level'].upper()]
                raw_level_from_metadata = DataRawLevel[data_model.metadata['raw_level'].upper()]
                no_original_raw_level = DataRawLevel.ORIGINAL not in (raw_level_from_storage, raw_level_from_metadata)
                # since raw_level 'cleaned' is compatible with 'normalized' ('cleaned' filtered out unnecessary columns),
                # allow using data with raw_level='cleaned' even the specified raw_level in metadata is 'normalized'
                if no_original_raw_level and raw_level_from_metadata >= raw_level_from_storage:
                    return storage
            if metadata_from_storage == data_model.metadata:
                return storage
    return None
    

def filter_non_standard_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Filter out unnecessary columns from raw data."""
    is_tick_data = 'price' in df.columns
    pdt = df['product'][0]
    ptype: tPRODUCT_TYPE = pdt.split('_')[2]
    if is_tick_data:
        df = df.loc[:, ['ts', 'product', 'resolution', 'side', 'volume', 'price']]
    else:
        if ptype == 'STK':
            df = df.loc[:, ['ts', 'product', 'resolution', 'open', 'high', 'low', 'close', 'volume', 'dividends', 'splits']]
        else:
            df = df.loc[:, ['ts', 'product', 'resolution', 'open', 'high', 'low', 'close', 'volume']]
    return df


def organize_columns(df: pd.DataFrame, resolution: Resolution, product: str, symbol: str='') -> pd.DataFrame:
    """Organizes the columns of a DataFrame.
    Moving 'ts', 'product', 'resolution', 'symbol' to the leftmost side.
    """
    df['resolution'] = repr(resolution)
    df['product'] = product.upper()
    left_cols = ['ts', 'resolution', 'product']
    if symbol:
        df['symbol'] = symbol.upper()
        left_cols.append('symbol')
    return df.reindex(left_cols + [col for col in df.columns if col not in left_cols], axis=1)
    

def resample_data(
    df: pd.DataFrame, 
    target_resolution: str | Resolution, 
) -> pd.DataFrame:
    '''
    Resamples the input data based on the specified resolution and returns the resampled data in Parquet format.
    '''
    if isinstance(target_resolution, str):
        resolution = Resolution(target_resolution)
    else:
        resolution = target_resolution
    
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


def get_storage(data_model: tDataModel, storage: tSTORAGE, **kwargs) -> BaseStorage | None:
    from pfeed.storages import LocalStorage, MinioStorage, CacheStorage, S3Storage
    from minio import ServerError
    storage = DataStorage[storage.upper()]
    if storage == DataStorage.LOCAL:
        return LocalStorage(name=storage, data_model=data_model, **kwargs)
    elif storage == DataStorage.MINIO:
        try:
            return MinioStorage(name=storage, data_model=data_model, **kwargs)
        except ServerError:
            return None
    elif storage == DataStorage.CACHE:
        cache_storage = CacheStorage(name=storage, data_model=data_model, **kwargs)
        cache_storage.clear_caches()
        return cache_storage
    # elif storage == DataStorage.S3:
    #     pass
    # elif storage == DataStorage.AZURE:
    #     pass
    # elif storage == DataStorage.GCP:
    #     pass
    else:
        raise NotImplementedError(f'{storage=}')


def load_data(
    data_model: tDataModel, 
    data: bytes | pd.DataFrame, 
    storage: tSTORAGE, 
    **kwargs
) -> list[BaseStorage]:
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
    from pfeed.utils.utils import get_dates_in_between
    storage_literal = storage
    storages: list[BaseStorage] = []
    if isinstance(data, pd.DataFrame):
        is_data_include_multi_dates = data_model.end_date is not None
        if not is_data_include_multi_dates:
            if storage := get_storage(data_model, storage_literal, **kwargs):
                write_data(data, storage, metadata=data_model.metadata, compression=data_model.compression)
                storages.append(storage)
        else:
            dates = get_dates_in_between(data_model.start_date, data_model.end_date)
            # storage is per date, set end_date to None, only start_date is used for the filename
            data_model.update_end_date(None)
            # split data into chunks by date
            data_chunks = [group for _, group in data.groupby(data['ts'].dt.date)]
            data_chunks_per_date = {data_chunk['ts'].dt.date.iloc[0]: data_chunk for data_chunk in data_chunks}
            for date in dates:
                metadata = data_model.metadata.copy()
                if date in data_chunks_per_date:
                    data_chunk = data_chunks_per_date[date]
                else:   
                    # Create an empty DataFrame with the same columns
                    data_chunk = pd.DataFrame(columns=data.columns)
                    # NOTE: used as an indicator for successful download, there is just no data on that date (e.g. weekends, holidays, etc.)
                    metadata['is_placeholder'] = 'true'
                data_model.update_start_date(date)
                if storage := get_storage(data_model, storage_literal, **kwargs):
                    write_data(data_chunk, storage, metadata=metadata, compression=data_model.compression)
                    storages.append(storage)
    else:
        raise NotImplementedError(f'{type(data)=}')
    if print_disk_usage:
        if hasattr(storage, 'local_data_path'):
            print_disk_usage(storage.local_data_path)
        else:
            print_disk_usage(storage.data_path)
    return storages


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
    elif SparkDataFrame and isinstance(data, SparkDataFrame):
        return data.toPandas()
    else:
        raise ValueError(f'{type(data)=}')


def convert_to_user_df(df: pd.DataFrame, data_tool: DataTool) -> tDataFrame:
    if data_tool == DataTool.PANDAS:
        return df
    elif data_tool == DataTool.POLARS:
        return pl.from_pandas(df).lazy()
    # elif data_tool == DataTool.DASK:
    #     return dd.from_pandas(df, npartitions=1)
    # elif data_tool == DataTool.SPARK:
    #     spark = SparkSession.builder.getOrCreate()
    #     return spark.createDataFrame(df)
    else:
        raise ValueError(f'{data_tool=}')
