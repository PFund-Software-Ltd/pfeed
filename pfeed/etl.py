from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    import pyarrow as pa
    import pyarrow.parquet as pq
    from narwhals.typing import IntoFrameT, Frame
    from pfeed.typing.literals import tSTORAGE, tPRODUCT_TYPE
    from pfeed.typing.core import tDataFrame, tData
    from pfeed.data_models.base_data_model import BaseDataModel
    from pfeed.storages.base_storage import BaseStorage

import os

import pandas as pd
import polars as pl
import narwhals as nw

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
        storage.file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path = str(storage.file_path)
    elif storage.name == DataStorage.MINIO:
        file_path = str(storage.file_path).replace('s3://', '')
    else:
        raise NotImplementedError(f'{storage.name=}')
    fs = get_filesystem(storage.name)

    # Write data based on its type
    if isinstance(data, pd.DataFrame):
        table = pa.Table.from_pandas(data, preserve_index=False)
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


def extract_data(data_model: BaseDataModel, storage: tSTORAGE) -> BaseStorage | None:
    from pfeed.data_models.market_data_model import MarketDataModel
    storage: BaseStorage | None = get_storage(data_model, storage)
    if not (storage and storage.exists()):
        return None
    _, metadata_from_storage = read_data(storage, compression=data_model.compression)
    if isinstance(storage.data_model, MarketDataModel):
        raw_level_from_storage = DataRawLevel[metadata_from_storage['raw_level'].upper()]
        raw_level_from_metadata = DataRawLevel[data_model.metadata['raw_level'].upper()]
        no_original_raw_level = DataRawLevel.ORIGINAL not in (raw_level_from_storage, raw_level_from_metadata)
        # since raw_level 'cleaned' is compatible with 'normalized' ('cleaned' filtered out unnecessary columns),
        # allow using data in storage with raw_level='cleaned' even the specified raw_level in metadata is 'normalized'
        if no_original_raw_level and raw_level_from_metadata >= raw_level_from_storage:
            return storage
    if metadata_from_storage == data_model.metadata:
        return storage


def standardize_columns(df: pd.DataFrame, resolution: Resolution, product: str, symbol: str='') -> pd.DataFrame:
    """Standardizes the columns of a DataFrame.
    Adds columns 'resolution', 'product', 'symbol', and convert 'ts' to datetime
    """
    from pandas.api.types import is_datetime64_any_dtype as is_datetime
    from pfeed.utils.utils import determine_timestamp_integer_unit_and_scaling_factor
    df['resolution'] = repr(resolution)
    df['product'] = product.upper()
    if symbol:
        df['symbol'] = symbol.upper()
    if not is_datetime(df['ts']):
        first_ts = df.loc[0, 'ts']
        ts_unit, scaling_factor = determine_timestamp_integer_unit_and_scaling_factor(first_ts)
        df['ts'] = pd.to_datetime(df['ts'] * scaling_factor, unit=ts_unit)
    return df


def filter_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Filter out unnecessary columns from raw data."""
    is_tick_data = 'price' in df.columns
    pdt = df['product'][0]
    ptype: tPRODUCT_TYPE = pdt.split('_')[2]
    if is_tick_data:
        standard_cols = ['ts', 'product', 'resolution', 'side', 'volume', 'price']
    else:
        standard_cols = ['ts', 'product', 'resolution', 'open', 'high', 'low', 'close', 'volume']
    df_cols = df.columns
    extra_cols = ['symbol']
    # EXTEND
    if ptype == 'STK':
        extra_cols.extend(['dividends', 'splits'])
    for extra_col in extra_cols:
        if extra_col in df_cols:
            standard_cols.append(extra_col)
    df = df.loc[:, standard_cols]
    return df


def organize_columns(df: pd.DataFrame) -> pd.DataFrame:
    '''Moves 'ts', 'product', 'resolution', 'symbol' to the leftmost side.'''
    left_cols = ['ts', 'resolution', 'product']
    if 'symbol' in df.columns:
        left_cols.append('symbol')
    df = df.reindex(left_cols + [col for col in df.columns if col not in left_cols], axis=1)
    return df
    

def resample_data(df: IntoFrameT, resolution: str | Resolution) -> IntoFrameT:
    '''Resamples the input dataframe based on the target resolution.
    Args:
        df: The input dataframe to be resampled.
        resolution: The target resolution to resample the data to.
    Returns:
        The resampled dataframe.
    '''
    if isinstance(df, pd.DataFrame):
        data_tool = DataTool.pandas
    elif isinstance(df, (pl.LazyFrame, pl.DataFrame)):
        data_tool = DataTool.polars
    elif isinstance(df, dd.DataFrame):
        data_tool = DataTool.dask
    else:
        raise ValueError(f'{type(df)=}')
    
    df = convert_to_pandas_df(df)
    
    if isinstance(resolution, str):
        resolution = Resolution(resolution)
        
    # converts to pandas's resolution format
    eresolution = (
        repr(resolution)
        # 'min' means minute in pandas, please refer to https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#dateoffset-objects
        .replace('m', 'min')
        .replace('d', 'D')
        .replace('M', 'MS')  # MS = Month Start
        .replace('y', 'YS')  # YS = Year Start
        .replace('w', 'W-MON')  # W = Week, starting from Monday, otherwise, default is Sunday
    )
    
    is_tick_data = 'price' in df.columns
    assert not df.empty, 'data is empty'
    
    resample_logic = {
        'price': 'ohlc',
        'volume': 'sum',
    } if is_tick_data else {
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum',
    }
    
    for col in ['resolution', 'product', 'symbol']:
        if col in df.columns:
            resample_logic[col] = 'first'
    if 'dividends' in df.columns:
        resample_logic['dividends'] = 'sum'
    if 'splits' in df.columns:
        resample_logic['splits'] = 'prod'
          
    resampled_df = (
        df
        .set_index('ts')
        .resample(
            eresolution, 
            label='left',
            closed='left',  # closed is only default to be 'right' when resolution is week
        )
        .agg(resample_logic)
        # drop an unnecessary level created by 'ohlc' in the resample_logic
        .pipe(lambda df: df.droplevel(0, axis=1) if is_tick_data else df)
        .dropna()
        .reset_index()
    )
    resampled_df['resolution'] = repr(resolution)
    # after resampling, the columns order is not guaranteed to be the same as the original, so need to organize them
    # otherwise, polars will not be able to collect correctly
    resampled_df = organize_columns(resampled_df)
    return convert_to_user_df(resampled_df, data_tool)


def get_storage(data_model: BaseDataModel, storage: tSTORAGE, **kwargs) -> BaseStorage | None:
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
    data_model: BaseDataModel, 
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
    from pfeed.typing.core import is_dataframe
    if isinstance(data, bytes):
        return convert_raw_data_to_pandas_df(data)
    elif is_dataframe(data):
        df: Frame = nw.from_native(data)
        if isinstance(df, nw.LazyFrame):
            df = df.collect()
        return df.to_pandas()
    else:
        raise ValueError(f'{type(data)=}')


def convert_to_user_df(df: pd.DataFrame, data_tool: DataTool) -> tDataFrame:
    '''Converts the input dataframe to the user's desired data tool.
    Args:
        df: The input dataframe to be converted.
        data_tool: The data tool to convert the dataframe to.
            e.g. if data_tool is 'pandas', the returned the dataframe is a pandas dataframe.
    Returns:
        The converted dataframe.
    '''
    data_tool = data_tool.lower()
    if data_tool == DataTool.pandas:
        return df
    elif data_tool == DataTool.polars:
        return pl.from_pandas(df).lazy()
    elif data_tool == DataTool.dask:
        return dd.from_pandas(df, npartitions=1)
    # elif data_tool == DataTool.spark:
    #     spark = SparkSession.builder.getOrCreate()
    #     return spark.createDataFrame(df)
    else:
        raise ValueError(f'{data_tool=}')
