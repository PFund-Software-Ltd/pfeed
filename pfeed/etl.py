'''ETL = Extract, Transform, Load data.
Except extracting and loading data, this module uses "pandas" for data transformation.
'''
from __future__ import annotations
from typing import TYPE_CHECKING, Literal
if TYPE_CHECKING:
    from pfeed.types.common_literals import (
        tSUPPORTED_ENVIRONMENTS,
        tSUPPORTED_DOWNLOAD_DATA_SOURCES, 
        tSUPPORTED_STORAGES, 
        tSUPPORTED_DATA_TOOLS,
    )
    tOUTPUT_FORMATS = Literal['bytes'] | tSUPPORTED_DATA_TOOLS

import logging
import importlib

try:
    import pandas as pd
    import polars as pl
except ImportError:
    pass

from pfeed.resolution import ExtendedResolution
from pfeed.datastore import Datastore
from pfeed.filepath import FilePath
from pfeed.config_handler import get_config
from pfeed.const.common import (
    SUPPORTED_ENVIRONMENTS, 
    SUPPORTED_STORAGES, 
    SUPPORTED_DOWNLOAD_DATA_SOURCES, 
    SUPPORTED_DATA_TOOLS,
)
from pfeed.types.common_literals import tSUPPORTED_DATA_TOOLS
from pfeed.utils.utils import derive_trading_venue
from pfeed.utils.file_format import read_raw_data

try:
    from pfeed.utils.monitor import print_disk_usage
except ImportError:
    print_disk_usage = None


OUTPUT_FORMATS = ['bytes'] + SUPPORTED_DATA_TOOLS
DataFrame = pd.DataFrame | pl.DataFrame | pl.LazyFrame


__all__ = [
    'get_data',
    'extract_data',
    'transform_data',
    'load_data',
    'clean_raw_data',
    'standardize_raw_data',
    'resample_data',
]


def get_data(
    env: tSUPPORTED_ENVIRONMENTS,
    data_source: tSUPPORTED_DOWNLOAD_DATA_SOURCES,
    pdt: str,
    resolution: str | ExtendedResolution,
    date: str,
    storages: list[tSUPPORTED_STORAGES] | None = None,
    trading_venue: str='',
    output_format: tOUTPUT_FORMATS='pandas',
) -> bytes | DataFrame | None:
    """Extract data without specifying the data origin. 
    This function will try to extract data from all supported data origins.

    Args:
        env: trading environment, e.g. 'PAPER' | 'LIVE'.
        data_source (Literal['BYBIT']): The data source to extract data from.
        pdt (str): product, e.g. BTC_USDT_PERP.
        resolution: Data resolution. e.g. '1m' = 1 minute as the unit of each data bar/candle.
            Also supports raw resolution such as 'r1m', where 'r' stands for raw.            
            Default is '1d' = 1 day.
        date (str): The date of the data to extract.
        storages: origins of data to search from, default is all supported storages
        trading_venue (str): trading venue's name, e.g. exchange's name or dapp's name
        output_format: The format of the output data. Default is 'pandas'.
    Returns:
        bytes | DataFrame | None: The extracted data as bytes, or None if the data is not found.
    """    
    logger = logging.getLogger(data_source.lower() + '_data')
    storages = storages or SUPPORTED_STORAGES
    for storage in storages:
        logger.debug(f'searching {storage=} for {data_source} {pdt} {resolution} {date} data')
        data: bytes | pd.DataFrame | None = extract_data(env, storage, data_source, pdt, resolution, date, trading_venue=trading_venue, output_format=output_format)
        if data is not None:
            return data


def extract_data(
    env: tSUPPORTED_ENVIRONMENTS,
    storage: tSUPPORTED_STORAGES,
    data_source: tSUPPORTED_DOWNLOAD_DATA_SOURCES,
    pdt: str,
    resolution: str | ExtendedResolution,
    date: str,
    trading_venue: str='',
    output_format: tOUTPUT_FORMATS='pandas',
) -> bytes | DataFrame | None:
    """
    Extracts data from a specified data source and returns it as bytes.

    Args:
        env: trading environment, e.g. 'PAPER' | 'LIVE'.
        storage: The origin of the data (e.g. local or minio).
        data_source: The source of the data.
        pdt (str): product, e.g. BTC_USDT_PERP.
        resolution: Data resolution. e.g. '1m' = 1 minute as the unit of each data bar/candle.
            Also supports raw resolution such as 'r1m', where 'r' stands for raw.            
            Default is '1d' = 1 day.
        date (str): The date of the data.
        trading_venue: trading venue's name, e.g. exchange's name or dapp's name
        output_format: The format of the output data. Default is 'pandas'.
    Returns:
        bytes | DataFrame | None: The extracted data as bytes, or None if extraction fails.

    Raises:
        AssertionError: If any of the input parameters are invalid.
        NotImplementedError: If the data origin is not supported.
        MinioException: If MinIO is not running / set up correctly.
    """
    logger = logging.getLogger(data_source.lower() + '_data')
    env, storage, data_source, pdt, output_format = env.upper(), storage.lower(), data_source.upper(), pdt.upper(), output_format.lower()
    trading_venue = trading_venue or derive_trading_venue(data_source)
    trading_venue = trading_venue.upper()
    assert env in SUPPORTED_ENVIRONMENTS, f'Invalid {env=}, {SUPPORTED_ENVIRONMENTS=}'
    assert storage in SUPPORTED_STORAGES, f'Invalid {storage=}, {SUPPORTED_STORAGES=}'
    assert data_source in SUPPORTED_DOWNLOAD_DATA_SOURCES, f'Invalid {data_source=}, SUPPORTED DATA SOURCES={SUPPORTED_DOWNLOAD_DATA_SOURCES}'
    assert output_format in OUTPUT_FORMATS, f'Invalid {output_format=}, {OUTPUT_FORMATS=}'
    if isinstance(resolution, str):
        resolution = ExtendedResolution(resolution)
    if output_format != 'bytes':
        data_tool = importlib.import_module(f'pfeed.data_tools.data_tool_{output_format.lower()}')
    config = get_config()
    fp = FilePath(env, data_source, trading_venue, pdt, resolution, date, file_extension='.parquet', data_path=config.data_path)
    try:
        if storage == 'local':
            if fp.exists():
                if output_format == 'bytes':
                    with open(fp.file_path, 'rb') as f:
                        data: bytes = f.read()
                else:
                    data: DataFrame = data_tool.read_parquet(fp.file_path)
                logger.debug(f'extracted {data_source} {pdt} {date} {resolution} data from local path {fp.file_path}')
                return data
            else:
                logger.debug(f'failed to extract {data_source} {pdt} {date} {resolution} data from local path {fp.file_path}')
        elif storage == 'minio':
            datastore = Datastore(storage)
            object_name = fp.storage_path
            if datastore.exist_object(object_name):
                if output_format != 'bytes':
                    file_path = "s3://" + datastore.BUCKET_NAME + "/" + object_name
                    data: DataFrame = data_tool.read_parquet(file_path, storage='minio')
                else:
                    data: bytes | None = datastore.get_object(object_name)
                logger.debug(f'extracted {data_source} {pdt} {date} {resolution} data from MinIO')
                return data
            else:
                logger.debug(f'No data found in MinIO for {data_source} {pdt} {date} {resolution}')
        else:
            raise NotImplementedError(f'{storage=}')
    except Exception as err:
        logger.exception(f'failed to extract {data_source} {pdt} {date} {resolution} data from {storage}, {err=}')
    
    
def transform_data(
    data_source: tSUPPORTED_DOWNLOAD_DATA_SOURCES,
    data: bytes | pd.DataFrame | pl.LazyFrame,
    data_resolution: str | ExtendedResolution,
    target_resolution: str | ExtendedResolution,
) -> bytes | pd.DataFrame | pl.LazyFrame:
    """Transforms data to a target resolution"""
    if isinstance(data_resolution, str):
        data_resolution = ExtendedResolution(data_resolution)
    if isinstance(target_resolution, str):
        target_resolution = ExtendedResolution(target_resolution)
    
    data_source = data_source.upper()
    assert data_source in SUPPORTED_DOWNLOAD_DATA_SOURCES, f'Invalid {data_source=}, SUPPORTED DATA SOURCES={SUPPORTED_DOWNLOAD_DATA_SOURCES}'
    assert data_resolution.is_ge(target_resolution), f'{data_resolution=} is less than {target_resolution=}'
    
    if data_resolution == target_resolution:
        return data
    elif data_resolution.is_raw() and target_resolution.is_raw():  # e.g. 'r1t' -> 'r1m
        raise Exception(f'{data_resolution=} and {target_resolution=} are both raw resolutions')
    else:
        data: bytes | pd.DataFrame | pl.LazyFrame = standardize_raw_data(data, data_resolution.is_tick())
        if target_resolution.is_tick():
            return data
        else:
            return resample_data(data, target_resolution)


def load_data(
    env: tSUPPORTED_ENVIRONMENTS,
    storage: tSUPPORTED_STORAGES,
    data_source: tSUPPORTED_DOWNLOAD_DATA_SOURCES,
    data: bytes,
    pdt: str,
    resolution: str | ExtendedResolution,
    date: str,
    trading_venue: str='',
    **kwargs
) -> None:
    """
    Loads data into the specified data destination.

    Args:
        env: trading environment, e.g. 'PAPER' | 'LIVE'.
        storage: The destination where the data will be loaded. 
            It can be either 'local' or 'minio'.
        data_source: The source of the data.
        data (bytes): The data to be loaded.
        pdt (str): product, e.g. BTC_USDT_PERP.
        resolution: Data resolution. e.g. '1m' = 1 minute as the unit of each data bar/candle.
            Also supports raw resolution such as 'r1m', where 'r' stands for raw.            
            Default is '1d' = 1 day.
        date (str): The date of the data.
        trading_venue: trading venue's name, e.g. exchange's name or dapp's name
        **kwargs: Additional keyword arguments for MinIO.

    Returns:
        None
        
    Raises:
        AssertionError: If any of the input parameters are invalid.
        NotImplementedError: If the specified data destination is not implemented.
        MinioException: If MinIO is not running / set up correctly.
    """
    logger = logging.getLogger(data_source.lower() + '_data')
    
    env, storage, data_source, pdt = env.upper(), storage.lower(), data_source.upper(), pdt.upper()
    trading_venue = trading_venue or derive_trading_venue(data_source)
    trading_venue = trading_venue.upper()
    assert env in SUPPORTED_ENVIRONMENTS, f'Invalid {env=}, {SUPPORTED_ENVIRONMENTS=}'
    assert storage in SUPPORTED_STORAGES, f'Invalid {storage=}, {SUPPORTED_STORAGES=}'
    assert data_source in SUPPORTED_DOWNLOAD_DATA_SOURCES, f'Invalid {data_source=}, SUPPORTED DATA SOURCES={SUPPORTED_DOWNLOAD_DATA_SOURCES}'
    if isinstance(resolution, str):
        resolution = ExtendedResolution(resolution)
    
    config = get_config()
    fp = FilePath(env, data_source, trading_venue, pdt, resolution, date, file_extension='.parquet', data_path=config.data_path)
    if storage == 'local':
        fp.parent.mkdir(parents=True, exist_ok=True)
        with open(fp.file_path, 'wb') as f:
            f.write(data)
            logger.info(f'loaded {data_source} data to {fp.file_path}')
    elif storage == 'minio':
        datastore = Datastore(storage)
        object_name = fp.storage_path
        datastore.put_object(object_name, data, **kwargs)
        logger.info(f'loaded {data_source} data to MinIO object {object_name} {kwargs=}')
    else:
        raise NotImplementedError(f'{storage=}')
    if print_disk_usage:
        print_disk_usage(config.data_path)
        

def clean_raw_data(
    data_source: tSUPPORTED_DOWNLOAD_DATA_SOURCES, 
    data: bytes,
) -> bytes:
    '''
    Cleans raw data by renaming columns, mapping columns, and converting timestamp.
    bytes (any format, e.g. csv.gzip) in, bytes (parquet file) out.

    Args:
        data_source: The source of the data.
        data (bytes): The raw data to be cleaned.
    
    Returns:
        bytes: The cleaned raw data.
    '''
    assert data_source in SUPPORTED_DOWNLOAD_DATA_SOURCES, f'Invalid {data_source=}, SUPPORTED DATA SOURCES={SUPPORTED_DOWNLOAD_DATA_SOURCES}'
    
    const = importlib.import_module(f'pfeed.sources.{data_source.lower()}.const')
    utils = importlib.import_module(f'pfeed.sources.{data_source.lower()}.utils')
    
    df: pd.DataFrame = _convert_data_to_pandas_df(data)
    if RENAMING_COLS := getattr(const, 'RENAMING_COLS', {}):
        df = df.rename(columns=RENAMING_COLS)
    if MAPPING_COLS := getattr(const, 'MAPPING_COLS', {}):
        df['side'] = df['side'].map(MAPPING_COLS)
    df = utils.standardize_ts_column(df)
    return _handle_result(data, df)


def standardize_raw_data(
    data: bytes | pd.DataFrame | pl.LazyFrame, 
    is_tick: bool
) -> bytes | pd.DataFrame | pl.LazyFrame:
    """Filter out unnecessary columns from raw data.

    Args:
        data (bytes): The raw data in bytes format.

    Returns:
        bytes | pd.DataFrame | pl.LazyFrame: The standardized data.
    """
    df: pd.DataFrame = _convert_data_to_pandas_df(data)
    assert 'ts' in df.columns, 'ts column not found, please check if the raw data has been cleaned'
    if is_tick:
        df = df.loc[:, ['ts', 'side', 'volume', 'price']]
    else:
        df = df.loc[:, ['ts', 'open', 'high', 'low', 'close', 'volume']]
    return _handle_result(data, df)


def resample_data(
    data: bytes | pd.DataFrame | pl.LazyFrame, 
    resolution: str | ExtendedResolution, 
) -> bytes | pd.DataFrame | pl.LazyFrame:
    '''
    Resamples the input data based on the specified resolution and returns the resampled data in Parquet format.
    
    Args:
        data (bytes): The input data to be resampled.
        resolution (str | Resolution): The resolution at which the data should be resampled. 
            if string, it should be in the format of "# + unit (s/m/h/d)", e.g. "1s".
    '''
    # standardize resolution by following pfund's standard, e.g. '1minute' -> '1m'
    if isinstance(resolution, str):
        resolution = ExtendedResolution(resolution)
        
    # converts to pandas's resolution format
    eresolution = repr(resolution)
        
    # 'min' means minute in pandas, please refer to https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#dateoffset-objects
    eresolution = eresolution.replace('m', 'min')
    eresolution = eresolution.replace('d', 'D')
    
    df: pd.DataFrame = _convert_data_to_pandas_df(data)
    
    is_tick_data = True if 'price' in df.columns else False
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
    
    return _handle_result(data, resampled_df)


def _convert_data_to_pandas_df(data: bytes | pd.DataFrame | pl.LazyFrame) -> pd.DataFrame:
    """Converts data to pandas DataFrame."""
    if isinstance(data, bytes):
        df = read_raw_data(data)
    elif isinstance(data, pd.DataFrame):
        df = data
    elif isinstance(data, pl.LazyFrame):
        df = data.collect().to_pandas()
    else:
        raise TypeError(f'Invalid data type {type(data)}, expected bytes or pd.DataFrame or pl.LazyFrame')
    return df


def _handle_result(input_data: bytes | pd.DataFrame | pl.LazyFrame, output_df: pd.DataFrame) -> bytes | pd.DataFrame | pl.LazyFrame:
    """Outputs the data in the same format as the input data."""
    if isinstance(input_data, bytes):
        return output_df.to_parquet(compression='zstd')
    elif isinstance(input_data, pd.DataFrame):
        return output_df
    elif isinstance(input_data, pl.LazyFrame):
        return pl.from_pandas(output_df).lazy()
    else:
        raise TypeError(f'Invalid data type {type(input_data)}, expected bytes or pd.DataFrame or pl.LazyFrame')