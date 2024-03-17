'''ETL = Extract, Transform, Load data'''
import io
import logging
import importlib

from typing import Literal

import numpy as np
import pandas as pd
from minio.error import MinioException

from pfeed.datastore import Datastore
from pfeed.filepath import FilePath
from pfeed.config_handler import ConfigHandler
from pfeed.const.commons import SUPPORTED_DATA_TYPES, SUPPORTED_DATA_STORAGES, SUPPORTED_DOWNLOAD_DATA_SOURCES, SUPPORTED_DATA_MODES
from pfeed.utils.monitor import print_disk_usage


logger = logging.getLogger('pfeed')


def _convert_raw_dtype_to_explicit(data_source: str, dtype: str):
    """Covnerts implicit dtype 'raw' to explicit one by using the first element in SUPPORTED_RAW_DATA_TYPES, e.g. 'raw_tick'."""
    try:
        SUPPORTED_RAW_DATA_TYPES = getattr(importlib.import_module(f'pfeed.sources.{data_source.lower()}.const'), 'SUPPORTED_RAW_DATA_TYPES')
    except ModuleNotFoundError:
        raise ValueError(f'Unsupported {dtype=} for {data_source=}')
    dtype = SUPPORTED_RAW_DATA_TYPES[0]
    return dtype


def get_data(
    data_source: Literal['BYBIT'],
    dtype: Literal['raw_tick', 'raw', 'tick', 'second', 'minute', 'hour', 'daily'],
    pdt: str,
    date: str,
    mode: Literal['historical', 'streaming']='historical',
) -> bytes | None:
    """Extract data without specifying the data origin. 
    This function will try to extract data from all supported data origins.

    Args:
        data_source (Literal['BYBIT']): The data source to extract data from.
        dtype (Literal['raw_tick', 'raw', 'tick', 'second', 'minute', 'hour', 'daily']): The type of data to extract.
        pdt (str): product, e.g. BTC_USDT_PERP.
        date (str): The date of the data to extract.
        mode (Literal['historical', 'streaming'], optional): The mode of extraction. Defaults to 'historical'.

    Returns:
        bytes | None: The extracted data as bytes, or None if the data is not found.
    """
    for data_origin in SUPPORTED_DATA_STORAGES:
        try:
            data: bytes = extract_data(data_origin, data_source, dtype, pdt, date, mode=mode)
        except MinioException:
            data = None
        if data:
            return data
    else:
        logger.warning(f'{data_source} {pdt} {date} {dtype} data is nowhere to be found, {SUPPORTED_DATA_STORAGES=}')


def extract_data(
    data_origin: Literal['local', 'minio'],
    data_source: Literal['BYBIT'],
    dtype: Literal['raw_tick', 'raw', 'tick', 'second', 'minute', 'hour', 'daily'],
    pdt: str,
    date: str,
    mode: Literal['historical', 'streaming']='historical',
) -> bytes | None:
    """
    Extracts data from a specified data source and returns it as bytes.

    Args:
        data_origin (Literal['local', 'minio']): The origin of the data (local or minio).
        data_source (Literal['BYBIT']): The source of the data.
        dtype (Literal['raw_tick', 'raw', 'tick', 'second', 'minute', 'hour', 'daily']): The type of data to extract.
        pdt (str): product, e.g. BTC_USDT_PERP.
        date (str): The date of the data.
        mode (Literal['historical', 'streaming'], optional): The mode of extraction. Defaults to 'historical'.

    Returns:
        bytes | None: The extracted data as bytes, or None if extraction fails.

    Raises:
        AssertionError: If any of the input parameters are invalid.
        NotImplementedError: If the data origin is not supported.
        MinioException: If MinIO is not running / set up correctly.
    """
    
    assert data_origin in SUPPORTED_DATA_STORAGES, f'Invalid {data_origin=}, {SUPPORTED_DATA_STORAGES=}'
    assert data_source in SUPPORTED_DOWNLOAD_DATA_SOURCES, f'Invalid {data_source=}, SUPPORTED DATA SOURCES={SUPPORTED_DOWNLOAD_DATA_SOURCES}'
    if dtype == 'raw':
        dtype = _convert_raw_dtype_to_explicit(data_source, dtype)
    assert dtype in SUPPORTED_DATA_TYPES, f'Invalid {dtype=}, {SUPPORTED_DATA_TYPES=}'
    assert mode in SUPPORTED_DATA_MODES, f'Invalid {mode=}, {SUPPORTED_DATA_MODES=}'
     
    config = ConfigHandler.load_config()
    fp = FilePath(data_source, mode, dtype, pdt, date, data_path=config.data_path, file_extension='.parquet')
    if data_origin == 'local':
        if fp.exists():
            with open(fp.file_path, 'rb') as f:
                data: bytes = f.read()
            logger.debug(f'extracted {data_source} data from {fp.storage_path}')
            return data
        else:
            logger.debug(f'failed to extract {data_source} data from local path: {fp.storage_path}')
    elif data_origin == 'minio':
        datastore = Datastore()
        object_name = fp.storage_path
        data: bytes | None = datastore.get_object(object_name)
        if data:
            logger.debug(f'extracted {data_source} data from MinIO object {object_name}')
        else:
            logger.debug(f'failed to extract {data_source} data from MinIO object {object_name}')
        return data
    else:
        raise NotImplementedError(f'{data_origin=}')


def load_data(
    data_destination: Literal['local', 'minio'],
    data_source: Literal['BYBIT'],
    data: bytes,
    dtype: Literal['raw_tick', 'raw', 'tick', 'second', 'minute', 'hour', 'daily'],
    pdt: str,
    date: str,
    mode: Literal['historical', 'streaming'] = 'historical',
    **kwargs
) -> None:
    """
    Loads data into the specified data destination.

    Args:
        data_destination (Literal['local', 'minio']): The destination where the data will be loaded. 
            It can be either 'local' or 'minio'.
        data_source (Literal['BYBIT']): The source of the data.
        data (bytes): The data to be loaded.
        dtype (Literal['raw_tick', 'raw', 'tick', 'second', 'minute', 'hour', 'daily']): The type of the data.
        pdt (str): product, e.g. BTC_USDT_PERP.
        date (str): The date of the data.
        mode (Literal['historical', 'streaming'], optional): The mode of loading the data. 
            Defaults to 'historical'.
        **kwargs: Additional keyword arguments for MinIO.

    Returns:
        None
        
    Raises:
        AssertionError: If any of the input parameters are invalid.
        NotImplementedError: If the specified data destination is not implemented.
        MinioException: If MinIO is not running / set up correctly.
    """
      
    assert data_destination in SUPPORTED_DATA_STORAGES, f'Invalid {data_destination=}, {SUPPORTED_DATA_STORAGES=}'
    assert data_source in SUPPORTED_DOWNLOAD_DATA_SOURCES, f'Invalid {data_source=}, SUPPORTED DATA SOURCES={SUPPORTED_DOWNLOAD_DATA_SOURCES}'
    if dtype == 'raw':
        dtype = _convert_raw_dtype_to_explicit(data_source, dtype)
    assert dtype in SUPPORTED_DATA_TYPES, f'Invalid {dtype=}, {SUPPORTED_DATA_TYPES=}'
    assert mode in SUPPORTED_DATA_MODES, f'Invalid {mode=}, {SUPPORTED_DATA_MODES=}'
    
    config = ConfigHandler.load_config()
    fp = FilePath(data_source, mode, dtype, pdt, date, data_path=config.data_path, file_extension='.parquet')
    if data_destination == 'local':
        fp.parent.mkdir(parents=True, exist_ok=True)
        with open(fp.file_path, 'wb') as f:
            f.write(data)
            logger.debug(f'loaded {data_source} data to {fp.storage_path}')
    elif data_destination == 'minio':
        datastore = Datastore()
        object_name = fp.storage_path
        datastore.put_object(object_name, data, **kwargs)
        logger.debug(f'loaded {data_source} data to MinIO object {object_name} {kwargs=}')
    else:
        raise NotImplementedError(f'{data_destination=}')
    print_disk_usage(config.data_path)
        

def clean_raw_data(data_source: str, raw_data: bytes) -> bytes:
    """
    Convert raw data to internal data format, e.g. renaming columns, setting index to datetime.

    Args:
        data_source (str): The data source from which the raw data is obtained.
        raw_data (bytes): The raw data to be cleaned.

    Returns:
        bytes: The cleaned data in bytes format.

    Raises:
        AssertionError: If the data source is not supported.
    """
    assert data_source in SUPPORTED_DOWNLOAD_DATA_SOURCES, f'Invalid {data_source=}, SUPPORTED DATA SOURCES={SUPPORTED_DOWNLOAD_DATA_SOURCES}'
    RENAMING_COLS = getattr(importlib.import_module(f'pfeed.sources.{data_source.lower()}.const'), 'RENAMING_COLS')
    
    df = pd.read_csv(io.BytesIO(raw_data), compression='gzip')
    df['side'] = df['side'].map({'Buy': 1, 'Sell': -1})
    df = df.rename(columns=RENAMING_COLS)
    # NOTE: for ptype SPOT, unit is ms, e.g. 1671580800123, in milliseconds
    unit = 'ms' if df['ts'][0] > 10**12 else 's'  # REVIEW
    # NOTE: this may make the `ts` value inaccurate, e.g. 1671580800.9906 -> 1671580800.990600192
    df['ts'] = pd.to_datetime(df['ts'], unit=unit)
    raw_tick: bytes = df.to_parquet(compression='snappy')
    return raw_tick


def clean_raw_tick_data(raw_tick: bytes) -> bytes:
    """Filter out unnecessary columns from raw tick data.

    Args:
        raw_tick (bytes): The raw tick data in bytes format.

    Returns:
        bytes: The cleaned tick data in bytes format.
    """
    df = pd.read_parquet(io.BytesIO(raw_tick))
    df = df.loc[:, ['ts', 'side', 'volume', 'price']]
    tick_data: bytes = df.to_parquet(compression='snappy')
    return tick_data


def resample_data(data: bytes, resolution: str, only_ohlcv=False) -> bytes:
    '''
    Resamples the input data based on the specified resolution and returns the resampled data in Parquet format.
    
    Args:
        data (bytes): The input data to be resampled.
        resolution (str): The resolution at which the data should be resampled. It should be in the format of "# + unit (s/m/h/d)", e.g. "1s".
        only_ohlcv (bool, optional): If True, only the OHLCV (Open, High, Low, Close, Volume) columns will be included in the resampled data. Defaults to False.
    
    Returns:
        bytes: The resampled data in Parquet format.
    '''
    from pfund.datas.resolution import Resolution
    
    def find_high_or_low_first(series: pd.Series):
        if not series.empty:
            argmax, argmin = series.argmax(), series.argmin()
            if argmax < argmin:
                return 'H'
            elif argmax > argmin:
                return 'L'
        return np.nan
    
    def determine_first(row):
        if row['arg_high'] < row['arg_low']:
            return 'H'
        elif row['arg_high'] > row['arg_low']:
            return 'L'
        else:
            # If arg_high == arg_low, return the original value of 'first'
            return row['first']
        
    resolution = repr(Resolution(resolution))
        
    # 'min' means minute in pandas, please refer to https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#dateoffset-objects
    if 'm' in resolution:
        eresolution = resolution.replace('m', 'min')
    elif 'd' in resolution:
        eresolution = resolution.replace('d', 'D')
    else:
        eresolution = resolution
    
    if type(data) is bytes:
        df = pd.read_parquet(io.BytesIO(data))
    else:
        raise TypeError(f'invalid data type {type(data)}')
    
    is_tick_data = True if 'price' in df.columns else False
    assert not df.empty, 'data is empty'
    df.set_index('ts', inplace=True)
    
    if is_tick_data:
        df['num_buys'] = df['side'].map({1: 1, -1: np.nan})
        df['num_sells'] = df['side'].map({1: np.nan, -1: 1})
        df['buy_volume'] = np.where(df['side'] == 1, df['volume'], np.nan)
        df['sell_volume'] = np.where(df['side'] == -1, df['volume'], np.nan)
        df['first'] = df['price']
        resample_logic = {
            'num_buys': 'sum',
            'num_sells': 'sum',
            'volume': 'sum',
            'buy_volume': 'sum',
            'sell_volume': 'sum',
            'price': 'ohlc',
            'first': find_high_or_low_first
        }
    else:
        df['arg_high'] = df['high']
        df['arg_low'] = df['low']
        resample_logic = {
            'num_buys': 'sum',
            'num_sells': 'sum',
            'buy_volume': 'sum',
            'sell_volume': 'sum',
            'volume': 'sum',
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'arg_high': lambda series: series.argmax() if not series.empty else None,
            'arg_low': lambda series: series.argmin() if not series.empty else None,
        }
        if 'first' in df.columns:
            resample_logic['first'] = 'first'
            
    resampled_df = (
        df
        .resample(eresolution)
        .apply(resample_logic)
    )
    
    if is_tick_data:
        resampled_df = resampled_df.droplevel(0, axis=1)
        # convert float to int
        resampled_df['num_buys'] = resampled_df['num_buys'].astype(int)
        resampled_df['num_sells'] = resampled_df['num_sells'].astype(int)
    else:
        resampled_df['first'] = resampled_df.apply(determine_first, axis=1)
        resampled_df.drop(columns=['arg_high', 'arg_low'], inplace=True)

    resampled_df.dropna(subset=[col for col in resampled_df.columns if col != 'first'], inplace=True)
    
    if only_ohlcv:
        resampled_df = resampled_df[['open', 'high', 'low', 'close', 'volume']]
    
    resampled_df.reset_index(inplace=True)
    return resampled_df.to_parquet(compression='snappy')
