'''ETL = Extract, Transform, Load data'''
from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfeed.types.common_literals import (
        tSUPPORTED_ENVIRONMENTS,
        tSUPPORTED_DOWNLOAD_DATA_SOURCES, 
        tSUPPORTED_DATA_SINKS, 
        tSUPPORTED_DATA_TYPES, 
    )
    try:
        import pandas as pd
    except ImportError:
        pass
    from pfund.datas.resolution import Resolution
    
import io
import logging
import importlib

try:
    import polars as pl
except ImportError:
    pass

from pfeed.datastore import Datastore
from pfeed.filepath import FilePath
from pfeed.config_handler import get_config
from pfeed.const.common import (
    SUPPORTED_ENVIRONMENTS, 
    SUPPORTED_DATA_TYPES, 
    SUPPORTED_DATA_SINKS, 
    SUPPORTED_DOWNLOAD_DATA_SOURCES, 
)
from pfeed.utils.utils import derive_trading_venue

try:
    from pfeed.utils.monitor import print_disk_usage
except ImportError:
    print_disk_usage = None


__all__ = [
    'get_data',
    'extract_data',
    'load_data',
    'clean_raw_data',
    'clean_raw_tick_data',
    'resample_data',
]


def _convert_raw_dtype_to_explicit(data_source: str, dtype: str):
    """Covnerts implicit dtype 'raw' to explicit one by using the first element in SUPPORTED_RAW_DATA_TYPES, e.g. 'raw_tick'."""
    try:
        SUPPORTED_RAW_DATA_TYPES = getattr(importlib.import_module(f'pfeed.sources.{data_source.lower()}.const'), 'SUPPORTED_RAW_DATA_TYPES')
    except ModuleNotFoundError:
        raise ValueError(f'Unsupported {dtype=} for {data_source=}')
    dtype = SUPPORTED_RAW_DATA_TYPES[0]
    return dtype


def get_data(
    env: tSUPPORTED_ENVIRONMENTS,
    data_source: tSUPPORTED_DOWNLOAD_DATA_SOURCES,
    dtype: tSUPPORTED_DATA_TYPES,
    pdt: str,
    date: str,
    trading_venue: str='',
) -> bytes | None:
    """Extract data without specifying the data origin. 
    This function will try to extract data from all supported data origins.

    Args:
        env: trading environment, e.g. 'PAPER' | 'LIVE'.
        data_source (Literal['BYBIT']): The data source to extract data from.
        dtype (Literal['raw_tick', 'raw', 'tick', 'second', 'minute', 'hour', 'daily']): The type of data to extract.
        pdt (str): product, e.g. BTC_USDT_PERP.
        date (str): The date of the data to extract.
        trading_venue (str): trading venue's name, e.g. exchange's name or dapp's name

    Returns:
        bytes | None: The extracted data as bytes, or None if the data is not found.
    """    
    try:
        from minio.error import MinioException
    except ImportError:
        MinioException = Exception
    
    trading_venue = trading_venue or derive_trading_venue(data_source)
    
    for data_sink in SUPPORTED_DATA_SINKS:
        try:
            data: bytes = extract_data(env, data_sink, data_source, trading_venue, dtype, pdt, date)
        except MinioException:
            data = None
        if data:
            return data


def extract_data(
    env: tSUPPORTED_ENVIRONMENTS,
    data_sink: tSUPPORTED_DATA_SINKS,
    data_source: tSUPPORTED_DOWNLOAD_DATA_SOURCES,
    trading_venue: str,
    dtype: tSUPPORTED_DATA_TYPES,
    pdt: str,
    date: str,
) -> bytes | None:
    """
    Extracts data from a specified data source and returns it as bytes.

    Args:
        env: trading environment, e.g. 'PAPER' | 'LIVE'.
        data_sink: The origin of the data (local or minio).
        data_source: The source of the data.
        trading_venue: trading venue's name, e.g. exchange's name or dapp's name
        dtype: The type of data to extract.
        pdt (str): product, e.g. BTC_USDT_PERP.
        date (str): The date of the data.
    Returns:
        bytes | None: The extracted data as bytes, or None if extraction fails.

    Raises:
        AssertionError: If any of the input parameters are invalid.
        NotImplementedError: If the data origin is not supported.
        MinioException: If MinIO is not running / set up correctly.
    """
    logger = logging.getLogger(data_source.lower() + '_data')
    
    env, data_sink, data_source, dtype, pdt = env.upper(), data_sink.lower(), data_source.upper(), dtype.lower(), pdt.upper()
    assert env in SUPPORTED_ENVIRONMENTS, f'Invalid {env=}, {SUPPORTED_ENVIRONMENTS=}'
    assert data_sink in SUPPORTED_DATA_SINKS, f'Invalid {data_sink=}, {SUPPORTED_DATA_SINKS=}'
    assert data_source in SUPPORTED_DOWNLOAD_DATA_SOURCES, f'Invalid {data_source=}, SUPPORTED DATA SOURCES={SUPPORTED_DOWNLOAD_DATA_SOURCES}'
    if dtype == 'raw':
        dtype = _convert_raw_dtype_to_explicit(data_source, dtype)
    assert dtype in SUPPORTED_DATA_TYPES, f'Invalid {dtype=}, {SUPPORTED_DATA_TYPES=}'
     
    config = get_config()
    fp = FilePath(env, data_source, trading_venue, dtype, pdt, date, file_extension='.parquet', data_path=config.data_path)
    if data_sink == 'local':
        if fp.exists():
            with open(fp.file_path, 'rb') as f:
                data: bytes = f.read()
            logger.debug(f'extracted {data_source} {dtype} data from local path {fp.file_path}')
            return data
        else:
            logger.debug(f'failed to extract {data_source} {dtype} data from local path {fp.file_path}')
    elif data_sink == 'minio':
        datastore = Datastore(data_sink)
        object_name = fp.storage_path
        data: bytes | None = datastore.get_object(object_name)
        if data:
            logger.debug(f'extracted {data_source} {dtype} data from MinIO object {object_name}')
        else:
            logger.debug(f'failed to extract {data_source} {dtype} data from MinIO object {object_name}')
        return data
    else:
        raise NotImplementedError(f'{data_sink=}')


def load_data(
    env: tSUPPORTED_ENVIRONMENTS,
    data_sink: tSUPPORTED_DATA_SINKS,
    data_source: tSUPPORTED_DOWNLOAD_DATA_SOURCES,
    trading_venue: str,
    data: bytes,
    dtype: tSUPPORTED_DATA_TYPES,
    pdt: str,
    date: str,
    **kwargs
) -> None:
    """
    Loads data into the specified data destination.

    Args:
        env: trading environment, e.g. 'PAPER' | 'LIVE'.
        data_sink: The destination where the data will be loaded. 
            It can be either 'local' or 'minio'.
        data_source: The source of the data.
        trading_venue: trading venue's name, e.g. exchange's name or dapp's name
        data (bytes): The data to be loaded.
        dtype: The type of the data.
        pdt (str): product, e.g. BTC_USDT_PERP.
        date (str): The date of the data.
        **kwargs: Additional keyword arguments for MinIO.

    Returns:
        None
        
    Raises:
        AssertionError: If any of the input parameters are invalid.
        NotImplementedError: If the specified data destination is not implemented.
        MinioException: If MinIO is not running / set up correctly.
    """
    logger = logging.getLogger(data_source.lower() + '_data')
    
    env, data_sink, data_source, dtype, pdt = env.upper(), data_sink.lower(), data_source.upper(), dtype.lower(), pdt.upper()
    assert env in SUPPORTED_ENVIRONMENTS, f'Invalid {env=}, {SUPPORTED_ENVIRONMENTS=}'
    assert data_sink in SUPPORTED_DATA_SINKS, f'Invalid {data_sink=}, {SUPPORTED_DATA_SINKS=}'
    assert data_source in SUPPORTED_DOWNLOAD_DATA_SOURCES, f'Invalid {data_source=}, SUPPORTED DATA SOURCES={SUPPORTED_DOWNLOAD_DATA_SOURCES}'
    if dtype == 'raw':
        dtype = _convert_raw_dtype_to_explicit(data_source, dtype)
    assert dtype in SUPPORTED_DATA_TYPES, f'Invalid {dtype=}, {SUPPORTED_DATA_TYPES=}'
    
    config = get_config()
    fp = FilePath(env, data_source, trading_venue, dtype, pdt, date, file_extension='.parquet', data_path=config.data_path)
    if data_sink == 'local':
        fp.parent.mkdir(parents=True, exist_ok=True)
        with open(fp.file_path, 'wb') as f:
            f.write(data)
            logger.info(f'loaded {data_source} data to {fp.file_path}')
    elif data_sink == 'minio':
        datastore = Datastore(data_sink)
        object_name = fp.storage_path
        datastore.put_object(object_name, data, **kwargs)
        logger.info(f'loaded {data_source} data to MinIO object {object_name} {kwargs=}')
    else:
        raise NotImplementedError(f'{data_sink=}')
    if print_disk_usage:
        print_disk_usage(config.data_path)
        

def clean_raw_data(data_source: tSUPPORTED_DOWNLOAD_DATA_SOURCES, raw_data: bytes) -> bytes:
    import pandas as pd
    module = importlib.import_module(f'pfeed.sources.{data_source.lower()}.const')
    RENAMING_COLS = getattr(module, 'RENAMING_COLS')
    MAPPING_COLS = getattr(module, 'MAPPING_COLS')
    df = pd.read_csv(io.BytesIO(raw_data), compression='gzip')
    df = df.rename(columns=RENAMING_COLS)
    df['side'] = df['side'].map(MAPPING_COLS)
    # NOTE: for ptype SPOT, unit is 'ms', e.g. 1671580800123, in milliseconds
    unit = 'ms' if df['ts'][0] > 10**12 else 's'  # REVIEW
    # EXTEND: not all 'ts' columns from raw data are float, 
    # they could be: 'datetime' | 'datetime_str' | 'float' | 'float_str'
    # NOTE: this may make the `ts` value inaccurate, e.g. 1671580800.9906 -> 1671580800.990600192
    df['ts'] = pd.to_datetime(df['ts'], unit=unit)
    raw_tick: bytes = df.to_parquet(compression='zstd')
    return raw_tick


def clean_raw_tick_data(raw_tick: bytes) -> bytes:
    """Filter out unnecessary columns from raw tick data.

    Args:
        raw_tick (bytes): The raw tick data in bytes format.

    Returns:
        bytes: The cleaned tick data in bytes format.
    """
    import pandas as pd
    df = pd.read_parquet(io.BytesIO(raw_tick))
    df = df.loc[:, ['ts', 'side', 'volume', 'price']]
    tick_data: bytes = df.to_parquet(compression='zstd')
    return tick_data


def resample_data(
    data: bytes | pd.DataFrame | pl.LazyFrame, 
    resolution: str | Resolution, 
) -> bytes | pd.DataFrame | pl.LazyFrame:
    '''
    Resamples the input data based on the specified resolution and returns the resampled data in Parquet format.
    
    Args:
        data (bytes): The input data to be resampled.
        resolution (str | Resolution): The resolution at which the data should be resampled. 
            if string, it should be in the format of "# + unit (s/m/h/d)", e.g. "1s".
    '''
    try:
        import pandas as pd
    except ImportError:
        pass
    from pfund.datas.resolution import Resolution

    # standardize resolution by following pfund's standard, e.g. '1minute' -> '1m'
    if type(resolution) is not Resolution:
        resolution = Resolution(resolution)
    eresolution = repr(resolution)
        
    # 'min' means minute in pandas, please refer to https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#dateoffset-objects
    eresolution = eresolution.replace('m', 'min')
    eresolution = eresolution.replace('d', 'D')
    
    if isinstance(data, bytes):
        df = pd.read_parquet(io.BytesIO(data))
    elif isinstance(data, pd.DataFrame):
        df = data
    elif isinstance(data, pl.LazyFrame):
        df = data.collect().to_pandas()
    else:
        raise TypeError(f'Invalid data type {type(data)}, expected bytes or pd.DataFrame or pl.LazyFrame')
    
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
    
    if isinstance(data, bytes):
        return resampled_df.to_parquet(compression='zstd')
    elif isinstance(data, pd.DataFrame):
        return resampled_df
    elif isinstance(data, pl.LazyFrame):
        return pl.from_pandas(resampled_df).lazy()
    else:
        return resampled_df