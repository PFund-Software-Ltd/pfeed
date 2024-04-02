'''ETL = Extract, Transform, Load data'''
import io
import logging
import importlib

from typing import Literal

import pandas as pd
from minio.error import MinioException

from pfeed.datastore import Datastore
from pfeed.filepath import FilePath
from pfeed.config_handler import ConfigHandler
from pfeed.const.commons import SUPPORTED_DATA_TYPES, SUPPORTED_DATA_SINKS, SUPPORTED_DOWNLOAD_DATA_SOURCES, SUPPORTED_DATA_MODES
from pfeed.utils.monitor import print_disk_usage
from pfund.datas.resolution import Resolution


DataSink = Literal['local', 'minio']
DataSource = Literal['BYBIT']
DataTool = Literal['pandas', 'polars', 'pyspark']
DataType = Literal['raw_tick', 'raw_second', 'raw_minute', 'raw_hour', 'raw_daily', 'raw', 'tick', 'second', 'minute', 'hour', 'daily']
DataMode = Literal['historical', 'streaming']


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
    data_source: DataSource,
    dtype: DataType,
    pdt: str,
    date: str,
    mode: DataMode='historical',
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
    for data_sink in SUPPORTED_DATA_SINKS:
        try:
            data: bytes = extract_data(data_sink, data_source, dtype, pdt, date, mode=mode)
        except MinioException:
            data = None
        if data:
            return data
    else:
        logger.info(f'{data_source} {pdt} {date} {dtype} data is nowhere to be found, {SUPPORTED_DATA_SINKS=}')


def extract_data(
    data_sink: DataSink,
    data_source: DataSource,
    dtype: DataType,
    pdt: str,
    date: str,
    mode: DataMode='historical',
) -> bytes | None:
    """
    Extracts data from a specified data source and returns it as bytes.

    Args:
        data_sink: The origin of the data (local or minio).
        data_source: The source of the data.
        dtype: The type of data to extract.
        pdt (str): product, e.g. BTC_USDT_PERP.
        date (str): The date of the data.
        mode (optional): The mode of extraction. Defaults to 'historical'.

    Returns:
        bytes | None: The extracted data as bytes, or None if extraction fails.

    Raises:
        AssertionError: If any of the input parameters are invalid.
        NotImplementedError: If the data origin is not supported.
        MinioException: If MinIO is not running / set up correctly.
    """
    data_sink, data_source, dtype, pdt, mode = data_sink.lower(), data_source.upper(), dtype.lower(), pdt.upper(), mode.lower()
    assert data_sink in SUPPORTED_DATA_SINKS, f'Invalid {data_sink=}, {SUPPORTED_DATA_SINKS=}'
    assert data_source in SUPPORTED_DOWNLOAD_DATA_SOURCES, f'Invalid {data_source=}, SUPPORTED DATA SOURCES={SUPPORTED_DOWNLOAD_DATA_SOURCES}'
    if dtype == 'raw':
        dtype = _convert_raw_dtype_to_explicit(data_source, dtype)
    assert dtype in SUPPORTED_DATA_TYPES, f'Invalid {dtype=}, {SUPPORTED_DATA_TYPES=}'
    assert mode in SUPPORTED_DATA_MODES, f'Invalid {mode=}, {SUPPORTED_DATA_MODES=}'
     
    config = ConfigHandler.load_config()
    fp = FilePath(data_source, mode, dtype, pdt, date, data_path=config.data_path, file_extension='.parquet')
    if data_sink == 'local':
        if fp.exists():
            with open(fp.file_path, 'rb') as f:
                data: bytes = f.read()
            logger.debug(f'extracted {data_source} data from local path {fp.storage_path}')
            return data
        else:
            logger.debug(f'failed to extract {data_source} data from local path {fp.storage_path}')
    elif data_sink == 'minio':
        datastore = Datastore()
        object_name = fp.storage_path
        data: bytes | None = datastore.get_object(object_name)
        if data:
            logger.debug(f'extracted {data_source} data from MinIO object {object_name}')
        else:
            logger.debug(f'failed to extract {data_source} data from MinIO object {object_name}')
        return data
    else:
        raise NotImplementedError(f'{data_sink=}')


def load_data(
    data_sink: DataSink,
    data_source: DataSource,
    data: bytes,
    dtype: DataType,
    pdt: str,
    date: str,
    mode: DataMode = 'historical',
    **kwargs
) -> None:
    """
    Loads data into the specified data destination.

    Args:
        
        data_sink: The destination where the data will be loaded. 
            It can be either 'local' or 'minio'.
        data_source: The source of the data.
        data (bytes): The data to be loaded.
        dtype: The type of the data.
        pdt (str): product, e.g. BTC_USDT_PERP.
        date (str): The date of the data.
        mode (optional): The mode of loading the data. 
            Defaults to 'historical'.
        **kwargs: Additional keyword arguments for MinIO.

    Returns:
        None
        
    Raises:
        AssertionError: If any of the input parameters are invalid.
        NotImplementedError: If the specified data destination is not implemented.
        MinioException: If MinIO is not running / set up correctly.
    """
    data_sink, data_source, dtype, pdt, mode = data_sink.lower(), data_source.upper(), dtype.lower(), pdt.upper(), mode.lower()
    assert data_sink in SUPPORTED_DATA_SINKS, f'Invalid {data_sink=}, {SUPPORTED_DATA_SINKS=}'
    assert data_source in SUPPORTED_DOWNLOAD_DATA_SOURCES, f'Invalid {data_source=}, SUPPORTED DATA SOURCES={SUPPORTED_DOWNLOAD_DATA_SOURCES}'
    if dtype == 'raw':
        dtype = _convert_raw_dtype_to_explicit(data_source, dtype)
    assert dtype in SUPPORTED_DATA_TYPES, f'Invalid {dtype=}, {SUPPORTED_DATA_TYPES=}'
    assert mode in SUPPORTED_DATA_MODES, f'Invalid {mode=}, {SUPPORTED_DATA_MODES=}'
    
    config = ConfigHandler.load_config()
    fp = FilePath(data_source, mode, dtype, pdt, date, data_path=config.data_path, file_extension='.parquet')
    if data_sink == 'local':
        fp.parent.mkdir(parents=True, exist_ok=True)
        with open(fp.file_path, 'wb') as f:
            f.write(data)
            logger.info(f'loaded {data_source} data to {fp.storage_path}')
    elif data_sink == 'minio':
        datastore = Datastore()
        object_name = fp.storage_path
        datastore.put_object(object_name, data, **kwargs)
        logger.info(f'loaded {data_source} data to MinIO object {object_name} {kwargs=}')
    else:
        raise NotImplementedError(f'{data_sink=}')
    print_disk_usage(config.data_path)
        

def clean_raw_data(data_source: DataSource, raw_data: bytes) -> bytes:
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


def resample_data(data: bytes, resolution: str | Resolution, data_tool: DataTool='polars', check_if_drop_last_bar=False) -> bytes:
    data_tool = importlib.import_module(f'pfeed.data_tools.data_tool_{data_tool.lower()}')
    return data_tool.resample_data(data, resolution, check_if_drop_last_bar=check_if_drop_last_bar)
