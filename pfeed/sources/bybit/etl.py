'''ETL = Extract, Transform, Load data'''
import io
import logging

from typing import Literal

import numpy as np
import pandas as pd
from minio.error import MinioException

from pfeed.sources.bybit.const import DATA_SOURCE, SELECTED_COLS, RENAMING_COLS, SUPPORTED_RAW_DATA_TYPES
from pfeed.datastore import Datastore
from pfeed.filepath import FilePath
from pfeed.config_handler import ConfigHandler
from pfeed.const.commons import SUPPORTED_DATA_TYPES, SUPPORTED_DATA_STORAGES


logger = logging.getLogger(DATA_SOURCE.lower())


def get_data(
    dtype: Literal['raw_tick', 'raw', 'tick', 'second', 'minute', 'hour', 'daily'],
    pdt: str,
    date: str,
    mode: Literal['historical', 'streaming']='historical',
) -> bytes | None:
    """Extract data without specifying the data origin. 
    This function will try to extract data from all supported data origins.
    """
    for data_origin in SUPPORTED_DATA_STORAGES:
        try:
            data: bytes = extract_data(data_origin, dtype, pdt, date, mode=mode)
        except MinioException:
            data = None
        if data:
            return data
    else:
        logger.warning(f'{pdt} {date} {dtype} data is nowhere to be found, {SUPPORTED_DATA_STORAGES=}')


def extract_data(
    data_origin: Literal['local', 'minio', 'timescaledb'],
    dtype: Literal['raw_tick', 'raw', 'tick', 'second', 'minute', 'hour', 'daily'],
    pdt: str,
    date: str,
    mode: Literal['historical', 'streaming']='historical',
) -> bytes | None:
    assert data_origin in SUPPORTED_DATA_STORAGES, f'Invalid {data_origin=}, {SUPPORTED_DATA_STORAGES=}'
    # convert 'raw' to 'raw_tick'
    if dtype == 'raw':
        dtype = SUPPORTED_RAW_DATA_TYPES[0]
    assert dtype in SUPPORTED_DATA_TYPES, f'Invalid {dtype=}, {SUPPORTED_DATA_TYPES=}'
    assert mode in ['historical', 'streaming'], f'Invalid {mode=}'
     
    config = ConfigHandler.load_config()
    fp = FilePath(DATA_SOURCE, mode, dtype, pdt, date, data_path=config.data_path, file_extension='.parquet')
    if data_origin == 'local':
        if fp.exists():
            with open(fp.file_path, 'rb') as f:
                data: bytes = f.read()
            logger.debug(f'extracted data from {fp.storage_path}')
            return data
        else:
            logger.debug(f'failed to extract data from local path: {fp.storage_path}')
    elif data_origin == 'minio':
        datastore = Datastore()
        object_name = fp.storage_path
        data: bytes | None = datastore.get_object(object_name)
        if data:
            logger.debug(f'extracted data from MinIO object {object_name}')
        else:
            logger.debug(f'failed to extract data from MinIO object {object_name}')
        # TODO: convert to df
        # ...
        return data
    # TODO
    elif data_origin == 'timescaledb':
        pass
    else:
        raise NotImplementedError(f'{data_origin=}')


def load_data(
    data_destination: Literal['local', 'minio', 'timescaledb'],
    data: bytes,
    dtype: Literal['raw_tick', 'raw', 'tick', 'second', 'minute', 'hour', 'daily'],
    pdt: str,
    date: str,
    mode: Literal['historical', 'streaming'] = 'historical',
    **kwargs
):  
    assert data_destination in SUPPORTED_DATA_STORAGES, f'Invalid {data_destination=}, {SUPPORTED_DATA_STORAGES=}'
    # convert 'raw' to 'raw_tick'
    if dtype == 'raw':
        dtype = SUPPORTED_RAW_DATA_TYPES[0]  
    assert dtype in SUPPORTED_DATA_TYPES, f'Invalid {dtype=}, {SUPPORTED_DATA_TYPES=}'
    assert mode in ['historical', 'streaming'], f'Invalid {mode=}'
    
    config = ConfigHandler.load_config()
    fp = FilePath(DATA_SOURCE, mode, dtype, pdt, date, data_path=config.data_path, file_extension='.parquet')
    if data_destination == 'local':
        fp.parent.mkdir(parents=True, exist_ok=True)
        with open(fp.file_path, 'wb') as f:
            f.write(data)
            logger.debug(f'loaded data to {fp.storage_path}')
    elif data_destination == 'minio':
        datastore = Datastore()
        object_name = fp.storage_path
        datastore.put_object(object_name, data, **kwargs)
        logger.debug(f'loaded data to MinIO object {object_name} {kwargs=}')
    # TODO
    elif data_destination == 'timescaledb':
        pass
    else:
        raise NotImplementedError(f'{data_destination=}')
        

def clean_raw_data(raw_data: bytes) -> bytes:
    """Convert raw data to internal data format, e.g. renaming, setting index to datetime etc."""
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
    """filter out unnecessary columns"""
    df = pd.read_parquet(io.BytesIO(raw_tick))
    df = df.loc[:, SELECTED_COLS]
    tick_data: bytes = df.to_parquet(compression='snappy')
    return tick_data


def resample_data(data: bytes, resolution: str, only_ohlcv=False) -> bytes:
    '''
    Args:
        resolution: # + unit (s/m/h/d), e.g. 1s
    '''
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
