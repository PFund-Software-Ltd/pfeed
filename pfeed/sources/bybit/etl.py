'''ETL = Extract, Transform, Load data'''
import io
import logging

from typing import Literal

import numpy as np
import pandas as pd

from pfeed.const.paths import DATA_PATH
from pfeed.sources.bybit.const import DATA_SOURCE, SELECTED_RAW_COLS, RENAMING_COLS, RAW_DATA_TIMESTAMP_UNITS
from pfeed.datastore import Datastore, check_if_minio_running, check_if_minio_access_key_and_secret_key_provided
from pfeed.filepath import FilePath


logger = logging.getLogger(DATA_SOURCE.lower())


def extract_data(
    pdt: str,
    date: str,
    dtype: Literal['raw', 'tick', 'second', 'minute', 'hour', 'daily'],
    mode: Literal['historical', 'streaming']='historical',
    data_path: str=str(DATA_PATH),
) -> bytes | None:
    file_extension = '.csv.gz' if dtype == 'raw' else '.parquet.gz'
    fp = FilePath(DATA_SOURCE, mode, dtype, pdt, date, data_path=data_path, file_extension=file_extension)
    if fp.exists():
        with open(fp.file_path, 'rb') as f:
            data: bytes = f.read()
            logger.debug(f'read data from {fp.file_path}')
            return data
    else:
        # print(f'failed to find {fp.file_path}, trying to extract data from MinIO')
        if check_if_minio_running() and check_if_minio_access_key_and_secret_key_provided():
            datastore = Datastore()
            object_name = fp.storage_path
            data: bytes | None = datastore.get_object(object_name)
            if data:
                logger.debug(f'extracted data from MinIO object {object_name}')
            else:
                logger.warning(f'failed to extract data from MinIO object {object_name}')
            return data
        else:
            return None


def load_data(
    mode: Literal['historical', 'streaming'],
    dtype: Literal['raw', 'tick', 'second', 'minute', 'hour', 'daily'],
    pdt: str,
    date: str,
    data: bytes,
    data_path: str=str(DATA_PATH),
    use_minio=True,
    **kwargs
):  
    file_extension = '.csv.gz' if dtype == 'raw' else '.parquet.gz'
    fp = FilePath(DATA_SOURCE, mode, dtype, pdt, date, data_path=data_path, file_extension=file_extension)
    if use_minio:
        datastore = Datastore()
        object_name = fp.storage_path
        datastore.put_object(object_name, data, **kwargs)
        logger.debug(f'loaded data to object {object_name} {kwargs=}')
    else:
        fp.parent.mkdir(parents=True, exist_ok=True)
        with open(fp.file_path, 'wb') as f:
            f.write(data)
            logger.debug(f'loaded data to {fp.file_path}')


def clean_data(ptype: str, data: bytes) -> bytes:
    df = pd.read_csv(io.BytesIO(data), compression='gzip')
    df = df.loc[:, SELECTED_RAW_COLS[ptype]]
    df['side'] = df['side'].map({'Buy': 1, 'Sell': -1})
    df = df.rename(columns=RENAMING_COLS[ptype])
    df.set_index('ts', inplace=True)
    # NOTE: this may make the `ts` value inaccurate, 
    # e.g. 1671580800.9906 -> 1671580800.990600192
    df.index = pd.to_datetime(df.index, unit=RAW_DATA_TIMESTAMP_UNITS[ptype])
    return df.to_parquet()


def resample_data(data: bytes | pd.DataFrame, resolution: str, is_tick=False, to_parquet=True) -> bytes:
    '''
    Args:
        is_tick: if True, use tick data to resample data
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
        
    # EXTEND:
    # 'min' means minute in pandas, please refer to https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#dateoffset-objects
    if 'm' in resolution:
        resolution = resolution.replace('m', 'min')
    elif 'd' in resolution:
        resolution = resolution.replace('d', 'D')
    
    if type(data) is bytes:
        df = pd.read_parquet(io.BytesIO(data))
    elif type(data) is pd.DataFrame:
        df = data
    else:
        raise TypeError(f'invalid data type {type(data)}')
    
    assert not df.empty, 'data is empty'
    
    if is_tick:
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
        .resample(resolution)
        .apply(resample_logic)
        .dropna(subset=[col for col in df.columns if col != 'first'])
    )
    
    if is_tick:
        resampled_df = resampled_df.droplevel(0, axis=1)
        # convert float to int
        resampled_df['num_buys'] = resampled_df['num_buys'].astype(int)
        resampled_df['num_sells'] = resampled_df['num_sells'].astype(int)
    else:
        resampled_df['first'] = resampled_df.apply(determine_first, axis=1)
        resampled_df.drop(columns=['arg_high', 'arg_low'], inplace=True)
    
    if to_parquet:
        return resampled_df.to_parquet()
    else:
        return resampled_df


if __name__ == '__main__':
    pdt = 'BTC_USDT_PERP'
    date = '2024-02-14'
    dtype = 'daily'
    data: bytes = extract_data(pdt, date, dtype)
    df = resample_data(data, '1d', is_tick=(dtype == 'tick'), to_parquet=False)
    print(df)