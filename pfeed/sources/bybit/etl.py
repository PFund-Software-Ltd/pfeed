'''ETL = Extract, Transform, Load data'''
import os
import io
import logging

from typing import Literal

import numpy as np
import pandas as pd

from pfeed.const.paths import DATA_PATH
from pfeed.sources.bybit.const import DATA_SOURCE, SELECTED_RAW_COLS, RENAMING_COLS, RAW_DATA_TIMESTAMP_UNITS
from pfeed.datastore import Datastore
from pfeed.filepath import FilePath


logger = logging.getLogger(DATA_SOURCE.lower())


def extract_data(
    pdt: str,
    date: str,
    dtype: Literal['raw', 'tick', 'second', 'minute', 'hour', 'daily'],
    env: Literal['PAPER', 'LIVE']='LIVE',
    mode: Literal['historical', 'streaming']='historical',
    data_path: str=str(DATA_PATH),
) -> bytes:
    file_extension = '.csv.gz' if dtype == 'raw' else '.parquet.gz'
    fp = FilePath(data_path, env, DATA_SOURCE, dtype, mode, pdt, date, file_extension)
    if os.path.exists(fp.file_path):
        with open(fp.file_path, 'rb') as f:
            data: bytes = f.read()
            logger.debug(f'read data from {fp.file_path}')
            return data
    else:
        datastore = Datastore()
        object_name = fp.storage_path
        data: bytes | None = datastore.get_object(object_name)
        if data:
            logger.debug(f'extracted data from MinIO object {object_name}')
        else:
            logger.error(f'failed to extract data from MinIO object {object_name}')
        return data


def load_data(
    pdt: str,
    date: str,
    dtype: Literal['raw', 'tick', 'second', 'minute', 'hour', 'daily'],
    data: bytes,
    env: Literal['PAPER', 'LIVE']='LIVE',
    mode: Literal['historical', 'streaming']='historical',
    data_path: str=str(DATA_PATH),
    use_minio=True,
    **kwargs
):  
    if not os.path.exists(data_path):
        os.makedirs(data_path)
        print(f'created {data_path=}')
    file_extension = '.csv.gz' if dtype == 'raw' else '.parquet.gz'
    fp = FilePath(data_path, env, DATA_SOURCE, dtype, mode, pdt, date, file_extension)
    if use_minio:
        datastore = Datastore()
        object_name = fp.storage_path
        datastore.put_object(object_name, data, **kwargs)
        logger.debug(f'loaded data to object {object_name} {kwargs=}')
    else:
        directory = os.path.dirname(fp.file_path)
        if not os.path.exists(directory):
            os.makedirs(directory)
        with open(fp.file_path, 'wb') as f:
            f.write(data)
            logger.debug(f'loaded data to {fp.file_path}')


def clean_data(category: str, data: bytes) -> bytes:
    df = pd.read_csv(io.BytesIO(data), compression='gzip')
    df = df.loc[:, SELECTED_RAW_COLS[category]]
    df['side'] = df['side'].map({'Buy': 1, 'Sell': -1})
    df = df.rename(columns=RENAMING_COLS[category])
    return df.to_parquet()


def resample_data(data: bytes, resolution: str, is_tick=False, category='') -> bytes:
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
            else:
                return 'N'
    # T means minute, refer to https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#dateoffset-objects
    resolution = resolution.upper().replace('M', 'T')
    df = pd.read_parquet(io.BytesIO(data))
    if 'ts' in df.columns:
        assert category, 'category must be provided'
        df.set_index('ts', inplace=True)
        # NOTE: this may make the `ts` value inaccurate, 
        # e.g. 1671580800.9906 -> 1671580800.990600192
        df.index = pd.to_datetime(df.index, unit=RAW_DATA_TIMESTAMP_UNITS[category])
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
    resampled_df = (
        df
        .resample(resolution)
        .apply(resample_logic)
        .dropna()
    )
    if is_tick:
        resampled_df = resampled_df.droplevel(0, axis=1)
        # convert float to int
        resampled_df['num_buys'] = resampled_df['num_buys'].astype(int)
        resampled_df['num_sells'] = resampled_df['num_sells'].astype(int)
    else:
        resampled_df['first'] = (resampled_df['arg_high'] < resampled_df['arg_low']).map({True: 'H', False: 'L'})
        resampled_df['first'].where(resampled_df['arg_high'] != resampled_df['arg_low'], other='N', inplace=True)
        resampled_df.drop(columns=['arg_high', 'arg_low'], inplace=True)
    return resampled_df.to_parquet()


if __name__ == '__main__':
    pdt = 'BTC_USDT_PERP'
    date = '2023-11-02'
    dtype = 'raw'
    data: bytes = extract_data(pdt, date, dtype)