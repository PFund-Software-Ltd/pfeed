"""This module contains functions for data manipulation using Pandas."""
import io
import datetime
import logging

import numpy as np
import pandas as pd

from pfund.datas.resolution import Resolution


logger = logging.getLogger('pfeed')


# TODO: separate features from OHLCV
def resample_data(data: bytes, resolution: str | Resolution, check_if_drop_last_bar: bool=False) -> bytes:
    '''
    Resamples the input data based on the specified resolution and returns the resampled data in Parquet format.
    
    Args:
        data (bytes): The input data to be resampled.
        resolution (str): The resolution at which the data should be resampled. It should be in the format of "# + unit (s/m/h/d)", e.g. "1s".
        check_if_drop_last_bar: Whether to check if the last bar is complete. Defaults to False.
    
    Returns:
        bytes: The resampled data in Parquet format.
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
    
    # standardize resolution by following pfund's standard, e.g. '1minute' -> '1m'
    if type(resolution) is not Resolution:
        resolution = Resolution(resolution)
    eresolution = repr(resolution)
        
    # 'min' means minute in pandas, please refer to https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#dateoffset-objects
    eresolution = eresolution.replace('m', 'min')
    eresolution = eresolution.replace('d', 'D')
    
    if type(data) is bytes:
        df = pd.read_parquet(io.BytesIO(data))
    else:
        raise TypeError(f'Invalid data type {type(data)}, expected bytes')
    
    is_tick_data = True if 'price' in df.columns else False  # REVIEW
    assert not df.empty, 'data is empty'
    df.set_index('ts', inplace=True)
    
    if is_tick_data:
        # TEMP
        # df['num_buys'] = df['side'].map({1: 1, -1: np.nan})
        # df['num_sells'] = df['side'].map({1: np.nan, -1: 1})
        # df['buy_volume'] = np.where(df['side'] == 1, df['volume'], np.nan)
        # df['sell_volume'] = np.where(df['side'] == -1, df['volume'], np.nan)
        # df['first'] = df['price']
        resample_logic = {
            # TEMP
            # 'num_buys': 'sum',
            # 'num_sells': 'sum',
            # 'buy_volume': 'sum',
            # 'sell_volume': 'sum',
            # 'first': find_high_or_low_first
            'price': 'ohlc',
            'volume': 'sum',
        }
    else:
        # TEMP
        # df['arg_high'] = df['high']
        # df['arg_low'] = df['low']
        resample_logic = {
            # TEMP
            # 'num_buys': 'sum',
            # 'num_sells': 'sum',
            # 'buy_volume': 'sum',
            # 'sell_volume': 'sum',
            # 'arg_high': lambda series: series.argmax() if not series.empty else None,
            # 'arg_low': lambda series: series.argmin() if not series.empty else None,
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum',
        }
        # TEMP
        # if 'first' in df.columns:
        #     resample_logic['first'] = 'first'
            
    resampled_df = (
        df
        .resample(eresolution)
        .apply(resample_logic)
    )
    
    if is_tick_data:
        resampled_df = resampled_df.droplevel(0, axis=1)
    # TEMP
    #     # convert float to int
    #     resampled_df['num_buys'] = resampled_df['num_buys'].astype(int)
    #     resampled_df['num_sells'] = resampled_df['num_sells'].astype(int)
    # else:
    #     resampled_df['first'] = resampled_df.apply(determine_first, axis=1)
    #     resampled_df.drop(columns=['arg_high', 'arg_low'], inplace=True)

    resampled_df.dropna(subset=[col for col in resampled_df.columns if col != 'first'], inplace=True)
    
    # REVIEW
    if check_if_drop_last_bar:
        correct_timedelta = datetime.timedelta(seconds=resolution._value())
        final_timedelta = df.index[-1] - resampled_df.index[-1] + datetime.timedelta(seconds=resolution.timeframe.unit)
        is_drop_last_bar = (final_timedelta - correct_timedelta).total_seconds() != 0
        if is_drop_last_bar:
            logger.info(f'dropped the incomplete last bar {resampled_df.index[-1]} in the resampled data where {resolution=}') 
            resampled_df = resampled_df.iloc[:-1]
    
    resampled_df.reset_index(inplace=True)
    
    return resampled_df.to_parquet(compression='snappy')