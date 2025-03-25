'''
ETL for market data.
'''
from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfeed.typing import tPRODUCT_TYPE

import pandas as pd

from pfund.datas.resolution import Resolution


def standardize_columns(df: pd.DataFrame, resolution: Resolution, product_name: str, symbol: str='') -> pd.DataFrame:
    """Standardizes the columns of a DataFrame.
    Adds columns 'resolution', 'product', 'symbol', and convert 'date' to datetime
    Args:
        product_name: Full name of the product, using the 'name' attribute of the product
    """
    from pfeed._etl.base import standardize_date_column
    df['resolution'] = repr(resolution)
    df['product'] = product_name.upper()
    if symbol:
        df['symbol'] = symbol.upper()
    df = standardize_date_column(df)
    return df


def filter_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Filter out unnecessary columns from raw data."""
    is_tick_data = 'price' in df.columns
    pdt = df['product'][0]
    ptype: tPRODUCT_TYPE = pdt.split('_')[2]
    if is_tick_data:
        standard_cols = ['date', 'product', 'resolution', 'side', 'volume', 'price']
    else:
        standard_cols = ['date', 'product', 'resolution', 'open', 'high', 'low', 'close', 'volume']
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
    '''Moves 'date', 'product', 'resolution', 'symbol' to the leftmost side.'''
    left_cols = ['date', 'resolution', 'product']
    if 'symbol' in df.columns:
        left_cols.append('symbol')
    df = df.reindex(left_cols + [col for col in df.columns if col not in left_cols], axis=1)
    return df
    

def resample_data(df: pd.DataFrame, resolution: str | Resolution) -> pd.DataFrame:
    '''Resamples the input dataframe based on the target resolution.
    Args:
        df: The input dataframe to be resampled.
        resolution: The target resolution to resample the data to.
    Returns:
        The resampled dataframe.
    '''
    if isinstance(resolution, str):
        resolution = Resolution(resolution)
        
    if 'resolution' in df.columns:
        df_resolution = Resolution(df['resolution'][0])
        is_resample_required = resolution < df_resolution
        if not is_resample_required:
            return df
        
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
        .set_index('date')
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
    # resampled_df = organize_columns(resampled_df)
    return resampled_df

