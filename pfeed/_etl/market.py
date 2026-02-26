'''
ETL for market data.
'''
from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    import pandas as pd
    from pfund.entities.products.product_base import BaseProduct


from pfund.datas.resolution import Resolution
from pfund.datas.timeframe import Timeframe


def standardize_columns(df: pd.DataFrame, product: BaseProduct, resolution: Resolution) -> pd.DataFrame:
    """Standardizes the columns of a DataFrame.
    Adds columns 'resolution', 'symbol'
    """
    # df['product'] = product.name
    df['resolution'] = repr(resolution)
    df['symbol'] = product.symbol
    return df


def filter_columns(df: pd.DataFrame, product: BaseProduct | None = None) -> pd.DataFrame:
    """Filter out unnecessary columns from raw data."""
    is_tick_data = 'price' in df.columns
    if is_tick_data:
        standard_cols = ['date', 'resolution', 'symbol', 'side', 'volume', 'price']
    else:
        standard_cols = ['date', 'resolution', 'symbol', 'open', 'high', 'low', 'close', 'volume']
    df_cols = df.columns
    extra_cols: list[str] = []
    if product:
        if product.is_stock() or product.is_etf():
            extra_cols.extend(['dividends', 'splits'])
    for extra_col in extra_cols:
        if extra_col in df_cols:
            standard_cols.append(extra_col)
    df = df.loc[:, standard_cols]
    return df


def organize_columns(df: pd.DataFrame) -> pd.DataFrame:
    '''Moves 'date', 'resolution', 'symbol' to the leftmost side.'''
    left_cols = ['date', 'resolution', 'symbol']
    target_cols = left_cols + [col for col in df.columns if col not in left_cols]
    
    # Only reindex if columns are not already in the target order
    if list(df.columns) != target_cols:
        df = df.reindex(target_cols, axis=1)
    
    return df
    

def resample_data(df: pd.DataFrame, resolution: str | Resolution, product: BaseProduct | None = None, offset: str | None = None) -> pd.DataFrame:
    '''Resamples the input dataframe based on the target resolution.
    Args:
        df: The input dataframe to be resampled.
        resolution: The target resolution to resample the data to.
        offset: Optional time offset string (e.g., '30min') to shift the resampling window.
               For example, with '1h' resolution and '30min' offset, timestamps will be XX:30:00.
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
    
    df = filter_columns(df, product=product)
        
    # converts to pandas's resolution format
    eresolution = {
        # 'min' means minute in pandas, please refer to https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#dateoffset-objects
        Timeframe.MINUTE: 'min',
        Timeframe.DAY: 'D',
        Timeframe.WEEK: 'W-MON',  # W = Week, starting from Monday, otherwise, default is Sunday
        Timeframe.MONTH: 'MS',  # MS = Month Start
        Timeframe.YEAR: 'YS',  # YS = Year Start
    }.get(resolution.timeframe, resolution.timeframe.canonical)
    
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
    
    for col in ['resolution', 'symbol']:
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
            offset=offset,
        )
        .agg(resample_logic)  # pyright: ignore[reportArgumentType]
        # drop an unnecessary level created by 'ohlc' in the resample_logic
        .pipe(lambda df: df.droplevel(0, axis=1) if is_tick_data else df)
        .dropna()
        .reset_index()
    )
    resampled_df['resolution'] = repr(resolution)
    return resampled_df

