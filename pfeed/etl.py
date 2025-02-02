from __future__ import annotations
from typing import TYPE_CHECKING
from types import ModuleType
if TYPE_CHECKING:
    from narwhals.typing import IntoFrame, Frame
    from pfeed.typing.literals import tPRODUCT_TYPE, tDATA_TOOL
    from pfeed.typing.core import tDataFrame, tData

import importlib

import pandas as pd
import polars as pl
import narwhals as nw

from pfund.datas.resolution import Resolution
from pfeed.typing.core import dd
from pfeed.const.enums import DataTool
from pfeed.utils.dataframe import is_dataframe


def get_data_tool(data_tool: DataTool | tDATA_TOOL) -> ModuleType:
    dtl = DataTool[data_tool.lower()] if isinstance(data_tool, str) else data_tool
    return importlib.import_module(f'pfeed.data_tools.data_tool_{dtl}')


def standardize_columns(df: pd.DataFrame, resolution: Resolution, product_name: str, symbol: str='') -> pd.DataFrame:
    """Standardizes the columns of a DataFrame.
    Adds columns 'resolution', 'product', 'symbol', and convert 'ts' to datetime
    Args:
        product_name: Full name of the product, using the 'name' attribute of the product
    """
    from pandas.api.types import is_datetime64_any_dtype as is_datetime
    from pfeed.utils.utils import determine_timestamp_integer_unit_and_scaling_factor
    df['resolution'] = repr(resolution)
    df['product'] = product_name.upper()
    if symbol:
        df['symbol'] = symbol.upper()
    if not is_datetime(df['ts']):
        first_ts = df.loc[0, 'ts']
        ts_unit, scaling_factor = determine_timestamp_integer_unit_and_scaling_factor(first_ts)
        df['ts'] = pd.to_datetime(df['ts'] * scaling_factor, unit=ts_unit)
    return df


def filter_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Filter out unnecessary columns from raw data."""
    is_tick_data = 'price' in df.columns
    pdt = df['product'][0]
    ptype: tPRODUCT_TYPE = pdt.split('_')[2]
    if is_tick_data:
        standard_cols = ['ts', 'product', 'resolution', 'side', 'volume', 'price']
    else:
        standard_cols = ['ts', 'product', 'resolution', 'open', 'high', 'low', 'close', 'volume']
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
    '''Moves 'ts', 'product', 'resolution', 'symbol' to the leftmost side.'''
    left_cols = ['ts', 'resolution', 'product']
    if 'symbol' in df.columns:
        left_cols.append('symbol')
    df = df.reindex(left_cols + [col for col in df.columns if col not in left_cols], axis=1)
    return df
    

def resample_data(df: IntoFrame, resolution: str | Resolution) -> pd.DataFrame:
    '''Resamples the input dataframe based on the target resolution.
    Args:
        df: The input dataframe to be resampled.
        resolution: The target resolution to resample the data to.
    Returns:
        The resampled dataframe.
    '''
    df = convert_to_pandas_df(df)
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
        .set_index('ts')
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


def convert_to_pandas_df(data: tData) -> pd.DataFrame:
    import io
    from pfeed.utils.file_formats import decompress_data, is_parquet, is_likely_csv
    if isinstance(data, bytes):
        data = decompress_data(data)
        if is_parquet(data):
            return pd.read_parquet(io.BytesIO(data))
        elif is_likely_csv(data):
            return pd.read_csv(io.BytesIO(data))
        else:
            raise ValueError("Unknown or unsupported format")
    elif isinstance(data, pd.DataFrame):
        return data
    elif is_dataframe(data):
        df: Frame = nw.from_native(data)
        if isinstance(df, nw.LazyFrame):
            df = df.collect()
        return df.to_pandas()
    else:
        raise ValueError(f'{type(data)=}')


def convert_to_user_df(df: tDataFrame, data_tool: DataTool | tDATA_TOOL) -> tDataFrame:
    '''Converts the input dataframe to the user's desired data tool.
    Args:
        df: The input dataframe to be converted.
        data_tool: The data tool to convert the dataframe to.
            e.g. if data_tool is 'pandas', the returned the dataframe is a pandas dataframe.
    Returns:
        The converted dataframe.
    '''
    data_tool = data_tool.lower()
    # if the input dataframe is already in the desired data tool, return it directly
    if isinstance(df, pd.DataFrame) and data_tool == DataTool.pandas:
        return df
    elif isinstance(df, (pl.DataFrame, pl.LazyFrame)) and data_tool == DataTool.polars:
        return df.lazy() if isinstance(df, pl.DataFrame) else df
    elif isinstance(df, dd.DataFrame) and data_tool == DataTool.dask:
        return df

    nw_df = nw.from_native(df)
    if data_tool == DataTool.pandas:
        return nw_df.to_pandas()
    elif data_tool == DataTool.polars:
        return nw_df.to_polars().lazy()
    elif data_tool == DataTool.dask:
        return dd.from_pandas(nw_df.to_pandas(), npartitions=1)
    # elif data_tool == DataTool.spark:
    #     spark = SparkSession.builder.getOrCreate()
    #     return spark.createDataFrame(df)
    else:
        raise ValueError(f'{data_tool=}')
