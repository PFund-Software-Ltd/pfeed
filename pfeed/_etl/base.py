from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfeed.typing import tDataTool
    from pfeed.typing import GenericFrame, GenericData
    

import numpy as np
import pandas as pd
import polars as pl
import narwhals as nw

from pfeed.enums import DataTool
from pfeed.utils.dataframe import is_dataframe
from pfeed.typing import dd, ps, SparkDataFrame


def standardize_date_column(df: pd.DataFrame) -> pd.DataFrame:
    from pandas.api.types import is_datetime64_any_dtype
    from pfeed.utils import determine_timestamp_integer_unit_and_scaling_factor
    if not is_datetime64_any_dtype(df['date']):
        first_date = df.loc[0, 'date']
        if isinstance(first_date, str):
            df['date'] = pd.to_datetime(df['date']).dt.tz_localize(None)
        elif isinstance(first_date, (float, int, np.floating, np.integer)):
            ts_unit, scaling_factor = determine_timestamp_integer_unit_and_scaling_factor(first_date)
            df['date'] = pd.to_datetime(df['date'] * scaling_factor, unit=ts_unit)
        else:
            raise ValueError(f'{type(first_date)=}, {first_date=}')
    df.sort_values(by='date', inplace=True)
    return df


def convert_to_pandas_df(data: GenericData) -> pd.DataFrame:
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
    elif is_dataframe(data):
        return convert_to_user_df(data, DataTool.pandas)
    else:
        raise ValueError(f'{type(data)=}')


def convert_to_user_df(df: GenericFrame, data_tool: DataTool | tDataTool) -> pd.DataFrame | pl.LazyFrame | dd.DataFrame | SparkDataFrame:
    '''Converts the input dataframe to the user's desired data tool.
    Args:
        df: The input dataframe to be converted.
        data_tool: The data tool to convert the dataframe to.
            e.g. if data_tool is 'pandas', the returned the dataframe is a pandas dataframe.
    Returns:
        The converted dataframe.
    '''

    data_tool = DataTool[data_tool.lower()]

    def _narwhalify(_df: GenericFrame) -> nw.DataFrame:
        # narwhals only supports SparkDataFrame, not ps.DataFrame, so convert to SparkDataFrame first
        if isinstance(_df, ps.DataFrame):
            _df = _df.to_spark()
        nw_df = nw.from_native(_df)
        if isinstance(nw_df, nw.LazyFrame):
            nw_df = nw_df.collect()
        return nw_df
    
    # if the input dataframe is already in the desired data tool, return it directly
    if data_tool == DataTool.pandas:
        if isinstance(df, pd.DataFrame):
            return df
        else:
            nw_df = _narwhalify(df)
            return nw_df.to_pandas()
    elif data_tool == DataTool.polars:
        if isinstance(df, (pl.LazyFrame, pl.DataFrame)):
            return df.lazy()
        else:
            nw_df = _narwhalify(df)
            return nw_df.to_polars().lazy()
    elif data_tool == DataTool.dask:
        if isinstance(df, pd.DataFrame):
            pass
        elif isinstance(df, dd.DataFrame):
            return df
        else:
            nw_df = _narwhalify(df)
            df = nw_df.to_pandas()
        return dd.from_pandas(df, npartitions=1)
    elif data_tool == DataTool.spark:
        if isinstance(df, pd.DataFrame):
            pass
        elif isinstance(df, SparkDataFrame):
            return df
        else:
            nw_df = _narwhalify(df)
            df = nw_df.to_pandas()
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        return spark.createDataFrame(df)
    else:
        raise ValueError(f'{data_tool=}')
