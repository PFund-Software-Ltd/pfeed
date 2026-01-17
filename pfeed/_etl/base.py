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


def convert_to_desired_df(data: GenericData, data_tool: DataTool | tDataTool) -> pd.DataFrame | pl.LazyFrame | dd.DataFrame | SparkDataFrame:
    '''Converts the input data to the user's desired data tool.
    Args:
        data: The input data to be converted.
        data_tool: The data tool to convert the dataframe to.
            e.g. if data_tool is 'pandas', the returned the dataframe is a pandas dataframe.
    Returns:
        The converted dataframe.
    '''
    import io
    from pfeed.enums.compression import Compression
    from pfeed.enums.io_format import IOFormat

    if isinstance(data, bytes):
        data = Compression.decompress(data)
        io_format = IOFormat.detect(data)
        if io_format == IOFormat.PARQUET:
            df = pd.read_parquet(io.BytesIO(data))
        elif io_format == IOFormat.CSV:
            df = pd.read_csv(io.BytesIO(data))
        else:
            raise ValueError(f'{io_format=}')
    elif is_dataframe(data):
        df: GenericFrame = data
    else:
        raise ValueError(f'{type(data)=}')

    data_tool = DataTool[data_tool.lower()]

    def _narwhalify(_df: GenericFrame) -> nw.DataFrame:
        # narwhals only supports SparkDataFrame, not ps.DataFrame, so convert to SparkDataFrame first
        if isinstance(_df, ps.DataFrame):
            _df = _df.to_spark()
        nwdf = nw.from_native(_df)
        if isinstance(nwdf, nw.LazyFrame):
            nwdf = nwdf.collect()
        return nwdf
    
    # if the input dataframe is already in the desired data tool, return it directly
    if data_tool == DataTool.pandas:
        if isinstance(df, pd.DataFrame):
            return df
        else:
            nwdf = _narwhalify(df)
            return nwdf.to_pandas()
    elif data_tool == DataTool.polars:
        if isinstance(df, (pl.LazyFrame, pl.DataFrame)):
            return df.lazy()
        else:
            nwdf = _narwhalify(df)
            return nwdf.to_polars().lazy()
    elif data_tool == DataTool.dask:
        if isinstance(df, pd.DataFrame):
            pass
        elif isinstance(df, dd.DataFrame):
            return df
        else:
            nwdf = _narwhalify(df)
            df = nwdf.to_pandas()
        return dd.from_pandas(df, npartitions=1)
    elif data_tool == DataTool.spark:
        if isinstance(df, pd.DataFrame):
            pass
        elif isinstance(df, SparkDataFrame):
            return df
        else:
            nwdf = _narwhalify(df)
            df = nwdf.to_pandas()
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        return spark.createDataFrame(df)
    else:
        raise ValueError(f'{data_tool=}')
