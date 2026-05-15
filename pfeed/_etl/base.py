# pyright: reportReturnType=false, reportUnknownMemberType=false, reportUnknownVariableType=false
from __future__ import annotations
from typing import TYPE_CHECKING, Any
if TYPE_CHECKING:
    from narwhals.typing import IntoFrame

import polars as pl
import narwhals as nw

from pfeed.enums import DataTool


def standardize_date_column(df: pl.LazyFrame, date_col: str) -> pl.LazyFrame:
    from pfeed.utils.temporal import infer_ts_unit

    date_dtype = df.collect_schema()[date_col]

    if date_dtype.is_temporal():
        return df.sort(date_col)

    if date_dtype == pl.String:
        return df.with_columns(
            pl.col(date_col).str.to_datetime().dt.replace_time_zone(None)
        ).sort(date_col)

    if date_dtype.is_numeric():
        first_date = df.select(pl.col(date_col).first()).collect().item()
        ts_unit = infer_ts_unit(first_date)
        return df.with_columns(
            pl.from_epoch(pl.col(date_col), time_unit=ts_unit).alias(date_col)
        ).sort(date_col)

    raise ValueError(f'{date_dtype=}')


def convert_dataframe(df: Any, data_tool: DataTool | str | None = None) -> IntoFrame:
    '''Convert `df` to the native dataframe type of `data_tool`.

    Polars output is always returned as a LazyFrame; dask/spark/daft return their
    native lazy type. Only pandas returns eager.
    '''
    from pfeed.config import get_config
    from pfeed.utils.dataframe import is_dataframe, from_native
    if not is_dataframe(df):
        raise ValueError(f'{type(df)=}')
    config = get_config()
    data_tool = data_tool or config.data_tool
    data_tool = DataTool[data_tool.lower()]

    def _to_pandas():
        nwdf = from_native(df)
        if isinstance(nwdf, nw.LazyFrame):
            nwdf = nwdf.collect()
        return nwdf.to_pandas()

    if data_tool == DataTool.pandas:
        import pandas as pd
        return df if isinstance(df, pd.DataFrame) else _to_pandas()
    elif data_tool == DataTool.polars:
        if isinstance(df, pl.LazyFrame):
            return df
        elif isinstance(df, pl.DataFrame):
            return df.lazy()
        nwdf = from_native(df)
        if isinstance(nwdf, nw.LazyFrame):
            nwdf = nwdf.collect()
        return nwdf.to_polars().lazy()
    elif data_tool == DataTool.dask:
        import dask.dataframe as dd
        if isinstance(df, dd.DataFrame):
            return df
        return dd.from_pandas(_to_pandas(), npartitions=1)
    elif data_tool == DataTool.spark:
        from pyspark.sql import DataFrame as SparkDataFrame, SparkSession
        if isinstance(df, SparkDataFrame):
            return df
        spark = SparkSession.builder.getOrCreate()
        return spark.createDataFrame(_to_pandas())
    elif data_tool == DataTool.daft:
        import daft
        if isinstance(df, daft.DataFrame):
            return df
        return daft.from_pandas(_to_pandas())
    else:
        raise ValueError(f'{data_tool=}')
