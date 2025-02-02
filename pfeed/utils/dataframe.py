import pandas as pd
import polars as pl
import narwhals as nw
from pfeed.typing.core import dd, ps, SparkDataFrame


def is_dataframe(value, include_narwhals=True) -> bool:
    return (
        isinstance(value, (pd.DataFrame, pl.DataFrame, pl.LazyFrame, dd.DataFrame, ps.DataFrame, SparkDataFrame))
        or (include_narwhals and isinstance(value, (nw.DataFrame, nw.LazyFrame)))
    )


def is_empty_dataframe(df) -> bool:
    if isinstance(df, (pd.DataFrame, ps.DataFrame)):
        return df.empty
    elif isinstance(df, pl.DataFrame):
        return df.is_empty()
    elif isinstance(df, pl.LazyFrame):
        return df.limit(1).collect().is_empty()
    elif isinstance(df, dd.DataFrame):
        return len(df.index) == 0
    elif isinstance(df, SparkDataFrame):
        return df.rdd.isEmpty()
    else:
        raise ValueError(f'Unsupported dataframe type: {type(df)}')


def is_series(value, include_narwhals=True) -> bool:
    return (
        isinstance(value, (pd.Series, pl.Series, dd.Series, ps.Series))
        or (include_narwhals and isinstance(value, (nw.Series)))
    )