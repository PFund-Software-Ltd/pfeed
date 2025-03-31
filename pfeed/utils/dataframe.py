from narwhals.typing import IntoFrame, Frame
from pfeed.typing import dd, ps, SparkDataFrame

import pandas as pd
import polars as pl
import narwhals as nw


def is_dataframe(df: IntoFrame, include_narwhals=True) -> bool:
    return (
        isinstance(df, (pd.DataFrame, pl.DataFrame, pl.LazyFrame, dd.DataFrame, ps.DataFrame, SparkDataFrame))
        or (include_narwhals and isinstance(df, (nw.DataFrame, nw.LazyFrame)))
    )


def is_lazyframe(df: IntoFrame, include_narwhals=True) -> bool:
    return (
        isinstance(df, (pl.LazyFrame, dd.DataFrame, SparkDataFrame))
        or (include_narwhals and isinstance(df, (nw.LazyFrame)))
    )


def is_empty_dataframe(df: IntoFrame) -> bool:
    df: Frame = nw.from_native(df)
    if isinstance(df, nw.LazyFrame):
        df = df.head(1).collect()
    return df.is_empty()


def is_series(value, include_narwhals=True) -> bool:
    return (
        isinstance(value, (pd.Series, pl.Series, dd.Series, ps.Series))
        or (include_narwhals and isinstance(value, (nw.Series)))
    )