from narwhals.typing import IntoFrame, Frame
from pfeed.typing import dd, ps, SparkDataFrame

import pandas as pd
import polars as pl
import narwhals as nw


def is_dataframe(df: IntoFrame, eagerframe_only=False, include_narwhals=True) -> bool:
    """
    Returns True if `df` is any known DataFrame type.
    
    Args:
        eagerframe_only: If True, only eager dataframes (e.g., pd.DataFrame, pl.DataFrame) are considered.
        include_narwhals: Whether to include Narwhals' frame types in the check.
    """
    df_types = (pd.DataFrame, pl.DataFrame, pl.LazyFrame, dd.DataFrame, ps.DataFrame, SparkDataFrame)
    narwhals_types = (nw.DataFrame, nw.LazyFrame) if include_narwhals else ()
    if not isinstance(df, df_types + narwhals_types):
        return False
    if eagerframe_only and is_lazyframe(df, include_narwhals=include_narwhals):
        return False
    return True


def is_lazyframe(df: IntoFrame, include_narwhals=True) -> bool:
    """
    Returns True if `df` is a lazily evaluated dataframe.

    Args:
        include_narwhals: Whether to include Narwhals' lazyframes in the check.
    """
    lazy_types = (pl.LazyFrame, dd.DataFrame, ps.DataFrame, SparkDataFrame)
    narwhals_types = (nw.LazyFrame,) if include_narwhals else ()
    return isinstance(df, lazy_types + narwhals_types)


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