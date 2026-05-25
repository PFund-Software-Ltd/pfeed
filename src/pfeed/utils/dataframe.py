# pyright: reportUnknownVariableType=false
from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from narwhals.typing import Frame

import narwhals as nw
from narwhals.dependencies import (
    is_dask_dataframe,
    is_pandas_dataframe,
    is_polars_dataframe,
    is_polars_lazyframe,
    is_pyspark_dataframe,
)

__all__ = [
    "from_native",
    "is_dataframe",
    "is_eagerframe",
    "is_empty_dataframe",
    "is_lazyframe",
]


# narwhals' dependency helpers don't cover these two — fall back to
# class/module sniffing, which also avoids importing the packages.
def _is_pyspark_pandas_dataframe(df: Any) -> bool:
    """pyspark.pandas.DataFrame. Narwhals doesn't process it directly —
    callers must convert via `.to_spark()` first."""
    cls = type(df)
    return cls.__name__ == "DataFrame" and cls.__module__.startswith("pyspark.pandas")


def _is_daft_dataframe(df: Any) -> bool:
    cls = type(df)
    return cls.__name__ == "DataFrame" and cls.__module__.startswith("daft")


def is_lazyframe(df: Any) -> bool:
    if isinstance(df, nw.LazyFrame):
        return True
    if is_pandas_dataframe(df):
        return False
    return (
        is_polars_lazyframe(df)
        or is_dask_dataframe(df)
        or is_pyspark_dataframe(df)
        or _is_pyspark_pandas_dataframe(df)  # lazy under the hood
        or _is_daft_dataframe(df)  # lazy by default
    )


def is_eagerframe(df: Any) -> bool:
    if isinstance(df, nw.DataFrame):
        return True
    if is_polars_lazyframe(df):
        return False
    return is_pandas_dataframe(df) or is_polars_dataframe(df)


def is_dataframe(df: Any) -> bool:
    """
    Returns True if the input is recognized as a supported DataFrame or LazyFrame.

    This checks for Pandas, Polars (eager and lazy), Dask, PySpark, pyspark.pandas,
    Daft, and any Narwhals DataFrame/LazyFrame types.

    Args:
        df: The object to check.

    Returns:
        bool: True if `df` is a supported dataframe type, otherwise False.
    """
    if isinstance(df, (nw.DataFrame, nw.LazyFrame)):
        return True
    if is_pandas_dataframe(df) or is_polars_dataframe(df):
        return True
    return (
        is_polars_lazyframe(df)
        or is_dask_dataframe(df)
        or is_pyspark_dataframe(df)
        or _is_pyspark_pandas_dataframe(df)
        or _is_daft_dataframe(df)
    )


def from_native(df: Any) -> Frame:
    """Wraps `nw.from_native` with a `.to_spark()` step for pyspark.pandas.
    Prefer this over `nw.from_native` anywhere in pfeed."""
    if _is_pyspark_pandas_dataframe(df):
        df = df.to_spark()
    return nw.from_native(df)


def is_empty_dataframe(df: Any) -> bool:
    nwdf = from_native(df)
    if isinstance(nwdf, nw.LazyFrame):
        nwdf = nwdf.head(1).collect()
    return nwdf.is_empty()
