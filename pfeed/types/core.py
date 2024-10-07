from typing import TypeVar

try:
    import pandas as pd
    import polars as pl
except ImportError:
    pass


tDataFrame = TypeVar('tDataFrame', pd.DataFrame, pl.DataFrame, pl.LazyFrame)
tSeries = TypeVar('tSeries', pd.Series, pl.Series)
