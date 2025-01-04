from typing import TypeVar

import os
import pandas as pd
import polars as pl
import narwhals as nw

try:
    import dask.dataframe as dd
except ImportError:
    class dd:
        class DataFrame:
            pass

        class Series:
            pass
    
try:
    os.environ['PYARROW_IGNORE_TIMEZONE'] = '1'  # used to suppress warning
    import pyspark.pandas as ps
    from pyspark.sql import DataFrame as SparkDataFrame
except ImportError:
    class ps:
        class DataFrame:
            pass

        class Series:
            pass

    class SparkDataFrame:
        pass
           
from pfeed.data_models import MarketDataModel, FundamentalDataModel

tDataFrame = pd.DataFrame | pl.DataFrame | pl.LazyFrame | dd.DataFrame | ps.DataFrame | SparkDataFrame
tSeries = pd.Series | pl.Series | dd.Series | ps.Series
tData = tDataFrame | bytes
tDataModel = MarketDataModel | FundamentalDataModel  # EXTEND


def is_dataframe(value, include_narwhals=True) -> bool:
    return (
        isinstance(value, (pd.DataFrame, pl.DataFrame, pl.LazyFrame, dd.DataFrame, ps.DataFrame, SparkDataFrame))
        or (include_narwhals and isinstance(value, (nw.DataFrame, nw.LazyFrame)))
    )


def is_series(value, include_narwhals=True) -> bool:
    return (
        isinstance(value, (pd.Series, pl.Series, dd.Series, ps.Series))
        or (include_narwhals and isinstance(value, (nw.Series)))
    )