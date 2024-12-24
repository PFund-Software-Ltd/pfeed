from typing import TypeVar

import os
import pandas as pd
try:
    import polars as pl
except ImportError:
    class pl:
        class DataFrame:
            pass
        class LazyFrame:
            pass
        class Series:
            pass

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

tDataFrame = TypeVar('tDataFrame', pd.DataFrame, pl.DataFrame, pl.LazyFrame, dd.DataFrame, SparkDataFrame)
tSeries = TypeVar('tSeries', pd.Series, pl.Series, dd.Series, ps.Series)
tData = TypeVar('tData', tDataFrame, bytes)  # EXTEND
tDataModel = TypeVar('tDataModel', MarketDataModel, FundamentalDataModel)  # EXTEND


def is_dataframe(value) -> bool:
    return isinstance(value, (pd.DataFrame, pl.DataFrame, pl.LazyFrame, dd.DataFrame, ps.DataFrame, SparkDataFrame))


def is_series(value) -> bool:
    return isinstance(value, (pd.Series, pl.Series, dd.Series, ps.Series))