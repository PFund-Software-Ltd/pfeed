import os

import pandas as pd
import polars as pl
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
    from pyspark.sql import DataFrame as SparkDataFrame, SparkSession
except ImportError:
    class ps:
        class DataFrame:
            pass

        class Series:
            pass

    class SparkDataFrame:
        pass

    class SparkSession:
        pass
           

tDataFrame = pd.DataFrame | pl.DataFrame | pl.LazyFrame | dd.DataFrame | ps.DataFrame | SparkDataFrame
tSeries = pd.Series | pl.Series | dd.Series | ps.Series
tData = tDataFrame | bytes