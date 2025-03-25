from typing import Any, Literal, Protocol

import os

from narwhals.series import Series
from narwhals.dataframe import DataFrame, LazyFrame
class DataFrameLike(Protocol):
    def __dataframe__(self, *args: Any, **kwargs: Any) -> Any: ...


try:
    import dask.dataframe as dd
except ImportError:
    class dd:
        class DataFrame:
            pass
try:
    os.environ['PYARROW_IGNORE_TIMEZONE'] = '1'  # used to suppress warning
    import pyspark.pandas as ps
    from pyspark.sql import DataFrame as SparkDataFrame
except ImportError:
    class ps:
        class DataFrame:
            pass
    class SparkDataFrame:
        pass
    

GenericFrame = DataFrame[Any] | LazyFrame[Any] | DataFrameLike
GenericSeries = Series[Any]
GenericData = GenericFrame | bytes


tDATA_SOURCE = Literal['YAHOO_FINANCE', 'DATABENTO', 'BYBIT', 'FINANCIAL_MODELING_PREP']
tDATA_LAYER = Literal['raw', 'cleaned', 'curated']
tPRODUCT_TYPE = Literal[
    'STK', 'FUT', 'ETF', 'OPT', 'FX', 'CRYPTO', 'BOND', 'MTF', 'CMDTY', 'INDEX',
    'PERP', 'IPERP', 'SPOT', 'IFUT'
]
tSTORAGE = Literal['cache', 'local', 'minio', 'duckdb']
tDATA_TOOL = Literal['pandas', 'polars', 'dask']
tENVIRONMENT = Literal['BACKTEST', 'SANDBOX', 'PAPER', 'LIVE']