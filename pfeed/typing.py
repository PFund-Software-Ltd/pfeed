from typing_extensions import TypedDict
from typing import Any, Literal, Protocol

import os
import datetime

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
GenericData = GenericFrame | bytes | dict | list[dict]


tDataTool = Literal['pandas', 'polars']
tDataSource = Literal['YAHOO_FINANCE', 'DATABENTO', 'BYBIT', 'FINANCIAL_MODELING_PREP']
tDataLayer = Literal['RAW', 'CLEANED', 'CURATED']
tDataCategory = Literal['MARKET_DATA', 'NEWS_DATA']
tDataType = Literal['quote_L3', 'quote_L2', 'quote_L1', 'quote', 'tick', 'second', 'minute', 'hour', 'day']
tStorage = Literal['CACHE', 'LOCAL', 'MINIO', 'DUCKDB']


class StorageMetadata(TypedDict, total=False):
    file_metadata: dict[str, Any]
    missing_file_paths: list[str]
    missing_dates: list[datetime.date]
