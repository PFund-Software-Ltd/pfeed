from typing import Any, Literal, Protocol
from pathlib import Path
from cloudpathlib import CloudPath
from narwhals.series import Series
from narwhals.dataframe import DataFrame, LazyFrame
from pfeed.messaging.streaming_message import StreamingMessage

class DataFrameLike(Protocol):
    def __dataframe__(self, *args: Any, **kwargs: Any) -> Any: ...

try:
    import dask.dataframe as dd
except ImportError:
    class dd:
        class DataFrame:
            pass
try:
    import pyspark.pandas as ps
    from pyspark.sql import DataFrame as SparkDataFrame
except ImportError:
    class ps:
        class DataFrame:
            pass
    class SparkDataFrame:
        pass

FilePath = CloudPath | Path
 
GenericFrame = DataFrame[Any] | LazyFrame[Any] | DataFrameLike
GenericFrameOrNone = GenericFrame | None
GenericSeries = Series[Any]
GenericData = GenericFrame | bytes
StreamingData = dict | StreamingMessage

tDataTool = Literal['pandas', 'polars', 'dask', 'spark']
tDataSource = Literal['YAHOO_FINANCE', 'BYBIT', 'FINANCIAL_MODELING_PREP']
tDataLayer = Literal['RAW', 'CLEANED', 'CURATED']
tDataCategory = Literal['MARKET_DATA', 'NEWS_DATA']
tDataType = Literal['quote_L3', 'quote_L2', 'quote_L1', 'quote', 'tick', 'second', 'minute', 'hour', 'day']
tStorage = Literal['CACHE', 'LOCAL', 'MINIO', 'DUCKDB', 'LANCEDB']
tStreamMode = Literal["SAFE", "FAST"]
