from typing import Any, Literal, Protocol, TypeAlias
from narwhals.series import Series
from narwhals.dataframe import DataFrame, LazyFrame
from pfeed.messaging.streaming_message import StreamingMessage

class DataFrameLike(Protocol):
    def __dataframe__(self, *args: Any, **kwargs: Any) -> Any: ...

GenericFrame: TypeAlias = DataFrame[Any] | LazyFrame[Any] | DataFrameLike
GenericSeries = Series[Any]
GenericData = GenericFrame | bytes
StreamingData = dict | StreamingMessage

tDataTool = Literal['pandas', 'polars', 'dask', 'spark']
tDataSource = Literal['YAHOO_FINANCE', 'BYBIT', 'FINANCIAL_MODELING_PREP']
tDataCategory = Literal['MARKET_DATA', 'NEWS_DATA']
tDataType = Literal['quote_L3', 'quote_L2', 'quote_L1', 'quote', 'tick', 'second', 'minute', 'hour', 'day']
tStreamMode = Literal["SAFE", "FAST"]
