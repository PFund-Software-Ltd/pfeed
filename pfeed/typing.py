from typing import Any, Protocol, TypeAlias
from narwhals.series import Series
from narwhals.dataframe import DataFrame, LazyFrame
from pfeed.streaming.streaming_message import StreamingMessage


class DataFrameLike(Protocol):
    def __dataframe__(self, *args: Any, **kwargs: Any) -> Any: ...


GenericFrame: TypeAlias = DataFrame[Any] | LazyFrame[Any] | DataFrameLike
GenericSeries = Series[Any]
GenericData = GenericFrame | bytes
StreamingData = dict[str, Any] | StreamingMessage


__all__ = [
    "DataFrame",
    "LazyFrame",
    "DataFrameLike",
    "GenericFrame",
    "GenericSeries",
    "GenericData",
    "StreamingData",
]