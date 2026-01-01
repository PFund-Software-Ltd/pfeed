import pandera.pandas as pa
from pandera.typing import Series

from pfeed.schemas.time_based_data_schema import TimeBasedDataSchema


class NewsDataSchema(TimeBasedDataSchema):
    title: Series[str]
    content: Series[str]
    publisher: Series[str]
    url: Series[str]
    product: Series[str] = pa.Field(nullable=True)
    author: Series[str] | None = pa.Field(nullable=True)
    exchange: Series[str] | None = pa.Field(nullable=True)
    symbol: Series[str] | None
