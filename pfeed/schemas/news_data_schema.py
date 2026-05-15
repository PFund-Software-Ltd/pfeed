from typing import Optional

import pandera.polars as pa

from pfeed.schemas.time_based_data_schema import TimeBasedDataSchema


class NewsDataSchema(TimeBasedDataSchema):
    title: str
    content: str
    publisher: str
    url: str
    product: str = pa.Field(nullable=True)
    author: Optional[str] = pa.Field(nullable=True)
    exchange: Optional[str] = pa.Field(nullable=True)
    symbol: Optional[str] = pa.Field(nullable=True)
