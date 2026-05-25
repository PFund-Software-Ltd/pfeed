import pandera.polars as pa

from pfeed.schemas.time_based_data_schema import TimeBasedDataSchema


class NewsDataSchema(TimeBasedDataSchema):
    title: str
    content: str
    publisher: str
    url: str
    product: str = pa.Field(nullable=True)
    author: str | None = pa.Field(nullable=True)
    exchange: str | None = pa.Field(nullable=True)
    symbol: str | None = pa.Field(nullable=True)
