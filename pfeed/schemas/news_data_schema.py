import datetime

import pandera as pa
from pandera.typing import Series


class NewsDataSchema(pa.DataFrameModel):
    date: Series[datetime.datetime]
    title: Series[str]
    content: Series[str]
    publisher: Series[str]
    url: Series[str]
    product: Series[str] = pa.Field(nullable=True)
    author: Series[str] | None = pa.Field(nullable=True)
    exchange: Series[str] | None = pa.Field(nullable=True)
    symbol: Series[str] | None
