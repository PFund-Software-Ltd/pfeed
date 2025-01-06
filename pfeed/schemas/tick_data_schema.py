import pandera as pa
from pandera.typing import Series

from pfeed.schemas import MarketDataSchema


class TickDataSchema(MarketDataSchema):
    price: Series[float]
    side: Series[int] = pa.Field(isin=[1, -1])
    volume: Series[float]
    