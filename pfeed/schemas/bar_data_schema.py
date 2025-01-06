from pandera.typing import Series

from pfeed.schemas import MarketDataSchema


class BarDataSchema(MarketDataSchema):
    open: Series[float]
    high: Series[float]
    low: Series[float]
    close: Series[float]
    volume: Series[float]
