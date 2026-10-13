from typing import Final

import pandera.polars as pa
import polars as pl
from pfund.datas.resolution import Resolution

from pfeed.schemas.market_data_schema import RESOLUTION, VOLUME, MarketDataSchema

# column names of cleaned tick data, typed as str for code referring to them
PRICE: Final = "price"
SIDE: Final = "side"


class TickDataSchema(MarketDataSchema):
    # tick data is always 1_TICK, e.g. 2_TICK is aggregated data, not tick data
    resolution: str = pa.Field(alias=RESOLUTION, eq=str(Resolution("1t")))
    price: float = pa.Field(alias=PRICE, gt=0, lt=float("inf"))
    volume: float = pa.Field(alias=VOLUME, gt=0, lt=float("inf"))
    # not all tick sources expose trade direction (e.g. Bybit streaming ticks have none).
    # pin Int8 (the compact dtype producers emit, e.g. Bybit's replace_strict to {1, -1});
    # a bare `int` annotation would default to Int64 and reject the Int8 data.
    side: pl.Int8 = pa.Field(alias=SIDE, isin=[1, -1], nullable=True)
