from typing import Final

import pandera.polars as pa
import polars as pl

from pfeed.schemas.market_data_schema import VOLUME, MarketDataSchema

# column names of cleaned tick data, typed as str for code referring to them
PRICE: Final = "price"
SIDE: Final = "side"


class TickDataSchema(MarketDataSchema):
    price: float = pa.Field(alias=PRICE, gt=0)
    # not all tick sources expose trade direction (e.g. Bybit streaming ticks have none).
    # pin Int8 (the compact dtype producers emit, e.g. Bybit's replace_strict to {1, -1});
    # a bare `int` annotation would default to Int64 and reject the Int8 data.
    side: pl.Int8 = pa.Field(alias=SIDE, isin=[1, -1], nullable=True)
    volume: float = pa.Field(alias=VOLUME, gt=0)

    @pa.check(SIDE)
    @classmethod
    def validate_side_has_both_buy_and_sell(cls, data: pa.PolarsData) -> pl.LazyFrame:
        return data.lazyframe.select(pl.col(data.key).n_unique() == 2)
