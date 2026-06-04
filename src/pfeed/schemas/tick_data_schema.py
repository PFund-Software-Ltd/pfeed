import pandera.polars as pa
import polars as pl

from pfeed.schemas.market_data_schema import MarketDataSchema


class TickDataSchema(MarketDataSchema):
    price: float = pa.Field(gt=0)
    # not all tick sources expose trade direction (e.g. Bybit streaming ticks have none).
    # pin Int8 (the compact dtype producers emit, e.g. Bybit's replace_strict to {1, -1});
    # a bare `int` annotation would default to Int64 and reject the Int8 data.
    side: pl.Int8 = pa.Field(isin=[1, -1], nullable=True)
    volume: float = pa.Field(gt=0)

    @pa.check("side")
    @classmethod
    def validate_side_has_both_buy_and_sell(cls, data: pa.PolarsData) -> pl.LazyFrame:
        return data.lazyframe.select(pl.col(data.key).n_unique() == 2)
