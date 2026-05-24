import polars as pl
import pandera.polars as pa

from pfeed.schemas import MarketDataSchema


class TickDataSchema(MarketDataSchema):
    price: float = pa.Field(gt=0)
    # not all tick sources expose trade direction (e.g. Bybit streaming ticks have none)
    side: int | None = pa.Field(isin=[1, -1], nullable=True)
    volume: float = pa.Field(gt=0)

    @pa.check('side')
    def validate_side_has_both_buy_and_sell(cls, data: pa.PolarsData) -> pl.LazyFrame:
        return data.lazyframe.select(pl.col(data.key).n_unique() == 2)
