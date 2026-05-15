import polars as pl
import pandera.polars as pa

from pfeed.schemas.time_based_data_schema import TimeBasedDataSchema
from pfeed.enums import MarketDataType
from pfund.datas.resolution import Resolution


class MarketDataSchema(TimeBasedDataSchema):
    product: str = pa.Field(nullable=True)
    resolution: str = pa.Field(isin=
        set(repr(Resolution(dtype)) for dtype in MarketDataType.__members__.keys())
    )

    @pa.check('resolution')
    def validate_unique_resolution(cls, data: pa.PolarsData) -> pl.LazyFrame:
        return data.lazyframe.select(pl.col(data.key).n_unique() <= 1)

    @pa.check('product')
    def validate_unique_product(cls, data: pa.PolarsData) -> pl.LazyFrame:
        return data.lazyframe.select(pl.col(data.key).n_unique() <= 1)
