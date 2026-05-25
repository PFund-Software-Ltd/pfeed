import pandera.polars as pa
import polars as pl
from pfund.datas.resolution import Resolution

from pfeed.enums import MarketDataType
from pfeed.schemas.time_based_data_schema import TimeBasedDataSchema


class MarketDataSchema(TimeBasedDataSchema):
    product: str = pa.Field(nullable=True)
    resolution: str = pa.Field(
        isin={str(Resolution(dtype)) for dtype in MarketDataType.__members__}
    )

    @pa.check("resolution")
    @classmethod
    def validate_unique_resolution(cls, data: pa.PolarsData) -> pl.LazyFrame:
        return data.lazyframe.select(pl.col(data.key).n_unique() <= 1)

    @pa.check("product")
    @classmethod
    def validate_unique_product(cls, data: pa.PolarsData) -> pl.LazyFrame:
        return data.lazyframe.select(pl.col(data.key).n_unique() <= 1)
