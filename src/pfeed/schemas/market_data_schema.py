from __future__ import annotations

from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from pfund.entities.products.product_base import BaseProduct

import pandera.polars as pa
import polars as pl
from pfund.datas.resolution import Resolution

from pfeed.enums import MarketDataType
from pfeed.schemas.time_based_data_schema import DATE, TimeBasedDataSchema

# column names of cleaned market data, typed as str for code referring to them
PRODUCT: Final = "product"
RESOLUTION: Final = "resolution"
# shared by tick and bar data
VOLUME: Final = "volume"
# columns that identify a row, same as pfund's KEY_COLS
KEY_COLS: Final = (DATE, PRODUCT, RESOLUTION)


class MarketDataSchema(TimeBasedDataSchema):
    product: str = pa.Field(alias=PRODUCT, nullable=True)
    resolution: str = pa.Field(
        alias=RESOLUTION,
        isin={str(Resolution(dtype)) for dtype in MarketDataType.__members__}
    )

    @pa.check(RESOLUTION)
    @classmethod
    def validate_unique_resolution(cls, data: pa.PolarsData) -> pl.LazyFrame:
        return data.lazyframe.select(pl.col(data.key).n_unique() <= 1)

    @pa.check(PRODUCT)
    @classmethod
    def validate_unique_product(cls, data: pa.PolarsData) -> pl.LazyFrame:
        return data.lazyframe.select(pl.col(data.key).n_unique() <= 1)


def get_market_data_schema(product: BaseProduct, resolution: Resolution) -> type[MarketDataSchema]:
    """The schema of cleaned market data, by the product's asset type and the resolution's data type.

    Takes the product instead of its asset type, so schemas can later depend on its specs (e.g. options).

    Raises:
        NotImplementedError: If the resolution is quote data.
    """
    # lazy: these schemas subclass MarketDataSchema, importing them at the top would be circular
    from pfeed.schemas.bar_data_schema import BarDataSchema
    from pfeed.schemas.stock_data_schema import StockBarDataSchema, StockTickDataSchema
    from pfeed.schemas.tick_data_schema import TickDataSchema

    is_stock = product.is_stock() or product.is_etf()
    if resolution.is_quote():
        raise NotImplementedError("quote data is not supported yet")
    elif resolution.is_tick():
        return StockTickDataSchema if is_stock else TickDataSchema
    elif resolution.is_bar():
        return StockBarDataSchema if is_stock else BarDataSchema
    else:
        return MarketDataSchema
