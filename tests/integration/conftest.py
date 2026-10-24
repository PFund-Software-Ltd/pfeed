from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar

if TYPE_CHECKING:
    from pathlib import Path

    from pfeed.base.feed import BaseFeed
    from pfeed.io.base_io import BaseIO
    from pfeed.market.data_model import MarketDataModel
    from pfeed.market.requests.base_request import MarketFeedBaseRequest

import datetime

import polars as pl
import pytest
from pfund.entities.products.product_base import BaseProduct
from pfund.entities.products.product_basis import ProductBasis

import pfeed as pe
from pfeed.enums import DataAccessType, DataCategory, DataProviderType, DataType
from pfeed.io.ducklake_io import DuckLakeIO
from pfeed.io.parquet_io import ParquetIO
from pfeed.source import BaseSource, SourceMetadata

TICK_INTERVAL = datetime.timedelta(hours=6)

class FakeMarketFeed(pe.MarketFeed):
    class Requests(pe.MarketFeed.Requests):  # ty: ignore[invalid-attribute-override]
        stream = None

    downloaded_data_date_cols: ClassVar[list[str]] = ["ts"]
    download_dataflow_per_date: ClassVar[bool] = True

    @staticmethod
    def _normalize_downloaded_data(df: pl.LazyFrame) -> pl.LazyFrame:
        # the fake vendor already ships pfeed's columns and dtypes
        return df

    @staticmethod
    def _download_impl(request: MarketFeedBaseRequest, data_model: MarketDataModel) -> pl.LazyFrame | None:
        date: datetime.date = data_model.start_date
        ticks = pl.datetime_range(
            date, date + datetime.timedelta(days=1), TICK_INTERVAL, closed="left", eager=True
        )
        day = date.toordinal()  # distinct values per day, so rows from different dates can't be confused
        return pl.LazyFrame(
            {
                # epoch ms, so the feed's timestamp unit inference runs
                "ts": ticks.dt.epoch("ms"),
                "price": [float(day + i) for i in range(len(ticks))],
                "volume": [float(i + 1) for i in range(len(ticks))],
                "side": [1 if i % 2 == 0 else -1 for i in range(len(ticks))],
            },
            schema_overrides={"side": pl.Int8},
        )


class FakeProduct(BaseProduct):
    pass


class FakeSource(BaseSource):
    """Deterministic in-memory vendor: one tick every TICK_INTERVAL per date."""

    Feeds: ClassVar[dict[DataCategory, type[BaseFeed]]] = {DataCategory.MARKET_DATA: FakeMarketFeed}
    metadata: ClassVar[SourceMetadata] = SourceMetadata(
        name="FAKE",
        url="https://example.com",
        data_categories={DataCategory.MARKET_DATA: {DataType.TICK: ["PERPETUAL"]}},
        provider_type=DataProviderType.VENUE,
        access_type=DataAccessType.FREE,
    )
    market_feed: FakeMarketFeed

    @classmethod
    def create_product(cls, basis: str, symbol: str = "", **specs: Any) -> FakeProduct:
        # symbol must be set, otherwise BaseProduct names every product "FAKE_"
        return FakeProduct(
            source=cls.metadata.name, basis=ProductBasis(basis=basis), symbol=symbol or basis, specs=specs
        )


@pytest.fixture
def fake_source() -> FakeSource:
    return FakeSource()


@pytest.fixture
def fake_pipeline_source() -> FakeSource:
    return FakeSource(pipeline_mode=True)


@pytest.fixture
def tick_interval() -> datetime.timedelta:
    return TICK_INTERVAL


@pytest.fixture(params=[ParquetIO, DuckLakeIO], ids=lambda c: c.__name__)
def io(request: pytest.FixtureRequest, tmp_path: Path) -> BaseIO:
    # always pass io explicitly: retrieve(io=None) falls back to ParquetIO() under the real data_path
    return request.param(base_path=str(tmp_path))
