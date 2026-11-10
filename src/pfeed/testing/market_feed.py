from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar

if TYPE_CHECKING:
    from pathlib import Path

    from pfund_blocks.entities.products.product_base import BaseProduct

    from pfeed.source import BaseSource

import datetime
from dataclasses import dataclass, field

import polars as pl
import pytest
from pfund_blocks.resolution import Resolution
from polars.testing import assert_frame_equal

from pfeed.dataflow.result import RunResult
from pfeed.io.parquet_io import ParquetIO
from pfeed.market.feed import MarketFeed
from pfeed.schemas.market_data_schema import PRODUCT, RESOLUTION, get_market_data_schema
from pfeed.schemas.time_based_data_schema import DATE


@dataclass(frozen=True)
class MarketFeedCase:
    """One product + resolution to test against the real vendor."""

    product: str
    resolution: str
    product_specs: dict[str, Any] = field(default_factory=dict)
    # None = the suite's start_date/end_date
    start_date: str | None = None
    end_date: str | None = None

    def __str__(self) -> str:
        return f"{self.product}-{self.resolution}"


class MarketFeedE2ETests:
    """End-to-end tests of a source's market feed against its real vendor.

    Not collected by pytest itself (no `Test` prefix), subclass it as `TestXxxMarketFeed`.
    Detects vendor drift that the fake-source integration tests in pfeed can't,
    e.g. a changed file format or timestamp unit that breaks normalization.
    """

    source: ClassVar[type[BaseSource]]
    cases: ClassVar[list[MarketFeedCase]] = []
    start_date: ClassVar[str]
    end_date: ClassVar[str]
    # dates the vendor has no data for (e.g. before the product was listed), None = skip the test
    missing_dates_case: ClassVar[MarketFeedCase | None] = None

    def pytest_generate_tests(self, metafunc: pytest.Metafunc) -> None:
        if "case" in metafunc.fixturenames:
            metafunc.parametrize("case", self.cases, ids=str)

    def _create_feed(self) -> MarketFeed:
        # the source sets market_feed dynamically, so find it in the typed feeds instead
        return next(
            feed for feed in self.source().feeds if isinstance(feed, MarketFeed)
        )

    def _dates(self, case: MarketFeedCase) -> tuple[datetime.date, datetime.date]:
        return (
            datetime.date.fromisoformat(case.start_date or self.start_date),
            datetime.date.fromisoformat(case.end_date or self.end_date),
        )

    def expected_dates(
        self, product: BaseProduct, start_date: datetime.date, end_date: datetime.date
    ) -> list[datetime.date]:
        """Dates that must have data. Defaults to every day (crypto trades 24/7),
        override for sources with trading calendars, e.g. stocks."""
        return pl.date_range(start_date, end_date, eager=True).to_list()

    def check_data(
        self, df: pl.DataFrame, product: BaseProduct, case: MarketFeedCase
    ) -> None:
        """Assertions on downloaded/retrieved data; override to add source-specific ones."""
        resolution = Resolution(case.resolution)
        assert df.height > 0
        # columns, dtypes, sorted dates, and for bars unique dates and sane OHLC;
        # vendor columns beyond the schema are kept and allowed
        get_market_data_schema(product, resolution).validate(df)
        assert df[PRODUCT].unique().to_list() == [product.name]
        assert df[RESOLUTION].unique().to_list() == [str(resolution)]
        start_date, end_date = self._dates(case)
        assert df[DATE].dt.date().unique().sort().to_list() == self.expected_dates(
            product, start_date, end_date
        )

    def test_download_and_retrieve(self, case: MarketFeedCase, tmp_path: Path) -> None:
        feed = self._create_feed()
        product = feed.data_source.create_product(case.product, **case.product_specs)
        start_date, end_date = self._dates(case)
        io = ParquetIO(base_path=str(tmp_path))
        frames: list[pl.DataFrame] = []
        for func in (feed.download, feed.retrieve):
            result = func(
                product=case.product,
                resolution=case.resolution,
                start_date=start_date,
                end_date=end_date,
                io=io,
                **case.product_specs,
            )
            assert isinstance(result, RunResult)
            assert result.success, result.errors
            assert isinstance(result.data, pl.LazyFrame)
            df = result.data.collect()
            self.check_data(df, product, case)
            frames.append(df)
        downloaded_df, retrieved_df = frames
        # what was written is what comes back
        assert_frame_equal(retrieved_df, downloaded_df)

    def test_download_missing_dates(self, tmp_path: Path) -> None:
        case = self.missing_dates_case
        if case is None:
            pytest.skip("missing_dates_case is not set")
        start_date, end_date = self._dates(case)
        result = self._create_feed().download(
            product=case.product,
            resolution=case.resolution,
            start_date=start_date,
            end_date=end_date,
            io=ParquetIO(base_path=str(tmp_path)),
            **case.product_specs,
        )
        assert isinstance(result, RunResult)
        # the vendor's "no data" must become an empty result, not e.g. an HTTP error
        assert result.dataflows
        assert not result.completed
        assert result.errors
        assert all(
            isinstance(error, ValueError) and "produced no data" in str(error)
            for error in result.errors
        )
