"""How downloaded data is stored: overwrites, missing dates and vendor failures."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from conftest import FakeSource
    from pytest_mock import MockerFixture

    from pfeed.io.base_io import BaseIO
    from pfeed.market.data_model import MarketDataModel
    from pfeed.market.requests.base_request import MarketFeedBaseRequest

import datetime

import polars as pl
from polars.testing import assert_frame_equal

from tests.integration.conftest import FakeMarketFeed

DATE_1 = datetime.date(2026, 9, 1)
DATE_2 = datetime.date(2026, 9, 2)
DATE_3 = datetime.date(2026, 9, 3)
UNSTORED_DATE = datetime.date(2026, 10, 1)

_vendor_download = FakeMarketFeed._download_impl


def _revised(
    request: MarketFeedBaseRequest, data_model: MarketDataModel
) -> pl.LazyFrame | None:
    """The vendor's data with revised prices."""
    df = _vendor_download(request, data_model)
    return None if df is None else df.with_columns(pl.col("price") + 1000)


def _dates(df: pl.DataFrame) -> list[datetime.date]:
    return df["date"].dt.date().unique().sort().to_list()


def test_redownload_replaces_overlapping_dates(
    fake_source: FakeSource, io: BaseIO, mocker: MockerFixture
):
    feed = fake_source.market_feed
    first = feed.download(
        "BTC_USDT_PERP", "1t", start_date=DATE_1, end_date=DATE_2, io=io
    )

    # the vendor has revised its prices since the first download
    _ = mocker.patch.object(FakeMarketFeed, "_download_impl", staticmethod(_revised))
    second = feed.download(
        "BTC_USDT_PERP", "1t", start_date=DATE_2, end_date=DATE_3, io=io
    )
    retrieved = feed.retrieve(
        "BTC_USDT_PERP", "1t", start_date=DATE_1, end_date=DATE_3, io=io
    )

    assert first.success, first.errors
    assert second.success, second.errors
    assert retrieved.success, retrieved.errors
    first_df = cast("pl.LazyFrame", first.data).collect()
    second_df = cast("pl.LazyFrame", second.data).collect()
    # DATE_1 is kept from the first download, DATE_2 is replaced by the second, not duplicated
    expected_df = pl.concat(
        [first_df.filter(pl.col("date").dt.date() == DATE_1), second_df]
    )
    assert_frame_equal(cast("pl.LazyFrame", retrieved.data).collect(), expected_df)


def test_retrieve_unstored_dates_gives_no_data(
    fake_source: FakeSource, io: BaseIO, mocker: MockerFixture
):
    feed = fake_source.market_feed
    warning = mocker.spy(feed.logger, "warning")
    downloaded = feed.download(
        "BTC_USDT_PERP", "1t", start_date=DATE_1, end_date=DATE_1, io=io
    )
    retrieved = feed.retrieve(
        "BTC_USDT_PERP", "1t", start_date=UNSTORED_DATE, end_date=UNSTORED_DATE, io=io
    )

    assert downloaded.success, downloaded.errors
    # nothing stored isn't a failure
    assert retrieved.success, retrieved.errors
    assert retrieved.data is None
    warning.assert_called_once()
    assert str(UNSTORED_DATE) in warning.call_args.args[0]


def test_retrieve_partly_stored_dates_gives_the_stored_ones(
    fake_source: FakeSource, io: BaseIO, mocker: MockerFixture
):
    feed = fake_source.market_feed
    downloaded = feed.download(
        "BTC_USDT_PERP", "1t", start_date=DATE_1, end_date=DATE_1, io=io
    )
    warning = mocker.spy(feed.logger, "warning")
    retrieved = feed.retrieve(
        "BTC_USDT_PERP", "1t", start_date=DATE_1, end_date=DATE_2, io=io
    )

    assert downloaded.success, downloaded.errors
    assert retrieved.success, retrieved.errors
    assert_frame_equal(
        cast("pl.LazyFrame", retrieved.data).collect(),
        cast("pl.LazyFrame", downloaded.data).collect(),
    )
    # the missing date is reported
    warning.assert_called_once()
    assert str(DATE_2) in warning.call_args.args[0]


def test_retrieve_prefers_the_stored_resolution(
    fake_source: FakeSource, io: BaseIO, mocker: MockerFixture
):
    feed = fake_source.market_feed
    ticks = feed.download(
        "BTC_USDT_PERP", "1t", start_date=DATE_1, end_date=DATE_1, io=io
    )
    # 1d bars from revised prices, so they differ from the bars resampled from the stored ticks
    _ = mocker.patch.object(FakeMarketFeed, "_download_impl", staticmethod(_revised))
    bars = feed.download(
        "BTC_USDT_PERP", "1d", start_date=DATE_1, end_date=DATE_1, io=io
    )
    retrieved = feed.retrieve(
        "BTC_USDT_PERP", "1d", start_date=DATE_1, end_date=DATE_1, io=io
    )

    assert ticks.success, ticks.errors
    assert bars.success, bars.errors
    assert retrieved.success, retrieved.errors
    assert_frame_equal(
        cast("pl.LazyFrame", retrieved.data).collect(),
        cast("pl.LazyFrame", bars.data).collect(),
    )


def test_vendor_failure_on_one_date_stores_only_the_others(
    fake_source: FakeSource, io: BaseIO, mocker: MockerFixture
):
    error = RuntimeError("vendor is down")

    def failing_on_date_2(
        request: MarketFeedBaseRequest, data_model: MarketDataModel
    ) -> pl.LazyFrame | None:
        if data_model.start_date == DATE_2:
            raise error
        return _vendor_download(request, data_model)

    _ = mocker.patch.object(
        FakeMarketFeed, "_download_impl", staticmethod(failing_on_date_2)
    )
    feed = fake_source.market_feed
    downloaded = feed.download(
        "BTC_USDT_PERP", "1t", start_date=DATE_1, end_date=DATE_3, io=io
    )

    assert not downloaded.success
    assert downloaded.errors == [error]
    assert len(downloaded.completed) == 2
    # the result holds the dates that succeeded
    assert _dates(cast("pl.LazyFrame", downloaded.data).collect()) == [DATE_1, DATE_3]
    # and only they are stored, nothing of the failed date
    retrieved = feed.retrieve(
        "BTC_USDT_PERP", "1t", start_date=DATE_1, end_date=DATE_3, io=io
    )
    assert retrieved.success, retrieved.errors
    assert _dates(cast("pl.LazyFrame", retrieved.data).collect()) == [DATE_1, DATE_3]
