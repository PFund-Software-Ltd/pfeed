from __future__ import annotations

from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from conftest import FakeSource

    from pfeed.dataflow.result import RunResult
    from pfeed.io.base_io import BaseIO

import datetime

import polars as pl
from pfund_blocks.resolution import Resolution
from polars.testing import assert_frame_equal

START_DATE = datetime.date(2026, 9, 1)
END_DATE = datetime.date(2026, 9, 2)


def test_download_and_retrieve(fake_source: FakeSource, io: BaseIO, tick_interval: datetime.timedelta):
    feed = fake_source.market_feed
    downloaded = cast(
        "RunResult", feed.download("BTC_USDT_PERP", "1t", start_date=START_DATE, end_date=END_DATE, io=io)
    )
    retrieved = cast(
        "RunResult", feed.retrieve("BTC_USDT_PERP", "1t", start_date=START_DATE, end_date=END_DATE, io=io)
    )

    assert downloaded.success, downloaded.errors
    assert retrieved.success, retrieved.errors
    retrieved_df = cast("pl.LazyFrame", retrieved.data).collect()
    # what was written is what comes back
    assert_frame_equal(retrieved_df, cast("pl.LazyFrame", downloaded.data).collect())
    # both dates came back, with the vendor's epoch ms converted to the right timestamps
    expected_dates = pl.datetime_range(
        START_DATE, END_DATE + datetime.timedelta(days=1), tick_interval, closed="left", eager=True
    )
    assert retrieved_df["date"].to_list() == expected_dates.to_list()


def test_download_and_retrieve_resampled(fake_source: FakeSource, io: BaseIO):
    feed = fake_source.market_feed
    downloaded = cast(
        "RunResult", feed.download("BTC_USDT_PERP", "1t", start_date=START_DATE, end_date=END_DATE, io=io)
    )
    retrieved = cast(
        "RunResult", feed.retrieve("BTC_USDT_PERP", "1d", start_date=START_DATE, end_date=END_DATE, io=io)
    )

    assert downloaded.success, downloaded.errors
    assert retrieved.success, retrieved.errors
    # one bar per day, aggregated from that day's downloaded ticks
    expected_df = (
        cast("pl.LazyFrame", downloaded.data)
        .group_by(pl.col("date").dt.truncate("1d"), maintain_order=True)
        .agg(
            open=pl.col("price").first(),
            high=pl.col("price").max(),
            low=pl.col("price").min(),
            close=pl.col("price").last(),
            volume=pl.col("volume").sum(),
            n_data_points=pl.len(),
        )
        .collect()
    )
    retrieved_df = cast("pl.LazyFrame", retrieved.data).collect()
    assert_frame_equal(retrieved_df.select(expected_df.columns), expected_df)
    assert retrieved_df["resolution"].to_list() == [str(Resolution("1d"))] * len(expected_df)


def test_download_and_retrieve_date_subset(fake_source: FakeSource, io: BaseIO):
    feed = fake_source.market_feed
    downloaded = cast(
        "RunResult", feed.download("BTC_USDT_PERP", "1t", start_date=START_DATE, end_date=END_DATE, io=io)
    )
    # only the second of the two downloaded dates
    retrieved = cast(
        "RunResult", feed.retrieve("BTC_USDT_PERP", "1t", start_date=END_DATE, end_date=END_DATE, io=io)
    )

    assert downloaded.success, downloaded.errors
    assert retrieved.success, retrieved.errors
    expected_df = cast("pl.LazyFrame", downloaded.data).filter(pl.col("date").dt.date() == END_DATE).collect()
    assert_frame_equal(cast("pl.LazyFrame", retrieved.data).collect(), expected_df)


def test_pipeline_download_and_retrieve(fake_pipeline_source: FakeSource, io: BaseIO):
    feed = fake_pipeline_source.market_feed
    bases = ["BTC_USDT_PERP", "ETH_USDT_PERP"]
    # requests queue up and run together in a single run()
    for basis in bases:
        feed.download(basis, "1t", start_date=START_DATE, end_date=END_DATE, io=io)
    downloaded = feed.run()
    for basis in bases:
        feed.retrieve(basis, "1t", start_date=START_DATE, end_date=END_DATE, io=io)
    retrieved = feed.run()

    assert downloaded.success, downloaded.errors
    assert retrieved.success, retrieved.errors
    retrieved_df = cast("pl.LazyFrame", retrieved.data).collect()
    assert_frame_equal(retrieved_df, cast("pl.LazyFrame", downloaded.data).collect())
    # every queued product made it through
    expected_products = {fake_pipeline_source.create_product(basis).name for basis in bases}
    assert set(retrieved_df["product"].unique()) == expected_products
