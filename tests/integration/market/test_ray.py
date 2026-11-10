"""Download and retrieve with Ray workers writing to and reading from a real IO."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from pfeed.io.base_io import BaseIO

import datetime

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from tests.integration.conftest import FakeSource

_ = pytest.importorskip("ray")

START_DATE = datetime.date(2026, 9, 1)
END_DATE = datetime.date(2026, 9, 4)


def test_ray_download_and_retrieve_match_without_ray(io: BaseIO):
    ray_feed = FakeSource(num_workers=2).market_feed
    feed = FakeSource().market_feed
    # one dataflow per date, so the workers write the dates' partitions concurrently
    downloaded = ray_feed.download(
        "BTC_USDT_PERP", "1t", start_date=START_DATE, end_date=END_DATE, io=io
    )
    ray_retrieved = ray_feed.retrieve(
        "BTC_USDT_PERP",
        "1t",
        start_date=START_DATE,
        end_date=END_DATE,
        dataflow_per_date=True,
        io=io,
    )
    retrieved = feed.retrieve(
        "BTC_USDT_PERP", "1t", start_date=START_DATE, end_date=END_DATE, io=io
    )
    expected = feed.download(
        "BTC_USDT_PERP", "1t", start_date=START_DATE, end_date=END_DATE
    )

    assert downloaded.success, downloaded.errors
    assert ray_retrieved.success, ray_retrieved.errors
    assert retrieved.success, retrieved.errors
    expected_df = cast("pl.LazyFrame", expected.data).collect()
    # the results come back in date order, whichever worker finished first
    assert_frame_equal(cast("pl.LazyFrame", downloaded.data).collect(), expected_df)
    assert_frame_equal(cast("pl.LazyFrame", ray_retrieved.data).collect(), expected_df)
    # every date's partition was written
    assert_frame_equal(cast("pl.LazyFrame", retrieved.data).collect(), expected_df)
