from __future__ import annotations

from typing import TYPE_CHECKING, Literal, cast

if TYPE_CHECKING:
    from conftest import FakeSource

    from pfeed.io.base_io import BaseIO
    from pfeed.market.data_model import MarketDataModel
    from pfeed.market.requests.base_request import MarketFeedBaseRequest

import datetime
from types import SimpleNamespace

import polars as pl
from pfund_blocks.resolution import Resolution
from polars.testing import assert_frame_equal

from tests.integration.conftest import FakeMarketFeed

START_DATE = datetime.date(2026, 9, 1)
END_DATE = datetime.date(2026, 9, 2)


def test_download_and_retrieve(
    fake_source: FakeSource, io: BaseIO, tick_interval: datetime.timedelta
):
    feed = fake_source.market_feed
    downloaded = feed.download(
        "BTC_USDT_PERP", "1t", start_date=START_DATE, end_date=END_DATE, io=io
    )
    retrieved = feed.retrieve(
        "BTC_USDT_PERP", "1t", start_date=START_DATE, end_date=END_DATE, io=io
    )

    assert downloaded.success, downloaded.errors
    assert retrieved.success, retrieved.errors
    retrieved_df = cast("pl.LazyFrame", retrieved.data).collect()
    # what was written is what comes back
    assert_frame_equal(retrieved_df, cast("pl.LazyFrame", downloaded.data).collect())
    # both dates came back, with the vendor's epoch ms converted to the right timestamps
    expected_dates = pl.datetime_range(
        START_DATE,
        END_DATE + datetime.timedelta(days=1),
        tick_interval,
        closed="left",
        eager=True,
    )
    assert retrieved_df["date"].to_list() == expected_dates.to_list()


def test_download_and_retrieve_resampled(fake_source: FakeSource, io: BaseIO):
    feed = fake_source.market_feed
    downloaded = feed.download(
        "BTC_USDT_PERP", "1t", start_date=START_DATE, end_date=END_DATE, io=io
    )
    retrieved = feed.retrieve(
        "BTC_USDT_PERP", "1d", start_date=START_DATE, end_date=END_DATE, io=io
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
    assert retrieved_df["resolution"].to_list() == [str(Resolution("1d"))] * len(
        expected_df
    )


def test_download_and_retrieve_date_subset(fake_source: FakeSource, io: BaseIO):
    feed = fake_source.market_feed
    downloaded = feed.download(
        "BTC_USDT_PERP", "1t", start_date=START_DATE, end_date=END_DATE, io=io
    )
    # only the second of the two downloaded dates
    retrieved = feed.retrieve(
        "BTC_USDT_PERP", "1t", start_date=END_DATE, end_date=END_DATE, io=io
    )

    assert downloaded.success, downloaded.errors
    assert retrieved.success, retrieved.errors
    expected_df = (
        cast("pl.LazyFrame", downloaded.data)
        .filter(pl.col("date").dt.date() == END_DATE)
        .collect()
    )
    assert_frame_equal(cast("pl.LazyFrame", retrieved.data).collect(), expected_df)


def test_pipeline_download_and_retrieve(
    fake_pipeline_source: FakeSource[Literal[True]], io: BaseIO
):
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
    expected_products = {
        fake_pipeline_source.create_product(basis).name for basis in bases
    }
    assert set(retrieved_df["product"].unique()) == expected_products


def test_pipeline_transform_and_load_target_the_previous_download_only(
    fake_pipeline_source: FakeSource[Literal[True]], fake_source: FakeSource, io: BaseIO
):
    def double_price(df: pl.LazyFrame) -> pl.LazyFrame:
        return df.with_columns(pl.col("price") * 2)

    feed = fake_pipeline_source.market_feed
    # transform() and load() apply to BTC's download only, ETH is downloaded without io, so it isn't stored
    (
        feed.download("BTC_USDT_PERP", "1t", start_date=START_DATE, end_date=END_DATE)
        .transform(double_price)
        .load(io=io, data_layer="curated")
        .download("ETH_USDT_PERP", "1t", start_date=START_DATE, end_date=END_DATE)
    )
    result = feed.run()

    assert result.success, result.errors
    btc, eth = (
        fake_pipeline_source.create_product(basis).name
        for basis in ["BTC_USDT_PERP", "ETH_USDT_PERP"]
    )
    # the vendor's data, without any user transformation, for comparison
    untransformed = cast(
        "pl.LazyFrame",
        fake_source.market_feed.download(
            "BTC_USDT_PERP", "1t", start_date=START_DATE, end_date=END_DATE
        ).data,
    ).collect()
    result_df = cast("pl.LazyFrame", result.data).collect()
    btc_df = result_df.filter(pl.col("product") == btc)
    eth_df = result_df.filter(pl.col("product") == eth)
    assert_frame_equal(btc_df, untransformed.with_columns(pl.col("price") * 2))
    # ETH's prices are the vendor's, the fake vendor's prices don't depend on the product
    assert eth_df["price"].to_list() == untransformed["price"].to_list()

    def retrieve(basis: str, data_layer: str) -> pl.DataFrame | None:
        retrieved = fake_source.market_feed.retrieve(
            basis,
            "1t",
            start_date=START_DATE,
            end_date=END_DATE,
            data_layer=data_layer,
            io=io,
        )
        assert retrieved.success, retrieved.errors
        return None if retrieved.data is None else retrieved.data.collect()

    # BTC is stored only in the curated layer, with its transformed prices
    assert_frame_equal(
        cast("pl.DataFrame", retrieve("BTC_USDT_PERP", "curated")), btc_df
    )
    assert retrieve("BTC_USDT_PERP", "cleaned") is None
    # ETH is stored nowhere
    assert retrieve("ETH_USDT_PERP", "cleaned") is None
    assert retrieve("ETH_USDT_PERP", "curated") is None


def test_download_and_retrieve_raw(fake_source: FakeSource, io: BaseIO):
    feed = fake_source.market_feed
    downloaded = feed.download(
        "BTC_USDT_PERP",
        "1t",
        start_date=START_DATE,
        end_date=END_DATE,
        data_layer="raw",
        io=io,
    )
    retrieved = feed.retrieve(
        "BTC_USDT_PERP",
        "1t",
        start_date=START_DATE,
        end_date=END_DATE,
        data_layer="raw",
        io=io,
    )

    assert downloaded.success, downloaded.errors
    assert retrieved.success, retrieved.errors
    retrieved_df = cast("pl.LazyFrame", retrieved.data).collect()
    assert_frame_equal(retrieved_df, cast("pl.LazyFrame", downloaded.data).collect())
    # raw data is the vendor's data as is, plus pfeed's internal date column
    # the fake vendor only reads the data model's start date
    vendor_df = pl.concat(
        [
            cast(
                "pl.LazyFrame",
                FakeMarketFeed._download_impl(
                    cast("MarketFeedBaseRequest", None),
                    cast("MarketDataModel", SimpleNamespace(start_date=date)),
                ),
            )
            for date in [START_DATE, END_DATE]
        ]
    ).collect()
    assert_frame_equal(retrieved_df.drop("_pfeed_date"), vendor_df)
    # nothing was cleaned, so nothing is in the cleaned layer
    cleaned = feed.retrieve(
        "BTC_USDT_PERP", "1t", start_date=START_DATE, end_date=END_DATE, io=io
    )
    assert cleaned.success, cleaned.errors
    assert cleaned.data is None


def test_retrieve_per_date_matches_a_single_dataflow(
    fake_source: FakeSource, io: BaseIO
):
    feed = fake_source.market_feed
    end_date = START_DATE + datetime.timedelta(days=2)
    downloaded = feed.download(
        "BTC_USDT_PERP", "1t", start_date=START_DATE, end_date=end_date, io=io
    )
    assert downloaded.success, downloaded.errors

    # downsampling is the case dataflow_per_date is meant for
    per_date = feed.retrieve(
        "BTC_USDT_PERP",
        "1d",
        start_date=START_DATE,
        end_date=end_date,
        dataflow_per_date=True,
        io=io,
    )
    single = feed.retrieve(
        "BTC_USDT_PERP", "1d", start_date=START_DATE, end_date=end_date, io=io
    )

    assert per_date.success, per_date.errors
    assert single.success, single.errors
    assert len(per_date.dataflows) == 3
    assert len(single.dataflows) == 1
    assert_frame_equal(
        cast("pl.LazyFrame", per_date.data).collect(),
        cast("pl.LazyFrame", single.data).collect(),
    )
