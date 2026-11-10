"""Replay downloaded data, from a feed and through the DataEngine."""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal, cast

if TYPE_CHECKING:
    from pfund_blocks.typing import Message
    from pytest_mock import MockerFixture

    from pfeed.io.base_io import BaseIO

import datetime

import polars as pl
from pfund_blocks.datas.bar_data import Bar
from pfund_blocks.datas.tick_data import Tick
from pfund_blocks.resolution import Resolution

from pfeed import registry
from pfeed.engine import DataEngine
from tests.integration.conftest import FakeMarketFeed, FakeSource

START_DATE = datetime.date(2026, 9, 1)
END_DATE = datetime.date(2026, 9, 2)
NS_PER_DAY = 86_400 * 1_000_000_000


def _date_ns(df: pl.DataFrame) -> list[int]:
    return df["date"].cast(pl.Int64).to_list()


def test_replay_downloaded_ticks(fake_source: FakeSource, io: BaseIO):
    feed = fake_source.market_feed
    downloaded = feed.download(
        "BTC_USDT_PERP", "1t", start_date=START_DATE, end_date=END_DATE, io=io
    )
    messages: list[Message] = []
    replayed = feed.replay(
        "BTC_USDT_PERP",
        "1t",
        start_date=START_DATE,
        end_date=END_DATE,
        io=io,
        callback=messages.append,
    )

    assert downloaded.success, downloaded.errors
    assert replayed.success, replayed.errors
    downloaded_df = cast("pl.LazyFrame", downloaded.data).collect()
    ticks = [cast("list[Tick]", message["data"])[0] for message in messages]
    # every stored tick, in order, received at its timestamp
    assert [tick.ts for tick in ticks] == _date_ns(downloaded_df)
    assert [message["received_at"] for message in messages] == _date_ns(downloaded_df)
    assert [tick.price for tick in ticks] == downloaded_df["price"].to_list()
    assert [tick.volume for tick in ticks] == downloaded_df["volume"].to_list()
    assert {message["product"] for message in messages} == {
        fake_source.create_product("BTC_USDT_PERP").name
    }


def test_engine_merges_replays_of_feeds_by_time(
    fake_source: FakeSource, io: BaseIO, mocker: MockerFixture
):
    feed = fake_source.market_feed
    ticks = feed.download(
        "BTC_USDT_PERP", "1t", start_date=START_DATE, end_date=END_DATE, io=io
    )
    bars = feed.download(
        "ETH_USDT_PERP", "1d", start_date=START_DATE, end_date=END_DATE, io=io
    )
    assert ticks.success, ticks.errors
    assert bars.success, bars.errors

    # the engine creates its feeds by source name
    _ = mocker.patch.object(registry, "get_source", return_value=FakeSource)
    _ = mocker.patch.object(registry, "get_feed", return_value=FakeMarketFeed)
    messages: list[Message] = []
    engine = DataEngine(callback=messages.append)
    btc_feed, eth_feed = (
        cast("FakeMarketFeed[Literal[True]]", engine.add_feed("FAKE")) for _ in range(2)
    )
    _ = btc_feed.replay(
        "BTC_USDT_PERP", "1t", start_date=START_DATE, end_date=END_DATE, io=io
    )
    _ = eth_feed.replay(
        "ETH_USDT_PERP", "1d", start_date=START_DATE, end_date=END_DATE, io=io
    )
    results = engine.run()

    assert all(result.success for result in results.values()), [
        result.errors for result in results.values()
    ]
    received_at = [message["received_at"] for message in messages]
    # one stream ordered by when each message would have been received live
    assert received_at == sorted(received_at)
    btc, eth = (
        fake_source.create_product(basis).name
        for basis in ["BTC_USDT_PERP", "ETH_USDT_PERP"]
    )
    btc_messages = [message for message in messages if message["product"] == btc]
    eth_messages = [message for message in messages if message["product"] == eth]
    assert len(btc_messages) + len(eth_messages) == len(messages)
    ticks_df = cast("pl.LazyFrame", ticks.data).collect()
    bars_df = cast("pl.LazyFrame", bars.data).collect()
    assert [
        cast("list[Tick]", message["data"])[0].price for message in btc_messages
    ] == ticks_df["price"].to_list()
    replayed_bars = [cast("list[Bar]", message["data"])[0] for message in eth_messages]
    assert [bar.close for bar in replayed_bars] == bars_df["close"].to_list()
    assert all(
        bar.resolution == Resolution("1d") and bar.is_closed for bar in replayed_bars
    )
    # a bar is received when it closes, at the end of its day
    assert [message["received_at"] for message in eth_messages] == [
        ns + NS_PER_DAY for ns in _date_ns(bars_df)
    ]
