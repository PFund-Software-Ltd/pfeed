"""Stream from the fake stream API, store the stream, then retrieve it."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from pathlib import Path

    from conftest import FakeSource
    from pfund_blocks.typing import Message

import polars as pl
from pfund_blocks.datas.tick_data import Tick
from pfund_kit.utils.temporal import get_utc_now

from pfeed.io.ducklake_io import DuckLakeIO
from tests.integration.conftest import NUM_STREAMED_TICKS


def test_stream_store_and_retrieve(fake_source: FakeSource, tmp_path: Path):
    # NOTE: only DuckLakeIO, ParquetIO can't append, so it can't store streams
    io = DuckLakeIO(base_path=str(tmp_path))
    feed = fake_source.market_feed
    messages: list[Message] = []
    streamed = feed.stream(
        "BTC_USDT_PERP", "1t", io=io, callback=messages.append, flush_interval=0.01
    )
    today = get_utc_now().date()
    retrieved = feed.retrieve(
        "BTC_USDT_PERP", "1t", start_date=today, end_date=today, env="LIVE", io=io
    )
    backtest = feed.retrieve(
        "BTC_USDT_PERP", "1t", start_date=today, end_date=today, io=io
    )

    assert streamed.success, streamed.errors
    assert retrieved.success, retrieved.errors
    assert len(messages) == NUM_STREAMED_TICKS
    ticks = [cast("list[Tick]", message["data"])[0] for message in messages]
    retrieved_df = cast("pl.LazyFrame", retrieved.data).collect()
    # every streamed tick was stored and comes back as it was streamed
    assert retrieved_df["date"].cast(pl.Int64).to_list() == [tick.ts for tick in ticks]
    assert retrieved_df["price"].to_list() == [tick.price for tick in ticks]
    assert retrieved_df["volume"].to_list() == [tick.volume for tick in ticks]
    assert set(retrieved_df["product"]) == {
        fake_source.create_product("BTC_USDT_PERP").name
    }
    # streamed data is stored in its env, not with the downloaded data
    assert backtest.success, backtest.errors
    assert backtest.data is None
