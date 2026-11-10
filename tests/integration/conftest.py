from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Literal, cast

if TYPE_CHECKING:
    from pathlib import Path

    from pfund_blocks.entities.products.product_base import BaseProduct
    from pfund_blocks.entities.products.product_basis import ProductBasis
    from pfund_blocks.typing import Message

    from pfeed.base.feed import BaseFeed
    from pfeed.base.requests.stream_request import BaseStreamRequest
    from pfeed.io.base_io import BaseIO
    from pfeed.market.data_model import MarketDataModel
    from pfeed.market.requests.base_request import MarketFeedBaseRequest
    from pfeed.market.requests.stream_request import MarketFeedStreamRequest

import asyncio
import datetime

import polars as pl
import pytest
from pfund_blocks.datas.tick_data import Tick
from pfund_blocks.enums.side import Side
from pfund_blocks.resolution import Resolution
from pfund_kit.utils.temporal import get_utc_now

import pfeed as pe
from pfeed.enums import DataAccessType, DataCategory, DataProviderType, DataType
from pfeed.io.ducklake_io import DuckLakeIO
from pfeed.io.parquet_io import ParquetIO
from pfeed.source import BaseSource, SourceMetadata
from pfeed.streaming.stream_api import BaseStreamAPI

TICK_INTERVAL = datetime.timedelta(hours=6)
NUM_STREAMED_TICKS = 3


class FakeStreamAPI(BaseStreamAPI):
    """Streams NUM_STREAMED_TICKS ticks per channel, one second apart from today's midnight (UTC), then disconnects."""

    def __init__(self, env: Any):
        super().__init__(env=env)
        self._requests: list[MarketFeedStreamRequest] = []
        self._task: asyncio.Task[None] | None = None

    def get_supported_resolutions(self, product: BaseProduct) -> list[Resolution]:
        return [Resolution("1t")]

    def add_channel(self, request: BaseStreamRequest) -> None:
        self._requests.append(cast("MarketFeedStreamRequest", request))

    async def _run(self) -> None:
        assert self._callback is not None
        midnight = datetime.datetime.combine(
            get_utc_now().date(), datetime.time(), tzinfo=datetime.UTC
        )
        midnight_ns = int(midnight.timestamp()) * 1_000_000_000
        for request in self._requests:
            for i in range(NUM_STREAMED_TICKS):
                ts = midnight_ns + i * 1_000_000_000
                message: Message = {
                    "source": "FAKE",
                    "account": None,
                    "channel": "trades",
                    "product": request.product.name,
                    "resolution": repr(request.data_resolution),
                    "received_at": ts,
                    "message": {"i": i},
                    "data": [Tick(ts=ts, price=100.0 + i, volume=1.0, side=Side.BUY)],
                    "error": None,
                    "extra": {},
                }
                if (result := self._callback(message)) is not None:
                    await result

    async def connect(self) -> None:
        self._task = asyncio.create_task(self._run())

    async def wait_until_disconnected(self) -> None:
        if self._task is not None:
            await self._task

    async def disconnect(self) -> None:
        if self._task is not None:
            _ = self._task.cancel()
            _ = await asyncio.wait([self._task])


class FakeMarketFeed[PipelineT: bool = Literal[False]](
    pe.MarketFeed[
        "FakeSource[PipelineT]", "MarketFeedBaseRequest", "MarketDataModel", PipelineT
    ]
):
    downloaded_data_date_cols: ClassVar[list[str]] = ["ts"]
    download_dataflow_per_date: ClassVar[bool] = True

    @staticmethod
    def _normalize_downloaded_data(df: pl.LazyFrame) -> pl.LazyFrame:
        # the fake vendor already ships pfeed's columns and dtypes
        return df

    @staticmethod
    def _download_impl(
        request: MarketFeedBaseRequest, data_model: MarketDataModel
    ) -> pl.LazyFrame | None:
        date: datetime.date = data_model.start_date
        ticks = pl.datetime_range(
            date,
            date + datetime.timedelta(days=1),
            TICK_INTERVAL,
            closed="left",
            eager=True,
        )
        day = (
            date.toordinal()
        )  # distinct values per day, so rows from different dates can't be confused
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

    @staticmethod
    def _parse_streaming_message(msg: Message) -> Message:
        # the fake stream API sends parsed messages
        return msg


class FakeSource[PipelineT: bool = Literal[False]](BaseSource[PipelineT]):
    """Deterministic in-memory vendor: one tick every TICK_INTERVAL per date."""

    Feeds: ClassVar[dict[DataCategory, type[BaseFeed]]] = {
        DataCategory.MARKET_DATA: FakeMarketFeed
    }
    metadata: ClassVar[SourceMetadata] = SourceMetadata(
        name="FAKE",
        url="https://example.com",
        data_categories={DataCategory.MARKET_DATA: {DataType.TICK: ["PERPETUAL"]}},
        provider_type=DataProviderType.VENUE,
        access_type=DataAccessType.FREE,
    )
    market_feed: FakeMarketFeed[PipelineT]
    _stream_api: FakeStreamAPI | None = None

    @classmethod
    def create_product(
        cls, basis: str | ProductBasis, symbol: str = "", name: str = "", **specs: Any
    ) -> BaseProduct:
        # symbol must be set, the fake source has no adapter to derive it
        return super().create_product(
            basis, symbol=symbol or str(basis), name=name, **specs
        )

    def get_stream_api(self, env: Any) -> FakeStreamAPI:
        # one stream API per source, the feed adds its channels and connects to the same one
        if self._stream_api is None:
            self._stream_api = FakeStreamAPI(env)
        return self._stream_api


@pytest.fixture
def fake_source() -> FakeSource:
    return FakeSource()


@pytest.fixture
def fake_pipeline_source() -> FakeSource[Literal[True]]:
    return FakeSource(pipeline_mode=True)


@pytest.fixture
def tick_interval() -> datetime.timedelta:
    return TICK_INTERVAL


@pytest.fixture(params=[ParquetIO, DuckLakeIO], ids=lambda c: c.__name__)
def io(request: pytest.FixtureRequest, tmp_path: Path) -> BaseIO:
    # always pass io explicitly: retrieve(io=None) falls back to ParquetIO() under the real data_path
    return request.param(base_path=str(tmp_path))
