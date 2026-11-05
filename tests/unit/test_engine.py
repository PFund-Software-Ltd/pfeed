"""DataEngine, running real MarketFeeds that stream from fake stream APIs or replay data stored in a real ParquetIO."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from pathlib import Path

    from pfund_blocks.entities.products.product_base import BaseProduct
    from pfund_blocks.typing import Message
    from pytest_mock import MockerFixture, MockType

    from pfeed.base.requests.stream_request import BaseStreamRequest
    from pfeed.market.requests.replay_request import MarketFeedReplayRequest

import asyncio
import contextlib
import datetime
import threading

import polars as pl
import pytest
from pfund_blocks.datas.bar_data import Bar
from pfund_blocks.datas.tick_data import Tick
from pfund_blocks.resolution import Resolution

from pfeed.engine import DataEngine
from pfeed.enums import DataCategory
from pfeed.io.parquet_io import ParquetIO
from pfeed.market.feed import MarketFeed
from pfeed.source import BaseSource
from pfeed.streaming.stream_api import BaseStreamAPI

TICK, MINUTE = '1t', '1m'
NS = 1_700_000_000_000_000_000


class _StreamAPI(BaseStreamAPI):
    """Sends the given messages to the callback once connected, then disconnects, or stays connected if endless."""

    def __init__(self, messages: list[Message] | None = None, *, endless: bool = False, error: Exception | None = None):
        super().__init__(env=cast("Any", 'LIVE'))
        self.messages = messages or []
        self.endless = endless
        self.error = error
        self._task: asyncio.Task[None] | None = None

    def get_supported_resolutions(self, product: BaseProduct) -> list[Resolution]:
        return [Resolution(TICK), Resolution(MINUTE)]

    def add_channel(self, request: BaseStreamRequest) -> None:
        pass

    async def _run(self):
        assert self._callback is not None
        for message in self.messages:
            result = self._callback(message)
            if result is not None:
                await result
        if self.error:
            raise self.error
        if self.endless:
            await asyncio.Event().wait()

    async def connect(self) -> None:
        self._task = asyncio.create_task(self._run())

    async def wait_until_disconnected(self) -> None:
        if self._task is not None:
            await self._task

    async def disconnect(self) -> None:
        if self._task is not None:
            _ = self._task.cancel()
            _ = await asyncio.wait([self._task])


class _Feed(MarketFeed):
    # module-level so cloudpickle pickles _parse_streaming_message by reference into the Ray workers
    class Requests(MarketFeed.Requests):  # ty: ignore[invalid-attribute-override]
        download = None
        retrieve = None

    DataSource = BaseSource

    @staticmethod
    def _parse_streaming_message(msg: Message) -> Message:
        # NOTE: like a real feed, only parses the messages the stream API left unparsed, i.e. with Ray
        if msg['data'] is not None:
            return msg
        return {**msg, 'data': [_tick()], 'extra': {**msg['extra'], 'parsed': True}}


def _tick() -> Tick:
    return Tick(ts=NS, price=1.0, volume=1.0)


def _to_bar(msg: Message) -> Message:
    # module-level so cloudpickle pickles it by reference into the Ray workers
    bar = Bar(
        resolution=Resolution(MINUTE), start_ts=NS, end_ts=NS + 60, updated_ts=NS + 60,
        open=1.0, high=1.0, low=1.0, close=1.0, volume=1.0, is_closed=True,
    )
    return {**msg, 'data': [bar]}


def _tag_by_user(msg: Message) -> Message:
    # module-level so cloudpickle pickles it by reference into the Ray workers
    return {**msg, 'extra': {**msg['extra'], 'user': True}}


def _summary(msg: Message) -> str:
    """e.g. "user:parsed:0", the message's id, prefixed by the transformations it went through."""
    extra = msg['extra']
    return f"{'user:' if extra.get('user') else ''}{'parsed:' if extra.get('parsed') else ''}{msg['message']}"


def _source(mocker: MockerFixture, name: str, product: BaseProduct, stream_api: _StreamAPI) -> MockType:
    source = mocker.Mock(spec=BaseSource)
    source.name = name
    source.Feeds = {DataCategory.MARKET_DATA: _Feed}
    source.create_product.return_value = product
    source.get_stream_api.return_value = stream_api
    return source


def _stream_msg(product: BaseProduct, i: Any, *, parsed: bool = True) -> Message:
    return {
        'source': 'FAKE',
        'account': None,
        'channel': 'trades',
        'product': product.name,
        'resolution': repr(Resolution(TICK)),
        'received_at': 0,
        # NOTE: the message's id, the data is the same tick for every message
        'message': i,
        'data': [_tick()] if parsed else None,
        'error': None,
        'extra': {},
    }


def _add_streaming_feed(
    mocker: MockerFixture,
    engine: DataEngine,
    product: BaseProduct,
    stream_api: _StreamAPI,
    *,
    name: str = 'FAKE',
    num_workers: int | None = None,
    **stream_kwargs: Any,
) -> _Feed:
    """Like engine.add_feed(...).stream(...), with a feed of a fake source."""
    _ = mocker.patch('pfeed.base.get_feed', return_value=_Feed(_source(mocker, name, product, stream_api), pipeline_mode=True, num_workers=num_workers))
    feed = cast("_Feed", engine.add_feed(name))
    _ = feed.stream(str(product.basis), TICK, **stream_kwargs)
    _ = feed.transform(_tag_by_user)
    return feed


def _run_in_thread_and_end(engine: DataEngine, delay: float) -> threading.Timer:
    timer = threading.Timer(delay, engine.end)
    timer.start()
    return timer


# --- add_feed ---


def test_add_feed_sets_the_engine_on_the_feed(mocker: MockerFixture, product: BaseProduct):
    engine = DataEngine()
    feed = _add_streaming_feed(mocker, engine, product, _StreamAPI())
    assert engine.feeds == [feed]
    assert feed._engine is engine


def test_add_feed_without_stream_or_replay_raises(mocker: MockerFixture, product: BaseProduct):
    mocker.patch.object(_Feed.Requests, 'stream', None)
    mocker.patch.object(_Feed.Requests, 'replay', None)
    _ = mocker.patch('pfeed.base.get_feed', return_value=_Feed(_source(mocker, 'FAKE', product, _StreamAPI()), pipeline_mode=True))
    with pytest.raises(ValueError, match='supports neither stream'):
        _ = DataEngine().add_feed('FAKE')


# --- validation ---


def test_run_without_feeds_raises():
    with pytest.raises(RuntimeError, match='No feeds to run'):
        _ = DataEngine().run()


def test_run_with_a_feed_with_nothing_to_run_raises(mocker: MockerFixture, product: BaseProduct):
    engine = DataEngine()
    _ = mocker.patch('pfeed.base.get_feed', return_value=_Feed(_source(mocker, 'FAKE', product, _StreamAPI()), pipeline_mode=True))
    _ = engine.add_feed('FAKE')
    with pytest.raises(RuntimeError, match='have nothing to run'):
        _ = engine.run()


def test_run_with_feeds_of_different_envs_raises(mocker: MockerFixture, product: BaseProduct):
    engine = DataEngine()
    _ = _add_streaming_feed(mocker, engine, product, _StreamAPI(), name='FAKE', env='LIVE')
    _ = _add_streaming_feed(mocker, engine, product, _StreamAPI(), name='FAKE2', env='PAPER')
    with pytest.raises(ValueError, match='cannot run feeds of different envs'):
        _ = engine.run()


@pytest.mark.asyncio
async def test_run_inside_an_event_loop_raises():
    with pytest.raises(RuntimeError, match=r'Did you mean to call engine\.run_async'):
        _ = DataEngine().run()


# --- output ---


def test_outputs_after_the_transformations_go_to_the_callback(mocker: MockerFixture, product: BaseProduct):
    outputs: list[Any] = []
    raw: list[Any] = []
    engine = DataEngine(callback=lambda msg: outputs.append(_summary(msg)))
    feed1 = _add_streaming_feed(
        mocker, engine, product, _StreamAPI([_stream_msg(product, i) for i in range(2)]),
        name='FAKE', callback=lambda msg: raw.append(_summary(msg)),
    )
    feed2 = _add_streaming_feed(mocker, engine, product, _StreamAPI([_stream_msg(product, 'a')]), name='FAKE2')

    results = engine.run()

    assert list(results) == [feed1, feed2]
    assert all(result.success for result in results.values())
    # the feed's callback gets the messages before the transformations, the engine's after them
    assert raw == ['0', '1']
    assert sorted(outputs) == ['user:0', 'user:1', 'user:a']
    assert not engine.is_running()
    assert not feed1.is_running()


@pytest.mark.asyncio
async def test_async_callback_is_awaited(mocker: MockerFixture, product: BaseProduct):
    outputs: list[Any] = []

    async def callback(msg: Message) -> None:
        await asyncio.sleep(0)
        outputs.append(_summary(msg))

    engine = DataEngine(callback=callback)
    _ = _add_streaming_feed(mocker, engine, product, _StreamAPI([_stream_msg(product, i) for i in range(2)]))

    _ = await engine.run_async()

    assert outputs == ['user:0', 'user:1']


def test_failed_callback_skips_only_its_output(mocker: MockerFixture, product: BaseProduct):
    outputs: list[Any] = []

    def callback(msg: Message) -> None:
        if _summary(msg) == 'user:0':
            raise RuntimeError('callback failed')
        outputs.append(_summary(msg))

    engine = DataEngine(callback=callback)
    feed = _add_streaming_feed(mocker, engine, product, _StreamAPI([_stream_msg(product, i) for i in range(2)]))

    results = engine.run()

    assert results[feed].success
    assert outputs == ['user:1']


def test_outputs_are_published_on_their_dataflows_channel(mocker: MockerFixture, product: BaseProduct):
    import zmq

    engine = DataEngine()
    feed = _add_streaming_feed(mocker, engine, product, _StreamAPI([_stream_msg(product, i) for i in range(2)]))
    channel, topic = feed.dataflows[0].data_model.to_zmq_route()
    engine.setup_publishing()
    assert engine._msg_queue is not None
    xpub = engine._msg_queue.sender
    [port] = engine._msg_queue.get_ports_in_use(xpub)
    ctx = zmq.Context()
    sub = ctx.socket(zmq.SUB)
    try:
        sub.connect(f'tcp://localhost:{port}')
        sub.setsockopt(zmq.SUBSCRIBE, channel.encode())
        # NOTE: XPUB receives the subscription, then the SUB is known to get every message sent after it
        assert xpub.poll(5000)
        _ = xpub.recv()

        _ = engine.run()

        received = []
        while sub.poll(1000):
            received.append(sub.recv_multipart())
    finally:
        sub.close(linger=0)
        ctx.term()

    from msgspec import msgpack
    assert [(c.decode(), t.decode(), _summary(msgpack.decode(d))) for c, t, d, _ts in received] == [
        (channel, topic, 'user:0'), (channel, topic, 'user:1'),
    ]
    # the XPUB is closed when the run ends
    assert engine._msg_queue is None


@pytest.mark.asyncio
async def test_aiter_yields_every_output_until_the_feeds_end(mocker: MockerFixture, product: BaseProduct):
    engine = DataEngine()
    _ = _add_streaming_feed(mocker, engine, product, _StreamAPI([_stream_msg(product, i) for i in range(2)]), name='FAKE')
    _ = _add_streaming_feed(mocker, engine, product, _StreamAPI([_stream_msg(product, 'a')]), name='FAKE2')

    outputs = [_summary(msg) async for msg in engine]

    assert sorted(outputs) == ['user:0', 'user:1', 'user:a']
    assert engine._iter_queue is None


@pytest.mark.asyncio
async def test_aiter_closing_after_break_ends_the_feeds(mocker: MockerFixture, product: BaseProduct):
    engine = DataEngine()
    feeds = [
        _add_streaming_feed(mocker, engine, product, _StreamAPI([_stream_msg(product, i)], endless=True), name=name)
        for i, name in enumerate(['FAKE', 'FAKE2'])
    ]

    async with contextlib.aclosing(aiter(engine)) as outputs:
        async for _ in outputs:
            break

    assert not engine.is_running()
    assert not any(feed.is_running() for feed in feeds)


@pytest.mark.asyncio
async def test_aiter_full_queue_drops_the_oldest_output(mocker: MockerFixture, product: BaseProduct):
    mocker.patch.object(DataEngine, 'QUEUE_MAXSIZE', 2)
    engine = DataEngine()
    # NOTE: a sync stream API callback never yields, so every message is output before the consumer runs
    _ = _add_streaming_feed(mocker, engine, product, _StreamAPI([_stream_msg(product, i) for i in range(4)]))

    outputs = [_summary(msg) async for msg in engine]

    assert outputs[-2:] == ['user:2', 'user:3']
    assert len(outputs) < 4


# --- lifecycle ---


def test_failed_feed_doesnt_stop_the_others(mocker: MockerFixture, product: BaseProduct):
    outputs: list[Any] = []
    engine = DataEngine(callback=lambda msg: outputs.append(_summary(msg)))
    failing = _add_streaming_feed(mocker, engine, product, _StreamAPI(error=RuntimeError('stream failed')), name='FAKE')
    other = _add_streaming_feed(mocker, engine, product, _StreamAPI([_stream_msg(product, 'a')]), name='FAKE2')

    results = engine.run()

    assert not results[failing].success
    assert 'stream failed' in str(results[failing].errors[0])
    assert results[other].success
    assert outputs == ['user:a']


def test_feed_raising_gets_a_failed_result(mocker: MockerFixture, product: BaseProduct):
    engine = DataEngine()
    failing = _add_streaming_feed(mocker, engine, product, _StreamAPI(), name='FAKE')
    other = _add_streaming_feed(mocker, engine, product, _StreamAPI([_stream_msg(product, 'a')]), name='FAKE2')
    _ = mocker.patch.object(failing, 'run_async', side_effect=TimeoutError('workers not ready'))

    results = engine.run()

    assert not results[failing].success
    assert isinstance(results[failing].errors[0], TimeoutError)
    assert results[other].success


def test_failed_feed_ends_the_others_without_isolate_failures(mocker: MockerFixture, product: BaseProduct):
    engine = DataEngine(isolate_failures=False)
    failing = _add_streaming_feed(mocker, engine, product, _StreamAPI(error=RuntimeError('stream failed')), name='FAKE')
    endless = _add_streaming_feed(mocker, engine, product, _StreamAPI(endless=True), name='FAKE2')

    results = engine.run()

    assert not results[failing].success
    # ended, not failed
    assert results[endless].success
    assert not endless.is_running()


def test_end_from_another_thread_ends_the_feeds(mocker: MockerFixture, product: BaseProduct):
    engine = DataEngine()
    feeds = [
        _add_streaming_feed(mocker, engine, product, _StreamAPI([_stream_msg(product, i)], endless=True), name=name)
        for i, name in enumerate(['FAKE', 'FAKE2'])
    ]
    timer = _run_in_thread_and_end(engine, 0.2)

    results = engine.run()

    timer.join()
    assert list(results) == feeds
    assert all(result.success for result in results.values())
    assert not engine.is_running()


def test_end_when_not_running_does_nothing():
    DataEngine().end()


# --- replay ---


D1, D2 = datetime.date(2025, 1, 1), datetime.date(2025, 1, 2)
NS_PER_MINUTE = 60 * 1_000_000_000


def _ns(date: datetime.date, minutes: float = 0) -> int:
    midnight = datetime.datetime.combine(date, datetime.time(), tzinfo=datetime.UTC)
    return int(midnight.timestamp()) * 1_000_000_000 + int(minutes * NS_PER_MINUTE)


def _stored_ticks(product: BaseProduct, timestamps: list[int]) -> pl.DataFrame:
    n = len(timestamps)
    return pl.DataFrame({
        'date': pl.Series(timestamps, dtype=pl.Int64).cast(pl.Datetime('ns')),
        'product': [product.name] * n,
        'resolution': [str(Resolution(TICK))] * n,
        'price': [float(i) for i in range(n)],
        'volume': [1.0] * n,
        'side': pl.Series([1] * n, dtype=pl.Int8),
    })


def _add_replaying_feed(
    mocker: MockerFixture,
    engine: DataEngine,
    product: BaseProduct,
    name: str,
    tmp_path: Path,
    timestamps: list[int],
    **replay_kwargs: Any,
) -> _Feed:
    """Like engine.add_feed(...).replay(...) from D1 to D2, after storing ticks at the timestamps where replay() reads them."""
    _ = mocker.patch('pfeed.base.get_feed', return_value=_Feed(_source(mocker, name, product, _StreamAPI()), pipeline_mode=True))
    feed = cast("_Feed", engine.add_feed(name))
    _ = feed.replay(str(product.basis), TICK, start_date=D1, end_date=D2, io=ParquetIO(base_path=str(tmp_path / name)), **replay_kwargs)
    request = cast("MarketFeedReplayRequest", feed._requests[-1])
    stored = _stored_ticks(product, timestamps)
    for daily_data_model in request.to_data_model().split_by_date():
        daily = stored.filter(pl.col('date').dt.date() == daily_data_model.date)
        if not daily.is_empty():
            request.create_data_resolution_handler(daily_data_model).write_batch(daily)
    return feed


def test_replays_of_all_feeds_are_merged_by_time(mocker: MockerFixture, product: BaseProduct, tmp_path: Path):
    outputs: list[tuple[str, int]] = []
    engine = DataEngine(callback=lambda msg: outputs.append((msg['source'], msg['received_at'])))
    _ = _add_replaying_feed(mocker, engine, product, 'FAKE', tmp_path, [_ns(D1, 0), _ns(D1, 3), _ns(D2, 1)])
    _ = _add_replaying_feed(mocker, engine, product, 'FAKE2', tmp_path, [_ns(D1, 1), _ns(D1, 3), _ns(D2, 0)])

    results = engine.run()

    assert all(result.success for result in results.values())
    assert outputs == [
        ('FAKE', _ns(D1, 0)),
        ('FAKE2', _ns(D1, 1)),
        # received at the same time, so in the order the feeds were added
        ('FAKE', _ns(D1, 3)),
        ('FAKE2', _ns(D1, 3)),
        ('FAKE2', _ns(D2, 0)),
        ('FAKE', _ns(D2, 1)),
    ]
    assert engine._replay_clock is None


def test_replays_with_different_paces_raise(mocker: MockerFixture, product: BaseProduct, tmp_path: Path):
    engine = DataEngine()
    _ = _add_replaying_feed(mocker, engine, product, 'FAKE', tmp_path, [], replay_pace=0)
    _ = _add_replaying_feed(mocker, engine, product, 'FAKE2', tmp_path, [], replay_pace=1)
    with pytest.raises(ValueError, match='share one pace'):
        _ = engine.run()


# --- Ray ---


def test_ray_and_non_ray_feeds_both_reach_the_engine(mocker: MockerFixture, product: BaseProduct):
    pytest.importorskip('ray')
    outputs: list[Any] = []
    engine = DataEngine(callback=lambda msg: outputs.append(_summary(msg)))
    ray_feed = _add_streaming_feed(
        mocker, engine, product, _StreamAPI([_stream_msg(product, i, parsed=False) for i in range(3)]), name='FAKE', num_workers=1,
    )
    feed = _add_streaming_feed(mocker, engine, product, _StreamAPI([_stream_msg(product, 'a')]), name='FAKE2')

    results = engine.run()

    assert results[ray_feed].success
    assert results[feed].success
    # the worker's STOP signal is not an output
    assert sorted(outputs) == ['user:a', 'user:parsed:0', 'user:parsed:1', 'user:parsed:2']
    assert engine._worker_msg_queue is None
    assert engine._worker_thread is None


def test_ray_outputs_data_points_reach_the_engine_as_structs(mocker: MockerFixture, product: BaseProduct):
    pytest.importorskip('ray')
    outputs: list[Message] = []
    engine = DataEngine(callback=outputs.append)
    ray_feed = _add_streaming_feed(
        mocker, engine, product, _StreamAPI([_stream_msg(product, i, parsed=False) for i in range(2)]), num_workers=1,
    )
    _ = ray_feed.transform(_to_bar)

    results = engine.run()

    assert results[ray_feed].success
    # e.g. a Bar and its Resolution come back as a Bar, not a dict, like without Ray
    assert [(type(bar), bar.resolution, bar.close) for msg in outputs for bar in msg['data'] or []] == [(Bar, Resolution(MINUTE), 1.0)] * 2
