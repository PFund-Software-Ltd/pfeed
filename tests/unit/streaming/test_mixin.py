"""StreamingMixin, through a real MarketFeed streaming from a fake stream API.

The data engine part (pushing the Ray workers' transformed data to the engine) is not covered here.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Generator
    from pathlib import Path

    from pfund_blocks.entities.products.product_base import BaseProduct
    from pfund_blocks.typing import Message
    from pytest_mock import MockerFixture, MockType

    from pfeed.base.requests.stream_request import BaseStreamRequest
    from pfeed.dataflow.dataflow import DataFlow

import asyncio
import contextlib
import json
import logging
from types import SimpleNamespace

import pytest
from pfund_blocks.resolution import Resolution

from pfeed.dataflow.result import RunResult
from pfeed.enums import DataCategory
from pfeed.io.base_io import IOCapabilities
from pfeed.io.parquet_io import ParquetIO
from pfeed.market.feed import MarketFeed
from pfeed.source import BaseSource
from pfeed.streaming.market import MarketFeedStreamingMixin
from pfeed.streaming.stream_api import BaseStreamAPI

TICK, MINUTE = '1t', '1m'


class _StreamAPI(BaseStreamAPI):
    """Sends the given messages to the callback once connected, then disconnects, or stays connected if endless."""

    def __init__(self, messages: list[Message] | None = None, *, endless: bool = False, error: Exception | None = None):
        super().__init__(env=cast("Any", 'LIVE'))
        self.messages = messages or []
        self.endless = endless
        self.error = error
        self.channels: list[BaseStreamRequest] = []
        self.skip_parse_message: bool | None = None
        self.is_disconnected = False
        self._task: asyncio.Task[None] | None = None

    def get_supported_resolutions(self, product: BaseProduct) -> list[Resolution]:
        return [Resolution(TICK), Resolution(MINUTE)]

    def add_channel(self, request: BaseStreamRequest) -> None:
        self.channels.append(request)

    def set_callback(self, callback: Callable[[Message], Awaitable[None] | None], *, skip_parse_message: bool = False) -> None:
        super().set_callback(callback, skip_parse_message=skip_parse_message)
        self.skip_parse_message = skip_parse_message

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
        self.is_disconnected = True


class _Feed(MarketFeedStreamingMixin, MarketFeed):
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
        return {**msg, 'data': f"parsed:{msg['message']}"}


class _Handler:
    """Stand-in for a data handler, buffers the messages' data and writes them to a JSON file on flush().

    A file, so that what a Ray worker's copy flushes can be checked from the main process.
    """

    def __init__(self, path: Path, *, fail_write: bool = False, fail_flush: bool = False):
        self.io: Any = SimpleNamespace(CAPABILITIES=ParquetIO.CAPABILITIES)
        self.path = path
        self.fail_write = fail_write
        self.fail_flush = fail_flush
        self.rows: list[Any] = []

    def write_stream(self, msg: Message) -> None:
        if self.fail_write:
            raise RuntimeError('write failed')
        self.rows.append(msg['data'])

    def flush(self) -> None:
        if not self.rows:
            return
        if self.fail_flush:
            raise RuntimeError('flush failed')
        existing = json.loads(self.path.read_text()) if self.path.exists() else []
        self.path.write_text(json.dumps(existing + self.rows))
        self.rows.clear()

    def written(self) -> list[Any]:
        return json.loads(self.path.read_text()) if self.path.exists() else []


@pytest.fixture
def stream_api() -> _StreamAPI:
    return _StreamAPI()


@pytest.fixture
def source(mocker: MockerFixture, product: BaseProduct, stream_api: _StreamAPI) -> MockType:
    source = mocker.Mock(spec=BaseSource)
    source.name = 'FAKE'
    source.Feeds = {DataCategory.MARKET_DATA: _Feed}
    source.create_product.return_value = product
    source.get_stream_api.return_value = stream_api
    return source


@pytest.fixture
def feed(source: MockType) -> _Feed:
    return _Feed(source, pipeline_mode=True)


@contextlib.contextmanager
def _capture_feed_logs(caplog: pytest.LogCaptureFixture) -> Generator[None]:
    """Captures the feed logger's records with caplog.

    pfeed's logging doesn't propagate to the root logger where caplog listens, so the handler is attached to the feed's logger;
    after stream(), since stream() sets up pfeed's logging again, which removes it.
    """
    logger = logging.getLogger('pfeed.fake')
    logger.addHandler(caplog.handler)
    try:
        yield
    finally:
        logger.removeHandler(caplog.handler)


def _stream_msg(product: BaseProduct, resolution: str, i: int, *, parsed: bool = True) -> Message:
    return {
        'source': 'FAKE',
        'account': None,
        'channel': 'trades',
        'product': product.name,
        'resolution': repr(Resolution(resolution)),
        'received_at': i,
        'message': i,
        'data': i if parsed else None,
        'error': None,
    }


def _record_into(received: list[Any]) -> Callable[[Message], Message]:
    def record(msg: Message) -> Message:
        received.append(msg['data'])
        return msg
    return record


def _stream(feed: _Feed, product: BaseProduct, resolution: str = TICK, **kwargs: Any) -> DataFlow:
    _ = feed.stream(str(product.basis), resolution, **kwargs)
    return feed.dataflows[-1]


# --- __aiter__ ---


def _error_msg(i: int) -> Message:
    """A message for no dataflow, so only the callback and __aiter__() get it."""
    return {
        'source': 'FAKE',
        'account': None,
        'channel': 'trades',
        'product': None,
        'resolution': None,
        'received_at': i,
        'message': i,
        'data': i,
        'error': 'rejected',
    }


@pytest.mark.asyncio
async def test_aiter_yields_every_message_until_the_streams_end(feed: _Feed, product: BaseProduct, stream_api: _StreamAPI):
    stream_api.messages = [_stream_msg(product, TICK, i) for i in range(3)]
    received: list[Any] = []
    _ = _stream(feed, product, callback=lambda msg: received.append(msg['data']))
    assert [msg['data'] async for msg in feed] == [0, 1, 2]
    # the callback still gets every message
    assert received == [0, 1, 2]
    assert stream_api.is_disconnected
    assert feed._iter_queue is None
    assert not feed.is_running()


@pytest.mark.asyncio
async def test_aiter_without_streams_raises(feed: _Feed):
    with pytest.raises(RuntimeError, match='No streaming dataflow to iterate over'):
        _ = [msg async for msg in feed]


@pytest.mark.asyncio
async def test_aiter_reraises_the_runs_error(feed: _Feed, product: BaseProduct, tmp_path: Path):
    feed.set_num_workers(1)
    dataflow = _stream(feed, product)
    handler = _Handler(tmp_path / 'rows.json')
    handler.io = SimpleNamespace(CAPABILITIES=IOCapabilities(append=True, concurrent_partition_writes=False))
    dataflow._handler = cast("Any", handler)
    with pytest.raises(RuntimeError, match='does not support concurrent partition writes'):
        _ = [msg async for msg in feed]
    assert feed._iter_queue is None


@pytest.mark.asyncio
async def test_aiter_full_queue_drops_the_oldest_message(
    mocker: MockerFixture, feed: _Feed, product: BaseProduct, stream_api: _StreamAPI, caplog: pytest.LogCaptureFixture
):
    mocker.patch.object(_Feed, 'QUEUE_MAXSIZE', 2)
    # NOTE: the stream API sends all messages before the consumer gets the first one, since _callback() doesn't yield control for them
    stream_api.messages = [_error_msg(i) for i in range(3)]
    _ = _stream(feed, product)
    with _capture_feed_logs(caplog):
        assert [msg['data'] async for msg in feed] == [1, 2]
    assert 'iteration queue is full, dropping the oldest message' in caplog.text


@pytest.mark.asyncio
async def test_aiter_closing_after_break_ends_the_streams(feed: _Feed, product: BaseProduct, stream_api: _StreamAPI):
    stream_api.messages = [_stream_msg(product, TICK, i) for i in range(3)]
    stream_api.endless = True
    _ = _stream(feed, product)
    async with contextlib.aclosing(aiter(feed)) as messages:
        async for msg in messages:
            assert msg['data'] == 0
            break
        assert not stream_api.is_disconnected
    assert stream_api.is_disconnected
    assert feed._iter_queue is None
    assert not feed.is_running()


@pytest.mark.asyncio
async def test_aiter_cancelling_the_consumer_ends_the_streams_and_propagates(
    feed: _Feed, product: BaseProduct, stream_api: _StreamAPI
):
    stream_api.messages = [_stream_msg(product, TICK, 0)]
    stream_api.endless = True
    _ = _stream(feed, product)
    first_received = asyncio.Event()

    async def consume():
        async for _ in feed:
            first_received.set()

    task = asyncio.create_task(consume())
    await first_received.wait()
    _ = task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert stream_api.is_disconnected
    assert feed._iter_queue is None
    assert not feed.is_running()


# --- setting up stream dataflows ---


def test_stream_sets_up_the_stream_api(feed: _Feed, product: BaseProduct, stream_api: _StreamAPI):
    _ = _stream(feed, product)
    [request] = feed._requests
    assert stream_api.channels == [request]
    assert stream_api._callback == feed._callback
    assert stream_api.skip_parse_message is False
    assert list(feed._queues) == [(product.name, repr(Resolution(TICK)))]


def test_stream_with_ray_leaves_parsing_to_the_workers(feed: _Feed, product: BaseProduct, stream_api: _StreamAPI):
    feed.set_num_workers(1)
    _ = _stream(feed, product)
    assert stream_api.skip_parse_message is True


def test_streams_share_one_faucet(feed: _Feed, product: BaseProduct):
    tick_dataflow = _stream(feed, product, TICK)
    minute_dataflow = _stream(feed, product, MINUTE)
    assert tick_dataflow.faucet is minute_dataflow.faucet
    assert len(feed._queues) == 2


def test_streaming_the_same_product_and_resolution_twice_raises(feed: _Feed, product: BaseProduct):
    _ = _stream(feed, product)
    with pytest.raises(ValueError, match='is already being streamed'):
        _ = _stream(feed, product)


def test_streaming_different_envs_in_one_feed_raises(feed: _Feed, product: BaseProduct):
    _ = _stream(feed, product, TICK, env='LIVE')
    with pytest.raises(ValueError, match='cannot stream PAPER and LIVE in the same feed'):
        _ = _stream(feed, product, MINUTE, env='PAPER')


def test_same_callback_for_every_stream_is_allowed(feed: _Feed, product: BaseProduct):
    def callback(msg: Message) -> None: ...

    _ = _stream(feed, product, TICK, callback=callback)
    _ = _stream(feed, product, MINUTE, callback=callback)
    assert feed._user_callback is callback


def test_different_callbacks_raise(feed: _Feed, product: BaseProduct):
    def callback(msg: Message) -> None: ...

    def other_callback(msg: Message) -> None: ...

    _ = _stream(feed, product, TICK, callback=callback)
    with pytest.raises(ValueError, match='streaming callback is already set') as exc_info:
        _ = _stream(feed, product, MINUTE, callback=other_callback)
    assert 'lambda functions are always distinct objects' not in str(exc_info.value)


def test_different_lambda_callbacks_raise_with_a_hint(feed: _Feed, product: BaseProduct):
    _ = _stream(feed, product, TICK, callback=lambda msg: None)
    with pytest.raises(ValueError, match='lambda functions are always distinct objects'):
        _ = _stream(feed, product, MINUTE, callback=lambda msg: None)


# --- running stream dataflows ---


def test_stream_runs_until_the_stream_ends(source: MockType, product: BaseProduct, stream_api: _StreamAPI):
    stream_api.messages = [_stream_msg(product, TICK, i) for i in range(3)]
    received: list[Any] = []
    feed = _Feed(source)

    result = feed.stream(str(product.basis), TICK, callback=lambda msg: received.append(msg['data']))

    assert isinstance(result, RunResult)
    assert result.success
    assert result.data is None
    [dataflow] = result.dataflows
    assert dataflow.result.success
    assert received == [0, 1, 2]
    assert stream_api.is_disconnected
    # the run's state is reset
    assert feed._queues == {}
    assert feed._user_callback is None
    assert feed._requests == []
    assert not feed.is_running()


@pytest.mark.asyncio
async def test_async_callback_is_awaited(feed: _Feed, product: BaseProduct, stream_api: _StreamAPI):
    stream_api.messages = [_stream_msg(product, TICK, i) for i in range(3)]
    received: list[Any] = []

    async def callback(msg: Message) -> None:
        await asyncio.sleep(0)
        received.append(msg['data'])

    _ = _stream(feed, product, callback=callback)
    result = await feed.run_async()
    assert result.success
    assert received == [0, 1, 2]


def test_messages_go_to_their_own_dataflow(feed: _Feed, product: BaseProduct, stream_api: _StreamAPI):
    stream_api.messages = [
        _stream_msg(product, TICK, 0),
        _stream_msg(product, MINUTE, 1),
        _error_msg(9),
        _stream_msg(product, TICK, 2),
    ]
    callback_received: list[Any] = []
    tick_received: list[Any] = []
    minute_received: list[Any] = []
    callback = _record_into(callback_received)
    _ = _stream(feed, product, TICK, callback=callback)
    _ = feed.transform(_record_into(tick_received))
    _ = _stream(feed, product, MINUTE, callback=callback)
    _ = feed.transform(_record_into(minute_received))

    result = feed.run()

    assert result.success
    # the callback gets every message, including the error message which has no dataflow
    assert callback_received == [0, 1, 9, 2]
    assert tick_received == [0, 2]
    assert minute_received == [1]


def test_transformed_messages_are_loaded(feed: _Feed, product: BaseProduct, stream_api: _StreamAPI, tmp_path: Path):
    stream_api.messages = [_stream_msg(product, TICK, i) for i in range(3)]
    dataflow = _stream(feed, product)
    handler = _Handler(tmp_path / 'rows.json')
    dataflow._handler = cast("Any", handler)

    def double(msg: Message) -> Message:
        return {**msg, 'data': cast("int", msg['data']) * 2}

    _ = feed.transform(double)
    result = feed.run()

    assert result.success
    # end_stream() flushes what's still buffered
    assert handler.written() == [0, 2, 4]


def test_failed_transformation_skips_the_message(
    feed: _Feed, product: BaseProduct, stream_api: _StreamAPI, tmp_path: Path, caplog: pytest.LogCaptureFixture
):
    stream_api.messages = [_stream_msg(product, TICK, i) for i in range(3)]
    dataflow = _stream(feed, product)
    handler = _Handler(tmp_path / 'rows.json')
    dataflow._handler = cast("Any", handler)

    def fail_on_one(msg: Message) -> Message:
        if msg['data'] == 1:
            raise ValueError('bad message')
        return msg

    _ = feed.transform(fail_on_one)
    with _capture_feed_logs(caplog):
        result = feed.run()

    assert result.success
    assert handler.written() == [0, 2]
    assert 'transformation failed, skipping message' in caplog.text


def test_failed_dataflow_doesnt_stop_the_others(
    feed: _Feed, product: BaseProduct, stream_api: _StreamAPI, tmp_path: Path, caplog: pytest.LogCaptureFixture
):
    stream_api.messages = [_stream_msg(product, resolution, i) for i in range(3) for resolution in (TICK, MINUTE)]
    tick_dataflow = _stream(feed, product, TICK)
    tick_dataflow._handler = cast("Any", _Handler(tmp_path / 'tick.json', fail_write=True))
    minute_dataflow = _stream(feed, product, MINUTE)
    minute_handler = _Handler(tmp_path / 'minute.json')
    minute_dataflow._handler = cast("Any", minute_handler)

    with _capture_feed_logs(caplog):
        result = feed.run()

    assert not result.success
    assert not tick_dataflow.result.success
    assert minute_dataflow.result.success
    assert minute_handler.written() == [0, 1, 2]
    assert 'FAKE has 1 failed dataflows' in caplog.text


def test_stream_api_error_fails_every_dataflow(feed: _Feed, product: BaseProduct, stream_api: _StreamAPI):
    stream_api.messages = [_stream_msg(product, TICK, 0)]
    stream_api.error = ConnectionError('connection lost')
    tick_dataflow = _stream(feed, product, TICK)
    minute_dataflow = _stream(feed, product, MINUTE)

    result = feed.run()

    assert not result.success
    for dataflow in (tick_dataflow, minute_dataflow):
        assert isinstance(dataflow.result.error, ConnectionError)
    assert stream_api.is_disconnected
    assert not feed.is_running()


@pytest.mark.asyncio
async def test_cancelling_the_run_ends_the_streams(feed: _Feed, product: BaseProduct, stream_api: _StreamAPI, tmp_path: Path):
    stream_api.messages = [_stream_msg(product, TICK, i) for i in range(3)]
    stream_api.endless = True
    dataflow = _stream(feed, product)
    handler = _Handler(tmp_path / 'rows.json')
    dataflow._handler = cast("Any", handler)

    task = asyncio.create_task(feed.run_async())
    while len(handler.rows) < 3:
        await asyncio.sleep(0)
    _ = task.cancel()
    result = await task

    # cancellation is a graceful shutdown, not a failure
    assert result.success
    assert stream_api.is_disconnected
    assert handler.written() == [0, 1, 2]
    assert not feed.is_running()


@pytest.mark.asyncio
async def test_run_inside_an_event_loop_raises(feed: _Feed, product: BaseProduct):
    _ = _stream(feed, product)
    with pytest.raises(RuntimeError, match=r'Cannot call feed.run\(\) from within a running event loop'):
        feed.run()


@pytest.mark.asyncio
async def test_run_async_without_streams_raises(feed: _Feed):
    with pytest.raises(RuntimeError, match=r'run_async\(\) is only for streaming dataflows'):
        await feed.run_async()


# --- running stream dataflows with Ray ---


@pytest.mark.parametrize('num_workers', [1, 2])
def test_ray_workers_transform_and_flush(
    feed: _Feed, product: BaseProduct, stream_api: _StreamAPI, tmp_path: Path, num_workers: int
):
    pytest.importorskip('ray')
    stream_api.messages = [
        _stream_msg(product, resolution, i, parsed=False) for i in range(3) for resolution in (TICK, MINUTE)
    ]
    received: list[Any] = []
    feed.set_num_workers(num_workers)
    tick_dataflow = _stream(feed, product, TICK, callback=lambda msg: received.append(msg['data']))
    tick_handler = _Handler(tmp_path / 'tick.json')
    tick_dataflow._handler = cast("Any", tick_handler)
    minute_dataflow = _stream(feed, product, MINUTE)
    minute_handler = _Handler(tmp_path / 'minute.json')
    minute_dataflow._handler = cast("Any", minute_handler)

    result = feed.run()

    assert result.success
    # the workers parse the messages, the callback gets them unparsed
    assert received == [None] * 6
    # the workers' copies of the handlers are flushed when the workers stop
    assert tick_handler.written() == minute_handler.written() == ['parsed:0', 'parsed:1', 'parsed:2']
    assert not feed.is_running()


def test_ray_worker_failed_flush_fails_its_dataflows(
    feed: _Feed, product: BaseProduct, stream_api: _StreamAPI, tmp_path: Path
):
    pytest.importorskip('ray')
    stream_api.messages = [_stream_msg(product, TICK, i, parsed=False) for i in range(3)]
    feed.set_num_workers(1)
    dataflow = _stream(feed, product)
    # NOTE: the main process's copy never gets any rows with Ray, so only the worker's copy raises
    dataflow._handler = cast("Any", _Handler(tmp_path / 'rows.json', fail_flush=True))

    result = feed.run()

    assert not result.success
    assert 'flush failed' in str(dataflow.result.error)


def test_ray_worker_not_stopping_in_time_fails_its_dataflows(
    mocker: MockerFixture, feed: _Feed, product: BaseProduct, stream_api: _StreamAPI
):
    pytest.importorskip('ray')
    import ray

    stream_api.messages = [_stream_msg(product, TICK, 0, parsed=False)]
    feed.set_num_workers(1)
    dataflow = _stream(feed, product)
    # as if the worker were still flushing when the wait timed out
    mocker.patch.object(ray, 'wait', return_value=([], []))

    result = feed.run()

    assert not result.success
    assert isinstance(dataflow.result.error, TimeoutError)
    assert 'did not stop within' in str(dataflow.result.error)
