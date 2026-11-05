from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, cast

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Callable

    from pfund_blocks.typing import Message

    from pfeed.base.feed import BaseFeed
    from pfeed.dataflow.dataflow import DataFlow
    from pfeed.dataflow.result import RunResult
    from pfeed.streaming.mixin import StreamingMixin
    from pfeed.streaming.replay_clock import ReplayClock
    from pfeed.streaming.zeromq import ZeroMQ, ZeroMQChannel, ZeroMQTopic

    type StreamingFeed = StreamingMixin[Any, Any]
    type Output = tuple[ZeroMQChannel, ZeroMQTopic, Message]

import asyncio
import contextlib
import inspect
import logging
import threading

from pfund_blocks.enums.env import Environment

from pfeed.enums import DataCategory


# TODO: backfilling, e.g. a BackfillCoordinator the engine creates and runs alongside the feeds, see pfeed/batch_api.py
class DataEngine:
    """Runs the streams or replays of several feeds together and sends out their output, e.g. for pfund.

    The output is each streamed message after the feeds' transformations (e.g. a resampled bar), with or without Ray, sent to:
    - the callback, if given
    - the iteration, i.e. `async for data in engine`
    - the subscribers of the engine's XPUB, if setup_publishing() was called
    """

    QUEUE_MAXSIZE: ClassVar[int] = 1000

    def __init__(
        self,
        callback: Callable[[Message], Awaitable[None] | None] | None = None,
        isolate_failures: bool = True,
    ):
        """
        Args:
            callback: Sync or async callable called with each output, i.e. a message after the feeds' transformations.
                It is awaited before the next output, so a slow callback delays every feed, offload heavy work, e.g. to a task or a thread.
            isolate_failures: If True (default), a failed feed ends alone, the others keep running.
                If False, the first failed feed ends all the others.
        """
        self._logger = logging.getLogger("pfeed")
        self._callback = callback
        self._isolate_failures = isolate_failures
        self._feeds: list[StreamingFeed] = []
        self._is_running = False
        # XPUB publishing the output to other processes, only set up by setup_publishing()
        self._msg_queue: ZeroMQ | None = None
        # PULL receiving the Ray workers' output, set up by run_async() when a feed uses Ray
        self._worker_msg_queue: ZeroMQ | None = None
        self._worker_thread: threading.Thread | None = None
        self._stop_worker_thread = threading.Event()
        # NOTE: only set while replaying, merges the replays of all feeds by time
        self._replay_clock: ReplayClock | None = None
        # NOTE: only set while iterating the engine with __aiter__()
        self._iter_queue: asyncio.Queue[Message] | None = None
        self._zmq_routes: dict[DataFlow[Any, Any], tuple[ZeroMQChannel, ZeroMQTopic]] = {}
        # NOTE: only set while running, so that end() can cancel the feeds from another thread
        self._loop: asyncio.AbstractEventLoop | None = None
        self._feed_tasks: list[asyncio.Task[RunResult]] = []

    def is_running(self) -> bool:
        return self._is_running

    @property
    def feeds(self) -> list[BaseFeed[Any, Any, Any, Any]]:
        return cast("list[BaseFeed[Any, Any, Any, Any]]", self._feeds)

    def add_feed(
        self,
        data_source: str,
        data_category: DataCategory | str = DataCategory.MARKET_DATA,
        num_workers: int | None = None,
    ) -> BaseFeed[Any, Any, Any, Any]:
        """Creates a feed in pipeline mode, to call stream() or replay() on, run by the engine.

        Args:
            num_workers: Number of Ray workers for the feed's streams, None to run them without Ray.
                Feeds with and without Ray can be mixed.
        """
        from pfeed.base import get_feed
        from pfeed.streaming.mixin import StreamingMixin

        if self.is_running():
            raise RuntimeError("cannot add a feed while the data engine is running")
        feed: BaseFeed[Any, Any, Any, Any] = get_feed(data_source, data_category, pipeline_mode=True, num_workers=num_workers)
        if not isinstance(feed, StreamingMixin) or not (feed._supports("stream") or feed._supports("replay")):
            raise ValueError(f"{feed.name} supports neither stream() nor replay(), the data engine only runs streams and replays")
        feed._set_engine(self)
        self._feeds.append(feed)
        return feed

    def setup_publishing(
        self,
        zmq_url: str | None = None,
        zmq_sender_port: int | None = None,
        io_threads: int = 1,
    ) -> None:
        """Sets up the XPUB publishing the output to other processes, e.g. pfund's.

        Each output is sent on its dataflow's channel, see BaseDataModel.to_zmq_route().
        The XPUB is closed when the run ends.

        Args:
            zmq_url: e.g. "tcp://localhost" (default).
            zmq_sender_port: The XPUB's port, a random free one if None, see `engine._msg_queue.get_ports_in_use()`.
        """
        import zmq

        from pfeed.streaming.zeromq import ZeroMQ

        if self._msg_queue is not None:
            raise RuntimeError("Publishing is already set up, cannot set it up again")
        self._msg_queue = ZeroMQ(
            name="data_engine",
            logger=self._logger,
            io_threads=io_threads,
            sender_type=zmq.XPUB,
        )
        self._msg_queue.bind(self._msg_queue.sender, port=zmq_sender_port, url=zmq_url or ZeroMQ.DEFAULT_URL)

    def _get_worker_ports(self) -> list[int]:
        """The ports of the PULL the Ray workers push their output to."""
        assert self._worker_msg_queue is not None, "the data engine's PULL for the Ray workers is not set up"
        return self._worker_msg_queue.get_ports_in_use(self._worker_msg_queue.receiver)

    def _validate_feeds(self) -> None:
        if not self._feeds:
            raise RuntimeError("No feeds to run, add one with add_feed()")
        if idle := [feed.name for feed in self._feeds if not feed.is_streaming()]:
            raise RuntimeError(f"{idle} have nothing to run, call stream() or replay() on them first")
        envs = {feed: {request.env for request in feed._requests} for feed in self._feeds}
        if len(set().union(*envs.values())) > 1:
            details = ", ".join(f"{feed.name}={sorted(feed_envs)}" for feed, feed_envs in envs.items())
            raise ValueError(f"cannot run feeds of different envs in one data engine: {details}")
        if self._is_replaying():
            replay_paces = {request.replay_pace for feed in self._feeds for request in feed._requests}
            if len(replay_paces) > 1:
                raise ValueError(f"the replays of a data engine share one pace, got different replay_pace: {replay_paces}")

    def _register_codecs(self, msg_queue: ZeroMQ) -> None:
        """Encodes and decodes each dataflow's channel with its data model's wire format, e.g. Bars for market data."""
        for dataflow, (channel, _topic) in self._zmq_routes.items():
            data_model = dataflow.data_model
            msg_queue.register_codec(channel, data_model.StreamMessage, data_model.enc_hook, data_model.dec_hook)

    def _is_replaying(self) -> bool:
        # NOTE: no mixed envs, see _validate_feeds()
        return self._feeds[0]._requests[0].env == Environment.BACKTEST

    # --- output ---

    async def _forward_dataflow_message(self, dataflow: DataFlow[Any, Any], data: Message) -> None:
        """Called by a feed's dataflow (without Ray) with each message's output."""
        channel, topic = self._zmq_routes[dataflow]
        await self._emit(channel, topic, data)

    async def _emit(self, channel: ZeroMQChannel, topic: ZeroMQTopic, data: Message) -> None:
        """Sends an output to the callback, the iteration and the XPUB's subscribers."""
        if self._callback:
            # NOTE: a failed callback skips only this output, with or without Ray
            try:
                result = self._callback(data)
                if inspect.isawaitable(result):
                    await result
            except Exception:
                self._logger.exception(f"Data Engine callback failed on {channel}, skipping the output:")
        if (iter_queue := self._iter_queue) is not None:
            # NOTE: drop the oldest output instead of blocking, a slow consumer would otherwise stall every feed
            if iter_queue.full():
                self._logger.warning(
                    "data engine iteration queue is full, dropping the oldest output - consider increasing QUEUE_MAXSIZE "
                    + f"(current: {self.QUEUE_MAXSIZE}) or improving the consumer's speed"
                )
                _ = iter_queue.get_nowait()
            iter_queue.put_nowait(data)
        if self._msg_queue is not None:
            # NOTE: a failed send skips only this output, it must not stop the dataflow that produced it
            try:
                self._msg_queue.send(channel=channel, topic=topic, data=data)
            except Exception:
                self._logger.exception(f"Data Engine failed to send the output on {channel}, skipping it:")

    def _setup_worker_messaging(self, inbox: asyncio.Queue[Output]) -> None:
        """Binds the PULL the Ray workers push their output to, a thread receives from it and puts the output in the inbox."""
        import zmq

        from pfeed.streaming.zeromq import ZeroMQ, ZeroMQDataChannel

        loop = asyncio.get_running_loop()
        msg_queue = ZeroMQ(name="data_engine.workers", logger=self._logger, receiver_type=zmq.PULL, receiver_method="bind")
        # NOTE: decodes the outputs back into their types, e.g. Bar/Tick for market data, like the outputs without Ray
        self._register_codecs(msg_queue)
        msg_queue.bind(msg_queue.receiver)
        self._worker_msg_queue = msg_queue
        self._stop_worker_thread.clear()

        def _receive():
            # NOTE: only this thread uses the PULL after this, a zmq socket isn't thread-safe
            try:
                while True:
                    if (msg := msg_queue.recv()) is None:
                        # NOTE: stops only once nothing is left to receive, the workers' last outputs can arrive after they have stopped
                        if self._stop_worker_thread.is_set():
                            break
                        continue
                    channel, topic, data, _msg_ts = msg
                    # NOTE: each worker sends STOP when it terminates, it is not an output
                    if channel == ZeroMQDataChannel.signal:
                        continue
                    _ = loop.call_soon_threadsafe(inbox.put_nowait, (channel, topic, data))
            finally:
                msg_queue.terminate()

        self._worker_thread = threading.Thread(target=_receive, name="data_engine.workers", daemon=True)
        self._worker_thread.start()

    def _teardown_worker_messaging(self) -> None:
        self._stop_worker_thread.set()
        if self._worker_thread is not None:
            self._worker_thread.join(timeout=10)
            if self._worker_thread.is_alive():
                self._logger.warning("data engine's thread receiving from the Ray workers did not stop in time")
        self._worker_thread = None
        self._worker_msg_queue = None

    async def _forward_worker_output(self, inbox: asyncio.Queue[Output]) -> None:
        while True:
            channel, topic, data = await inbox.get()
            try:
                await self._emit(channel, topic, data)
            # NOTE: keeps forwarding, run_async() waits for the inbox to be emptied when the feeds have ended
            except Exception:
                self._logger.exception(f"Data Engine failed to send the output on {channel}:")
            finally:
                inbox.task_done()

    # --- running ---

    def run(self) -> dict[BaseFeed[Any, Any, Any, Any], RunResult]:
        """Runs every feed's streams or replays until they end or end() is called, see run_async()."""
        try:
            _ = asyncio.get_running_loop()
        except RuntimeError:
            # no running event loop, safe to use asyncio.run()
            pass
        else:
            raise RuntimeError(
                "Cannot call engine.run() from within a running event loop.\n"
                + "Did you mean to call engine.run_async()?"
            )
        return asyncio.run(self.run_async())

    async def run_async(self) -> dict[BaseFeed[Any, Any, Any, Any], RunResult]:
        """Runs every feed's streams or replays until they end or end() is called.

        Returns:
            Each feed's result. A feed that raised gets its dataflows failed with the error.
        """
        from pfeed.dataflow.result import DataFlowResult, RunResult
        from pfeed.streaming.replay_clock import ReplayClock

        if self.is_running():
            raise RuntimeError("Data Engine is already running, cannot run again")
        self._validate_feeds()
        self._is_running = True
        self._loop = asyncio.get_running_loop()
        inbox: asyncio.Queue[Output] = asyncio.Queue()
        forwarding: asyncio.Task[None] | None = None
        # NOTE: kept since a feed clears its dataflows when its run ends
        dataflows = {feed: feed.dataflows for feed in self._feeds}
        try:
            # NOTE: each dataflow's channel, its outputs are sent on it, e.g. to the XPUB's subscribers
            self._zmq_routes = {
                dataflow: dataflow.data_model.to_zmq_route() for feed_dataflows in dataflows.values() for dataflow in feed_dataflows
            }
            if self._msg_queue is not None:
                self._register_codecs(self._msg_queue)
            if any(feed._is_using_ray() for feed in self._feeds):
                self._setup_worker_messaging(inbox)
                forwarding = asyncio.create_task(self._forward_worker_output(inbox))
            if self._is_replaying():
                self._replay_clock = ReplayClock(self._feeds[0]._requests[0].replay_pace)
                # NOTE: every feed joins before any starts, otherwise a feed could replay ahead of one that hasn't joined yet
                for feed in self._feeds:
                    self._replay_clock.join(feed)
            self._feed_tasks = [asyncio.create_task(feed.run_async()) for feed in self._feeds]
            try:
                await self._wait_for_feeds()
            except asyncio.CancelledError:
                self._logger.warning("Data Engine was cancelled, ending the feeds...")
            finally:
                # NOTE: e.g. cancelled, or a feed failed with isolate_failures=False
                if pending := [task for task in self._feed_tasks if not task.done()]:
                    for task in pending:
                        _ = task.cancel()
                    _ = await asyncio.wait(pending)
            results: dict[BaseFeed[Any, Any, Any, Any], RunResult] = {}
            for feed, task in zip(self._feeds, self._feed_tasks, strict=True):
                # NOTE: a cancelled feed's dataflows keep their results, a cancelled stream is not a failure
                if task.cancelled():
                    results[feed] = RunResult(data=None, dataflows=dataflows[feed])
                elif (err := task.exception()) is not None:
                    self._logger.error(f"{feed.name} failed: {err!r}")
                    for dataflow in dataflows[feed]:
                        dataflow._result = DataFlowResult.failed(error=err)
                    results[feed] = RunResult(data=None, dataflows=dataflows[feed])
                else:
                    results[feed] = task.result()
            if failed := [feed.name for feed, result in results.items() if not result.success]:
                self._logger.warning(f"Data Engine has {len(failed)} failed feeds: {failed}")
            return results
        finally:
            # NOTE: every feed has ended, and with them the Ray workers
            self._teardown_worker_messaging()
            if forwarding is not None:
                # NOTE: lets the thread's last call_soon_threadsafe() puts run, then sends what is left in the inbox
                await asyncio.sleep(0)
                if not forwarding.done():
                    await inbox.join()
                _ = forwarding.cancel()
                _ = await asyncio.wait([forwarding])
            if self._msg_queue is not None:
                self._msg_queue.terminate()
                self._msg_queue = None
            self._replay_clock = None
            self._zmq_routes.clear()
            self._feed_tasks = []
            self._loop = None
            self._is_running = False

    async def _wait_for_feeds(self) -> None:
        """Waits until every feed has ended, or until the first failed one if isolate_failures is False."""
        pending: set[asyncio.Task[RunResult]] = set(self._feed_tasks)
        while pending:
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            if self._isolate_failures:
                continue
            for task in done:
                if not task.cancelled() and (task.exception() is not None or not task.result().success):
                    self._logger.warning("a feed has failed, ending the other feeds since isolate_failures=False")
                    return

    def end(self) -> None:
        """Ends the feeds, run() / run_async() then return their results. Can be called from any thread."""
        if (loop := self._loop) is None:
            return
        self._logger.debug("Data Engine is ending")

        def _cancel_feeds():
            for task in self._feed_tasks:
                _ = task.cancel()

        # NOTE: raised if the loop has just closed, i.e. the run has ended
        with contextlib.suppress(RuntimeError):
            _ = loop.call_soon_threadsafe(_cancel_feeds)

    async def __aiter__(self) -> AsyncGenerator[Message]:
        """Runs the feeds and yields each output, like the callback gets it.

        Breaking out of the loop ends the feeds only when the generator is closed, which Python does lazily,
        wrap it in `contextlib.aclosing()` to end them right at the break.
        """
        queue: asyncio.Queue[Message] = asyncio.Queue(maxsize=self.QUEUE_MAXSIZE)
        self._iter_queue = queue
        producer = asyncio.create_task(self.run_async())
        # NOTE: a non-immediate shutdown lets the consumer drain the queued outputs before get() raises QueueShutDown
        producer.add_done_callback(lambda _: queue.shutdown())
        try:
            while True:
                try:
                    data = await queue.get()
                except asyncio.QueueShutDown:
                    break
                yield data
            # re-raises the producer's exception, if any
            _ = await producer
        finally:
            # NOTE: the consumer stopped early, e.g. break or cancelled, end the feeds
            if not producer.done():
                _ = producer.cancel()
                # NOTE: wait() instead of awaiting the producer, so the producer's cancellation isn't raised here, but this task's own is
                _ = await asyncio.wait([producer])
            self._iter_queue = None
