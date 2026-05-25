# pyright: reportArgumentType=false, reportUnknownMemberType=false, reportOptionalMemberAccess=false, reportUnusedCallResult=false, reportAssignmentType=false, reportCallIssue=false, reportAttributeAccessIssue=false, reportUnknownArgumentType=false, reportUnknownVariableType=false, reportUnknownParameterType=false
from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from narwhals.typing import IntoFrame

    from pfeed.feeds.base_feed import BaseFeed
    from pfeed.feeds.streaming_feed_mixin import RawMessage, WebSocketName
    from pfeed.streaming.zeromq import ZeroMQ

import asyncio
import logging
from threading import Thread

from pfund_kit.style import RichColor, TextStyle, cprint

from pfeed.enums import DataCategory, DataSource


# TODO: backfilling, running deltalake optimize/compact etc.
class DataEngine:
    def __init__(self):
        self._logger = logging.getLogger("pfeed")
        self._feeds: list[BaseFeed] = []
        self._is_running: bool = False
        self._has_setup_messaging: bool = False
        self._msg_queue: ZeroMQ | None = None
        self._streaming_queue: (
            asyncio.Queue[tuple[WebSocketName, RawMessage] | None] | None
        ) = None
        self._zmq_thread: Thread | None = None

    def is_running(self) -> bool:
        return self._is_running

    @property
    def feeds(self) -> list[BaseFeed]:
        return self._feeds

    def setup_messaging(
        self,
        zmq_url: str | None = None,
        zmq_sender_port: int | None = None,
        zmq_receiver_port: int | None = None,
        io_threads: int = 2,
    ) -> None:
        import zmq

        from pfeed.streaming import BarMessage, TickMessage
        from pfeed.streaming.zeromq import ZeroMQ

        if self._has_setup_messaging:
            raise RuntimeError("Messaging is already setup, cannot setup again")
        zmq_url = zmq_url or ZeroMQ.DEFAULT_URL
        self._msg_queue = ZeroMQ(
            name="data_engine",
            logger=self._logger,
            io_threads=io_threads,
            sender_type=zmq.XPUB,
            receiver_type=zmq.PULL,  # pull from Ray workers in _run_stream_dataflows()
            receiver_method="bind",
            recv_type=TickMessage | BarMessage,
        )
        self._msg_queue.bind(self._msg_queue.sender, port=zmq_sender_port, url=zmq_url)
        self._msg_queue.bind(
            self._msg_queue.receiver, port=zmq_receiver_port, url=zmq_url
        )
        self._zmq_thread = Thread(target=self._run_zmq_loop, daemon=True)
        self._has_setup_messaging = True

    def _run_zmq_loop(self):
        """receive messages from Ray workers"""
        while self.is_running():
            msg = self._msg_queue.recv()
            if msg is None:
                continue
            # NOTE: received transformed/standardized data from Ray workers
            channel, topic, data, msg_ts = msg  # pyright: ignore[reportUnusedVariable]
            # send to subscribers, e.g. strategies, models in pfund
            self._msg_queue.send(channel=channel, topic=topic, data=data)
        self._msg_queue.terminate()

    def add_feed(
        self,
        data_source: DataSource | str,
        data_category: DataCategory | str = DataCategory.MARKET_DATA,
        num_workers: int | None = None,
    ) -> BaseFeed:
        from pfeed.feeds import create_feed

        feed: BaseFeed = create_feed(
            data_source=data_source,
            data_category=data_category,
            pipeline_mode=True,
            num_workers=num_workers,
        )
        if feed.supports_streaming():
            feed._set_engine(self)
            # HACK: add a add_feed() dynamically to the feed for chaining purpose:
            # e.g. engine.add_feed(...).stream(...).add_feed(...).stream(...)
            feed.add_feed = self.add_feed
        if self._is_using_ray() and num_workers is None:
            raise ValueError(f'{feed} has to set "num_workers" when using Ray')
        self._feeds.append(feed)
        return feed

    def _is_using_ray(self) -> bool:
        return any(feed._num_workers is not None for feed in self._feeds)

    def _is_streaming_feeds(self) -> bool:
        # either all streaming dataflows or all batch dataflows, cannot mix them
        if is_streaming_feeds := any(feed.streaming_dataflows for feed in self._feeds):
            assert all(feed.streaming_dataflows for feed in self._feeds), (
                "All feeds must be streaming feeds if any feed is streaming"
            )
        return is_streaming_feeds

    def run(self, **prefect_kwargs: Any) -> dict[BaseFeed, IntoFrame | None] | None:
        if self.is_running():
            raise RuntimeError("Data Engine is already running, cannot run again")
        self._is_running = True
        if self._is_using_ray() and not self._has_setup_messaging:
            self.setup_messaging()
        if not self._is_streaming_feeds():
            results: dict[BaseFeed, IntoFrame | None] = {}
            for feed in self._feeds:
                data = feed.run(**prefect_kwargs)
                results[feed] = data
            return results
        else:
            try:
                asyncio.get_running_loop()
            except (
                RuntimeError
            ):  # if no running loop, asyncio.get_running_loop() will raise RuntimeError
                # No running event loop, safe to use asyncio.run()
                pass
            else:
                cprint(
                    "Cannot call engine.run() from within a running event loop.\n"
                    + "Did you mean to call engine.run_async()?",
                    style=TextStyle.BOLD + RichColor.YELLOW,
                )
                return
            return asyncio.run(self.run_async())

    async def run_async(self) -> None:
        assert self._is_streaming_feeds(), (
            "Only streaming feeds can be run asynchronously"
        )
        if self._zmq_thread:
            self._zmq_thread.start()
        try:
            await asyncio.gather(*[feed.run_async() for feed in self._feeds])
        except asyncio.CancelledError:
            self._logger.warning(
                "Data Engine feeds were cancelled, ending data engine..."
            )
        finally:
            await asyncio.gather(
                *[
                    dataflow.end_stream()
                    for feed in self._feeds
                    for dataflow in feed.streaming_dataflows
                ]
            )
            self.end()

    def end(self):
        if not self.is_running():
            return
        self._logger.debug("Data Engine is ending")
        self._is_running = False
        if self._zmq_thread and self._zmq_thread.is_alive():
            self._logger.debug("waiting for ZMQ thread to finish")
            self._zmq_thread.join(timeout=10)
            self._logger.debug(
                f"ZMQ thread finished (alive={self._zmq_thread.is_alive()})"
            )
        self._zmq_thread = None
        self._msg_queue = None
        self._has_setup_messaging = False

    def __aiter__(self) -> AsyncGenerator[tuple[WebSocketName, RawMessage], None]:
        assert self._is_streaming_feeds(), (
            "Only streaming feeds support async iteration"
        )
        from pfeed.dataflow.faucet import Faucet

        STREAMING_QUEUE_MAXSIZE = Faucet.STREAMING_QUEUE_MAXSIZE
        if self._streaming_queue is None:
            self._streaming_queue = asyncio.Queue(maxsize=STREAMING_QUEUE_MAXSIZE)
        # NOTE: make all faucets share the same streaming queue
        for feed in self._feeds:
            faucet = feed.streaming_dataflows[0].faucet
            faucet._streaming_queue = self._streaming_queue
        queue = self._streaming_queue

        async def _iter():
            async with asyncio.TaskGroup() as task_group:
                producers = [  # noqa: F841
                    task_group.create_task(feed.run_async()) for feed in self._feeds
                ]
                remaining = len(self._feeds)
                while remaining:
                    # try:
                    msg = await queue.get()
                    # except asyncio.CancelledError:
                    #     for producer in producers:
                    #         _ = producer.cancel()
                    #     break
                    if msg is None:  # one faucet finished
                        remaining -= 1
                        continue
                    yield msg

        return _iter()
