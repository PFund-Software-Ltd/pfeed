# pyright: reportArgumentType=false, reportUnknownMemberType=false, reportCallIssue=false, reportUnknownVariableType=false
from __future__ import annotations
from typing import Callable, TYPE_CHECKING, Any, ClassVar, cast
if TYPE_CHECKING:
    from collections.abc import Awaitable
    from pfeed.sources.base_source import BaseSource
    from pfeed.data_models.base_data_model import BaseDataModel
    from pfeed.feeds.streaming_feed_mixin import ChannelKey, WebSocketName, Message
    from pfeed.dataflow.dataflow import DataFlow
    from pfeed.streaming.zeromq import ZeroMQ

import logging
import asyncio
import inspect

import polars as pl

from pfeed.enums import ExtractType


class Faucet:
    '''Faucet is the starting point of a dataflow
    It contains a data model and a flow function to perform the extraction.
    '''
    STREAMING_QUEUE_MAXSIZE: ClassVar[int] = 1000

    def __init__(
        self,
        data_source: BaseSource,
        extract_func: Callable[[BaseDataModel], Any],  # e.g. _download_impl(), _stream_impl(), _retrieve_impl(), _fetch_impl()
        extract_type: ExtractType,
    ):
        self.data_source = data_source
        self._logger = logging.getLogger(f'pfeed.{self.data_source.name.lower()}')
        self.extract_type = ExtractType[extract_type.lower()] if isinstance(extract_type, str) else extract_type
        self._extract_func = extract_func
        self._is_stream_opened = False
        self._streaming_queue: asyncio.Queue[tuple[WebSocketName, Message] | None] | None = None
        self._user_streaming_callback: Callable[[WebSocketName, Message], Awaitable[None] | None] | None = None
        self._streaming_bindings: dict[ChannelKey, DataFlow] = {}
        self._msg_queue: ZeroMQ | None = None
        self._stream_workers: list[str] = []

    @property
    def streaming_queue(self) -> asyncio.Queue[tuple[WebSocketName, Message] | None]:
        if self._streaming_queue is None:
            self._streaming_queue = asyncio.Queue(maxsize=self.STREAMING_QUEUE_MAXSIZE)
        return self._streaming_queue

    def _setup_messaging(self):
        import zmq
        from pfeed.streaming.zeromq import ZeroMQ
        assert self._logger is not None, 'logger is not set'
        self._msg_queue = ZeroMQ(
            name='Faucet',
            logger=self._logger,
            sender_type=zmq.ROUTER,
        )
        self._msg_queue.bind(self._msg_queue.sender)

    def add_stream_worker(self, worker_name: str):
        if not self._stream_workers:
            self._setup_messaging()
        if worker_name not in self._stream_workers:
            self._stream_workers.append(worker_name)

    def open_batch(self, data_model: BaseDataModel) -> pl.LazyFrame | None:
        # All extract impls (download, retrieve, fetch) return just the frame.
        # Storage-side info (e.g. missing-dates-in-storage) is read separately
        # by the run aggregator via `dataflow.storage.read_metadata()`.
        return cast(pl.LazyFrame | None, self._extract_func(data_model=data_model))

    async def open_stream(self):
        # NOTE: streaming dataflows share the same faucet, so we only need to start the extraction once
        if not self._is_stream_opened:
            self._is_stream_opened = True
            await self._extract_func(faucet_callback=self._streaming_callback)

    async def close_stream(self):
        if self._is_stream_opened:
            # Signal that streaming is ending
            if self._streaming_queue:
                await self._streaming_queue.put(None)
            stream_api = self.data_source.get_stream_api()
            await stream_api.disconnect()
            # reset the states for streaming
            self._is_stream_opened = False
            self._streaming_queue = None
            self._user_streaming_callback = None
            self._streaming_bindings.clear()
        if self._msg_queue:
            self._msg_queue.terminate(target_identities=self._stream_workers)
            self._msg_queue = None
            self._stream_workers.clear()

    async def _streaming_callback(self, ws_name: WebSocketName, msg: Message, channel_key: ChannelKey | None) -> None:
        # NOTE: only send raw data (not transformed) to user callback and streaming queue
        # if user wants to use transformed data, they should use the dataflow's transform() method
        if self._user_streaming_callback:
            result = self._user_streaming_callback(ws_name, msg)
            if inspect.isawaitable(result):
                await result
        # put msg to streaming queue, which is mainly used in feed's __aiter__()
        if self._streaming_queue:
            if self._streaming_queue.full():
                self._logger.warning(f"Streaming queue full, dropping oldest message - consider increasing maxsize (current: {self.STREAMING_QUEUE_MAXSIZE}) or improving consumer speed")
                _ = self._streaming_queue.get_nowait()  # Remove oldest
            await self._streaming_queue.put((ws_name, msg))
        if channel_key:
            if channel_key in self._streaming_bindings:
                dataflow = self._streaming_bindings[channel_key]
                dataflow._run_stream_etl(msg)
            else:
                self._logger.error(f'Missing dataflow binding for channel key {channel_key}')

    def bind_channel_key_to_dataflow(self, channel_key: ChannelKey, dataflow: DataFlow):
        if channel_key in self._streaming_bindings:
            raise ValueError(f'channel key {channel_key} is already bound to a dataflow')
        self._streaming_bindings[channel_key] = dataflow

    def set_streaming_callback(self, callback: Callable[[WebSocketName, Message], Awaitable[None] | None]):
        if self._user_streaming_callback is not None and self._user_streaming_callback != callback:
            from pfeed.utils import is_lambda
            msg = (
                f'streaming callback is already set, existing callback function: {self._user_streaming_callback}\n' +
                f'new callback function: {callback}'
            )
            if is_lambda(callback) or is_lambda(self._user_streaming_callback):
                msg += (
                    '\nNote: lambda functions are always distinct objects even with identical code, ' +
                    'consider using a named function (def) instead of a lambda to avoid this'
                )
            raise ValueError(msg)
        self._user_streaming_callback = callback
