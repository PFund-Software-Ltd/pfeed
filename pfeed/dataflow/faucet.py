from __future__ import annotations
from typing import Callable, TYPE_CHECKING, Any, ClassVar
if TYPE_CHECKING:
    from collections.abc import Awaitable
    from pfeed.feeds.streaming_feed_mixin import ChannelKey, WebSocketName, Message
    from pfeed.data_models.base_data_model import BaseDataModel
    from pfeed.sources.data_provider_source import DataProviderSource
    from pfeed.dataflow.dataflow import DataFlow
    from pfeed.typing import GenericData
    from pfeed.data_handlers.base_data_handler import BaseMetadata
    from pfeed.streaming.zeromq import ZeroMQ

import logging    
import asyncio
import inspect

from pfeed.enums.extract_type import ExtractType

class Faucet:
    '''Faucet is the starting point of a dataflow
    It contains a data model and a flow function to perform the extraction.
    '''
    STREAMING_QUEUE_MAXSIZE: ClassVar[int] = 1000

    def __init__(
        self, 
        extract_func: Callable[..., Any],  # e.g. _download_impl(), _stream_impl(), _retrieve_impl(), _fetch_impl()
        extract_type: ExtractType,
        data_model: BaseDataModel | None = None, 
        close_stream: Callable[..., Awaitable[None]] | None=None,
    ):
        '''
        Args:
            close_stream:
                A function to disconnect the streaming after running the extract_func.
        '''
        self.extract_type = ExtractType[extract_type.lower()] if isinstance(extract_type, str) else extract_type
        if self.extract_type == ExtractType.stream:
            assert close_stream is not None, 'close_stream is required for streaming'
        self._data_model: BaseDataModel | None = data_model
        if data_model is not None:
            self._logger = logging.getLogger(f'pfeed.{self.data_source.name.lower()}')
        else:
            self._logger = logging.getLogger('pfeed')
        self._extract_func = extract_func
        self._close_stream = close_stream
        self._is_stream_opened = False
        self._streaming_queue: asyncio.Queue[tuple[WebSocketName, Message] | None] | None = None
        self._user_streaming_callback: Callable[[WebSocketName, Message], Awaitable[None] | None] | None = None
        self._streaming_bindings: dict[ChannelKey, DataFlow] = {}
        self._msg_queue: ZeroMQ | None = None
        self._stream_workers: list[str] = []
    
    def __str__(self):
        return f'{self.data_source.name}.{self.extract_type}'
    
    @property
    def data_source(self) -> DataProviderSource:
        assert self._data_model is not None, 'data_model is not set'
        return self._data_model.data_source
    
    @property
    def data_model(self) -> BaseDataModel | None:
        return self._data_model
    
    @property
    def streaming_queue(self) -> asyncio.Queue[tuple[WebSocketName, Message] | None]:
        if self._streaming_queue is None:
            self._streaming_queue = asyncio.Queue(maxsize=self.STREAMING_QUEUE_MAXSIZE)
        return self._streaming_queue
    
    def open_batch(self) -> tuple[GenericData | None, BaseMetadata | None]:
        data: GenericData | None = None
        metadata: BaseMetadata | None = None
        if self.extract_type == ExtractType.retrieve:
            data, metadata = self._extract_func(self.data_model)
        else:
            data = self._extract_func(self.data_model)
        return data, metadata
    
    def _setup_messaging(self):
        import zmq
        from pfeed.streaming.zeromq import ZeroMQ
        self._msg_queue = ZeroMQ(
            name='Faucet',
            logger=self._logger, 
            sender_type=zmq.ROUTER,  # pyright: ignore[reportArgumentType]
        )
        self._msg_queue.bind(self._msg_queue.sender)  # pyright: ignore[reportUnknownMemberType]
    
    def add_stream_worker(self, worker_name: str):
        if not self._stream_workers:
            self._setup_messaging()
        if worker_name not in self._stream_workers:
            self._stream_workers.append(worker_name)
    
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
            assert self._close_stream is not None, 'close_stream is required for streaming'
            await self._close_stream()
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
            raise ValueError(f'streaming callback is already set, existing callback function: {self._user_streaming_callback}')
        self._user_streaming_callback = callback
