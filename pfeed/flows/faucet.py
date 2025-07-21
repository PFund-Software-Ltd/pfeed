from __future__ import annotations
from typing import Callable, TYPE_CHECKING, Awaitable
if TYPE_CHECKING:
    from pfund.typing import FullDataChannel
    from pfeed.data_models.base_data_model import BaseDataModel
    from pfeed.sources.base_source import BaseSource
    from pfeed.flows.dataflow import DataFlow
    from pfeed.typing import GenericData
    
import asyncio
import inspect

from pfeed.enums.extract_type import ExtractType


class Faucet:
    '''Faucet is the starting point of a dataflow
    It contains a data model and a flow function to perform the extraction.
    '''
    STREAMING_QUEUE_MAXSIZE = 1000
    def __init__(
        self, 
        data_source: BaseSource,
        extract_func: Callable,  # e.g. _download_impl(), _stream_impl(), _retrieve_impl(), _fetch_impl()
        extract_type: ExtractType,
        close_stream: Callable | None=None,
        data_model: BaseDataModel | None=None, 
    ):
        '''
        Args:
            close_stream:
                A function to disconnect the streaming after running the extract_func.
        '''
        self._extract_func = extract_func
        self._extract_type = ExtractType[extract_type.lower()] if isinstance(extract_type, str) else extract_type
        self._data_source: BaseSource = data_source
        self._data_model: BaseDataModel | None = data_model
        if self._extract_type != ExtractType.stream:
            assert data_model is not None, 'data_model is required'
        else:
            assert close_stream is not None, 'close_stream is required'
        self._close_stream = close_stream
        self._is_stream_opened = False
        self._streaming_queue: asyncio.Queue | None = None
        self._user_streaming_callback: Callable[[dict], Awaitable[None] | None] | None = None
        self._streaming_bindings: dict[FullDataChannel, DataFlow] = {}

    def __str__(self):
        return f'{self._data_source.name}.{self._extract_type}'
    
    @property
    def data_source(self) -> BaseSource:
        return self._data_source
    
    @property
    def data_model(self) -> BaseDataModel | None:
        return self._data_model
    
    def open_batch(self):
        if self._extract_type == ExtractType.retrieve:
            data, metadata = self._extract_func(self._data_model)
            if 'updated_resolution' in metadata:
                self._data_model.update_resolution(metadata['updated_resolution'])
                del metadata['updated_resolution']
        else:
            data: GenericData | None = self._extract_func(self._data_model)
            # NOTE: currently no metadata for other extract_types
            metadata = {}
        return data, metadata
    
    async def open_stream(self):
        # NOTE: streaming dataflows share the same faucet, so we only need to start the extraction once
        if not self._is_stream_opened:
            self._is_stream_opened = True
            await self._extract_func(self._streaming_callback)
    
    async def close_stream(self):
        if self._is_stream_opened:
            # Signal that streaming is ending
            if self._streaming_queue:
                await self._streaming_queue.put(None)
            await self._close_stream()
            self._is_stream_opened = False
            self._streaming_queue = None
    
    async def _streaming_callback(self, channel_key: str, data: dict):
        channel: FullDataChannel = data[channel_key] if channel_key in data else None
        if self._user_streaming_callback:
            result = self._user_streaming_callback(data)
            if inspect.isawaitable(result):
                await result
        if self._streaming_queue:
            if self._streaming_queue.full():
                self.logger.warning(f"Streaming queue full, dropping oldest message - consider increasing maxsize (current: {self.STREAMING_QUEUE_MAXSIZE}) or improving consumer speed")
                self._streaming_queue.get_nowait()  # Remove oldest
            await self._streaming_queue.put(data)
        if channel:
            dataflow: DataFlow = self._streaming_bindings[channel]
            await dataflow._run_stream_etl(data)
        
    def set_streaming_callback(self, callback: Callable[[dict], Awaitable[None] | None]):
        if self._user_streaming_callback is not None and self._user_streaming_callback != callback:
            raise ValueError(f'streaming callback is already set, existing callback function: {self._user_streaming_callback}')
        self._user_streaming_callback = callback
        
    def bind_channel_to_dataflow(self, dataflow: DataFlow, channel: FullDataChannel):
        self._streaming_bindings[channel] = dataflow

    def get_streaming_queue(self) -> asyncio.Queue:
        if self._streaming_queue is None:
            self._streaming_queue = asyncio.Queue(maxsize=self.STREAMING_QUEUE_MAXSIZE)
        return self._streaming_queue
