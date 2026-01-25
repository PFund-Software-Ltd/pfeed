from __future__ import annotations
from typing import Callable, TYPE_CHECKING, Awaitable, TypeAlias
if TYPE_CHECKING:
    from pfeed.data_models.base_data_model import BaseDataModel
    from pfeed.sources.data_provider_source import DataProviderSource
    from pfeed.dataflow.dataflow import DataFlow
    from pfeed.typing import GenericData
    from pfeed.data_handlers.base_data_handler import BaseMetadata

import logging    
import asyncio
import inspect

from pfeed.enums.extract_type import ExtractType


DataModelStr: TypeAlias = str


class Faucet:
    '''Faucet is the starting point of a dataflow
    It contains a data model and a flow function to perform the extraction.
    '''
    STREAMING_QUEUE_MAXSIZE = 1000
    def __init__(
        self, 
        data_model: BaseDataModel, 
        extract_func: Callable,  # e.g. _download_impl(), _stream_impl(), _retrieve_impl(), _fetch_impl()
        extract_type: ExtractType,
        close_stream: Callable | None=None,
    ):
        '''
        Args:
            close_stream:
                A function to disconnect the streaming after running the extract_func.
        '''
        self.extract_type = ExtractType[extract_type.lower()] if isinstance(extract_type, str) else extract_type
        if self.extract_type == ExtractType.stream:
            assert close_stream is not None, 'close_stream is required for streaming'
        self._data_model: BaseDataModel = data_model
        self._extract_func = extract_func
        self._close_stream = close_stream
        self._is_stream_opened = False
        self._streaming_queue: asyncio.Queue | None = None
        self._user_streaming_callback: Callable[[dict], Awaitable[None] | None] | None = None
        self._streaming_bindings: dict[DataModelStr, DataFlow] = {}
        self._logger: logging.Logger = logging.getLogger(f'pfeed.{self.data_source.name.lower()}')
    
    def __str__(self):
        return f'{self.data_source.name}.{self.extract_type}'
    
    @property
    def data_source(self) -> DataProviderSource:
        return self._data_model.data_source
    
    @property
    def data_model(self) -> BaseDataModel | None:
        # NOTE: multiple data models are sharing the same faucet during streaming, 
        # return None to prevent from using the wrong data model
        if self.extract_type == ExtractType.stream:
            return None
        return self._data_model
    
    def open_batch(self) -> tuple[GenericData | None, BaseMetadata | None]:
        data: GenericData | None = None
        metadata: BaseMetadata | None = None
        if self.extract_type == ExtractType.retrieve:
            data, metadata = self._extract_func(self.data_model)
        else:
            data = self._extract_func(self.data_model)
        return data, metadata
    
    async def open_stream(self):
        # NOTE: streaming dataflows share the same faucet, so we only need to start the extraction once
        if not self._is_stream_opened:
            self._is_stream_opened = True
            await self._extract_func(faucet_streaming_callback=self._streaming_callback)
    
    async def close_stream(self):
        if self._is_stream_opened:
            # Signal that streaming is ending
            if self._streaming_queue:
                await self._streaming_queue.put(None)
            await self._close_stream()
            self._is_stream_opened = False
            self._streaming_queue = None
    
    async def _streaming_callback(self, ws_name: str, msg: dict, data_model: BaseDataModel | None):
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
                self._streaming_queue.get_nowait()  # Remove oldest
            await self._streaming_queue.put((ws_name, msg))
        if data_model:
            dataflow = self._streaming_bindings[str(data_model)]
            dataflow._run_stream_etl(msg)
    
    def bind_data_model_to_dataflow(self, data_model: BaseDataModel, dataflow: DataFlow):
        self._streaming_bindings[str(data_model)] = dataflow
        
    def set_streaming_callback(self, callback: Callable[[dict], Awaitable[None] | None]):
        if self._user_streaming_callback is not None and self._user_streaming_callback != callback:
            raise ValueError(f'streaming callback is already set, existing callback function: {self._user_streaming_callback}')
        self._user_streaming_callback = callback
        
    def get_streaming_queue(self) -> asyncio.Queue:
        if self._streaming_queue is None:
            self._streaming_queue = asyncio.Queue(maxsize=self.STREAMING_QUEUE_MAXSIZE)
        return self._streaming_queue
