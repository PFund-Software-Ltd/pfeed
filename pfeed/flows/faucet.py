from __future__ import annotations
from typing import Callable, TYPE_CHECKING, Any, Awaitable
if TYPE_CHECKING:
    from pfeed.data_models.base_data_model import BaseDataModel
    from pfeed.sources.base_source import BaseSource
    from pfeed.typing import GenericData
    
import asyncio

from pfeed.enums.extract_type import ExtractType


class Faucet:
    '''Faucet is the starting point of a dataflow
    It contains a data model and a flow function to perform the extraction.
    '''
    def __init__(
        self, 
        data_source: BaseSource,
        extract_func: Callable,  # e.g. _download_impl(), _stream_impl(), _retrieve_impl(), _fetch_impl()
        extract_type: ExtractType,
        data_model: BaseDataModel | None=None, 
    ):
        self._extract_func = extract_func
        self._extract_type = ExtractType[extract_type.lower()] if isinstance(extract_type, str) else extract_type
        self._data_source: BaseSource = data_source
        self._data_model: BaseDataModel | None = data_model
        if self._extract_type != ExtractType.stream:
            assert data_model is not None, 'data_model is required'
        self._is_streaming_started = False

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
        
    async def open_stream(self, streaming_callback: Callable[[dict], Awaitable[None] | None]):
        # NOTE: streaming dataflows share the same faucet, so we only need to start the extraction once
        if not self._is_streaming_started:            
            await self._extract_func(streaming_callback)
        self._is_streaming_started = True
        
    
    # TODO: useful for streaming, disconnect the stream?
    # def close(self):
    #     # async def stop(self):
    #     self._closed.set()
    #     if self._producer_task:
    #         await self._producer_task
