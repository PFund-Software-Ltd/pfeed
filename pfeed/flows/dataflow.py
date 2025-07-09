from __future__ import annotations
from typing import Callable, TYPE_CHECKING, Literal, Awaitable
if TYPE_CHECKING:
    from prefect import Flow as PrefectDataFlow
    from pfeed.typing import GenericData
    from pfeed.flows.faucet import Faucet
    from pfeed.flows.sink import Sink
    from pfeed.data_models.base_data_model import BaseDataModel

import asyncio
import logging
import inspect
from enum import StrEnum

from prefect import flow, task
from prefect.utilities.annotations import quote

from pfeed.enums import ExtractType
from pfeed.flows.result import FlowResult


class FlowType(StrEnum):
    native = 'native'
    prefect = 'prefect'


class DataFlow:
    def __init__(
        self, 
        data_model: BaseDataModel, 
        faucet: Faucet,
        streaming_callback: Callable[[dict], Awaitable[None] | None] | None=None,
    ):
        data_source = data_model.data_source
        self.logger = logging.getLogger(f'{data_source.name.lower()}_data')
        self.name = f'{data_source.name}_DataFlow'
        self._data_model: BaseDataModel = data_model
        self._faucet: Faucet = faucet
        self._sink: Sink | None = None
        self._transformations: list[Callable] = []
        self._result = FlowResult()
        self._streaming_queue: asyncio.Queue | None = None
        self._user_streaming_callback = streaming_callback
    
    @property
    def data_model(self) -> BaseDataModel:
        return self._data_model

    @property
    def faucet(self) -> Faucet:
        return self._faucet
    
    @property
    def sink(self) -> Sink:
        return self._sink

    @property
    def extract_type(self) -> ExtractType:
        return self.faucet._extract_type
    
    @property
    def result(self) -> FlowResult:
        return self._result
    
    def add_transformations(self, *funcs: tuple[Callable, ...]):
        self._transformations.extend(funcs)
    
    def set_sink(self, sink: Sink):
        self._sink = sink
    
    def __str__(self):
        return f'{self.name}.{self.extract_type}'
    
    def is_streaming(self) -> bool:
        return self.faucet._extract_type == ExtractType.stream
    
    def _run_batch_etl(self, flow_type: FlowType=FlowType.native) -> GenericData | None:
        from pfeed.utils.dataframe import is_dataframe, is_empty_dataframe
        data: GenericData | None = self._extract_batch(flow_type=flow_type)
        if (data is not None) and not (is_dataframe(data) and is_empty_dataframe(data)):
            data: GenericData = self._transform(data, flow_type=flow_type)
            self._load(data, flow_type=flow_type)
        return data
    
    def run_batch(
        self, 
        flow_type: FlowType | Literal['native', 'prefect']='native', 
        prefect_kwargs: dict | None=None
    ) -> FlowResult:
        flow_type = FlowType[flow_type.lower()]
        if flow_type == FlowType.prefect:
            prefect_dataflow = self.to_prefect_dataflow(**(prefect_kwargs or {}))
            data: GenericData | None = prefect_dataflow()
            self._result.set_data(data)
        else:
            data: GenericData | None = self._run_batch_etl(flow_type=flow_type)
            self._result.set_data(data)
        return self._result
    run = run_batch
    
    # FIXME: user callback and streaming queue should be shared with the first dataflow?
    async def run_stream(self, flow_type: FlowType=FlowType.native):
        async def _run_stream_etl(data: dict):
            # TODO: push the data to zeromq if in use
            if self._user_streaming_callback:
                res = self._user_streaming_callback(data)
                if inspect.isawaitable(res):
                    await res
            # TODO: Backpressure if Ray processes fall behind: Use bounded asyncio.Queue(maxsize=N) and await queue.put() to naturally throttle?
            if self._streaming_queue:
                await self._streaming_queue.put(data)
            # TODO: use msgspec?
            data: dict = self._transform(data, flow_type=flow_type)
            # TODO:
            # self._load(data, flow_type=flow_type)
        await self._extract_stream(streaming_callback=_run_stream_etl)
    stream = run_stream
        
    def get_streaming_queue(self) -> asyncio.Queue:
        if self._streaming_queue is None:
            self._streaming_queue = asyncio.Queue()
        return self._streaming_queue

    def _extract_batch(self, flow_type: FlowType=FlowType.native) -> GenericData | None:
        extract = task(self.faucet.open_batch) if flow_type == FlowType.prefect else self.faucet.open_batch
        data, metadata = extract()
        if metadata:
            self._result.set_metadata(metadata)
        return data
    
    async def _extract_stream(self, streaming_callback: Callable[[dict], Awaitable[None] | None] | None=None) -> GenericData | None:
        await self.faucet.open_stream(streaming_callback=streaming_callback)
    
    def _transform(self, data: GenericData, flow_type: FlowType=FlowType.native) -> GenericData:
        for func in self._transformations:
            if not self.is_streaming():
                transform = task(func) if flow_type == FlowType.prefect else func
                # NOTE: Removing prefect's task introspection with `quote(data)` to save time
                data: GenericData = transform(quote(data)) if flow_type == FlowType.prefect else transform(data)
                self.logger.debug(f"transformed {self.faucet.data_model} data by '{func.__name__}'")
            else:
                # TODO: streaming, send to zeromq if any
                pass
        return data
    
    def _load(self, data: GenericData, flow_type: FlowType=FlowType.native):
        if self.sink is None:
            if self.extract_type != ExtractType.retrieve:
                self.logger.debug(f'{self.name} {self.extract_type} has no destination storage (to_storage=None)')
            return
        if not self.is_streaming():
            load = task(self.sink.flush) if flow_type == FlowType.prefect else self.sink.flush
            success = load(data)
            if not success:
                self.logger.warning(f'failed to load {self.data_model} data to {self.sink}')
            else:
                self.logger.info(f'loaded {self.data_model} data to {self.sink}')
        else:
            # TODO: streaming
            pass
            # self.sink.flush(...)

    def to_prefect_dataflow(self, **kwargs) -> PrefectDataFlow:
        '''
        Converts dataflow to prefect flow
        Args:
            kwargs: kwargs specific to prefect @flow decorator
        '''
        if 'log_prints' not in kwargs:
            kwargs['log_prints'] = True
        @flow(name=self.name, flow_run_name=str(self.data_model), **kwargs)
        def prefect_flow():
            # from prefect.logging import get_run_logger
            # prefect_logger = get_run_logger()  # this is a logger adapter
            return self._run_batch_etl(FlowType.prefect)
        return prefect_flow
