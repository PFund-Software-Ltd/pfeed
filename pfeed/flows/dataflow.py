from __future__ import annotations
from typing import Callable, TYPE_CHECKING, Literal
if TYPE_CHECKING:
    from prefect import Flow as PrefectDataFlow
    from pfeed.typing import GenericData
    from pfeed.flows.faucet import Faucet
    from pfeed.flows.sink import Sink
    from pfeed.data_models.base_data_model import BaseDataModel

import logging

from pfeed.messaging import BarMessage
from pfeed.flows.result import FlowResult
from pfeed.enums import ExtractType, FlowType


StreamingMessage = BarMessage


class DataFlow:
    def __init__(self, data_model: BaseDataModel, faucet: Faucet):
        data_source = data_model.data_source
        self.logger = logging.getLogger(f'{data_source.name.lower()}_data')
        self.name = f'{data_source.name}_DataFlow'
        self._data_model: BaseDataModel = data_model
        self._faucet: Faucet = faucet
        self._sink: Sink | None = None
        self._transformations: list[Callable] = []
        self._result = FlowResult()
        self._flow_type: FlowType = FlowType.native
        
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
        if not self.is_streaming():
            return f'{self.name}.{self.extract_type}'
        else:
            return f'{self.name}.{self.extract_type}.{self.data_model.product.name}.{self.data_model.resolution!r}'
    
    def is_streaming(self) -> bool:
        return self.faucet._extract_type == ExtractType.stream
    
    def _run_batch_etl(self) -> GenericData | None:
        from pfeed.utils.dataframe import is_dataframe, is_empty_dataframe
        data: GenericData | None = self._extract_batch()
        if (data is not None) and not (is_dataframe(data) and is_empty_dataframe(data)):
            data: GenericData = self._transform(data)
            self._load(data)
        return data
    
    def run_batch(self, flow_type: Literal['native', 'prefect']='native', prefect_kwargs: dict | None=None) -> FlowResult:
        self._flow_type = FlowType[flow_type.lower()]
        if self._flow_type == FlowType.prefect:
            prefect_dataflow = self.to_prefect_dataflow(**(prefect_kwargs or {}))
            data: GenericData | None = prefect_dataflow()
            self._result.set_data(data)
        else:
            data: GenericData | None = self._run_batch_etl()
            self._result.set_data(data)
        return self._result
    
    async def _run_stream_etl(self, msg: dict):
        # TODO: push the data to zeromq if in use
        msg: StreamingMessage = self._transform(msg)
        print('***MESSAGE***:', msg.to_dict())
        # TODO: streaming
        # self._load(msg)
    
    async def run_stream(self, flow_type: Literal['native']='native'):
        self._flow_type = FlowType[flow_type.lower()]
        await self._extract_stream()
        
    def _extract_batch(self) -> GenericData | None:
        if self._flow_type == FlowType.prefect:
            from prefect import task
            extract = task(self.faucet.open_batch)
        else:
            extract = self.faucet.open_batch
        data, metadata = extract()
        if metadata:
            self._result.set_metadata(metadata)
        return data
    
    async def _extract_stream(self) -> GenericData | None:
        await self.faucet.open_stream()
    
    def _transform(self, data: GenericData) -> GenericData:
        for func in self._transformations:
            if self._flow_type == FlowType.prefect:
                from prefect import task
                from prefect.utilities.annotations import quote
                transform = task(func)
                # NOTE: Removing prefect's task introspection with `quote(data)` to save time
                data: GenericData = transform(quote(data))
            else:
                transform = func
                data: GenericData = transform(data)
            self.logger.debug(f"transformed {self.data_model} data by '{func.__name__}'")
        return data
    
    def _load(self, data: GenericData):
        if self.sink is None:
            if self.extract_type != ExtractType.retrieve:
                self.logger.debug(f'{self.name} {self.extract_type} has no destination storage (to_storage=None)')
            return
        if not self.is_streaming():
            if self._flow_type == FlowType.prefect:
                from prefect import task
                load = task(self.sink.flush)
            else:
                load = self.sink.flush
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
        from prefect import flow
        if 'log_prints' not in kwargs:
            kwargs['log_prints'] = True
        @flow(name=self.name, flow_run_name=str(self.data_model), **kwargs)
        def prefect_flow():
            # from prefect.logging import get_run_logger
            # prefect_logger = get_run_logger()  # this is a logger adapter
            return self._run_batch_etl()
        return prefect_flow
