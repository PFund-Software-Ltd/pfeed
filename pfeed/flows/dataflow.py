from __future__ import annotations
from typing import Callable, TYPE_CHECKING, Literal
if TYPE_CHECKING:
    from prefect import Flow as PrefectDataFlow
    from pfeed.typing import GenericData
    from pfeed.flows.faucet import Faucet
    from pfeed.flows.sink import Sink
    from pfeed.data_models.base_data_model import BaseDataModel

import logging
from enum import StrEnum

from prefect import flow, task
from prefect.utilities.annotations import quote

from pfeed.enums import ExtractType
from pfeed.flows.result import FlowResult


class FlowType(StrEnum):
    native = 'native'
    prefect = 'prefect'


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
        return self.faucet.extract_type
    
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
        return self.faucet._streaming
    
    def run(
        self, 
        flow_type: FlowType | Literal['native', 'prefect']='native',
    ) -> FlowResult:
        flow_type = FlowType[flow_type.lower()]
        if flow_type == FlowType.prefect:
            prefect_dataflow = self.to_prefect_dataflow()
            data: GenericData | None = prefect_dataflow()
            self._result.set_data(data)
        else:
            data: GenericData | None = self._run()
            self._result.set_data(data)
        return self._result
    
    def _run(self, flow_type: FlowType=FlowType.native) -> GenericData | None:
        from pfeed.utils.dataframe import is_dataframe, is_empty_dataframe
        data: GenericData | None = self._extract(flow_type=flow_type)
        if (data is not None) and not (is_dataframe(data) and is_empty_dataframe(data)):
            data: GenericData = self._transform(data, flow_type=flow_type)
            self._load(data, flow_type=flow_type)
        return data
    
    def _extract(self, flow_type: FlowType=FlowType.native) -> GenericData | None:
        if not self.is_streaming():
            extract = task(self.faucet.open) if flow_type == FlowType.prefect else self.faucet.open
            data, metadata = extract()
            if metadata:
                self._result.set_metadata(metadata)
            return data
        else:
            # TODO: streaming
            pass
    
    def _transform(
        self, 
        data: GenericData, 
        flow_type: FlowType=FlowType.native,
    ) -> GenericData:
        if self.is_streaming():
            stream = data
        for func in self._transformations:
            if not self.is_streaming():
                transform = task(func) if flow_type == FlowType.prefect else func
                # NOTE: Removing prefect's task introspection with `quote(data)` to save time
                data: GenericData = transform(quote(data)) if flow_type == FlowType.prefect else transform(data)
                self.logger.debug(f"transformed {self.faucet.data_model} data by '{func.__name__}'")
            else:
                # TODO: streaming
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
            stream = data
            # TODO: streaming
            self.sink.flush(...)

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
            return self._run(FlowType.prefect)
        return prefect_flow
