from __future__ import annotations
from typing import Callable, TYPE_CHECKING, Literal
if TYPE_CHECKING:
    from narwhals.typing import Frame
    from prefect import Flow as PrefectDataFlow
    from bytewax.inputs import Source as BytewaxSource
    from bytewax.outputs import Sink as BytewaxSink
    from pfeed.typing import GenericData
    from pfeed.flows.faucet import Faucet
    from pfeed.flows.sink import Sink
    from pfeed.data_models.base_data_model import BaseDataModel

import logging
from enum import StrEnum

from prefect import flow, task
from prefect.utilities.annotations import quote
try:
    from bytewax.dataflow import Dataflow as BytewaxDataFlow
    from bytewax.dataflow import Stream as BytewaxStream
    import bytewax.operators as bytewax_op
except ImportError:
    class BytewaxDataFlow:
        pass
    class BytewaxStream:
        pass
    bytewax_op = None

from pfeed.enums import ExtractType
from pfeed.flows.result import FlowResult


class FlowType(StrEnum):
    native = 'native'
    prefect = 'prefect'
    bytewax = 'bytewax'


class DataFlow:
    def __init__(self, data_model: BaseDataModel, faucet: Faucet):
        data_source = data_model.data_source
        self.logger = logging.getLogger(f'{data_source.name.lower()}_data')
        self.name = f'{data_source.name}_DataFlow'
        self._data_model: BaseDataModel = data_model
        self._faucet: Faucet = faucet
        self._sink: Sink | None = None
        self._transformations: list[Callable] = []
        self._bytewax_sink: BytewaxSink | str | None = None
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
    
    def set_bytewax_sink(self, bytewax_sink: BytewaxSink | str):
        self._bytewax_sink = bytewax_sink

    def __str__(self):
        return f'{self.name}.{self.extract_type}'
    
    def is_streaming(self) -> bool:
        return self.faucet._streaming
    
    def run(
        self, 
        flow_type: FlowType | Literal['native', 'prefect', 'bytewax']='native',
    ) -> FlowResult:
        flow_type = FlowType[flow_type.lower()]
        if flow_type == FlowType.prefect:
            prefect_dataflow = self.to_prefect_dataflow()
            data: GenericData | None = prefect_dataflow()
            self._result.set_data(data)
        elif flow_type == FlowType.bytewax:
            bytewax_dataflow = self.to_bytewax_dataflow()
            # REVIEW, using run_main() to execute the dataflow in the current thread, NOT for production
            from bytewax.testing import run_main
            run_main(bytewax_dataflow)
            self._result.set_data(bytewax_dataflow)
        else:
            data: GenericData | None = self._run()
            self._result.set_data(data)
        return self._result
    
    def _run(self, flow_type: FlowType=FlowType.native) -> GenericData | None | BytewaxDataFlow:
        from pfeed.utils.dataframe import is_dataframe, is_empty_dataframe
        data: GenericData | None | BytewaxStream = self._extract(flow_type=flow_type)
        if (data is not None) and not (is_dataframe(data) and is_empty_dataframe(data)):
            data: GenericData | BytewaxStream = self._transform(data, flow_type=flow_type)
            self._load(data, flow_type=flow_type)
        return data
    
    def _extract(self, flow_type: FlowType=FlowType.native) -> GenericData | None | BytewaxStream:
        if not self.is_streaming():
            extract = task(self.faucet.open) if flow_type == FlowType.prefect else self.faucet.open
            data, metadata = extract()
            if metadata:
                self._result.set_metadata(metadata)
            return data
        else:
            if flow_type == FlowType.bytewax:
                assert self._bytewax_dataflow is not None, f'{self.name} has no bytewax dataflow'
                source: BytewaxSource | BytewaxStream = self.faucet.open()
                if isinstance(source, BytewaxSource):
                    stream: BytewaxStream = bytewax_op.input(self.name, self._bytewax_dataflow, source)
                else:
                    stream: BytewaxStream = source
                return stream
            else:
                # TODO: streaming without bytewax
                pass
    
    def _transform(
        self, 
        data: GenericData | BytewaxStream, 
        flow_type: FlowType=FlowType.native,
    ) -> GenericData | BytewaxStream:
        if self.is_streaming():
            stream = data
        for func in self._transformations:
            if not self.is_streaming():
                transform = task(func) if flow_type == FlowType.prefect else func
                # NOTE: Removing prefect's task introspection with `quote(data)` to save time
                data: GenericData = transform(quote(data)) if flow_type == FlowType.prefect else transform(data)
                self.logger.debug(f"transformed {self.faucet.data_model} data by '{func.__name__}'")
            else:
                if flow_type == FlowType.bytewax:
                    # REVIEW: only supports passing in stream to the user's function
                    stream: BytewaxStream = func(stream)
                else:
                    # TODO: streaming without bytewax
                    pass
        return data
    
    def _load(self, data: GenericData | BytewaxStream, flow_type: FlowType=FlowType.native):
        if self.sink is None:
            if self.extract_type != ExtractType.retrieve:
                self.logger.warning(f'{self.name} {self.extract_type} has no destination storage (to_storage=None)')
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
            if flow_type == FlowType.bytewax:
                assert self._bytewax_sink is not None, f'{self.name} has no bytewax sink'
                bytewax_op.output(self._bytewax_sink.__name__, stream, self._bytewax_sink)
            else:
                # TODO: streaming without bytewax
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
    
    def to_bytewax_dataflow(self, **kwargs) -> BytewaxDataFlow:
        '''
        Args:
            kwargs: kwargs specific to bytewax Dataflow()
        '''
        assert self.is_streaming(), f'{self.name} is not a streaming dataflow'
        bytewax_dataflow = BytewaxDataFlow(self.name, **kwargs)
        self._run(FlowType.bytewax)
        return bytewax_dataflow