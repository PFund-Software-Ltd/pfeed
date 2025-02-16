from __future__ import annotations
from typing import Callable, TYPE_CHECKING
if TYPE_CHECKING:
    from prefect import Flow as PrefectDataFlow
    from bytewax.inputs import Source as BytewaxSource
    from bytewax.outputs import Sink as BytewaxSink
    from pfeed.typing.core import tData
    from pfeed.storages.base_storage import BaseStorage
    from pfeed.flows.faucet import Faucet

import logging
from enum import StrEnum

from prefect import flow, task
from prefect.logging import get_run_logger
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

from pfeed.utils.dataframe import is_dataframe, is_empty_dataframe


class FlowType(StrEnum):
    native = 'native'
    prefect = 'prefect'
    bytewax = 'bytewax'


class DataFlow:
    def __init__(self, faucet: Faucet):
        data_source = faucet.data_source
        self.logger = logging.getLogger(f'{data_source.name.lower()}_data')
        self.name = f'{data_source.name}_DataFlow'
        self._faucet: Faucet = faucet
        self._transformations: list[Callable] = []
        self._storage_creator: Callable[[], BaseStorage] | None = None
        self._bytewax_sink: BytewaxSink | str | None = None
        self._bytewax_dataflow: BytewaxDataFlow | None = None
        self.output: tData | None = None
    
    def add_transformations(self, *funcs: tuple[Callable, ...]):
        self._transformations.extend(funcs)
    
    def lazy_create_storage(self, create_storage_func: Callable[[], BaseStorage]):
        self._storage_creator = create_storage_func
    
    def set_bytewax_sink(self, bytewax_sink: BytewaxSink | str):
        self._bytewax_sink = bytewax_sink

    def set_bytewax_dataflow(self, bytewax_dataflow: BytewaxDataFlow):
        self._bytewax_dataflow = bytewax_dataflow

    @property
    def faucet(self) -> Faucet:
        return self._faucet

    @property
    def op_type(self) -> str:
        return self._faucet.op_type
    
    @property
    def data_model(self) -> str:
        return self._faucet.data_model
    
    def __str__(self):
        return f'{self.name}.{self.op_type}'
    
    def is_streaming(self) -> bool:
        return self._faucet._streaming
    
    def run(self, flow_type: FlowType=FlowType.native) -> tData | None | BytewaxDataFlow:
        data: tData | None | BytewaxStream = self._extract(flow_type=flow_type)
        if (data is not None) and not (is_dataframe(data) and is_empty_dataframe(data)):
            data: tData | BytewaxStream = self._transform(data, flow_type=flow_type)
            self._load(data, flow_type=flow_type)
        # if use bytewax, return bytewax dataflow instead
        if isinstance(data, BytewaxStream):
            self.output = self._bytewax_dataflow
        else:
            self.output = data
        return self.output
    
    def _extract(self, flow_type: FlowType=FlowType.native) -> tData | None | BytewaxStream:
        data = None
        # self.logger.debug(f"extracting {self.data_model} data by '{self._faucet.name}'")
        if not self.is_streaming():
            extract = task(self._faucet.open) if flow_type == FlowType.prefect else self._faucet.open
            data: tData | None = extract()
            if data is not None:
                self.logger.debug(f"extracted {self.data_model} data by '{self._faucet.name}'")
            else:
                self.logger.debug(f'failed to extract {self.data_model} data by {self._faucet.name}')
        else:
            if flow_type == FlowType.bytewax:
                assert self._bytewax_dataflow is not None, f'{self.name} has no bytewax dataflow'
                source: BytewaxSource | BytewaxStream = self._faucet.open()
                if isinstance(source, BytewaxSource):
                    stream: BytewaxStream = bytewax_op.input(self.name, self._bytewax_dataflow, source)
                else:
                    stream = source
                return stream
            else:
                # TODO: streaming without bytewax
                pass
        return data
    
    def _transform(
        self, 
        data: tData | BytewaxStream, 
        flow_type: FlowType=FlowType.native,
    ) -> tData | BytewaxStream:
        if self.is_streaming():
            stream = data
        for func in self._transformations:
            # self.logger.debug(f"transforming {self.data_model} data using '{func_name}'")
            if not self.is_streaming():
                transform = task(func) if flow_type == FlowType.prefect else func
                data: tData = transform(data)
                self.logger.debug(f"transformed {self.data_model} data by '{func.__name__}'")
            else:
                if flow_type == FlowType.bytewax:
                    # REVIEW: only supports passing in stream to the user's function
                    stream: BytewaxStream = func(stream)
                else:
                    # TODO: streaming without bytewax
                    pass
        return data
    
    def _load(self, data: tData | BytewaxStream, flow_type: FlowType=FlowType.native):
        if self._storage_creator is None:
            if self.op_type != "retrieve":
                raise Exception(f'{self.name} has no destination storage')
            else:
                return
        storage = self._storage_creator(self.data_model)
        if not self.is_streaming():
            load = task(storage.write_data) if flow_type == FlowType.prefect else storage.write_data
            success = load(data)
            if not success:
                self.logger.warning(f'failed to load {self.data_model} data to {storage.name}')
            else:
                self.logger.info(f'loaded {self.data_model} data to {storage.name}')
        else:
            stream = data
            if flow_type == FlowType.bytewax:
                assert self._bytewax_sink is not None, f'{self.name} has no bytewax sink'
                bytewax_op.output(self._bytewax_sink.__name__, stream, self._bytewax_sink)
            else:
                # TODO: streaming without bytewax
                storage.write_data(...)

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
            self.logger = get_run_logger()
            self.run(FlowType.prefect)
        return prefect_flow
    
    def to_bytewax_dataflow(self, **kwargs) -> BytewaxDataFlow:
        '''
        Args:
            kwargs: kwargs specific to bytewax Dataflow()
        '''
        assert self.is_streaming(), f'{self.name} is not a streaming dataflow'
        bytewax_dataflow = BytewaxDataFlow(self.name, **kwargs)
        self.run(FlowType.bytewax)
        self.set_bytewax_dataflow(bytewax_dataflow)
        return bytewax_dataflow