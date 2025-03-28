from __future__ import annotations
from typing import Callable, TYPE_CHECKING, Literal
if TYPE_CHECKING:
    from prefect import Flow as PrefectDataFlow
    from bytewax.inputs import Source as BytewaxSource
    from bytewax.outputs import Sink as BytewaxSink
    from pfeed.typing import GenericData
    from pfeed.storages.base_storage import BaseStorage
    from pfeed.flows.faucet import Faucet
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
    def __init__(self, faucet: Faucet):
        data_source = faucet.data_source
        self.logger = logging.getLogger(f'{data_source.name.lower()}_data')
        self.name = f'{data_source.name}_DataFlow'
        self._faucet: Faucet = faucet
        self._transformations: list[Callable] = []
        self._storage_creator: Callable[[], BaseStorage] | None = None
        self._bytewax_sink: BytewaxSink | str | None = None
        self._result = FlowResult()
    
    def add_transformations(self, *funcs: tuple[Callable, ...]):
        self._transformations.extend(funcs)
    
    def lazy_create_storage(self, create_storage_func: Callable[[], BaseStorage]):
        self._storage_creator = create_storage_func
    
    def set_bytewax_sink(self, bytewax_sink: BytewaxSink | str):
        self._bytewax_sink = bytewax_sink

    @property
    def result(self) -> FlowResult:
        return self._result

    @property
    def faucet(self) -> Faucet:
        return self._faucet

    @property
    def extract_type(self) -> ExtractType:
        return self._faucet.extract_type
    
    @property
    def data_model(self) -> BaseDataModel:
        return self._faucet.data_model
    
    def __str__(self):
        return f'{self.name}.{self.extract_type}'
    
    def is_streaming(self) -> bool:
        return self._faucet._streaming
    
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
        # self.logger.debug(f"extracting {self.data_model} data by '{self._faucet.name}'")
        if not self.is_streaming():
            extract = task(self._faucet.open) if flow_type == FlowType.prefect else self._faucet.open
            data, metadata = extract()
            if metadata:
                self._result.set_metadata(metadata)
            if data is not None:
                self.logger.debug(f"extracted {self.data_model} data by '{self._faucet.name}'")
            else:
                self.logger.debug(f'failed to extract {self.data_model} data by {self._faucet.name}')
            return data
        else:
            if flow_type == FlowType.bytewax:
                assert self._bytewax_dataflow is not None, f'{self.name} has no bytewax dataflow'
                source: BytewaxSource | BytewaxStream = self._faucet.open()
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
            # self.logger.debug(f"transforming {self.data_model} data using '{func_name}'")
            if not self.is_streaming():
                transform = task(func) if flow_type == FlowType.prefect else func
                # NOTE: Removing prefect's task introspection with `quote(data)` to save time
                data: GenericData = transform(quote(data)) if flow_type == FlowType.prefect else transform(data)
                self.logger.debug(f"transformed {self.data_model} data by '{func.__name__}'")
            else:
                if flow_type == FlowType.bytewax:
                    # REVIEW: only supports passing in stream to the user's function
                    stream: BytewaxStream = func(stream)
                else:
                    # TODO: streaming without bytewax
                    pass
        return data
    
    def _update_data_model_for_storage_after_transform(self, data: GenericData | BytewaxStream) -> BaseDataModel:
        """
        Updates the data model in dataflow for storage after transformations.

        For market data model, this method checks if the data has been resampled to a different resolution.
        If so, it updates the resolution of the data model accordingly. 
        Note that `self.data_model` represents the data model for the data created by the faucet for the dataflow, 
        NOT the storage data. 
        For example, data might be downloaded as tick data but stored as 1-minute data,
        which means the data model for the storage data is different from the data model for the dataflow.
        """
        from pfund.datas.resolution import Resolution
        from pfeed.data_models.market_data_model import MarketDataModel

        storage_data_model = self.data_model
        if isinstance(self.data_model, MarketDataModel):
            data_resolution = Resolution(data['resolution'][0])
            if data_resolution != self.data_model.resolution:
                storage_data_model = self.data_model.model_copy(deep=False)
                storage_data_model.update_resolution(data_resolution)
        return storage_data_model
    
    def _load(self, data: GenericData | BytewaxStream, flow_type: FlowType=FlowType.native):
        if self._storage_creator is None:
            if self.extract_type != ExtractType.retrieve:
                raise Exception(f'{self.name} has no destination storage')
            else:
                return
        storage_data_model = self._update_data_model_for_storage_after_transform(data)
        storage: BaseStorage = self._storage_creator(storage_data_model)
        if not self.is_streaming():
            load = task(storage.write_data) if flow_type == FlowType.prefect else storage.write_data
            success = load(data)
            if not success:
                self.logger.warning(f'failed to load {storage_data_model} data to {storage.name}')
            else:
                self.logger.info(f'loaded {storage_data_model} data to {storage.name}')
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