from __future__ import annotations
from typing import Callable, TYPE_CHECKING, Literal
if TYPE_CHECKING:
    from prefect import Flow as PrefectDataFlow
    from pfeed._typing import GenericData, StreamingData
    from pfeed.flows.faucet import Faucet
    from pfeed.flows.sink import Sink
    from pfeed.sources.base_source import BaseSource
    from pfeed.data_models.base_data_model import BaseDataModel
    from pfeed.messaging.zeromq import ZeroMQ

import logging

from pfeed.flows.result import FlowResult
from pfeed.enums import ExtractType, FlowType


class DataFlow:
    def __init__(self, data_model: BaseDataModel, faucet: Faucet):
        self._data_model: BaseDataModel = data_model
        self._is_streaming = faucet._extract_type == ExtractType.stream
        self._faucet: Faucet = faucet
        self._sink: Sink | None = None
        self._transformations: list[Callable] = []
        self._result = FlowResult()
        self._flow_type: FlowType = FlowType.native
        self._msg_queue: ZeroMQ | None = None
        self._logger: logging.Logger = logging.getLogger(f"{self.data_source.name.lower()}_data")
    
    @property
    def name(self):
        return f'{self.data_source.name}_DataFlow'
    
    @property
    def data_source(self) -> BaseSource:
        return self._data_model.data_source
    
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
    
    def is_sealed(self):
        '''Check if the dataflow is sealed, i.e. cannot add transformations'''
        return self._sink is not None
    
    def __str__(self):
        if not self.is_streaming():
            return f'{self.name}.{self.extract_type}'
        else:
            return f'{self.name}.{self.extract_type}.{self.data_model.product.symbol}.{self.data_model.resolution!r}'
    
    def is_streaming(self) -> bool:
        return self._is_streaming
    
    def _run_batch_etl(self) -> GenericData | None:
        from pfeed.utils.dataframe import is_dataframe, is_empty_dataframe
        if self._flow_type == FlowType.prefect:
            from prefect import task
            extract = task(self.faucet.open_batch)
        else:
            extract = self.faucet.open_batch
        data, metadata = extract()
        if metadata:
            self._result.set_metadata(metadata)
        if (data is not None) and not (is_dataframe(data) and is_empty_dataframe(data)):
            data: GenericData = self._transform(data)
            self._load(data)
        return data
    
    def run_batch(self, flow_type: Literal['native', 'prefect']='native', prefect_kwargs: dict | None=None) -> FlowResult:
        self._logger.info(f'{self.name} {self.extract_type} data={self.data_model} to storage={self.sink.storage.data_path if self.sink else None}')
        self._flow_type = FlowType[flow_type.lower()]
        if self._flow_type == FlowType.prefect:
            prefect_dataflow = self.to_prefect_dataflow(**(prefect_kwargs or {}))
            data: GenericData | None = prefect_dataflow()
            self._result.set_data(data)
        else:
            data: GenericData | None = self._run_batch_etl()
            self._result.set_data(data)
        return self._result
    
    def _setup_messaging(self, worker_name: str):
        '''
        Args:
            worker_name: Ray worker name (e.g. worker-1) thats responsible for handling the dataflow's ETL
        '''
        import zmq
        from pfeed.messaging.zeromq import ZeroMQ
        self._msg_queue = ZeroMQ(
            name=f'{self.name}', 
            logger=self._logger,
            sender_type=zmq.ROUTER
        )
        self._msg_queue.bind(self._msg_queue.sender)
        self._msg_queue.set_target_identity(worker_name)  # store zmq.DEALER's identity to send to
    
    def _run_stream_etl(self, msg: dict) -> None | StreamingData:
        # if zeromq is in use (when use_ray=True), send msg to Ray's worker and let it perform ETL
        if self._msg_queue:
            self._msg_queue.send(channel=self.name, topic=str(self.data_model), data=msg)
        else:
            data: StreamingData = self._transform(msg)
            self._load(data)
            return data
    
    async def run_stream(self, flow_type: Literal['native']='native'):
        self._logger.info(f'{self.name} {self.extract_type} data={self.data_model} to storage={self.sink.storage.data_path if self.sink else None}')
        if self.sink:
            assert self.sink.storage.use_deltalake, \
                'writing streaming data is only supported when using deltalake, please set use_deltalake=True'
        self._flow_type = FlowType[flow_type.lower()]
        await self.faucet.open_stream()  # this will trigger _run_stream_etl()
        
    async def end_stream(self):
        await self.faucet.close_stream()
        
    def _transform(self, data: GenericData) -> GenericData | StreamingData:
        for transform in self._transformations:
            if self.is_streaming():
                data: StreamingData = transform(data)
            else:
                if self._flow_type == FlowType.prefect:
                    from prefect import task
                    from prefect.utilities.annotations import quote
                    transform = task(transform)
                    # NOTE: Removing prefect's task introspection with `quote(data)` to save time
                    data: GenericData = transform(quote(data))
                else:
                    data: GenericData = transform(data)
            self._logger.debug(f"transformed {self.data_model} data by '{transform.__name__}'")
        return data
    
    def _load(self, data: GenericData | StreamingData):
        if self.sink is None:
            if self.extract_type != ExtractType.retrieve:
                self._logger.debug(f'{self.name} {self.extract_type} has no destination storage (to_storage=None)')
            return
        if self.is_streaming():
            self.sink.flush(data, streaming=True)
        else:
            if self._flow_type == FlowType.prefect:
                from prefect import task
                load = task(self.sink.flush)
            else:
                load = self.sink.flush
            load(data)

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
