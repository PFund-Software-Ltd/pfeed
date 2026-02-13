from __future__ import annotations
from typing import Callable, TYPE_CHECKING, Literal
if TYPE_CHECKING:
    from prefect import Flow as PrefectDataFlow
    from pfeed.typing import GenericData, StreamingData
    from pfeed.dataflow.faucet import Faucet
    from pfeed.dataflow.sink import Sink
    from pfeed.sources.data_provider_source import DataProviderSource
    from pfeed.data_models.base_data_model import BaseDataModel
    from pfeed.data_handlers.base_data_handler import BaseMetadata
    from pfeed.streaming.zeromq import ZeroMQ

import logging

from pfeed.dataflow.result import DataFlowResult
from pfeed.enums import ExtractType, FlowType


class DataFlow:
    def __init__(self, data_model: BaseDataModel, faucet: Faucet):
        self._data_model: BaseDataModel = data_model
        self._logger: logging.Logger = logging.getLogger(f'pfeed.{self.data_source.name.lower()}')
        self._is_streaming = faucet.extract_type == ExtractType.stream
        self._faucet: Faucet = faucet
        self._sink: Sink | None = None
        self._transformations: list[Callable] = []
        self._result = DataFlowResult()
        self._flow_type: FlowType = FlowType.native
        self._msg_queue: ZeroMQ | None = None
        self._zmq_channel: str | None = None
        self._zmq_topic: str | None = None
    
    @property
    def name(self):
        return f'{self.data_source.name}_DataFlow'
    
    @property
    def data_source(self) -> DataProviderSource:
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
        return self.faucet.extract_type
    
    @property
    def result(self) -> DataFlowResult:
        return self._result
    
    def add_transformations(self, *funcs: tuple[Callable, ...]):
        self._transformations.extend(funcs)
    
    def set_sink(self, sink: Sink):
        self._sink = sink
    
    def __str__(self):
        if not self.is_streaming():
            return f'{self.name}.{self.extract_type}'
        else:
            return f'{self.name}.{self.extract_type}.{self.data_model.product.symbol}.{self.data_model.resolution!r}'
    
    def is_streaming(self) -> bool:
        return self._is_streaming
    
    def _run_batch_etl(self) -> tuple[GenericData | None, BaseMetadata | None]:
        from pfeed.utils.dataframe import is_dataframe, is_empty_dataframe
        if self._flow_type == FlowType.prefect:
            from prefect import task
            extract = task(self.faucet.open_batch)
        else:
            extract = self.faucet.open_batch
        data: GenericData | None
        metadata: BaseMetadata | None
        data, metadata = extract()
        if (data is not None) and not (is_dataframe(data) and is_empty_dataframe(data)):
            data: GenericData = self._transform(data)
            self._load(data)
        return data, metadata
    
    def run_batch(self, flow_type: FlowType=FlowType.native, prefect_kwargs: dict | None=None) -> DataFlowResult:
        self._logger.debug(f'{self.name} {self.extract_type} data={self.data_model} to storage={self.sink.storage.data_path if self.sink else None}')
        self._flow_type = FlowType[flow_type.lower()]
        if self._flow_type == FlowType.prefect:
            prefect_dataflow = self.to_prefect_dataflow(**(prefect_kwargs or {}))
            data, metadata = prefect_dataflow()
        else:
            data, metadata = self._run_batch_etl()

        if self.faucet.extract_type == ExtractType.download:
            if self.sink:
                self._result.set_data_loader(self.sink.storage.read_data)
            else:
                self._result.set_data(data)
        elif self.faucet.extract_type == ExtractType.retrieve:
            # NOTE: if using ray, setting large data directly in result will be inefficient, since ray will copy the data back to the main thread
            self._result.set_data(data)
        else:
            raise ValueError(f'Unhandled extract type for result: {self.faucet.extract_type}')

        if metadata:
            self._result.set_metadata(metadata)

        # NOTE: EMPTY dataframe is considered as success
        self._result.set_success(data is not None)
        return self._result
    
    def _setup_messaging(self, worker_name: str):
        '''
        Args:
            worker_name: Ray worker name (e.g. worker-1) thats responsible for handling the dataflow's ETL
        '''
        import zmq
        from pfund.enums.data_channel import PublicDataChannel
        from pfeed.streaming.zeromq import ZeroMQ, ZeroMQDataChannel
        self._msg_queue = ZeroMQ(name=f'{self.name}', logger=self._logger, sender_type=zmq.ROUTER)
        self._msg_queue.bind(self._msg_queue.sender)
        self._msg_queue.set_target_identity(worker_name)  # store zmq.DEALER's identity to send to
        self._zmq_channel = ZeroMQDataChannel.create_market_data_channel(
            data_source=self.data_source.name,
            product=self.data_model.product,
            resolution=self.data_model.resolution
        )
        if self.data_model.resolution.is_quote():
            self._zmq_topic = PublicDataChannel.orderbook
        elif self.data_model.resolution.is_tick():
            self._zmq_topic = PublicDataChannel.tradebook
        else:
            self._zmq_topic = PublicDataChannel.candlestick
    
    def _run_stream_etl(self, msg: dict) -> None | StreamingData:
        # if zeromq is in use (when use_ray=True), send msg to Ray's worker and let it perform ETL
        if self._msg_queue:
            self._msg_queue.send(channel=self._zmq_channel, topic=self._zmq_topic, data=(self.name, msg))
        else:
            data: StreamingData = self._transform(msg)
            self._load(data)
            return data
    
    async def run_stream(self, flow_type: Literal['native']='native'):
        self._logger.info(f'{self.name} {self.extract_type} data={self.data_model} to storage={self.sink.storage.data_path if self.sink else None}')
        if not self.sink:
            self._logger.debug(f'{self.name} {self.extract_type} has no destination storage (to_storage=None)')
        self._flow_type = FlowType[flow_type.lower()]
        await self.faucet.open_stream()  # this will trigger _run_stream_etl()
        
    async def end_stream(self):
        if self._msg_queue:
            self._msg_queue.terminate()
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
            assert data is not None, f'transform function {transform} should return transformed data, but got None'
            self._logger.debug(f"transformed {self.data_model} data by '{transform.__name__}'")
        return data
    
    def _load(self, data: GenericData | StreamingData):
        if self.sink is None:
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
            self._flow_type = FlowType.prefect
            return self._run_batch_etl()
        return prefect_flow
