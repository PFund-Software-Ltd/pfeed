from __future__ import annotations
from typing import TYPE_CHECKING, Callable, Any, overload, Literal, TypeAlias, AsyncGenerator, ClassVar
if TYPE_CHECKING:
    import polars as pl
    from prefect import Flow as PrefectFlow
    from pfund.typing import tEnvironment
    from pfund.products.product_base import BaseProduct
    from pfeed.messaging.streaming_message import StreamingMessage
    from pfeed.sources.base_source import BaseSource
    from pfeed.data_models.base_data_model import BaseDataModel
    from pfeed.typing import tStorage, tDataTool, tDataLayer, GenericData, tStreamMode
    from pfeed.storages.base_storage import BaseStorage
    from pfeed.flows.dataflow import DataFlow
    from pfeed.flows.faucet import Faucet
    from pfeed.flows.sink import Sink
    from pfeed.flows.result import FlowResult
    
import os
import asyncio
import logging
import inspect
from collections import defaultdict
from abc import ABC, abstractmethod
from logging.handlers import QueueHandler, QueueListener
from pprint import pformat

from pfund import print_warning
from pfund.enums import Environment
from pfeed.enums import DataTool, DataStorage, LocalDataStorage, ExtractType, StreamMode


__all__ = ["BaseFeed"]


def clear_subflows(func):
    def wrapper(feed: BaseFeed, *args, **kwargs):
        feed._clear_subflows()
        return func(feed, *args, **kwargs)
    return wrapper
    
    
class BaseFeed(ABC):
    data_domain = 'general_data'
    
    def __init__(
        self, 
        data_tool: tDataTool='polars', 
        pipeline_mode: bool=False,
        use_ray: bool=True,
        use_prefect: bool=False,
        use_deltalake: bool=False,
        env: tEnvironment='LIVE',
    ):
        '''
        Args:
            env: The trading environment, such as 'LIVE' or 'BACKTEST'.
                Certain functions like `download()` and `get_historical_data()` will automatically use 'BACKTEST' as the environment.
                Other functions, such as `stream()` and `retrieve()`, will utilize the environment specified here.
            storage_options: storage specific kwargs, e.g. if storage is 'minio', kwargs are minio specific kwargs
        '''
        self._env = Environment[env.upper()]
        self._setup_logging()
        self._data_tool = DataTool[data_tool.lower()]
        self.data_source: BaseSource = self._create_data_source(env=self._env)
        self.logger = logging.getLogger(self.name.lower() + '_data')
        self._pipeline_mode: bool = pipeline_mode
        self._use_ray: bool = use_ray
        self._use_prefect: bool = use_prefect
        self._use_deltalake: bool = use_deltalake
        self._dataflows: list[DataFlow] = []
        self._subflows: list[DataFlow] = []
        self._failed_dataflows: list[DataFlow] = []
        self._completed_dataflows: list[DataFlow] = []
        self._storage_options: dict[tStorage, dict] = {}
        self._storage_kwargs: dict[tStorage, dict] = {}
    
    @property
    def name(self):
        return self.data_source.name
    
    def _setup_logging(self):
        from pfund._logging import setup_logging_config
        from pfund._logging.config import LoggingDictConfigurator
        from pfeed.config import get_config
        is_loggers_set_up = bool(logging.getLogger('pfeed').handlers)
        if not is_loggers_set_up:
            config = get_config()
            log_path = f'{config.log_path}/{self._env}'
            user_logging_config = config.logging_config
            logging_config_file_path = config.logging_config_file_path
            logging_config = setup_logging_config(log_path, logging_config_file_path, user_logging_config=user_logging_config)
            # ≈ logging.config.dictConfig(logging_config) with a custom configurator
            logging_configurator = LoggingDictConfigurator(logging_config)
            logging_configurator.configure()
    
    def create_product(self, basis: str, symbol: str='', **specs) -> BaseProduct:
        if not hasattr(self.data_source, 'create_product'):
            raise NotImplementedError(f'{self.data_source.name} does not support creating products')
        return self.data_source.create_product(basis, symbol=symbol, **specs)

    @property
    def streaming_dataflows(self) -> list[DataFlow]:
        return [dataflow for dataflow in self._dataflows if dataflow.is_streaming()]
    
    def __aiter__(self) -> AsyncGenerator:
        if not self.streaming_dataflows:
            raise RuntimeError("No streaming dataflow to iterate over")
        # get the first dataflow, doesn't matter, they share the same faucet
        streaming_dataflow = self.streaming_dataflows[0]
        queue: asyncio.Queue = streaming_dataflow.faucet.get_streaming_queue()
        async def _iter():
            async with asyncio.TaskGroup() as task_group:
                producer = task_group.create_task(self.run_async())
                while True:
                    msg = await queue.get()
                    if msg is None:          # sentinel from faucet.close_stream()
                        break
                    yield msg
                await producer
        return _iter()
    
    @abstractmethod
    def _normalize_raw_data(self, data: Any) -> Any:
        pass
    
    @staticmethod
    @abstractmethod
    def _create_data_source(env: Environment, *args, **kwargs) -> BaseSource:
        pass

    @abstractmethod
    def create_data_model(self, *args, **kwargs) -> BaseDataModel:
        pass

    @abstractmethod
    def _create_batch_dataflows(self, *args, **kwargs) -> list[DataFlow]:
        pass
    
    @overload
    def configure_storage(
        self, 
        storage: Literal['duckdb'],
        storage_options: dict, 
        in_memory: bool=False, 
        memory_limit: str='4GB', 
    ) -> BaseFeed:
        ...
    
    @overload
    def configure_storage(
        self, 
        storage: Literal['minio'],
        storage_options: dict, 
        enable_bucket_versioning: bool=False,
    ) -> BaseFeed:
        ...
    
    def configure_storage(self, storage: tStorage, storage_options: dict, **storage_kwargs) -> BaseFeed:
        '''Configure storage kwargs for the given storage
        Args:
            storage_options: A dictionary containing configuration options that are universally applicable across different storage systems. These options typically include settings such as connection parameters, authentication credentials, and other general configurations that are not specific to a particular storage type.
            storage_kwargs: Storage-specific kwargs, e.g., if storage is 'minio', kwargs are Minio-specific kwargs.
        '''
        self._storage_options[storage] = storage_options
        self._storage_kwargs[storage] = storage_kwargs
        return self

    def is_pipeline(self) -> bool:
        return self._pipeline_mode

    def _init_ray(self, **kwargs):
        import ray
        if not ray.is_initialized():
            ray.init(**kwargs)

    def _shutdown_ray(self):
        import ray
        if ray.is_initialized():
            ray.shutdown()

    @abstractmethod
    def download(self, *args, **kwargs) -> GenericData | None | BaseFeed:
        pass
    
    @abstractmethod
    def stream(self, *args, **kwargs) -> BaseFeed:
        pass

    @abstractmethod
    def retrieve(self, *args, **kwargs) -> GenericData | None:
        pass
    
    # TODO: maybe integrate it with llm call? e.g. fetch("get news of AAPL")
    def fetch(self, *args, **kwargs) -> GenericData | None | BaseFeed:
        raise NotImplementedError(f'{self.name} fetch() is not implemented')
    
    def get_historical_data(self, *args, **kwargs) -> GenericData | None:
        raise NotImplementedError(f'{self.name} get_historical_data() is not implemented')
    
    def get_realtime_data(self, *args, **kwargs) -> GenericData | None:
        raise NotImplementedError(f'{self.name} get_realtime_data() is not implemented')
    
    @abstractmethod
    def _download_impl(self, data_model: BaseDataModel) -> GenericData:
        pass

    @abstractmethod
    def _add_default_transformations_to_download(self, *args, **kwargs):
        pass
    
    def _stream_impl(self, data_model: BaseDataModel) -> GenericData:
        raise NotImplementedError(f"{self.name} _stream_impl() is not implemented")
    
    def _close_stream(self):
        raise NotImplementedError(f'{self.name} _close_stream() is not implemented')
    
    def _add_default_transformations_to_stream(self, *args, **kwargs):
        raise NotImplementedError(f'{self.name} _add_default_transformations_to_stream() is not implemented')
    
    @abstractmethod
    def _add_default_transformations_to_retrieve(self, *args, **kwargs):
        pass

    def _retrieve_impl(
        self,
        data_model: BaseDataModel, 
        data_domain: str, 
        data_layer: tDataLayer,
        from_storage: tStorage | None,
        storage_options: dict | None,
    ) -> tuple[dict[tStorage, pl.LazyFrame | None], dict[tStorage, dict[str, Any]]]:
        '''Retrieves data by searching through all local storages, using polars for scanning data'''
        from minio import ServerError as MinioServerError
        
        storage_options = storage_options or {}
        if storage_options:
            assert from_storage is not None, 'from_storage is required when storage_options is provided'
        # NOTE: skip searching for DUCKDB, should require users to explicitly specify the storage
        search_storages = [_storage for _storage in LocalDataStorage.__members__ if _storage.upper() != DataStorage.DUCKDB] if from_storage is None else [from_storage]
        
        data_in_storages: dict[tStorage, pl.LazyFrame | None] = {}
        metadata_in_storages: dict[tStorage, dict[str, Any]] = {}
        for search_storage in search_storages:
            search_storage_options = storage_options or self._storage_options.get(search_storage, {})
            Storage = DataStorage[search_storage.upper()].storage_class
            self.logger.debug(f'searching for data {data_model} in {search_storage} ({data_layer=})...')
            try:
                storage: BaseStorage = Storage.from_data_model(
                    data_model=data_model,
                    data_layer=data_layer, 
                    data_domain=data_domain,
                    use_deltalake=self._use_deltalake,
                    storage_options=search_storage_options,
                )
                data, metadata = storage.read_data()
                metadata['from_storage'] = search_storage
                metadata['data_domain'] = data_domain
                metadata['data_layer'] = data_layer
                data_in_storages[search_storage] = data
                metadata_in_storages[search_storage] = metadata
            except Exception as e:  # e.g. minio's ServerError if server is not running
                if not isinstance(e, MinioServerError):
                    self.logger.exception(f'Error in retrieving data {data_model} in {search_storage} ({data_layer=}):')
        return data_in_storages, metadata_in_storages

    # TODO
    def _fetch_impl(self, data_model: BaseDataModel, *args, **kwargs) -> GenericData | None:
        raise NotImplementedError(f'{self.name} _fetch_impl() is not implemented')
    
    @abstractmethod
    def _eager_run_batch(self, include_metadata: bool=False, ray_kwargs: dict | None=None, prefect_kwargs: dict | None=None) -> tuple[list[DataFlow], list[DataFlow]]:
        pass
    
    @abstractmethod
    def _eager_run_stream(self, ray_kwargs: dict | None=None) -> tuple[list[DataFlow], list[DataFlow]]:
        pass
    
    def create_dataflow(self, data_model: BaseDataModel, faucet: Faucet) -> DataFlow:
        from pfeed.flows.dataflow import DataFlow
        dataflow = DataFlow(data_model=data_model, faucet=faucet)
        if self._dataflows:
            existing_dataflow = self._dataflows[0]
            assert existing_dataflow.is_streaming() == dataflow.is_streaming(), \
                'Cannot mix streaming and non-streaming dataflows in the same feed'
        self._subflows.append(dataflow)
        self._dataflows.append(dataflow)
        return dataflow
    
    def _create_faucet(
        self, 
        data_model: BaseDataModel,
        extract_func: Callable, 
        extract_type: ExtractType, 
        close_stream: Callable | None=None,
    ) -> Faucet:
        '''
        Args:
            close_stream: a function that closes the streaming dataflow after running the extract_func
        '''
        from pfeed.flows.faucet import Faucet
        return Faucet(
            data_model=data_model,
            extract_func=extract_func, 
            extract_type=extract_type,
            close_stream=close_stream,
        )
    
    @staticmethod
    def _create_sink(data_model: BaseDataModel, storage: BaseStorage) -> Sink:
        from pfeed.flows.sink import Sink
        return Sink(data_model, storage)
    
    def _clear_subflows(self):
        '''Clear subflows
        This is necessary to achieve the following behaviour:
        download(...).transform(...).load(...).stream(...).transform(...).load(...)
        1. subflows: download(...).transform(...).load(...)
        2. clear subflows so that the operations of the next batch of dataflows are independent of the previous batch
        3. subflows: stream(...).transform(...).load(...)
        '''
        self._subflows.clear()
    
    def transform(self, *funcs) -> BaseFeed:
        for dataflow in self._subflows:
            if dataflow.is_sealed():
                raise RuntimeError(
                    f'{dataflow} is sealed, cannot add transformations. i.e. this pattern is currently not allowed:\n'
                    '''
                    .transform(...)
                    .load(...)
                    .transform(...)
                    .load(...)
                    '''
                )
            dataflow.add_transformations(*funcs)
        return self
    
    def load(
        self, 
        to_storage: tStorage,
        data_layer: tDataLayer='CLEANED',
        data_domain: str='',
        storage_options: dict | None=None,
        stream_mode: tStreamMode='FAST', 
        **storage_kwargs,
    ) -> BaseFeed:
        '''
        Args:
            data_domain: custom domain of the data, used in data_path/data_layer/data_domain
                useful for grouping data
            stream_mode: SAFE or FAST
                if "FAST" is chosen, streaming data will be cached to memory to a certain amount before writing to disk,
                faster write speed, but data loss risk will increase.
                if "SAFE" is chosen, streaming data will be written to disk immediately,
                slower write speed, but data loss risk will be minimized.
        '''
        is_storage_duckdb = to_storage.upper() == DataStorage.DUCKDB.value
        if self._use_ray:
            assert not is_storage_duckdb, 'DuckDB is not thread-safe, cannot be used with Ray'

        Storage = DataStorage[to_storage.upper()].storage_class
        data_domain = data_domain or self.data_domain
        storage_options = storage_options or self._storage_options.get(to_storage, {})
        if self._subflows and self._subflows[0].is_streaming() and not is_storage_duckdb and not self._use_deltalake:
            use_deltalake = True
            print_warning("Automatically setting use_deltalake=True for streaming dataflows")
        else:
            use_deltalake = self._use_deltalake

        for dataflow in self._subflows:
            data_model = dataflow.data_model
            storage = Storage.from_data_model(
                data_model=data_model,
                data_layer=data_layer,
                data_domain=data_domain,
                use_deltalake=use_deltalake,
                storage_options=storage_options,
                stream_mode=StreamMode[stream_mode.upper()],
                **storage_kwargs,
            )
            sink: Sink = self._create_sink(data_model, storage)
            dataflow.set_sink(sink)
        return self
    
    def _clear_dataflows_before_run(self):
        self._completed_dataflows.clear()
        self._failed_dataflows.clear()
    
    def _clear_dataflows_after_run(self):
        self._subflows.clear()
        self._dataflows.clear()
    
    def _run_batch_dataflows(self, ray_kwargs: dict, prefect_kwargs: dict) -> tuple[list[DataFlow], list[DataFlow]]:
        from tqdm import tqdm
        from pfeed.utils.utils import generate_color
        
        color = generate_color(self.name.value)
        
        def _run_dataflow(dataflow: DataFlow, logger: logging.Logger | None=None) -> FlowResult:
            if self._use_prefect:
                flow_type = 'prefect'
            else:
                flow_type = 'native'
            result: FlowResult = dataflow.run_batch(flow_type=flow_type, prefect_kwargs=prefect_kwargs)
            # NOTE: EMPTY dataframe is considered as success
            success = result.data is not None
            return success
        
        self._clear_dataflows_before_run()
        if self._use_ray:
            import atexit
            import ray
            from ray.util.queue import Queue
            atexit.register(lambda: ray.shutdown())  # useful in jupyter notebook environment
            
            @ray.remote
            def ray_task(feed_name: str, use_prefect: bool, dataflow: DataFlow) -> tuple[bool, DataFlow]:
                success = False
                try:
                    logger = logging.getLogger(feed_name.lower() + '_data')
                    if not logger.handlers:
                        logger.addHandler(QueueHandler(log_queue))
                        logger.setLevel(logging.DEBUG)
                        # needs this to avoid triggering the root logger's stream handlers with level=DEBUG
                        logger.propagate = False
                    success = _run_dataflow(dataflow, logger=logger)
                except RuntimeError as err:
                    if use_prefect:
                        logger.exception(f'Error in running prefect {dataflow}:')
                    else:
                        raise err
                except Exception:
                    logger.exception(f'Error in running {dataflow}:')
                return success, dataflow
            
            try:
                self._init_ray(**ray_kwargs)
                log_queue = Queue()
                log_listener = QueueListener(log_queue, *self.logger.handlers, respect_handler_level=True)
                log_listener.start()
                batch_size = ray_kwargs['num_cpus']
                dataflow_batches = [self._dataflows[i: i + batch_size] for i in range(0, len(self._dataflows), batch_size)]
                for dataflow_batch in tqdm(dataflow_batches, desc=f'Running {self.name} dataflows', colour=color):
                    futures = [
                        ray_task.remote(
                            feed_name=self.name.value, 
                            use_prefect=self._use_prefect, 
                            dataflow=dataflow,
                        ) for dataflow in dataflow_batch
                    ]
                    returns = ray.get(futures)
                    for success, dataflow in returns:
                        if not success:
                            self._failed_dataflows.append(dataflow)
                        else:
                            self._completed_dataflows.append(dataflow)
            except KeyboardInterrupt:
                print(f"KeyboardInterrupt received, stopping {self.name} dataflows...")
            except Exception:
                self.logger.exception(f'Error in running {self.name} dataflows:')
            finally:
                log_listener.stop()
                self.logger.debug('shutting down ray...')
                self._shutdown_ray()
        else:
            try:
                for dataflow in tqdm(self._dataflows, desc=f'Running {self.name} dataflows', colour=color):
                    success = _run_dataflow(dataflow)
                    if not success:
                        self._failed_dataflows.append(dataflow)
                    else:
                        self._completed_dataflows.append(dataflow)
            except RuntimeError as err:
                if self._use_prefect:
                    self.logger.exception(f'Error in running prefect {dataflow}:')
                else:
                    raise err
            except Exception:
                self.logger.exception(f'Error in running {self.name} dataflows:')

        if self._failed_dataflows:
            # retrieve_dataflows = [dataflow for dataflow in failed_dataflows if dataflow.extract_type == 'retrieve']
            non_retrieve_dataflows = [dataflow for dataflow in self._failed_dataflows if dataflow.extract_type != ExtractType.retrieve]
            if non_retrieve_dataflows:
                self.logger.warning(
                    f'{self.name} failed dataflows:\n'
                    f'{pformat([str(dataflow) for dataflow in non_retrieve_dataflows])}\n'
                    f'check {self.logger.name}.log for details'
                )
        self._clear_dataflows_after_run()
        return self._completed_dataflows, self._failed_dataflows
    
    async def _run_stream_dataflows(self, ray_kwargs: dict):
        async def _run_dataflows():
            try:
                await asyncio.gather(*[dataflow.run_stream(flow_type='native') for dataflow in self._dataflows])
            except asyncio.CancelledError:
                self.logger.warning(f'{self.name} dataflows were cancelled')
                await asyncio.gather(*[dataflow.end_stream() for dataflow in self._dataflows])
                    
        WorkerName: TypeAlias = str
        DataFlowName: TypeAlias = str

        self._clear_dataflows_before_run()
        if self._use_ray:
            import atexit
            import ray
            from ray.util.queue import Queue
            atexit.register(lambda: ray.shutdown())  # useful in jupyter notebook environment
            
            import zmq
            from pfeed.messaging.zeromq import ZeroMQ, ZeroMQDataChannel, ZeroMQSignal

            @ray.remote
            def ray_task(
                feed_name: str, 
                worker_name: str, 
                transformations_per_dataflow: dict[DataFlowName, list[Callable]], 
                # TODO:
                # sinks_per_dataflow: dict[DataFlowName, Sink],
                ports_to_connect: list[int],
                ready_queue: Queue,
            ):
                logger = logging.getLogger(feed_name.lower() + '_data')
                if not logger.handlers:
                    logger.addHandler(QueueHandler(log_queue))
                    logger.setLevel(logging.DEBUG)
                    # needs this to avoid triggering the root logger's stream handlers with level=DEBUG
                    logger.propagate = False
                logger.debug(f"Ray {worker_name} started")
                
                try:
                    msg_queue = ZeroMQ(
                        name=f'{feed_name}.stream.{worker_name}',
                        logger=logger,
                        sender_type=zmq.PUSH,  # TODO: push to Data Engine if in use
                        receiver_type=zmq.DEALER,
                    )
                    msg_queue.receiver.setsockopt(zmq.IDENTITY, worker_name.encode())
                    for port in ports_to_connect:
                        msg_queue.connect(msg_queue.receiver, port)
                    ready_queue.put(worker_name)
                    
                    while True:
                        msg = msg_queue.recv()
                        if msg is None:
                            continue
                        channel, topic, data, msg_ts = msg
                        if channel == ZeroMQDataChannel.signal:
                            signal: ZeroMQSignal = data
                            if signal == ZeroMQSignal.STOP:
                                break
                        else:
                            dataflow_name = channel
                            # data_model_in_str = topic
                            transformations = transformations_per_dataflow[dataflow_name]
                            for transform in transformations:
                                data: dict | StreamingMessage = transform(data)
                            # TODO: streaming, load data
                            # load()
                    msg_queue.terminate()
                except Exception:
                    logger.exception(f'Error in streaming Ray {worker_name}:')
            
            try:
                self._init_ray(**ray_kwargs)
                log_queue = Queue()
                log_listener = QueueListener(log_queue, *self.logger.handlers, respect_handler_level=True)
                log_listener.start()
                num_workers = min(ray_kwargs['num_cpus'], len(self._dataflows))
                
                # Distribute dataflows' transformations across workers
                def _create_worker_name(worker_num: int) -> str:
                    return f'worker-{worker_num}'
                transformations_per_worker: dict[WorkerName, dict[DataFlowName, list[Callable]]] = defaultdict(dict)
                sinks_per_worker: dict[WorkerName, dict[DataFlowName, Sink]] = defaultdict(dict)
                ports_to_connect: dict[WorkerName, list[int]] = defaultdict(list)
                dataflow_zmqs: list[ZeroMQ] = [] 
                for i, dataflow in enumerate(self._dataflows):
                    worker_num: int = i % num_workers
                    worker_num += 1  # convert to 1-indexed, i.e. starting from 1
                    worker_name = _create_worker_name(worker_num)
                    dataflow._setup_messaging(worker_name)
                    transformations_per_worker[worker_name][dataflow.name] = dataflow._transformations
                    sinks_per_worker[worker_name][dataflow.name] = dataflow._sink
                    dataflow_zmq: ZeroMQ = dataflow._msg_queue
                    dataflow_zmqs.append(dataflow_zmq)
                    ports_in_use: list[int] = dataflow_zmq.get_ports_in_use(dataflow_zmq.sender)
                    ports_to_connect[worker_name].extend(ports_in_use)

                worker_names = [_create_worker_name(worker_num) for worker_num in range(1, num_workers+1)]
                ready_queue = Queue()  # let ray worker notify the main thread that it's ready to receive messages

                # start ray workers
                futures = [
                    ray_task.remote(
                        feed_name=self.name.value,
                        worker_name=worker_name,
                        transformations_per_dataflow=transformations_per_worker[worker_name],
                        # TODO
                        # sinks_per_dataflow=sinks_per_worker[worker_name],
                        ports_to_connect=ports_to_connect[worker_name],
                        ready_queue=ready_queue,
                    ) for worker_name in worker_names
                ]

                # wait for ray workers to be ready
                timeout = 10
                while timeout:
                    timeout -= 1
                    worker_name = ready_queue.get(timeout=5)
                    worker_names.remove(worker_name)
                    is_workers_ready = not worker_names
                    if is_workers_ready:
                        break
                else:
                    raise RuntimeError("Timeout: Not all workers reported ready")
                
                # start streaming
                await _run_dataflows()
            except KeyboardInterrupt:
                print(f"KeyboardInterrupt received, stopping {self.name} dataflows...")
            except Exception:
                self.logger.exception(f'Error in running {self.name} dataflows:')
            finally:
                log_listener.stop()
                self.logger.debug('waiting for ray tasks to finish...')
                # send STOP signal to notice the while loop in the ray worker to break
                for dataflow_zmq in dataflow_zmqs:
                    dataflow_zmq.send(
                        channel=ZeroMQDataChannel.signal,
                        topic='',
                        data=ZeroMQSignal.STOP,
                    )
                    dataflow_zmq.terminate()
                ray.get(futures)
                self.logger.debug('shutting down ray...')
                self._shutdown_ray()
        else:
            await _run_dataflows()
        self._clear_dataflows_after_run()
        
    def _eager_run(self, ray_kwargs: dict | None=None, prefect_kwargs: dict | None=None, include_metadata: bool=False) -> GenericData | asyncio.Future:
        ray_kwargs = ray_kwargs or {}
        prefect_kwargs = prefect_kwargs or {}
        if self._use_ray:
            if 'num_cpus' not in ray_kwargs:
                ray_kwargs['num_cpus'] = os.cpu_count()
        if not self.streaming_dataflows:
            return self._eager_run_batch(ray_kwargs=ray_kwargs, prefect_kwargs=prefect_kwargs, include_metadata=include_metadata)
        else:
            return self._eager_run_stream(ray_kwargs=ray_kwargs)
    
    def run(self, prefect_kwargs: dict | None=None, include_metadata: bool=False, **ray_kwargs):
        result = self._eager_run(ray_kwargs=ray_kwargs, prefect_kwargs=prefect_kwargs, include_metadata=include_metadata)
        if inspect.isawaitable(result):
            try:
                asyncio.get_running_loop()
            except RuntimeError:  # if no running loop, asyncio.get_running_loop() will raise RuntimeError
                # No running event loop, safe to use asyncio.run()
                pass
            else:
                print_warning(
                    "Cannot call .run() from within a running event loop.\n"
                    "Did you mean to call .run_async() or forget to set 'pipeline_mode=True'?"
                )
                # We're in a running event loop, can't use asyncio.run()
                # Close the coroutine to prevent RuntimeWarning about unawaited coroutine
                result.close()
                return
            return asyncio.run(result)
        else:
            return result
    
    async def run_async(self, prefect_kwargs: dict | None=None, include_metadata: bool=False, **ray_kwargs):
        result = self._eager_run(ray_kwargs=ray_kwargs, prefect_kwargs=prefect_kwargs, include_metadata=include_metadata)
        if inspect.isawaitable(result):
            return await result
        else:
            return result
    
    @property
    def dataflows(self) -> list[DataFlow]:
        return self._dataflows
    
    @property 
    def failed_dataflows(self) -> list[DataFlow]:
        """Returns list of dataflows that failed in the last run"""
        return self._failed_dataflows

    @property
    def completed_dataflows(self) -> list[DataFlow]:
        """Returns list of dataflows that completed successfully in the last run"""
        return self._completed_dataflows
    
    def to_prefect_dataflows(self, **kwargs) -> list[PrefectFlow]:
        '''
        Args:
            kwargs: kwargs specific to prefect @flow decorator
        '''
        return [dataflow.to_prefect_dataflow(**kwargs) for dataflow in self._dataflows]
    