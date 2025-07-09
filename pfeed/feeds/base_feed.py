from __future__ import annotations
from typing import TYPE_CHECKING, Callable, Any, overload, Literal, Awaitable, AsyncGenerator
if TYPE_CHECKING:
    import polars as pl
    from prefect import Flow as PrefectFlow
    from pfund.typing import tEnvironment
    from pfund.products.product_base import BaseProduct
    from pfeed.sources.base_source import BaseSource
    from pfeed.data_models.base_data_model import BaseDataModel
    from pfeed.typing import tStorage, tDataTool, tDataLayer, GenericData, tDataSource
    from pfeed.storages.base_storage import BaseStorage
    from pfeed.flows.dataflow import DataFlow
    from pfeed.flows.faucet import Faucet
    from pfeed.flows.sink import Sink
    from pfeed.flows.result import FlowResult
    
import os
import asyncio
from abc import ABC, abstractmethod
import logging
from logging.handlers import QueueHandler, QueueListener
from pprint import pformat

from pfund.enums import Environment
from pfeed.enums import DataSource, DataTool, DataStorage, LocalDataStorage, ExtractType


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
        data_source: BaseSource | tDataSource,
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
        from pfeed.sources.base_source import BaseSource
        
        self._env = Environment[env.upper()]
        self._setup_logging()
        if not isinstance(data_source, BaseSource):
            self.data_source: BaseSource = DataSource[data_source.upper()].create_data_source(env)
        else:
            self.data_source: BaseSource = data_source
        self._data_tool = DataTool[data_tool.lower()]
        self.name: DataSource = self.data_source.name
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
        streaming_dataflow = self.streaming_dataflows[0]
        queue: asyncio.Queue = streaming_dataflow.get_streaming_queue()
        async def _iter():
            while True:
                msg = await queue.get()
                yield msg
        return _iter()
    
    @abstractmethod
    def _normalize_raw_data(self, data: Any) -> Any:
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
    
    def _stream_impl(self, data_model: BaseDataModel) -> GenericData:
        raise NotImplementedError(f"{self.name} _stream_impl() is not implemented")

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
    
    def create_dataflow(
        self, 
        data_model: BaseDataModel, 
        faucet: Faucet,
        streaming_callback: Callable[[dict], Awaitable[None] | None] | None=None,
    ) -> DataFlow:
        '''
        Args:
            streaming_callback: user's custom streaming callback to be called when data is available
        '''
        from pfeed.flows.dataflow import DataFlow
        dataflow = DataFlow(data_model=data_model, faucet=faucet, streaming_callback=streaming_callback)
        if self._dataflows:
            existing_dataflow = self._dataflows[0]
            assert existing_dataflow.is_streaming() == dataflow.is_streaming(), \
                'Cannot mix streaming and non-streaming dataflows in the same feed'
        self._subflows.append(dataflow)
        self._dataflows.append(dataflow)
        return dataflow
    
    def _create_faucet(
        self, 
        extract_func: Callable, 
        extract_type: ExtractType, 
        data_model: BaseDataModel | None=None,
    ) -> Faucet:
        from pfeed.flows.faucet import Faucet
        # reuse existing faucet for streaming dataflows since they share the same extract_func
        if self.streaming_dataflows:
            faucet: Faucet = self.streaming_dataflows[0].faucet
            return faucet
        else:
            return Faucet(self.data_source, extract_func, extract_type, data_model=data_model)
    
    @staticmethod
    def _create_sink(data_model: BaseDataModel, create_storage_func: Callable) -> Sink:
        from pfeed.flows.sink import Sink
        return Sink(data_model, create_storage_func)
    
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
            # NOTE: if using ray, only add transformations to the first dataflow since transformations in ray workers are not per dataflow
            if self._use_ray and dataflow.is_streaming():
                is_first_dataflow = (dataflow is self._dataflows[0])
                if is_first_dataflow:
                    dataflow.add_transformations(*funcs)
            else:
                dataflow.add_transformations(*funcs)
        return self
    
    def load(
        self, 
        to_storage: tStorage,
        data_layer: tDataLayer,
        data_domain: str,
        storage_options: dict | None=None,
        **storage_kwargs,
    ) -> BaseFeed:
        '''
        Args:
            data_domain: custom domain of the data, used in data_path/data_layer/data_domain
                useful for grouping data
        '''
        if self._use_ray:
            assert to_storage.upper() != DataStorage.DUCKDB, 'DuckDB is not thread-safe, cannot be used with Ray'

        storage_options = storage_options or self._storage_options.get(to_storage, {})
        Storage = DataStorage[to_storage.upper()].storage_class
        # NOTE: lazy creation of storage to avoid pickle errors when using ray
        # e.g. minio client is using socket, which is not picklable
        def _create_storage(data_model: BaseDataModel):
            return Storage.from_data_model(
                data_model=data_model,
                data_layer=data_layer,
                data_domain=data_domain,
                use_deltalake=self._use_deltalake,
                storage_options=storage_options,
                **storage_kwargs,
            )

        for dataflow in self._subflows:
            sink: Sink = self._create_sink(dataflow.data_model, _create_storage)
            dataflow.set_sink(sink)
        return self
    
    def _clear_dataflows_before_run(self):
        self._completed_dataflows.clear()
        self._failed_dataflows.clear()
    
    def _clear_dataflows_after_run(self):
        self._subflows.clear()
        self._dataflows.clear()
    
    def _run_batch_dataflows(self, ray_kwargs: dict | None=None, prefect_kwargs: dict | None=None) -> tuple[list[DataFlow], list[DataFlow]]:
        from tqdm import tqdm
        from pfeed.utils.utils import generate_color
        
        ray_kwargs = ray_kwargs or {}
        prefect_kwargs = prefect_kwargs or {}
        if self._use_ray:
            if 'num_cpus' not in ray_kwargs:
                ray_kwargs['num_cpus'] = os.cpu_count()
        
        color = generate_color(self.name.value)
        
        def _run_dataflow(dataflow: DataFlow) -> FlowResult:
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
            def ray_task(dataflow: DataFlow) -> bool:
                success = False
                try:
                    if not self.logger.handlers:
                        self.logger.addHandler(QueueHandler(log_queue))
                        self.logger.setLevel(logging.DEBUG)
                        # needs this to avoid triggering the root logger's stream handlers with level=DEBUG
                        self.logger.propagate = False
                    success = _run_dataflow(dataflow)
                except RuntimeError as err:
                    if self._use_prefect:
                        self.logger.exception(f'Error in running prefect {dataflow}:')
                    else:
                        raise err
                except Exception:
                    self.logger.exception(f'Error in running {dataflow}:')

                return success, dataflow
            
            try:
                self._init_ray(**ray_kwargs)
                log_queue = Queue()
                log_listener = QueueListener(log_queue, *self.logger.handlers, respect_handler_level=True)
                log_listener.start()
                batch_size = ray_kwargs['num_cpus']
                dataflow_batches = [self._dataflows[i: i + batch_size] for i in range(0, len(self._dataflows), batch_size)]
                for dataflow_batch in tqdm(dataflow_batches, desc=f'Running {self.name} dataflows', colour=color):
                    futures = [ray_task.remote(dataflow) for dataflow in dataflow_batch]
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
                if log_listener:
                    log_listener.stop()
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
    
    async def _run_stream_dataflows(self, ray_kwargs: dict | None=None):
        ray_kwargs = ray_kwargs or {}
        if self._use_ray:
            if 'num_cpus' not in ray_kwargs:
                ray_kwargs['num_cpus'] = os.cpu_count()
        
        self._clear_dataflows_before_run()
        if self._use_ray:
            import atexit
            import ray
            from ray.util.queue import Queue
            atexit.register(lambda: ray.shutdown())  # useful in jupyter notebook environment

            @ray.remote
            def ray_task(transformations: list[Callable]):
                for func in transformations:
                    func()
                # TODO: zeromq while loop for receiving msgs
                try:
                    if not self.logger.handlers:
                        self.logger.addHandler(QueueHandler(log_queue))
                        self.logger.setLevel(logging.DEBUG)
                        # needs this to avoid triggering the root logger's stream handlers with level=DEBUG
                        self.logger.propagate = False
                except Exception:
                    self.logger.exception(f'Error in running {dataflow}:')
            
            try:
                self._init_ray(**ray_kwargs)
                log_queue = Queue()
                log_listener = QueueListener(log_queue, *self.logger.handlers, respect_handler_level=True)
                log_listener.start()
                num_workers = ray_kwargs['num_cpus']
                first_dataflow_transformations = self._dataflows[0]._transformations
                futures = [ray_task.remote(first_dataflow_transformations) for _ in range(num_workers)]
                # returns = ray.get(futures)
                
                for dataflow in self._dataflows:
                    is_first_dataflow = (dataflow is self._dataflows[0])
                    if not is_first_dataflow:
                        dataflow.add_transformations(*self._dataflows[0]._transformations)
                    dataflow.run_stream(flow_type='native')
                # for success, dataflow in returns:
                #     if not success:
                #         self._failed_dataflows.append(dataflow)
                #     else:
                #         self._completed_dataflows.append(dataflow)
            except KeyboardInterrupt:
                print(f"KeyboardInterrupt received, stopping {self.name} dataflows...")
            except Exception:
                self.logger.exception(f'Error in running {self.name} dataflows:')
            finally:
                if log_listener:
                    log_listener.stop()
                self._shutdown_ray()
        else:
            try:
                async def _run_dataflow(dataflow: DataFlow):
                    await dataflow.run_stream(flow_type='native')
                await asyncio.gather(*[_run_dataflow(dataflow) for dataflow in self._dataflows])
            except asyncio.CancelledError:
                self.logger.warning(f'{self.name} dataflows were cancelled')
        self._clear_dataflows_after_run()
    
    def run(self, ray_kwargs: dict | None=None, prefect_kwargs: dict | None=None):
        return self._eager_run_batch(ray_kwargs=ray_kwargs, prefect_kwargs=prefect_kwargs)
    
    async def arun(self, ray_kwargs: dict | None=None):
        await self._eager_run_stream(ray_kwargs=ray_kwargs)
    
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
    