from __future__ import annotations
from typing import TYPE_CHECKING, Callable, Any, ClassVar, overload
if TYPE_CHECKING:
    import polars as pl
    from prefect import Flow as PrefectFlow
    from ray.util.queue import Queue
    from pfund.products.product_base import BaseProduct
    from pfeed.sources.data_provider_source import DataProviderSource
    from pfeed.engine import DataEngine
    from pfeed.data_models.base_data_model import BaseDataModel
    from pfeed.typing import GenericData
    from pfeed.requests.base_request import BaseRequest
    from pfeed.storages.base_storage import BaseStorage
    from pfeed.dataflow.dataflow import DataFlow
    from pfeed.dataflow.faucet import Faucet
    from pfeed.dataflow.sink import Sink
    from pfeed.dataflow.result import FlowResult
    from pfeed.data_handlers.base_data_handler import BaseMetadata

import os
import logging
from pathlib import Path
from abc import ABC, abstractmethod
from pprint import pformat

from pfeed.config import setup_logging, get_config
from pfeed.enums import DataStorage, ExtractType, IOFormat, Compression, DataLayer, FlowType, DataCategory
from pfeed.requests.load_request import LoadRequest


__all__ = ["BaseFeed"]


config = get_config()


class BaseFeed(ABC):
    data_model_class: ClassVar[type[BaseDataModel]]
    data_domain: ClassVar[DataCategory]
    
    def __init__(self, pipeline_mode: bool=False, **ray_kwargs):
        setup_logging()
        self.data_source: DataProviderSource = self._create_data_source()
        self.logger = logging.getLogger(f'pfeed.{self.name.lower()}')
        self._engine: DataEngine | None = None
        self._pipeline_mode: bool = pipeline_mode
        self._dataflows: list[DataFlow] = []
        self._failed_dataflows: list[DataFlow] = []
        self._completed_dataflows: list[DataFlow] = []
        self._pending_transformations: list[Callable] = []
        self._current_request: BaseRequest | None = None
        self._load_request: LoadRequest | None = None
        self._storage_options: dict[DataStorage, dict] = {}
        self._io_options: dict[IOFormat, dict] = {}
        self._ray_kwargs: dict = ray_kwargs
        if self._ray_kwargs:
            assert 'num_cpus' in self._ray_kwargs, 'num_cpus is required when using Ray'
            self._init_ray()
            
    @property
    def name(self):
        return self.data_source.name
    
    def create_product(self, basis: str, symbol: str='', **specs) -> BaseProduct:
        if not hasattr(self.data_source, 'create_product'):
            raise NotImplementedError(f'{self.data_source.name} does not support creating products')
        return self.data_source.create_product(basis, symbol=symbol, **specs)
    
    @abstractmethod
    def _normalize_raw_data(self, data: Any) -> Any:
        pass
    
    @staticmethod
    @abstractmethod
    def _create_data_source(*args, **kwargs) -> DataProviderSource:
        pass

    @abstractmethod
    def _create_data_model_from_request(self, request: BaseRequest) -> BaseDataModel:
        pass
    
    @abstractmethod
    def create_data_model(self, *args, **kwargs) -> BaseDataModel:
        pass

    @abstractmethod
    def _create_batch_dataflows(self, *args, **kwargs):
        pass
    
    def configure_io(self, io_format: IOFormat, **io_options) -> BaseFeed:
        self._io_options[io_format] = io_options
        return self
    
    def configure_storage(self, storage: DataStorage, storage_options: dict) -> BaseFeed:
        '''Configure storage kwargs for the given storage
        Args:
            storage_options: A dictionary containing configuration options that are universally applicable across different storage systems. These options typically include settings such as connection parameters, authentication credentials, and other general configurations that are not specific to a particular storage type.
        '''
        self._storage_options[storage] = storage_options
        return self

    def is_pipeline(self) -> bool:
        return self._pipeline_mode

    def _init_ray(self):
        import ray
        import atexit
        if not ray.is_initialized():
            # disable this warning: FutureWarning: Tip: In future versions of Ray, Ray will no longer override accelerator visible devices env var if num_gpus=0 or num_gpus=None (default).
            os.environ["RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO"] = "0"
            ray.init(**self._ray_kwargs)
            atexit.register(lambda: ray.shutdown())  # useful in jupyter notebook environment

    def _shutdown_ray(self):
        import ray
        if ray.is_initialized():
            ray.shutdown()
    
    def _set_engine(self, engine: DataEngine) -> None:
        self._engine = engine

    @abstractmethod
    def download(self, *args, **kwargs) -> GenericData | None | BaseFeed:
        pass
   
    @abstractmethod
    def retrieve(self, *args, **kwargs) -> GenericData | None:
        pass
    
    # TODO: maybe integrate it with llm call? e.g. fetch("get news of AAPL")
    def fetch(self, *args, **kwargs) -> GenericData | None | BaseFeed:
        raise NotImplementedError(f'{self.name} fetch() is not implemented')
    
    @abstractmethod
    def _download_impl(self, data_model: BaseDataModel) -> GenericData | None:
        pass

    @abstractmethod
    def _add_default_transformations_to_download(self, *args, **kwargs):
        pass
    
    @abstractmethod
    def _add_default_transformations_to_retrieve(self, *args, **kwargs):
        pass

    def _retrieve_impl(
        self,
        data_path: Path,
        data_model: BaseDataModel, 
        data_layer: DataLayer,
        data_domain: str,
        from_storage: DataStorage,
        io_format: IOFormat,
    ) -> tuple[pl.LazyFrame | None, BaseMetadata]:
        '''Retrieves data by searching through all local storages, using polars for scanning data'''
        data_storage = DataStorage[from_storage.upper()]
        io_format = IOFormat[io_format.upper()]
        storage_options = self._storage_options.get(data_storage, {})
        io_options = self._io_options.get(io_format, {})
        self.logger.debug(f'searching for data {data_model} in {data_storage} ({data_layer=})...')
        Storage = data_storage.storage_class
        try:
            storage = (
                Storage(
                    data_path=data_path,
                    data_layer=data_layer,
                    data_domain=data_domain,
                    storage_options=storage_options,
                )
                .with_data_model(data_model)
                .with_io(
                    io_format, 
                    io_options=io_options,
                )
            )
            data, metadata = storage.read_data()
            if data is not None:
                self.logger.info(f'found data {data_model} in {data_storage} ({data_layer=})')
            else:
                self.logger.debug(f'failed to find data {data_model} in {data_storage} ({data_layer=})')
        except Exception:
            self.logger.exception(f'Error in retrieving data {data_model} in {data_storage} ({data_layer=}):')
        return data, metadata

    # TODO
    def _fetch_impl(self, data_model: BaseDataModel, *args, **kwargs) -> GenericData | None:
        raise NotImplementedError(f'{self.name} _fetch_impl() is not implemented')
    
    @abstractmethod
    def run(self, **prefect_kwargs) -> GenericData | None:
        pass
    
    def _create_dataflow(self, data_model: BaseDataModel, faucet: Faucet) -> DataFlow:
        from pfeed.dataflow.dataflow import DataFlow
        dataflow = DataFlow(data_model=data_model, faucet=faucet)
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
        from pfeed.dataflow.faucet import Faucet
        return Faucet(
            data_model=data_model,
            extract_func=extract_func, 
            extract_type=extract_type,
            close_stream=close_stream,
        )
    
    @staticmethod
    def _create_sink(data_model: BaseDataModel, storage: BaseStorage) -> Sink:
        from pfeed.dataflow.sink import Sink
        return Sink(data_model, storage)
    
    def transform(self, *funcs) -> BaseFeed:
        is_default_transformations = all(func.__name__.startswith('__') for func in funcs)
        if is_default_transformations:
            for dataflow in self._dataflows:
                dataflow.add_transformations(*funcs)
        else:
            self._pending_transformations.extend(funcs)
        return self
    
    def _add_default_transformations(self):
        if self._current_request.request_type == ExtractType.download:
            self._add_default_transformations_to_download()
        elif self._current_request.request_type == ExtractType.stream:
            self._add_default_transformations_to_stream()
        elif self._current_request.request_type == ExtractType.retrieve:
            self._add_default_transformations_to_retrieve()
        elif self._current_request.request_type == ExtractType.fetch:
            self._add_default_transformations_to_fetch()
        else:
            raise ValueError(f'Unknown request type: {self._current_request.request_type}')
    
    def _flush_pending_transformations(self):
        if self._pending_transformations and self._load_request.data_layer != DataLayer.CURATED:
            raise RuntimeError(
                'Custom transformations are only allowed when data layer is CURATED'
            )
        for dataflow in self._dataflows:
            dataflow.add_transformations(*self._pending_transformations)
        self._pending_transformations.clear()

    # REVIEW: 
    def _check_if_io_supports_parallel_writes(self, io_format: IOFormat) -> bool:
        from pfeed._io.file_io import FileIO
        IO = io_format.io_class
        # check if not supports parallel writes and not a file io
        # assume that if it's a file io, it supports parallel writes to multiple files
        # e.g. ParquetIO doesn't support parallel writes to a single file but supports parallel writes to multiple files
        if not IO.SUPPORTS_PARALLEL_WRITES and IO.__bases__[0] is not FileIO:
            raise RuntimeError(f'{IO.__name__} does not support parallel writes, cannot be used with Ray')

    @overload
    def load(
        self, 
        to_storage: DataStorage = DataStorage.DUCKDB,
        data_layer: DataLayer = DataLayer.CLEANED,
        data_domain: str = '',
        in_memory: bool=True,
        memory_limit: str='4GB',
    ) -> BaseFeed:
        ...
        
    def load(
        self, 
        to_storage: DataStorage = DataStorage.LOCAL,
        data_layer: DataLayer = DataLayer.CLEANED,
        data_domain: str = '',
        io_format: IOFormat = IOFormat.PARQUET,
        compression: Compression = Compression.SNAPPY,
        **io_kwargs,
    ) -> BaseFeed:
        '''
        Args:
            **io_kwargs: specific io options for the given storage
                e.g. in_memory, memory_limit, for DuckDBStorage
        '''
        from pfeed.feeds.streaming_feed_mixin import StreamingFeedMixin
        
        self._load_request = LoadRequest(
            to_storage=to_storage,
            data_layer=data_layer,
            data_domain=data_domain,
            io_format=io_format,
            compression=compression,
        )
        # NOTE: need to confirm the final destination before adding any transformations
        # so transformations are applied only after the load request above
        self._add_default_transformations()
        self._flush_pending_transformations()
        if self._ray_kwargs:
            self._check_if_io_supports_parallel_writes(self._load_request.io_format)
        Storage = self._load_request.to_storage.storage_class
        for dataflow in self._dataflows:
            data_model = dataflow.data_model
            storage = (
                Storage(
                    data_layer=self._load_request.data_layer,
                    data_domain=self._load_request.data_domain or self.data_domain.value,
                    storage_options=self._storage_options.get(self._load_request.to_storage, {}),
                )
                .with_data_model(data_model)
                .with_io(
                    io_options=self._io_options.get(self._load_request.io_format, {}),
                    io_format=self._load_request.io_format,
                    compression=self._load_request.compression,
                    **io_kwargs
                )
            )
            if isinstance(self, StreamingFeedMixin) and self._streaming_settings:
                storage.data_handler.create_stream_buffer(self._streaming_settings)
            sink: Sink = self._create_sink(data_model, storage)
            dataflow.set_sink(sink)
        self._load_request = None
        return self
    
    def _auto_load(self):
        '''
        Automatically call load() if it hasn't been called yet
        '''
        has_sink = any(dataflow.sink is not None for dataflow in self._dataflows)
        if not has_sink:
            if self._load_request and self._load_request.to_storage is not None:
                self.load(
                    to_storage=self._load_request.to_storage,
                    data_layer=self._load_request.data_layer,
                    data_domain=self._load_request.data_domain,
                    io_format=self._load_request.io_format,
                    compression=self._load_request.compression,
                )
            else:
                self._flush_pending_transformations()
    
    def _clear_dataflows(self):
        self._completed_dataflows.clear()
        self._failed_dataflows.clear()
        self._dataflows.clear()
    
    def _is_prefect_running(self) -> bool:
        import httpx

        PREFECT_API_URL = os.getenv('PREFECT_API_URL', 'http://127.0.0.1:4200/api').rstrip('/')
        if not PREFECT_API_URL.startswith('http'):
            PREFECT_API_URL = f'http://{PREFECT_API_URL}'
        try:
            response = httpx.get(f'{PREFECT_API_URL}/health', timeout=2.0)
            return response.status_code == 200
        except Exception:
            # Catch all exceptions - if we can't verify Prefect is running, assume it's not
            return False
    
    def _run_batch_dataflows(self, prefect_kwargs: dict) -> tuple[list[DataFlow], list[DataFlow]]:
        from pfund_kit.utils.progress_bar import track
        
        use_prefect = self._is_prefect_running()
        self._auto_load()
        
        def _run_dataflow(dataflow: DataFlow) -> FlowResult:
            if use_prefect:
                flow_type = FlowType.prefect
            else:
                flow_type = FlowType.native
            result: FlowResult = dataflow.run_batch(flow_type=flow_type, prefect_kwargs=prefect_kwargs)
            # NOTE: EMPTY dataframe is considered as success
            success = result.data is not None
            return success
        
        if self._ray_kwargs:
            import ray
            from pfeed.utils.ray_logging import setup_logger_in_ray_task, ray_logging_context
            
            @ray.remote
            def ray_task(logger_name: str, dataflow: DataFlow, log_queue: Queue) -> tuple[bool, DataFlow]:
                success = False
                try:
                    logger = setup_logger_in_ray_task(logger_name, log_queue)
                    success = _run_dataflow(dataflow)
                except RuntimeError as err:
                    if use_prefect:
                        logger.exception(f'Error in running prefect {dataflow}:')
                    else:
                        raise err
                except Exception:
                    logger.exception(f'Error in running {dataflow}:')
                return success, dataflow
            
            with ray_logging_context(self.logger) as log_queue:
                try:
                    batch_size = self._ray_kwargs['num_cpus']
                    dataflow_batches = [self._dataflows[i: i + batch_size] for i in range(0, len(self._dataflows), batch_size)]
                    for dataflow_batch in track(dataflow_batches, description=f'Running {self.name} dataflows'):
                        futures = [
                            ray_task.remote(
                                logger_name=self.logger.name,
                                dataflow=dataflow,
                                log_queue=log_queue,
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
            self.logger.debug('shutting down ray...')
            self._shutdown_ray()
        else:
            try:
                for dataflow in track(self._dataflows, description=f'Running {self.name} dataflows'):
                    success = _run_dataflow(dataflow)
                    if not success:
                        self._failed_dataflows.append(dataflow)
                    else:
                        self._completed_dataflows.append(dataflow)
            except RuntimeError as err:
                if use_prefect:
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
        return self._completed_dataflows, self._failed_dataflows
    
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
        # execute the deferred LoadRequest created by e.g. download() if load() hasn't been called yet
        self._auto_load()
        return [dataflow.to_prefect_dataflow(**kwargs) for dataflow in self._dataflows]
    