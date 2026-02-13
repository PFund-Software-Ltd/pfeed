from __future__ import annotations
from typing import TYPE_CHECKING, Callable, ClassVar, Literal, overload
if TYPE_CHECKING:
    from collections.abc import Sequence
    import polars as pl
    from prefect import Flow as PrefectFlow
    from ray.util.queue import Queue
    from pfund.entities.products.product_base import BaseProduct
    from pfeed.sources.data_provider_source import DataProviderSource
    from pfeed.engine import DataEngine
    from pfeed.data_models.base_data_model import BaseDataModel
    from pfeed.typing import GenericData
    from pfeed.requests.base_request import BaseRequest
    from pfeed.storages.base_storage import BaseStorage
    from pfeed.dataflow.dataflow import DataFlow
    from pfeed.dataflow.faucet import Faucet
    from pfeed.dataflow.sink import Sink
    from pfeed.dataflow.result import DataFlowResult
    from pfeed.data_handlers.base_data_handler import BaseMetadata

import logging
from pathlib import Path
from abc import ABC, abstractmethod

from pfund_kit.style import cprint
from pfeed.config import setup_logging, get_config
from pfeed.enums import DataStorage, ExtractType, IOFormat, Compression, DataLayer, FlowType, DataCategory
from pfeed.storages.storage_config import StorageConfig


__all__ = ["BaseFeed"]


config = get_config()


class BaseFeed(ABC):
    data_model_class: ClassVar[type[BaseDataModel]]
    data_domain: ClassVar[DataCategory]
    
    def __init__(
        self, 
        pipeline_mode: bool=False, 
        num_batch_workers: int | None = None,
        num_stream_workers: int | None = None,
    ):
        '''
        Args:
            pipeline_mode: whether to run in pipeline mode
            num_batch_workers: number of Ray tasks to run the batch dataflows in parallel
                when this is not None, Ray will be automatically initialized with the number of CPUs available if ray.init() hasn't been called yet
            num_stream_workers: number of Ray tasks to run the streaming dataflows in parallel
                when this is not None, Ray will be automatically initialized with the number of CPUs available if ray.init() hasn't been called yet
        '''
        setup_logging()
        self.data_source: DataProviderSource = self._create_data_source()
        self.logger = logging.getLogger(f'pfeed.{self.name.lower()}')
        self._engine: DataEngine | None = None
        self._pipeline_mode: bool = pipeline_mode
        self._dataflows: list[DataFlow] = []
        self._failed_dataflows: list[DataFlow] = []
        self._completed_dataflows: list[DataFlow] = []
        self._custom_transformations: Sequence[Callable] = []
        self._current_request: BaseRequest | None = None
        self._storage_options: dict[DataStorage, dict] = {}
        self._io_options: dict[IOFormat, dict] = {}
        self._num_batch_workers: int | None = num_batch_workers
        self._num_stream_workers: int | None = num_stream_workers
        if self._num_batch_workers or self._num_stream_workers:
            from pfeed.utils.ray import setup_ray
            setup_ray()
            
    @property
    def name(self):
        return self.data_source.name
    
    def create_product(self, basis: str, symbol: str='', **specs) -> BaseProduct:
        if not hasattr(self.data_source, 'create_product'):
            raise NotImplementedError(f'{self.data_source.name} does not support creating products')
        return self.data_source.create_product(basis, symbol=symbol, **specs)
    
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
    def _get_default_transformations_for_download(self, *args, **kwargs):
        pass
    
    @abstractmethod
    def _get_default_transformations_for_retrieve(self, *args, **kwargs):
        pass

    @abstractmethod
    def _retrieve_impl(self, data_model: BaseDataModel, *args, **kwargs) -> tuple[pl.LazyFrame | None, BaseMetadata | None]:
        pass

    # TODO
    def _fetch_impl(self, data_model: BaseDataModel, *args, **kwargs) -> GenericData | None:
        raise NotImplementedError(f'{self.name} _fetch_impl() is not implemented')
    
    @abstractmethod
    def run(self, **prefect_kwargs) -> GenericData | None:
        pass
    
    @staticmethod
    def _create_dataflow(data_model: BaseDataModel, faucet: Faucet) -> DataFlow:
        from pfeed.dataflow.dataflow import DataFlow
        dataflow = DataFlow(data_model=data_model, faucet=faucet)
        return dataflow
    
    @staticmethod
    def _create_faucet(
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
        if isinstance(self._custom_transformations, list):
            self._custom_transformations.extend(funcs)
        # NOTE: self._custom_transformations will be converted to a tuple (conceptually sealed) after calling load()
        elif isinstance(self._custom_transformations, tuple):
            raise ValueError('dataflow is sealed, cannot add transformations')
        else:
            raise ValueError(f'unknown custom transformations type: {type(self._custom_transformations)}')
        return self
    
    def _get_default_transformations(self) -> list[Callable]:
        request = self._current_request
        if request.extract_type == ExtractType.download:
            return self._get_default_transformations_for_download()
        elif request.extract_type == ExtractType.stream:
            return self._get_default_transformations_for_stream()
        elif request.extract_type == ExtractType.retrieve:
            return self._get_default_transformations_for_retrieve()
        elif request.extract_type == ExtractType.fetch:
            return self._get_default_transformations_for_fetch()
        else:
            raise ValueError(f'Unknown extract type: {request.extract_type}')
    
    def _add_transformations(self):
        default_transformations = self._get_default_transformations()
        all_transformations = default_transformations + list(self._custom_transformations)
        for dataflow in self._dataflows:
            dataflow.add_transformations(*all_transformations)
        self._custom_transformations = []

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
        to_storage: Literal[DataStorage.LOCAL] = DataStorage.LOCAL,
        data_path: Path | str | None = None,
        data_layer: DataLayer = DataLayer.CLEANED,
        data_domain: str = '',
        io_format: IOFormat = IOFormat.PARQUET,
        compression: Compression = Compression.SNAPPY,
        **io_kwargs,
    ) -> BaseFeed:
        ...
        
    @overload
    def load(
        self, 
        to_storage: Literal[DataStorage.DUCKDB] = DataStorage.DUCKDB,
        data_path: Path | str | None = None,
        data_layer: DataLayer = DataLayer.CLEANED,
        data_domain: str = '',
        in_memory: bool = False,
        memory_limit: str = '4GB',
        **io_kwargs,
    ) -> BaseFeed:
        ...

    def load(
        self,
        storage: DataStorage | None = DataStorage.LOCAL,
        data_path: Path | str | None = None,
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
        
        # allowing passing in None is useful for dynamically determining if load() is needed
        if storage is None:
            return self

        storage_config = StorageConfig(
            storage=storage,
            data_path=data_path,
            data_layer=data_layer,
            data_domain=data_domain,
            io_format=io_format,
            compression=compression,
        )
        
        is_using_ray = self._num_batch_workers or self._num_stream_workers
        if is_using_ray:
            self._check_if_io_supports_parallel_writes(storage_config.io_format)
        
        if self._custom_transformations and storage_config.data_layer == DataLayer.RAW:
            raise RuntimeError(
                'Custom transformations are not allowed when data layer is RAW'
            )
        
        for dataflow in self._dataflows:
            data_model = dataflow.data_model
            Storage = storage_config.storage.storage_class
            storage = (
                Storage(
                    data_path=storage_config.data_path,
                    data_layer=storage_config.data_layer,
                    data_domain=storage_config.data_domain or self.data_domain.value,
                    storage_options=self._storage_options.get(storage_config.storage, {}),
                )
                .with_data_model(data_model)
                .with_io(
                    io_options=self._io_options.get(storage_config.io_format, {}),
                    io_format=storage_config.io_format,
                    compression=storage_config.compression,
                    **io_kwargs
                )
            )
            if isinstance(self, StreamingFeedMixin) and self._streaming_settings:
                storage.data_handler.create_stream_buffer(self._streaming_settings)
            sink: Sink = self._create_sink(data_model, storage)
            dataflow.set_sink(sink)
        
        # conceptually seal the custom transformations by converting it to a tuple after calling load()
        self._custom_transformations = tuple(self._custom_transformations)
        self._current_request.set_loaded()
        return self
    
    def _auto_load(self):
        '''
        if storage_config is created in e.g. download() during pipeline mode, 
        automatically call load() if it hasn't been called yet.
        '''
        if (
            self._current_request and 
            self._current_request.extract_type == ExtractType.download and
            self._current_request.storage_config and 
            not self._current_request.is_loaded
        ):
            storage_config = self._current_request.storage_config
            self.load(
                storage=storage_config.storage,
                data_path=storage_config.data_path,
                data_layer=storage_config.data_layer,
                data_domain=storage_config.data_domain,
                io_format=storage_config.io_format,
                compression=storage_config.compression,
            )
    
    def _clear_dataflows(self):
        self._completed_dataflows.clear()
        self._failed_dataflows.clear()
        self._dataflows.clear()
    
    def _run_batch_dataflows(self, prefect_kwargs: dict) -> tuple[list[DataFlow], list[DataFlow]]:
        from pfund_kit.utils.progress_bar import track, ProgressBar
        from pfeed.utils import is_prefect_running
        
        use_prefect = is_prefect_running()
        self._auto_load()
        self._add_transformations()
        
        def _run_dataflow(dataflow: DataFlow) -> DataFlowResult:
            if use_prefect:
                flow_type = FlowType.prefect
            else:
                flow_type = FlowType.native
            result: DataFlowResult = dataflow.run_batch(flow_type=flow_type, prefect_kwargs=prefect_kwargs)
            return result.success
        
        is_using_ray = self._num_batch_workers
        if is_using_ray:
            import ray
            from pfeed.utils.ray import shutdown_ray, setup_logger_in_ray_task, ray_logging_context
            
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
                    batch_size = self._num_batch_workers
                    dataflow_batches = [self._dataflows[i: i + batch_size] for i in range(0, len(self._dataflows), batch_size)]
                    with ProgressBar(total=len(self._dataflows), description=f'Running {self.name} dataflows') as pbar:
                        for dataflow_batch in dataflow_batches:
                            futures = [
                                ray_task.remote(
                                    logger_name=self.logger.name,
                                    dataflow=dataflow,
                                    log_queue=log_queue,
                                ) for dataflow in dataflow_batch
                            ]
                            unfinished = futures
                            while unfinished:
                                finished, unfinished = ray.wait(unfinished, num_returns=1)
                                for future in finished:
                                    success, dataflow = ray.get(future)
                                    if not success:
                                        self._failed_dataflows.append(dataflow)
                                    else:
                                        self._completed_dataflows.append(dataflow)
                                    pbar.advance(1)
                except KeyboardInterrupt:
                    print(f"KeyboardInterrupt received, stopping {self.name} dataflows...")
                except Exception:
                    self.logger.exception(f'Error in running {self.name} dataflows:')
            self.logger.debug('shutting down ray...')
            shutdown_ray()
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
            self.logger.warning(
                f'{self.name} has {len(self._failed_dataflows)} failed dataflows, check {self.logger.name}.log for more details'
            )
        return self._completed_dataflows, self._failed_dataflows
    
    @property
    def dataflows(self) -> list[DataFlow]:
        is_using_ray = self._num_batch_workers
        if is_using_ray and (self._completed_dataflows or self._failed_dataflows):
            cprint(
                'Accessing `dataflows` after execution with Ray returns the original (pre-execution) dataflows. '
                'Use `completed_dataflows` and `failed_dataflows` for post-execution results.',
                style='bold'
            )
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
    