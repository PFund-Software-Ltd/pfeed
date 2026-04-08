from __future__ import annotations
from typing import TYPE_CHECKING, Callable, ClassVar, Literal, overload, Any, cast
if TYPE_CHECKING:
    from collections.abc import Sequence
    from prefect import Flow as PrefectFlow
    from ray.util.queue import Queue
    from pfund.entities.products.product_base import BaseProduct
    from pfeed.sources.data_provider_source import DataProviderSource
    from pfeed.data_models.base_data_model import BaseDataModel
    from pfeed.typing import GenericData
    from pfeed.requests.base_request import BaseRequest
    from pfeed.dataflow.dataflow import DataFlow
    from pfeed.dataflow.faucet import Faucet
    from pfeed.dataflow.result import DataFlowResult

import os
import logging
from pathlib import Path
from collections import defaultdict
from abc import ABC, abstractmethod

from pfund_kit.style import cprint
from pfeed.config import get_config
from pfeed.enums import DataStorage, ExtractType, IOFormat, Compression, DataLayer, FlowType, DataCategory
from pfeed.storages.storage_config import StorageConfig


__all__ = ["BaseFeed"]


config = get_config()


class BaseFeed(ABC):
    data_model_class: ClassVar[type[BaseDataModel]]
    data_domain: ClassVar[DataCategory]
    
    def __init__(self, pipeline_mode: bool=False, num_workers: int | None = None):
        '''
        Args:
            pipeline_mode: whether to run in pipeline mode
            num_workers: number of Ray tasks to run the batch/streaming dataflows in parallel.
                When provided, Ray will be automatically initialized if ray.init() hasn't been called yet.
                To customize Ray initialization (e.g. specifying num_cpus), call ray.init() explicitly before using this.
        '''
        from pfeed.config import setup_logging
        setup_logging()
        self.data_source: DataProviderSource = self._create_data_source()
        self.logger: logging.Logger = logging.getLogger(f'pfeed.{self.name.lower()}')
        self._pipeline_mode: bool = pipeline_mode
        self._dataflows: dict[BaseRequest, list[DataFlow]] = {}
        self._failed_dataflows: list[DataFlow] = []
        self._completed_dataflows: list[DataFlow] = []
        self._requests: list[BaseRequest] = []
        self._custom_transformations: dict[BaseRequest, Sequence[Callable[..., Any]]] = defaultdict(list)
        self._storage_options: dict[DataStorage, dict[str, Any]] = {}
        self._io_options: dict[IOFormat, dict[str, Any]] = {}
        self._num_workers: int | None = num_workers
        self._is_running: bool = False
        if self._num_workers and self._num_workers > 0:
            num_cpus: int = cast(int, os.cpu_count())
            self._num_workers = min(self._num_workers, num_cpus)
            from pfeed.utils.ray import setup_ray
            setup_ray()
            
    @property
    def _current_request(self) -> BaseRequest | None:
        return self._requests[-1] if self._requests else None

    @property
    def name(self):
        return self.data_source.name
    
    def is_running(self) -> bool:
        return self._is_running
    
    def _set_running(self, is_running: bool) -> None:
        if self.is_running() and is_running:
            raise RuntimeError(f'{self} is already running')
        self._is_running = is_running
    
    def create_product(self, basis: str, name: str='', symbol: str='', **specs: Any) -> BaseProduct:
        if not hasattr(self.data_source, 'create_product'):
            raise NotImplementedError(f'{self.data_source.name} does not support creating products')
        return self.data_source.create_product(basis, name=name, symbol=symbol, **specs)
    
    @staticmethod
    @abstractmethod
    def _create_data_source() -> DataProviderSource:
        pass

    @abstractmethod
    def _create_data_model_from_request(self, request: BaseRequest) -> BaseDataModel:
        pass
    
    @abstractmethod
    def create_data_model(self, *args: Any, **kwargs: Any) -> BaseDataModel:
        pass

    @abstractmethod
    def _create_batch_dataflows(self, *args: Any, **kwargs: Any):
        pass
    
    def configure_io(self, io_format: IOFormat, **io_options: Any) -> BaseFeed:
        self._io_options[IOFormat[io_format.upper()]] = io_options
        return self
    
    def configure_storage(self, storage: DataStorage, storage_options: dict[str, Any]) -> BaseFeed:
        '''Configure storage kwargs for the given storage
        Args:
            storage_options: A dictionary containing configuration options that are universally applicable across different storage systems. These options typically include settings such as connection parameters, authentication credentials, and other general configurations that are not specific to a particular storage type.
        '''
        self._storage_options[DataStorage[storage.upper()]] = storage_options
        return self

    def is_pipeline(self) -> bool:
        return self._pipeline_mode
    
    @abstractmethod
    def run(self, **prefect_kwargs: Any) -> GenericData | None:
        pass
    
    @staticmethod
    def _create_dataflow(data_model: BaseDataModel, faucet: Faucet) -> DataFlow:
        from pfeed.dataflow.dataflow import DataFlow
        dataflow = DataFlow(data_model=data_model, faucet=faucet)
        return dataflow
    
    @staticmethod
    def _create_faucet(
        extract_func: Callable[..., Any], 
        extract_type: ExtractType, 
        data_model: BaseDataModel | None = None,
        close_stream: Callable[..., Any] | None=None,
    ) -> Faucet:
        '''
        Args:
            extract_func: the function to perform the extraction, e.g. _download_impl(), _stream_impl(), _retrieve_impl()
            extract_type: the type of the extraction, e.g. download, stream, retrieve
            data_model: the data model to be used for the dataflow
                It is None for streaming dataflows since streaming dataflows and data models are not one-to-one mapping
            close_stream: a function that closes the streaming dataflow after running the extract_func
        '''
        from pfeed.dataflow.faucet import Faucet
        return Faucet(
            data_model=data_model,
            extract_func=extract_func, 
            extract_type=extract_type,
            close_stream=close_stream,
        )
    
    
    def transform(self, *funcs: Callable[..., Any]) -> BaseFeed:
        assert self._current_request is not None, 'transform() must be called after functions such as stream()/download()/retrieve() in pipeline mode'
        request = self._current_request
        transformations = self._custom_transformations[request]
        if isinstance(transformations, list):
            transformations.extend(funcs)
        # NOTE: self._custom_transformations will be converted to a tuple (conceptually sealed) after calling load()
        elif isinstance(transformations, tuple):
            raise ValueError('dataflow is sealed, cannot add transformations')
        else:
            raise ValueError(f'unknown custom transformations type: {type(transformations)}')
        return self
    
    def _get_default_transformations(self, request: BaseRequest) -> list[Callable[..., Any]]:
        if request.extract_type == ExtractType.download:
            return self._get_default_transformations_for_download(request)
        elif request.extract_type == ExtractType.stream:
            return self._get_default_transformations_for_stream(request)  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType,reportAttributeAccessIssue]
        elif request.extract_type == ExtractType.retrieve:
            return self._get_default_transformations_for_retrieve(request)
        elif request.extract_type == ExtractType.fetch:
            return self._get_default_transformations_for_fetch(request)  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType,reportAttributeAccessIssue]
        else:
            raise ValueError(f'Unknown extract type: {request.extract_type}')

    def _add_transformations(self, request: BaseRequest):
        default_transformations = self._get_default_transformations(request)
        custom_transformations = self._custom_transformations[request]
        all_transformations = default_transformations + list(custom_transformations)
        for dataflow in self._dataflows[request]:
            dataflow.add_transformations(*all_transformations)

    # REVIEW: 
    def _check_if_io_supports_parallel_writes(self, io_format: IOFormat) -> None:
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
        *,
        storage: Literal[DataStorage.LOCAL] = DataStorage.LOCAL,
        data_path: Path | str | None = None,
        data_layer: DataLayer = DataLayer.CLEANED,
        data_domain: str = '',
        io_format: IOFormat = IOFormat.PARQUET,
        compression: Compression = Compression.SNAPPY,
        **io_kwargs: Any,
    ) -> BaseFeed:
        ...
        
    @overload
    def load(
        self, 
        *,
        storage: Literal[DataStorage.DUCKDB] = DataStorage.DUCKDB,
        data_path: Path | str | None = None,
        data_layer: DataLayer = DataLayer.CLEANED,
        data_domain: str = '',
        in_memory: bool = False,
        memory_limit: str = '4GB',
        **io_kwargs: Any,
    ) -> BaseFeed:
        ...

    def load(  
        self,
        *,
        storage: DataStorage | None = DataStorage.LOCAL,
        data_path: Path | str | None = None,
        data_layer: DataLayer = DataLayer.CLEANED,
        data_domain: str = '',
        io_format: IOFormat = IOFormat.PARQUET,
        compression: Compression = Compression.SNAPPY,
        **io_kwargs: Any,
    ) -> BaseFeed:
        '''
        Args:
            **io_kwargs: specific io options for the given storage
                e.g. in_memory, memory_limit, for DuckDBStorage
        '''
        assert self._current_request is not None, 'load() must be called after functions such as stream()/download()/retrieve() in pipeline mode'
        request = self._current_request
        
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
        
        is_using_ray = bool(self._num_workers)
        if is_using_ray:
            self._check_if_io_supports_parallel_writes(storage_config.io_format)
        
        custom_transformations = self._custom_transformations[request]
        if custom_transformations and storage_config.data_layer == DataLayer.RAW:
            raise RuntimeError(
                'Custom transformations are not allowed when data layer is RAW'
            )

        for dataflow in self._dataflows[request]:
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
            if request.is_streaming():
                storage.data_handler.create_stream_buffer(
                    mode=request.mode,
                    flush_interval=request.flush_interval,
                )
            dataflow.set_storage(storage)

        # conceptually seal the custom transformations by converting it to a tuple after calling load()
        self._custom_transformations[request] = tuple(custom_transformations)
        request.set_loaded()
        return self
    
    def _auto_load(self, request: BaseRequest):
        '''
        if storage_config is created in e.g. download() during pipeline mode,
        automatically call load() if it hasn't been called yet.
        '''
        if (
            request.extract_type == ExtractType.download and
            request.storage_config and
            not request.is_loaded
        ):
            storage_config = request.storage_config
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
        
    def _prepare_before_run(self):
        for request in self._requests:
            self._auto_load(request)
            self._add_transformations(request)
        self._clear_dataflows()
        self._set_running(True)
    
    def _reset_after_run(self):
        self._requests.clear()
        self._dataflows = {}
        self._custom_transformations = defaultdict(list)
        self._set_running(False)
    
    def _run_batch_dataflows(self, prefect_kwargs: dict[str, Any]) -> tuple[list[DataFlow], list[DataFlow]]:
        from pfund_kit.utils.progress_bar import track, ProgressBar
        from pfeed.utils import is_prefect_running
        
        use_prefect = is_prefect_running()
        self._prepare_before_run()

        def _run_dataflow(dataflow: DataFlow) -> DataFlowResult:
            if use_prefect:
                flow_type = FlowType.prefect
            else:
                flow_type = FlowType.native
            result: DataFlowResult = dataflow.run_batch(flow_type=flow_type, prefect_kwargs=prefect_kwargs)
            return result.success
        
        is_using_ray = bool(self._num_workers)
        if is_using_ray:
            import ray
            from pfeed.utils.ray import shutdown_ray, setup_logger_in_ray_task, ray_logging_context
            
            @ray.remote
            def ray_task(logger_name: str, dataflow: DataFlow, log_queue: Queue) -> tuple[bool, DataFlow]:
                success = False
                logger = setup_logger_in_ray_task(logger_name, log_queue)
                try:
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
                    batch_size = self._num_workers
                    dataflow_batches = [self.dataflows[i: i + batch_size] for i in range(0, len(self.dataflows), batch_size)]
                    with ProgressBar(total=len(self.dataflows), description=f'Running {self.name} dataflows') as pbar:
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
                    self.logger.warning(f"KeyboardInterrupt received, stopping {self.name} dataflows...")
                except Exception:
                    self.logger.exception(f'Error in running {self.name} dataflows:')
            self.logger.debug('shutting down ray...')
            shutdown_ray()
        else:
            try:
                for dataflow in track(self.dataflows, description=f'Running {self.name} dataflows'):
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
            only_retrieval_dataflows = all(dataflow.extract_type == ExtractType.retrieve for dataflow in self._failed_dataflows)
            if only_retrieval_dataflows:
                log_level = logging.DEBUG
            else:
                log_level = logging.WARNING
            self.logger.log(
                log_level,
                f'{self.name} has {len(self._failed_dataflows)} failed dataflows, check {self.logger.name}.log for more details'
            )

        self._reset_after_run()
        return self._completed_dataflows, self._failed_dataflows
    
    @property
    def dataflows(self) -> list[DataFlow]:
        is_using_ray = bool(self._num_workers)
        if is_using_ray and (self._completed_dataflows or self._failed_dataflows):
            cprint(
                'Accessing `dataflows` after execution with Ray returns the original (pre-execution) dataflows. ' +
                'Use `completed_dataflows` and `failed_dataflows` for post-execution results.',
                style='bold'
            )
        return [df for dfs in self._dataflows.values() for df in dfs]

    @property
    def failed_dataflows(self) -> list[DataFlow]:
        """Returns list of dataflows that failed in the last run"""
        return self._failed_dataflows

    @property
    def completed_dataflows(self) -> list[DataFlow]:
        """Returns list of dataflows that completed successfully in the last run"""
        return self._completed_dataflows
    
    def to_prefect_dataflows(self, **kwargs: Any) -> list[PrefectFlow]:
        '''
        Args:
            kwargs: kwargs specific to prefect @flow decorator
        '''
        self._prepare_before_run()
        return [dataflow.to_prefect_dataflow(**kwargs) for dataflow in self.dataflows]
    