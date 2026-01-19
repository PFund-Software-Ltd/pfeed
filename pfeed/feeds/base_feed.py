from __future__ import annotations
from typing import TYPE_CHECKING, Callable, Any, Literal, ClassVar, overload
if TYPE_CHECKING:
    import polars as pl
    from prefect import Flow as PrefectFlow
    from pfund.products.product_base import BaseProduct
    from pfeed.sources.base_source import BaseSource
    from pfeed.engine import DataEngine
    from pfeed.data_models.base_data_model import BaseDataModel
    from pfeed.storages.duckdb_storage import DuckDBStorage
    from pfeed.typing import GenericData
    from pfeed.storages.base_storage import BaseStorage
    from pfeed.dataflow.dataflow import DataFlow
    from pfeed.dataflow.faucet import Faucet
    from pfeed.dataflow.sink import Sink
    from pfeed.dataflow.result import FlowResult
    from pfeed.data_handlers.base_data_handler import BaseMetadata
    
import logging
from pathlib import Path
from abc import ABC, abstractmethod
from pprint import pformat

from pfeed.config import setup_logging, get_config
from pfund_kit.style import cprint, TextStyle
from pfeed.enums import DataStorage, ExtractType, IOFormat, Compression, DataLayer, FlowType


__all__ = ["BaseFeed"]


config = get_config()


def clear_subflows(func):
    def wrapper(feed: BaseFeed, *args, **kwargs):
        feed._clear_subflows()
        return func(feed, *args, **kwargs)
    return wrapper
    
    
class BaseFeed(ABC):
    data_model_class: ClassVar[type[BaseDataModel]]
    
    def __init__(self, pipeline_mode: bool=False, **ray_kwargs):
        self.data_source: BaseSource = self._create_data_source()
        self.logger = logging.getLogger(self.name.lower())
        self._engine: DataEngine | None = None
        self._pipeline_mode: bool = pipeline_mode
        self._dataflows: list[DataFlow] = []
        self._subflows: list[DataFlow] = []
        self._failed_dataflows: list[DataFlow] = []
        self._completed_dataflows: list[DataFlow] = []
        self._storage_options: dict[DataStorage, dict] = {}
        self._io_options: dict[IOFormat, dict] = {}
        self._ray_kwargs: dict = ray_kwargs
        if not self._ray_kwargs:
            cprint(
                f'{self.name} is NOT using Ray, consider passing in e.g. Feed(num_cpus=1) to enable Ray', 
                style=TextStyle.BOLD
            )
        else:
            assert 'num_cpus' in self._ray_kwargs, 'num_cpus is required when using Ray'
            self._init_ray()
        setup_logging()
    
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
    def _create_data_source(*args, **kwargs) -> BaseSource:
        pass

    @abstractmethod
    def create_data_model(self, *args, **kwargs) -> BaseDataModel:
        pass

    @abstractmethod
    def _create_batch_dataflows(self, *args, **kwargs) -> list[DataFlow]:
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
    def run(self, prefect_kwargs: dict | None=None) -> GenericData | None:
        pass
    
    def _create_dataflow(self, data_model: BaseDataModel, faucet: Faucet) -> DataFlow:
        from pfeed.dataflow.dataflow import DataFlow
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
    
    @overload
    def load(
        self, 
        to_storage: DataStorage = DataStorage.LOCAL,
        data_layer: DataLayer = DataLayer.CLEANED,
        io_format: IOFormat = IOFormat.PARQUET,
        compression: Compression = Compression.SNAPPY,
    ) -> BaseFeed:
        ...
        
    @overload
    def load(
        self, 
        to_storage: DataStorage = DataStorage.DUCKDB,
        data_layer: DataLayer = DataLayer.CLEANED,
        io_format: Literal[IOFormat.DUCKDB] = IOFormat.DUCKDB,
        in_memory: bool=DuckDBStorage.DEFAULT_IN_MEMORY,
        memory_limit: str=DuckDBStorage.DEFAULT_MEMORY_LIMIT,
    ) -> BaseFeed:
        ...
        
    @overload
    def load(
        self, 
        to_storage: DataStorage = DataStorage.LANCEDB,
        data_layer: DataLayer = DataLayer.CLEANED,
        io_format: Literal[IOFormat.LANCEDB] = IOFormat.LANCEDB,
    ) -> BaseFeed:
        ...

    def load(
        self, 
        to_storage: DataStorage,
        data_layer: DataLayer=DataLayer.CLEANED,
        io_format: IOFormat=IOFormat.DELTALAKE,
        **io_kwargs,
    ) -> BaseFeed:
        '''
        Args:
            **io_kwargs: specific io options for the given storage
                e.g. in_memory, memory_limit, for DuckDBStorage
        '''
        from pfeed.storages.file_based_storage import FileBasedStorage
        from pfeed.feeds.streaming_feed_mixin import StreamingFeedMixin
        
        data_storage = DataStorage[to_storage.upper()]
        data_layer = DataLayer[str(data_layer).upper()]
        io_format = IOFormat[io_format.upper()] if io_format is not None else data_storage.default_io_format
        storage_options = self._storage_options.get(data_storage, {})
        io_options = self._io_options.get(io_format, {})
        # FIXME:
        # if data_layer == DataLayer.RAW and io_format == IOFormat.DELTALAKE:
        #     raise RuntimeError(f'Delta Lake is not supported for {data_layer=}')
        Storage = data_storage.storage_class
        if issubclass(Storage, FileBasedStorage):
            io_kwargs['io_format'] = io_format

        if self._ray_kwargs and (data_storage == DataStorage.DUCKDB):
            raise RuntimeError('DuckDB is not thread-safe, cannot be used with Ray')

        for dataflow in self._subflows:
            data_model = dataflow.data_model
            storage = (
                Storage(
                    data_layer=data_layer,
                    storage_options=storage_options,
                )
                .with_data_model(data_model)
                .with_io(
                    io_options=io_options,
                    **io_kwargs
                )
            )
            if isinstance(self, StreamingFeedMixin) and self._streaming_settings:
                storage.data_handler.create_stream_buffer(self._streaming_settings)
            sink: Sink = self._create_sink(data_model, storage)
            dataflow.set_sink(sink)
        return self
    
    def _clear_dataflows_before_run(self):
        self._completed_dataflows.clear()
        self._failed_dataflows.clear()
    
    def _clear_dataflows_after_run(self):
        self._subflows.clear()
        self._dataflows.clear()
    
    def _run_batch_dataflows(self, prefect_kwargs: dict) -> tuple[list[DataFlow], list[DataFlow]]:
        from pfund_kit.utils.progress_bar import track
        
        use_prefect = prefect_kwargs is not None
        
        def _run_dataflow(dataflow: DataFlow, logger: logging.Logger | None=None) -> FlowResult:
            if use_prefect:
                flow_type = FlowType.prefect
            else:
                flow_type = FlowType.native
            result: FlowResult = dataflow.run_batch(flow_type=flow_type, prefect_kwargs=prefect_kwargs)
            # NOTE: EMPTY dataframe is considered as success
            success = result.data is not None
            return success
        
        self._clear_dataflows_before_run()
        if self._ray_kwargs:
            import ray
            from pfeed.utils.logging import setup_logger_in_ray_task, ray_logging_context
            
            
            @ray.remote
            def ray_task(logger_name: str, dataflow: DataFlow) -> tuple[bool, DataFlow]:
                success = False
                try:
                    logger = setup_logger_in_ray_task(logger_name, log_queue)
                    success = _run_dataflow(dataflow, logger=logger)
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
                                logger_name=self.name.lower(), 
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
        self._clear_dataflows_after_run()
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
        return [dataflow.to_prefect_dataflow(**kwargs) for dataflow in self._dataflows]
    