from __future__ import annotations
from typing import TYPE_CHECKING, Literal, Callable
from types import ModuleType
if TYPE_CHECKING:
    import pandas as pd
    from prefect import Flow as PrefectFlow
    from bytewax.dataflow import Dataflow as BytewaxDataFlow
    from bytewax.inputs import Source as BytewaxSource
    from bytewax.outputs import Sink as BytewaxSink
    from bytewax.dataflow import Stream as BytewaxStream
    from pfund.products.product_base import BaseProduct
    from pfeed.data_models.base_data_model import BaseDataModel
    from pfeed.typing.core import tData
    from pfeed.typing.literals import tSTORAGE, tDATA_TOOL, tDATA_LAYER
    from pfeed.const.enums import DataSource
    from pfeed.sources.base_source import BaseSource
    from pfeed.storages.base_storage import BaseStorage
    
import os
from abc import ABC, abstractmethod
import importlib
import datetime
import logging
from logging.handlers import QueueHandler, QueueListener
from pprint import pformat

from pfeed.flows.faucet import Faucet
from pfeed.config import get_config
from pfeed.const.enums import DataTool, DataStorage
from pfeed.flows.dataflow import DataFlow
from pfeed.utils.utils import rollback_date_range


__all__ = ["BaseFeed"]
        

def clear_subflows(func):
    def wrapper(feed: BaseFeed, *args, **kwargs):
        feed._clear_subflows()
        return func(feed, *args, **kwargs)
    return wrapper
    
    
class BaseFeed(ABC):
    DATA_DOMAIN = 'general_data'
    
    def __init__(
        self, 
        data_source: BaseSource | None=None,
        data_tool: tDATA_TOOL='polars', 
        pipeline_mode: bool=False,
        use_ray: bool=True,
        use_prefect: bool=False,
        use_bytewax: bool=False,
        use_deltalake: bool=False,
    ):
        '''
        Args:
            storage_configs: storage specific kwargs, e.g. if storage is 'minio', kwargs are minio specific kwargs
        '''
        from pfund.plogging import set_up_loggers
        self.config = get_config()
        is_loggers_set_up = bool(logging.getLogger('pfeed').handlers)
        if not is_loggers_set_up:
            set_up_loggers(self.config.log_path, self.config.logging_config_file_path, user_logging_config=self.config.logging_config)
        self.data_tool: ModuleType = importlib.import_module(f'pfeed.data_tools.data_tool_{DataTool[data_tool.lower()]}')
        if data_source is None:
            assert hasattr(self, '_create_data_source'), '_create_data_source() is not implemented'
            self.data_source: BaseSource = self._create_data_source()
        else:
            self.data_source: BaseSource = data_source
        self.name: DataSource = self.data_source.name
        self.logger = logging.getLogger(self.name.lower() + '_data')

        self._pipeline_mode = pipeline_mode
        self._use_ray = use_ray
        self._use_prefect = use_prefect
        self._use_bytewax = use_bytewax
        
        # FIXME:
        assert not self._use_bytewax, 'Bytewax is not supported yet'
        
        if self._use_prefect and self._use_bytewax:
            raise ValueError('Cannot use both prefect and bytewax at the same time')
        self._use_deltalake = use_deltalake
        self._dataflows: list[DataFlow] = []
        self._subflows: list[DataFlow] = []
        self._failed_dataflows: list[DataFlow] = []
        self._completed_dataflows: list[DataFlow] = []
        self._storage_configs: dict[tSTORAGE, dict] = {}
    
    @property
    def api(self):
        return self.data_source.api if hasattr(self.data_source, 'api') else None
    
    @staticmethod
    @abstractmethod
    def _normalize_raw_data(df: pd.DataFrame) -> pd.DataFrame:
        pass
    
    @abstractmethod
    def create_data_model(self, *args, **kwargs) -> BaseDataModel:
        pass

    @abstractmethod
    def create_storage(self, *args, **kwargs) -> BaseStorage:
        pass
    
    def create_product(self, product_basis: str, symbol: str='', **product_specs) -> BaseProduct:
        return self.data_source.create_product(product_basis, symbol=symbol, **product_specs)

    def configure_storage(self, storage: tSTORAGE, storage_configs: dict) -> BaseFeed:
        '''Configure storage kwargs for the given storage
        Args:
            storage_configs: storage specific kwargs, e.g. if storage is 'minio', kwargs are minio specific kwargs
        '''
        self._storage_configs[storage] = storage_configs
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
    def download(self, *args, **kwargs) -> tData | None | BaseFeed:
        pass
    
    def stream(
        self, 
        # NOTE: supportts BytewaxStream, useful for users passing in BytewaxStream objects, e.g.:
        # merge_stream = op.merge('merge', op.input("input1", ...), op.input("input2", ...))
        bytewax_source: BytewaxSource | BytewaxStream | str | None=None,
        *args, 
        **kwargs,
    ) -> BaseFeed:
        raise NotImplementedError(f"{self.name} stream() is not implemented")

    @abstractmethod
    def retrieve(self, *args, **kwargs) -> tData | None:
        pass
    
    # TODO: maybe integrate it with llm call? e.g. fetch("get news of AAPL")
    def fetch(self, *args, **kwargs) -> tData | None | BaseFeed:
        raise NotImplementedError(f'{self.name} fetch() is not implemented')
    
    def get_historical_data(self, *args, **kwargs) -> tData | None:
        raise NotImplementedError(f'{self.name} get_historical_data() is not implemented')
    
    def get_realtime_data(self, *args, **kwargs) -> tData | None:
        raise NotImplementedError(f'{self.name} get_realtime_data() is not implemented')
    
    @abstractmethod
    def _create_download_dataflows(self, *args, **kwargs) -> list[DataFlow]:
        pass
    
    @abstractmethod
    def _execute_download(self, data_model: BaseDataModel) -> tData:
        pass
    
    def _execute_stream(
        self, 
        data_model: BaseDataModel, 
        bytewax_source: BytewaxSource | BytewaxStream | str | None=None,
    ) -> tData | BytewaxSource | BytewaxStream:
        raise NotImplementedError(f"{self.name} _execute_stream() is not implemented")

    def _execute_retrieve(
        self,
        data_model: BaseDataModel, 
        data_layer: tDATA_LAYER, 
        data_domain: str, 
        from_storage: tSTORAGE | None=None,
        storage_configs: dict | None=None,
    ) -> tData | None:
        search_storages = ['cache', 'local', 'minio', 'duckdb'] if from_storage is None else [from_storage]
        data = None
        storage_configs = storage_configs or {}
        if storage_configs:
            assert from_storage is not None, 'from_storage is required when storage_configs is provided'
        for search_storage in search_storages:
            self.logger.debug(f'searching for data {data_model} in {search_storage.upper()}...')
            search_storage_configs = storage_configs or self._storage_configs.get(search_storage, {})
            Storage = DataStorage[search_storage.upper()].storage_class
            try:
                storage: BaseStorage = Storage.from_data_model(
                    data_model,
                    data_layer, 
                    data_domain,
                    use_deltalake=self._use_deltalake,
                    **search_storage_configs,
                )
                data = storage.read_data(data_tool=self.data_tool.name)
                if data is not None:
                    self.logger.debug(f'found data {data_model} in {search_storage.upper()}')
                    break
            except Exception as e:  # e.g. minio's ServerError if server is not running
                continue
        return data

    # TODO
    def _execute_fetch(self, data_model: BaseDataModel, *args, **kwargs) -> tData | None:
        raise NotImplementedError(f'{self.name} _execute_fetch() is not implemented')
    
    def _standardize_dates(self, start_date: str, end_date: str, rollback_period: str | Literal['ytd', 'max']) -> tuple[datetime.date, datetime.date]:
        '''Standardize start_date and end_date based on input parameters.

        Args:
            start_date: Start date string in YYYY-MM-DD format.
                If not provided, will be determined by rollback_period.
            end_date: End date string in YYYY-MM-DD format.
                If not provided and start_date is provided, defaults to yesterday.
                If not provided and start_date is not provided, will be determined by rollback_period.
            rollback_period: Period to rollback from today if start_date is not provided.
                Can be a period string like '1d', '1w', '1m', '1y' etc.
                Or 'ytd' to use the start date of the current year.
                Or 'max' to use data source's start_date if available.

        Returns:
            tuple[datetime.date, datetime.date]: Standardized (start_date, end_date)

        Raises:
            ValueError: If rollback_period='max' but data source has no start_date attribute
        '''
        if start_date:
            start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
            if end_date:
                end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()
            else:
                yesterday = datetime.datetime.now(tz=datetime.timezone.utc).date() - datetime.timedelta(days=1)
                end_date = yesterday
        else:
            if rollback_period == 'max':
                if self.data_source.start_date:
                    start_date = self.data_source.start_date
                else:
                    raise ValueError(f'{self.name} {rollback_period=} is not supported')
            else:
                start_date, end_date = rollback_date_range(rollback_period)
        return start_date, end_date
    
    def create_dataflow(self, faucet: Faucet) -> DataFlow:
        dataflow = DataFlow(faucet)
        self._subflows.append(dataflow)
        self._dataflows.append(dataflow)
        return dataflow
    
    @staticmethod
    def create_faucet(data_model: BaseDataModel, execute_func: Callable, op_type: Literal['download', 'stream', 'retrieve', 'fetch']) -> Faucet:
        return Faucet(data_model, execute_func, op_type)
    
    def _clear_subflows(self):
        '''Clear subflows
        This is necessary to allow the following behaviour:
        download(...).transform(...).load(...).stream(...).transform(...).load(...)
        1. subflows: download(...).transform(...).load(...)
        2. clear subflows so that the operations of the next batch of dataflows are independent of the previous batch
        3. subflows: stream(...).transform(...).load(...)
        '''
        self._subflows.clear()
    
    def _extract_download(self, data_model: BaseDataModel) -> DataFlow:
        faucet = self.create_faucet(data_model, lambda: self._execute_download(data_model), op_type='download')
        return self.create_dataflow(faucet)
    
    def _extract_stream(
        self, 
        data_model: BaseDataModel, 
        bytewax_source: BytewaxSource | BytewaxStream | str | None=None,
    ) -> DataFlow:
        faucet = self.create_faucet(
            data_model, 
            lambda: self._execute_stream(data_model, bytewax_source=bytewax_source), 
            op_type='stream',
        )
        return self.create_dataflow(faucet)
    
    def _extract_retrieve(
        self, 
        data_model: BaseDataModel,
        data_layer: tDATA_LAYER,
        data_domain: str,
        from_storage: tSTORAGE | None=None,
        storage_configs: dict | None=None,
    ) -> DataFlow:
        faucet = self.create_faucet(
            data_model, 
            lambda: self._execute_retrieve(
                data_model,
                data_layer,
                data_domain,
                from_storage=from_storage,
                storage_configs=storage_configs,
            ),
            op_type='retrieve',
        )
        return self.create_dataflow(faucet)
    
    def _extract_fetch(self, data_model: BaseDataModel) -> DataFlow:
        faucet = self.create_faucet(data_model, lambda: self._execute_fetch(data_model), op_type='fetch')
        return self.create_dataflow(faucet)
    
    def transform(self, *funcs) -> BaseFeed:
        for dataflow in self._subflows:
            dataflow.add_transformations(*funcs)
        return self
    
    def load(
        self, 
        to_storage: tSTORAGE='local',   
        data_layer: tDATA_LAYER='curated',
        data_domain: str='',
        storage_configs: dict | None=None,
        bytewax_sink: BytewaxSink | str | None=None,
    ) -> BaseFeed:
        '''
        Args:
            data_domain: custom domain of the data, used in data_path/data_layer/data_domain
                useful for grouping data
        '''
        if self._use_ray:
            assert to_storage.lower() != 'duckdb', 'DuckDB is not thread-safe, cannot be used with Ray'
        storage_configs = storage_configs or self._storage_configs.get(to_storage, {})
        Storage = DataStorage[to_storage.upper()].storage_class

        # NOTE: lazy creation of storage to avoid pickle errors when using ray
        # e.g. minio client is using socket, which is not picklable
        def _create_storage(data_model: BaseDataModel):
            return Storage.from_data_model(
                data_model,
                data_layer,
                data_domain or self.DATA_DOMAIN,
                use_deltalake=self._use_deltalake,
                **storage_configs,
            )

        for dataflow in self._subflows:
            dataflow.lazy_create_storage(_create_storage)
            if self._use_bytewax:
                from bytewax.connectors.stdio import StdOutSink
                dataflow.set_bytewax_sink(bytewax_sink or StdOutSink())
        return self
    
    def run(self, ray_kwargs: dict | None=None) -> tuple[list[DataFlow], list[DataFlow]]:
        '''Run dataflows'''
        from tqdm import tqdm
        from pfeed.utils.utils import generate_color
        dataflows = self.to_prefect_dataflows() if self._use_prefect else self._dataflows
        if self._use_bytewax:
            dataflows.append(self.to_bytewax_dataflow())
        completed_dataflows: list[DataFlow] = []
        failed_dataflows: list[DataFlow] = []
        color = generate_color(self.name.value)
        prefect_error_msg = 'Error in running {name} prefect dataflows: {err}, did you forget to run "prefect server start" to start prefect\'s server?'

        def _handle_result(res: tData | None) -> bool:
            if not self._use_prefect:
                success = False if res is None else True
            else:
                # for prefect, use dashboard to check failed dataflows
                # FIXME:
                success = True
            return success
        
        if self._use_ray:
            import atexit
            import ray
            from ray.util.queue import Queue
            atexit.register(lambda: ray.shutdown())  # useful in jupyter notebook environment
            
            @ray.remote
            def ray_task(dataflow) -> bool:
                try:
                    if not self.logger.handlers:
                        self.logger.addHandler(QueueHandler(log_queue))
                        self.logger.setLevel(logging.DEBUG)
                        # needs this to avoid triggering the root logger's stream handlers with level=DEBUG
                        self.logger.propagate = False
                        
                    if self._use_prefect:
                        res: tData | None = dataflow()
                    elif self._use_bytewax:
                        # REVIEW, using run_main() to execute the dataflow in the current thread, NOT for production
                        from bytewax.testing import run_main
                        run_main(dataflow)
                    else:
                        res: tData | None = dataflow.run()
                    
                    success = _handle_result(res)
                    return success, dataflow
                except RuntimeError as err:
                    if self._use_prefect:
                        self.logger.error(prefect_error_msg.format(name=self.name, err=err))
                    else:
                        raise err
                    return False, dataflow
                except Exception:
                    self.logger.exception(f'Error in running {dataflow}:')
                    return False, dataflow
            
            try:
                ray_kwargs = ray_kwargs or {}
                if 'num_cpus' not in ray_kwargs:
                    ray_kwargs['num_cpus'] = os.cpu_count()
                self._init_ray(**ray_kwargs)
                log_queue = Queue()
                log_listener = QueueListener(log_queue, *self.logger.handlers, respect_handler_level=True)
                log_listener.start()
                batch_size = ray_kwargs['num_cpus']
                dataflow_batches = [dataflows[i: i + batch_size] for i in range(0, len(dataflows), batch_size)]
                for dataflow_batch in tqdm(dataflow_batches, desc=f'Running {self.name} dataflows', colour=color):
                    futures = [ray_task.remote(dataflow) for dataflow in dataflow_batch]
                    returns = ray.get(futures)
                    for success, dataflow in returns:
                        if not success:
                            failed_dataflows.append(dataflow)
                        else:
                            completed_dataflows.append(dataflow)
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
                for dataflow in tqdm(dataflows, desc=f'Running {self.name} dataflows', colour=color):
                    if self._use_prefect:
                        res: tData | None = dataflow()
                    elif self._use_bytewax:
                        # REVIEW, using run_main() to execute the dataflow in the current thread, NOT for production
                        from bytewax.testing import run_main
                        run_main(dataflow)
                    else:
                        res: tData | None = dataflow.run()
                    success = _handle_result(res)
                    if not success:
                        failed_dataflows.append(dataflow)
                    else:
                        completed_dataflows.append(dataflow)
            except RuntimeError as err:
                if self._use_prefect:
                    self.logger.error(prefect_error_msg.format(name=self.name, err=err))
                else:
                    raise err
            except Exception:
                self.logger.exception(f'Error in running {self.name} dataflows:')

        if failed_dataflows:
            retrieve_dataflows = [dataflow for dataflow in failed_dataflows if dataflow.op_type == 'retrieve']
            if retrieve_dataflows:
                self.logger.debug(f'{self.name} failed dataflows: {[str(dataflow) for dataflow in retrieve_dataflows]}')
            non_retrieve_dataflows = [dataflow for dataflow in failed_dataflows if dataflow.op_type != 'retrieve']
            if non_retrieve_dataflows:
                self.logger.warning(
                    f'{self.name} failed dataflows:\n'
                    f'{pformat([str(dataflow) for dataflow in non_retrieve_dataflows])}\n'
                    f'check {self.logger.name}.log for details'
                )

        self._completed_dataflows = completed_dataflows
        self._failed_dataflows = failed_dataflows
        self._subflows.clear()
        self._dataflows.clear()
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
    
    def to_bytewax_dataflow(self, **kwargs) -> BytewaxDataFlow:
        '''
        Args:
            kwargs: kwargs specific to bytewax Dataflow()
        '''
        bytewax_flows = [dataflow.to_bytewax_dataflow(**kwargs) for dataflow in self._dataflows]
        assert len(bytewax_flows) == 1, 'Multiple dataflows are not supported for bytewax'
        return bytewax_flows[0]
    