from __future__ import annotations
from typing import TYPE_CHECKING, Literal, Callable, ModuleType
if TYPE_CHECKING:
    import pandas as pd
    from prefect import Flow as PrefectFlow
    from bytewax.dataflow import Dataflow as BytewaxDataFlow
    from bytewax.inputs import Source as BytewaxSource
    from bytewax.outputs import Sink as BytewaxSink
    from bytewax.dataflow import Stream as BytewaxStream
    from pfund.products.product_base import BaseProduct
    from pfeed.data_models.base_data_model import BaseDataModel
    from pfeed.typing.core import tData, tDataFrame
    from pfeed.typing.literals import tSTORAGE, tDATA_TOOL, tDATA_LAYER
    from pfeed.const.enums import DataSource
    from pfeed.sources.base_source import BaseSource
    from pfeed.storages.base_storage import BaseStorage
    from pfeed.flows.faucet import Faucet
    
import os
from abc import ABC, abstractmethod
import importlib
import datetime
import logging
from logging.handlers import QueueHandler, QueueListener
from pprint import pformat

from bytewax.testing import run_main
from bytewax.connectors.stdio import StdOutSink

from pfund import print_warning
from pfeed.config import get_config
from pfeed.const.enums import DataTool, DataLayer, DataStorage
from pfeed.flows.dataflow import DataFlow
from pfeed.utils.utils import lambda_with_name, rollback_date_range


__all__ = ["BaseFeed"]
        

def clear_subflows(func):
    def wrapper(feed: BaseFeed, *args, **kwargs):
        feed._clear_subflows()
        return func(feed, *args, **kwargs)
    return wrapper
    
    
class BaseFeed(ABC):
    def __init__(
        self, 
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
        self._pipeline_mode = pipeline_mode
        self._use_ray = use_ray
        self._use_prefect = use_prefect
        self._use_bytewax = use_bytewax
        if self._use_prefect and self._use_bytewax:
            raise ValueError('Cannot use both prefect and bytewax at the same time')
        self._use_deltalake = use_deltalake
        self._storage_configs: dict[tSTORAGE, dict] = {}
        self._dataflows: list[DataFlow] = []
        self._subflows: list[DataFlow] = []
        self.source: BaseSource = self.get_data_source()
        self.api = self.source.api if hasattr(self.source, 'api') else None
        self.name: DataSource = self.source.name
        self.data_tool: ModuleType = importlib.import_module(f'pfeed.data_tools.data_tool_{DataTool[data_tool.lower()]}')
        self.config = get_config()
        is_loggers_set_up = bool(logging.getLogger('pfeed').handlers)
        if not is_loggers_set_up:
            set_up_loggers(self.config.log_path, self.config.logging_config_file_path, user_logging_config=self.config.logging_config)
        self.logger = logging.getLogger(self.name.lower() + '_data')

        if self.config.print_msg:
            print(f'''Hint:
                You can run command "pfeed config set --data-path {{your_path}}" to set your data path that stores downloaded data.
                The current data path is: {self.config.data_path}
            ''')
            
    @staticmethod
    @abstractmethod
    def get_data_source() -> BaseSource:
        pass
    
    @staticmethod
    @abstractmethod
    def _normalize_raw_data(self, df: pd.DataFrame) -> pd.DataFrame:
        pass
    
    @abstractmethod
    def _validate_schema(self, df: pd.DataFrame, data_model: BaseDataModel) -> pd.DataFrame:
        pass

    @abstractmethod
    def create_data_model(self, *args, **kwargs) -> BaseDataModel:
        pass

    def configure_storage(self, storage: tSTORAGE, storage_configs: dict) -> BaseFeed:
        '''Configure storage kwargs for the given storage
        Args:
            storage_configs: storage specific kwargs, e.g. if storage is 'minio', kwargs are minio specific kwargs
        '''
        self._storage_configs[storage] = storage_configs
        return self

    def is_pipeline(self) -> bool:
        return self._pipeline_mode

    def _print_minio_warning(self, to_storage: tSTORAGE):
        if self.config.print_msg and to_storage.lower() not in ['minio', 'cache']:
            print_warning('''
                pfeed is designed to work natively with MinIO as a data lake.
                It is recommended to use MinIO as your data storage solution.
                You can do that by setting `to_storage='minio'`.
            ''')
    
    def _assert_data_quality(self, df: pd.DataFrame, data_model: BaseDataModel, data_layer: tDATA_LAYER) -> pd.DataFrame:
        '''Asserts that the data conforms to pfeed's internal standards before loading it into storage.'''
        if data_layer == DataLayer.RAW:
            return df
        return self._validate_schema(df, data_model)

    def _init_ray(self, **kwargs):
        import ray
        if not ray.is_initialized():
            ray.init(**kwargs)

    def _shutdown_ray(self):
        import ray
        if ray.is_initialized():
            ray.shutdown()

    def download(self, *args, **kwargs) -> BaseFeed:
        raise NotImplementedError(f"{self.name} download() is not implemented")
    
    def stream(
        self, 
        # NOTE: supportts BytewaxStream, useful for users passing in BytewaxStream objects, e.g.:
        # merge_stream = op.merge('merge', op.input("input1", ...), op.input("input2", ...))
        bytewax_source: BytewaxSource | BytewaxStream | str | None=None,
        *args, 
        **kwargs,
    ) -> BaseFeed:
        raise NotImplementedError(f"{self.name} stream() is not implemented")

    def retrieve(self, *args, **kwargs) -> tData | None:
        raise NotImplementedError(f'{self.name} retrieve() is not implemented')
    
    def fetch(self, *args, **kwargs) -> tData | None:
        raise NotImplementedError(f'{self.name} fetch() is not implemented')
    
    def get_historical_data(self, *args, **kwargs) -> tData | None:
        raise NotImplementedError(f'{self.name} get_historical_data() is not implemented')
    
    def get_realtime_data(self, *args, **kwargs) -> tData | None:
        raise NotImplementedError(f'{self.name} get_realtime_data() is not implemented')
    
    def _execute_download(self, data_model: BaseDataModel) -> tData:
        raise NotImplementedError(f"{self.name} _execute_download() is not implemented")
    
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
        include_metadata: bool=False,
        storage_configs: dict | None=None,
    ) -> tuple[tData | None, dict]:
        search_storages = ['cache', 'local', 'minio', 'duckdb'] if from_storage is None else [from_storage]
        data, metadata = None, {}
        storage_configs = storage_configs or {}
        if storage_configs:
            assert from_storage is not None, 'from_storage is required when storage_configs is provided'
        for search_storage in search_storages:
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
                data, metadata = storage.read_data(data_tool=self.data_tool.name)
                if data is not None:
                    break
            except Exception as e:  # e.g. minio's ServerError if server is not running
                continue
        if include_metadata:
            return data, metadata
        else:
            return data

    def get_metadata(
        self,
        data_model: BaseDataModel,
        data_layer: tDATA_LAYER,
        data_domain: str,
        from_storage: tSTORAGE,
    ) -> dict:
        _, metadata = self._execute_retrieve(
            data_model,
            data_layer,
            data_domain,
            from_storage=from_storage,
            include_metadata=True,
        )
        return metadata
    
    # TODO
    def _execute_fetch(self, data_model: BaseDataModel, *args, **kwargs) -> tData | None:
        raise NotImplementedError(f'{self.name} _execute_fetch() is not implemented')

    def create_product(self, product_basis: str, symbol: str='', **product_specs) -> BaseProduct:
        return self.source.create_product(product_basis, symbol=symbol, **product_specs)
    
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
                if self.source.start_date:
                    start_date = self.source.start_date
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
    def create_faucet(data_model: BaseDataModel, extract_func: Callable) -> Faucet:
        return Faucet(data_model, extract_func)
    
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
        faucet = self.create_faucet(data_model, lambda: self._execute_download(data_model))
        return self.create_dataflow(faucet)
    
    def _extract_stream(
        self, 
        data_model: BaseDataModel, 
        bytewax_source: BytewaxSource | BytewaxStream | str | None=None,
    ) -> DataFlow:
        faucet = self.create_faucet(
            data_model, 
            lambda: self._execute_stream(data_model, bytewax_source=bytewax_source), 
            streaming=True,
        )
        return self.create_dataflow(faucet)
    
    def _extract_retrieve(
        self, 
        data_model: BaseDataModel,
        data_layer: tDATA_LAYER,
        data_domain: str,
        from_storage: tSTORAGE | None=None,
    ) -> DataFlow:
        faucet = self.create_faucet(
            data_model, 
            lambda: self._execute_retrieve(
                data_model, 
                data_layer, 
                data_domain, 
                from_storage=from_storage,
            )
        )
        return self.create_dataflow(faucet)
    
    def _extract_fetch(self, data_model: BaseDataModel) -> DataFlow:
        faucet = self.create_faucet(data_model, lambda: self._execute_fetch(data_model))
        return self.create_dataflow(faucet)
    
    def transform(self, *funcs) -> BaseFeed:
        for dataflow in self._subflows:
            dataflow.add_transformations(*funcs)
        return self
    
    def load(
        self, 
        to_storage: tSTORAGE='local',   
        data_layer: tDATA_LAYER='curated',
        data_domain: str='general_data',
        metadata: dict | None=None,
        storage_configs: dict | None=None,
        bytewax_sink: BytewaxSink | str | None=None,
    ) -> BaseFeed:
        '''
        Args:
            data_domain: custom domain of the data, used in data_path/data_layer/data_domain
                useful for grouping data
            metadata: custom metadata to be added to the data
        '''
        def _create_assert_data_quality_function(_data_model: BaseDataModel):
            return lambda_with_name(
                '_assert_data_quality',
                lambda df: self._assert_data_quality(df, _data_model, data_layer)
            )
        if self._use_ray:
            assert to_storage.lower() != 'duckdb', 'DuckDB is not thread-safe, cannot be used with Ray'
        Storage = DataStorage[to_storage.upper()].storage_class
        storage_configs = storage_configs or self._storage_configs.get(to_storage, {})
        for dataflow in self._subflows:
            data_model: BaseDataModel = dataflow.data_model
            if metadata:
                data_model.add_metadata(metadata)
            # assert data standards before loading into storage
            # NOTE: remember when looping, if you pass in e.g. dataflow to lambda dataflow: ..., due to python lambda's late binding, you are passing in the last dataflow object to all lambdas
            # so this is wrong: dataflow.add_transformations(lambda df: self._assert_data_quality(df, dataflow.data_model)) <- dataflow object is always the last one in the loop
            dataflow.add_transformations(_create_assert_data_quality_function(data_model))
            storage: BaseStorage = Storage.from_data_model(
                data_model,
                data_layer,
                data_domain,
                use_deltalake=self._use_deltalake,
                **storage_configs,
            )
            dataflow.set_storage(storage)
            if self._use_bytewax:
                dataflow.set_bytewax_sink(bytewax_sink or StdOutSink())
        return self
    
    def run(
        self, 
        dataflows: list[DataFlow] | None=None,
        ray_kwargs: dict | None=None,
    ) -> tDataFrame | None:
        '''Run dataflows'''
        from tqdm import tqdm
        from pfeed.utils.utils import generate_color
        if dataflows is None:
            dataflows = self.to_prefect_dataflows() if self._use_prefect else self._dataflows
            if self._use_bytewax:
                dataflows.append(self.to_bytewax_dataflow())
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
            if self.config.print_msg:
                print('''Note:
                If Ray appears to be running sequentially rather than in parallel, it may be due to insufficient network bandwidth for parallel downloads.
                ''')
            
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
                    if self._use_prefect:
                        res: tData | None = dataflow()
                    elif self._use_bytewax:
                        # REVIEW, using run_main() to execute the dataflow in the current thread, NOT for production
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
                    res: tData | None = dataflow()
                    success = _handle_result(res)
                    if not success:
                        failed_dataflows.append(dataflow)
            except RuntimeError as err:
                if self._use_prefect:
                    self.logger.error(prefect_error_msg.format(name=self.name, err=err))
                else:
                    raise err
            except Exception:
                self.logger.exception(f'Error in running {self.name} dataflows:')
        if failed_dataflows:
            self.logger.warning(f'{self.name} failed dataflows:\n{pformat([str(dataflow) for dataflow in self._failed_dataflows])}\ncheck {self.logger.name}.log for details')

        completed_dataflows = [dataflow for dataflow in dataflows if dataflow not in failed_dataflows]
        self._subflows.clear()
        self._dataflows.clear()
        return completed_dataflows, failed_dataflows
    
    @property
    def dataflows(self) -> list[DataFlow]:
        return self._dataflows
    
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
    