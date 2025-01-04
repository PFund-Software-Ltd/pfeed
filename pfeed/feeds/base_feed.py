from __future__ import annotations
from typing import TYPE_CHECKING, Literal
if TYPE_CHECKING:
    import pandas as pd
    from prefect import Flow as PrefectFlow
    from pfund.products.product_base import BaseProduct
    
    from pfeed.types.core import tData, tDataModel
    from pfeed.types.literals import tSTORAGE, tDATA_TOOL
    from pfeed.const.enums import DataSource
    from pfeed.sources.base_source import BaseSource

import os    
import sys
from abc import ABC, abstractmethod
import importlib
import datetime
import logging
from logging.handlers import QueueHandler, QueueListener
from pprint import pformat

import click

from pfund import print_warning
from pfeed.config import get_config
from pfeed import etl
from pfeed.const.enums import DataTool
from pfeed.flows.dataflow import DataFlow
from pfeed.utils.utils import lambda_with_name, rollback_date_range


__all__ = ["BaseFeed"]
        

def clear_current_dataflows(func):
    def wrapper(feed: BaseFeed, *args, **kwargs):
        feed._clear_current_dataflows()
        return func(feed, *args, **kwargs)
    return wrapper
    
    
class BaseFeed(ABC):
    def __init__(
        self, 
        data_tool: tDATA_TOOL='pandas', 
        use_ray: bool=True,
        use_prefect: bool=False,
        pipeline_mode: bool=False,
        ray_kwargs: dict | None=None,
        prefect_kwargs: dict | None=None,
    ):
        from pfund.plogging import set_up_loggers
        self._pipeline_mode = pipeline_mode
        self._use_ray = use_ray
        self._use_prefect = use_prefect
        self._ray_kwargs = ray_kwargs or {}
        if 'num_cpus' not in self._ray_kwargs:
            self._ray_kwargs['num_cpus'] = os.cpu_count()
        self._prefect_kwargs = prefect_kwargs or {}
        self._dataflows: list[DataFlow] = []
        self._failed_dataflows: list[DataFlow] = []
        self._current_dataflows: list[DataFlow] = []
        
        self.data_source: BaseSource = self.get_data_source()
        self.api = self.data_source.api if hasattr(self.data_source, 'api') else None
        self.name: DataSource = self.data_source.name
        data_tool = data_tool.lower()
        assert data_tool in DataTool.__members__, f"Invalid {data_tool=}, SUPPORTED_DATA_TOOLS={list(DataTool.__members__.keys())}"
        self.data_tool = importlib.import_module(f'pfeed.data_tools.data_tool_{data_tool}')
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
    
    @abstractmethod
    def _normalize_raw_data(self, df: pd.DataFrame) -> pd.DataFrame:
        pass
    
    @abstractmethod
    def _assert_data_standards(self, df: pd.DataFrame) -> pd.DataFrame:
        '''
        Assert that the data conforms to the pfeed's internal standards.
        Different data feeds have different standards.
        '''
        pass

    def _init_ray(self):
        import ray
        if not ray.is_initialized():
            ray.init(**self._ray_kwargs)

    def _shutdown_ray(self):
        import ray
        if ray.is_initialized():
            ray.shutdown()

    def download(self, *args, **kwargs) -> BaseFeed:
        raise NotImplementedError(f"{self.name} download() is not implemented")
    
    def download_historical_data(self, *args, **kwargs) -> BaseFeed:
        return self.download(*args, **kwargs)
    
    def stream(self, *args, **kwargs) -> BaseFeed:
        raise NotImplementedError(f"{self.name} stream() is not implemented")
    
    def stream_realtime_data(self, *args, **kwargs) -> BaseFeed:
        return self.stream(*args, **kwargs)
    
    def _execute_download(self, data_model: tDataModel) -> tData:
        raise NotImplementedError(f"{self.name} _execute_download() is not implemented")
    
    def _execute_stream(self, data_model: tDataModel) -> tData:
        raise NotImplementedError(f"{self.name} _execute_stream() is not implemented")

    def create_product(self, product_basis: str, symbol: str='', **product_specs) -> BaseProduct:
        return self.data_source.create_product(product_basis, symbol=symbol, **product_specs)

    def _prepare_products(self, pdts: list[str], ptypes: list[str] | None=None) -> list[str]:
        '''Prepare products based on input products and product types
        If both "products" and "product_types" are provided, only "products" will be used.
        If "products" is not provided, use "product_types" to get products.
        If neither "products" nor "product_types" are provided, use all products of all product types.
        '''
        def _standardize_pdts_or_ptypes(pdts_or_ptypes: str | list[str] | None) -> list[str]:
            '''Standardize product names or product types'''
            if pdts_or_ptypes is None:
                pdts_or_ptypes = []
            elif isinstance(pdts_or_ptypes, str):
                pdts_or_ptypes = [pdts_or_ptypes]
            pdts_or_ptypes = [pdt.replace('-', '_').upper() for pdt in pdts_or_ptypes]
            return pdts_or_ptypes
        ptypes = ptypes or []
        if pdts and ptypes:
            print_warning('Warning: both "products" and "product_types" provided, only "products" will be used')
        # no pdts -> use ptypes; no ptypes -> use data_source.product_types
        if not (pdts := _standardize_pdts_or_ptypes(pdts)):
            ptypes = _standardize_pdts_or_ptypes(ptypes)
            if not ptypes and hasattr(self.data_source, 'get_products_by_types'):
                print_warning(f'Warning: no "products" or "product_types" provided, downloading ALL products with ALL product types {ptypes} from {self.name}')
                if not click.confirm('Do you want to continue?', default=False):
                    sys.exit(1)
                ptypes = self.data_source.product_types
            if hasattr(self.data_source, 'get_products_by_types'):
                pdts = self.data_source.get_products_by_types(ptypes)
            else:
                raise ValueError(f'"products" cannot be empty for {self.name}')
        return pdts
    
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
    
    def create_dataflow(self, data_model: tDataModel) -> DataFlow:
        dataflow = DataFlow(self.logger, data_model)
        self._current_dataflows.append(dataflow)
        self._dataflows.append(dataflow)
        return dataflow
    
    def _clear_current_dataflows(self):
        '''Clear current dataflows
        This is necessary to allow the following behaviour:
        download(...).transform(...).load(...).stream(...).transform(...).load(...)
        1. current dataflows: download(...).transform(...).load(...)
        2. clear current dataflows so that the operations of the next batch of dataflows are independent of the previous batch
        3. current dataflows: stream(...).transform(...).load(...)
        '''
        self._current_dataflows.clear()
    
    def extract(self, op_type: Literal['download', 'stream'], data_model: tDataModel) -> DataFlow:
        dataflow = self.create_dataflow(data_model)
        
        if op_type == 'download':
            dataflow.add_operation(
                'extract',
                lambda_with_name(op_type, lambda: self._execute_download(data_model))
            )
        elif op_type == 'stream':
            dataflow.add_operation(
                'extract', 
                lambda_with_name(op_type, lambda: self._execute_stream(data_model))
            )
        return dataflow
    
    def transform(self, *funcs, dataflows: list[DataFlow] | None=None) -> BaseFeed:
        dataflows = dataflows or self._current_dataflows
        for dataflow in dataflows:
            dataflow.add_operation('transform', *funcs)
        return self
    
    def load(self, storage: tSTORAGE='local', dataflows: list[DataFlow] | None=None, **kwargs) -> BaseFeed:
        '''
        Args:
            kwargs: storage specific kwargs, e.g. if storage is 'minio', kwargs are minio specific kwargs
        '''
        def _create_load_function(data_model):
            return lambda_with_name(
                'etl.load_data',
                lambda data: etl.load_data(data_model, data, storage, **kwargs)
            )
        def _create_assert_data_standards_function(data_model):
            return lambda_with_name(
                '_assert_data_standards',
                lambda df: self._assert_data_standards(df, data_model.metadata)
            )
        dataflows = dataflows or self._current_dataflows
        # NOTE: remember when looping, if you pass in e.g. dataflow to lambda dataflow: ..., due to python lambda's late binding, you are passing in the last dataflow object to all lambdas
        # so this is wrong: dataflow.add_operation('transform', lambda df: self._assert_data_standards(df, dataflow.data_model.metadata)) <- dataflow object is always the last one in the loop
        for dataflow in dataflows:
            # assert data standards before loading into storage
            dataflow.add_operation('transform', _create_assert_data_standards_function(dataflow.data_model))
            dataflow.add_operation('load', _create_load_function(dataflow.data_model))
        if dataflows == self._current_dataflows:
            self._clear_current_dataflows()
        return self
    
    def run(self):
        from tqdm import tqdm
        from pfeed.utils.utils import generate_color
        color = generate_color(self.name.value)
        # if dataflow has no load operation, load to local storage by default
        if dataflows_with_no_load_operation := [dataflow for dataflow in self._dataflows if not dataflow.has_load_operation()]:
            self.load(storage='local', dataflows=dataflows_with_no_load_operation)
        dataflows = self._dataflows if not self._use_prefect else self.to_prefect_flows()
        prefect_error_msg = 'Error in running {name} prefect dataflows: {err}, did you forget to run "prefect server run" to start prefect\'s server?'
        
        def _handle_result(res: tData | None) -> bool:
            if not self._use_prefect:
                success = False if res is None else True
            else:
                # for prefect, use dashboard to check failed dataflows
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
            atexit.register(lambda: ray.shutdown())
            
            @ray.remote
            def ray_task(dataflow) -> bool:
                try:
                    if not self.logger.handlers:
                        self.logger.addHandler(QueueHandler(log_queue))
                        self.logger.setLevel(logging.DEBUG)
                    res = dataflow()
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
                self._init_ray()
                log_queue = Queue()
                log_listener = QueueListener(log_queue, *self.logger.handlers, respect_handler_level=True)
                log_listener.start()
                batch_size = self._ray_kwargs['num_cpus']
                dataflow_batches = [dataflows[i: i + batch_size] for i in range(0, len(dataflows), batch_size)]
                for dataflow_batch in tqdm(dataflow_batches, desc=f'Running {self.name} dataflows', colour=color):
                    futures = [ray_task.remote(dataflow) for dataflow in dataflow_batch]
                    returns = ray.get(futures)
                    for success, dataflow in returns:
                        if not success:
                            self._failed_dataflows.append(dataflow)
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
                    res = dataflow()
                    success = _handle_result(res)
                    if not success:
                        self._failed_dataflows.append(dataflow)
            except RuntimeError as err:
                if self._use_prefect:
                    self.logger.error(prefect_error_msg.format(name=self.name, err=err))
                else:
                    raise err
            except Exception:
                self.logger.exception(f'Error in running {self.name} dataflows:')
        if self._failed_dataflows:
            self.logger.warning(f'some {self.name} dataflows failed\n{pformat([str(dataflow) for dataflow in self._failed_dataflows])}\ncheck {self.logger.name}.log for details')
                    
    
    def to_prefect_flows(self, **kwargs) -> list[PrefectFlow]:
        prefect_flows = []
        kwargs = self._prefect_kwargs or kwargs
        for dataflow in self._dataflows:
            prefect_flow = dataflow.to_prefect_flow(**kwargs)
            prefect_flows.append(prefect_flow)
        return prefect_flows
    
    # TODO
    def to_bytewax_flows(self):
        pass
    
    def _get_historical_data_from_storage(self, *args, **kwargs) -> tData | None:
        raise NotImplementedError(f'{self.name} _get_historical_data_from_storage() is not implemented')
    
    def _get_historical_data_from_source(self, *args, **kwargs) -> tData | None:
        raise NotImplementedError(f'{self.name} _get_historical_data_from_source() is not implemented')

    def get_historical_data(self, *args, **kwargs) -> tData | None:
        raise NotImplementedError(f'{self.name} get_historical_data() is not implemented')

    def get_realtime_data(self, *args, **kwargs) -> tData | None:
        raise NotImplementedError(f'{self.name} get_realtime_data() is not implemented')
