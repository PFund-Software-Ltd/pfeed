from __future__ import annotations
from typing import TYPE_CHECKING, Literal
if TYPE_CHECKING:
    from prefect import Flow as PrefectFlow

    from pfeed.types.core import tData, tDataModel
    from pfeed.types.literals import tSTORAGE, tDATA_TOOL
    from pfeed.const.enums import DataSource
    from pfeed.sources.base_data_source import BaseDataSource

import os    
import sys
import importlib
import datetime
import logging
from logging.handlers import QueueHandler, QueueListener
from pprint import pformat

import click
from rich.console import Console

from pfeed.config_handler import get_config
from pfeed import etl
from pfeed.const.enums import DataTool
from pfeed.flows.dataflow import DataFlow
from pfeed.utils.utils import lambda_with_name


__all__ = ["BaseFeed"]


def clear_current_dataflows(func):
    def wrapper(feed: BaseFeed, *args, **kwargs):
        feed.clear_current_dataflows()
        return func(feed, *args, **kwargs)
    return wrapper
    
    
class BaseFeed:
    def __init__(
        self, 
        data_source: BaseDataSource, 
        data_tool: tDATA_TOOL='pandas', 
        use_ray: bool=True,
        use_prefect: bool=False,
        pipeline_mode: bool=False,
    ):
        from pfund.plogging import set_up_loggers
        self._pipeline_mode = pipeline_mode
        self._use_ray = use_ray
        self._use_prefect = use_prefect
        self._printed_hint = False
        self._dataflows: list[DataFlow] = []
        self._failed_dataflows: list[DataFlow] = []
        self._current_dataflows: list[DataFlow] = []
        
        self.data_source: BaseDataSource = data_source
        self.name: DataSource = data_source.name
        assert data_tool.upper() in DataTool.__members__, f"Invalid {data_tool=}, SUPPORTED_DATA_TOOLS={list(DataTool.__members__.keys())}"
        self.data_tool = importlib.import_module(f'pfeed.data_tools.data_tool_{data_tool.lower()}')
        self.config = get_config()
        is_loggers_set_up = bool(logging.getLogger('pfeed').handlers)
        if not is_loggers_set_up:
            set_up_loggers(self.config.log_path, self.config.logging_config_file_path, user_logging_config=self.config.logging_config)
        self.logger = logging.getLogger(self.name.lower() + '_data')

    def download(self, *args, **kwargs) -> BaseFeed:
        raise NotImplementedError(f"{self.name} download() is not implemented")
    
    def stream(self, *args, **kwargs) -> BaseFeed:
        raise NotImplementedError(f"{self.name} stream() is not implemented")
    
    def _execute_download(self, data_model: tDataModel) -> tData:
        raise NotImplementedError(f"{self.name} _execute_download() is not implemented")
    
    def _execute_stream(self, data_model: tDataModel) -> tData:
        raise NotImplementedError(f"{self.name} _execute_stream() is not implemented")

    def _prepare_pdts(self, pdts: list[str], ptypes: list[str]) -> list[str]:
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
        if pdts and ptypes:
            Console().print('Warning: both "products" and "product_types" provided, only "products" will be used', style='bold red')
        # no pdts -> use ptypes; no ptypes -> use data_source.product_types
        if not (pdts := _standardize_pdts_or_ptypes(pdts)):
            ptypes = _standardize_pdts_or_ptypes(ptypes)
            if not ptypes and hasattr(self.data_source, 'get_products_by_ptypes'):
                Console().print(f'Warning: no "products" or "product_types" provided, downloading ALL products with ALL product types {ptypes} from {self.name}', style='bold red')
                if not click.confirm('Do you want to continue?', default=False):
                    sys.exit(1)
                ptypes = self.data_source.product_types
            if hasattr(self.data_source, 'get_products_by_ptypes'):
                pdts = self.data_source.get_products_by_ptypes(ptypes)
            else:
                raise ValueError(f'"products" cannot be empty for {self.name}')
        return pdts
    
    def _standardize_dates(self, start_date: str, end_date: str) -> tuple[datetime.date, datetime.date]:
        '''Standardize start_date and end_date
        If start_date is not specified:
            If the data source has a 'start_date' attribute, use it as the start date.
            Otherwise, use yesterday's date as the default start date.
        If end_date is not specified, use today's date as the end date.
        '''
        today = datetime.datetime.now(tz=datetime.timezone.utc).date()
        yesterday = today - datetime.timedelta(days=1)
        start_date = start_date or getattr(self.data_source, 'start_date', yesterday)
        end_date = end_date or today
        return start_date, end_date
    
    def create_dataflow(self, data_model: tDataModel) -> DataFlow:
        dataflow = DataFlow(self.logger, data_model)
        self._current_dataflows.append(dataflow)
        self._dataflows.append(dataflow)
        return dataflow
    
    def clear_current_dataflows(self):
        '''Clear current dataflows
        This is necessary to allow the following behaviour:
        download(...).transform(...).load(...).stream(...).transform(...).load(...)
        1. current dataflows: download(...).transform(...).load(...)
        2. clear current dataflows so that the operations of the next batch of dataflows are independent of the previous batch
        3. current dataflows: stream(...).transform(...).load(...)
        '''
        self._current_dataflows.clear()
    
    def extract(self, op_type: Literal['download', 'stream'], data_model: tDataModel):
        dataflow = self.create_dataflow(data_model)
        if op_type == 'download':
            if not self._printed_hint:
                print(f'''Hint:
                    You can use the command "pfeed config set --data-path {{your_path}}" to set your data path that stores downloaded data.
                    The current data path is: {self.config.data_path}.
                ''')
                self._printed_hint = True
            dataflow.add_operation(
                'extract', 
                lambda_with_name(op_type, lambda: self._execute_download(data_model))
            )
        elif op_type == 'stream':
            dataflow.add_operation(
                'extract', 
                lambda_with_name(op_type, lambda: self._execute_stream(data_model))
            )
        return self
    
    def transform(self, *funcs):
        for dataflow in self._current_dataflows:
            dataflow.add_operation('transform', *funcs)
        return self

    def load(self, storage: tSTORAGE='local', **kwargs):
        '''
        Args:
            kwargs: storage specific kwargs, e.g. if storage is 'minio', kwargs are minio specific kwargs
        '''
        def _create_load_function(data_model):
            return lambda_with_name(
                'etl.load_data', 
                lambda data: etl.load_data(data_model, data, storage, **kwargs)
            )
        for dataflow in self._current_dataflows:
            dataflow.add_operation('load', _create_load_function(dataflow.data_model))
        return self
    
    def run(self, ray_init_kwargs: dict | None=None, prefect_kwargs: dict | None=None):
        from tqdm import tqdm
        from pfeed.utils.utils import generate_color
        ray_init_kwargs, prefect_kwargs = ray_init_kwargs or {}, prefect_kwargs or {}
        color = generate_color(self.name.value)
        dataflows = self._dataflows if not self._use_prefect else self.to_prefect_flows(**prefect_kwargs)
        
        # REVIEW: prefect only supports running tasks (not flows) in parallel using prefect-ray
        if self._use_ray and self._use_prefect:
            self._use_ray = False
            Console().print('Prefect flows cannot be run in parallel using Ray, Ray will be disabled.', style='bold')
            
        if self._use_ray:
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
                    success = False if res is None else True
                    return success, dataflow
                except Exception:
                    self.logger.exception(f'Error in running {dataflow}:')
                    return False, dataflow
            
            try:            
                if 'num_cpus' not in ray_init_kwargs:
                    ray_init_kwargs['num_cpus'] = os.cpu_count()
                ray.init(**ray_init_kwargs)
                log_queue = Queue()
                log_listener = QueueListener(log_queue, *self.logger.handlers, respect_handler_level=True)
                log_listener.start()
                batch_size = ray_init_kwargs['num_cpus']
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
                ray.shutdown()
        else:
            for dataflow in tqdm(dataflows, desc=f'Running {self.name} dataflows', colour=color):
                res = dataflow()
                # for prefect, use dashboard to check failed dataflows
                if self._use_prefect:
                    continue
                success = False if res is None else True
                if not success:
                    self._failed_dataflows.append(dataflow)
        if self._failed_dataflows:
            self.logger.warning(f'some {self.name} dataflows failed\n{pformat([str(dataflow) for dataflow in self._failed_dataflows])}\ncheck {self.logger.name}.log for details')
                    
    
    def to_prefect_flows(self, **kwargs) -> list[PrefectFlow]:
        prefect_flows = []
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


    download_historical_data = download
    stream_realtime_data = stream