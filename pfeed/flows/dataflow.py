from __future__ import annotations
from typing import Literal, Callable, TYPE_CHECKING
if TYPE_CHECKING:
    from prefect import Flow as PrefectFlow
    
    from pfeed.types.core import tDataModel, tData
    from pfeed.storages.base_storage import BaseStorage
    from pfeed.const.enums import DataSource


class DataFlow:
    def __init__(self, logger, data_model: tDataModel):
        self.logger = logger
        self.data_model = data_model
        self.data_source: DataSource = data_model.source.name
        self.name = f'{self.data_source}_DataFlow'
        self.operations = []
        self.storages: list[BaseStorage] = []
        self._has_extract_operation = False
        self._has_load_operation = False
    
    def add_operation(self, op_type: Literal['extract', 'transform', 'load'], *funcs: tuple[Callable, ...]):
        if op_type == 'extract':
            self._has_extract_operation = True
        elif op_type == 'load':
            self._has_load_operation = True
        self.operations.append((op_type, funcs))

    def __str__(self):
        return f'{self.data_model}_DataFlow'

    def __call__(self):
        return self.run()
        
    def run(self) -> tData | None:
        self._assert_operations()
        for op_type, funcs in self.operations:
            if op_type == 'extract':
                func = funcs[0]
                data: tData | None = self._extract(func, func.__name__)
                if data is None:
                    return None
            elif op_type == 'transform':
                for func in funcs:
                    data = self._transform(func, func.__name__, data)
            elif op_type == 'load':
                etl_load_data = funcs[0]
                self._load(etl_load_data, data)
        else:
            return data
    
    def _assert_operations(self):
        assert self._has_extract_operation, f'{self.name} has no extract operation'
        assert self._has_load_operation, f'{self.name} has no load operation, please add load(storage=...) to the flow'
        
    def _extract(self, func: Callable, func_name: str) -> tData | None:
        # self.logger.debug(f"extracting {self.data_model} data by '{func_name}'")
        data: tData | None = func()
        if data is None:
            self.logger.warning(f'failed to extract {self.data_model} data by {func_name}')
        else:
            self.logger.info(f"extracted {self.data_model} data by '{func_name}'")
        return data
                
    def _transform(self, func: Callable, func_name: str, data: tData) -> tData:
        # self.logger.debug(f"transforming {self.data_model} data using '{func_name}'")
        data = func(data)
        self.logger.info(f"transformed {self.data_model} data by '{func_name}'")
        return data
    
    def _load(self, func: Callable, data: tData) -> BaseStorage:
        storage: BaseStorage = func(data)
        self.storages.append(storage)
        self.logger.info(f'loaded {self.data_source} data to {type(storage).__name__} {storage.file_path}')
        return storage

    def to_prefect_flow(self, log_prints: bool = True, **kwargs) -> PrefectFlow:
        from prefect import flow, task
        from prefect.logging import get_run_logger
        @flow(name=self.name, log_prints=log_prints, flow_run_name=str(self.data_model), **kwargs)
        def prefect_flow():
            self.logger = get_run_logger()
            self._assert_operations()
            for op_type, funcs in self.operations:
                if op_type == 'extract':
                    func = funcs[0]
                    extract_task = task(func)
                    data: tData | None = self._extract(extract_task, func.__name__)
                elif op_type == 'transform':
                    for func in funcs:
                        transform_task = task(func)
                        data: tData = self._transform(transform_task, func.__name__, data)
                elif op_type == 'load':
                    etl_load_data = funcs[0]
                    load_task = task(etl_load_data)
                    self._load(load_task, data)
        return prefect_flow