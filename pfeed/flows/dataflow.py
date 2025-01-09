from __future__ import annotations
from typing import Literal, Callable, TYPE_CHECKING
if TYPE_CHECKING:
    from prefect import Flow as PrefectFlow
    from pfeed.typing.core import tData
    from pfeed.data_models.base_data_model import BaseDataModel
    from pfeed.const.enums import DataSource
    from pfeed.storages.base_storage import BaseStorage

import pandas as pd


class DataFlow:
    def __init__(self, logger, data_model: BaseDataModel):
        self.logger = logger
        self.data_model = data_model
        self.data_source: DataSource = data_model.source.name
        self.name = f'{self.data_source}_DataFlow'
        self.operations = {'extract': [], 'transform': [], 'load': []}
        self._has_extract_operation = False
        self._has_load_operation = False
    
    def add_operation(self, op_type: Literal['extract', 'transform', 'load'], *funcs: tuple[Callable, ...]):
        if op_type == 'extract':
            self._has_extract_operation = True
        elif op_type == 'load':
            self._has_load_operation = True
        self.operations[op_type].extend(funcs)
    
    def has_extract_operation(self) -> bool:
        return self._has_extract_operation
    
    def has_load_operation(self) -> bool:
        return self._has_load_operation

    def __str__(self):
        return f'{self.data_model}_DataFlow'

    def __call__(self):
        return self.run()
        
    def run(self) -> tData | None:
        self._assert_operations()
        for op_type, funcs in self.operations.items():
            if op_type == 'extract':
                assert len(funcs) == 1, f'{self.name} has multiple extract operations'
                func = funcs[0]
                raw_data: tData | None = self._extract(func, func.__name__)
                if raw_data is None:
                    break
            elif op_type == 'transform':
                data = raw_data
                for func in funcs:
                    data: tData = self._transform(func, func.__name__, data)
                    if isinstance(data, pd.DataFrame) and data.empty:
                        self.logger.warning(f'dataframe is empty, skipping transformations for {self.data_model}')
                        break
            elif op_type == 'load':
                assert len(funcs) == 1, f'{self.name} has multiple load operations'
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
    
    def _load(self, func: Callable, data: tData):
        storages: list[BaseStorage] = func(data)
        if not storages:
            self.logger.warning(f'failed to load {self.data_source} data to storage')
        else:
            for storage in storages:
                self.logger.info(f'loaded {storage.data_model} data to {type(storage).__name__} {storage.file_path}')

    def to_prefect_flow(self, log_prints: bool = True, **kwargs) -> PrefectFlow:
        from prefect import flow, task
        from prefect.logging import get_run_logger
        @flow(name=self.name, log_prints=log_prints, flow_run_name=str(self.data_model), **kwargs)
        def prefect_flow():
            self.logger = get_run_logger()
            self._assert_operations()
            for op_type, funcs in self.operations.items():
                if op_type == 'extract':
                    assert len(funcs) == 1, f'{self.name} has multiple extract operations'
                    func = funcs[0]
                    raw_data: tData | None = self._extract(task(func), func.__name__)
                    if raw_data is None:
                        break
                elif op_type == 'transform':
                    data = raw_data
                    for func in funcs:
                        data: tData = self._transform(task(func), func.__name__, data)
                        if isinstance(data, pd.DataFrame) and data.empty:
                            self.logger.warning(f'dataframe is empty, skipping transformations for {self.data_model}')
                            break
                elif op_type == 'load':
                    assert len(funcs) == 1, f'{self.name} has multiple load operations'
                    etl_load_data = funcs[0]
                    self._load(task(etl_load_data), data)
        return prefect_flow