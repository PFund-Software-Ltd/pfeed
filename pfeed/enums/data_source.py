from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfund.typing import tEnvironment
    from pfeed.sources.base_source import BaseSource

from enum import StrEnum

from pfeed.utils.utils import to_camel_case


class DataSource(StrEnum):
    YAHOO_FINANCE = 'YAHOO_FINANCE'
    FINANCIAL_MODELING_PREP = 'FINANCIAL_MODELING_PREP'
    BYBIT = 'BYBIT'
    BINANCE = 'BINANCE'
    # DATABENTO = 'DATABENTO'

    @property
    def data_client_class(self):
        import pfeed as pe
        return getattr(pe, to_camel_case(self))
    
    @property
    def data_source_class(self):
        import importlib
        return getattr(importlib.import_module(f'pfeed.sources.{self.lower()}.source'), f'{to_camel_case(self)}Source')
    
    def create_data_source(self, env: tEnvironment='BACKTEST') -> BaseSource:
        from pfund.utils.utils import get_function_signature
        
        DataSourceClass: type[BaseSource] = self.data_source_class
        if 'env' in get_function_signature(DataSourceClass).parameters:
            data_source_params = {'env': env}
        else:
            data_source_params = {}
        return DataSourceClass(**data_source_params)