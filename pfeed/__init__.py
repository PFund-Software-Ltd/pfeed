from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    # need these imports to support IDE hints:
    from pfeed.aliases import ALIASES as alias
    from pfeed.sources.yahoo_finance import (
        YahooFinance,
        YahooFinance as YF,
    )
    from pfeed.storages.local_storage import LocalStorage
    from pfeed.storages.cache_storage import CacheStorage
    from pfeed.storages.duckdb_storage import DuckDBStorage
    from pfeed.sources.bybit import Bybit
    from pfeed.sources.financial_modeling_prep import (
        FinancialModelingPrep,
        FinancialModelingPrep as FMP,
    )
    from pfeed.engine import DataEngine
    from pfeed.sources.pfund import PFund

import os
from importlib.metadata import version

from pfeed.config import configure, get_config, configure_logging


os.environ['PYARROW_IGNORE_TIMEZONE'] = '1'  # used to suppress warning from pyspark


def __getattr__(name: str):
    if name == 'alias':
        from pfeed.aliases import ALIASES
        return ALIASES
    elif name == 'DataEngine':
        from pfeed.engine import DataEngine
        return DataEngine
    elif name in ('YahooFinance', 'YF'):
        from pfeed.sources.yahoo_finance import YahooFinance
        return YahooFinance
    elif name.lower() == 'bybit':
        from pfeed.sources.bybit import Bybit
        return Bybit
    elif name in ('FinancialModelingPrep', 'FMP'):
        from pfeed.sources.financial_modeling_prep import FinancialModelingPrep
        return FinancialModelingPrep
    elif name.lower() == 'pfund':
        from pfeed.sources.pfund import PFund
        return PFund
    elif name == 'LocalStorage':
        from pfeed.storages.local_storage import LocalStorage
        return LocalStorage
    elif name == 'CacheStorage':
        from pfeed.storages.cache_storage import CacheStorage
        return CacheStorage
    elif name == 'DuckDBStorage':
        from pfeed.storages.duckdb_storage import DuckDBStorage
        return DuckDBStorage
    raise AttributeError(f"'{__name__}' object has no attribute '{name}'")
    
    
__version__ = version("pfeed")
__all__ = (
    "__version__",
    "configure",
    'get_config',
    'configure_logging',
    "alias",
    "plot",
    "DataEngine",
    # storages
    "LocalStorage",
    "CacheStorage",
    "DuckDBStorage",
    # data sources
    "YahooFinance", "YF",
    "Bybit",
    "PFund",
    "FinancialModelingPrep", "FMP",
)
def __dir__():
    return sorted(__all__)
