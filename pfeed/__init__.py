from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    # need these imports to support IDE hints:
    import pfund_plot as plot
    from pfeed.utils.aliases import ALIASES as alias
    from pfeed.storages.storage_config import StorageConfig
    from pfeed.sources.yahoo_finance import (
        YahooFinance,
        YahooFinance as YF,
        YahooFinance as YFinance,
    )
    from pfeed.storages.local_storage import LocalStorage
    from pfeed.storages.cache_storage import CacheStorage
    from pfeed.storages.duckdb_storage import DuckDBStorage
    from pfeed.storages.lancedb_storage import LanceDBStorage
    from pfeed.storages.huggingface_storage import HuggingFaceStorage
    from pfeed.sources.bybit import Bybit
    from pfeed.sources.financial_modeling_prep import (
        FinancialModelingPrep,
        FinancialModelingPrep as FMP,
    )
    from pfeed.engine import DataEngine
    from pfeed.sources.pfund import PFund
    from pfeed.sources.alphafund import AlphaFund

import os
from importlib.metadata import version

from pfeed.config import configure, get_config, configure_logging


os.environ['PYARROW_IGNORE_TIMEZONE'] = '1'  # used to suppress warning from pyspark
# disable this warning in Ray: FutureWarning: Tip: In future versions of Ray, Ray will no longer override accelerator visible devices env var if num_gpus=0 or num_gpus=None (default).
os.environ["RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO"] = "0"


def __getattr__(name: str):
    if name == 'alias':
        from pfeed.utils.aliases import ALIASES
        return ALIASES
    elif name == 'plot':
        import pfund_plot as plot
        return plot
    elif name == 'StorageConfig':
        from pfeed.storages.storage_config import StorageConfig
        return StorageConfig
    elif name == 'DataEngine':
        from pfeed.engine import DataEngine
        return DataEngine
    elif name in ('YahooFinance', 'YFinance', 'YF'):
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
    elif name.lower() == 'alphafund':
        from pfeed.sources.alphafund import AlphaFund
        return AlphaFund
    elif name == 'LocalStorage':
        from pfeed.storages.local_storage import LocalStorage
        return LocalStorage
    elif name == 'CacheStorage':
        from pfeed.storages.cache_storage import CacheStorage
        return CacheStorage
    elif name == 'DuckDBStorage':
        from pfeed.storages.duckdb_storage import DuckDBStorage
        return DuckDBStorage
    elif name == 'LanceDBStorage':
        from pfeed.storages.lancedb_storage import LanceDBStorage
        return LanceDBStorage
    elif name == 'HuggingFaceStorage':
        from pfeed.storages.huggingface_storage import HuggingFaceStorage
        return HuggingFaceStorage
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")
    
    
__version__ = version("pfeed")
__all__ = (
    "__version__",
    "alias",
    # config
    "configure",
    'get_config',
    'configure_logging',
    "StorageConfig",
    # plot
    "plot",
    # engine
    "DataEngine",
    # storages
    "LocalStorage",
    "CacheStorage",
    "DuckDBStorage",
    "LanceDBStorage",
    "HuggingFaceStorage",
    # data sources
    "PFund",
    "AlphaFund",
    "YahooFinance", "YFinance", "YF",
    "Bybit",
    "FinancialModelingPrep", "FMP",
)
def __dir__():
    return sorted(__all__)
