from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    # need these imports to support IDE hints:
    import pfund_plot as plot
    from pfeed.storages.duckdb_storage import DuckDBStorage
    from pfeed.storages.minio_storage import MinioStorage
    from pfeed.storages.local_storage import LocalStorage
    from pfeed.const.aliases import ALIASES as aliases
    from pfeed.sources.yahoo_finance import (
        YahooFinance,
        YahooFinance as YF,
    )
    from pfeed.sources.bybit import Bybit
    from pfeed.sources.financial_modeling_prep import (
        FinancialModelingPrep,
        FinancialModelingPrep as FMP,
    )
    from pfeed.sources.pfund import PFund

from importlib.metadata import version
from pfeed.config import configure, get_config
from pfeed.storages import create_storage
from pfeed.feeds import get_market_feed


def __getattr__(name: str):
    if name == 'aliases':
        from pfeed.const.aliases import ALIASES
        return ALIASES
    elif name == 'plot':
        import pfund_plot as plot
        return plot
    elif name == 'DuckDBStorage':
        from pfeed.storages.duckdb_storage import DuckDBStorage
        return DuckDBStorage
    elif name == 'MinioStorage':
        from pfeed.storages.minio_storage import MinioStorage
        return MinioStorage
    elif name == 'LocalStorage':
        from pfeed.storages.local_storage import LocalStorage
        return LocalStorage
    elif name in ('YahooFinance', 'YF'):
        from pfeed.sources.yahoo_finance import YahooFinance
        return YahooFinance
    elif name in ('Bybit',):
        from pfeed.sources.bybit import Bybit
        return Bybit
    elif name in ('FinancialModelingPrep', 'FMP'):
        from pfeed.sources.financial_modeling_prep import FinancialModelingPrep
        return FinancialModelingPrep
    elif name == 'PFund':
        from pfeed.sources.pfund import PFund
        return PFund
    raise AttributeError(f"'{__name__}' object has no attribute '{name}'")
    
    
__version__ = version("pfeed")
__all__ = (
    "__version__",
    "configure",
    "get_config",
    "aliases",
    "plot",
    # sugar functions
    "create_storage",
    "get_market_feed",
    # storage classes
    "DuckDBStorage",
    "MinioStorage",
    "LocalStorage",
    # data sources
    "YahooFinance", "YF",
    "Bybit",
    "PFund",
    "FinancialModelingPrep", "FMP",
)
def __dir__():
    return sorted(__all__)
