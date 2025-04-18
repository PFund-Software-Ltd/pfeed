from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    # need these imports to support IDE hints:
    import pfund_plot as plot
    from pfund_plugins.base_plugin import BasePlugin
    from pfeed.storages.duckdb_storage import DuckDBStorage
    from pfeed.storages.minio_storage import MinioStorage
    from pfeed.storages.local_storage import LocalStorage
    from pfeed.const.aliases import ALIASES as aliases
    from pfeed.feeds.yahoo_finance.yahoo_finance import (
        YahooFinance,
        YahooFinance as YF,
    )
    from pfeed.feeds.bybit.bybit import (
        BybitMarketFeed as Bybit,
    )
    from pfeed.feeds.financial_modeling_prep.financial_modeling_prep import (
        FinancialModelingPrep,
        FinancialModelingPrep as FMP,
    )
    from pfeed.feeds.pfund import (
        PFund,
    )

from importlib.metadata import version
from pfeed.config import configure, get_config
from pfeed.storages import create_storage
from pfeed.feeds import get_market_feed


# FIXME
plugins = {}
def add_plugin(plugin: BasePlugin):
    plugins[plugin.name] = plugin


def __getattr__(name: str):
    if name == 'aliases':
        from pfeed.const.aliases import ALIASES
        from pfund.const.aliases import ALIASES as PFUND_ALIASES
        return {**ALIASES, **PFUND_ALIASES}
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
        from pfeed.feeds.yahoo_finance.yahoo_finance import YahooFinance
        return YahooFinance
    elif name in ('Bybit',):
        from pfeed.feeds.bybit.bybit import BybitMarketFeed as Bybit
        return Bybit
    elif name in ('FinancialModelingPrep', 'FMP'):
        from pfeed.feeds.financial_modeling_prep.financial_modeling_prep import FinancialModelingPrep
        return FinancialModelingPrep
    elif name == 'PFund':
        from pfeed.feeds.pfund import PFund
        return PFund
    elif name in plugins:
        return plugins[name]
    raise AttributeError(f"'{__name__}' object has no attribute '{name}'")
    
    
# TODO: add llm plugin to explain "what is xxx?"
def what_is(alias: str) -> str | None:
    from pfeed.const.aliases import ALIASES
    from pfund.const.aliases import ALIASES as PFUND_ALIASES
    if alias in PFUND_ALIASES or alias.upper() in PFUND_ALIASES:
        return PFUND_ALIASES.get(alias, PFUND_ALIASES.get(alias.upper(), None))
    elif alias in ALIASES or alias.upper() in ALIASES:
        return aliases.get(alias, aliases.get(alias.upper(), None))


__version__ = version("pfeed")
__all__ = (
    "__version__",
    "configure",
    "get_config",
    "add_plugin",
    "aliases",
    "plot",
    "what_is",
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
    "FinancialModelingPrep", "FMP",
    "PFund",
)
def __dir__():
    return sorted(__all__)
