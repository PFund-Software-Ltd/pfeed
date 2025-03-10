from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfund_plugins.base_plugin import BasePlugin
    # need these imports to support IDE hints:
    aliases = ...
    from pfeed.feeds import (
        Bybit,
        YahooFinance, YF,
        FinancialModelingPrep, FMP,
    )

from importlib.metadata import version
from pfeed.config import configure, get_config



plugins = {}
def add_plugin(plugin: BasePlugin):
    plugins[plugin.name] = plugin
    


def __getattr__(name: str):
    if name == 'aliases':
        from pfeed.const.aliases import ALIASES
        from pfund.const.aliases import ALIASES as PFUND_ALIASES
        return {**ALIASES, **PFUND_ALIASES}
    elif name in ('YahooFinance', 'YF'):
        from pfeed.feeds import YahooFinance
        return YahooFinance
    elif name in ('Bybit',):
        from pfeed.feeds import Bybit
        return Bybit
    elif name in ('FinancialModelingPrep', 'FMP'):
        from pfeed.feeds import FinancialModelingPrep
        return FinancialModelingPrep
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
    "what_is",
    "YahooFinance", "YF",
    "Bybit",
    "FinancialModelingPrep", "FMP",
)
