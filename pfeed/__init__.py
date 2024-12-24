from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfeed.plugins.base_plugin import BasePlugin
    # need these imports to support IDE hints:
    config = ...
    configure = ...
    aliases = ...
    from pfeed.feeds import (
        BybitFeed,
        YahooFinanceFeed, 
    )

from importlib.metadata import version


plugins = {}
def add_plugin(plugin: BasePlugin):
    plugins[plugin.name] = plugin
    

def __getattr__(name: str):
    if name == 'config':
        from pfeed.config_handler import get_config
        return get_config()
    elif name == 'configure':
        from pfeed.config_handler import configure
        return configure
    elif name == 'aliases':
        from pfeed.const.aliases import ALIASES
        from pfund.const.aliases import ALIASES as PFUND_ALIASES
        return {**ALIASES, **PFUND_ALIASES}
    elif name == 'YahooFinanceFeed':
        from pfeed.feeds.yahoo_finance_feed import YahooFinanceFeed
        return YahooFinanceFeed
    elif name == 'BybitFeed':
        from pfeed.feeds.bybit_feed import BybitFeed
        return BybitFeed
    if name in plugins:
        return plugins[name]
    raise AttributeError(f"'{__name__}' object has no attribute '{name}'")
    
    
def what_is(alias: str) -> str | None:
    from pfeed.const.aliases import ALIASES
    from pfund.const.aliases import ALIASES as PFUND_ALIASES
    if alias in PFUND_ALIASES or alias.upper() in PFUND_ALIASES:
        return PFUND_ALIASES.get(alias, PFUND_ALIASES.get(alias.upper(), None))
    elif alias in ALIASES or alias.upper() in ALIASES:
        return aliases.get(alias, aliases.get(alias.upper(), None))


print_error = lambda msg: print(f'\033[91m{msg}\033[0m')
print_warning = lambda msg: print(f'\033[93m{msg}\033[0m')


__version__ = version("pfeed")
__all__ = (
    "__version__",
    "configure",
    "get_config",
    "add_plugin",
    "aliases",
    "what_is",
)
