from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfeed.plugins.base_plugin import BasePlugin

from importlib.metadata import version

from pfeed.config_handler import configure, get_config
from pfeed.const.aliases import ALIASES as aliases


plugins = {}
def add_plugin(plugin: BasePlugin):
    plugins[plugin.name] = plugin
    

def __getattr__(name: str):
    if name in plugins:
        return plugins[name]
    raise AttributeError(f"'{__name__}' object has no attribute '{name}'")
    
    
def what_is(alias: str) -> str | None:
    from pfund.const.aliases import ALIASES as pfund_aliases
    if alias in pfund_aliases or alias.upper() in pfund_aliases:
        return pfund_aliases.get(alias, pfund_aliases.get(alias.upper(), None))
    elif alias in aliases or alias.upper() in aliases:
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
