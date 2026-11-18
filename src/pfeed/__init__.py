from __future__ import annotations

from typing import TYPE_CHECKING

import sys

if TYPE_CHECKING:
    # need these imports to support IDE hints:
    # NOTE: data sources (plugins), for IDE hints of pe.<Source> only, never executed at runtime.
    # pe.<Source> is reserved for OFFICIAL sources, only they are listed here and in __getattr__.
    # third-party sources are NOT exposed as pe.<Source>, use either:
    #   from pfeed_xxx import XXX
    #   pe.get_feed("xxx", ...)

    # from pfeed_binance import Binance  # TODO
    # from pfeed_okx import OKX  # TODO
    from alphafund._pfeed_source import AlphaFund  # ty: ignore[import-not-found]
    from pfeed_bybit import Bybit
    from pfund._pfeed_source import PFund  # ty: ignore[import-not-found]

    from pfeed.engine import DataEngine
    from pfeed.io.ducklake_io import DuckLakeIO
    from pfeed.io.parquet_io import ParquetIO
    from pfeed.market.feed import MarketFeed
    from pfeed.registry import get_feed

from pfeed.config import configure, configure_logging, get_config


def __getattr__(name: str):
    if name == "__version__":
        from importlib.metadata import version

        return version("pfeed")
    elif name == "ParquetIO":
        from pfeed.io.parquet_io import ParquetIO

        return ParquetIO
    elif name == "DuckLakeIO":
        from pfeed.io.ducklake_io import DuckLakeIO

        return DuckLakeIO
    elif name == "MarketFeed":
        from pfeed.market.feed import MarketFeed

        return MarketFeed
    elif name == "get_feed":
        from pfeed.registry import get_feed

        return get_feed
    elif name == "DataEngine":
        from pfeed.engine import DataEngine

        return DataEngine
    elif name == "Bybit":
        return _import_official_source("pfeed_bybit", "Bybit")

    elif name == "PFund":
        return _get_official_source("PFund", install="pfund")

    elif name == "AlphaFund":
        return _get_official_source("AlphaFund", install="alphafund")
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")


def _import_official_source(module: str, source: str):
    """Imports an official data source class (e.g. pe.Bybit) from its plugin, e.g. module="pfeed_bybit".

    The install hint points to pfeed's extra (e.g. "pfeed[bybit]"), which pins a verified plugin version.
    """
    from pfund_kit.plugins import import_official_plugin

    extra = module.removeprefix("pfeed_")
    return import_official_plugin(
        module, source, label=f"pe.{source}", install=f'"pfeed[{extra}]"'
    )


def _get_official_source(source: str, install: str):
    """Loads an official data source class (e.g. pe.PFund) via its "pfeed.sources" entry point.

    Used for sources shipped inside another package (e.g. pfund), so pfeed doesn't hardcode
    their (private) module path, the package registering the entry point owns it.
    """
    from pfeed.registry import get_entry_points, get_source

    if source.upper() not in get_entry_points():
        raise ModuleNotFoundError(
            f"pe.{source} requires '{install}', install it with: pip install {install}"
        )
    return get_source(source)


__all__ = (  # noqa: RUF022
    "get_config",
    "configure",
    "configure_logging",
    "plot",
    "DataEngine",
    "get_feed",
    # Feeds (for the convenience of inheritance)
    "MarketFeed",
    # IOs
    "DuckLakeIO",
    "ParquetIO",
    # data sources
    "AlphaFund",
    "PFund",
    "Bybit",
)

# pdoc only documents names already in the module, not ones from __getattr__
if "pdoc" in sys.modules:
    from pfeed.engine import DataEngine
    from pfeed.io.ducklake_io import DuckLakeIO
    from pfeed.io.parquet_io import ParquetIO
    from pfeed.market.feed import MarketFeed
    from pfeed.registry import get_feed

    # pdoc getattr()s every name in __all__, sources whose package isn't installed raise ModuleNotFoundError
    __all__ = tuple(n for n in __all__ if n not in ("AlphaFund", "PFund", "Bybit"))


def __dir__():
    return sorted(__all__)
