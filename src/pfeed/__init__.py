from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    # need these imports to support IDE hints:
    import pfund_plot as plot

    # NOTE: data sources (plugins), for IDE hints of pe.<Source> only, never executed at runtime.
    # pe.<Source> is reserved for OFFICIAL sources, only they are listed here and in __getattr__.
    # third-party sources are NOT exposed as pe.<Source>, use either:
    #   from pfeed_xxx import XXX
    #   pe.get_feed("xxx", ...)
    from pfeed_bybit import Bybit as Bybit

    from pfeed.base import get_feed
    from pfeed.engine import DataEngine
    from pfeed.io.ducklake_io import DuckLakeIO
    from pfeed.io.parquet_io import ParquetIO
    from pfeed.market.feed import MarketFeed

from pfeed.config import configure, configure_logging, get_config


def __getattr__(name: str):
    if name == "__version__":
        from importlib.metadata import version

        return version("pfeed")
    elif name == "plot":
        import pfund_plot as plot

        return plot
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
        from pfeed.base import get_feed

        return get_feed
    elif name == "DataEngine":
        from pfeed.engine import DataEngine

        return DataEngine
    elif name == "Bybit":
        return _import_official_source("pfeed_bybit", "Bybit")
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")


def _import_official_source(module: str, source: str):
    """Imports an official data source class (e.g. pe.Bybit) from its plugin, e.g. module="pfeed_bybit"."""
    from importlib import import_module

    try:
        return getattr(import_module(module), source)
    except ModuleNotFoundError as err:
        # only rewrite the error if the plugin itself is missing, not a broken import inside it
        if err.name != module:
            raise
        package = module.replace("_", "-")
        raise ModuleNotFoundError(
            f"pe.{source} requires the official plugin '{package}', install it with: pip install {package}",
            name=err.name,
        ) from err


__all__ = (  # noqa: RUF022
    "get_config", "configure", "configure_logging",
    "plot",
    "DataEngine",
    "get_feed",
    # Feeds (for the convenience of inheritance)
    "MarketFeed",
    # IOs
    "DuckLakeIO",
    "ParquetIO",
)


def __dir__():
    return sorted(__all__)
