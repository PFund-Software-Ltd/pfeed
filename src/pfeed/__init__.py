from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    # need these imports to support IDE hints:
    import pfund_plot as plot

    # NOTE: data sources (plugins), for IDE hints of pe.<Client> only, never executed at runtime.
    # official: pfeed_bybit
    # third-party (merged via PR, NOT installed in pfeed's env, do not install them;
    #   listed in ty.toml allowed-unresolved-imports instead): (none yet)
    from pfeed_bybit import Bybit as Bybit

    from pfeed.engine import DataEngine
    from pfeed.feeds import get_feed
    from pfeed.io.ducklake_io import DuckLakeIO
    from pfeed.io.parquet_io import ParquetIO

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
    elif name == "get_feed":
        from pfeed.feeds import get_feed

        return get_feed
    elif name == "DataEngine":
        from pfeed.engine import DataEngine

        return DataEngine
    elif name in _get_clients():
        # data source client class (e.g. pe.Bybit), exposed by a plugin's entry point
        return _get_clients()[name].load()
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")


__all__ = (  # noqa: RUF022
    "get_config", "configure", "configure_logging",
    "plot",
    "DataEngine",
    "get_feed",
    # IOs
    "DuckLakeIO",
    "ParquetIO",
)


def _get_clients():
    """Returns {client class name: entry point} of installed data sources, e.g. {"Bybit": ep}."""
    from pfeed.registry import get_entry_points

    return {ep.attr: ep for ep in get_entry_points().values()}


def __dir__():
    return sorted([*__all__, *_get_clients()])
