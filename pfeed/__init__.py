from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pfeed.config_handler import ConfigHandler
    from pfeed.types.common_literals import tSUPPORTED_DOWNLOAD_DATA_SOURCES, tSUPPORTED_DATA_TYPES

import importlib
from importlib.metadata import version

from pfeed import etl
from pfeed.config_handler import configure
from pfeed.const.common import ALIASES
from pfeed.sources import bybit
from pfeed.feeds import BybitFeed, YahooFinanceFeed


def download(
    data_source: tSUPPORTED_DOWNLOAD_DATA_SOURCES,
    pdts: str | list[str] | None = None,
    dtypes: tSUPPORTED_DATA_TYPES | list[tSUPPORTED_DATA_TYPES] | None = None,
    ptypes: str | list[str] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    num_cpus: int = 8,
    use_ray: bool = True,
    use_minio: bool = False,
    debug: bool = False,
    config: ConfigHandler | None = None,
    env_file_path: str | None = None,
):
    data_source = importlib.import_module(f"pfeed.sources.{data_source.lower()}")
    return data_source.download(
        pdts,
        dtypes,
        ptypes,
        start_date,
        end_date,
        num_cpus,
        use_ray,
        use_minio,
        debug,
        config,
        env_file_path,
    )


__version__ = version("pfeed")
__all__ = (
    "__version__",
    "configure",
    "ALIASES",
    "etl",
    "bybit",
    "binance",
    "YahooFinanceFeed",
    "BybitFeed",
    "BinanceFeed",
)
