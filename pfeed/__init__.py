from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pfeed.types.common_literals import tSUPPORTED_DOWNLOAD_DATA_SOURCES, tSUPPORTED_DATA_TYPES

import importlib
from importlib.metadata import version

from pfeed.config_handler import configure, get_config
from pfeed.const.common import ALIASES
from pfeed.sources import bybit
from pfeed.feeds import BybitFeed, YahooFinanceFeed


def download_historical_data(
    data_source: tSUPPORTED_DOWNLOAD_DATA_SOURCES,
    pdts: str | list[str] | None = None,
    dtypes: tSUPPORTED_DATA_TYPES | list[tSUPPORTED_DATA_TYPES] | None = None,
    ptypes: str | list[str] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    num_cpus: int = 8,
    use_ray: bool = True,
    use_minio: bool = False,
):
    data_source = importlib.import_module(f"pfeed.sources.{data_source.lower()}")
    return data_source.download_historical_data(
        pdts=pdts,
        dtypes=dtypes,
        ptypes=ptypes,
        start_date=start_date,
        end_date=end_date,
        num_cpus=num_cpus,
        use_ray=use_ray,
        use_minio=use_minio,
    )


# TODO
def stream_realtime_data(data_source: tSUPPORTED_DOWNLOAD_DATA_SOURCES):
    data_source = importlib.import_module(f"pfeed.sources.{data_source.lower()}")
    return data_source.stream_realtime_data()
    


download = download_historical_data
stream = stream_realtime_data


__version__ = version("pfeed")
__all__ = (
    "__version__",
    "configure",
    "get_config",
    "ALIASES",
    "bybit",
    "binance",
    "YahooFinanceFeed",
    "BybitFeed",
    "BinanceFeed",
)
