from __future__ import annotations
import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pfeed.types.common_literals import tSUPPORTED_DOWNLOAD_DATA_SOURCES, tSUPPORTED_DATA_TYPES

import importlib
from importlib.metadata import version

from pfeed.llm_plugin import LLM, tFREE_LLM_PROVIDERS, get_llm_providers, get_free_llm_api_key
from pfeed.config_handler import configure, get_config
from pfeed.const.aliases import ALIASES as aliases
from pfeed.sources import bybit
from pfeed.feeds import BybitFeed, YahooFinanceFeed


llm_config = {}
def configure_llm(provider: tFREE_LLM_PROVIDERS | str, model: str=''):
    assert os.getenv(f'{provider.upper()}_API_KEY'), f'No {provider.upper()}_API_KEY found in environment variables, please set one'
    llm_config['provider'] = provider
    llm_config['model'] = model


def __getattr__(name: str):
    if name == 'llm':
        if not llm_config:
            raise Exception('No LLM provider found, please call configure_llm() first')
        return LLM(**llm_config)
    raise AttributeError(f"'{__name__}' object has no attribute '{name}'")
    
    
def what_is(alias: str) -> str | None:
    from pfund.const.aliases import ALIASES as pfund_aliases
    if alias in pfund_aliases or alias.upper() in pfund_aliases:
        return pfund_aliases.get(alias, pfund_aliases.get(alias.upper(), None))
    elif alias in aliases or alias.upper() in aliases:
        return aliases.get(alias, aliases.get(alias.upper(), None))


def download_historical_data(
    data_source: tSUPPORTED_DOWNLOAD_DATA_SOURCES,
    products: str | list[str] | None = None,
    dtypes: tSUPPORTED_DATA_TYPES | list[tSUPPORTED_DATA_TYPES] | None = None,
    ptypes: str | list[str] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    use_minio: bool = False,
    use_ray: bool = True,
    num_cpus: int = 8,
):
    data_source = importlib.import_module(f"pfeed.sources.{data_source.lower()}")
    return data_source.download_historical_data(
        products=products,
        dtypes=dtypes,
        ptypes=ptypes,
        start_date=start_date,
        end_date=end_date,
        use_minio=use_minio,
        use_ray=use_ray,
        num_cpus=num_cpus,
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
    "get_free_llm_api_key",
    "get_llm_providers",
    "aliases",
    "bybit",
    "binance",
    "YahooFinanceFeed",
    "BybitFeed",
    "BinanceFeed",
)
