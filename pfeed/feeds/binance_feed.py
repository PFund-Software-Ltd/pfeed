"""High-level API for getting historical/streaming data from Binance."""
from __future__ import annotations

import datetime

from typing import Literal, TYPE_CHECKING
if TYPE_CHECKING:
    from pfeed.types.common_literals import tSUPPORTED_DATA_TOOLS

try:
    import pandas as pd
    import polars as pl
    from pfeed.data_tools.data_tool_polars import estimate_memory_usage
except ImportError:
    pass

from pfeed.config_handler import ConfigHandler
from pfeed.const.common import SUPPORTED_DATA_TOOLS
from pfeed.feeds.base_feed import BaseFeed
from pfeed.sources.binance import api
from pfeed.sources.binance.const import DATA_SOURCE, SUPPORTED_PRODUCT_TYPES, create_efilename, SUPPORTED_RAW_DATA_TYPES
from pfeed.utils.utils import get_dates_in_between, rollback_date_range
from pfeed.utils.validate import validate_pdt


__all__ = ['BinanceFeed']


class BinanceFeed(BaseFeed):
    def __init__(self, config: ConfigHandler | None=None):
        super().__init__('binance', config=config)
        
    def get_historical_data(
        self,
        pdt: str,
        rollback_period: str='1w',
        # HACK: mixing resolution with dtypes for convenience
        resolution: str | Literal['raw', 'raw_tick']='1d',  
        start_date: str=None,
        end_date: str=None,
        data_tool: tSUPPORTED_DATA_TOOLS='pandas',
        memory_usage_limit_in_gb: int=2,  # in GB
    ):
        from pfund.datas.resolution import Resolution
        from pfeed import etl

        pass
        
    
    # TODO
    def get_realtime_data(self, env='LIVE'):
        pass
