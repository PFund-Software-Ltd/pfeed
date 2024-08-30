"""High-level API for getting historical/streaming data from Binance."""
from __future__ import annotations

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfeed.types.common_literals import tSUPPORTED_DATA_TOOLS
    from pfeed.config_handler import ConfigHandler

from pfeed.feeds.base_feed import BaseFeed


__all__ = ['BinanceFeed']


class BinanceFeed(BaseFeed):
    def __init__(self, data_tool: tSUPPORTED_DATA_TOOLS='pandas', config: ConfigHandler | None=None):
        super().__init__('binance', data_tool=data_tool, config=config)
        
    # TODO
    def get_realtime_data(self, env='LIVE'):
        pass
