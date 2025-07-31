from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfeed.sources.bybit.market_feed import BybitMarketFeed

from pfeed.data_client import DataClient
from pfeed.sources.bybit.mixin import BybitMixin


__all__ = ['Bybit']


class Bybit(BybitMixin, DataClient):
    market_feed: BybitMarketFeed
