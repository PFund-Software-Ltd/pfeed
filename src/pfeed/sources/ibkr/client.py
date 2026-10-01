from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pfeed.sources.ibkr.market_feed import InteractiveBrokersMarketFeed

from pfeed.data_client import DataClient
from pfeed.sources.ibkr.mixin import InteractiveBrokersMixin


class InteractiveBrokers(InteractiveBrokersMixin, DataClient):
    market_feed: InteractiveBrokersMarketFeed
