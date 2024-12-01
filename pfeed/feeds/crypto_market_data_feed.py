from __future__ import annotations
from typing import Literal, TYPE_CHECKING
if TYPE_CHECKING:
    from pfeed.types.core import tDataFrame
    from pfeed.types.literals import tSTORAGE

from pfeed.feeds.market_data_feed import MarketDataFeed


class CryptoMarketDataFeed(MarketDataFeed):
    pass
