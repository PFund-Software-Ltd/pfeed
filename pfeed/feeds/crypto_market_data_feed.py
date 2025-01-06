from __future__ import annotations
from typing import Literal, TYPE_CHECKING
if TYPE_CHECKING:
    from pfeed.typing.core import tDataFrame
    from pfeed.typing.literals import tSTORAGE

from pfeed.feeds.market_data_feed import MarketDataFeed


class CryptoMarketDataFeed(MarketDataFeed):
    pass
