from __future__ import annotations
from typing import Literal, TYPE_CHECKING
if TYPE_CHECKING:
    from pfeed._typing import GenericFrame
    from pfeed._typing import tStorage

from pfeed.feeds.market_feed import MarketFeed


class CryptoMarketFeed(MarketFeed):
    pass
