from __future__ import annotations
from typing import Literal, TYPE_CHECKING
if TYPE_CHECKING:
    from pfeed.typing import GenericFrame
    from pfeed.typing import tSTORAGE

from pfeed.feeds.market_feed import MarketFeed


class CryptoMarketFeed(MarketFeed):
    pass
