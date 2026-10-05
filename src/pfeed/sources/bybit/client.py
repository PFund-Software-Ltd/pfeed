from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

if TYPE_CHECKING:
    from pfeed.sources.bybit.market_feed import BybitMarketFeed

from pfeed.client import DataClient
from pfeed.sources.bybit.source import BybitSource


class Bybit(DataClient):
    DataSource: ClassVar[type[BybitSource]] = BybitSource
    data_source: BybitSource

    market_feed: BybitMarketFeed
