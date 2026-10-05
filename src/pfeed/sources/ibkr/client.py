from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

if TYPE_CHECKING:
    from pfeed.sources.ibkr.market_feed import InteractiveBrokersMarketFeed

from pfeed.client import DataClient
from pfeed.sources.ibkr.source import InteractiveBrokersSource


class InteractiveBrokers(DataClient):
    DataSource: ClassVar[type[InteractiveBrokersSource]] = InteractiveBrokersSource
    data_source: InteractiveBrokersSource

    market_feed: InteractiveBrokersMarketFeed
