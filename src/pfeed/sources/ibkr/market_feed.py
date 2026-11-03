from typing import ClassVar

from pfeed.market.feed import MarketFeed
from pfeed.sources.ibkr.source import InteractiveBrokersSource
from pfeed.streaming.feed_mixin import StreamingMixin


class InteractiveBrokersMarketFeed(StreamingMixin, MarketFeed):
    DataSource: ClassVar[type[InteractiveBrokersSource]] = InteractiveBrokersSource
    data_source: InteractiveBrokersSource
