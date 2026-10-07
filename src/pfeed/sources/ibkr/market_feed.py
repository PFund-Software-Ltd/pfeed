from typing import ClassVar

from pfeed.market.feed import MarketFeed
from pfeed.sources.ibkr.source import InteractiveBrokersSource
from pfeed.streaming.feed_mixin import StreamingFeedMixin


class InteractiveBrokersMarketFeed(StreamingFeedMixin, MarketFeed):
    DataSource: ClassVar[type[InteractiveBrokersSource]] = InteractiveBrokersSource
    data_source: InteractiveBrokersSource
