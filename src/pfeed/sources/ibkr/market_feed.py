from typing import ClassVar

from pfeed.feeds.market_feed import MarketFeed
from pfeed.feeds.streaming_feed_mixin import StreamingFeedMixin
from pfeed.sources.ibkr.source import InteractiveBrokersSource


class InteractiveBrokersMarketFeed(StreamingFeedMixin, MarketFeed):
    DataSource: ClassVar[type[InteractiveBrokersSource]] = InteractiveBrokersSource
    data_source: InteractiveBrokersSource
