from pfeed.feeds.market_feed import MarketFeed
from pfeed.feeds.streaming_feed_mixin import StreamingFeedMixin
from pfeed.sources.ibkr.mixin import InteractiveBrokersMixin


class InteractiveBrokersMarketFeed(
    StreamingFeedMixin, InteractiveBrokersMixin, MarketFeed
):
    pass
