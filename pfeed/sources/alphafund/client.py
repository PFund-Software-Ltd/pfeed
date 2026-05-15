from pfeed.data_client import DataClient
from pfeed.sources.alphafund.mixin import AlphaFundMixin
from pfeed.sources.alphafund.chat_feed import AlphaFundChatFeed


__all__ = ['AlphaFund']


class AlphaFund(AlphaFundMixin, DataClient):
    chat_feed: AlphaFundChatFeed
    
    def _create_feeds(self):
        self.chat_feed = AlphaFundChatFeed()
