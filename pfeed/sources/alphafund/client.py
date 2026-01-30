from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfeed.feeds.base_feed import BaseFeed
    from pfeed.sources.alphafund.chat_feed import ChatFeed

from pfeed.sources.alphafund.source import AlphaFundSource
from pfeed.sources.alphafund.mixin import AlphaFundMixin
from pfeed.data_client import DataClient
from pfeed.sources.alphafund.source import AlphaFundDataCategory


__all__ = ['AlphaFund']


class AlphaFund(AlphaFundMixin, DataClient):
    chat_feed: ChatFeed
    
    def __init__(self):
        self.chat_feed: ChatFeed | None = None
        
    def _create_feeds(self):
        for data_category in self.data_categories:
            if data_category == AlphaFundDataCategory.CHAT_DATA:
                self.chat_feed = ChatFeed()
            else:
                raise ValueError(f'{data_category} is not supported')
    
    def get_feed(self, data_category: AlphaFundDataCategory) -> BaseFeed | None:
        return getattr(self, AlphaFundDataCategory[data_category.upper()].feed_name, None)
        
        
        
# TEMP
if __name__ == '__main__':
    alphafund = AlphaFund()