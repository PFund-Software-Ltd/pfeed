from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfeed.sources.base_source import BaseSource
    from pfeed.feeds.base_feed import BaseFeed

from abc import ABC, abstractmethod

from pfeed.feeds import create_feed


class DataClient(ABC):
    def __init__(self, pipeline_mode: bool=False):
        self._pipeline_mode: bool = pipeline_mode
        self.data_source: BaseSource = self._create_data_source()
        self._feeds: list[BaseFeed] = []
        self._create_feeds()
    
    @staticmethod
    @abstractmethod
    def _create_data_source() -> BaseSource:
        pass
    
    @property
    def name(self) -> str:
        return self.data_source.name
    
    @property
    def feeds(self) -> list[BaseFeed]:
        return self._feeds    
            
    def is_pipeline(self) -> bool:
        return self._pipeline_mode
    
    def _create_feeds(self):
        for data_category in self.data_source.get_data_categories():
            feed: BaseFeed = create_feed(
                data_source=self.name,
                data_category=data_category,
                pipeline_mode=self._pipeline_mode,
            )
            if feed not in self._feeds:
                self._feeds.append(feed)
            # dynamically set attributes e.g. self.market_feed
            setattr(self, data_category.feed_name, feed)
