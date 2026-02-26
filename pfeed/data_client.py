from __future__ import annotations
from typing import TYPE_CHECKING, Literal
if TYPE_CHECKING:
    from pfeed.sources.base_source import BaseSource
    from pfeed.feeds.base_feed import BaseFeed

from abc import ABC, abstractmethod

from pfeed.enums import DataCategory
from pfeed.feeds import create_feed


class DataClient(ABC):
    def __init__(
        self,
        # NOTE: these params should be the same as the ones in BaseFeed
        pipeline_mode: bool=False,
        num_batch_workers: int | dict[Literal['market_feed', 'news_feed'], int] | None = None,
        num_stream_workers: int | dict[Literal['market_feed', 'news_feed'], int] | None = None,
    ):
        self._pipeline_mode: bool = pipeline_mode
        self.data_source: BaseSource = self._create_data_source()
        self._num_batch_workers: int | dict[Literal['market_feed', 'news_feed'], int] | None = num_batch_workers
        self._num_stream_workers: int | dict[Literal['market_feed', 'news_feed'], int] | None = num_stream_workers

        # initialize data feeds
        self._create_feeds()
            
    def is_pipeline(self) -> bool:
        return self._pipeline_mode
    
    def get_feed(self, data_category: DataCategory) -> BaseFeed | None:
        return getattr(self, DataCategory[data_category.upper()].feed_name, None)
    
    @staticmethod
    @abstractmethod
    def _create_data_source() -> BaseSource:
        pass

    def _create_feeds(self):
        for data_category in self.data_categories:
            feed_name = data_category.feed_name
            num_batch_workers: int | None = (
                self._num_batch_workers[feed_name] 
                if isinstance(self._num_batch_workers, dict) 
                else self._num_batch_workers
            )
            num_stream_workers: int | None = (
                self._num_stream_workers[feed_name] 
                if isinstance(self._num_stream_workers, dict) 
                else self._num_stream_workers
            )
            feed: BaseFeed = create_feed(
                data_source=self.data_source.name,
                data_category=data_category,
                pipeline_mode=self._pipeline_mode,
                num_batch_workers=num_batch_workers,
                num_stream_workers=num_stream_workers,
            )
            # dynamically set attributes e.g. self.market_feed
            setattr(self, feed_name, feed)

    @property
    def name(self) -> str:
        return self.data_source.name
    
    @property
    def data_categories(self) -> list[DataCategory]:
        return self.data_source.data_categories