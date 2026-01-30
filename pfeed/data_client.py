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
        ray_kwargs: dict | dict[Literal['market_feed', 'news_feed'], dict] | None = None,
    ):
        self._pipeline_mode: bool = pipeline_mode
        self.data_source: BaseSource = self._create_data_source()
        self._ray_kwargs: dict = ray_kwargs or {}

        # initialize data feeds
        self._create_feeds()
            
    def is_pipeline(self) -> bool:
        return self._pipeline_mode
    
    def get_feed(self, data_category: DataCategory) -> BaseFeed | None:
        return getattr(self, DataCategory[data_category.upper()].feed_name, None)
    
    @staticmethod
    @abstractmethod
    def _create_data_source(*args, **kwargs) -> BaseSource:
        pass

    def _create_feeds(self):
        for data_category in self.data_categories:
            feed: BaseFeed = create_feed(
                data_source=self.data_source.name,
                data_category=data_category,
                pipeline_mode=self._pipeline_mode,
                **self._ray_kwargs.get(data_category.feed_name, self._ray_kwargs),
            )
            # dynamically set attributes e.g. self.market_feed
            setattr(self, data_category.feed_name, feed)

    @property
    def name(self) -> str:
        return self.data_source.name
    
    @property
    def data_categories(self) -> list[DataCategory]:
        return self.data_source.data_categories