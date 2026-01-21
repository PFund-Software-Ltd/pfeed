from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfeed.typing import tDataCategory
    from pfeed.enums import DataCategory
    from pfeed.sources.data_provider_source import DataProviderSource
    from pfeed.feeds.base_feed import BaseFeed

from abc import ABC, abstractmethod

from pfeed.feeds import create_feed


class DataClient(ABC):
    def __init__(
        self,
        # NOTE: these params should be the same as the ones in BaseFeed
        pipeline_mode: bool=False,
        **kwargs,
    ):
        '''
        Args:
            kwargs: kwargs specific to the data client, e.g. api_key for Databento
        '''

        params = {k: v for k, v in locals().items() if k not in ['self', 'kwargs']}
        params.update(kwargs)

        self._pipeline_mode: bool = pipeline_mode
        self.data_source = self._create_data_source()

        # initialize data feeds
        for data_category in self.data_categories:
            feed: BaseFeed = create_feed(
                data_source=self.name,
                data_category=data_category,
                **params,
            )
            # dynamically set attributes e.g. self.market_feed
            setattr(self, data_category.feed_name, feed)
    
    def get_feed(self, data_category: DataCategory | tDataCategory) -> BaseFeed | None:
        return getattr(self, DataCategory[data_category.upper()].feed_name, None)
    
    @staticmethod
    @abstractmethod
    def _create_data_source(*args, **kwargs) -> DataProviderSource:
        pass

    @property
    def name(self) -> str:
        return self.data_source.name
    
    @property
    def data_categories(self) -> list[DataCategory]:
        return self.data_source.data_categories