from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfund.typing import tEnvironment
    from pfeed.typing import tDataTool, tDataCategory
    from pfeed.enums import DataCategory
    from pfeed.sources.base_source import BaseSource
    from pfeed.feeds.base_feed import BaseFeed

import importlib

from pfund.enums import Environment
from pfeed.utils.utils import to_snake_case, to_camel_case


class DataClient:
    def __init__(
        self,
        env: tEnvironment,
        # NOTE: these params should be the same as the ones in BaseFeed
        data_tool: tDataTool='polars',
        pipeline_mode: bool=False,
        use_ray: bool=True,
        use_prefect: bool=False,
        use_deltalake: bool=False,
        **kwargs,
    ):
        '''
        Args:
            kwargs: kwargs specific to the data client, e.g. api_key for Databento
        '''
        params = {k: v for k, v in locals().items() if k not in ['self', 'env', 'kwargs']}
        params.update(kwargs)
        self._env = Environment[env.upper()]
        class_name = self.__class__.__name__
        script_name = to_snake_case(class_name)
        
        # initialize data source
        DataSourceClass = getattr(importlib.import_module(f'pfeed.sources.{script_name}.source'), f'{class_name}Source')
        self.data_source: BaseSource = DataSourceClass(env=env)

        # initialize data feeds
        for data_category in self.data_categories:
            feed_name = data_category.feed_name
            DataFeed = self.get_Feed(data_category)
            feed = DataFeed(data_source=self.data_source, **params)
            setattr(self, feed_name, feed)  # e.g. self.market_feed = MarketFeed(data_source=self.data_source, **params)
    
    @classmethod
    def get_Feed(cls, data_category: DataCategory | tDataCategory) -> type[BaseFeed]:
        class_name = cls.__name__
        script_name = to_snake_case(class_name)
        feed_name = data_category.feed_name
        Feed = getattr(importlib.import_module(f'pfeed.sources.{script_name}.{feed_name}'), f'{class_name}{to_camel_case(feed_name)}')
        return Feed
        
    def get_feed(self, data_category: DataCategory | tDataCategory) -> BaseFeed | None:
        return getattr(self, DataCategory[data_category.upper()].feed_name, None)

    @property
    def name(self) -> str:
        return self.data_source.name
    
    @property
    def data_categories(self) -> list[DataCategory]:
        return self.data_source.data_categories