from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfund.typing import tEnvironment
    from pfeed.typing import tDataTool, tDataSource, tDataCategory
    from pfeed.feeds.base_feed import BaseFeed
    from pfeed.data_client import DataClient
    from pfeed.flows.dataflow import DataFlow

from pfeed.enums import DataSource, DataCategory


class DataEngine:
    def __init__(
        self,
        env: tEnvironment,
        data_tool: tDataTool='polars', 
        use_ray: bool=True,
        use_prefect: bool=False,
        use_deltalake: bool=False,
    ):
        self._params = {k: v for k, v in locals().items() if k not in ['self']}
        # NOTE: pipeline_mode must be turned on for the engine to work so that users can add tasks to the feed
        self._params['pipeline_mode'] = True
        self._feeds: list[BaseFeed] = []
    
    @property
    def feeds(self) -> dict[DataCategory, list[BaseFeed]]:
        return self._feeds
    
    def add_feed(self, data_source: tDataSource, data_category: tDataCategory, **kwargs) -> BaseFeed:
        '''
        Args:
            kwargs: kwargs for the data client to override the default params in the engine
        '''
        assert 'pipeline_mode' not in kwargs, 'pipeline_mode cannot be overridden'
        DataClientClass: type[DataClient] = DataSource[data_source.upper()].data_client
        Feed: type[BaseFeed] = DataClientClass.get_Feed(data_category)
        feed: BaseFeed = Feed(**self._params, **kwargs)
        self._feeds.append(feed)
        return feed
    
    def get_feeds(self, data_source: tDataSource | None=None, data_category: tDataCategory | None=None) -> list[BaseFeed]:
        """Filter feeds by source and/or category"""
        filtered_feeds = self._feeds
        if data_source:
            filtered_feeds = [f for f in filtered_feeds if f.data_source.name == DataSource[data_source.upper()]]
        if data_category:
            filtered_feeds = [f for f in filtered_feeds if f.data_domain == DataCategory[data_category.upper()]]
        return filtered_feeds
    
    def run(self, ray_kwargs: dict | None=None, prefect_kwargs: dict | None=None) -> tuple[list[DataFlow], list[DataFlow]]:
        for feed in self._feeds:
            # TODO: assert a feed has a task
            feed.run()
    
    # TODO
    def end(self):
        pass
