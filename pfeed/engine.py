from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfeed.typing import tDataTool, tDataSource, tDataCategory
    from pfeed.feeds.base_feed import BaseFeed

from pfeed.enums import DataSource, DataCategory


class DataEngine:
    def __init__(
        self, 
        data_tool: tDataTool='polars', 
        pipeline_mode: bool=False,
        use_ray: bool=True,
        use_prefect: bool=False,
        use_deltalake: bool=False,
    ):
        self._params = {k: v for k, v in locals().items() if k not in ['self']}
        self._feeds: dict[DataCategory, list[BaseFeed]] = {}
    
    @property
    def feeds(self) -> dict[DataCategory, list[BaseFeed]]:
        return self._feeds
    
    def add_feed(self, data_source: tDataSource, data_category: tDataCategory, **kwargs) -> BaseFeed:
        '''
        Args:
            kwargs: kwargs for the data client to override the default params in the engine
        '''
        DataClient = DataSource[data_source.upper()].data_client
        data_category = DataCategory[data_category.upper()]
        feed_name = data_category.lower().replace('_data', '_feed')
        data_client = DataClient(**self._params, **kwargs)
        feed: BaseFeed = getattr(data_client, feed_name)
        if data_category not in self._feeds:
            self._feeds[data_category] = []
        self._feeds[data_category].append(feed)
        return feed
    
    # TODO
    def run(self):
        pass
    
    # TODO
    def end(self):
        pass
