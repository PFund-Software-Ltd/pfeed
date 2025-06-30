from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfund.typing import tEnvironment
    from pfeed.typing import tDataTool, tDataCategory
    from pfeed.enums import DataCategory
    from pfeed.sources.base_source import BaseSource
    from pfeed.feeds.base_feed import BaseFeed

from pfund.enums import Environment
from pfeed.enums import DataSource, DataTool


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
        from pfeed.utils.utils import to_snake_case
        from pfeed.feeds import get_feed

        params = {k: v for k, v in locals().items() if k not in ['self', 'kwargs']}
        params.update(kwargs)
        
        self._env = Environment[env.upper()]
        self._data_tool = DataTool[data_tool.lower()]
        self._pipeline_mode: bool = pipeline_mode
        self._use_ray: bool = use_ray
        self._use_prefect: bool = use_prefect
        self._use_deltalake: bool = use_deltalake

        data_source: BaseSource = DataSource[to_snake_case(self.__class__.__name__).upper()].create_data_source(env)
        
        # initialize data feeds
        for data_category in self.data_categories:
            feed: BaseFeed = get_feed(
                data_source=data_source,
                data_category=data_category,
                **params,
            )
            # dynamically set attributes e.g. self.market_feed
            setattr(self, data_category.feed_name, feed)  
    
    def get_feed(self, data_category: DataCategory | tDataCategory) -> BaseFeed | None:
        return getattr(self, DataCategory[data_category.upper()].feed_name, None)

    @property
    def name(self) -> str:
        return self.data_source.name
    
    @property
    def data_categories(self) -> list[DataCategory]:
        return self.data_source.data_categories