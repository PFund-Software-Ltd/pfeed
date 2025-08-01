from __future__ import annotations
from typing import TYPE_CHECKING, Coroutine
if TYPE_CHECKING:
    from pfund._typing import tEnvironment
    from pfeed._typing import tDataTool, tDataSource, tDataCategory, GenericData
    from pfeed.feeds.base_feed import BaseFeed

import asyncio

import pfeed as pe
from pfeed.enums import DataSource, DataCategory


# NOTE: only data engine has the ability to run background tasks in pfeed
class DataEngine:
    def __init__(
        self,
        env: tEnvironment,
        data_tool: tDataTool='polars',
        use_ray: bool=True,
        use_prefect: bool=False,
        use_deltalake: bool=False,
        # TODO
        # backfill: bool=True,
    ):
        '''
        Args:
            backfill: Whether to backfill the data during streaming.
                only matters to streaming dataflows (i.e. feed.stream())
        '''
        self._params = {k: v for k, v in locals().items() if k not in ['self', 'backfill']}
        # NOTE: pipeline_mode must be turned on for the engine to work so that users can add tasks to the feed
        self._params['pipeline_mode'] = True
        self._feeds: list[BaseFeed] = []
        # self._backfill: bool = backfill
    
    @property
    def feeds(self) -> dict[DataCategory, list[BaseFeed]]:
        return self._feeds
    
    # TODO: async background task
    def backfill(self):
        raise NotImplementedError('Backfill is not implemented yet')
    
    def add_feed(self, data_source: tDataSource, data_category: tDataCategory, **kwargs) -> BaseFeed:
        '''
        Args:
            kwargs: kwargs for the data client to override the default params in the engine
        '''
        assert 'pipeline_mode' not in kwargs, 'pipeline_mode is True by default and cannot be overridden'
        feed: BaseFeed = pe.create_feed(data_source=data_source, data_category=data_category, **self._params, **kwargs)
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
    
    def _is_streaming_feeds(self) -> bool:
        # either all streaming dataflows or all batch dataflows, cannot mix them
        if is_streaming_feeds := any(feed.streaming_dataflows for feed in self._feeds):
            assert all(feed.streaming_dataflows for feed in self._feeds), 'All feeds must be streaming feeds if any feed is streaming'
        return is_streaming_feeds
    
    def _eager_run(
        self, 
        ray_kwargs: dict | None=None, 
        prefect_kwargs: dict | None=None, 
        include_metadata: bool=False
    ) -> dict[BaseFeed, GenericData] | list[Coroutine]:
        if not self._is_streaming_feeds():
            result: dict[BaseFeed, GenericData] = {}
            for feed in self._feeds:
                data = feed.run(prefect_kwargs=prefect_kwargs, include_metadata=include_metadata, **ray_kwargs)
                result[feed] = data
        else:
            result: list[Coroutine] = []
            for feed in self._feeds:
                coro: Coroutine = feed.run_async(prefect_kwargs=prefect_kwargs, include_metadata=include_metadata, **ray_kwargs)
                result.append(coro)
        return result
    
    def run(self, prefect_kwargs: dict | None=None, include_metadata: bool=False, **ray_kwargs) -> dict[BaseFeed, GenericData] | None:
        result: dict[BaseFeed, GenericData] | list[Coroutine] = self._eager_run(ray_kwargs=ray_kwargs, prefect_kwargs=prefect_kwargs, include_metadata=include_metadata)
        if not self._is_streaming_feeds():
            return result
        else:
            return asyncio.run(asyncio.gather(*result))
    
    async def run_async(self, prefect_kwargs: dict | None=None, include_metadata: bool=False, **ray_kwargs) -> None:
        assert self._is_streaming_feeds(), 'Only streaming feeds can be run asynchronously'
        result: list[Coroutine] = self._eager_run(ray_kwargs=ray_kwargs, prefect_kwargs=prefect_kwargs, include_metadata=include_metadata)
        return await asyncio.gather(*result)
    
    