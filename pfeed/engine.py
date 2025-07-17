from __future__ import annotations
from typing import TYPE_CHECKING, Coroutine
if TYPE_CHECKING:
    from pfund.typing import tEnvironment
    from pfeed.typing import tDataTool, tDataSource, tDataCategory
    from pfeed.feeds.base_feed import BaseFeed
    from pfeed.flows.dataflow import DataFlow

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
        backfill: bool=True,
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
        self._backfill: bool = backfill
    
    @property
    def feeds(self) -> dict[DataCategory, list[BaseFeed]]:
        return self._feeds
    
    # TODO: async background task
    def backfill(self):
        pass
    
    def add_feed(self, data_source: tDataSource, data_category: tDataCategory, **kwargs) -> BaseFeed:
        '''
        Args:
            kwargs: kwargs for the data client to override the default params in the engine
        '''
        assert 'pipeline_mode' not in kwargs, 'pipeline_mode cannot be overridden'
        feed: BaseFeed = pe.get_feed(data_source=data_source, data_category=data_category, **self._params, **kwargs)
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
        coroutines: list[Coroutine] = []
        # FIXME: currently ws_api in bybit stream_api is different from the one in pfund.
        for feed in self._feeds:
            # TODO: assert a feed has a task
            # TODO: use a thread?
            # TODO: add callback to feed.stream()
            if not feed.streaming_dataflows:
                feed.run()
            else:
                coro: Coroutine = feed.run_async()
                coroutines.append(coro)
                
    
    # TODO
    def end(self):
        pass
