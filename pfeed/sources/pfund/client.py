from __future__ import annotations
from typing import TYPE_CHECKING, cast
if TYPE_CHECKING:
    from pfeed.feeds.base_feed import BaseFeed

from pfund.enums import Environment
from pfeed.sources.pfund.component_feed import ComponentFeed
from pfeed.sources.pfund.engine_feed import EngineFeed
from pfeed.sources.pfund.source import PFundDataCategory
from pfeed.sources.pfund.mixin import PFundMixin
from pfeed.data_client import DataClient


__all__ = ['PFund']


class PFund(PFundMixin, DataClient):
    engine_feed: EngineFeed
    component_feed: ComponentFeed

    def __init__(
        self, 
        env: Environment,
        engine_name: str='engine',
        pipeline_mode: bool=False,
    ):
        self._env = Environment[env.upper()]
        self._engine_name = engine_name
        self.engine_feed: EngineFeed | None = None
        self.component_feed: ComponentFeed | None = None
        super().__init__(pipeline_mode=pipeline_mode)
    
    def _create_feeds(self):
        for data_category in cast(list[PFundDataCategory], self.data_categories):
            if data_category == PFundDataCategory.ENGINE_DATA:
                self.engine_feed = EngineFeed(env=self._env, engine_name=self._engine_name)
            elif data_category == PFundDataCategory.COMPONENT_DATA:
                self.component_feed = ComponentFeed(env=self._env, engine_name=self._engine_name)
            else:
                raise ValueError(f'{data_category} is not supported')

    def get_feed(self, data_category: PFundDataCategory) -> BaseFeed | None:
        return getattr(self, PFundDataCategory[data_category.upper()].feed_name, None)
