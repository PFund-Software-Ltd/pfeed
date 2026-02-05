from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfeed.feeds.base_feed import BaseFeed
    from pfeed.sources.pfund.component_feed import PFundComponentFeed
    from pfeed.sources.pfund.engine_feed import PFundEngineFeed

from pfund.enums import Environment
from pfeed.sources.pfund.source import PFundDataCategory
from pfeed.sources.pfund.mixin import PFundMixin
from pfeed.data_client import DataClient


__all__ = ['PFund']


class PFund(PFundMixin, DataClient):
    engine_feed: PFundEngineFeed
    component_feed: PFundComponentFeed

    def __init__(self, env: Environment):
        self.env = Environment[env.upper()]
        self.engine_feed: PFundEngineFeed | None = None
        self.component_feed: PFundComponentFeed | None = None
    
    def _create_feeds(self):
        for data_category in self.data_categories:
            if data_category == PFundDataCategory.ENGINE_DATA:
                self.engine_feed = PFundEngineFeed()
            elif data_category == PFundDataCategory.COMPONENT_DATA:
                self.component_feed = PFundComponentFeed()
            else:
                raise ValueError(f'{data_category} is not supported')

    def get_feed(self, data_category: PFundDataCategory) -> BaseFeed | None:
        return getattr(self, PFundDataCategory[data_category.upper()].feed_name, None)
