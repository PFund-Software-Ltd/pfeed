from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfeed.feeds.base_feed import BaseFeed
    from pfeed.sources.pfund.engine_feed import PFundEngineFeed

from pfund.enums import Environment
from pfeed.sources.pfund.source import PFundDataCategory
from pfeed.sources.pfund.mixin import PFundMixin
from pfeed.data_client import DataClient


__all__ = ['PFund']


class PFund(PFundMixin, DataClient):
    engine_feed: PFundEngineFeed
    
    def __init__(self, env: Environment):
        params = {k: v for k, v in locals().items() if k not in ['self', 'env']}
        self.env = Environment[env.upper()]
    
    # TODO: create PFundEngineFeed
    def _create_feeds(self):
        pass
        # for data_category in self.data_categories:
        #     pass

    def get_feed(self, data_category: PFundDataCategory) -> BaseFeed | None:
        return getattr(self, PFundDataCategory[data_category.upper()].feed_name, None)
