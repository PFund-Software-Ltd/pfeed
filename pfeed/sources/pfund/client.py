from pfeed.data_client import DataClient
from pfeed.sources.pfund.mixin import PFundMixin
from pfeed.sources.pfund.engine_feed import PFundEngineFeed
from pfeed.sources.pfund.component_feed import PFundComponentFeed


__all__ = ['PFund']


class PFund(PFundMixin, DataClient):
    engine_feed: PFundEngineFeed
    component_feed: PFundComponentFeed

    def _create_feeds(self):
        self.engine_feed = PFundEngineFeed()
        self.component_feed = PFundComponentFeed()
