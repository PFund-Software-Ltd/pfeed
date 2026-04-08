from __future__ import annotations
from typing import TYPE_CHECKING, Literal
if TYPE_CHECKING:
    from pfeed.feeds.base_feed import BaseFeed

from pfeed.sources.pfund.component_feed import ComponentFeed
from pfeed.sources.pfund.engine_feed import EngineFeed
from pfeed.sources.pfund.mixin import PFundMixin
from pfeed.data_client import DataClient


__all__ = ['PFund']


class PFund(PFundMixin, DataClient):
    engine_feed: EngineFeed
    component_feed: ComponentFeed

    def _create_feeds(self):
        self.engine_feed = EngineFeed()
        self.component_feed = ComponentFeed()

    def get_feed(self, data_category: Literal['engine', 'component']) -> BaseFeed | None:
        if data_category == 'engine':
            return self.engine_feed
        elif data_category == 'component':
            return self.component_feed
        else:
            raise ValueError(f'{data_category} is not supported')
