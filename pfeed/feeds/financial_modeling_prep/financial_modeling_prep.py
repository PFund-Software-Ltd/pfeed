from __future__ import annotations
from typing import Literal

import os
from enum import StrEnum

from pfeed.typing.literals import tDATA_TOOL
from pfeed.feeds.financial_modeling_prep import (
    FMPEconomicFeed,
    FMPFundamentalFeed,
    FMPNewsFeed,
    FMPMarketFeed,
    FMPAnalystFeed,
)


class FMPCategory(StrEnum):
    economic = 'economic'
    fundamental = 'fundamental'
    news = 'news'
    market = 'market'
tCATEGORY = Literal['economic', 'fundamental', 'news', 'market']
FMPDataFeed = FMPEconomicFeed | FMPFundamentalFeed | FMPNewsFeed | FMPMarketFeed


class FinancialModelingPrep:
    def __init__(
        self, 
        api_key: str | None=None,
        data_tool: tDATA_TOOL='polars', 
        pipeline_mode: bool=False,
        use_ray: bool=True,
        use_prefect: bool=False,
        use_bytewax: bool=False,
        use_deltalake: bool=False,
    ):
        params = {k: v for k, v in locals().items() if k != 'self'}
        self.economic = FMPEconomicFeed(**params)
        self.fundamental = FMPFundamentalFeed(**params)
        self.news = FMPNewsFeed(**params)
        self.market = FMPMarketFeed(**params)
        self.analyst = FMPAnalystFeed(**params)
    
    @property
    def categories(self) -> list[FMPCategory]:
        return [category.value for category in FMPCategory]
    
    def download(self, category: tCATEGORY, **kwargs) -> FMPDataFeed:
        category = FMPCategory[category]
        if category == FMPCategory.economic:
            return self.economic.download(**kwargs)
        elif category == FMPCategory.fundamental:
            return self.fundamental.download(**kwargs)
        elif category == FMPCategory.news:
            return self.news.download(**kwargs)
        elif category == FMPCategory.market:
            return self.market.download(**kwargs)
    
    def retrieve(self, category: tCATEGORY, **kwargs) -> FMPDataFeed:
        category = FMPCategory[category]
        if category == FMPCategory.economic:
            return self.economic.retrieve(**kwargs)
        elif category == FMPCategory.fundamental:
            return self.fundamental.retrieve(**kwargs)
        elif category == FMPCategory.news:
            return self.news.retrieve(**kwargs)
        elif category == FMPCategory.market:
            return self.market.retrieve(**kwargs)
    
    def fetch(self, category: tCATEGORY, **kwargs) -> FMPDataFeed:
        category = FMPCategory[category]
        if category == FMPCategory.economic:
            return self.economic.fetch(**kwargs)
        elif category == FMPCategory.fundamental:
            return self.fundamental.fetch(**kwargs)
        elif category == FMPCategory.news:
            return self.news.fetch(**kwargs)
        elif category == FMPCategory.market:
            return self.market.fetch(**kwargs)
