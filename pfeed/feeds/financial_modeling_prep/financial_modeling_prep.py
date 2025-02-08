
from __future__ import annotations
from typing import Literal

import os
from enum import StrEnum

from pfeed.typing.literals import tDATA_TOOL
from pfeed.feeds.financial_modeling_prep import (
    FMPEconomicDataFeed,
    FMPFundamentalDataFeed,
    FMPNewsDataFeed,
    FMPMarketDataFeed,
    FMPAnalystDataFeed,
)


class FMPCategory(StrEnum):
    economic = 'economic'
    fundamental = 'fundamental'
    news = 'news'
    market = 'market'
tCATEGORY = Literal['economic', 'fundamental', 'news', 'market']
FMPDataFeed = FMPEconomicDataFeed | FMPFundamentalDataFeed | FMPNewsDataFeed | FMPMarketDataFeed

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
        self._api_key = api_key or os.getenv('FMP_API_KEY')
        assert self._api_key, 'FMP_API_KEY is not set'
        params = {k: v for k, v in locals().items() if k != 'self'}
        self.economic = FMPEconomicDataFeed(**params)
        self.fundamental = FMPFundamentalDataFeed(**params)
        self.news = FMPNewsDataFeed(**params)
        self.market = FMPMarketDataFeed(**params)
        self.analyst = FMPAnalystDataFeed(**params)
    
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
