from __future__ import annotations
from typing import Literal

from enum import StrEnum

from pfeed.typing.literals import tDATA_TOOL
from pfeed.feeds.financial_modeling_prep import (
    FMPEconomicsFeed,
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
FMPDataFeed = FMPEconomicsFeed | FMPFundamentalFeed | FMPNewsFeed | FMPMarketFeed


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
        self.news = FMPNewsFeed(**params)
        # self.economic = FMPEconomicsFeed(**params)
        # self.fundamental = FMPFundamentalFeed(**params)
        # self.market = FMPMarketFeed(**params)
        # self.analyst = FMPAnalystFeed(**params)
        self.source = self.news.source
        self.api = self.source.api
    
    @property
    def categories(self) -> list[FMPCategory]:
        return [category.value for category in FMPCategory]
    