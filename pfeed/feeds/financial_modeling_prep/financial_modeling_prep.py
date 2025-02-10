from __future__ import annotations
from typing import Literal, TYPE_CHECKING
if TYPE_CHECKING:
    from fmp_api_client import FMPClient
    from pfeed.typing.literals import tDATA_TOOL

from enum import StrEnum

from fmp_api_client.plan import FMPPlan

from pfeed.sources.financial_modeling_prep.source import FinancialModelingPrepSource
from pfeed.feeds.financial_modeling_prep import (
    FMPEconomicsFeed,
    FMPNewsFeed,
    FMPMarketFeed,
    FMPAnalystFeed,
)


class FMPCategory(StrEnum):
    economic = 'economic'
    news = 'news'
    market = 'market'
tCATEGORY = Literal['economic', 'news', 'market']
FMPDataFeed = (
    FMPEconomicsFeed | 
    FMPNewsFeed | 
    FMPMarketFeed |
    FMPAnalystFeed
)


class FinancialModelingPrep:
    def __init__(
        self, 
        api_key: str | None=None,
        fmp_plan: Literal['basic', 'starter', 'premium', 'ultimate']='basic',
        data_tool: tDATA_TOOL='polars', 
        pipeline_mode: bool=False,
        use_ray: bool=False,
        use_prefect: bool=False,
        use_bytewax: bool=False,
        use_deltalake: bool=False,
    ):
        params = {k: v for k, v in locals().items() if k not in ['self', 'api_key', 'fmp_plan']}
        params['data_source'] = FinancialModelingPrepSource(api_key=api_key, fmp_plan=fmp_plan)
        self.news = FMPNewsFeed(**params)
        # self.economic = FMPEconomicsFeed(**params)
        # self.market = FMPMarketFeed(**params)
        # self.analyst = FMPAnalystFeed(**params)

    @property
    def data_source(self) -> FinancialModelingPrepSource:
        return self.news.data_source
    
    @property
    def api(self) -> FMPClient:
        return self.data_source.api
    
    @property
    def plan(self) -> FMPPlan:
        return self.data_source.plan
    
    @property
    def categories(self) -> list[FMPCategory]:
        return [category.value for category in FMPCategory]
    