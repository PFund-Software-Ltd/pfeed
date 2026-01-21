from typing import Literal

import os

from fmp_api_client import FMPClient, FMPPlan

from pfeed.sources.tradfi_data_provider_source import TradFiDataProviderSource


__all__ = ["FinancialModelingPrepSource"]


class FinancialModelingPrepSource(TradFiDataProviderSource):
    def __init__(
        self, 
        api_key: str | None=None, 
        fmp_plan: Literal['basic', 'starter', 'premium', 'ultimate']='',
    ):
        super().__init__('FINANCIAL_MODELING_PREP', api_key=api_key)
        self.api = FMPClient(api_key=self._api_key)
        fmp_plan = fmp_plan or os.getenv('FMP_PLAN') or os.getenv('FINANCIAL_MODELING_PREP_PLAN') or 'basic'
        self.plan = FMPPlan[fmp_plan.upper()]