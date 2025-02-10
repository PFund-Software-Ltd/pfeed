from typing import Literal

from fmp_api_client import FMPClient, FMPPlan
from pfeed.sources.tradfi_source import TradFiSource


__all__ = ["FinancialModelingPrepSource"]


class FinancialModelingPrepSource(TradFiSource):
    def __init__(
        self, 
        api_key: str | None=None, 
        fmp_plan: Literal['basic', 'starter', 'premium', 'ultimate']='basic',
    ):
        super().__init__('FINANCIAL_MODELING_PREP', api_key=api_key)
        self.api = FMPClient(api_key=self._api_key)
        self.plan = FMPPlan[fmp_plan.upper()]