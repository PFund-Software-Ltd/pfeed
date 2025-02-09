from fmp_api_client import FMPClient

from pfeed.sources.base_source import BaseSource


__all__ = ["FinancialModelingPrepSource"]


class FinancialModelingPrepSource(BaseSource):
    def __init__(self, api_key: str | None=None):
        super().__init__('FINANCIAL_MODELING_PREP', api_key=api_key)
        self.api = FMPClient(api_key=self._api_key)
        