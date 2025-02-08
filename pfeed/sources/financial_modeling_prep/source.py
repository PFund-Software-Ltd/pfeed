from fmp_api_client import FMPClient

from pfeed.sources.base_source import BaseSource


__all__ = ["FinancialModelingPrepSource"]


class FinancialModelingPrepSource(BaseSource):
    def __init__(self):
        super().__init__('FINANCIAL_MODELING_PREP')
        self._api_key = os.getenv('FMP_API_KEY')
        assert self._api_key, 'FMP_API_KEY is not set'
        self.api = FMPClient(api_key=api_key)
    
    def get_products_by_type(self, product_type: str) -> list[str]:
        pass
