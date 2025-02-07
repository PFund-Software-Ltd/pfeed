import os


class FinancialModelingPrep:
    def __init__(self, api_key: str | None=None):
        self._api_key = api_key or os.getenv('FMP_API_KEY')
        assert self._api_key, 'FMP_API_KEY is not set'
        self.economic = EconomicDataFeed(api_key=self._api_key)
        self.fundamental = FundamentalDataFeed(api_key=self._api_key)
        self.news = NewsDataFeed(api_key=self._api_key)
    
    # TODO: how to handle duplicated functions like run(), download(), etc?
    def __getattr__(self, name):
        # Try economic methods first
        if hasattr(self.economic, name):
            return getattr(self.economic, name)
        # Then try fundamental methods
        if hasattr(self.fundamental, name):
            return getattr(self.fundamental, name)
        # If method not found in any component, raise AttributeError
        raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")