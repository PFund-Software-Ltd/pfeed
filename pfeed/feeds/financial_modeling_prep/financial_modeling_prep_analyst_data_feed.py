from pfeed.feeds.analyst_data_feed import AnalystDataFeed


class FinancialModelingPrepAnalystDataFeed(AnalystDataFeed):
    def __init__(self, api_key: str, **kwargs):
        super().__init__(**kwargs)
        self._api_key = api_key
    
    def fetch(self):
        pass
