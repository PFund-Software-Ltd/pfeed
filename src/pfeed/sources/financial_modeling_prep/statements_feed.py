from pfeed.feeds.statements_feed import StatementsFeed


class FinancialModelingPrepStatementsFeed(StatementsFeed):
    def __init__(self, api_key: str):
        super().__init__()
        self._api_key = api_key
