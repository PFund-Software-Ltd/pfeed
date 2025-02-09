from pfeed.feeds.news_feed import NewsFeed


class FinancialModelingPrepNewsFeed(NewsFeed):
    def __init__(self, api_key: str):
        super().__init__()
        self._api_key = api_key
