from pfeed.feeds.base_feed import BaseFeed


class AnalystDataFeed(BaseFeed):
    DATA_DOMAIN = 'analyst_data'

    def fetch(self):
        pass