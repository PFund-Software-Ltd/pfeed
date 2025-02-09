from pfeed.feeds.base_feed import BaseFeed


class AnalystFeed(BaseFeed):
    DATA_DOMAIN = 'analyst_data'

    def fetch(self):
        pass