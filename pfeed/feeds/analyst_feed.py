from pfeed.feeds.base_feed import BaseFeed


class AnalystFeed(BaseFeed):
    data_domain = 'analyst_data'

    def fetch(self):
        pass