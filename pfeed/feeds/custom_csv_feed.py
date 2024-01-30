from pfeed.feeds.base_feed import BaseFeed


__all__ = ['CustomCsvFeed']


# TODO
class CustomCsvFeed(BaseFeed):
    def __init__(self, name='custom_csv'):
        super().__init__(name)
        
    def get_historical_data(self, *args, **kwargs):
        raise NotImplementedError