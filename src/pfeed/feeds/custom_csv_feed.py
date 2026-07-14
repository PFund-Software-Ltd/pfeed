from pfeed.feeds.base_feed import BaseFeed


# TODO
class CustomCsvFeed(BaseFeed):
    def __init__(self, name="custom_csv"):
        super().__init__(name)
