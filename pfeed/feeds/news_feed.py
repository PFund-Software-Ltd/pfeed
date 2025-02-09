from pfeed.feeds.base_feed import BaseFeed
from pfeed.data_models.news_data_model import NewsDataModel
from pfeed.storages.base_storage import BaseStorage


class NewsFeed(BaseFeed):
    DATA_DOMAIN = 'news_data'

    def create_data_model(self, *args, **kwargs) -> NewsDataModel:
        pass
    
    def create_storage(self, *args, **kwargs) -> BaseStorage:
        pass
