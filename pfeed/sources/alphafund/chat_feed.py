from pfeed.sources.alphafund.mixin import AlphaFundMixin
from pfeed.sources.alphafund.chat_data_model import ChatDataModel
from pfeed.feeds.base_feed import BaseFeed


# TODO
class ChatFeed(AlphaFundMixin, BaseFeed):
    def create_data_model(self, *args, **kwargs):
        pass
    
    def download(self):
        pass
    
    def _download_impl(self, data_model: ChatDataModel):
        pass

    def retrieve(self):
        pass

    def _retrieve_impl(self, data_model: ChatDataModel):
        pass
    
    def _create_data_model_from_request(self, request):
        pass
    
    def _create_batch_dataflows(self, *args, **kwargs):
        pass
    
    def run(self, **prefect_kwargs):
        pass
