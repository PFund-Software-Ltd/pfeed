from pfeed.sources.pfund.mixin import PFundMixin
from pfeed.sources.pfund.component_data_model import PFundComponentDataModel
from pfeed.feeds.base_feed import BaseFeed


# TODO
class PFundComponentFeed(PFundMixin, BaseFeed):
    def create_data_model(self, *args, **kwargs):
        pass
    
    def download(self):
        pass
    
    def _download_impl(self, data_model: PFundComponentDataModel):
        pass

    def _get_default_transformations_for_download(self, *args, **kwargs):
        pass

    def retrieve(self):
        pass

    def _retrieve_impl(self, data_model: PFundComponentDataModel):
        pass
    
    def _get_default_transformations_for_retrieve(self, *args, **kwargs):
        pass

    def _create_data_model_from_request(self, request):
        pass
    
    def _create_batch_dataflows(self, *args, **kwargs):
        pass
    
    def run(self, **prefect_kwargs):
        pass
