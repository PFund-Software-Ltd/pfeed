from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfund.enums import ComponentType, Environment
    
from pfeed.sources.pfund.mixin import PFundMixin
from pfeed.sources.pfund.component_data_model import ComponentDataModel
from pfeed.feeds.base_feed import BaseFeed


class ComponentFeed(PFundMixin, BaseFeed):
    def __init__(self, env: Environment, engine_name: str, **params):
        self._env = env
        self._engine_name = engine_name
        super().__init__(**params)

    def create_data_model(self, *args, **kwargs) -> ComponentDataModel:
        pass
    
    def _create_data_model_from_request(self, request):
        pass

    # TODO: download component's artifacts that are stored in cloud?
    # def download(self, *args, **kwargs):
    #     raise NotImplementedError(f'{self.name} download() is not implemented')
    # def _download_impl(self, data_model: ComponentDataModel):
    #     pass
    # def _get_default_transformations_for_download(self, *args, **kwargs):
    #     pass

    def retrieve(self, component_name: str, component_type: str | ComponentType):
        pass

    def _retrieve_impl(self, data_model: ComponentDataModel):
        pass

    def _get_default_transformations_for_retrieve(self, *args, **kwargs):
        pass
    
    # TODO: stream component's signals
    def stream(self, component_name: str, component_type: str | ComponentType):
        pass
    
    def _stream_impl(self, data_model: ComponentDataModel):
        pass
    
    def _create_batch_dataflows(self, *args, **kwargs):
        pass
    
    def run(self, **prefect_kwargs):
        pass
