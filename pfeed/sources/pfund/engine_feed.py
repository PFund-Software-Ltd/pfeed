from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfund.enums import ComponentType, Environment

import datetime

from pfeed.feeds.base_feed import BaseFeed
from pfeed.sources.pfund.mixin import PFundMixin
from pfeed.sources.pfund.engine_data_model import EngineDataModel


# TODO: get around the data source issue, since technically its data source is the engine.
# this feed should be able to get backtesting data from pfund's BacktestEngine for monitoring and analysis puporse
# some functions require api calls (e.g. get dynamic backtest results) and some do not (e.g. load backtest hisory)
# TODO: need to specify which engine if there are multiple engines running
class EngineFeed(PFundMixin, BaseFeed):
    # FIXME: handle params, need to pass them to BaseFeed?
    def __init__(self, env: Environment, engine_name: str, **params):
        self._env = env
        self._engine_name = engine_name
        super().__init__(**params)
        
    def create_data_model(
        self, 
        start_date: datetime.date | None=None,
        end_date: datetime.date | None=None,
        component_name: str | None=None,
        component_type: ComponentType | None=None,
    ) -> EngineDataModel:
        return EngineDataModel(
            ...
        )
    
    def download(self):
        pass
    
    def _download_impl(self, data_model: EngineDataModel):
        pass

    def _get_default_transformations_for_download(self, *args, **kwargs):
        pass

    def retrieve(self):
        pass

    def _retrieve_impl(self, data_model: EngineDataModel):
        pass
    
    def _get_default_transformations_for_retrieve(self, *args, **kwargs):
        pass

    def _create_data_model_from_request(self, request):
        pass
    
    def _create_batch_dataflows(self, *args, **kwargs):
        pass
    
    def run(self, **prefect_kwargs):
        pass
