from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfund.enums import ComponentType, Environment

import datetime

from pfeed.feeds.base_feed import BaseFeed
from pfeed.sources.pfund.mixin import PFundMixin
from pfeed.sources.pfund.data_model import PFundDataModel


# TODO: get around the data source issue, since technically its data source is the engine.
# this feed should be able to get backtesting data from pfund's BacktestEngine for monitoring and analysis puporse
# some functions require api calls (e.g. get dynamic backtest results) and some do not (e.g. load backtest hisory)
class PFundEngineFeed(PFundMixin, BaseFeed):
    def __init__(self, env: Environment, **params):
        self._env = env
        super().__init__(**params)
    
    def create_data_model(
        self, 
        start_date: datetime.date | None=None,
        end_date: datetime.date | None=None,
        component_name: str | None=None,
        component_type: ComponentType | None=None,
    ) -> PFundDataModel:
        return PFundDataModel(
            ...
        )