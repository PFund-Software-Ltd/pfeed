from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfund.typing import ResolutionRepr

from pfeed.data_models.market_data_model import MarketDataModel
from pfund.datas.resolution import Resolution


class RetrieveMarketDataModel(MarketDataModel):
    '''
    Data model for retrieving market data used by retrieve().
    '''
    auto_resample: bool = True
    
    def update_resolution(self, resolution: Resolution | ResolutionRepr) -> None:
        if isinstance(resolution, str):
            resolution = Resolution(resolution)
        self.resolution = resolution