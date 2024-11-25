from __future__ import annotations
from typing import Literal, TYPE_CHECKING
if TYPE_CHECKING:
    from pfeed.types.core import tDataFrame
    from pfeed.types.literals import tSTORAGE

from pfeed.feeds.market_data_feed import MarketDataFeed


class CryptoMarketDataFeed(MarketDataFeed):
    def get_historical_data(
        self,
        product: str,
        resolution: str="1tick",
        rollback_period: str="1d",
        start_date: str="",
        end_date: str="",
        raw_level: Literal['cleaned', 'normalized', 'original']='normalized',
        from_storage: tSTORAGE | None=None,
    ) -> tDataFrame:
        return super().get_historical_data(
            product, 
            resolution=resolution, 
            rollback_period=rollback_period, 
            start_date=start_date, 
            end_date=end_date, 
            raw_level=raw_level,
            from_storage=from_storage,
        )
