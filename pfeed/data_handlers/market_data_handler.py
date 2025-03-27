from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    import pandas as pd
    from pfeed.typing import GenericFrame
    from pfeed.data_models.market_data_model import MarketDataModel

from pfund.datas.resolution import Resolution
from pfeed.data_handlers.time_based_data_handler import TimeBasedDataHandler


class MarketDataHandler(TimeBasedDataHandler):
    def _validate_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        from pfeed.schemas import MarketDataSchema, TickDataSchema, BarDataSchema
        data_model: MarketDataModel = self._data_model
        resolution: Resolution = data_model.resolution
        if resolution.is_quote():
            raise NotImplementedError('quote data is not supported yet')
        elif resolution.is_tick():
            schema = TickDataSchema
        elif resolution.is_bar():
            schema = BarDataSchema
        else:
            schema = MarketDataSchema
        return schema.validate(df)

    def write(self, df: GenericFrame):
        data_model: MarketDataModel = self._data_model
        metadata = {
            'start_date': data_model.start_date, 
            'end_date': data_model.end_date,
            'resolution': data_model.resolution,
            'product': data_model.product.name,
            'product_type': data_model.product.type,
            'data_source': data_model.data_source.name,
            'data_origin': data_model.data_origin,
        }
        return super().write(df, metadata=metadata)
